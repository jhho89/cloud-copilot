from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
K8S_AGENTS = REPO_ROOT / "services" / "k8s-copilot" / "src" / "agents"
if str(K8S_AGENTS) not in sys.path:
    sys.path.insert(0, str(K8S_AGENTS))

from dba_agent import clickhouse, tools  # noqa: E402


def _pod(
    name: str,
    namespace: str = "analytics",
    labels: dict[str, str] | None = None,
    image: str = "clickhouse/clickhouse-server:23.8",
    container_name: str = "clickhouse",
    statefulset: str | None = "clickhouse",
) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace=namespace,
            labels=labels or {"app.kubernetes.io/name": "clickhouse"},
            owner_references=[SimpleNamespace(kind="StatefulSet", name=statefulset)] if statefulset else [],
        ),
        spec=SimpleNamespace(containers=[SimpleNamespace(name=container_name, image=image)]),
        status=SimpleNamespace(phase="Running"),
    )


def test_find_database_pods_matches_altinity_bitnami_and_keeper(monkeypatch) -> None:
    altinity = _pod(
        "chi-clickhouse-0-0",
        labels={
            "clickhouse.altinity.com/shard": "01",
            "clickhouse.altinity.com/replica": "02",
            "clickhouse.altinity.com/cluster": "prod",
        },
    )
    bitnami = _pod(
        "clickhouse-0",
        namespace="reports",
        labels={"app.kubernetes.io/name": "clickhouse", "app.kubernetes.io/component": "primary"},
        image="bitnami/clickhouse:23.8",
    )
    keeper = _pod(
        "zookeeper-0",
        namespace="coordination",
        labels={"app.kubernetes.io/name": "zookeeper"},
        image="bitnami/zookeeper:3.9",
        container_name="zookeeper",
    )
    pod_list = SimpleNamespace(items=[altinity, bitnami, keeper])

    class DiscoveryApi:
        def list_pod_for_all_namespaces(self, **_kwargs):
            return pod_list

    class CustomApi:
        def list_namespaced_custom_object(self, **_kwargs):
            return {"items": [{"metadata": {"name": "prod", "namespace": "analytics"}}]}

    custom = SimpleNamespace(api_client=object())
    v1 = SimpleNamespace(list_pod_for_all_namespaces=lambda **kwargs: pod_list, api_client=object())

    def fake_custom(v1):
        return CustomApi()

    original_custom = clickhouse._clickhouse_custom_objects
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: v1)
    clickhouse._clickhouse_custom_objects = fake_custom
    try:
        server_result = tools.find_database_pods("clickhouse", installation_name="prod")
        keeper_result = tools.find_database_pods("keeper")
    finally:
        clickhouse._clickhouse_custom_objects = original_custom

    assert "analytics/chi-clickhouse-0-0" in server_result
    assert "statefulset=clickhouse" in server_result
    assert "ordinal=0" in server_result
    assert "shard=01" in server_result
    assert "replica=02" in server_result
    assert "clickhouseinstallation=analytics/prod" in server_result
    assert "reports/clickhouse-0" in server_result
    assert "zookeeper" not in server_result
    assert "coordination/zookeeper-0" in keeper_result


def test_find_database_pods_reports_optional_installation_rbac_denial(monkeypatch) -> None:
    pod = _pod("clickhouse-0")
    pod_list = SimpleNamespace(items=[pod])
    api = SimpleNamespace(list_namespaced_pod=lambda **kwargs: pod_list, api_client=object())

    class CustomApi:
        def list_namespaced_custom_object(self, **_kwargs):
            raise RuntimeError('403 Forbidden: clickhouseinstallations is forbidden')

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(clickhouse, "_clickhouse_custom_objects", lambda _v1: CustomApi())

    result = tools.find_database_pods("clickhouse", namespace="analytics")

    assert "analytics/clickhouse-0" in result
    assert "ClickHouseInstallation access denied" in result


def test_clickhouse_client_detection_prefers_client_and_falls_back_to_binary(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_exec(namespace, pod, command, container=None, operation="", timeout_seconds=30, target_path=None):
        calls.append(list(command))
        if command[0] == "clickhouse-client":
            raise RuntimeError("exec failed")
        return "ClickHouse local version 23.8.1"

    monkeypatch.setattr(tools, "_pod_exec", fake_exec)
    prefix = clickhouse._detect_clickhouse_client("analytics", "clickhouse-0", "database")

    assert prefix == ["clickhouse", "client"]
    assert calls == [["clickhouse-client", "--version"], ["clickhouse", "client", "--version"]]


def test_clickhouse_query_uses_direct_argv_parameters_and_audit(monkeypatch, caplog) -> None:
    commands: list[list[str]] = []

    class ExecApi:
        def connect_get_namespaced_pod_exec(self, **kwargs):
            return "ok"

    def fake_stream(_connect_method, **kwargs):
        commands.append(list(kwargs["command"]))
        if kwargs["command"][0] == "clickhouse-client" and kwargs["command"][1] == "--version":
            return "version"
        return json.dumps({"readonly": "1", "version": "23.8.1.1"})

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: ExecApi())
    monkeypatch.setattr(tools.stream, "stream", fake_stream)
    caplog.set_level(logging.INFO, logger=tools._AUDIT_LOGGER.name)

    rows, _ = clickhouse._run_clickhouse_query(
        "analytics",
        "clickhouse-0",
        "clickhouse_readonly_profile.v1",
        {},
        "database",
    )

    assert rows == [{"readonly": "1", "version": "23.8.1.1"}]
    query_command = commands[-1]
    assert commands[0] == ["clickhouse-client", "--version"]
    assert query_command[:5] == [
        "clickhouse-client",
        "--config-file",
        "/etc/clickhouse-client/copilot_dba.xml",
        "--format",
        "JSONEachRow",
    ]
    assert "--query" in query_command
    assert query_command[query_command.index("--query") + 1].startswith("SELECT getSetting('readonly')")
    assert "--param_" not in query_command
    for command in commands:
        assert all(isinstance(argument, str) for argument in command)
        assert "password" not in [argument.lower() for argument in command]
        assert "secret" not in command
    assert len(caplog.records) >= 3
    audit = "\n".join(record.getMessage() for record in caplog.records)
    assert '"event":"dba.pod_exec"' in audit
    assert '"operation":"clickhouse_readonly_profile_check"' in audit
    assert '"operation":"clickhouse_readonly_query"' in audit
    assert "getSetting" not in audit
    assert "secret" not in audit


def test_clickhouse_query_argv_binds_parameters_without_interpolation() -> None:
    argv = clickhouse._clickhouse_query_argv(
        ["clickhouse-client"],
        "SELECT {start_time:DateTime}, {limit:UInt64}",
        {"start_time": "2026-01-01 00:00:00", "limit": "25"},
    )

    assert argv == [
        "clickhouse-client",
        "--config-file",
        "/etc/clickhouse-client/copilot_dba.xml",
        "--format",
        "JSONEachRow",
        "--param_limit",
        "25",
        "--param_start_time",
        "2026-01-01 00:00:00",
        "--query",
        "SELECT {start_time:DateTime}, {limit:UInt64}",
    ]


def test_clickhouse_refuses_non_readonly_profile(monkeypatch) -> None:
    def fake_exec(namespace, pod, command, container=None, operation="", timeout_seconds=30, target_path=None):
        if operation == "clickhouse_readonly_profile_check":
            return json.dumps({"readonly": 2, "version": "23.8.1.1"})
        return "version"

    monkeypatch.setattr(tools, "_pod_exec", fake_exec)

    with pytest.raises(clickhouse.ClickHouseReadOnlyProfileError, match="expected 1"):
        clickhouse._run_clickhouse_query("analytics", "clickhouse-0", "clickhouse_logging.v1")


def test_render_clickhouse_template_clamps_and_rejects_unknown_params() -> None:
    sql, bindings = clickhouse.render_clickhouse_template(
        "clickhouse_query_log.v1",
        {
            "start_time": "2026-01-01 00:00:00",
            "end_time": "2026-01-01 01:00:00",
            "min_duration_ms": 0,
            "limit": 999,
            "query_kind": "Select",
            "include_distributed": False,
            "sort_by": "read_bytes",
        },
    )

    assert "{start_time:DateTime}" in sql
    assert "{limit:UInt64}" in sql
    assert bindings["min_duration_ms"] == "1"
    assert bindings["limit"] == "100"
    assert bindings["include_distributed"] == "0"
    with pytest.raises(ValueError, match="Unknown ClickHouse template parameters"):
        clickhouse.render_clickhouse_template(
            "clickhouse_query_log.v1",
            {
                "start_time": "2026-01-01 00:00:00",
                "end_time": "2026-01-01 01:00:00",
                "min_duration_ms": 1000,
                "limit": 10,
                "query_kind": "Select",
                "include_distributed": False,
                "sort_by": "read_bytes",
                "table": "system.numbers",
            },
        )


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE t",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET x = 1",
        "DELETE FROM t",
        "ALTER TABLE t ADD COLUMN x Int32",
        "CREATE TABLE t (x Int32) ENGINE=MergeTree",
        "TRUNCATE TABLE t",
        "RENAME TABLE a TO b",
        "EXCHANGE TABLES a AND b",
        "ATTACH TABLE t",
        "DETACH TABLE t",
        "OPTIMIZE TABLE t",
        "SYSTEM FLUSH LOGS",
        "KILL QUERY WHERE query_id='x'",
        "SET max_threads = 1",
        "GRANT SELECT ON *.* TO u",
        "REVOKE SELECT ON *.* FROM u",
        "BACKUP TABLE t TO Disk('x', 'y')",
        "RESTORE TABLE t FROM Disk('x', 'y')",
        "UNDROP TABLE t",
        "MOVE PARTITION TO TABLE t2",
        "SELECT 1 SETTINGS max_threads=1",
        "SELECT 1 FORMAT JSON",
        "SELECT 1 INTO OUTFILE '/tmp/x'",
        "SELECT 1; SELECT 2",
        "SELECT 1 -- hidden SETTINGS",
        "SELECT 1 /* hidden FORMAT */",
    ],
)
def test_clickhouse_user_sql_guard_blocks_writes_settings_format_and_comments(sql: str) -> None:
    with pytest.raises(clickhouse.ClickHouseUserSQLBlockedError):
        clickhouse._guard_clickhouse_user_sql(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM url('https://example.invalid')",
        "SELECT * FROM s3('https://example.invalid/a.parquet')",
        "SELECT * FROM hdfs('hdfs://example.invalid/a.parquet')",
        "SELECT * FROM mysql('db:3306', 'db', 't', 'u', 'p')",
        "SELECT * FROM postgresql('db:5432', 'db', 't', 'u', 'p')",
        "SELECT * FROM file('data.csv', 'CSV')",
        "SELECT * FROM remote('clickhouse-1:9000', 'db', 't')",
        "SELECT * FROM clusterAllReplicas('prod', 'db.t')",
        "SELECT * FROM generateRandom('x UInt8')",
    ],
)
def test_clickhouse_user_sql_guard_blocks_external_table_functions(sql: str) -> None:
    with pytest.raises(clickhouse.ClickHouseUserSQLBlockedError, match="table function"):
        clickhouse._guard_clickhouse_user_sql(sql, allow_cluster_all_replicas=True)


def test_clickhouse_user_sql_guard_wraps_allowed_select_and_binds_values() -> None:
    allowed = clickhouse._guard_clickhouse_user_sql(
        "SELECT number FROM numbers(10) WHERE number > {page:UInt8}"
    )

    assert allowed == "EXPLAIN SELECT number FROM numbers(10) WHERE number > {page:UInt8}"
    with pytest.raises(clickhouse.ClickHouseUserSQLBlockedError, match="clusterAllReplicas"):
        clickhouse._guard_clickhouse_user_sql(
            "SELECT * FROM clusterAllReplicas('prod', 'db.t')",
            allow_cluster_all_replicas=False,
        )


def test_parse_clickhouse_query_log_rows_modern_and_legacy_variants() -> None:
    modern = clickhouse.parse_clickhouse_query_log_rows(
        "analytics",
        "clickhouse-0",
        [
            {
                "query_hash": "8123456789",
                "run_count": 3,
                "total_duration_ms": "9000.5",
                "max_duration_ms": 5000,
                "p50_duration_ms": 3000,
                "p95_duration_ms": 5000,
                "p99_duration_ms": 5000,
                "read_rows": "1000000",
                "read_bytes": "102400000",
                "peak_memory_usage": 2147483648,
                "total_memory_usage": 4294967296,
                "result_rows": 100,
                "written_rows": 0,
                "log_types": ["QueryFinish", "ExceptionWhileProcessing"],
                "exception_codes": [241, None],
                "exception": "Memory limit exceeded",
                "normalized_query": "SELECT count() FROM t WHERE x = ?",
                "ProfileEvents": {"ReadMarks": 25, "SelectedMarks": 25, "DiskReadBytes": 102400000},
            }
        ],
    )
    legacy = clickhouse.parse_clickhouse_query_log_rows(
        "analytics",
        "clickhouse-legacy-0",
        [
            {
                "query_id": "legacy-id",
                "run_count": 1,
                "query_duration_ms": 1500,
                "rows_read": 1000,
                "bytes_read": 5000,
                "memory_usage": None,
                "exception": None,
            }
        ],
    )[0]

    assert modern[0].query_hash == "8123456789"
    assert modern[0].total_duration_ms == 9000.5
    assert modern[0].read_bytes == 102400000
    assert modern[0].peak_memory_usage == 2147483648
    assert modern[0].profile_events["ReadMarks"] == 25
    assert clickhouse._clickhouse_marks_efficiency(modern[0]) == 40000
    assert legacy.query_hash == "legacy-id"
    assert legacy.p95_duration_ms == 0
    assert legacy.profile_events == {}


def test_clickhouse_query_log_template_selection_by_version() -> None:
    assert clickhouse._clickhouse_query_log_template("23.8.1.1") == "clickhouse_query_log.v1"
    assert clickhouse._clickhouse_query_log_template("20.3.9.89") == "clickhouse_query_log_legacy.v1"
    assert clickhouse._clickhouse_query_log_template("not-a-version") == "clickhouse_query_log_legacy.v1"


def test_get_clickhouse_slow_queries_reports_evidence_and_advisory_only(monkeypatch) -> None:
    pod = _pod("clickhouse-0")
    pod_list = SimpleNamespace(items=[pod])
    api = SimpleNamespace(
        list_namespaced_pod=lambda **_kwargs: pod_list,
        read_namespaced_pod=lambda **_kwargs: pod,
    )
    calls: list[str] = []

    def fake_run(namespace, pod_name, template_id, params=None, container=None):
        calls.append(template_id)
        if template_id == "clickhouse_readonly_profile.v1":
            return [{"readonly": "1", "version": "23.8.1.1"}], ""
        if template_id == "clickhouse_logging.v1":
            return [
                {
                    "query_logging": "1",
                    "flush_interval": "7500",
                    "query_log_engine_full": "TTL event_date + toIntervalDay(30)",
                }
            ], ""
        if template_id == "clickhouse_query_log.v1":
            return [
                {
                    "query_hash": "42",
                    "run_count": 2,
                    "total_duration_ms": 5000,
                    "p95_duration_ms": 3000,
                    "read_rows": 500000,
                    "read_bytes": 20401094656,
                    "peak_memory_usage": 2147483648,
                    "result_rows": 10,
                    "written_rows": 0,
                    "exception_codes": [241],
                    "exception": "Memory limit exceeded",
                    "normalized_query": "SELECT count() FROM events",
                    "ProfileEvents": {"ReadMarks": 10},
                }
            ], ""
        return [], ""

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fake_run)

    result = clickhouse.get_clickhouse_slow_queries(
        "analytics",
        "clickhouse-0",
        minutes=90,
        min_duration_ms=1000,
        sort_by="read_bytes",
        output_mode="human",
    )

    assert calls == [
        "clickhouse_readonly_profile.v1",
        "clickhouse_logging.v1",
        "clickhouse_query_log.v1",
        "clickhouse_processes.v1",
    ]
    assert "analytics/clickhouse-0 (server 23.8.1.1)" in result
    assert "query_logging=1" in result
    assert "flush_interval=7500" in result
    assert "TTL event_date + toIntervalDay(30)" in result
    assert "query_hash=42" in result
    assert "read_bytes=20401094656" in result
    assert "rows/mark=50000.0" in result
    assert "Review primary-key/range filtering" in result
    assert "No changes were made" in result

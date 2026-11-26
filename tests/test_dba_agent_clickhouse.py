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


def test_get_clickhouse_replication_report_reports_topology_and_advisory_only(monkeypatch) -> None:
    altinity = _pod(
        "chi-clickhouse-0-0",
        labels={
            "clickhouse.altinity.com/shard": "01",
            "clickhouse.altinity.com/replica": "02",
            "clickhouse.altinity.com/cluster": "prod",
        },
    )
    generic = _pod(
        "clickhouse-1",
        namespace="reports",
        labels={"app.kubernetes.io/name": "clickhouse", "app.kubernetes.io/component": "replica"},
        image="bitnami/clickhouse:23.8",
    )

    def fake_run(_namespace, _pod_name, template_id, _params=None, _container=None):
        if template_id == "clickhouse_topology.v1":
            return [
                {
                    "cluster": "default",
                    "shard_num": 1,
                    "replica_num": 1,
                    "host_name": "clickhouse-0",
                    "host_address": "10.0.0.1",
                    "port": 9000,
                    "is_local": 0,
                    "errors_count": 0,
                    "estimated_recovery_time": 0,
                },
                {
                    "cluster": "default",
                    "shard_num": 1,
                    "replica_num": 2,
                    "host_name": "clickhouse-1",
                    "host_address": "10.0.0.2",
                    "port": 9000,
                    "is_local": 1,
                    "errors_count": 2,
                    "estimated_recovery_time": 30,
                },
            ], ""
        if template_id == "clickhouse_replicas.v1":
            return [
                {
                    "database": "events",
                    "table": "local_events",
                    "replica_name": "replica-2",
                    "replica_path": "/clickhouse/tables/1/events/local_events/replicas/replica-2",
                    "is_leader": 0,
                    "is_readonly": 1,
                    "is_session_expired": 1,
                    "absolute_delay": 65,
                    "queue_size": 12,
                    "inserts_in_queue": 8,
                    "merges_in_queue": 3,
                    "log_max_index": 110,
                    "log_pointer": 100,
                    "total_replicas": 2,
                    "active_replicas": 1,
                    "lost_parts": 1,
                    "last_queue_update_exception": "Cannot enqueue INSERT: session expired",
                    "recovery_time": 45,
                }
            ], ""
        if template_id == "clickhouse_replication_queue.v1":
            return [
                {
                    "database": "events",
                    "table": "local_events",
                    "replica_name": "replica-2",
                    "replica_path": "/clickhouse/tables/1/events/local_events/replicas/replica-2",
                    "node_name": "queue-1",
                    "type": "GET_PART",
                    "size": 2048,
                    "attempts": 4,
                    "last_exception": "NoKeeperConnection",
                    "postpone_reason": "Keeper session expired",
                    "last_attempt_time": "2026-01-01 00:00:00",
                }
            ], ""
        if template_id == "clickhouse_distribution_queue.v1":
            return [
                {
                    "database": "events",
                    "table": "distributed_events",
                    "address": "clickhouse-0:9000",
                    "errors_count": 3,
                    "data_files": 8,
                    "data_compressed_files": 8,
                    "bytes_to_send": 102400000,
                    "last_exception": "Connection refused",
                    "last_time": "2026-01-01 00:00:00",
                }
            ], ""
        if template_id == "clickhouse_keeper_connection.v1":
            return [
                {
                    "name": "keeper-0",
                    "host": "keeper-0",
                    "port": 2181,
                    "is_connected": 0,
                    "latency_ms": 0,
                    "errors_count": 5,
                    "timeout_count": 2,
                }
            ], ""
        return [], ""

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: object())
    monkeypatch.setattr(clickhouse, "_clickhouse_pod_candidates", lambda *_args, **_kwargs: [altinity, generic])
    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fake_run)

    result = clickhouse.get_clickhouse_replication_report(
        namespace="analytics",
        pod="chi-clickhouse-0-0",
        cluster="default",
        output_mode="human",
    )

    assert "analytics/chi-clickhouse-0-0 (replica-local)" in result
    assert "Runtime membership: cluster=default" in result
    assert "local_replica=2" in result
    assert "configured_replicas=2" in result
    assert "replica label '02' != runtime '2'" not in result
    assert "readonly=yes" in result
    assert "session_expired=yes" in result
    assert "absolute_delay=65s" in result
    assert "queue_size=12" in result
    assert "files=8" in result
    assert "bytes=102400000" in result
    assert "connected=no" in result
    assert "No changes were made" in result


def test_get_clickhouse_replication_report_preserves_source_errors_and_labels_mismatch(monkeypatch) -> None:
    pod = _pod(
        "chi-clickhouse-0-0",
        labels={
            "clickhouse.altinity.com/shard": "02",
            "clickhouse.altinity.com/replica": "02",
            "clickhouse.altinity.com/cluster": "prod",
        },
    )

    def fake_run(_namespace, _pod_name, template_id, _params=None, _container=None):
        if template_id == "clickhouse_topology.v1":
            return [
                {
                    "cluster": "default",
                    "shard_num": 1,
                    "replica_num": 1,
                    "host_name": "clickhouse-0",
                    "port": 9000,
                    "is_local": 1,
                }
            ], ""
        if template_id == "clickhouse_replicas.v1":
            raise ValueError("ClickHouse returned malformed JSONEachRow output.")
        return [], ""

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: object())
    monkeypatch.setattr(clickhouse, "_clickhouse_pod_candidates", lambda *_args, **_kwargs: [pod])
    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fake_run)

    result = clickhouse.get_clickhouse_replication_report("analytics", "chi-clickhouse-0-0", output_mode="json")

    payload = json.loads(result)
    assert payload["tool"] == "get_clickhouse_replication_report"
    assert payload["status"] == "ok"
    assert "ClickHouse returned malformed JSONEachRow output." in payload["data"]["report"]
    assert (
        "Topology mismatch: cluster label 'prod' != runtime 'default'; shard label '02' != runtime 1; "
        "replica label '02' != runtime 1." in payload["data"]["report"]
    )
    assert "system.clusters is the runtime evidence" in payload["data"]["report"]


def test_get_clickhouse_workload_report_reports_concurrency_errors_and_advisory_only(monkeypatch) -> None:
    pod = _pod("clickhouse-0")

    def fake_run(_namespace, _pod_name, template_id, _params=None, _container=None):
        if template_id == "clickhouse_readonly_profile.v1":
            return [{"readonly": "1", "version": "23.8.1.1"}], ""
        if template_id == "clickhouse_metrics.v1":
            return [
                {"metric": "Query", "value": 12},
                {"metric": "Merge", "value": 3},
                {"metric": "PoolTask", "value": 5},
                {"metric": "BackgroundPoolTask", "value": 4},
                {"metric": "HTTPConnection", "value": 8},
                {"metric": "TCPConnection", "value": 6},
                {"metric": "MySQLConnection", "value": 0},
                {"metric": "PostgreSQLConnection", "value": 1},
                {"metric": "InterserverConnection", "value": 2},
            ], ""
        if template_id == "clickhouse_workload_processes.v1":
            return [
                {
                    "query_id": "query-1",
                    "user": "analytics_user",
                    "elapsed": 320.5,
                    "query_kind": "Select",
                    "read_rows": 1000000,
                    "read_bytes": 20401094656,
                    "memory_usage": 2147483648,
                    "client_name": "python-driver; password=hunter2",
                    "http_user_agent": "service-agent",
                    "initial_address": "10.0.0.9",
                    "normalized_query": "SELECT count() FROM events WHERE user_token = ?",
                }
            ], ""
        if template_id == "clickhouse_settings.v1":
            return [
                {"name": "max_concurrent_queries_for_user", "value": 10},
                {"name": "max_threads", "value": 8},
            ], ""
        if template_id == "clickhouse_errors.v1":
            return [
                {
                    "error_code": 241,
                    "name": "MEMORY_LIMIT_EXCEEDED",
                    "value": 5,
                    "last_error_time": "2026-01-01 00:00:00",
                    "last_error_text": "Memory limit exceeded while INSERT INTO t VALUES ('secret')",
                    "remote": 0,
                }
            ], ""
        if template_id == "clickhouse_quotas_usage.v1":
            return [
                {
                    "quota_name": "service-quota",
                    "user_name": "analytics_user",
                    "application_name": "service",
                    "num_queries": 100,
                    "max_queries": 100,
                    "read_bytes": 2048,
                    "max_read_bytes": 4096,
                    "exceptions": 1,
                    "max_exceptions": 5,
                }
            ], ""
        if template_id == "clickhouse_workload_query_log.v1":
            return [
                {
                    "user": "analytics_user",
                    "client_name": "python-driver",
                    "http_user_agent": "service-agent",
                    "initial_address": "10.0.0.9",
                    "normalized_query": "SELECT count() FROM events",
                    "run_count": 20,
                    "total_duration_ms": 45000,
                    "p95_duration_ms": 5000,
                    "read_bytes": 20401094656,
                    "peak_memory_usage": 1073741824,
                    "error_count": 2,
                }
            ], ""
        return [], ""

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: object())
    monkeypatch.setattr(clickhouse, "_clickhouse_pod_candidates", lambda *_args, **_kwargs: [pod])
    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fake_run)

    result = clickhouse.get_clickhouse_workload_report("analytics", "clickhouse-0", minutes=90)

    assert "analytics/clickhouse-0 (server 23.8.1.1)" in result
    assert "Query=12" in result
    assert "HTTPConnection=8" in result
    assert "max_concurrent_queries_for_user=10" in result
    assert "elapsed=320.500s" in result
    assert "memory_usage=2147483648" in result
    assert "user=analytics_user" in result
    assert "initial_address=10.0.0.9" in result
    assert "MEMORY_LIMIT_EXCEEDED" in result
    assert "quota=service-quota" in result
    assert "queries/max=100/100" in result
    assert "Review concurrency limits and client pool sizing" in result
    assert "No changes were made" in result
    assert "hunter2" not in result


def test_clickhouse_report_templates_remain_registry_only_and_readonly() -> None:
    expected = {
        "clickhouse_topology.v1",
        "clickhouse_replicas.v1",
        "clickhouse_replication_queue.v1",
        "clickhouse_distribution_queue.v1",
        "clickhouse_keeper_connection.v1",
        "clickhouse_metrics.v1",
        "clickhouse_workload_processes.v1",
        "clickhouse_settings.v1",
        "clickhouse_errors.v1",
        "clickhouse_quotas_usage.v1",
        "clickhouse_workload_query_log.v1",
        "clickhouse_parts_summary.v1",
        "clickhouse_merges.v1",
        "clickhouse_mutations.v1",
        "clickhouse_detached_parts.v1",
        "clickhouse_part_log.v1",
        "clickhouse_merge_tree_settings.v1",
        "clickhouse_table_metadata.v1",
        "clickhouse_async_metrics.v1",
        "clickhouse_resource_metrics.v1",
        "clickhouse_server_settings.v1",
        "clickhouse_disks.v1",
        "clickhouse_storage_policies.v1",
        "clickhouse_largest_tables.v1",
        "clickhouse_largest_partitions.v1",
        "clickhouse_largest_columns.v1",
    }
    assert expected.issubset(set(clickhouse.CLICKHOUSE_SQL_TEMPLATES))
    for template_id in expected:
        params = {
            name: ("25" if name == "limit" else "2026-01-01 00:00:00")
            for name in clickhouse.get_clickhouse_template(template_id).required_params
        }
        sql, _ = clickhouse.render_clickhouse_template(template_id, params)
        assert sql.lstrip().upper().startswith("SELECT")


def test_get_clickhouse_parts_reports_too_many_parts_mutations_and_advisory_only(monkeypatch) -> None:
    pod = _pod("clickhouse-0")
    pod_list = SimpleNamespace(items=[pod])
    api = SimpleNamespace(
        list_namespaced_pod=lambda **_kwargs: pod_list,
        read_namespaced_pod=lambda **_kwargs: pod,
    )

    def fake_run(_namespace, _pod_name, template_id, params=None, container=None):
        assert params is None or isinstance(params, dict)
        if template_id == "clickhouse_readonly_profile.v1":
            return [{"readonly": "1", "version": "23.8.1.1"}], ""
        if template_id == "clickhouse_table_metadata.v1":
            return [
                {
                    "database": "analytics",
                    "table": "events",
                    "engine": "MergeTree",
                    "engine_full": "MergeTree ORDER BY event_date SETTINGS index_granularity=8192, parts_to_throw_insert=300, parts_to_delay_throw=150",
                    "partition_key": "toYYYYMM(event_date)",
                    "sorting_key": "event_date",
                }
            ], ""
        if template_id == "clickhouse_merge_tree_settings.v1":
            return [
                {"name": "parts_to_delay_throw", "value": "600", "changed": 0},
                {"name": "parts_to_throw_insert", "value": "1200", "changed": 0},
            ], ""
        if template_id == "clickhouse_parts_summary.v1":
            return [
                {
                    "database": "analytics",
                    "table": "events",
                    "partition": "202611",
                    "active_parts": 420,
                    "total_bytes": 1_200_000_000,
                    "total_rows": 100,
                    "avg_part_bytes": 1_048_576,
                    "largest_part_bytes": 2_097_152,
                    "newest_part_time": "2026-11-26 08:00:00",
                }
            ], ""
        if template_id == "clickhouse_merges.v1":
            return [
                {
                    "database": "analytics",
                    "table": "events",
                    "elapsed": 4500,
                    "progress": 0.25,
                    "num_parts": 8,
                    "rows_read": 1000,
                    "bytes_read_uncompressed": 1_000_000,
                    "total_size_bytes_compressed": 500_000,
                    "source_part_names": ["all_1_1", "all_2_2", "all_3_3"],
                    "result_part_name": "all_1_3",
                }
            ], ""
        if template_id == "clickhouse_mutations.v1":
            return [
                {
                    "database": "analytics",
                    "table": "events",
                    "mutation_id": "mutation-1",
                    "command": "ALTER TABLE events DELETE WHERE bad = 1",
                    "create_time": "2026-11-20 08:00:00",
                    "is_done": 0,
                    "parts_to_do": 3,
                    "latest_fail_time": "2026-11-25 08:00:00",
                    "latest_fail_reason": "Mutation was not applied to some parts",
                }
            ], ""
        if template_id == "clickhouse_detached_parts.v1":
            return [
                {
                    "database": "analytics",
                    "table": "events",
                    "partition": "202611",
                    "name": "bad-part",
                    "reason": "broken",
                    "disk_name": "default",
                },
                {
                    "database": "analytics",
                    "table": "events",
                    "partition": "202611",
                    "name": "manual-part",
                    "reason": "manual detach",
                    "disk_name": "default",
                },
            ], ""
        if template_id == "clickhouse_part_log.v1":
            return [], ""
        if template_id == "clickhouse_errors.v1":
            return [], ""
        if template_id == "clickhouse_resource_metrics.v1":
            return [
                {"metric": "BackgroundMergesAndMutationsPoolTask", "value": 2},
                {"metric": "BackgroundPoolTask", "value": 1},
            ], ""
        return [], ""

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fake_run)

    result = clickhouse.get_clickhouse_parts_report(
        "analytics",
        "clickhouse-0",
        database="analytics",
        table="events",
        output_mode="human",
    )

    assert "analytics/clickhouse-0 (server 23.8.1.1)" in result
    assert "Too many parts at throw threshold" in result
    assert "parts_to_throw_insert=300" in result
    assert "Small-part evidence: 1/1 partitions" in result
    assert "Running merge: analytics.events progress=25.0%; elapsed=4500.0s" in result
    assert "Failed mutation" in result
    assert "mutation_id=mutation-1" in result
    assert "parts_to_do=3" in result
    assert "latest_fail_reason=Mutation was not applied to some parts" in result
    assert "Detached parts: analytics.events reason=broken count=1" in result
    assert "Batch inserts or consolidate small parts" in result
    assert "part_log may be disabled, absent, or outside the retained window" in result
    assert "No mutation, merge, detach, DROP, ALTER, SYSTEM command" in result
    assert "No changes were made" in result


def test_get_clickhouse_resource_reports_pressure_pvc_and_handles_missing_columns(monkeypatch) -> None:
    pod = _pod("clickhouse-0")
    pod.spec.containers[0].resources = SimpleNamespace(limits={"memory": "32Gi"})
    pod.spec.containers[0].volume_mounts = [SimpleNamespace(name="data", mount_path="/var/lib/clickhouse")]
    pod_list = SimpleNamespace(items=[pod])
    claim = SimpleNamespace(
        metadata=SimpleNamespace(name="data"),
        spec=SimpleNamespace(resources=SimpleNamespace(requests={"storage": "100Gi"})),
    )
    api = SimpleNamespace(
        list_namespaced_pod=lambda **_kwargs: pod_list,
        read_namespaced_pod=lambda **_kwargs: pod,
        list_namespaced_persistent_volume_claim=lambda **_kwargs: SimpleNamespace(items=[claim]),
    )

    def fake_run(_namespace, _pod_name, template_id, params=None, container=None):
        if template_id == "clickhouse_readonly_profile.v1":
            return [{"readonly": "1", "version": "22.3.6.29"}], ""
        if template_id == "clickhouse_async_metrics.v1":
            return [
                {"metric": "MemoryTrackingForAllocations", "value": 16_106_127_360},
                {"metric": "MemoryResident", "value": 15_033_165_524},
                {"metric": "OSSMemoryAvailable", "value": 2_147_483_648},
                {"metric": "OSSMemoryTotal", "value": 34_359_738_368},
                {"metric": "MarkCacheBytes", "value": 1_073_741_824},
            ], ""
        if template_id == "clickhouse_resource_metrics.v1":
            return [{"metric": "QueryCacheHits", "value": 80}, {"metric": "QueryCacheMisses", "value": 20}], ""
        if template_id == "clickhouse_server_settings.v1":
            return [{"name": "max_server_memory_usage", "value": "16GiB"}], ""
        if template_id == "clickhouse_disks.v1":
            return [
                {
                    "name": "default",
                    "path": "/var/lib/clickhouse/data",
                    "type": "local",
                    "free_space": 10_737_418_240,
                    "total_space": 107_374_182_400,
                    "keep_free_space": 10_737_418_240,
                    "is_read_only": 0,
                }
            ], ""
        if template_id == "clickhouse_storage_policies.v1":
            return [
                {
                    "policy_name": "hot_cold",
                    "volume_name": "hot",
                    "volume_index": 0,
                    "disk_name": "default",
                    "path": "/var/lib/clickhouse/data",
                    "move_factor": 0.1,
                    "prefer_not_to_merge": 0,
                    "perform_ttl_move_on_insert": 1,
                }
            ], ""
        if template_id == "clickhouse_largest_tables.v1":
            return [
                {
                    "database": "analytics",
                    "table": "events",
                    "active_parts": 12,
                    "total_bytes": 214_748_364_800,
                    "total_rows": 1_000_000,
                    "avg_part_bytes": 17_895_697_066,
                }
            ], ""
        if template_id == "clickhouse_largest_partitions.v1":
            return [
                {
                    "database": "analytics",
                    "table": "events",
                    "partition": "202611",
                    "active_parts": 3,
                    "total_bytes": 100_663_296,
                    "total_rows": 500_000,
                }
            ], ""
        if template_id == "clickhouse_largest_columns.v1":
            raise RuntimeError("Unknown table system.parts_columns")
        if template_id == "clickhouse_table_metadata.v1":
            return [
                {
                    "database": "analytics",
                    "table": "events",
                    "engine": "MergeTree",
                    "engine_full": "MergeTree ORDER BY event_date",
                    "partition_key": "toYYYYMM(event_date)",
                    "sorting_key": "event_date",
                }
            ], ""
        return [], ""

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fake_run)

    result = clickhouse.get_clickhouse_resource_report(
        "analytics",
        "clickhouse-0",
        top_n=10,
        output_mode="human",
    )

    assert "analytics/clickhouse-0 (server 22.3.6.29)" in result
    assert "MemoryTrackingForAllocations" not in result
    assert "MemoryTracking=16106127360" in result
    assert "Memory usage ratio=93.8%" in result
    assert "server limit=16.0GiB and Kubernetes limit=32.0GiB" in result
    assert "Query cache hits/misses=80.0/20.0; hit_rate=80.0%" in result
    assert "Cache MarkCacheBytes=1.0GiB" in result
    assert "Disk default: free=10.0GiB/100.0GiB=90.0%" in result
    assert "Review disk pressure and retention" in result
    assert "at or below keep_free_space" in result
    assert "policy=hot_cold" in result
    assert "100.0GiB (100Gi on PVC data in analytics/clickhouse-0)" in result
    assert "analytics.events engine=MergeTree" in result
    assert "partition=202611" in result
    assert "system.parts_columns is unavailable" in result
    assert "Review retention or tiering" in result
    assert "no TTL expression" in result
    assert "never scans application data" not in result
    assert "No configuration, disk, part, TTL, codec, cache, or Kubernetes" in result
    assert "No changes were made" in result


def test_get_clickhouse_parts_json_output_is_stable_advisory_envelope(monkeypatch) -> None:
    pod = _pod("clickhouse-json")
    pod_list = SimpleNamespace(items=[pod])
    api = SimpleNamespace(
        list_namespaced_pod=lambda **_kwargs: pod_list,
        read_namespaced_pod=lambda **_kwargs: pod,
    )

    def fake_run(_namespace, _pod_name, template_id, params=None, container=None):
        if template_id == "clickhouse_readonly_profile.v1":
            return [{"readonly": "1", "version": "23.8.1.1"}], ""
        if template_id == "clickhouse_parts_summary.v1":
            return [], ""
        return [], ""

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fake_run)

    payload = json.loads(
        clickhouse.get_clickhouse_parts_report("analytics", "clickhouse-json", output_mode="json")
    )

    assert payload["schema_version"] == clickhouse.CLICKHOUSE_STRUCTURED_SCHEMA_VERSION
    assert payload["tool"] == "get_clickhouse_parts_report"
    assert payload["status"] == "ok"
    assert payload["metadata"]["database"] is None
    assert payload["data"]["report"].endswith("No changes were made; this analysis is advisory only.")

@pytest.mark.parametrize(
    ("mode", "expected_prefix"),
    [
        ("indexes", "EXPLAIN indexes = 1 SELECT"),
        ("actions", "EXPLAIN actions = 1 SELECT"),
        ("pipeline", "EXPLAIN PIPELINE SELECT"),
        ("estimate", "EXPLAIN ESTIMATE SELECT"),
    ],
)
def test_get_clickhouse_query_plan_supports_all_non_executing_modes(
    monkeypatch, mode, expected_prefix
) -> None:
    captured: dict[str, str] = {}

    def fake_statement(namespace, pod, sql, container=None, operation=""):
        captured.update(namespace=namespace, pod=pod, sql=sql, operation=operation)
        return "plan evidence"

    monkeypatch.setattr(clickhouse, "_clickhouse_pod_candidates", lambda *_args, **_kwargs: [_pod("clickhouse-0")])
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: SimpleNamespace(api_client=object()))
    monkeypatch.setattr(clickhouse, "_run_clickhouse_statement", fake_statement)

    result = clickhouse.get_clickhouse_query_plan(
        "analytics",
        "clickhouse-0",
        "SELECT count() FROM events",
        "analytics",
        mode,
    )

    assert captured["sql"].startswith(expected_prefix)
    assert captured["operation"] == "clickhouse_query_plan"
    assert "ClickHouse query plan (advisory only)" in result
    assert "was not executed" in result
    assert "plan evidence" in result
    assert "No changes were made" in result


def test_get_clickhouse_query_plan_rejects_invalid_and_unsafe_sql(monkeypatch) -> None:
    def fail_statement(*_args, **_kwargs):
        raise AssertionError("unsafe SQL must not reach the ClickHouse client")

    monkeypatch.setattr(clickhouse, "_run_clickhouse_statement", fail_statement)
    for mode, sql in (
        ("execute", "SELECT 1"),
        ("indexes", "INSERT INTO events VALUES (1)"),
        ("indexes", "SELECT 1; SELECT 2"),
        ("indexes", "SELECT 1 SETTINGS max_threads=1"),
    ):
        result = clickhouse.get_clickhouse_query_plan(
            "analytics", "clickhouse-0", sql, mode=mode
        )
        assert result.startswith("Error preparing ClickHouse query plan")


def test_collect_clickhouse_table_evidence_parses_catalog_metadata(monkeypatch) -> None:
    def fake_run(_namespace, _pod, template_id, params=None, container=None):
        if template_id == "clickhouse_table.v1":
            return [
                {
                    "database": "analytics",
                    "name": "events",
                    "engine": "MergeTree",
                    "engine_full": "MergeTree ORDER BY event_time",
                    "sorting_key": "event_time",
                    "primary_key": "event_time",
                    "partition_key": "toYYYYMM(event_time)",
                    "sampling_key": "",
                    "total_rows": 100,
                    "total_bytes": 2048,
                }
            ], ""
        if template_id == "clickhouse_columns.v1":
            return [
                {
                    "name": "event_time",
                    "type": "DateTime",
                    "compression_codec": "ZSTD(1)",
                    "default_kind": "",
                    "default_expression": "",
                    "is_in_partition_key": 1,
                    "is_in_sorting_key": 1,
                    "is_in_primary_key": 1,
                    "is_in_sampling_key": 0,
                }
            ], ""
        if template_id == "clickhouse_data_skipping_indices.v1":
            return [{"name": "idx_user", "type": "bloom_filter", "expr": "user_id", "granularity": "3", "is_built": 1}], ""
        return [{"name": "events_daily", "columns": "event_time", "sorting_key": "event_time"}], ""

    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fake_run)
    evidence = clickhouse._collect_clickhouse_table_evidence(
        "analytics", "clickhouse-0", [("analytics", "events")]
    )

    assert evidence.tables[("analytics", "events")].sorting_key == "event_time"
    assert evidence.columns[("analytics", "events")][0].in_sorting_key is True
    assert evidence.indices[("analytics", "events")][0].type == "bloom_filter"
    assert evidence.projections[("analytics", "events")][0].name == "events_daily"
    assert evidence.errors == []


def _clickhouse_table_evidence() -> clickhouse.ClickHouseTableEvidence:
    evidence = clickhouse.ClickHouseTableEvidence(namespace="analytics", pod="clickhouse-0")
    table = clickhouse.ClickHouseTableMetadata(
        database="analytics",
        name="events",
        engine="MergeTree",
        sorting_key="event_time",
        partition_key="toYYYYMM(event_time)",
        total_rows=1_000_000,
        total_bytes=12 * 1024 * 1024 * 1024,
    )
    evidence.tables[("analytics", "events")] = table
    evidence.columns[("analytics", "events")] = [
        clickhouse.ClickHouseColumnMetadata(name="user_id", type="String"),
        clickhouse.ClickHouseColumnMetadata(name="amount", type="Float64", codec="ZSTD(1)"),
    ]
    evidence.indices[("analytics", "events")] = [
        clickhouse.ClickHouseSkippingIndexMetadata(
            name="idx_time", type="minmax", expr="event_time", is_built=True
        )
    ]
    evidence.projections[("analytics", "events")] = []
    return evidence


def test_clickhouse_table_advice_covers_requested_design_topics() -> None:
    sql = (
        "SELECT user_id, count() FROM analytics.events FINAL "
        "WHERE user_id IN (SELECT user_id FROM customers) GROUP BY user_id "
        "JOIN customers ON customers.user_id = events.user_id"
    )
    aggregate = clickhouse.ClickHouseQueryLogAggregate(
        namespace="analytics",
        pod="clickhouse-0",
        query_hash="42",
        run_count=3,
        p95_duration_ms=3000,
        read_rows=1_000_000,
        read_bytes=2048,
        peak_memory_usage=2048,
        exceptions={"Memory limit exceeded"},
    )

    advice = clickhouse._clickhouse_table_advice(
        _clickhouse_table_evidence(), sql, "Parts: 20\nMarks: 30", aggregate
    )
    text = "\n".join(advice)

    assert "Sorting key design" in text
    assert "Partition pruning" in text
    assert "Skipping-index design" in text
    assert "Projection design" in text
    assert "FINAL use" in text
    assert "Join order/memory" in text
    assert "Large IN subqueries" in text
    assert "LowCardinality candidate" in text
    assert "Codec/compression" in text
    assert "query_hash=42" in text
    assert "marks=30" in text
    assert "CREATE INDEX" not in text
    assert "ALTER TABLE" not in text
    assert "Cost/risk" in text


def test_recommend_clickhouse_table_design_reports_metadata_and_privilege_gap(monkeypatch) -> None:
    evidence = _clickhouse_table_evidence()
    explain_calls: list[str] = []

    def fake_statement(_namespace, _pod, sql, container=None, operation=""):
        explain_calls.append(sql)
        raise RuntimeError("SELECT privilege is not granted to user")

    monkeypatch.setattr(clickhouse, "_clickhouse_pod_candidates", lambda *_args, **_kwargs: [_pod("clickhouse-0")])
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: SimpleNamespace(api_client=object()))
    monkeypatch.setattr(clickhouse, "_collect_clickhouse_table_evidence", lambda *_args, **_kwargs: evidence)
    monkeypatch.setattr(clickhouse, "_run_clickhouse_statement", fake_statement)

    result = clickhouse.recommend_clickhouse_table_design(
        "analytics",
        "clickhouse-0",
        sql="SELECT user_id FROM analytics.events WHERE user_id = 'x'",
        output_mode="json",
    )

    payload = json.loads(result)
    assert payload["status"] == "ok"
    assert payload["metadata"]["missing_select_grant"] is True
    assert explain_calls == ["EXPLAIN indexes = 1 SELECT user_id FROM analytics.events WHERE user_id = 'x'"]
    report = payload["data"]["report"]
    assert "SELECT privilege" in report
    assert "table-metadata-only" in report
    assert "Skipping-index design" in report
    assert "No changes were made" in report


def test_recommend_clickhouse_table_design_resolves_target_from_query_hash(monkeypatch) -> None:
    captured: dict[str, object] = {}
    aggregate = clickhouse.ClickHouseQueryLogAggregate(
        namespace="analytics",
        pod="clickhouse-0",
        query_hash="42",
        run_count=2,
        p95_duration_ms=2500,
        read_rows=1000,
        read_bytes=2048,
        normalized_query="SELECT user_id FROM analytics.events WHERE user_id = ?",
    )

    def fake_hash_aggregate(_namespace, _pod, query_hash, container=None):
        assert query_hash == "42"
        return aggregate

    def fake_collect(_namespace, _pod, targets, container=None):
        captured["targets"] = targets
        return _clickhouse_table_evidence()

    monkeypatch.setattr(clickhouse, "_clickhouse_pod_candidates", lambda *_args, **_kwargs: [_pod("clickhouse-0")])
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: SimpleNamespace(api_client=object()))
    monkeypatch.setattr(clickhouse, "_clickhouse_query_hash_aggregate", fake_hash_aggregate)
    monkeypatch.setattr(clickhouse, "_collect_clickhouse_table_evidence", fake_collect)

    result = clickhouse.recommend_clickhouse_table_design(
        "analytics",
        "clickhouse-0",
        normalized_query_hash="42",
    )

    assert captured["targets"] == [("analytics", "events")]
    assert "query_hash=42" in result
    assert "runs=2" in result
    assert "Application rows were not read" in result
    assert "No changes were made" in result


def test_get_clickhouse_query_plan_reports_missing_select_grant(monkeypatch) -> None:
    def failing_statement(*_args, **_kwargs):
        raise RuntimeError("SELECT privilege(s) are not granted")

    monkeypatch.setattr(clickhouse, "_clickhouse_pod_candidates", lambda *_args, **_kwargs: [_pod("clickhouse-0")])
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: SimpleNamespace(api_client=object()))
    monkeypatch.setattr(clickhouse, "_run_clickhouse_statement", failing_statement)

    result = clickhouse.get_clickhouse_query_plan(
        "analytics", "clickhouse-0", "SELECT 1", mode="indexes"
    )

    assert result.startswith("ClickHouse EXPLAIN failed")
    assert "SELECT privilege is missing" in result


def test_recommend_clickhouse_table_design_rejects_unsafe_sql_before_catalog_or_explain(monkeypatch) -> None:
    def fail_any(*_args, **_kwargs):
        raise AssertionError("unsafe SQL must not reach ClickHouse")

    monkeypatch.setattr(clickhouse, "_clickhouse_pod_candidates", fail_any)
    monkeypatch.setattr(clickhouse, "_collect_clickhouse_table_evidence", fail_any)
    monkeypatch.setattr(clickhouse, "_run_clickhouse_statement", fail_any)

    result = clickhouse.recommend_clickhouse_table_design(
        "analytics",
        "clickhouse-0",
        sql="SELECT 1 FORMAT JSON",
    )

    assert result.startswith("Error preparing ClickHouse table-design analysis")
    assert "FORMAT" in result

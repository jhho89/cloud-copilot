from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
import pytest
import json


REPO_ROOT = Path(__file__).resolve().parents[1]
K8S_AGENTS = REPO_ROOT / "services" / "k8s-copilot" / "src" / "agents"
if str(K8S_AGENTS) not in sys.path:
    sys.path.insert(0, str(K8S_AGENTS))

from dba_agent import tools  # noqa: E402


SLOW_LOG_TIMESTAMP = datetime.now(timezone.utc) - timedelta(seconds=60)
SLOW_LOG = f"""# Time: {SLOW_LOG_TIMESTAMP.isoformat()}
# User@Host: app_user[app_user] @ [10.0.0.5]
# Query_time: 2.500000 Lock_time: 0.010000 Rows_sent: 10 Rows_examined: 100000
SET timestamp=1732005602;
SELECT o.id
FROM orders o
JOIN users u ON u.id = o.user_id
WHERE u.email = 'alice@example.com';
"""


def _database_pod(name: str = "mysql-0", namespace: str = "db") -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace=namespace,
            labels={"app.kubernetes.io/name": "mysql"},
        ),
        spec=SimpleNamespace(
            containers=[SimpleNamespace(name="database", image="mysql:8.0")]
        ),
        status=SimpleNamespace(phase="Running"),
    )


def test_parse_slow_query_log_extracts_query_and_metrics() -> None:
    queries = tools._parse_slow_query_log(SLOW_LOG, "db", "mysql-0")

    assert len(queries) == 1
    query = queries[0]
    assert query.namespace == "db"
    assert query.pod == "mysql-0"
    assert query.query_time == 2.5
    assert query.lock_time == 0.01
    assert query.rows_sent == 10
    assert query.rows_examined == 100000
    assert query.sql == (
        "SELECT o.id\n"
        "FROM orders o\n"
        "JOIN users u ON u.id = o.user_id\n"
        "WHERE u.email = 'alice@example.com';"
    )
    assert query.logged_at is not None
    assert query.logged_at == SLOW_LOG_TIMESTAMP


def test_find_database_pods_matches_image(monkeypatch) -> None:
    pod = SimpleNamespace(
        metadata=SimpleNamespace(name="orders-db-0", namespace="orders", labels={"app": "orders"}),
        spec=SimpleNamespace(containers=[SimpleNamespace(name="database", image="mysql:8.0")]),
        status=SimpleNamespace(phase="Running"),
    )
    api = SimpleNamespace(list_pod_for_all_namespaces=lambda label_selector=None: SimpleNamespace(items=[pod]))
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)

    result = tools.find_database_pods("mysql")

    assert result == "orders/orders-db-0 (Status: Running)"


def test_get_mysql_slow_queries_reads_streamed_pod_logs(monkeypatch) -> None:
    pod = _database_pod()
    calls = []

    class StreamedApi:
        def list_pod_for_all_namespaces(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(items=[pod])

        def read_namespaced_pod_log(self, **kwargs):
            calls.append(kwargs)
            return SLOW_LOG

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: StreamedApi())

    result = tools.get_mysql_slow_queries(minutes=5, limit=5)

    assert "Query time: 2.500s" in result
    assert "SELECT o.id" in result
    assert calls[1]["since_seconds"] == 300
    assert calls[1]["tail_lines"] >= 1000
    assert "container" not in calls[1]


def test_get_mysql_slow_queries_returns_message_for_empty_streamed_log(monkeypatch) -> None:
    pod = _database_pod()

    class EmptyLogApi:
        def list_pod_for_all_namespaces(self, **kwargs):
            return SimpleNamespace(items=[pod])

        def read_namespaced_pod_log(self, **kwargs):
            return ""

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: EmptyLogApi())

    result = tools.get_mysql_slow_queries()

    assert result.startswith("No MySQL slow queries over 0.5s found in the last 5 minutes.")


def test_get_mysql_slow_queries_reports_streamed_log_access_errors(monkeypatch) -> None:
    pod = _database_pod()

    class FailingLogApi:
        def list_pod_for_all_namespaces(self, **kwargs):
            return SimpleNamespace(items=[pod])

        def read_namespaced_pod_log(self, **kwargs):
            raise RuntimeError("pod log access denied")

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: FailingLogApi())

    result = tools.get_mysql_slow_queries()

    assert "No MySQL slow queries" in result
    assert "Pod log errors:" in result
    assert "db/mysql-0: pod log access denied" in result


def test_validate_slow_log_path_rejects_ambiguous_paths() -> None:
    assert tools._validate_slow_log_path("/var/log/mysql/slow.log") == "/var/log/mysql/slow.log"
    for path in [
        "",
        "var/log/mysql/slow.log",
        "/var/log/../etc/passwd",
        "/var/./log/mysql/slow.log",
        "/var/log//mysql/slow.log",
        "/var/log/mysql/",
        "/var/log/mysql\\slow.log",
        "/var/log/mysql/slow.log\x00",
        "/var/log/mysql/slow.log\n",
    ]:
        with pytest.raises(tools.SlowLogFileConfigError):
            tools._validate_slow_log_path(path)


def test_resolve_slow_log_path_requires_operator_configuration(monkeypatch) -> None:
    monkeypatch.delenv("DBA_ALLOWED_SLOW_LOG_PATHS", raising=False)
    monkeypatch.delenv("DBA_SLOW_LOG_FILE_PATH", raising=False)
    with pytest.raises(tools.SlowLogFileConfigError, match="disabled"):
        tools._resolve_slow_log_path(None)

    monkeypatch.setenv("DBA_ALLOWED_SLOW_LOG_PATHS", "/logs/mysql/slow.log;/logs/mariadb/slow.log")
    with pytest.raises(tools.SlowLogFileConfigError, match="not allowlisted"):
        tools._resolve_slow_log_path("/var/log/mysql/slow.log")
    assert tools._resolve_slow_log_path("/logs/mysql/slow.log") == "/logs/mysql/slow.log"

    monkeypatch.delenv("DBA_ALLOWED_SLOW_LOG_PATHS", raising=False)
    monkeypatch.setenv("DBA_SLOW_LOG_FILE_PATH", "/logs/mysql/slow.log")
    assert tools._resolve_slow_log_path(None) == "/logs/mysql/slow.log"


def test_read_slow_log_file_uses_shell_free_tail_exec(monkeypatch) -> None:
    captured = {}

    class ExecApi:
        def connect_get_namespaced_pod_exec(self, **kwargs):
            captured.update(kwargs)
            return SLOW_LOG

    def fake_stream(connect_method, **kwargs):
        captured.update(kwargs)
        return SLOW_LOG

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: ExecApi())
    monkeypatch.setattr(tools.stream, "stream", fake_stream)

    output = tools._read_slow_log_file(
        "db",
        "mysql-0",
        "/var/log/mysql/slow.log",
        "database",
        100,
    )

    assert output == SLOW_LOG
    assert captured["command"] == [
        "/usr/bin/tail",
        "--lines=100",
        "--",
        "/var/log/mysql/slow.log",
    ]
    assert captured["container"] == "database"
    assert captured["stdin"] is False
    assert captured["tty"] is False
    assert captured["_preload_content"] is True


def test_pod_exec_audit_omits_sql_and_credentials(monkeypatch, caplog) -> None:
    captured = {}

    class ExecApi:
        def connect_get_namespaced_pod_exec(self, **kwargs):
            captured.update(kwargs)
            return "plan"

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: ExecApi())
    monkeypatch.setattr(tools.stream, "stream", lambda connect_method, **kwargs: "plan")
    caplog.set_level(logging.INFO, logger=tools._AUDIT_LOGGER.name)

    command = tools._mysql_shell_command("SELECT email FROM users", "orders", user="root", password="secret")
    tools._run_mysql_command("db", "mysql-0", command, "database")

    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert '"event":"dba.pod_exec"' in message
    assert '"operation":"mysql_readonly_query"' in message
    assert "SELECT email" not in message
    assert "secret" not in message
    assert "MYSQL_PASSWORD" not in message


def test_get_mysql_slow_queries_reads_allowlisted_file(monkeypatch) -> None:
    pod = _database_pod()
    captured = {}

    class DiscoveryApi:
        def list_pod_for_all_namespaces(self, **kwargs):
            return SimpleNamespace(items=[pod])

        def connect_get_namespaced_pod_exec(self, **kwargs):
            return SLOW_LOG

    def fake_exec(connect_method, **kwargs):
        captured.update(kwargs)
        return SLOW_LOG

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools.stream, "stream", fake_exec)
    monkeypatch.setenv("DBA_ALLOWED_SLOW_LOG_PATHS", "/var/log/mysql/slow.log")

    result = tools.get_mysql_slow_queries(
        log_source="file",
        file_path="/var/log/mysql/slow.log",
        container="database",
    )

    assert "Query time: 2.500s" in result
    assert captured["command"] == [
        "/usr/bin/tail",
        "--lines=1000",
        "--",
        "/var/log/mysql/slow.log",
    ]
    assert captured["container"] == "database"


def test_get_mysql_slow_queries_returns_message_for_empty_file_log(monkeypatch) -> None:
    pod = _database_pod()

    class DiscoveryApi:
        def list_pod_for_all_namespaces(self, **kwargs):
            return SimpleNamespace(items=[pod])

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools.stream, "stream", lambda api, **kwargs: "")
    monkeypatch.setenv("DBA_SLOW_LOG_FILE_PATH", "/var/log/mysql/slow.log")

    result = tools.get_mysql_slow_queries(log_source="file")

    assert result.startswith("No MySQL slow queries over 0.5s found in the last 5 minutes.")


def test_get_mysql_slow_queries_reports_file_access_errors(monkeypatch) -> None:
    pod = _database_pod()

    class DiscoveryApi:
        def list_pod_for_all_namespaces(self, **kwargs):
            return SimpleNamespace(items=[pod])

    def failing_exec(api, **kwargs):
        raise RuntimeError("container not found")

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools.stream, "stream", failing_exec)
    monkeypatch.setenv("DBA_SLOW_LOG_FILE_PATH", "/var/log/mysql/missing.log")

    result = tools.get_mysql_slow_queries(log_source="file")

    assert "No MySQL slow queries" in result
    assert "Pod log errors:" in result
    assert "Unable to read slow-log file /var/log/mysql/missing.log from db/mysql-0" in result


def test_validate_mysql_read_only_query_allows_select_and_rejects_writes() -> None:
    assert tools._validate_mysql_read_only_query(" SELECT 1;") == "SELECT 1"
    assert tools._validate_mysql_read_only_query("WITH x AS (SELECT 1) SELECT * FROM x")
    assert tools._validate_mysql_read_only_query("UPDATE users SET id = 1") == ""
    assert tools._validate_mysql_read_only_query("CREATE INDEX idx ON users(id)") == ""
    assert tools._validate_mysql_read_only_query("SELECT 1; DELETE FROM users") == ""


def test_mysql_shell_command_reads_pod_environment_and_quotes_sql() -> None:
    command = tools._mysql_shell_command("SELECT 1 FROM users", "orders")

    assert "export MYSQL_PWD=\"${MYSQL_PASSWORD:-$MYSQL_ROOT_PASSWORD}\"" in command
    assert "mysql --host=localhost --port=3306" in command
    assert "--user=\"${MYSQL_USER:-root}\"" in command
    assert "--database=orders" in command
    assert "--execute='SELECT 1 FROM users'" in command


def test_recommend_mysql_indexes_from_plan_flags_full_scan() -> None:
    plan = {
        "query_block": {
            "table": {
                "table_name": "orders",
                "access_type": "ALL",
                "key": None,
                "possible_keys": [],
                "rows": 100000,
                "Extra": "Using where; Using filesort",
            }
        }
    }
    recommendations = tools._recommend_mysql_indexes_from_plan(
        json.dumps(plan),
        "SELECT * FROM orders WHERE customer_id = 1 ORDER BY created_at",
    )

    assert "**orders**" in recommendations
    assert "full table scan" in recommendations
    assert "estimated 100000 rows" in recommendations
    assert "filesort" in recommendations
    assert "do not apply without validation" in recommendations


def test_parse_show_index_output_groups_columns() -> None:
    output = "Table\tNon_unique\tKey_name\tSeq_in_index\tColumn_name\norders\t0\tPRIMARY\t1\tid\norders\t1\tidx_customer\t1\tcustomer_id\norders\t1\tidx_customer\t2\tcreated_at"
    indexes = tools._format_existing_indexes(tools._parse_show_index_output(output))

    assert indexes == ["PRIMARY(id)", "idx_customer(customer_id, created_at)"]


def _redis_pod(
    name: str = "cache-1",
    namespace: str = "cache",
    image: str = "valkey/valkey:8.0",
    labels: dict[str, str] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace=namespace,
            labels=labels or {"app.kubernetes.io/name": "valkey"},
            owner_references=[SimpleNamespace(kind="StatefulSet", name="cache")],
        ),
        spec=SimpleNamespace(containers=[SimpleNamespace(name="redis", image=image)]),
        status=SimpleNamespace(phase="Running"),
    )


def test_find_database_pods_supports_valkey_with_topology_hints(monkeypatch) -> None:
    pod = _redis_pod(
        name="cache-2",
        labels={
            "app.kubernetes.io/name": "valkey",
            "app.kubernetes.io/component": "replica",
        },
    )
    api = SimpleNamespace(
        list_pod_for_all_namespaces=lambda label_selector=None: SimpleNamespace(items=[pod])
    )
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)

    result = tools.find_database_pods("valkey")

    assert result == (
        "cache/cache-2 (Status: Running; statefulset=cache, ordinal=2, role=replica)"
    )


def test_detect_redis_cli_prefers_valkey(monkeypatch) -> None:
    calls = []

    def fake_exec(namespace, pod, command, container, operation, timeout_seconds, target_path=None):
        calls.append(command)
        if command[0] == "valkey-cli":
            return "valkey-cli 8.0.0"
        return "redis-cli 7.2.0"

    monkeypatch.setattr(tools, "_pod_exec", fake_exec)

    assert tools._detect_redis_cli("cache", "cache-0") == "valkey-cli"
    assert calls == [["valkey-cli", "--version"]]


def test_detect_redis_cli_fails_clearly_when_both_are_missing(monkeypatch) -> None:
    def fake_exec(namespace, pod, command, container, operation, timeout_seconds, target_path=None):
        return (
            'OCI runtime exec failed: exec: "redis-cli": executable file not '
            "found in $PATH: unknown"
        )

    monkeypatch.setattr(tools, "_pod_exec", fake_exec)

    with pytest.raises(
        tools.RedisCliMissingError,
        match="Neither valkey-cli nor redis-cli is available",
    ):
        tools._detect_redis_cli("cache", "cache-0")


def test_validate_redis_command_blocks_destructive_and_maintenance_commands() -> None:
    for command in [
        "KEYS *",
        "DEBUG SLEEP 0",
        "CONFIG SET slowlog-max-len 1",
        "LATENCY RESET",
        "SLOWLOG RESET",
        "CLIENT KILL id 1",
        "MONITOR",
        "SAVE",
        "FLUSHDB",
        "FLUSHALL",
    ]:
        with pytest.raises(Exception):
            tools._validate_redis_command(command)

    assert tools._validate_redis_command(["SLOWLOG", "GET", "25"]) == [
        "SLOWLOG",
        "GET",
        "25",
    ]
    assert tools._validate_redis_command("SLOWLOG LEN") == ["SLOWLOG", "LEN"]
    assert tools._validate_redis_command(
        ["CONFIG", "GET", "slowlog-max-len"]
    ) == ["CONFIG", "GET", "slowlog-max-len"]


def test_redis_cli_argv_uses_container_auth_env_only(monkeypatch) -> None:
    monkeypatch.setenv("VALKEYCLI_AUTH", "super-secret")
    monkeypatch.setenv("REDISCLI_AUTH", "super-secret")

    argv = tools._redis_cli_argv(
        "valkey-cli",
        ["CONFIG", "GET", "slowlog-max-len"],
        6380,
    )

    assert argv == [
        "valkey-cli",
        "--json",
        "--port",
        "6380",
        "CONFIG",
        "GET",
        "slowlog-max-len",
    ]
    assert "super-secret" not in argv
    assert "--pass" not in argv
    assert "-a" not in argv


REDIS6_RESP = (
    b"*1\r\n*4\r\n:1\r\n:1700000000\r\n:1500\r\n"
    b"*2\r\n$3\r\nGET\r\n$7\r\nkey-six\r\n"
)
REDIS7_RESP = (
    b"*1\r\n*6\r\n:2\r\n:1700000001\r\n:2500\r\n"
    b"*2\r\n$3\r\nSET\r\n$9\r\nkey-seven\r\n"
    b"$14\r\n127.0.0.1:6379\r\n$8\r\nworker-7\r\n"
)
VALKEY8_RESP = (
    b"*1\r\n*6\r\n:3\r\n:1700000002\r\n:3750\r\n"
    b"*2\r\n$4\r\nSCAN\r\n$1\r\n0\r\n"
    b"*4\r\n$4\r\naddr\r\n$14\r\n127.0.0.1:6379\r\n$2\r\nfd\r\n$4\r\nname\r\n$8\r\nworker-8\r\n"
)


def test_parse_slowlog_entries_supports_redis_6_7_and_valkey_8_resp() -> None:
    redis6 = tools._parse_slowlog_entry(
        tools._parse_slowlog_entries(REDIS6_RESP.decode())[0],
        {"namespace": "cache", "pod": "redis-0"},
    )
    redis7 = tools._parse_slowlog_entry(
        tools._parse_slowlog_entries(REDIS7_RESP.decode())[0],
        {"namespace": "cache", "pod": "redis-1"},
    )
    valkey8 = tools._parse_slowlog_entry(
        tools._parse_slowlog_entries(VALKEY8_RESP.decode())[0],
        {"namespace": "cache", "pod": "valkey-0"},
    )

    assert redis6 and redis6.command == "GET key-six" and redis6.duration_ms == 1.5
    assert redis7 and redis7.command == "SET key-seven" and redis7.client == "127.0.0.1:6379"
    assert redis7 and redis7.client_name == "worker-7"
    assert valkey8 and valkey8.command == "SCAN 0" and valkey8.duration_ms == 3.75
    assert valkey8 and "127.0.0.1:6379" in valkey8.client_info


def test_get_redis_slowlog_shapes_report_and_filters_duration(monkeypatch) -> None:
    pod = _redis_pod(
        name="cache-1",
        labels={
            "app.kubernetes.io/name": "valkey",
            "app.kubernetes.io/component": "replica",
        },
    )
    commands = []

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def fake_cli(*args, **kwargs):
        return "valkey-cli"

    def fake_run_redis_command(namespace, pod_name, cli, redis_command, container=None, port=None):
        commands.append(list(redis_command))
        if redis_command[1] == "GET":
            return (
                '[["7", 1700000002, 1500, ["GET", "cache:key"], '
                '"127.0.0.1:6379", "worker"]]'
            )
        if redis_command[1] == "LEN":
            return "2"
        return '["slowlog-max-len", "2"]'

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", fake_cli)
    monkeypatch.setattr(tools, "_run_redis_command", fake_run_redis_command)

    result = tools.get_redis_slowlog("cache", "cache-1", count=25, min_duration_ms=1)

    assert "### 1. cache/cache-1 (CLI: valkey-cli)" in result
    assert "role=replica; statefulset=cache, ordinal=1" in result
    assert "Duration: 1.500ms" in result
    assert "`GET cache:key`" in result
    assert "No changes were made" in result
    assert commands[:3] == [
        ["SLOWLOG", "GET", "25"],
        ["SLOWLOG", "LEN"],
        ["CONFIG", "GET", "slowlog-max-len"],
    ]


def test_get_redis_slowlog_reports_saturation_and_no_changes(monkeypatch) -> None:
    pod = _redis_pod(name="cache-0")

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def fake_run_redis_command(namespace, pod_name, cli, redis_command, container=None, port=None):
        if redis_command[0] == "SLOWLOG" and redis_command[1] == "GET":
            return "[]"
        if redis_command[0] == "SLOWLOG" and redis_command[1] == "LEN":
            return "128"
        return '["slowlog-max-len", "128"]'

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "valkey-cli")
    monkeypatch.setattr(tools, "_run_redis_command", fake_run_redis_command)

    result = tools.get_redis_slowlog("cache", "cache-0", count=25)

    assert "Slow-log saturation detected:" in result
    assert "cache/cache-0" in result
    assert "No changes were made" in result


def test_pod_exec_captures_timeout_stderr_and_audit_only(monkeypatch, caplog) -> None:
    captured = {}

    class ExecApi:
        def connect_get_namespaced_pod_exec(self, **kwargs):
            captured.update(kwargs)
            return "stdout output"

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: ExecApi())
    monkeypatch.setattr(
        tools.stream,
        "stream",
        lambda connect_method, **kwargs: captured.update(kwargs) or "stdout output",
    )
    caplog.set_level(logging.INFO, logger=tools._AUDIT_LOGGER.name)

    output = tools._pod_exec(
        "cache",
        "cache-0",
        ["valkey-cli", "--json", "SLOWLOG", "LEN"],
        "redis",
        "redis_readonly_command",
        timeout_seconds=17,
    )

    assert output == "stdout output"
    assert captured["command"] == ["valkey-cli", "--json", "SLOWLOG", "LEN"]
    assert captured["stderr"] is True
    assert captured["stdin"] is False
    assert captured["tty"] is False
    assert captured["_preload_content"] is True
    assert captured["_request_timeout"] == (10, 17)
    message = caplog.records[0].getMessage()
    assert '"operation":"redis_readonly_command"' in message
    assert '"binary":"valkey-cli"' in message
    assert "SLOWLOG" not in message

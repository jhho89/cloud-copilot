from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
import json


REPO_ROOT = Path(__file__).resolve().parents[1]
K8S_AGENTS = REPO_ROOT / "services" / "k8s-copilot" / "src" / "agents"
if str(K8S_AGENTS) not in sys.path:
    sys.path.insert(0, str(K8S_AGENTS))

from dba_agent import tools  # noqa: E402


SLOW_LOG = """# Time: 2026-11-19T10:00:02.123456Z
# User@Host: app_user[app_user] @ [10.0.0.5]
# Query_time: 2.500000 Lock_time: 0.010000 Rows_sent: 10 Rows_examined: 100000
SET timestamp=1732005602;
SELECT o.id
FROM orders o
JOIN users u ON u.id = o.user_id
WHERE u.email = 'alice@example.com';
"""


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
    assert query.logged_at.isoformat() == "2026-11-19T10:00:02.123456+00:00"


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

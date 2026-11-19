from __future__ import annotations

import os
import re
import json
import shlex
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from typing import Any

from kubernetes import client, config, stream


@dataclass
class SlowQuery:
    pod: str
    namespace: str
    logged_at: Optional[datetime]
    query_time: float
    lock_time: float
    rows_sent: Optional[int]
    rows_examined: Optional[int]
    sql: str


def _load_kubernetes_client() -> client.CoreV1Api:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        config.load_kube_config(config_file=kubeconfig_path)
    return client.CoreV1Api()


def find_database_pods(
    db_type: str = "mysql",
    namespace: Optional[str] = None,
    label_selector: Optional[str] = None,
) -> str:
    """Lists database pods by image, container name, or pod labels.

    Use label_selector when the cluster uses custom labels such as
    app.kubernetes.io/name=mysql or app=mysql.
    """
    if db_type.lower() not in {"mysql", "mariadb"}:
        return "Unsupported database type. This tool currently supports mysql and mariadb."

    try:
        v1 = _load_kubernetes_client()
        if namespace:
            kwargs = {"namespace": namespace}
            if label_selector:
                kwargs["label_selector"] = label_selector
            pod_list = v1.list_namespaced_pod(**kwargs)
        else:
            kwargs = {"label_selector": label_selector} if label_selector else {}
            pod_list = v1.list_pod_for_all_namespaces(**kwargs)
    except Exception as exc:
        return f"Error discovering {db_type} pods: {exc}"

    matches = []
    for pod in pod_list.items:
        labels = pod.metadata.labels or {}
        container_values = []
        for container in pod.spec.containers or []:
            container_values.extend([container.name or "", container.image or ""])
        haystack = " ".join([pod.metadata.name or "", *labels.keys(), *labels.values(), *container_values])
        if db_type.lower() in haystack.lower():
            phase = pod.status.phase if pod.status else "Unknown"
            matches.append(f"{pod.metadata.namespace}/{pod.metadata.name} (Status: {phase})")

    if not matches:
        selector = f" and label selector {label_selector!r}" if label_selector else ""
        scope = namespace or "cluster-wide"
        return f"No {db_type} pods found {scope}{selector}. Try find_database_pods with a label_selector."

    return "\n".join(matches)


def _parse_timestamp(value: str) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _parse_slow_query_log(log: str, namespace: str, pod_name: str) -> list[SlowQuery]:
    entries: list[SlowQuery] = []
    current_lines: list[str] = []
    current_pod = pod_name
    current_namespace = namespace

    for line in log.splitlines():
        if line.startswith("# Time:"):
            if current_lines:
                query = _parse_slow_query_block(current_lines, current_namespace, current_pod)
                if query:
                    entries.append(query)
            current_lines = [line]
        elif current_lines:
            current_lines.append(line)

    if current_lines:
        query = _parse_slow_query_block(current_lines, current_namespace, current_pod)
        if query:
            entries.append(query)

    return entries


def _parse_slow_query_block(lines: list[str], namespace: str, pod_name: str) -> Optional[SlowQuery]:
    block = "\n".join(lines)
    query_time_match = re.search(r"^#\s*Query_time:\s*([\d.]+)", block, re.MULTILINE)
    if not query_time_match:
        return None

    time_match = re.search(r"^#\s*Time:\s*(.+)$", block, re.MULTILINE)
    logged_at = _parse_timestamp(time_match.group(1).strip()) if time_match else None
    lock_match = re.search(r"Lock_time:\s*([\d.]+)", block)
    rows_sent_match = re.search(r"Rows_sent:\s*(\d+)", block)
    rows_examined_match = re.search(r"Rows_examined:\s*(\d+)", block)

    sql_lines = []
    for line in lines:
        if line.startswith("#"):
            continue
        if re.fullmatch(r"(?i)SET\s+timestamp=\d+;?", line.strip()):
            continue
        if re.fullmatch(r"(?i)(USE|TIME)\b.*", line.strip()):
            continue
        sql_lines.append(line)

    sql = re.sub(r"\s+\n\s+", "\n", "\n".join(sql_lines).strip())
    sql = re.sub(r"[ \t]+", " ", sql)
    if not sql:
        return None

    return SlowQuery(
        pod=pod_name,
        namespace=namespace,
        logged_at=logged_at,
        query_time=float(query_time_match.group(1)),
        lock_time=float(lock_match.group(1)) if lock_match else 0.0,
        rows_sent=int(rows_sent_match.group(1)) if rows_sent_match else None,
        rows_examined=int(rows_examined_match.group(1)) if rows_examined_match else None,
        sql=sql,
    )


def get_mysql_slow_queries(
    namespace: Optional[str] = None,
    pod_name: Optional[str] = None,
    container: Optional[str] = None,
    label_selector: Optional[str] = None,
    minutes: int = 5,
    min_query_time: float = 0.5,
    limit: int = 20,
) -> str:
    """Reads recent MySQL/MariaDB slow-query entries from pod logs.

    The database must emit slow-query log output to stdout/stderr. For
    databases writing to a file, mount or configure that file as a container
    log stream, or specify the container emitting it.
    """
    try:
        v1 = _load_kubernetes_client()

        if pod_name:
            if not namespace:
                return "A namespace is required when pod_name is specified."
            pods = [v1.read_namespaced_pod(name=pod_name, namespace=namespace)]
        else:
            if namespace:
                kwargs = {"namespace": namespace}
                if label_selector:
                    kwargs["label_selector"] = label_selector
                pod_list = v1.list_namespaced_pod(**kwargs)
            else:
                kwargs = {"label_selector": label_selector} if label_selector else {}
                pod_list = v1.list_pod_for_all_namespaces(**kwargs)
            pods = [
                pod
                for pod in pod_list.items
                if "mysql" in _pod_identity(pod) or "mariadb" in _pod_identity(pod)
            ]

        if not pods:
            return "No MySQL/MariaDB pods found. Use find_database_pods first to confirm the labels and namespace."

        since_seconds = max(1, int(minutes * 60))
        tail_lines = max(limit * 20, 1000)
        all_queries: list[SlowQuery] = []
        errors = []

        for pod in pods:
            try:
                kwargs = {"name": pod.metadata.name, "namespace": pod.metadata.namespace, "since_seconds": since_seconds, "tail_lines": tail_lines}
                if container:
                    kwargs["container"] = container
                log = v1.read_namespaced_pod_log(**kwargs)
                all_queries.extend(_parse_slow_query_log(log or "", pod.metadata.namespace, pod.metadata.name))
            except Exception as exc:
                errors.append(f"{pod.metadata.namespace}/{pod.metadata.name}: {exc}")

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=since_seconds)
        recent = [query for query in all_queries if query.logged_at is None or query.logged_at >= cutoff]
        slow = [query for query in recent if query.query_time >= min_query_time]
        slow.sort(key=lambda query: (query.query_time, query.logged_at or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
        selected = slow[: max(1, limit)]

        if not selected:
            summary = f"No MySQL slow queries over {min_query_time:g}s found in the last {minutes} minutes."
            if errors:
                summary += "\n\nPod log errors:\n" + "\n".join(f"- {error}" for error in errors)
            return summary

        result = [
            f"MySQL slow queries in the last {minutes} minutes (threshold >= {min_query_time:g}s): {len(slow)} found; showing {len(selected)}.",
            "",
        ]
        for index, query in enumerate(selected, 1):
            logged_at = query.logged_at.isoformat() if query.logged_at else "unknown"
            rows_sent = query.rows_sent if query.rows_sent is not None else "unknown"
            rows_examined = query.rows_examined if query.rows_examined is not None else "unknown"
            result.extend([
                f"### {index}. {query.namespace}/{query.pod}",
                f"- Logged at: {logged_at}",
                f"- Query time: {query.query_time:.3f}s; Lock time: {query.lock_time:.3f}s",
                f"- Rows sent/examined: {rows_sent}/{rows_examined}",
                "```sql",
                query.sql,
                "```",
                "",
            ])
        if errors:
            result.extend(["Skipped pods:", *[f"- {error}" for error in errors]])
        return "\n".join(result)
    except Exception as exc:
        return f"Error reading MySQL slow-query logs: {exc}"


def _pod_identity(pod: client.V1Pod) -> str:
    values = [pod.metadata.name or ""]
    values.extend((pod.metadata.labels or {}).keys())
    values.extend((pod.metadata.labels or {}).values())
    for container in pod.spec.containers or []:
        values.extend([container.name or "", container.image or ""])
    return " ".join(values).lower()


def _validate_mysql_read_only_query(sql: str, allow_analyze: bool = False) -> str:
    """Normalizes a single SQL statement and rejects non-read-only statements."""
    normalized = sql.strip()
    if normalized.endswith(";"):
        normalized = normalized[:-1].strip()
    if not normalized or ";" in normalized:
        return ""

    first_keyword = re.match(r"(?i)^\s*([A-Za-z]+)", normalized)
    if not first_keyword:
        return ""
    keyword = first_keyword.group(1).upper()
    if allow_analyze:
        allowed = {"SELECT", "WITH"}
    else:
        allowed = {"SELECT", "WITH", "SHOW", "DESCRIBE", "DESC"}
    if keyword not in allowed:
        return ""
    return normalized


def _mysql_shell_command(
    sql: str,
    database: str,
    port: int = 3306,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> str:
    """Builds a mysql invocation that reads credentials from the pod environment."""
    user_value = shlex.quote(user) if user else '"${MYSQL_USER:-root}"'
    password_value = (
        shlex.quote(password)
        if password is not None
        else '"${MYSQL_PASSWORD:-$MYSQL_ROOT_PASSWORD}"'
    )
    return (
        f"export MYSQL_PWD={password_value}; "
        f"mysql --host=localhost --port={int(port)} --user={user_value} "
        f"--database={shlex.quote(database)} --batch --raw --skip-column-names "
        f"--execute={shlex.quote(sql)}"
    )


def _run_mysql_command(
    namespace: str,
    pod_name: str,
    command: str,
    container: Optional[str] = None,
) -> str:
    api = _load_kubernetes_client()
    exec_command = ["/bin/sh", "-c", command]
    kwargs: dict[str, Any] = {
        "name": pod_name,
        "namespace": namespace,
        "command": exec_command,
        "stderr": True,
        "stdin": False,
        "stdout": True,
        "tty": False,
        "_preload_content": True,
    }
    if container:
        kwargs["container"] = container
    return stream.stream(api.connect_get_namespaced_pod_exec, **kwargs)


def get_mysql_query_plan(
    namespace: str,
    pod_name: str,
    database: str,
    sql: str,
    container: Optional[str] = None,
    port: int = 3306,
    user: Optional[str] = None,
    password: Optional[str] = None,
    analyze: bool = False,
) -> str:
    """Runs EXPLAIN for one read-only SQL statement inside a MySQL pod."""
    normalized = _validate_mysql_read_only_query(sql, allow_analyze=analyze)
    if not normalized:
        return "Invalid SQL: provide one read-only SELECT, WITH, SHOW, DESCRIBE, or DESC statement without a semicolon or embedded statement separator."

    explained_sql = f"EXPLAIN ANALYZE {normalized}" if analyze else f"EXPLAIN FORMAT=JSON {normalized}"
    command = _mysql_shell_command(explained_sql, database, port, user, password)
    try:
        return _run_mysql_command(namespace, pod_name, command, container)
    except Exception as exc:
        return f"Error reading MySQL query plan from {namespace}/{pod_name}: {exc}"


def _collect_mysql_plan_tables(value: Any, tables: Optional[list[dict[str, Any]]] = None) -> list[dict[str, Any]]:
    if tables is None:
        tables = []
    if isinstance(value, dict):
        if isinstance(value.get("table_name"), str):
            tables.append(value)
        for child in value.values():
            _collect_mysql_plan_tables(child, tables)
    elif isinstance(value, list):
        for child in value:
            _collect_mysql_plan_tables(child, tables)
    return tables


def _parse_show_index_output(output: str) -> list[dict[str, str]]:
    lines = [line for line in output.splitlines() if line.strip()]
    if len(lines) < 2:
        return []
    headers = lines[0].split("\t")
    rows = []
    for line in lines[1:]:
        values = line.split("\t")
        rows.append({header: values[index] if index < len(values) else "" for index, header in enumerate(headers)})
    return rows


def _format_existing_indexes(index_rows: list[dict[str, str]]) -> list[str]:
    grouped: dict[str, list[str]] = {}
    for row in index_rows:
        key = row.get("Key_name", "unknown")
        column = row.get("Column_name", "")
        if column:
            grouped.setdefault(key, []).append(column)
    return [f"{key}({', '.join(columns)})" for key, columns in grouped.items()]


def _recommend_mysql_indexes_from_plan(plan_text: str, sql: str) -> str:
    try:
        plan = json.loads(plan_text)
    except json.JSONDecodeError:
        return "Could not parse the MySQL EXPLAIN JSON output. Review the raw query plan before changing indexes."

    table_nodes = _collect_mysql_plan_tables(plan)
    if not table_nodes:
        return "No MySQL table access nodes were found in the query plan."

    recommendations: list[str] = []
    for node in table_nodes[:10]:
        table = node.get("table_name", "unknown")
        access_type = node.get("access_type", "unknown")
        key = node.get("key")
        possible_keys = node.get("possible_keys") or []
        rows = node.get("rows")
        extra = node.get("Extra") or ""
        reasons: list[str] = []

        if access_type == "ALL" and not key:
            reasons.append("full table scan")
            if rows:
                reasons.append(f"estimated {rows} rows")
        if possible_keys and not key:
            reasons.append("optimizer did not use any available index")
        if "Using filesort" in extra:
            reasons.append("filesort")
        if "Using temporary" in extra:
            reasons.append("temporary table")

        if reasons:
            recommendation = f"**{table}**: {access_type} access ({', '.join(reasons)}). "
            recommendation += "Review the predicates and join columns for a selective composite index. "
            if possible_keys:
                recommendation += f"Existing candidate indexes: {', '.join(possible_keys)}."
            recommendations.append(recommendation)

    if not recommendations:
        return "The MySQL plan uses indexed access paths; no obvious index recommendation was found from this plan."

    result = [
        "MySQL index recommendations (analysis only; do not apply without validation):",
        "",
        "```sql",
        sql,
        "```",
        "",
    ]
    result.extend(f"- {recommendation}" for recommendation in recommendations)
    return "\n".join(result)


def recommend_mysql_indexes(
    namespace: str,
    pod_name: str,
    database: str,
    sql: str,
    container: Optional[str] = None,
    port: int = 3306,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> str:
    """Analyzes a read-only query and suggests indexes without creating them."""
    normalized = _validate_mysql_read_only_query(sql, allow_analyze=False)
    if not normalized:
        return "Invalid SQL: provide one read-only SELECT, WITH, SHOW, DESCRIBE, or DESC statement without a semicolon or embedded statement separator."

    plan_text = get_mysql_query_plan(
        namespace,
        pod_name,
        database,
        normalized,
        container,
        port,
        user,
        password,
        False,
    )
    if plan_text.startswith("Error reading MySQL query plan") or plan_text.startswith("Invalid SQL"):
        return plan_text

    recommendations = _recommend_mysql_indexes_from_plan(plan_text, normalized)
    try:
        plan = json.loads(plan_text)
        tables = list({node["table_name"] for node in _collect_mysql_plan_tables(plan) if node.get("table_name")})
    except (json.JSONDecodeError, TypeError):
        tables = []

    existing_index_lines: list[str] = []
    for table in tables[:5]:
        safe_table = table.replace("`", "``")
        command = _mysql_shell_command(f"SHOW INDEX FROM `{safe_table}`", database, port, user, password)
        try:
            output = _run_mysql_command(namespace, pod_name, command, container)
            indexes = _format_existing_indexes(_parse_show_index_output(output))
            if indexes:
                existing_index_lines.append(f"- `{table}`: {', '.join(indexes)}")
        except Exception as exc:
            existing_index_lines.append(f"- `{table}`: unable to read indexes ({exc})")

    if existing_index_lines:
        recommendations += "\n\n**Existing indexes**\n" + "\n".join(existing_index_lines)
    return recommendations

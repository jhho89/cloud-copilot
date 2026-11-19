from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from kubernetes import client, config


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

from __future__ import annotations

import os
import re
import json
import logging
import shlex
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Optional
from typing import Any

from kubernetes import client, config, stream

from .idle_metrics import (
    MetricSample,
    REDIS_METRIC_ALIASES,
    REDIS_METRIC_KINDS,
    read_monitoring_series,
    resolve_metrics_project_id,
)


_AUDIT_LOGGER = logging.getLogger("cloud_copilot.dba_agent")
_SLOW_LOG_READER = "/usr/bin/tail"
_REDIS_DATABASE_TYPES = {"redis", "valkey"}
_REDIS_ROLE_LABEL_KEYS = (
    "app.kubernetes.io/component",
    "redis-role",
    "valkey-role",
    "redis.kubernetes.io/role",
    "valkey.kubernetes.io/role",
)
_BLOCKED_REDIS_COMMANDS = {
    "DEBUG",
    "FLUSHALL",
    "FLUSHDB",
    "KEYS",
    "MONITOR",
    "SAVE",
    "LATENCY RESET",
    "SLOWLOG RESET",
    "CLIENT KILL",
    "SHUTDOWN",
}
_REDIS_INFO_SECTIONS = {"memory", "stats", "persistence", "replication"}
_REDIS_CLIENT_INFO_SECTIONS = {"clients", "replication"}
_REDIS_LATENCY_EVENT_CLASSES = {
    "command",
    "expire-cycle",
    "eviction-cycle",
    "eviction-del",
    "fork",
    "aof-fsync",
    "rdb-save",
}
_REDIS_CLIENT_QBUF_BYTES = 1024 * 1024
_REDIS_CLIENT_OMEM_BYTES = 8 * 1024 * 1024
_REDIS_CLIENT_LONG_AGE_SECONDS = 86400
_REDIS_CLIENT_IDLE_SECONDS = 600
_REDIS_CLIENT_HIGH_SUBSCRIPTIONS = 100
_REDIS_REMEDIATION_MIN_SLOW_MS = 100.0
_REDIS_REMEDIATION_FORK_USEC = 100_000
_REDIS_REMEDIATION_EXPIRED_KEYS = 1_000
_REDIS_REMEDIATION_TOP_COMMAND_COUNT = 10
_REDIS_STRUCTURED_SCHEMA_VERSION = "cloud-copilot.dba.redis.v1"
class SlowLogFileConfigError(ValueError):
    """Raised when a slow-log file path is missing, ambiguous, or not allowlisted."""


class SlowLogFileAccessError(RuntimeError):
    """Raised when an allowlisted slow-log file cannot be read via pod/exec."""


class RedisCommandBlockedError(ValueError):
    """Raised when a Redis command is explicitly forbidden."""


class RedisCommandNotAllowlistedError(ValueError):
    """Raised when a Redis command is not in the read-only command allowlist."""


class RedisCliMissingError(RuntimeError):
    """Raised when neither valkey-cli nor redis-cli is present in a pod."""


class RedisMetricsUnavailableError(RuntimeError):
    """Raised when no configured Redis metrics pipeline is discoverable."""


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


@dataclass
class RedisSlowLogEntry:
    namespace: str
    pod: str
    entry_id: str
    logged_at: Optional[datetime]
    duration_ms: float
    command: str
    client: str = ""
    client_name: str = ""
    client_info: str = ""
    cli: str = ""
    role: str = ""
    topology: str = ""


@dataclass(frozen=True)
class RedisRemediation:
    """One advisory Redis/Valkey change with its triggering measurements."""

    category: str
    title: str
    severity: str
    evidence: tuple[str, ...]
    change: str
    risk: str

    def render(self) -> list[str]:
        return [
            f"- {self.severity} | {self.category} | {self.title}",
            f"  - Evidence: {'; '.join(self.evidence)}",
            f"  - Suggested change: {self.change}",
            f"  - Risk/trade-off: {self.risk}",
            "  - No change was applied.",
        ]


RedisMetricSample = MetricSample


def _redis_output_mode(output_mode: object) -> str:
    if not isinstance(output_mode, str) or output_mode not in {"human", "json"}:
        raise ValueError("Invalid output_mode: use 'human' or 'json'.")
    return output_mode


def _redis_tool_output(
    tool: str,
    report: str,
    output_mode: object = "human",
    *,
    metadata: Optional[dict[str, Any]] = None,
) -> str:
    """Return the existing Markdown report, or a stable structured envelope."""
    mode = _redis_output_mode(output_mode)
    if mode == "human":
        return report
    payload: dict[str, Any] = {
        "schema_version": _REDIS_STRUCTURED_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tool": tool,
        "status": "ok",
        "data": {
            "format": "markdown",
            "report": report,
        },
    }
    if metadata is not None:
        payload["metadata"] = metadata
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _redis_tool_error(
    tool: str,
    message: str,
    output_mode: object = "human",
) -> str:
    """Return a Redis tool error without adding command output or credentials."""
    try:
        mode = _redis_output_mode(output_mode)
    except ValueError as exc:
        return str(exc)
    if mode == "human":
        return message
    payload = {
        "schema_version": _REDIS_STRUCTURED_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tool": tool,
        "status": "error",
        "errors": [message],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


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
    db_type = db_type.lower()
    if db_type not in {"mysql", "mariadb", *_REDIS_DATABASE_TYPES}:
        return "Unsupported database type. This tool supports mysql, mariadb, redis, and valkey."

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
        if db_type in haystack.lower():
            phase = pod.status.phase if pod.status else "Unknown"
            description = f"{pod.metadata.namespace}/{pod.metadata.name} (Status: {phase}"
            if db_type in _REDIS_DATABASE_TYPES:
                topology = _redis_topology_hints(pod)
                topology_parts = []
                if topology.get("statefulset"):
                    topology_parts.append(f"statefulset={topology['statefulset']}")
                if topology.get("ordinal") is not None:
                    topology_parts.append(f"ordinal={topology['ordinal']}")
                if topology.get("role"):
                    topology_parts.append(f"role={topology['role']}")
                if topology_parts:
                    description += f"; {', '.join(topology_parts)}"
            description += ")"
            matches.append(description)

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


def _validate_slow_log_path(path: object) -> str:
    """Validate an absolute POSIX slow-log path without invoking a shell."""
    if not isinstance(path, str):
        raise SlowLogFileConfigError("The slow-log path must be a string.")
    if not path or path != path.strip():
        raise SlowLogFileConfigError("The slow-log path must be non-empty and have no leading or trailing whitespace.")
    if "\x00" in path or "\\" in path:
        raise SlowLogFileConfigError("The slow-log path must be a POSIX path without NUL or backslash characters.")
    if any(ord(char) < 32 or ord(char) == 127 for char in path):
        raise SlowLogFileConfigError("The slow-log path must not contain control characters.")

    raw_components = path[1:].split("/")
    if not raw_components or any(component in {"", ".", ".."} for component in raw_components):
        raise SlowLogFileConfigError(
            "The slow-log path must contain no empty, '.', or '..' components."
        )

    parsed = PurePosixPath(path)
    if not parsed.is_absolute():
        raise SlowLogFileConfigError("The slow-log path must be absolute.")
    return path


def _configured_slow_log_paths() -> list[str]:
    """Return exact paths configured by the operator, validated before use."""
    allowlist = os.getenv("DBA_ALLOWED_SLOW_LOG_PATHS")
    configured = os.getenv("DBA_SLOW_LOG_FILE_PATH")
    if allowlist is not None:
        segments = allowlist.split(";")
        if any(not segment for segment in segments):
            raise SlowLogFileConfigError(
                "DBA_ALLOWED_SLOW_LOG_PATHS must not contain empty path entries."
            )
        raw_paths = segments
    else:
        raw_paths = [configured] if configured is not None else []
    return [_validate_slow_log_path(path) for path in raw_paths]


def _resolve_slow_log_path(file_path: Optional[str]) -> str:
    """Select a file path only from operator-configured choices."""
    configured_paths = _configured_slow_log_paths()
    if not configured_paths:
        raise SlowLogFileConfigError(
            "File-based slow-log reading is disabled. Set DBA_ALLOWED_SLOW_LOG_PATHS "
            "or DBA_SLOW_LOG_FILE_PATH to one validated absolute path."
        )
    if file_path is None:
        if len(configured_paths) != 1:
            choices = "; ".join(configured_paths)
            raise SlowLogFileConfigError(
                f"Multiple slow-log paths are configured ({choices}); pass file_path to select one."
            )
        return configured_paths[0]
    if not isinstance(file_path, str) or file_path not in configured_paths:
        raise SlowLogFileConfigError(
            "The requested slow-log path is not allowlisted. Use an exact value from "
            "DBA_ALLOWED_SLOW_LOG_PATHS or DBA_SLOW_LOG_FILE_PATH."
        )
    return file_path


def _audit_pod_exec(
    namespace: str,
    pod_name: str,
    container: Optional[str],
    operation: str,
    binary: Optional[str] = None,
    target_path: Optional[str] = None,
) -> None:
    """Emit minimal pod/exec metadata without SQL, output, or credentials."""
    payload: dict[str, Any] = {
        "event": "dba.pod_exec",
        "namespace": namespace,
        "operation": operation,
        "pod": pod_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if container:
        payload["container"] = container
    if binary is not None:
        payload["binary"] = binary
    if target_path is not None:
        payload["path"] = target_path
    _AUDIT_LOGGER.info(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def _pod_exec(
    namespace: str,
    pod_name: str,
    command: list[str],
    container: Optional[str] = None,
    operation: str = "pod_exec",
    timeout_seconds: int = 30,
    target_path: Optional[str] = None,
) -> str:
    """Run one fixed argv command over Kubernetes pod/exec.

    Redis commands use this helper directly without a remote shell. The MySQL
    path still supplies a shell invocation so existing credential defaults and
    environment expansion remain unchanged.
    """
    if not isinstance(command, (list, tuple)):
        raise ValueError("Pod exec command must be a sequence of argv values.")
    argv = list(command)
    if not argv or any(not isinstance(argument, str) or argument == "" for argument in argv):
        raise ValueError("Pod exec command must contain non-empty string arguments.")
    if any("\x00" in argument for argument in argv):
        raise ValueError("Pod exec command must not contain NUL bytes.")
    timeout = _validate_pod_exec_timeout(timeout_seconds)

    kwargs: dict[str, Any] = {
        "name": pod_name,
        "namespace": namespace,
        "command": argv,
        "stderr": True,
        "stdin": False,
        "stdout": True,
        "tty": False,
        "_preload_content": True,
        "_request_timeout": (10, timeout),
    }
    if container:
        kwargs["container"] = container
    _audit_pod_exec(namespace, pod_name, container, operation, argv[0], target_path)
    output = stream.stream(_load_kubernetes_client().connect_get_namespaced_pod_exec, **kwargs)
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace")
    return output if output is not None else ""


def _validate_pod_exec_timeout(value: object) -> int:
    try:
        timeout = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("The pod/exec timeout must be a positive number of seconds.") from exc
    if timeout < 1 or timeout > 120:
        raise ValueError("The pod/exec timeout must be between 1 and 120 seconds.")
    return timeout


def _read_slow_log_file(
    namespace: str,
    pod_name: str,
    slow_log_path: str,
    container: Optional[str] = None,
    tail_lines: int = 1000,
) -> str:
    """Tail an allowlisted slow-log file with one shell-free, read-only exec."""
    lines = max(1, int(tail_lines))
    command = [_SLOW_LOG_READER, f"--lines={lines}", "--", slow_log_path]
    try:
        return _pod_exec(
            namespace,
            pod_name,
            command,
            container,
            "slow_log_file_read",
            timeout_seconds=30,
            target_path=slow_log_path,
        )
    except Exception as exc:
        raise SlowLogFileAccessError(
            f"Unable to read slow-log file {slow_log_path} from {namespace}/{pod_name}: {exc}"
        ) from exc


def get_mysql_slow_queries(
    namespace: Optional[str] = None,
    pod_name: Optional[str] = None,
    container: Optional[str] = None,
    label_selector: Optional[str] = None,
    minutes: int = 5,
    min_query_time: float = 0.5,
    limit: int = 20,
    log_source: str = "container",
    file_path: Optional[str] = None,
) -> str:
    """Reads recent MySQL/MariaDB slow-query entries from pod logs or an allowlisted file.

    Container mode keeps the existing stdout/stderr pod-log path. File mode
    performs a read-only `tail` exec and accepts only an operator-configured
    path.
    """
    try:
        v1 = _load_kubernetes_client()
        if log_source not in {"container", "file"}:
            return "Invalid log_source: use 'container' for stdout/stderr logs or 'file'."
        if log_source == "container" and file_path is not None:
            return "file_path requires log_source='file'."
        slow_log_path = None
        if log_source == "file":
            slow_log_path = _resolve_slow_log_path(file_path)

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
                if slow_log_path is not None:
                    log = _read_slow_log_file(
                        pod.metadata.namespace,
                        pod.metadata.name,
                        slow_log_path,
                        container,
                        tail_lines,
                    )
                else:
                    kwargs = {
                        "name": pod.metadata.name,
                        "namespace": pod.metadata.namespace,
                        "since_seconds": since_seconds,
                        "tail_lines": tail_lines,
                    }
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
    except SlowLogFileConfigError as exc:
        return f"Slow-log file configuration error: {exc}"
    except Exception as exc:
        return f"Error reading MySQL slow-query logs: {exc}"


def _pod_identity(pod: client.V1Pod) -> str:
    values = [pod.metadata.name or ""]
    values.extend((pod.metadata.labels or {}).keys())
    values.extend((pod.metadata.labels or {}).values())
    for container in pod.spec.containers or []:
        values.extend([container.name or "", container.image or ""])
    return " ".join(values).lower()


def _redis_metrics_sidecars(pod: client.V1Pod) -> list[dict[str, str]]:
    """Find redis_exporter-style sidecars and their scrape annotations."""
    annotations = dict(getattr(pod.metadata, "annotations", None) or {})
    scrape = str(annotations.get("prometheus.io/scrape", "")).lower() in {"true", "1", "yes"}
    port = str(annotations.get("prometheus.io/port") or "").strip()
    path = str(annotations.get("prometheus.io/path") or "/metrics").strip() or "/metrics"
    sidecars: list[dict[str, str]] = []
    for container in pod.spec.containers or []:
        name = str(container.name or "")
        image = str(container.image or "")
        combined = f"{name} {image}".lower()
        if "redis-exporter" in combined or "redis_exporter" in combined or "valkey-exporter" in combined:
            sidecars.append(
                {
                    "container": name,
                    "image": image,
                    "port": port,
                    "path": path,
                    "scrape": "true" if scrape else "unknown",
                }
            )
    return sidecars


def _redis_metrics_discovery(
    v1: client.CoreV1Api,
    namespace: Optional[str],
    pod_name: Optional[str],
) -> dict[str, Any]:
    """Discover Kubernetes-visible Redis/Valkey exporter candidates."""
    pods = _redis_pods_for_analysis(v1, namespace, pod_name, include_sentinels=False)
    candidates: list[dict[str, Any]] = []
    for pod in pods:
        sidecars = _redis_metrics_sidecars(pod)
        if sidecars:
            candidates.append(
                {
                    "namespace": pod.metadata.namespace,
                    "pod": pod.metadata.name,
                    "source": "redis-exporter-sidecar",
                    "sidecars": sidecars,
                }
            )
    return {"pods": pods, "candidates": candidates}


def _redis_project_id(requested: Optional[str]) -> str:
    try:
        return resolve_metrics_project_id(requested)
    except Exception as exc:
        raise RedisMetricsUnavailableError(
            "A metrics project is not configured. Set DBA_REDIS_METRICS_PROJECT_ID "
            "or provide project_id when using Google Cloud Monitoring."
        ) from exc


def _read_redis_monitoring_series(
    project_id: str,
    start_time: datetime,
    end_time: datetime,
    target_namespace: str,
    target_pod: str,
) -> dict[str, dict[str, Any]]:
    metrics = read_monitoring_series(
        project_id,
        start_time,
        end_time,
        [(target_namespace, target_pod)],
        REDIS_METRIC_ALIASES,
        REDIS_METRIC_KINDS,
        descriptor_prefix="prometheus.googleapis.com/redis_",
    )
    # Preserve the long-standing logical names consumed by trend reporting.
    legacy_names = {"work_total": "commands_total", "instantaneous_work_rate": "instantaneous_ops"}
    return {legacy_names.get(logical, logical): metric for logical, metric in metrics.items()}
    if not metrics:
        raise RedisMetricsUnavailableError(
            "No Redis/Valkey metric series were found in the configured monitoring project "
            "for the requested time window."
        )
    return metrics


def _redis_metric_change(metric: Optional[dict[str, Any]], window_seconds: Optional[float]) -> dict[str, Any]:
    if not metric:
        return {"available": False, "reason": "metric not reported"}
    samples = list(metric["samples"])
    if not samples:
        return {"available": False, "reason": "no samples"}
    first, latest = samples[0], samples[-1]
    result: dict[str, Any] = {
        "available": True,
        "metric_type": metric["metric_type"],
        "kind": metric["kind"],
        "first": first.value,
        "latest": latest.value,
        "change": latest.value - first.value,
        "sample_count": len(samples),
        "window_seconds": window_seconds,
    }
    if metric["kind"] == "counter" and window_seconds and window_seconds > 0:
        if latest.value >= first.value:
            result["rate_per_second"] = (latest.value - first.value) / window_seconds
        else:
            result["rate_per_second"] = None
            result["note"] = "counter decreased; a process restart or reset is likely"
    if metric["kind"] == "gauge" and len(samples) > 1:
        result["mean"] = sum(sample.value for sample in samples) / len(samples)
    return result


def _redis_trends_from_metrics(
    metrics: dict[str, dict[str, Any]],
    start_time: datetime,
    end_time: datetime,
) -> dict[str, Any]:
    window_seconds = max(1.0, (end_time - start_time).total_seconds())
    trends: dict[str, Any] = {}
    for name in (
        "memory_used_bytes",
        "maxmemory_bytes",
        "fragmentation_ratio",
        "evicted_total",
        "connected_clients",
        "commands_total",
        "instantaneous_ops",
        "hits_total",
        "misses_total",
    ):
        trends[name] = _redis_metric_change(metrics.get(name), window_seconds)

    evicted = trends["evicted_total"]
    if evicted["available"] and evicted.get("rate_per_second") is not None:
        trends["eviction_rate_per_second"] = evicted["rate_per_second"]
    ops_counter = _redis_metric_change(metrics.get("commands_total"), window_seconds)
    if ops_counter["available"] and ops_counter.get("rate_per_second") is not None:
        trends["ops_per_second_from_counter"] = ops_counter["rate_per_second"]
    elif trends["instantaneous_ops"]["available"]:
        trends["ops_per_second_from_gauge"] = trends["instantaneous_ops"]["latest"]

    hits = trends["hits_total"]
    misses = trends["misses_total"]
    if hits["available"] and misses["available"]:
        hit_total = max(0.0, hits["latest"])
        miss_total = max(0.0, misses["latest"])
        total = hit_total + miss_total
        if total > 0:
            trends["hit_rate_latest"] = hit_total / total
        if (
            hits.get("rate_per_second") is not None
            and misses.get("rate_per_second") is not None
            and (hits["rate_per_second"] + misses["rate_per_second"]) > 0
        ):
            trends["hit_rate_from_window_rates"] = (
                hits["rate_per_second"] / (hits["rate_per_second"] + misses["rate_per_second"])
            )
    return trends


def get_redis_metrics_trends(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    minutes: int = 15,
    project_id: Optional[str] = None,
    output_mode: str = "human",
) -> str:
    """Report Redis/Valkey trends from a discovered metrics pipeline.

    Metrics are preferred for hit-rate, memory, eviction, client, and ops-rate
    trends. Pod/exec is intentionally not used as a fallback for trends.
    """
    tool = "get_redis_metrics_trends"
    try:
        output_mode = _redis_output_mode(output_mode)
        window_minutes = int(minutes)
        if window_minutes < 1 or window_minutes > 1440:
            raise ValueError("minutes must be between 1 and 1440.")
        if pod and not namespace:
            raise ValueError("A namespace is required when pod is specified.")
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=window_minutes)

        v1 = _load_kubernetes_client()
        discovery = _redis_metrics_discovery(v1, namespace, pod)
        pods = discovery["pods"]
        if not pods:
            return _redis_tool_error(
                tool,
                "No Redis or Valkey pods found. Use find_database_pods first to confirm labels and namespace.",
                output_mode,
            )
        selected = next(
            (
                candidate
                for candidate in discovery["candidates"]
                if (not pod or candidate["pod"] == pod) and (not namespace or candidate["namespace"] == namespace)
            ),
            discovery["candidates"][0] if discovery["candidates"] else None,
        )
        resolved_pod = pod or (selected["pod"] if selected else None)
        resolved_namespace = namespace or (selected["namespace"] if selected else None)
        if not resolved_pod and pods:
            resolved_pod = pods[0].metadata.name
            resolved_namespace = resolved_namespace or pods[0].metadata.namespace
        if not resolved_namespace or not resolved_pod:
            return _redis_tool_error(
                tool,
                "A metrics pipeline was found, but namespace/pod attribution is ambiguous. Pass a namespace and pod.",
                output_mode,
            )

        metrics = _read_redis_monitoring_series(
            _redis_project_id(project_id),
            start_time,
            end_time,
            resolved_namespace,
            resolved_pod,
        )
        trends = _redis_trends_from_metrics(metrics, start_time, end_time)
    except RedisMetricsUnavailableError as exc:
        return _redis_tool_error(tool, f"Metrics unavailable: {exc}", output_mode)
    except Exception as exc:
        return _redis_tool_error(tool, f"Error reading Redis/Valkey metrics trends: {exc}", output_mode)

    lines = [
        f"Redis/Valkey metrics trends for {resolved_namespace}/{resolved_pod}:",
        f"- Window: last {window_minutes} minutes (metrics-only; no pod/exec was used).",
        f"- Source: Google Cloud Monitoring in project {metrics and next(iter(metrics.values()))['metric_type'] and '' or ''}".rstrip(":"),
    ]
    for name, data in trends.items():
        if isinstance(data, bool):
            continue
        if data.get("available"):
            line = f"- {name}: latest={data['latest']:.6g}, change={data['change']:.6g}, samples={data['sample_count']}"
            if data.get("rate_per_second") is not None:
                line += f", rate={data['rate_per_second']:.6g}/s"
            if data.get("mean") is not None:
                line += f", mean={data['mean']:.6g}"
            lines.append(line)
        else:
            lines.append(f"- {name}: unavailable ({data.get('reason', 'unavailable')}).")
    if discovery["candidates"]:
        lines.append("- Sidecar discovery: yes (Kubernetes annotations/container image).")
    else:
        lines.append("- Sidecar discovery: no; metric data came from the existing monitoring pipeline.")
    lines.extend(
        [
            "",
            "Trends are observations, not causal proof. Use get_redis_memory_report or get_redis_client_report for "
            "point-in-time forensics.",
            "No changes were made; this analysis is advisory only.",
        ]
    )
    report = "\n".join(lines)
    return _redis_tool_output(
        tool,
        report,
        output_mode,
        metadata={
            "namespace": resolved_namespace,
            "pod": resolved_pod,
            "window_minutes": window_minutes,
            "sidecar_discovered": bool(discovery["candidates"]),
            "metrics_reported": sorted(metrics),
        },
    )


def _pod_matches_database_type(pod: client.V1Pod, db_type: str) -> bool:
    identity = _pod_identity(pod)
    if db_type == "valkey":
        return "valkey" in identity
    if db_type == "redis":
        return "redis" in identity
    return db_type in identity


def _redis_topology_hints(pod: client.V1Pod) -> dict[str, Any]:
    """Return cheap topology hints from pod metadata only."""
    metadata = pod.metadata
    labels = dict(getattr(metadata, "labels", None) or {})
    role = None
    role_aliases = {
        "master": "primary",
        "primary": "primary",
        "replica": "replica",
        "slave": "replica",
        "sentinel": "sentinel",
    }
    for key in _REDIS_ROLE_LABEL_KEYS:
        value = labels.get(key)
        if isinstance(value, str) and value.strip().lower() in role_aliases:
            role = role_aliases[value.strip().lower()]
            break

    statefulset = None
    for owner in getattr(metadata, "owner_references", None) or []:
        if getattr(owner, "kind", None) == "StatefulSet" and getattr(owner, "name", None):
            statefulset = owner.name
            break

    ordinal = None
    ordinal_match = re.search(r"-(\d+)$", metadata.name or "")
    if ordinal_match:
        ordinal = int(ordinal_match.group(1))

    topology_parts = []
    if statefulset:
        topology_parts.append(f"statefulset={statefulset}")
    if ordinal is not None:
        topology_parts.append(f"ordinal={ordinal}")
    return {
        "role": role,
        "statefulset": statefulset,
        "ordinal": ordinal,
        "description": ", ".join(topology_parts),
    }


def _validate_redis_command(command: object) -> list[str]:
    """Validate one read-only Redis/Valkey command against a strict allowlist."""
    if isinstance(command, str):
        try:
            tokens = shlex.split(command, posix=True)
        except ValueError as exc:
            raise ValueError(f"Invalid Redis command: {exc}") from exc
    elif isinstance(command, (list, tuple)):
        if not all(isinstance(token, str) for token in command):
            raise ValueError("Redis command arguments must be strings.")
        tokens = list(command)
    else:
        raise ValueError("A Redis command must be a string or sequence of strings.")

    if not tokens:
        raise ValueError("A Redis command cannot be empty.")
    if any("\x00" in token for token in tokens):
        raise ValueError("A Redis command must not contain NUL bytes.")

    operation = tokens[0].upper()
    operation_key = " ".join(tokens[:2]).upper() if len(tokens) > 1 else operation
    if operation_key in _BLOCKED_REDIS_COMMANDS or operation in _BLOCKED_REDIS_COMMANDS:
        raise RedisCommandBlockedError(
            "Blocked Redis command. This tool permits only selected read-only diagnostics."
        )
    if operation == "CONFIG" and len(tokens) > 1 and tokens[1].upper() != "GET":
        raise RedisCommandBlockedError(
            "Blocked Redis command. CONFIG SET and other mutations are not permitted."
        )
    if operation == "CLIENT" and len(tokens) > 1 and tokens[1].upper() not in {"LIST", "ID"}:
        raise RedisCommandBlockedError(
            "Blocked Redis command. Only CLIENT LIST and CLIENT ID are permitted."
        )
    if operation == "CLUSTER" and len(tokens) > 1 and tokens[1].upper() not in {
        "INFO",
        "MYID",
        "SHARDS",
        "NODES",
    }:
        raise RedisCommandBlockedError(
            "Blocked Redis command. Only selected CLUSTER read-only commands are permitted."
        )
    if operation == "SENTINEL" and len(tokens) > 1 and tokens[1].upper() not in {
        "MASTERS",
        "REPLICAS",
        "SENTINELS",
    }:
        raise RedisCommandBlockedError(
            "Blocked Redis command. Only selected SENTINEL read-only commands are permitted."
        )
    if operation == "SLOWLOG" and len(tokens) > 1 and tokens[1].upper() not in {"GET", "LEN"}:
        raise RedisCommandBlockedError(
            "Blocked Redis command. SLOWLOG RESET is not permitted."
        )

    if operation == "SLOWLOG" and len(tokens) == 2 and tokens[1].upper() == "LEN":
        return tokens
    if operation == "SLOWLOG" and len(tokens) == 3 and tokens[1].upper() == "GET":
        try:
            requested_count = int(tokens[2])
        except ValueError as exc:
            raise ValueError("SLOWLOG GET count must be an integer.") from exc
        if requested_count < 1 or requested_count > 128:
            raise ValueError("SLOWLOG GET count must be between 1 and 128.")
        return tokens
    if operation == "INFO" and (len(tokens) == 1 or (
        len(tokens) == 2 and tokens[1].lower() in _REDIS_INFO_SECTIONS
    )):
        return tokens
    if operation == "MEMORY" and len(tokens) == 2 and tokens[1].upper() == "STATS":
        return tokens
    if operation == "LATENCY" and len(tokens) == 2 and tokens[1].upper() == "DOCTOR":
        return tokens
    if operation == "LATENCY" and len(tokens) == 3 and tokens[1].upper() == "HISTORY":
        event = tokens[2].lower()
        if event in _REDIS_LATENCY_EVENT_CLASSES and re.fullmatch(r"[a-z0-9_-]+", event):
            return tokens
    if (
        operation == "CONFIG"
        and len(tokens) == 3
        and tokens[1].upper() == "GET"
        and tokens[2].lower() in {"slowlog-max-len", "io-threads", "client-output-buffer-limit"}
    ):
        return tokens
    if operation == "CLIENT" and len(tokens) == 2 and tokens[1].upper() in {"LIST", "ID"}:
        return tokens
    if operation == "INFO" and len(tokens) == 2 and tokens[1].lower() in _REDIS_CLIENT_INFO_SECTIONS:
        return tokens
    if operation == "CLUSTER" and len(tokens) == 2 and tokens[1].upper() in {"INFO", "MYID", "SHARDS", "NODES"}:
        return tokens
    if operation == "SENTINEL":
        if len(tokens) == 2 and tokens[1].upper() == "MASTERS":
            return tokens
        if (
            len(tokens) == 3
            and tokens[1].upper() in {"REPLICAS", "SENTINELS"}
            and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", tokens[2])
        ):
            return tokens

    raise RedisCommandNotAllowlistedError(
        "Redis command not allowlisted. This tool permits only SLOWLOG GET, "
        "SLOWLOG LEN, selected INFO/MEMORY/LATENCY/CLIENT/CLUSTER/SENTINEL "
        "diagnostics, and selected CONFIG GET parameters."
    )


def _redis_cli_argv(cli: str, redis_command: object, port: Optional[int] = None) -> list[str]:
    """Build redis-cli/valkey-cli argv; credentials are never command arguments."""
    tokens = _validate_redis_command(redis_command)
    argv = [cli, "--json"]
    if port is not None:
        try:
            normalized_port = int(port)
        except (TypeError, ValueError) as exc:
            raise ValueError("The Redis port must be an integer.") from exc
        if normalized_port < 1 or normalized_port > 65535:
            raise ValueError("The Redis port must be between 1 and 65535.")
        argv.extend(["--port", str(normalized_port)])
    argv.extend(tokens)
    return argv


def _run_redis_command(
    namespace: str,
    pod_name: str,
    cli: str,
    redis_command: object,
    container: Optional[str] = None,
    port: Optional[int] = None,
) -> str:
    """Run one allowlisted Redis command using direct argv and pod/exec."""
    argv = _redis_cli_argv(cli, redis_command, port)
    operation = "redis_readonly_command"
    return _pod_exec(
        namespace,
        pod_name,
        argv,
        container,
        operation,
        timeout_seconds=30,
    )


def _detect_redis_cli(
    namespace: str,
    pod_name: str,
    container: Optional[str] = None,
) -> str:
    """Prefer valkey-cli and never probe credentials or renamed binaries."""
    for cli, marker in (("valkey-cli", "valkey-cli"), ("redis-cli", "redis-cli")):
        output = _pod_exec(
            namespace,
            pod_name,
            [cli, "--version"],
            container,
            "redis_cli_detection",
            timeout_seconds=10,
        )
        output_text = output.lower()
        executable_missing = (
            "executable file not found" in output_text
            or "no such file or directory" in output_text
        )
        if marker in output_text and not executable_missing:
            return cli
    raise RedisCliMissingError(
        "Neither valkey-cli nor redis-cli is available in the selected pod/container."
    )


def _decode_resp(data: object) -> Any:
    """Decode a RESP2/RESP3 array response into Python values."""
    raw = data.encode("utf-8") if isinstance(data, str) else data
    if not isinstance(raw, (bytes, bytearray)):
        raise ValueError("RESP input must be text or bytes.")
    raw = bytes(raw)
    position = 0

    def read_line() -> bytes:
        nonlocal position
        end = raw.find(b"\r\n", position)
        if end < 0:
            raise ValueError("Truncated RESP response.")
        line = raw[position:end]
        position = end + 2
        return line

    def read_value():
        nonlocal position
        if position >= len(raw):
            raise ValueError("Truncated RESP response.")
        prefix = raw[position : position + 1]
        position += 1
        line = read_line()
        if prefix == b"*":
            count = int(line)
            if count < 0:
                return None
            return [read_value() for _ in range(count)]
        if prefix == b"+" or prefix == b"-":
            return line.decode("utf-8", errors="replace")
        if prefix == b":":
            return int(line)
        if prefix == b"$":
            length = int(line)
            if length < 0:
                return None
            value = raw[position : position + length]
            position += length + 2
            if len(value) != length:
                raise ValueError("Truncated RESP bulk string.")
            return value.decode("utf-8", errors="replace")
        raise ValueError(f"Unsupported RESP type {prefix!r}.")

    value = read_value()
    if position != len(raw):
        raise ValueError("Unexpected trailing bytes after RESP response.")
    return value


def _parse_redis_response(output: str) -> Any:
    """Parse CLI JSON or true RESP output from Redis/Valkey."""
    stripped = output.strip()
    if not stripped:
        return None
    if stripped.startswith("[") or stripped.startswith("{"):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON response from Redis CLI: {exc}") from exc
    if stripped.startswith("*"):
        return _decode_resp(output)
    return stripped


def _format_redis_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return " ".join(part for part in (_format_redis_value(item) for item in value) if part)
    return str(value)


def _parse_slowlog_entries(output: str) -> list[Any]:
    payload = _parse_redis_response(output)
    if payload is None or (isinstance(payload, list) and not payload):
        return []
    if isinstance(payload, list) and all(isinstance(entry, list) for entry in payload):
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        return payload
    return [payload]


def _parse_slowlog_entry(entry: Any, context: dict[str, str]) -> Optional[RedisSlowLogEntry]:
    if not isinstance(entry, (list, tuple)) or len(entry) < 4:
        return None
    try:
        entry_id = int(_format_redis_value(entry[0]))
        logged_at_epoch = int(_format_redis_value(entry[1]))
        duration_us = int(_format_redis_value(entry[2]))
    except (TypeError, ValueError):
        return None

    command = _format_redis_value(entry[3])
    if not command:
        return None

    client = ""
    client_name = ""
    client_info = ""
    saw_client = False
    saw_name = False
    for value in entry[4:]:
        if isinstance(value, (list, tuple)):
            text = _format_redis_value(value)
            if text and not client_info:
                client_info = text
            continue
        text = _format_redis_value(value)
        if not saw_client:
            client = text
            saw_client = True
        elif not saw_name:
            client_name = text
            saw_name = True
        elif text and not client_info:
            client_info = text

    logged_at = datetime.fromtimestamp(logged_at_epoch, timezone.utc)
    return RedisSlowLogEntry(
        namespace=context["namespace"],
        pod=context["pod"],
        entry_id=str(entry_id),
        logged_at=logged_at,
        duration_ms=duration_us / 1000.0,
        command=command,
        client=client,
        client_name=client_name,
        client_info=client_info,
        cli=context.get("cli", ""),
        role=context.get("role", ""),
        topology=context.get("topology", ""),
    )


def _parse_integer_response(output: str) -> Optional[int]:
    payload = _parse_redis_response(output)
    if isinstance(payload, int):
        return payload
    if isinstance(payload, list) and len(payload) == 1:
        try:
            return int(payload[0])
        except (TypeError, ValueError):
            return None
    try:
        return int(payload)
    except (TypeError, ValueError):
        return None


def _redis_result_context(pod: client.V1Pod, cli: str) -> dict[str, str]:
    topology = _redis_topology_hints(pod)
    return {
        "namespace": pod.metadata.namespace,
        "pod": pod.metadata.name,
        "cli": cli,
        "role": topology.get("role") or "",
        "topology": topology.get("description") or "",
    }


def _redis_context_label(context: dict[str, str]) -> str:
    label = f"{context['namespace']}/{context['pod']}"
    descriptors = []
    if "runtime_role" in context:
        descriptors.append(f"runtime-role={context.get('runtime_role') or 'unknown'}")
    if context.get("role"):
        descriptors.append(f"metadata-role={context['role']}")
    if context.get("topology"):
        descriptors.append(context["topology"])
    if descriptors:
        label += f" ({'; '.join(descriptors)})"
    return label


def _redis_command_family(command: str) -> str:
    match = re.match(r"([A-Za-z]+)", command.strip())
    return match.group(1).upper() if match else "UNKNOWN"


def _redis_pods_for_analysis(
    v1: client.CoreV1Api,
    namespace: Optional[str],
    pod: Optional[str],
    include_sentinels: bool = True,
) -> list[client.V1Pod]:
    if pod:
        return [v1.read_namespaced_pod(name=pod, namespace=namespace)]
    if namespace:
        pod_list = v1.list_namespaced_pod(namespace=namespace)
    else:
        pod_list = v1.list_pod_for_all_namespaces()
    candidates = [
        candidate
        for candidate in pod_list.items
        if _pod_matches_database_type(candidate, "redis")
        or _pod_matches_database_type(candidate, "valkey")
    ]
    return candidates if include_sentinels else [pod for pod in candidates if not _redis_pod_is_sentinel(pod)]


def _redis_pod_is_sentinel(pod: client.V1Pod) -> bool:
    identity = _pod_identity(pod)
    return "sentinel" in identity and (
        "redis" in identity or "valkey" in identity
    )


def _parse_info_sections(output: str) -> dict[str, Any]:
    """Normalize JSON/RESP INFO output into a case-insensitive flat map."""
    payload = _parse_redis_response(output)
    fields: dict[str, Any] = {}

    def collect(source: Any, section: Optional[str] = None) -> None:
        if isinstance(source, dict):
            for key, value in source.items():
                key_text = _format_redis_value(key).lstrip("#").strip()
                if isinstance(value, dict):
                    collect(value, section)
                else:
                    fields[f"{section}:{key_text}".lower() if section else key_text.lower()] = value
            return
        if isinstance(source, (list, tuple)):
            for item in source:
                collect(item, section)
            return
        if isinstance(source, bytes):
            source = source.decode("utf-8", errors="replace")
        if not isinstance(source, str):
            return
        for line in source.splitlines():
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            fields[key.strip().lower()] = value.strip()

    collect(payload)
    return fields


def _redis_info_value(metrics: dict[str, Any], name: str) -> Any:
    return metrics.get(name.lower())


def _redis_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    try:
        return float(value.strip())
    except ValueError:
        return None


def _read_redis_evidence(
    namespace: str,
    pod_name: str,
    cli: str,
    context: dict[str, str],
    container: Optional[str] = None,
    port: Optional[int] = None,
) -> dict[str, Any]:
    """Collect read-only INFO/MEMORY evidence and re-read runtime role."""
    metrics: dict[str, Any] = {}
    errors: list[str] = []
    for section in ("memory", "stats", "persistence", "replication"):
        try:
            section_fields = _parse_info_sections(
                _run_redis_command(
                    namespace,
                    pod_name,
                    cli,
                    ["INFO", section],
                    container,
                    port,
                )
            )
            metrics.update(section_fields)
        except Exception as exc:
            errors.append(f"INFO {section}: {exc}")

    try:
        metrics.update(
            _parse_info_sections(
                _run_redis_command(
                    namespace,
                    pod_name,
                    cli,
                    ["MEMORY", "STATS"],
                    container,
                    port,
                )
            )
        )
    except Exception as exc:
        errors.append(f"MEMORY STATS: {exc}")

    runtime_role = _redis_info_value(metrics, "role")
    context["runtime_role"] = str(runtime_role) if runtime_role else "unknown"
    return {
        "metrics": metrics,
        "errors": errors,
        "cli": cli,
        "context": context,
    }


def _read_redis_client_evidence(
    namespace: str,
    pod_name: str,
    cli: str,
    context: dict[str, str],
    container: Optional[str] = None,
    port: Optional[int] = None,
) -> dict[str, Any]:
    """Collect client, buffer, replication, and cluster evidence at report time."""
    metrics: dict[str, Any] = {}
    errors: list[str] = []

    for section in ("clients", "replication"):
        try:
            section_fields = _parse_info_sections(
                _run_redis_command(namespace, pod_name, cli, ["INFO", section], container, port)
            )
            metrics.update(section_fields)
        except Exception as exc:
            errors.append(f"INFO {section}: {exc}")

    buffer_limits: dict[str, dict[str, int]] = {}
    buffer_error = ""
    try:
        output = _run_redis_command(
            namespace,
            pod_name,
            cli,
            ["CONFIG", "GET", "client-output-buffer-limit"],
            container,
            port,
        )
        buffer_limits, buffer_error = _parse_output_buffer_limit(output)
        if buffer_error:
            errors.append(buffer_error)
    except Exception as exc:
        buffer_error = str(exc)
        errors.append(f"CONFIG GET client-output-buffer-limit: {exc}")

    records: list[dict[str, Any]] = []
    client_list_error = ""
    try:
        records, client_list_error = _parse_client_list(
            _run_redis_command(namespace, pod_name, cli, ["CLIENT", "LIST"], container, port)
        )
        if client_list_error:
            errors.append(client_list_error)
    except Exception as exc:
        client_list_error = str(exc)
        errors.append(f"CLIENT LIST: {exc}")

    client_id: Optional[int] = None
    try:
        client_id = _parse_integer_response(
            _run_redis_command(namespace, pod_name, cli, ["CLIENT", "ID"], container, port)
        )
    except Exception as exc:
        errors.append(f"CLIENT ID: {exc}")

    cluster_info: dict[str, Any] = {}
    cluster_error = ""
    try:
        cluster_info = _parse_cluster_info(
            _run_redis_command(namespace, pod_name, cli, ["CLUSTER", "INFO"], container, port)
        )
    except Exception as exc:
        cluster_error = str(exc)
        errors.append(f"CLUSTER INFO: {exc}")

    cluster_mode = _redis_number(_redis_info_value(cluster_info, "cluster_enabled")) == 1
    cluster_id: Optional[str] = None
    if cluster_mode:
        try:
            cluster_id = _parse_cluster_id(
                _run_redis_command(namespace, pod_name, cli, ["CLUSTER", "MYID"], container, port)
            )
        except Exception as exc:
            errors.append(f"CLUSTER MYID: {exc}")

    shards: list[dict[str, Any]] = []
    shards_error = ""
    if cluster_mode:
        try:
            shards, shards_error = _parse_cluster_shards(
                _run_redis_command(namespace, pod_name, cli, ["CLUSTER", "SHARDS"], container, port)
            )
            if shards_error:
                errors.append(shards_error)
        except Exception as exc:
            shards_error = str(exc)
            errors.append(f"CLUSTER SHARDS: {exc}")

    runtime_role = _redis_info_value(metrics, "role")
    context["runtime_role"] = str(runtime_role) if runtime_role else "unknown"
    return {
        "metrics": metrics,
        "records": records,
        "client_list_error": client_list_error,
        "client_id": client_id,
        "buffer_limits": buffer_limits,
        "cluster_info": cluster_info,
        "cluster_mode": cluster_mode,
        "cluster_id": cluster_id,
        "shards": shards,
        "errors": errors,
        "context": context,
    }


def _redis_replication_findings(metrics: dict[str, Any], runtime_role: str) -> list[str]:
    findings: list[str] = []
    if runtime_role in {"replica", "slave", "replica-of", "follower"}:
        link_status = str(_redis_info_value(metrics, "master_link_status") or "").lower()
        if link_status and link_status != "up":
            findings.append(f"master_link_status={link_status}; the replica is not currently replicating.")
        sync = _redis_number(_redis_info_value(metrics, "master_sync_in_progress"))
        if sync == 1:
            findings.append("master_sync_in_progress=1; a resync is currently underway.")

    master_offset = _redis_number(_redis_info_value(metrics, "master_repl_offset"))
    replica_offset = _redis_number(
        _redis_info_value(metrics, "slave_repl_offset")
        if _redis_info_value(metrics, "slave_repl_offset") is not None
        else _redis_info_value(metrics, "slave_read_repl_offset")
    )
    if master_offset is not None and replica_offset is not None:
        lag_bytes = max(0, int(master_offset - replica_offset))
        findings.append(f"replication lag evidence: {lag_bytes} bytes between master and replica offsets.")
        if lag_bytes >= 1024 * 1024:
            findings.append("Replica lag exceeds 1 MiB; inspect disk/network and command throughput.")

    last_io = _redis_number(_redis_info_value(metrics, "master_last_io_seconds_ago"))
    if last_io is not None and last_io >= 30:
        findings.append(
            f"master_last_io_seconds_ago={int(last_io)}; replication heartbeat has not been seen recently."
        )

    backlog_size = _redis_number(_redis_info_value(metrics, "repl_backlog_size"))
    backlog_len = _redis_number(_redis_info_value(metrics, "repl_backlog_histlen"))
    backlog_active = _redis_number(_redis_info_value(metrics, "repl_backlog_active"))
    if backlog_size is not None:
        if backlog_active == 0:
            findings.append(
                "The replication backlog is inactive; a reconnecting replica may require a full resync."
            )
        else:
            if backlog_len is not None:
                headroom = max(0, int(backlog_size - backlog_len))
                findings.append(
                    f"Replication backlog: size={int(backlog_size)}, used={int(backlog_len)}, headroom={headroom} bytes."
                )
                if backlog_len >= backlog_size:
                    findings.append(
                        "The replication backlog is full; an offline replica can miss its partial-sync window "
                        "and require an expensive full resync."
                    )
                elif headroom <= backlog_size * 0.1:
                    findings.append("Replication backlog headroom is at or below 10%.")

    replid = _redis_info_value(metrics, "master_replid")
    if replid:
        findings.append(f"master_replid={replid}.")
    else:
        findings.append("master_replid is unavailable; replication identity consistency cannot be assessed here.")
    return findings


def _format_redis_evidence(metrics: dict[str, Any], evidence_errors: list[str]) -> list[str]:
    memory_names = (
        "used_memory_human",
        "used_memory",
        "maxmemory",
        "maxmemory_human",
        "maxmemory_policy",
        "mem_fragmentation_ratio",
        "mem_allocator",
        "activedefrag_running",
        "total_system_memory",
    )
    lines = ["Memory evidence (INFO memory):"]
    shown = 0
    for name in memory_names:
        value = _redis_info_value(metrics, name)
        if value is not None:
            lines.append(f"- {name}: {value}")
            shown += 1
    if not shown:
        lines.append("- unavailable")

    stats_names = ("evicted_keys", "keyspace_misses", "keyspace_hits", "instantaneous_ops_per_sec")
    lines.append("Command and keyspace evidence (INFO stats):")
    shown = 0
    for name in stats_names:
        value = _redis_info_value(metrics, name)
        if value is not None:
            lines.append(f"- {name}: {value}")
            shown += 1
    if not shown:
        lines.append("- unavailable")

    persistence_names = (
        "loading",
        "rdb_bgsave_in_progress",
        "rdb_last_bgsave_status",
        "aof_enabled",
        "aof_rewrite_in_progress",
        "aof_last_bgrewrite_status",
        "aof_last_write_status",
        "latest_fork_usec",
    )
    lines.append("Persistence evidence (INFO persistence):")
    shown = 0
    for name in persistence_names:
        value = _redis_info_value(metrics, name)
        if value is not None:
            lines.append(f"- {name}: {value}")
            shown += 1
    if not shown:
        lines.append("- unavailable")

    memory_stat_names = ("dataset.bytes", "fragmentation", "fragmentation.bytes", "keys.count")
    lines.append("MEMORY STATS:")
    shown = 0
    for name in memory_stat_names:
        value = _redis_info_value(metrics, name)
        if value is not None:
            lines.append(f"- {name}: {value}")
            shown += 1
    if not shown:
        lines.append("- unavailable")
    if evidence_errors:
        lines.extend(["Unavailable/error responses: ", *(f"- {error}" for error in evidence_errors)])
    return lines


def _redis_memory_findings(metrics: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    used = _redis_number(_redis_info_value(metrics, "used_memory"))
    maxmemory = _redis_number(_redis_info_value(metrics, "maxmemory"))
    policy = _redis_info_value(metrics, "maxmemory_policy")
    policy_text = str(policy) if policy is not None else None
    evicted = _redis_number(_redis_info_value(metrics, "evicted_keys"))

    if maxmemory == 0:
        findings.append(
            "maxmemory=0 means the instance has no in-process limit; without a container "
            "limit and eviction policy that safely applies, writes can drive unbounded memory growth."
        )
    elif maxmemory is not None and used is not None and maxmemory > 0:
        ratio = used / maxmemory
        state = "at or above" if ratio >= 1 else "approaching" if ratio >= 0.9 else "within"
        findings.append(
            f"used_memory is {ratio * 100:.1f}% of maxmemory ({state} the configured limit)."
        )

    if policy_text:
        if policy_text == "noeviction":
            findings.append(
                "With noeviction, once the limit is reached writes fail instead of evicting data; "
                "that can surface as application write errors before memory stops growing."
            )
        elif policy_text.startswith("volatile-"):
            findings.append(
                f"Policy {policy_text} considers only keys with an expiry; keys without TTL can still "
                "prevent effective eviction and drive writes toward failure."
            )
        elif policy_text.startswith("allkeys-random"):
            findings.append(
                f"Policy {policy_text} evicts without recency evidence and can discard hot data."
            )
        elif policy_text.startswith("allkeys-"):
            findings.append(
                f"Policy {policy_text} can evict any key under pressure; confirm callers tolerate that."
            )

    if evicted is not None and evicted > 0:
        findings.append(
            f"evicted_keys={int(evicted)} is direct evidence that eviction has occurred; "
            "check whether misses or missing values followed it."
        )

    fragmentation = _redis_number(_redis_info_value(metrics, "mem_fragmentation_ratio"))
    if fragmentation is not None:
        if fragmentation >= 1.5:
            findings.append(
                f"mem_fragmentation_ratio={fragmentation:g} is high; resident memory is materially "
                "above logical used_memory, so deleting keys may not immediately return RSS."
            )
        elif fragmentation < 1:
            findings.append(
                f"mem_fragmentation_ratio={fragmentation:g} is below 1; resident memory is lower than "
                "logical usage, which can indicate swapping or compressed/shared pages and should be checked."
            )
    allocator = _redis_info_value(metrics, "mem_allocator")
    defrag_running = _redis_number(_redis_info_value(metrics, "activedefrag_running"))
    if allocator and str(allocator).lower() != "jemalloc":
        findings.append(
            f"mem_allocator={allocator} does not provide the same active-defrag behavior as jemalloc."
        )
    if defrag_running == 1:
        findings.append("activedefrag_running=1 shows active defragmentation is currently running.")

    fork_usec = _redis_number(_redis_info_value(metrics, "latest_fork_usec"))
    if fork_usec is not None and fork_usec > 0:
        findings.append(
            f"latest_fork_usec={int(fork_usec)} ({fork_usec / 1000:.1f}ms) shows fork cost; "
            "copy-on-write can also spike resident memory during persistence."
        )
    rdb_running = _redis_number(_redis_info_value(metrics, "rdb_bgsave_in_progress"))
    aof_rewrite = _redis_number(_redis_info_value(metrics, "aof_rewrite_in_progress"))
    if rdb_running == 1 or aof_rewrite == 1:
        findings.append(
            "A background save or AOF rewrite is in progress; expect possible fork and "
            "copy-on-write memory overhead until it finishes."
        )
    return findings


def _parse_bigkeys_output(output: str) -> tuple[list[dict[str, Any]], Optional[int]]:
    """Parse redis-cli --bigkeys/--memkeys summaries without running a scan."""
    findings: dict[str, dict[str, Any]] = {}
    pattern = re.compile(
        r"(?im)Biggest\s+([a-z]+)\s+(?:(?:seen|found)(?:\s+so far)?|so far)\s+"
        r"'(.*)'\s+has\s+(\d+)\s+(bytes|items|members|fields|elements|entries)"
    )
    for match in pattern.finditer(output):
        key_type = match.group(1).lower()
        size = int(match.group(3))
        current = findings.get(key_type)
        if current is None or size > int(current["size"]):
            findings[key_type] = {
                "key_type": key_type,
                "key": match.group(2),
                "size": size,
                "size_unit": match.group(4).lower(),
            }

    sampled_match = re.search(r"(?im)Sampled\s+(\d+)\s+keys", output)
    sampled = int(sampled_match.group(1)) if sampled_match else None
    return list(findings.values()), sampled


def analyze_redis_bigkeys(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    mode: str = "bigkeys",
    container: Optional[str] = None,
    port: Optional[int] = None,
    sample_interval_seconds: float = 0.01,
    output_mode: str = "human",
) -> str:
    """Analyze large keys with redis-cli/valkey-cli SCAN-based --bigkeys/--memkeys.

    This never invokes `KEYS`, a manual `SCAN`, `DEBUG`, or a mutation command.
    A namespace and pod are required to keep the O(N) SCAN bounded to one node.
    """
    tool = "analyze_redis_bigkeys"
    try:
        output_mode = _redis_output_mode(output_mode)
    except ValueError as exc:
        return _redis_tool_error(tool, str(exc), "human")
    if not namespace or not pod:
        return _redis_tool_error(
            tool,
            "A namespace and pod are required for big-key analysis so the O(N) "
            "SCAN-based CLI remains bounded to a single node.",
            output_mode,
        )
    if mode not in {"bigkeys", "memkeys"}:
        return _redis_tool_error(tool, "Invalid mode: use 'bigkeys' or 'memkeys'.", output_mode)
    try:
        interval = float(sample_interval_seconds)
    except (TypeError, ValueError):
        return _redis_tool_error(
            tool,
            "Invalid sample_interval_seconds: use a non-negative number of seconds.",
            output_mode,
        )
    if interval < 0 or interval > 1:
        return _redis_tool_error(
            tool,
            "Invalid sample_interval_seconds: use a value between 0 and 1.",
            output_mode,
        )

    try:
        v1 = _load_kubernetes_client()
        pod_item = v1.read_namespaced_pod(name=pod, namespace=namespace)
        context = _redis_result_context(pod_item, "")
        cli = _detect_redis_cli(namespace, pod, container)
        role_error = ""
        try:
            replication = _parse_info_sections(
                _run_redis_command(namespace, pod, cli, ["INFO", "replication"], container, port)
            )
            runtime_role = _redis_info_value(replication, "role")
            context["runtime_role"] = str(runtime_role) if runtime_role else "unknown"
        except Exception as exc:
            context["runtime_role"] = "unknown"
            role_error = str(exc)

        interval_text = f"{interval:.3f}".rstrip("0").rstrip(".")
        argv = [cli, f"--{mode}"]
        if interval_text:
            argv.extend(["-i", interval_text])
        output = _pod_exec(
            namespace,
            pod,
            argv,
            container,
            "redis_scan_based_key_analysis",
            timeout_seconds=120,
        )
        findings, sampled = _parse_bigkeys_output(output)
    except Exception as exc:
        return _redis_tool_error(
            tool,
            f"Error analyzing Redis/Valkey big keys in {namespace}/{pod}: {exc}",
            output_mode,
        )

    result = [
        f"Redis/Valkey big-key analysis ({mode}; SCAN-based) for {_redis_context_label(context)}:",
        f"- CLI: {cli}",
        f"- Runtime role: {context.get('runtime_role') or 'unknown'}",
        *(
            [f"- Role evidence unavailable: {role_error}"]
            if context.get("runtime_role") == "unknown" and role_error
            else []
        ),
        f"- Keys sampled by the CLI: {sampled if sampled is not None else 'unknown'}",
        "- Method: the CLI performed its own SCAN-based sampling; no KEYS, manual SCAN, "
        "DEBUG, or mutation command was issued.",
        "",
    ]
    if not findings:
        result.extend(
            [
                "No type/size summaries were parsed from the CLI output.",
                "Raw CLI evidence (truncated):",
                "```text",
                output.strip()[-4000:],
                "```",
            ]
        )
    else:
        findings.sort(key=lambda finding: (int(finding["size"]), finding["key_type"]), reverse=True)
        result.extend(["Type/size evidence:", ""])
        for finding in findings:
            result.append(
                f"- {finding['key_type']}: key `{finding['key']}` has "
                f"{finding['size']} {finding['size_unit']}"
            )

    result.extend(
        [
            "",
            "O(N) risk notes:",
            "- The analyzer itself iterates the keyspace and can add load on a large dataset.",
            "- Large lists, sets, sorted sets, hashes, and streams can turn type-specific reads, "
            "deletes, or expire work into O(N) or multi-key operations.",
            "- Size alone does not prove a problem; compare with access patterns before changing data.",
            "",
            "Recommendations are advisory only. No KEYS, manual SCAN, DEBUG, or mutation command was used.",
            "No changes were made; this analysis is advisory only.",
        ]
    )
    return _redis_tool_output(
        tool,
        "\n".join(result),
        output_mode,
        metadata={"namespace": namespace, "pod": pod, "mode": mode},
    )


def get_redis_memory_report(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    container: Optional[str] = None,
    port: Optional[int] = None,
    output_mode: str = "human",
) -> str:
    """Report Redis/Valkey memory, eviction, fragmentation, and fork evidence.

    Uses only `INFO memory`, `INFO stats`, `INFO persistence`,
    `INFO replication`, and `MEMORY STATS`; recommendations are advisory.
    """
    tool = "get_redis_memory_report"
    try:
        output_mode = _redis_output_mode(output_mode)
    except ValueError as exc:
        return _redis_tool_error(tool, str(exc), "human")
    if pod and not namespace:
        return _redis_tool_error(
            tool,
            "A namespace is required when pod is specified.",
            output_mode,
        )
    try:
        v1 = _load_kubernetes_client()
        pods = _redis_pods_for_analysis(v1, namespace, pod)
    except Exception as exc:
        return _redis_tool_error(
            tool,
            f"Error discovering or reading Redis/Valkey pods: {exc}",
            output_mode,
        )
    if not pods:
        return _redis_tool_error(
            tool,
            "No Redis or Valkey pods found. Use find_database_pods first "
            "to confirm the labels and namespace.",
            output_mode,
        )

    sections: list[str] = []
    errors: list[str] = []
    for pod_item in pods:
        context = _redis_result_context(pod_item, "")
        label = f"{context['namespace']}/{context['pod']}"
        try:
            cli = _detect_redis_cli(context["namespace"], context["pod"], container)
            evidence = _read_redis_evidence(
                context["namespace"],
                context["pod"],
                cli,
                context,
                container,
                port,
            )
            metrics = evidence["metrics"]
            evidence_errors = evidence["errors"]
            has_memory_evidence = any(
                name in metrics
                for name in (
                    "used_memory_human",
                    "used_memory",
                    "maxmemory",
                    "maxmemory_policy",
                    "mem_fragmentation_ratio",
                    "dataset.bytes",
                    "fragmentation",
                )
            )
            section = [f"### {label} (CLI: {cli})", f"- {_redis_context_label(context)}"]
            section.extend(_format_redis_evidence(metrics, evidence_errors))
            if has_memory_evidence:
                section.extend(["", "Evidence interpretation:"])
                findings = _redis_memory_findings(metrics)
                if findings:
                    section.extend(f"- {finding}" for finding in findings)
                else:
                    section.append("- No pressure, eviction, fragmentation, or persistence threshold was crossed.")
            else:
                section.extend(
                    [
                        "",
                        "Evidence interpretation: memory evidence is unavailable, so pressure and "
                        "eviction behavior cannot be assessed.",
                    ]
                )
            section.extend(
                [
                    "",
                    "Recommendations are advisory and evidence-linked; they have not been applied.",
                    "",
                ]
            )
            sections.append("\n".join(section))
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    result = [f"Redis/Valkey memory analysis: {len(sections)} pod(s) reported.", ""]
    result.extend("\n".join(sections).splitlines())
    if errors:
        result.extend(["Skipped pods:", *(f"- {error}" for error in errors), ""])
    result.append("No changes were made; this analysis is advisory only.")
    return _redis_tool_output(
        tool,
        "\n".join(result),
        output_mode,
        metadata={"namespace": namespace, "pod": pod, "pods_reported": len(sections)},
    )


_REDIS_SNAPSHOT_FIELDS = (
    "used_memory",
    "maxmemory",
    "mem_fragmentation_ratio",
    "fragmentation",
    "evicted_keys",
    "keyspace_hits",
    "keyspace_misses",
    "instantaneous_ops_per_sec",
    "connected_clients",
    "maxclients",
    "role",
)


def _read_redis_snapshot(
    namespace: str,
    pod_name: str,
    cli: str,
    context: dict[str, str],
    container: Optional[str],
    port: Optional[int],
) -> tuple[dict[str, Any], datetime, list[str]]:
    evidence = _read_redis_evidence(namespace, pod_name, cli, context, container, port)
    metrics = evidence["metrics"]
    try:
        metrics.update(
            _parse_info_sections(
                _run_redis_command(namespace, pod_name, cli, ["INFO", "clients"], container, port)
            )
        )
    except Exception as exc:
        evidence["errors"].append(f"INFO clients: {exc}")
    snapshot = {name: metrics.get(name) for name in _REDIS_SNAPSHOT_FIELDS if name in metrics}
    return snapshot, datetime.now(timezone.utc), list(evidence["errors"])


def _redis_snapshot_delta(
    first: dict[str, Any],
    second: dict[str, Any],
    elapsed_seconds: float,
) -> dict[str, Any]:
    def delta(name: str) -> Optional[float]:
        first_value = _redis_number(first.get(name))
        second_value = _redis_number(second.get(name))
        if first_value is None or second_value is None:
            return None
        return second_value - first_value

    fragmentation = delta("mem_fragmentation_ratio")
    if fragmentation is None:
        fragmentation = delta("fragmentation")
    evictions = delta("evicted_keys")
    eviction_rate = evictions / elapsed_seconds if evictions is not None and evictions >= 0 else None
    ops = delta("instantaneous_ops_per_sec")
    hits = delta("keyspace_hits")
    misses = delta("keyspace_misses")
    hit_ratio = None
    if hits is not None and misses is not None and (hits + misses) > 0:
        hit_ratio = hits / (hits + misses)
    return {
        "memory_bytes": delta("used_memory"),
        "maxmemory_bytes": delta("maxmemory"),
        "fragmentation": fragmentation,
        "evicted_keys": evictions,
        "eviction_rate_per_second": eviction_rate,
        "keyspace_hits": hits,
        "keyspace_misses": misses,
        "hit_ratio_from_delta": hit_ratio,
        "instantaneous_ops_per_sec": ops,
        "connected_clients": delta("connected_clients"),
        "maxclients": delta("maxclients"),
        "elapsed_seconds": elapsed_seconds,
    }


def get_redis_snapshot_diff(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    interval_minutes: int = 5,
    container: Optional[str] = None,
    port: Optional[int] = None,
    output_mode: str = "human",
) -> str:
    """Capture two read-only INFO snapshots N minutes apart and report deltas.

    The call intentionally blocks while the two snapshots are separated. Only
    selected numeric INFO fields and role evidence are retained; raw command
    output and credentials are never persisted in the report.
    """
    tool = "get_redis_snapshot_diff"
    try:
        output_mode = _redis_output_mode(output_mode)
        interval = int(interval_minutes)
        if interval < 1 or interval > 60:
            raise ValueError("interval_minutes must be between 1 and 60.")
        if pod and not namespace:
            raise ValueError("A namespace is required when pod is specified.")
        v1 = _load_kubernetes_client()
        pods = _redis_pods_for_analysis(v1, namespace, pod, include_sentinels=False)
        if not pods:
            return _redis_tool_error(
                tool,
                "No Redis or Valkey pods found. Use find_database_pods first to confirm labels and namespace.",
                output_mode,
            )

        sections: list[str] = []
        structured_snapshots: list[dict[str, Any]] = []
        errors: list[str] = []
        for pod_item in pods:
            context = _redis_result_context(pod_item, "")
            label = f"{context['namespace']}/{context['pod']}"
            try:
                cli = _detect_redis_cli(context["namespace"], context["pod"], container)
                first, first_at, first_errors = _read_redis_snapshot(
                    context["namespace"], context["pod"], cli, context, container, port
                )
                context["runtime_role"] = str(first.get("role") or "unknown")
                time.sleep(interval * 60)
                second, second_at, second_errors = _read_redis_snapshot(
                    context["namespace"], context["pod"], cli, context, container, port
                )
                elapsed = (second_at - first_at).total_seconds()
                deltas = _redis_snapshot_delta(first, second, elapsed)
                node_snapshot = {
                    "namespace": context["namespace"],
                    "pod": context["pod"],
                    "runtime_role": context["runtime_role"],
                    "first_at": first_at.isoformat(),
                    "second_at": second_at.isoformat(),
                    "first": first,
                    "second": second,
                    "deltas": deltas,
                    "errors": first_errors + second_errors,
                }
                structured_snapshots.append(node_snapshot)
                section = [
                    f"### {label} (CLI: {cli})",
                    f"- {_redis_context_label(context)}",
                    f"- First snapshot: {first_at.isoformat()}",
                    f"- Second snapshot: {second_at.isoformat()}",
                    f"- Elapsed: {elapsed:.1f}s",
                    "",
                    "Deltas:",
                    f"- memory: {deltas['memory_bytes'] if deltas['memory_bytes'] is not None else 'unavailable'} bytes",
                    f"- fragmentation: {deltas['fragmentation'] if deltas['fragmentation'] is not None else 'unavailable'}",
                    (
                        f"- evictions: {deltas['evicted_keys'] if deltas['evicted_keys'] is not None else 'unavailable'} "
                        f"({deltas['eviction_rate_per_second']:.6g}/s)"
                        if deltas["eviction_rate_per_second"] is not None
                        else "- eviction rate: unavailable"
                    ),
                    (
                        f"- ops/sec point value change: {deltas['instantaneous_ops_per_sec'] if deltas['instantaneous_ops_per_sec'] is not None else 'unavailable'}"
                    ),
                    (
                        f"- hit ratio from hit/miss deltas: {deltas['hit_ratio_from_delta']:.4f}"
                        if deltas["hit_ratio_from_delta"] is not None
                        else "- hit ratio delta: unavailable"
                    ),
                    (
                        f"- connected clients: {deltas['connected_clients'] if deltas['connected_clients'] is not None else 'unavailable'}"
                    ),
                    "",
                    "Point-in-time INFO role was re-read at both snapshots. Only selected numeric fields and role "
                    "evidence were retained; raw CLI output and credentials were not recorded.",
                    "No changes were made; this analysis is advisory only.",
                    "",
                ]
                sections.append("\n".join(section))
                if first_errors or second_errors:
                    errors.extend(first_errors + second_errors)
            except Exception as exc:
                errors.append(f"{label}: {exc}")

        report = [f"Redis/Valkey snapshot diff: {len(sections)} pod(s) reported.", ""]
        report.extend("\n".join(sections).splitlines())
        if errors:
            report.extend(["Evidence errors:", *(f"- {error}" for error in errors), ""])
        report.append("No changes were made; this analysis is advisory only.")
        return _redis_tool_output(
            tool,
            "\n".join(report),
            output_mode,
            metadata={
                "interval_minutes": interval,
                "snapshots": structured_snapshots,
            },
        )
    except ValueError as exc:
        return _redis_tool_error(tool, str(exc), output_mode)
    except Exception as exc:
        return _redis_tool_error(tool, f"Error diffing Redis/Valkey snapshots: {exc}", output_mode)


def _redis_sentinel_snapshot(
    namespace: str,
    pod_name: str,
    cli: str,
    container: Optional[str] = None,
    port: Optional[int] = None,
) -> dict[str, Any]:
    """Read a Sentinel's masters, their replicas, and peer Sentinels."""
    errors: list[str] = []
    masters: list[dict[str, Any]] = []
    try:
        masters = _redis_records_from_payload(
            _parse_redis_response(
                _run_redis_command(namespace, pod_name, cli, ["SENTINEL", "MASTERS"], container, port)
            )
        )
    except Exception as exc:
        errors.append(f"SENTINEL MASTERS: {exc}")

    replicas: list[dict[str, Any]] = []
    peers: list[dict[str, Any]] = []
    for master in masters:
        name = _format_redis_value(_sentinel_value(master, "name")).strip()
        if not name:
            continue
        for command, destination in (
            (["SENTINEL", "REPLICAS", name], replicas),
            (["SENTINEL", "SENTINELS", name], peers),
        ):
            try:
                destination.extend(
                    _redis_records_from_payload(
                        _parse_redis_response(
                            _run_redis_command(namespace, pod_name, cli, command, container, port)
                        )
                    )
                )
            except Exception as exc:
                errors.append(f"{command[0]} {command[1]} {name}: {exc}")
    return {"masters": masters, "replicas": replicas, "peers": peers, "errors": errors}


def _format_sentinel_snapshot(snapshot: dict[str, Any]) -> list[str]:
    lines = ["Sentinel topology evidence:"]
    if not snapshot["masters"] and not snapshot["errors"]:
        lines.append("- No Sentinel masters were returned.")
    for master in snapshot["masters"]:
        name = _format_redis_value(_sentinel_value(master, "name")) or "unknown"
        ip = _format_redis_value(_sentinel_value(master, "ip")) or "unknown"
        port = _format_redis_value(_sentinel_value(master, "port")) or "unknown"
        flags = _sentinel_flags(master)
        state = "primary" if "master" in flags else ",".join(sorted(flags)) or "unknown"
        quorum = _redis_number(_sentinel_value(master, "quorum"))
        other_sentinels = _redis_number(_sentinel_value(master, "num-other-sentinels"))
        replicas_count = _redis_number(_sentinel_value(master, "num-slaves"))
        lines.append(
            f"- Master `{name}`: {state} at {ip}:{port}; quorum={quorum if quorum is not None else 'unknown'}; "
            f"other_sentinels={other_sentinels if other_sentinels is not None else 'unknown'}; "
            f"replicas={replicas_count if replicas_count is not None else 'unknown'}"
        )
        if "sdown" in flags or "odown" in flags:
            lines.append(f"  - Subjective/objective failure flags are present: {','.join(sorted(flags))}.")
        if quorum is not None and other_sentinels is not None:
            needed_others = max(0, int(quorum) - 1)
            if other_sentinels < needed_others:
                lines.append(
                    f"  - Only {int(other_sentinels)} other Sentinel(s) are visible; quorum {int(quorum)} is not currently reachable."
                )
            else:
                lines.append(f"  - Quorum {int(quorum)} appears reachable from this Sentinel's view.")
        if "failover-in-progress" in flags:
            lines.append("  - A failover is currently in progress.")
    if snapshot["replicas"]:
        lines.append("- Sentinel replica observations:")
        for replica in snapshot["replicas"][:20]:
            name = _format_redis_value(_sentinel_value(replica, "name")) or "unknown"
            address = f"{_format_redis_value(_sentinel_value(replica, 'ip'))}:{_format_redis_value(_sentinel_value(replica, 'port'))}"
            flags = _sentinel_flags(replica)
            lines.append(f"  - `{name}` at {address}: {','.join(sorted(flags)) or 'unknown'}")
            if "sdown" in flags or "odown" in flags or "disconnected" in flags:
                lines.append("    - Failure or disconnect flag is present; this can lead to failover.")
    if snapshot["peers"]:
        lines.append(f"- Peer Sentinel observations: {len(snapshot['peers'])} sentinel(s) returned.")
    if snapshot["errors"]:
        lines.extend(["- Sentinel evidence errors:", *(f"  - {error}" for error in snapshot["errors"])])
    return lines


def _format_cluster_shards(evidence: dict[str, Any]) -> list[str]:
    cluster_info = evidence["cluster_info"]
    shards = evidence.get("shards", [])
    lines = ["Cluster topology evidence:"]
    state = _redis_info_value(cluster_info, "cluster_state")
    assigned = _redis_number(_redis_info_value(cluster_info, "cluster_slots_assigned"))
    failed = _redis_number(_redis_info_value(cluster_info, "cluster_slots_fail"))
    known = _redis_number(_redis_info_value(cluster_info, "cluster_known_nodes"))
    size = _redis_number(_redis_info_value(cluster_info, "cluster_size"))
    lines.append(
        f"- CLUSTER INFO: cluster_state={state or 'unknown'}, assigned_slots={assigned if assigned is not None else 'unknown'}, "
        f"failed_slots={failed if failed is not None else 'unknown'}, known_nodes={known if known is not None else 'unknown'}, "
        f"cluster_size={size if size is not None else 'unknown'}"
    )
    lines.append(f"- CLUSTER MYID: {evidence.get('cluster_id') or 'unknown'} (current pod node ID).")
    if state and str(state).lower() != "ok":
        lines.append(f"- cluster_state={state}; this cluster is not fully healthy.")
    if failed and failed > 0:
        lines.append(f"- {int(failed)} slot(s) are marked failed.")
    if evidence.get("cluster_id"):
        attributed = [
            (shard_index, node)
            for shard_index, shard in enumerate(shards, 1)
            for node in shard["nodes"]
            if str(node.get("node-id") or node.get("id") or "") == evidence["cluster_id"]
        ]
        if attributed:
            shard_index, node = attributed[0]
            lines.append(
                f"- Current pod is attributed to shard {shard_index} as {node.get('role') or 'unknown'}."
            )
    if shards:
        lines.append("- CLUSTER SHARDS layout:")
        for shard_index, shard in enumerate(shards, 1):
            slots = ", ".join(f"{start}-{end}" if start != end else str(start) for start, end in shard["slots"])
            node_parts = []
            for node in shard["nodes"]:
                role = str(node.get("role") or "unknown")
                node_id = str(node.get("node-id") or node.get("id") or "unknown")
                marker = " <-- this pod" if evidence.get("cluster_id") and node_id == evidence["cluster_id"] else ""
                node_parts.append(f"{node_id} ({role}, {node.get('endpoint') or 'endpoint unknown'}){marker}")
            lines.append(f"  - Shard {shard_index}: slots={slots or 'unknown'}; nodes: {'; '.join(node_parts)}")
        lines.append(
            "- Per-shard scanning note: this report is bounded to the node(s) discovered; "
            "cluster client health and keyspace-dependent behavior require checking every shard master."
        )
    return lines


def _redis_client_report_section(
    pod_item: client.V1Pod,
    evidence: dict[str, Any],
    cli: str,
) -> str:
    context = evidence["context"]
    metrics = evidence["metrics"]
    label = f"{context['namespace']}/{context['pod']}"
    section = [f"### {label} (CLI: {cli})", f"- {_redis_context_label(context)}"]
    section.append("- Runtime role was re-read from INFO replication during this report.")
    section.append(f"- Runtime role: {context.get('runtime_role') or 'unknown'}")
    section.append(f"- Cluster mode: {'yes' if evidence.get('cluster_mode') else 'no or unavailable'}")

    connected = _redis_number(_redis_info_value(metrics, "connected_clients"))
    maxclients = _redis_number(_redis_info_value(metrics, "maxclients"))
    blocked = _redis_number(_redis_info_value(metrics, "blocked_clients"))
    section.extend(["", "Connection evidence (INFO clients/CLIENT LIST):"])
    section.append(f"- connected_clients: {connected if connected is not None else 'unavailable'}")
    section.append(f"- maxclients: {maxclients if maxclients is not None else 'unavailable'}")
    if connected is not None and maxclients and maxclients > 0:
        used_ratio = connected / maxclients
        headroom = int(maxclients - connected)
        section.append(f"- maxclients headroom: {headroom} connections ({used_ratio * 100:.1f}% used)")
        if used_ratio >= 0.9:
            section.append("- WARNING: maxclients headroom is at or below 10%; connection churn or a leak may exhaust it.")
    else:
        section.append("- maxclients headroom: unavailable; both connected_clients and maxclients are needed.")
    section.append(f"- blocked_clients: {blocked if blocked is not None else 'unavailable'}")
    section.append(f"- CLIENT LIST records parsed: {len(evidence.get('records', []))}")
    section.append(f"- CLIENT ID: {evidence.get('client_id') if evidence.get('client_id') is not None else 'unavailable'}")

    findings = _client_findings(evidence.get("records", []))
    unnamed = sum(1 for record in evidence.get("records", []) if not _client_record_value(record, "name"))
    monitor_count = sum(1 for record in evidence.get("records", []) if "O" in str(_client_record_value(record, "flags") or "").upper())
    subscription_total = sum(
        sum(int(_redis_number(_client_record_value(record, field)) or 0) for field in ("sub", "psub", "ssub"))
        for record in evidence.get("records", [])
    )
    section.append(f"- Unnamed connections: {unnamed}")
    section.append(f"- Clients in MONITOR mode: {monitor_count}")
    section.append(f"- Total subscriptions across clients: {subscription_total}")
    if evidence.get("errors"):
        section.extend(["Evidence errors:", *(f"- {error}" for error in evidence.get("errors", []))])

    buffer_limits = evidence.get("buffer_limits", {})
    if buffer_limits:
        section.extend(["", "client-output-buffer-limit configuration:"])
        for client_class in sorted(buffer_limits):
            value = buffer_limits[client_class]
            section.append(
                f"- {client_class}: hard={value['hard_limit']}, soft={value['soft_limit']}, soft_seconds={value['soft_seconds']}"
            )

    section.extend(["", "Replication evidence and health:"])
    replication_lines = _redis_replication_findings(metrics, context.get("runtime_role") or "unknown")
    section.extend(f"- {line}" for line in replication_lines)

    if evidence.get("cluster_mode"):
        section.extend([""])
        section.extend(_format_cluster_shards(evidence))

    section.extend(["", "Offending client groups (ranked by buffer/impact):"])
    if findings:
        for index, group in enumerate(findings, 1):
            identity = f"{group['kind']} `{group['identity']}`"
            section.append(
                f"- {index}. {identity}: {len(group['ids'])} connection(s); "
                f"max qbuf={group['max_qbuf']}, omem={group['max_omem']}, age={group['max_age']}s, idle={group['max_idle']}s."
            )
            section.append(f"  - IDs: {', '.join(group['ids'][:10])}{'...' if len(group['ids']) > 10 else ''}")
            if group["names"]:
                section.append(f"  - Name(s): {', '.join(sorted(group['names']))}.")
            for reason in group["reasons"]:
                section.append(f"  - {reason}.")
    else:
        section.append("- No individual client crossed the configured qbuf, omem, long-idle, subscription, MONITOR, or blocked thresholds.")

    section.extend(
        [
            "",
            "Likely application-side causes to investigate:",
            "- Large qbuf/omem often means a slow consumer, oversized pub/sub fan-out, network/backpressure, "
            "or a client that is not reading responses.",
            "- Long-lived idle connections often point to leaked pools or missing keepalive/idle close policy.",
            "- Use `CLIENT SETNAME` per service and release channel (for example, orders-worker) so address-only evidence maps to an owner.",
            "- MONITOR can consume CPU and add latency; it should not be left enabled by an application or debug process.",
            "- Blocked clients can be normal for BLPOP/BRPOP/XREAD-style consumers, but unexpected blocking can stall worker capacity.",
        ]
    )
    section.extend(
        [
            "",
            "Recommendations are advisory and evidence-linked; they have not been applied.",
            "No changes were made; this analysis is advisory only.",
            "",
        ]
    )
    return "\n".join(section)


def get_redis_client_report(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    container: Optional[str] = None,
    port: Optional[int] = None,
    output_mode: str = "human",
) -> str:
    """Report Redis/Valkey client health and topology-aware role evidence.

    Uses only read-only CLIENT, INFO, CONFIG GET, CLUSTER, and SENTINEL
    diagnostics. Roles are read again while the report runs; findings are not
    inferred from Kubernetes role labels.
    """
    tool = "get_redis_client_report"
    try:
        output_mode = _redis_output_mode(output_mode)
    except ValueError as exc:
        return _redis_tool_error(tool, str(exc), "human")
    if pod and not namespace:
        return _redis_tool_error(
            tool,
            "A namespace is required when pod is specified.",
            output_mode,
        )
    try:
        v1 = _load_kubernetes_client()
        all_pods = _redis_pods_for_analysis(v1, namespace, pod)
        pods = [candidate for candidate in all_pods if not _redis_pod_is_sentinel(candidate)]
        sentinel_pods = [
            candidate
            for candidate in all_pods
            if _redis_pod_is_sentinel(candidate) and (not pod or candidate.metadata.name == pod)
        ]
    except Exception as exc:
        return _redis_tool_error(
            tool,
            f"Error discovering or reading Redis/Valkey pods: {exc}",
            output_mode,
        )
    if not pods and not sentinel_pods:
        return _redis_tool_error(
            tool,
            "No Redis, Valkey, or Sentinel pods found. Use find_database_pods first "
            "to confirm the labels and namespace.",
            output_mode,
        )

    sections: list[str] = []
    errors: list[str] = []
    server_evidence: list[tuple[str, dict[str, Any]]] = []
    for pod_item in pods:
        context = _redis_result_context(pod_item, "")
        label = f"{context['namespace']}/{context['pod']}"
        try:
            cli = _detect_redis_cli(context["namespace"], context["pod"], container)
            evidence = _read_redis_client_evidence(
                context["namespace"], context["pod"], cli, context, container, port
            )
            server_evidence.append((label, evidence))
            sections.append(_redis_client_report_section(pod_item, evidence, cli))
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    sentinel_sections: list[str] = []
    for pod_item in sentinel_pods:
        context = _redis_result_context(pod_item, "")
        label = f"{context['namespace']}/{context['pod']}"
        try:
            cli = _detect_redis_cli(context["namespace"], context["pod"], container)
            snapshot = _redis_sentinel_snapshot(context["namespace"], context["pod"], cli, container, port)
            sentinel_sections.append(
                "\n".join(
                    [
                        f"### Sentinel {label} (CLI: {cli})",
                        f"- {_redis_context_label(context)}",
                        *(_format_sentinel_snapshot(snapshot)),
                        "",
                    ]
                )
            )
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    result = [f"Redis/Valkey client and topology analysis: {len(sections)} node(s) reported.", ""]
    result.extend("\n".join(sections).splitlines())
    if sentinel_sections:
        result.extend(["Sentinel-managed topology:", *"\n".join(sentinel_sections).splitlines()])

    replication_ids = [
        str(_redis_info_value(evidence.get("metrics", {}), "master_replid"))
        for _, evidence in server_evidence
        if _redis_info_value(evidence.get("metrics", {}), "master_replid")
    ]
    if replication_ids and len(set(replication_ids)) > 1:
        if any(evidence.get("cluster_mode") for _, evidence in server_evidence):
            note = (
                "Distinct master_replid values were observed. In cluster mode this can be expected across shards; "
                "compare within each shard's primary/replica set."
            )
        else:
            note = "Multiple master_replid values were observed in a non-cluster layout; investigate replication identity or failover history."
        result.extend(["Replication identity notes:", f"- {note}", ""])
    elif replication_ids:
        result.extend(["Replication identity notes:", f"- master_replid values observed: {', '.join(sorted(set(replication_ids)))}.", ""])

    if errors:
        result.extend(["Skipped pods:", *(f"- {error}" for error in errors), ""])
    result.append("No changes were made; this analysis is advisory only.")
    return _redis_tool_output(
        tool,
        "\n".join(result),
        output_mode,
        metadata={"namespace": namespace, "pod": pod, "nodes_reported": len(sections)},
    )


def _parse_config_get_value(output: str, name: str) -> Optional[str]:
    payload = _parse_redis_response(output)
    wanted = name.lower()
    if isinstance(payload, dict):
        candidate = payload.get(wanted) or payload.get(name)
        return _format_redis_value(candidate) if candidate is not None else None
    if isinstance(payload, list):
        if len(payload) == 2 and _format_redis_value(payload[0]).lower() == wanted:
            return _format_redis_value(payload[1])
        if payload and isinstance(payload[0], (list, tuple)):
            for pair in payload:
                if (
                    isinstance(pair, (list, tuple))
                    and len(pair) == 2
                    and _format_redis_value(pair[0]).lower() == wanted
                ):
                    return _format_redis_value(pair[1])
    return None


def _redis_record_from_value(value: Any) -> Optional[dict[str, Any]]:
    """Normalize a Redis map or alternating RESP key/value record."""
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, (list, tuple)) or not value:
        return None
    if all(isinstance(item, (list, tuple)) and len(item) == 2 for item in value):
        return {_format_redis_value(item[0]): item[1] for item in value}
    if len(value) % 2 == 0 and not any(
        isinstance(item, (list, tuple, dict)) for item in value
    ):
        return {_format_redis_value(value[index]): value[index + 1] for index in range(0, len(value), 2)}
    return None


def _redis_records_from_payload(payload: Any) -> list[dict[str, Any]]:
    """Normalize a list of maps/RESP records from CLIENT LIST or Sentinel."""
    if payload is None:
        return []
    if isinstance(payload, dict):
        return [payload]
    if not isinstance(payload, (list, tuple)):
        return []
    if not payload:
        return []
    records = [_redis_record_from_value(item) for item in payload]
    if all(record is not None for record in records):
        return records
    single = _redis_record_from_value(payload)
    return [single] if single else []


def _parse_client_list(output: str) -> tuple[list[dict[str, Any]], Optional[str]]:
    """Parse CLIENT LIST JSON, RESP bulk output, or newline key/value rows."""
    try:
        payload = _parse_redis_response(output)
        records = _redis_records_from_payload(payload)
    except Exception as exc:
        return [], f"CLIENT LIST response could not be parsed: {exc}"
    if records:
        return records, None

    text = _format_redis_value(payload)
    records = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            parts = shlex.split(line, posix=True)
        except ValueError as exc:
            return [], f"CLIENT LIST response could not be parsed: {exc}"
        record: dict[str, Any] = {}
        for part in parts:
            if "=" not in part:
                return [], "CLIENT LIST response is missing a key=value field."
            key, value = part.split("=", 1)
            record[key.strip().lower()] = value
        if record:
            records.append(record)
    if not records:
        return [], "CLIENT LIST response was empty or malformed."
    return records, None


def _client_record_value(record: dict[str, Any], name: str) -> Any:
    for key, value in record.items():
        if str(key).lower() == name:
            return value
    return None


def _parse_output_buffer_limit(output: str) -> tuple[dict[str, dict[str, int]], Optional[str]]:
    value = _parse_config_get_value(output, "client-output-buffer-limit")
    if value is None:
        return {}, "client-output-buffer-limit is unavailable."
    if not isinstance(value, str):
        value = _format_redis_value(value)
    try:
        parts = [int(part) for part in value.split()]
    except (TypeError, ValueError):
        return {}, f"client-output-buffer-limit response was malformed: {value}"
    if len(parts) < 3 or len(parts) % 3:
        return {}, f"client-output-buffer-limit response was malformed: {value}"
    limits = {
        parts[index].lower(): {
            "hard_limit": parts[index + 1],
            "soft_limit": parts[index + 2],
            "soft_seconds": parts[index + 3],
        }
        for index in range(0, len(parts), 3)
    }
    return limits, None


def _client_findings(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        name = str(_client_record_value(record, "name") or "")
        address = str(_client_record_value(record, "addr") or "unknown")
        key = (name, "name") if name else (address, "address")
        group = grouped.setdefault(
            key,
            {
                "identity": name or address,
                "kind": "connection-name" if name else "address",
                "addresses": set(),
                "names": set(),
                "ids": [],
                "max_qbuf": 0,
                "max_omem": 0,
                "max_age": 0,
                "max_idle": 0,
                "subscriptions": 0,
                "monitor": False,
                "blocked": False,
                "unnamed": False,
            },
        )
        group["addresses"].add(address)
        if name:
            group["names"].add(name)
        else:
            group["unnamed"] = True
        group["ids"].append(str(_client_record_value(record, "id") or "unknown"))

        for source, field in (
            ("qbuf", "max_qbuf"),
            ("omem", "max_omem"),
            ("age", "max_age"),
            ("idle", "max_idle"),
        ):
            number = _redis_number(_client_record_value(record, source))
            if number is not None:
                group[field] = max(int(group[field]), int(number))

        subs = sum(
            int(_redis_number(_client_record_value(record, field)) or 0)
            for field in ("sub", "psub", "ssub")
            if _redis_number(_client_record_value(record, field)) is not None
        )
        group["subscriptions"] = max(int(group["subscriptions"]), subs)
        flags = str(_client_record_value(record, "flags") or "").upper()
        group["monitor"] = group["monitor"] or "O" in flags
        group["blocked"] = group["blocked"] or "B" in flags

    findings: list[dict[str, Any]] = []
    for group in grouped.values():
        reasons: list[str] = []
        if group["max_qbuf"] >= _REDIS_CLIENT_QBUF_BYTES:
            reasons.append(
                f"qbuf reached {group['max_qbuf']} bytes; input is arriving faster than commands are processed"
            )
        if group["max_omem"] >= _REDIS_CLIENT_OMEM_BYTES:
            reasons.append(
                f"omem reached {group['max_omem']} bytes; output is queued faster than the client reads it"
            )
        if group["max_age"] >= _REDIS_CLIENT_LONG_AGE_SECONDS and group["max_idle"] >= _REDIS_CLIENT_IDLE_SECONDS:
            reasons.append(
                f"a connection is {group['max_age']}s old with up to {group['max_idle']}s idle"
            )
        if group["subscriptions"] >= _REDIS_CLIENT_HIGH_SUBSCRIPTIONS:
            reasons.append(f"up to {group['subscriptions']} channel/pattern subscriptions")
        if group["monitor"]:
            reasons.append("a client is stuck in MONITOR mode")
        if group["blocked"]:
            reasons.append("a client is blocked in a blocking command")
        if group["unnamed"]:
            reasons.append("one or more connections have no CLIENT SETNAME")
        if reasons:
            impact = (
                group["max_omem"] * 2
                + group["max_qbuf"]
                + (1000000 if group["monitor"] else 0)
                + (500000 if group["blocked"] else 0)
                + group["subscriptions"] * 100
                + group["max_idle"]
            )
            findings.append({**group, "reasons": reasons, "impact": impact})
    findings.sort(key=lambda item: (item["impact"], len(item["ids"])), reverse=True)
    return findings


def _parse_cluster_info(output: str) -> dict[str, Any]:
    return _parse_info_sections(output)


def _parse_cluster_id(output: str) -> Optional[str]:
    payload = _parse_redis_response(output)
    if payload is None:
        return None
    value = _format_redis_value(payload).strip().strip('"')
    return value or None


def _parse_cluster_slots(value: Any) -> list[tuple[int, int]]:
    numbers = value if isinstance(value, (list, tuple)) else []
    flat: list[int] = []
    for item in numbers:
        number = _redis_number(item)
        if number is not None:
            flat.append(int(number))
    return [(flat[index], flat[index + 1]) for index in range(0, len(flat) - 1, 2)]


def _parse_cluster_shards(output: str) -> tuple[list[dict[str, Any]], Optional[str]]:
    try:
        payload = _parse_redis_response(output)
        shards = _redis_records_from_payload(payload)
    except Exception as exc:
        return [], f"CLUSTER SHARDS response could not be parsed: {exc}"
    if not shards:
        return [], "CLUSTER SHARDS response was empty or malformed."
    normalized = []
    for shard in shards:
        nodes = shard.get("nodes", [])
        if not isinstance(nodes, (list, tuple)):
            nodes = [nodes]
        parsed_nodes = []
        for node in nodes:
            parsed = _redis_record_from_value(node)
            if parsed:
                endpoint = parsed.get("endpoint")
                port = parsed.get("port")
                if isinstance(endpoint, (list, tuple)):
                    endpoint_text = _format_redis_value(endpoint[:1]) if endpoint else ""
                else:
                    endpoint_text = _format_redis_value(endpoint)
                parsed["endpoint"] = f"{endpoint_text}:{port}" if endpoint_text else ""
                parsed_nodes.append(parsed)
        slots = _parse_cluster_slots(shard.get("slots", []))
        normalized.append({"slots": slots, "nodes": parsed_nodes})
    if not any(shard["nodes"] for shard in normalized):
        return [], "CLUSTER SHARDS response contained no usable node records."
    return normalized, None


def _parse_cluster_nodes(output: str) -> tuple[list[dict[str, str]], Optional[str]]:
    text = _format_redis_value(_parse_redis_response(output))
    nodes = []
    for line in text.splitlines():
        if not line.strip():
            continue
        fields = line.split()
        if len(fields) < 8:
            return [], "CLUSTER NODES response was malformed."
        flags = set(fields[2].split(","))
        role = "master" if "master" in flags else "replica" if "slave" in flags else "unknown"
        nodes.append({"node_id": fields[0], "endpoint": fields[1], "role": role})
    if not nodes:
        return [], "CLUSTER NODES response was empty."
    return nodes, None


def _sentinel_value(record: dict[str, Any], name: str) -> Any:
    for key, value in record.items():
        if str(key).lower() == name:
            return value
    return None


def _sentinel_flags(record: dict[str, Any]) -> set[str]:
    raw = _format_redis_value(_sentinel_value(record, "flags") or "")
    return {part.strip().lower() for part in raw.split(",") if part.strip()}


def _parse_config_get_max_len(output: str) -> Optional[int]:
    value = _parse_config_get_value(output, "slowlog-max-len")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_latency_history(output: str) -> list[dict[str, Any]]:
    payload = _parse_redis_response(output)
    if not payload:
        return []
    if isinstance(payload, list) and len(payload) == 2 and not any(
        isinstance(value, list) for value in payload
    ):
        payload = [payload]
    if not isinstance(payload, list):
        return []
    events = []
    for entry in payload:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        try:
            timestamp = int(_format_redis_value(entry[0]))
            duration_ms = float(_format_redis_value(entry[1]))
        except (TypeError, ValueError):
            continue
        events.append({"timestamp": timestamp, "duration_ms": duration_ms})
    return events


def _redis_slowlog_snapshot(
    namespace: str,
    pod_name: str,
    cli: str,
    context: dict[str, str],
    container: Optional[str] = None,
    port: Optional[int] = None,
) -> dict[str, Any]:
    try:
        raw_entries = _parse_slowlog_entries(
            _run_redis_command(
                namespace,
                pod_name,
                cli,
                ["SLOWLOG", "GET", "25"],
                container,
                port,
            )
        )
        entries = [
            parsed
            for parsed in (_parse_slowlog_entry(entry, context) for entry in raw_entries)
            if parsed is not None
        ]
        slowlog_len = _parse_integer_response(
            _run_redis_command(namespace, pod_name, cli, ["SLOWLOG", "LEN"], container, port)
        )
        max_len = _parse_config_get_max_len(
            _run_redis_command(
                namespace,
                pod_name,
                cli,
                ["CONFIG", "GET", "slowlog-max-len"],
                container,
                port,
            )
        )
        saturated = slowlog_len is not None and max_len is not None and max_len > 0 and slowlog_len == max_len
        return {"entries": entries, "length": slowlog_len, "max_len": max_len, "saturated": saturated, "error": None}
    except Exception as exc:
        return {"entries": [], "length": None, "max_len": None, "saturated": False, "error": str(exc)}


def _redis_latency_correlations(
    histories: dict[str, list[dict[str, Any]]],
    metrics: dict[str, Any],
    slowlog: dict[str, Any],
) -> list[str]:
    correlations: list[str] = []
    def event_count(event: str) -> int:
        return len(histories.get(event, []))

    fork_usec = _redis_number(_redis_info_value(metrics, "latest_fork_usec"))
    if event_count("fork"):
        context = f" and latest_fork_usec={int(fork_usec)}" if fork_usec is not None else ""
        correlations.append(f"Fork history is present{context}; compare with save/rewrite timing before attribution.")
    evicted = _redis_number(_redis_info_value(metrics, "evicted_keys"))
    maxmemory = _redis_number(_redis_info_value(metrics, "maxmemory"))
    if event_count("eviction-cycle") or event_count("eviction-del"):
        if evicted is not None and evicted > 0 and maxmemory == 0:
            correlations.append("Eviction history plus evicted_keys>0 suggests pressure despite maxmemory=0 (a cluster-limit case).")
        elif evicted is not None and evicted > 0:
            correlations.append("Eviction history and evicted_keys>0 align with a memory-pressure/eviction path.")
        else:
            correlations.append("Eviction history exists, but current evicted_keys does not prove it was recent.")
    expired = _redis_number(_redis_info_value(metrics, "expired_keys"))
    if event_count("expire-cycle"):
        if expired is not None:
            correlations.append(
                f"Expire-cycle history is present while INFO stats reports expired_keys={int(expired)}."
            )
        else:
            correlations.append("Expire-cycle history is present, but expired_keys evidence is unavailable.")
    ops = _redis_number(_redis_info_value(metrics, "instantaneous_ops_per_sec"))
    if event_count("command"):
        if ops is not None:
            correlations.append(
                f"Command latency history is present at instantaneous_ops_per_sec={ops:g}."
            )
        else:
            correlations.append("Command latency history is present, but the current command rate is unavailable.")
    if (event_count("rdb-save") or event_count("aof-fsync")) and slowlog.get("entries"):
        families = sorted({_redis_command_family(entry.command) for entry in slowlog["entries"]})
        correlations.append(
            "Persistence latency history and slow-log families "
            f"({', '.join(families)}) are both present; compare timestamps before attributing command delay."
        )
    return correlations


def get_redis_latency_report(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    container: Optional[str] = None,
    port: Optional[int] = None,
    output_mode: str = "human",
) -> str:
    """Correlate LATENCY evidence with memory, persistence, command, and slowlog data."""
    tool = "get_redis_latency_report"
    try:
        output_mode = _redis_output_mode(output_mode)
    except ValueError as exc:
        return _redis_tool_error(tool, str(exc), "human")
    if pod and not namespace:
        return _redis_tool_error(
            tool,
            "A namespace is required when pod is specified.",
            output_mode,
        )
    try:
        v1 = _load_kubernetes_client()
        pods = _redis_pods_for_analysis(v1, namespace, pod)
    except Exception as exc:
        return _redis_tool_error(
            tool,
            f"Error discovering or reading Redis/Valkey pods: {exc}",
            output_mode,
        )
    if not pods:
        return _redis_tool_error(
            tool,
            "No Redis or Valkey pods found. Use find_database_pods first "
            "to confirm the labels and namespace.",
            output_mode,
        )

    sections: list[str] = []
    errors: list[str] = []
    for pod_item in pods:
        context = _redis_result_context(pod_item, "")
        label = f"{context['namespace']}/{context['pod']}"
        try:
            cli = _detect_redis_cli(context["namespace"], context["pod"], container)
            evidence = _read_redis_evidence(
                context["namespace"], context["pod"], cli, context, container, port
            )
            metrics = evidence["metrics"]
            histories: dict[str, list[dict[str, Any]]] = {}
            latency_errors: list[str] = []
            for event_class in sorted(_REDIS_LATENCY_EVENT_CLASSES):
                try:
                    histories[event_class] = _parse_latency_history(
                        _run_redis_command(
                            context["namespace"],
                            context["pod"],
                            cli,
                            ["LATENCY", "HISTORY", event_class],
                            container,
                            port,
                        )
                    )
                except Exception as exc:
                    latency_errors.append(f"LATENCY HISTORY {event_class}: {exc}")
            try:
                doctor_output = _run_redis_command(
                    context["namespace"], context["pod"], cli, ["LATENCY", "DOCTOR"], container, port
                )
                doctor_value = _parse_redis_response(doctor_output)
                doctor = doctor_value.strip() if isinstance(doctor_value, str) else str(doctor_value or "")
            except Exception as exc:
                doctor = ""
                latency_errors.append(f"LATENCY DOCTOR: {exc}")
            try:
                io_threads_output = _run_redis_command(
                    context["namespace"], context["pod"], cli, ["CONFIG", "GET", "io-threads"], container, port
                )
                io_threads_text = _parse_config_get_value(io_threads_output, "io-threads")
                io_threads = int(io_threads_text) if io_threads_text is not None else None
                io_threads_error = ""
            except Exception as exc:
                io_threads = None
                io_threads_error = str(exc)

            slowlog = _redis_slowlog_snapshot(
                context["namespace"], context["pod"], cli, context, container, port
            )
            if slowlog.get("error"):
                latency_errors.append(f"SLOWLOG: {slowlog['error']}")

            section = [f"### {label} (CLI: {cli})", f"- {_redis_context_label(context)}"]
            section.extend(_format_redis_evidence(metrics, evidence["errors"]))
            section.extend(["", "LATENCY HISTORY evidence:"])
            has_events = False
            for event_class in sorted(_REDIS_LATENCY_EVENT_CLASSES):
                events = histories.get(event_class, [])
                if events:
                    has_events = True
                    latest = max(events, key=lambda item: item["timestamp"])
                    logged_at = datetime.fromtimestamp(latest["timestamp"], timezone.utc).isoformat()
                    section.append(
                        f"- {event_class}: {len(events)} event(s); latest {latest['duration_ms']:.3f}ms at {logged_at}"
                    )
                else:
                    section.append(f"- {event_class}: no events")
            if not has_events:
                section.append("- No latency events were recorded in the queried classes.")
            section.extend(["", "LATENCY DOCTOR evidence:"])
            if doctor:
                section.extend(["```text", doctor, "```"])
            else:
                section.append("- unavailable or empty")
            if latency_errors:
                section.extend(["Latency evidence errors:", *(f"- {error}" for error in latency_errors)])

            section.extend(["", "Slow-log correlation:"])
            if slowlog.get("error"):
                section.append(f"- unavailable: {slowlog['error']}")
            else:
                entries = slowlog["entries"]
                section.append(f"- {len(entries)} slow-log entr(ies); length/max: {slowlog['length']}/{slowlog['max_len']}")
                if slowlog.get("saturated"):
                    section.append("- Slow log is saturated; older entries may have been evicted.")
                if entries:
                    family_summary: dict[str, dict[str, float]] = {}
                    for entry in entries:
                        family = _redis_command_family(entry.command)
                        summary = family_summary.setdefault(family, {"count": 0, "total_ms": 0.0})
                        summary["count"] += 1
                        summary["total_ms"] += entry.duration_ms
                    ordered = sorted(family_summary.items(), key=lambda item: item[1]["total_ms"], reverse=True)
                    section.append(
                        "- Families: "
                        + ", ".join(
                            f"{name} ({data['count']} entries, {data['total_ms']:.3f}ms)"
                            for name, data in ordered
                        )
                    )

            section.extend(["", "Event-loop and Valkey IO-thread context:"])
            if io_threads is None:
                if io_threads_error:
                    section.append(f"- io-threads: unavailable ({io_threads_error})")
                else:
                    section.append("- io-threads: unavailable")
            elif io_threads <= 1:
                section.append("- io-threads=1: client I/O and command execution share one event loop.")
            else:
                section.append(
                    f"- io-threads={io_threads}: I/O may be distributed, but command work and event-loop "
                    "attribution still require workload evidence; this value does not prove a cause."
                )

            correlations = _redis_latency_correlations(histories, metrics, slowlog)
            section.extend(["", "Evidence-linked correlations:"])
            if correlations:
                section.extend(f"- {item}" for item in correlations)
            else:
                section.append("- None proven by the available evidence.")
            section.extend(
                [
                    "",
                    "Recommendations are advisory and evidence-linked; they have not been applied.",
                    "No changes were made; this analysis is advisory only.",
                    "",
                ]
            )
            sections.append("\n".join(section))
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    result = [f"Redis/Valkey latency forensics: {len(sections)} pod(s) reported.", ""]
    result.extend("\n".join(sections).splitlines())
    if errors:
        result.extend(["Skipped pods:", *(f"- {error}" for error in errors), ""])
    return _redis_tool_output(
        tool,
        "\n".join(result),
        output_mode,
        metadata={"namespace": namespace, "pod": pod, "pods_reported": len(sections)},
    )


def get_redis_slowlog(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    count: int = 25,
    min_duration_ms: Optional[float] = None,
    container: Optional[str] = None,
    port: Optional[int] = None,
    output_mode: str = "human",
) -> str:
    """Analyze Redis/Valkey slow-log entries using read-only commands only.

    This tool runs `SLOWLOG GET`, `SLOWLOG LEN`, and
    `CONFIG GET slowlog-max-len`. Credentials must already exist in the
    container environment (`REDISCLI_AUTH` or `VALKEYCLI_AUTH`); they are never
    added to argv by this tool.
    """
    tool = "get_redis_slowlog"
    try:
        output_mode = _redis_output_mode(output_mode)
    except ValueError as exc:
        return _redis_tool_error(tool, str(exc), "human")
    if pod and not namespace:
        return _redis_tool_error(
            tool,
            "A namespace is required when pod is specified.",
            output_mode,
        )
    try:
        requested_count = int(count)
    except (TypeError, ValueError):
        return _redis_tool_error(tool, "Invalid count: use an integer between 1 and 128.", output_mode)
    if requested_count < 1 or requested_count > 128:
        return _redis_tool_error(tool, "Invalid count: use an integer between 1 and 128.", output_mode)
    if min_duration_ms is not None:
        try:
            threshold = float(min_duration_ms)
        except (TypeError, ValueError):
            return _redis_tool_error(
                tool,
                "Invalid min_duration_ms: use a non-negative number of milliseconds.",
                output_mode,
            )
        if threshold < 0:
            return _redis_tool_error(
                tool,
                "Invalid min_duration_ms: use a non-negative number of milliseconds.",
                output_mode,
            )
    else:
        threshold = 0.0

    try:
        v1 = _load_kubernetes_client()
        if pod:
            pods = [v1.read_namespaced_pod(name=pod, namespace=namespace)]
        else:
            if namespace:
                pod_list = v1.list_namespaced_pod(namespace=namespace)
            else:
                pod_list = v1.list_pod_for_all_namespaces()
            pods = [
                candidate
                for candidate in pod_list.items
                if _pod_matches_database_type(candidate, "redis")
                or _pod_matches_database_type(candidate, "valkey")
            ]

        if not pods:
            return _redis_tool_error(
                tool,
                "No Redis or Valkey pods found. Use find_database_pods first "
                "to confirm the labels and namespace.",
                output_mode,
            )

        contexts: list[dict[str, str]] = []
        entries: list[RedisSlowLogEntry] = []
        errors: list[str] = []
        saturated_pods: list[str] = []

        for pod_item in pods:
            context = _redis_result_context(pod_item, "")
            try:
                cli = _detect_redis_cli(
                    context["namespace"],
                    context["pod"],
                    container,
                )
                context["cli"] = cli
                raw_entries = _parse_slowlog_entries(
                    _run_redis_command(
                        context["namespace"],
                        context["pod"],
                        cli,
                        ["SLOWLOG", "GET", str(requested_count)],
                        container,
                        port,
                    )
                )
                parsed_entries = [
                    parsed
                    for parsed in (_parse_slowlog_entry(entry, context) for entry in raw_entries)
                    if parsed is not None
                ]
                slowlog_len = _parse_integer_response(
                    _run_redis_command(
                        context["namespace"],
                        context["pod"],
                        cli,
                        ["SLOWLOG", "LEN"],
                        container,
                        port,
                    )
                )
                max_len = _parse_config_get_max_len(
                    _run_redis_command(
                        context["namespace"],
                        context["pod"],
                        cli,
                        ["CONFIG", "GET", "slowlog-max-len"],
                        container,
                        port,
                    )
                )
                if (
                    slowlog_len is not None
                    and max_len is not None
                    and max_len > 0
                    and slowlog_len == max_len
                ):
                    saturated_pods.append(_redis_context_label(context))
                entries.extend(parsed_entries)
                contexts.append(context)
            except Exception as exc:
                errors.append(f"{context['namespace']}/{context['pod']}: {exc}")

        filtered = [entry for entry in entries if entry.duration_ms >= threshold]
        filtered.sort(
            key=lambda entry: (
                entry.duration_ms,
                entry.logged_at or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True,
        )

        if not filtered:
            result = [
                f"No Redis/Valkey slow-log entries over {threshold:g}ms were found.",
            ]
            if contexts:
                result.extend(
                    f"- {_redis_context_label(context)} (CLI: {context['cli'] or 'unknown'})"
                    for context in contexts
                )
            if saturated_pods:
                result.extend(
                    [
                        "",
                        "Slow-log saturation detected:",
                        *(f"- {label}" for label in saturated_pods),
                    ]
                )
            if errors:
                result.extend(["", "Skipped pods:", *(f"- {error}" for error in errors)])
            result.extend(
                [
                    "",
                    "No changes were made; this analysis is advisory only.",
                ]
            )
            return _redis_tool_output(tool, "\n".join(result), output_mode)

        family_summary: dict[str, dict[str, float]] = {}
        for entry in filtered:
            family = _redis_command_family(entry.command)
            summary = family_summary.setdefault(family, {"count": 0, "total_ms": 0.0})
            summary["count"] += 1
            summary["total_ms"] += entry.duration_ms
        ordered_families = sorted(
            family_summary.items(),
            key=lambda item: (item[1]["total_ms"], item[1]["count"]),
            reverse=True,
        )

        result = [
            (
                f"Redis/Valkey slow-log analysis (up to {requested_count} entries per pod; "
                f"threshold >= {threshold:g}ms): {len(filtered)} entries."
            ),
            "Command families by impact: "
            + ", ".join(
                f"{family} ({summary['count']} entries, {summary['total_ms']:.3f}ms total)"
                for family, summary in ordered_families
            ),
            "",
        ]
        for index, entry in enumerate(filtered, 1):
            logged_at = entry.logged_at.isoformat() if entry.logged_at else "unknown"
            result.extend(
                [
                    f"### {index}. {entry.namespace}/{entry.pod} (CLI: {entry.cli or 'unknown'})",
                ]
            )
            labels = []
            if entry.role:
                labels.append(f"role={entry.role}")
            if entry.topology:
                labels.append(entry.topology)
            if labels:
                result.append(f"- Topology: {'; '.join(labels)}")
            result.extend(
                [
                    f"- Entry ID: {entry.entry_id}",
                    f"- Logged at: {logged_at}",
                    f"- Duration: {entry.duration_ms:.3f}ms",
                    f"- Command: `{entry.command}`",
                ]
            )
            if entry.client:
                result.append(f"- Client: {entry.client}")
            if entry.client_name:
                result.append(f"- Client name: {entry.client_name}")
            if entry.client_info:
                result.append(f"- Client info: {entry.client_info}")
            result.append("")

        if saturated_pods:
            result.extend(
                [
                    "Slow-log saturation detected:",
                    *(f"- {label}" for label in saturated_pods),
                    "",
                ]
            )
        if errors:
            result.extend(["Skipped pods:", *(f"- {error}" for error in errors), ""])
        result.extend(
            [
                "Recommendations are advisory only. Review command families and workload "
                "patterns before changing slow-log or database configuration.",
                "No changes were made; this analysis is advisory only.",
            ]
        )
        return _redis_tool_output(tool, "\n".join(result), output_mode)
    except Exception as exc:
        return _redis_tool_error(tool, f"Error reading Redis/Valkey slow log: {exc}", output_mode)


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
    operation: str = "mysql_readonly_query",
) -> str:
    return _pod_exec(
        namespace,
        pod_name,
        ["/bin/sh", "-c", command],
        container,
        operation,
        timeout_seconds=30,
    )


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


def _redis_memory_quantity(value: object) -> Optional[int]:
    """Parse a Kubernetes memory quantity into bytes without relying on a shell."""
    text = str(value or "").strip()
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([kmgtp]i?b?)", text, re.IGNORECASE)
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2).lower()
    multipliers = {
        "": 1,
        "b": 1,
        "k": 1_000,
        "kb": 1_000,
        "ki": 1_024,
        "kib": 1_024,
        "m": 1_000_000,
        "mb": 1_000_000,
        "mi": 1_048_576,
        "mib": 1_048_576,
        "g": 1_000_000_000,
        "gb": 1_000_000_000,
        "gi": 1_073_741_824,
        "gib": 1_073_741_824,
        "t": 1_000_000_000_000,
        "tb": 1_000_000_000_000,
        "ti": 1_099_511_627_776,
        "tib": 1_099_511_627_776,
        "p": 1_000_000_000_000_000,
        "pb": 1_000_000_000_000_000,
        "pi": 1_125_899_906_842_624,
        "pib": 1_125_899_906_842_624,
    }
    multiplier = multipliers.get(unit)
    if multiplier is None:
        return None
    return int(number * multiplier)


def _redis_container_memory_limit(
    pod: client.V1Pod,
    selected_container: Optional[str] = None,
) -> tuple[Optional[int], str]:
    """Return the database container's memory limit and its raw value."""
    containers = list(getattr(pod.spec, "containers", None) or [])
    if not containers:
        return None, "absent (no containers)"
    selected = None
    if selected_container:
        selected = next((item for item in containers if item.name == selected_container), None)
    else:
        matching = [
            item
            for item in containers
            if "redis" in str(item.image or "").lower()
            or "valkey" in str(item.image or "").lower()
            or "redis" in str(item.name or "").lower()
            or "valkey" in str(item.name or "").lower()
        ]
        selected = matching[0] if len(matching) == 1 else (containers[0] if len(containers) == 1 else None)
    if selected is None:
        return None, f"ambiguous across {len(containers)} containers"
    limits = dict(getattr(getattr(selected, "resources", None), "limits", None) or {})
    memory_value = next((value for name, value in limits.items() if str(name).lower() == "memory"), None)
    label = selected.name or "database"
    if memory_value is None:
        return None, f"absent for container {label}"
    return _redis_memory_quantity(memory_value), f"{memory_value} on container {label}"


def _format_redis_bytes(value: Optional[float]) -> str:
    if value is None:
        return "unknown"
    value = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    scaled = value
    unit_index = 0
    while scaled >= 1024 and unit_index < len(units) - 1:
        scaled /= 1024
        unit_index += 1
    return f"{scaled:.1f}{units[unit_index]}"


def _redis_command_tokens(command: str) -> list[str]:
    try:
        return shlex.split(command, posix=True)
    except ValueError:
        return command.split()


def _redis_bigkey_match(command: str, findings: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    tokens = _redis_command_tokens(command)
    if len(tokens) < 2:
        return None
    key = tokens[1]
    return next((finding for finding in findings if str(finding.get("key")) == key), None)


def _redis_last_command_histogram(records: list[dict[str, Any]]) -> dict[str, int]:
    histogram: dict[str, int] = {}
    for record in records:
        command = str(_client_record_value(record, "cmd") or "").strip().upper()
        if command:
            histogram[command] = histogram.get(command, 0) + 1
    return histogram


def _read_redis_remediation_bigkeys(
    namespace: str,
    pod_name: str,
    cli: str,
    container: Optional[str],
    sample_interval_seconds: float,
) -> tuple[list[dict[str, Any]], Optional[int], list[str]]:
    """Run the existing SCAN-based CLI mode; this tool never issues manual SCAN or KEYS."""
    errors: list[str] = []
    try:
        interval_text = f"{sample_interval_seconds:.3f}".rstrip("0").rstrip(".")
        argv = [cli, "--bigkeys"]
        if interval_text:
            argv.extend(["-i", interval_text])
        output = _pod_exec(
            namespace,
            pod_name,
            argv,
            container,
            "redis_scan_based_key_analysis",
            timeout_seconds=120,
        )
        findings, sampled = _parse_bigkeys_output(output)
        return findings, sampled, errors
    except Exception as exc:
        errors.append(f"--bigkeys: {exc}")
        return [], None, errors


def _read_redis_remediation_evidence(
    namespace: str,
    pod_name: str,
    cli: str,
    context: dict[str, str],
    container: Optional[str] = None,
    port: Optional[int] = None,
    include_bigkeys: bool = True,
    sample_interval_seconds: float = 0.01,
) -> dict[str, Any]:
    """Collect existing diagnostic views into one evidence snapshot for recommendations."""
    slowlog = _redis_slowlog_snapshot(namespace, pod_name, cli, context, container, port)
    memory_evidence = _read_redis_evidence(namespace, pod_name, cli, context, container, port)
    client_evidence = _read_redis_client_evidence(namespace, pod_name, cli, context, container, port)

    metrics = memory_evidence["metrics"]
    metrics.update(client_evidence["metrics"])
    context["runtime_role"] = str(_redis_info_value(metrics, "role") or "unknown")

    histories: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = list(memory_evidence["errors"])
    errors.extend(client_evidence["errors"])
    if slowlog.get("error"):
        errors.append(f"SLOWLOG: {slowlog['error']}")
    for event_class in sorted(_REDIS_LATENCY_EVENT_CLASSES):
        try:
            histories[event_class] = _parse_latency_history(
                _run_redis_command(namespace, pod_name, cli, ["LATENCY", "HISTORY", event_class], container, port)
            )
        except Exception as exc:
            errors.append(f"LATENCY HISTORY {event_class}: {exc}")
    doctor = ""
    try:
        doctor_value = _parse_redis_response(
            _run_redis_command(namespace, pod_name, cli, ["LATENCY", "DOCTOR"], container, port)
        )
        doctor = doctor_value.strip() if isinstance(doctor_value, str) else str(doctor_value or "")
    except Exception as exc:
        errors.append(f"LATENCY DOCTOR: {exc}")
    io_threads: Optional[int] = None
    try:
        io_threads_text = _parse_config_get_value(
            _run_redis_command(
                namespace,
                pod_name,
                cli,
                ["CONFIG", "GET", "io-threads"],
                container,
                port,
            ),
            "io-threads",
        )
        io_threads = int(io_threads_text) if io_threads_text is not None else None
    except Exception as exc:
        errors.append(f"CONFIG GET io-threads: {exc}")

    bigkeys: list[dict[str, Any]] = []
    bigkeys_sampled: Optional[int] = None
    if include_bigkeys:
        bigkeys, bigkeys_sampled, bigkeys_errors = _read_redis_remediation_bigkeys(
            namespace,
            pod_name,
            cli,
            container,
            sample_interval_seconds,
        )
        errors.extend(bigkeys_errors)

    return {
        "cli": cli,
        "context": context,
        "metrics": metrics,
        "slowlog": slowlog,
        "latency_histories": histories,
        "doctor": doctor,
        "io_threads": io_threads,
        "bigkeys": bigkeys,
        "bigkeys_sampled": bigkeys_sampled,
        "client_records": client_evidence.get("records", []),
        "buffer_limits": client_evidence.get("buffer_limits", {}),
        "cluster_info": client_evidence.get("cluster_info", {}),
        "cluster_mode": bool(client_evidence.get("cluster_mode")),
        "cluster_id": client_evidence.get("cluster_id"),
        "shards": client_evidence.get("shards", []),
        "errors": errors,
    }


def _redis_remediation_command_recommendations(evidence: dict[str, Any]) -> list[RedisRemediation]:
    recommendations: list[RedisRemediation] = []
    slowlog = evidence.get("slowlog", {})
    entries = list(slowlog.get("entries") or [])
    grouped: dict[str, list[RedisSlowLogEntry]] = {}
    for entry in entries:
        grouped.setdefault(_redis_command_family(entry.command), []).append(entry)

    def summary(entries_: list[RedisSlowLogEntry]) -> str:
        maximum = max((entry.duration_ms for entry in entries_), default=0.0)
        commands = ", ".join(f"`{entry.command}`" for entry in entries_[:3])
        return f"SLOWLOG GET 25 returned {len(entries_)} entr(ies); max duration {maximum:.3f}ms; commands: {commands}"

    keys_entries = grouped.get("KEYS", [])
    if keys_entries:
        recommendations.append(
            RedisRemediation(
                "command-pattern",
                "Replace KEYS with cursor-paginated SCAN",
                "high",
                (
                    summary(keys_entries),
                    "The selected node received blocking full-keyspace iteration.",
                ),
                "Change callers to iterate with SCAN cursors, MATCH/COUNT bounds, retry-on-restart handling, and "
                "inter-cursor throttling. Do not add a manual SCAN to this diagnostic tool.",
                "SCAN is incremental but not a point-in-time snapshot and can span multiple round trips; "
                "throttle it to avoid replacing one blocking command with sustained load.",
            )
        )

    bigkeys = list(evidence.get("bigkeys") or [])
    collection_families = {"HGETALL", "SMEMBERS", "LRANGE", "ZRANGE"}
    for family in sorted(collection_families & set(grouped)):
        entries_ = grouped[family]
        matched = next((finding for entry in entries_ if (finding := _redis_bigkey_match(entry.command, bigkeys))), None)
        if max(entry.duration_ms for entry in entries_) < _REDIS_REMEDIATION_MIN_SLOW_MS and matched is None:
            continue
        suffix = ""
        if matched:
            suffix = (
                f"; matching {matched['key_type']} key `{matched['key']}` has "
                f"{matched['size']} {matched['size_unit']}"
            )
        alternatives = []
        if family in {"HGETALL", "SMEMBERS"}:
            alternatives = ["HSCAN for hashes", "SSCAN for sets"]
        elif family == "ZRANGE":
            alternatives = ["ZRANGE with LIMIT", "ZSCAN for full iteration"]
        else:
            alternatives = ["LRANGE with explicit positive start/stop windows"]
        recommendations.append(
            RedisRemediation(
                "command-pattern",
                f"Bound {family} collection reads",
                "high" if matched else "medium",
                (summary(entries_), f"The command was {family} (arguments were observed in the slow log){suffix}."),
                f"Replace or augment unbounded reads with {', '.join(alternatives)}; paginate by a workload-tested count.",
                "Cursor iteration or paging adds code complexity and may see a moving dataset; too-small pages "
                "increase round trips, so validate latency and client memory under real traffic.",
            )
        )

    del_entries = grouped.get("DEL", [])
    if del_entries:
        matched = next((finding for entry in del_entries if (finding := _redis_bigkey_match(entry.command, bigkeys))), None)
        if matched or max(entry.duration_ms for entry in del_entries) >= _REDIS_REMEDIATION_MIN_SLOW_MS:
            match_text = ""
            if matched:
                match_text = (
                    f"; matching {matched['key_type']} key `{matched['key']}` has "
                    f"{matched['size']} {matched['size_unit']}"
                )
            recommendations.append(
                RedisRemediation(
                    "command-pattern",
                    "Use UNLINK for large deletions",
                    "high" if matched else "medium",
                    (summary(del_entries), f"SLOWLOG GET 25 observed DEL command(s){match_text}."),
                    "Change the deleting caller from DEL to UNLINK so value reclamation can occur in background work.",
                    "UNLINK can still create blocking work under extreme conditions and delayed reclamation can keep "
                    "resident memory high; verify replication and eviction behavior during a measured rollout.",
                )
            )

    records = list(evidence.get("client_records") or [])
    histogram = _redis_last_command_histogram(records)
    if histogram:
        family, count = max(histogram.items(), key=lambda item: item[1])
        if count >= _REDIS_REMEDIATION_TOP_COMMAND_COUNT and count / sum(histogram.values()) >= 0.6:
            recommendations.append(
                RedisRemediation(
                    "command-pattern",
                    "Evaluate client-side pipelining or batching",
                    "medium",
                    (
                        f"CLIENT LIST: {len(records)} records were parsed; last-command histogram was "
                        f"{family}={count} of {sum(histogram.values())}.",
                    ),
                    "Group repeated independent operations into bounded client pipelines or multi-item commands, "
                    "then confirm application semantics and retry behavior.",
                    "Pipelines can improve round-trip cost but can also amplify failures, buffer output on the server, "
                    "and reorder or complicate error handling; cap pipeline depth and test against real latency.",
                )
            )
    return recommendations


def _redis_remediation_memory_recommendations(evidence: dict[str, Any]) -> list[RedisRemediation]:
    recommendations: list[RedisRemediation] = []
    metrics = evidence.get("metrics", {})
    used = _redis_number(_redis_info_value(metrics, "used_memory"))
    used_human = _redis_info_value(metrics, "used_memory_human")
    maxmemory = _redis_number(_redis_info_value(metrics, "maxmemory"))
    policy = str(_redis_info_value(metrics, "maxmemory_policy") or "")
    evicted = _redis_number(_redis_info_value(metrics, "evicted_keys"))
    expired = _redis_number(_redis_info_value(metrics, "expired_keys"))
    limit_bytes, limit_text = evidence.get("pod_memory_limit", (None, "absent"))
    if maxmemory == 0:
        guidance = (
            "Set maxmemory to a load-tested value below the container limit, leaving room for fragmentation, "
            "replication, copy-on-write, and server overhead. A common starting review range is 50-70%."
        )
        if limit_bytes:
            guidance += f" The observed container limit is {_format_redis_bytes(limit_bytes)}."
        recommendations.append(
            RedisRemediation(
                "memory",
                "Right-size maxmemory against the pod limit",
                "high",
                (
                    f"INFO memory maxmemory=0; used_memory={_format_redis_bytes(used)}"
                    + (f" ({used_human})" if used_human else ""),
                    f"Kubernetes container memory limit: {limit_text}.",
                ),
                guidance,
                "A limit that is too low causes evictions or write failures; one that is too close to the pod limit "
                "can trigger OOMKill during fragmentation or copy-on-write. Validate with peak traffic and persistence.",
            )
        )
    elif maxmemory is not None and limit_bytes and maxmemory > limit_bytes * 0.8:
        recommendations.append(
            RedisRemediation(
                "memory",
                "Increase maxmemory-to-pod headroom",
                "high",
                (
                    f"INFO memory maxmemory={_format_redis_bytes(maxmemory)}; "
                    f"Kubernetes memory limit={_format_redis_bytes(limit_bytes)} ({limit_text}).",
                ),
                "After load testing, lower maxmemory or increase the container limit so fragmentation and fork "
                "copy-on-write have headroom; prefer a measured 30-50% reserve rather than a universal constant.",
                "Raising a container limit consumes cluster capacity; lowering maxmemory can cause earlier eviction "
                "or write failures. Both changes can affect latency and availability.",
            )
        )

    if (
        maxmemory is not None
        and maxmemory > 0
        and used is not None
        and used / maxmemory >= 0.9
    ):
        recommendations.append(
            RedisRemediation(
                "memory",
                "Recover logical memory headroom",
                "high",
                (
                    f"INFO memory used_memory={_format_redis_bytes(used)} is "
                    f"{used / maxmemory * 100:.1f}% of maxmemory={_format_redis_bytes(maxmemory)}.",
                ),
                "Audit large/transient keys and TTL ownership before changing limits. Use the big-key evidence to "
                "target oversized structures, then size maxmemory from peak demand.",
                "Deleting or expiring keys changes application-visible data; adding capacity defers pressure and "
                "can mask a leak. Validate ownership and rollback before changing either.",
            )
        )

    if evicted is not None and evicted > 0:
        policy_tradeoff = "with noeviction, failures rather than evictions are expected"
        recommendations.append(
            RedisRemediation(
                "memory",
                "Choose an intentional eviction policy",
                "high" if policy == "noeviction" else "medium",
                (
                    f"INFO stats evicted_keys={int(evicted)}; INFO memory maxmemory_policy={policy or 'unavailable'}.",
                ),
                "Confirm which data may be evicted. If a cache can tolerate it, evaluate allkeys-lfu or allkeys-lru "
                "with metrics; if durability is required, add capacity or move that data outside Redis. "
                f"Note: {policy_tradeoff}.",
                "Changing policy can silently discard hot or durable-looking data; allkeys-random has no recency "
                "signal, and volatile policies skip keys without TTL. Test hit/miss and application behavior.",
            )
        )

    if policy.startswith("volatile-") or (expired is not None and expired >= _REDIS_REMEDIATION_EXPIRED_KEYS):
        policy_evidence = f"INFO memory maxmemory_policy={policy}" if policy else "maxmemory_policy unavailable"
        expired_evidence = f"INFO stats expired_keys={int(expired)}" if expired is not None else "expired_keys unavailable"
        recommendations.append(
            RedisRemediation(
                "memory",
                "Audit TTL hygiene",
                "medium",
                (policy_evidence, expired_evidence),
                "Inventory transient keys, assign deliberate TTL/JITTER where safe, and ensure durable keys are "
                "excluded from volatile eviction paths.",
                "TTLs change lifetime semantics; too short causes misses and repopulation, too long preserves "
                "pressure. This snapshot does not directly measure the share of keys without TTL.",
            )
        )

    if policy.startswith("allkeys-random"):
        recommendations.append(
            RedisRemediation(
                "memory",
                "Replace random eviction if recency matters",
                "medium",
                (f"INFO memory maxmemory_policy={policy}",),
                "Consider allkeys-lru/allkeys-lfu after workload review, or isolate mutable data into another service.",
                "LRU/LFU retain more bookkeeping and can still discard data; verify access patterns and cache semantics.",
            )
        )

    fragmentation = _redis_number(_redis_info_value(metrics, "mem_fragmentation_ratio"))
    if fragmentation is None:
        fragmentation = _redis_number(_redis_info_value(metrics, "fragmentation"))
    fragmentation_bytes = _redis_number(_redis_info_value(metrics, "fragmentation.bytes"))
    allocator = str(_redis_info_value(metrics, "mem_allocator") or "")
    defrag_running = _redis_number(_redis_info_value(metrics, "activedefrag_running"))
    if fragmentation is not None and (fragmentation >= 1.5 or fragmentation < 1):
        direction = "high" if fragmentation >= 1.5 else "low"
        evidence_values = [f"fragmentation ratio={fragmentation:g}"]
        if fragmentation_bytes is not None:
            evidence_values.append(f"MEMORY STATS fragmentation.bytes={_format_redis_bytes(fragmentation_bytes)}")
        if allocator:
            evidence_values.append(f"mem_allocator={allocator}")
        if defrag_running is not None:
            evidence_values.append(f"activedefrag_running={int(defrag_running)}")
        if direction == "high" and allocator.lower() == "jemalloc":
            recommendations.append(
                RedisRemediation(
                    "memory",
                    "Evaluate active defragmentation",
                    "medium",
                    tuple(evidence_values),
                    "If the version and build support it, enable active defragmentation on one replica/canary with "
                    "conservative CPU limits, then compare latency and RSS before fleet rollout.",
                    "Active defragmentation can add CPU and event-loop work and may not pay off if allocation churn "
                    "continues. It is not a substitute for right-sized data.",
                )
            )
        else:
            approach = (
                "Do not rely on active defragmentation with this allocator; first review allocation churn, "
                "container memory limits, node memory pressure, and swap behavior."
                if direction == "high"
                else "Investigate swapping, compressed/shared pages, or host memory pressure before tuning defragmentation."
            )
            recommendations.append(
                RedisRemediation(
                    "memory",
                    "Investigate fragmentation evidence",
                    "medium",
                    tuple(evidence_values),
                    approach,
                    "Fragmentation diagnostics are point-in-time. Restarting or reallocating capacity can reduce RSS "
                    "temporarily but may fail over traffic or lose an in-memory dataset if persistence is absent.",
                )
            )
    return recommendations


def _redis_remediation_persistence_recommendations(evidence: dict[str, Any]) -> list[RedisRemediation]:
    recommendations: list[RedisRemediation] = []
    metrics = evidence.get("metrics", {})
    histories = evidence.get("latency_histories", {})
    fork_usec = _redis_number(_redis_info_value(metrics, "latest_fork_usec"))
    fork_events = histories.get("fork", [])
    rdb_running = _redis_number(_redis_info_value(metrics, "rdb_bgsave_in_progress"))
    aof_rewrite = _redis_number(_redis_info_value(metrics, "aof_rewrite_in_progress"))
    persistence_trigger = bool(fork_events) or (
        fork_usec is not None and fork_usec >= _REDIS_REMEDIATION_FORK_USEC
    )
    role = str(evidence.get("context", {}).get("runtime_role") or "unknown")
    connected_replicas = _redis_number(_redis_info_value(metrics, "connected_slaves"))

    if persistence_trigger:
        evidence_values = []
        if fork_usec is not None:
            evidence_values.append(f"INFO persistence latest_fork_usec={int(fork_usec)}")
        if fork_events:
            evidence_values.append(f"LATENCY HISTORY fork: {len(fork_events)} event(s)")
        recommendations.append(
            RedisRemediation(
                "persistence",
                "Reduce persistence fork and copy-on-write impact",
                "high",
                tuple(evidence_values),
                "Schedule persistence off peak, provide memory headroom above peak logical usage, and consider "
                "taking backups from a replica or a managed persistence/backup facility rather than the primary.",
                "Copy-on-write can consume additional RSS during fork; offloading backup work adds replica lag and "
                "may produce a less current backup. Managed persistence changes operational and cost ownership.",
            )
        )

    if rdb_running == 1 or aof_rewrite == 1:
        current = []
        if rdb_running == 1:
            current.append("rdb_bgsave_in_progress=1")
        if aof_rewrite == 1:
            current.append("aof_rewrite_in_progress=1")
        recommendations.append(
            RedisRemediation(
                "persistence",
                "Validate work running during background saves",
                "medium",
                tuple(current),
                "Do not stack heavy scans, rewrites, or deployments on top of a background save; wait for completion "
                "and correlate any latency with the save window.",
                "This is a snapshot, not a completed duration. Delayed work can affect freshness or release timing.",
            )
        )

    aof_enabled = _redis_number(_redis_info_value(metrics, "aof_enabled"))
    rdb_status = str(_redis_info_value(metrics, "rdb_last_bgsave_status") or "").lower()
    aof_status = str(_redis_info_value(metrics, "aof_last_write_status") or "").lower()
    if aof_enabled == 0:
        recommendations.append(
            RedisRemediation(
                "persistence",
                "Review the RDB recovery window",
                "medium",
                ("INFO persistence aof_enabled=0.",),
                "If the acceptable data-loss window is shorter than the RDB save interval, evaluate AOF or a "
                "managed persistence option; otherwise document that RDB-only loss window as accepted.",
                "AOF generally narrows data loss but adds disk I/O/fsync latency, file-rewrite fork/COW cost, and "
                "storage management. RDB is smaller/faster but loses changes since the last snapshot.",
            )
        )
    if rdb_status and rdb_status not in {"ok", "none"}:
        recommendations.append(
            RedisRemediation(
                "persistence",
                "Resolve RDB save failures",
                "high",
                (f"INFO persistence rdb_last_bgsave_status={rdb_status}.",),
                "Check disk space, permissions, and replica-side backup capacity; restore a known-good persistence "
                "path before relying on snapshots.",
                "Retrying without capacity can worsen latency; disabling persistence trades recoverability for load.",
            )
        )
    if aof_enabled == 1 and aof_status and aof_status not in {"ok", "none"}:
        recommendations.append(
            RedisRemediation(
                "persistence",
                "Resolve AOF write or rewrite failures",
                "high",
                (f"INFO persistence aof_last_write_status={aof_status}.",),
                "Check disk capacity, fsync health, and AOF rewrite headroom. Keep durability available before "
                "changing fsync policy.",
                "Relaxing fsync improves latency by accepting more data loss; rewriting under pressure can add "
                "fork/COW and I/O overhead.",
            )
        )

    if persistence_trigger:
        if role in {"replica", "slave"}:
            recommendations.append(
                RedisRemediation(
                    "persistence",
                    "Prefer this replica for backup reads",
                    "medium",
                    (
                        f"Runtime role was re-read as {role}.",
                        *(evidence_values or ()),
                    ),
                    "Evaluate backups or persistence-heavy work from this replica after confirming lag, read load, "
                    "and failure isolation.",
                    "Backups can lag and consume replica CPU/network, reducing stale-read tolerance and failover "
                    "warmness.",
                )
            )
        else:
            replica_evidence = (
                f"INFO replication connected_slaves={int(connected_replicas)}."
                if connected_replicas is not None
                else "INFO replication connected_slaves unavailable."
            )
            recommendation_text = (
                "Consider a replica-based backup path for these workloads."
                if connected_replicas is not None and connected_replicas > 0
                else "A replica currently appears unavailable from this evidence; do not create one implicitly as a "
                "backup target without capacity and failover review."
            )
            recommendations.append(
                RedisRemediation(
                    "persistence",
                    "Move backup pressure off the primary",
                    "medium",
                    (f"Runtime role was re-read as {role}.", replica_evidence, *(evidence_values or ())),
                    recommendation_text,
                    "Adding a replica costs memory, network, and failover complexity; it can also expose stale data "
                    "during lag. A managed persistence option shifts that operational burden but may add cost and "
                    "provider lock-in.",
                )
            )
    return recommendations


def _redis_remediation_client_recommendations(evidence: dict[str, Any]) -> list[RedisRemediation]:
    recommendations: list[RedisRemediation] = []
    records = list(evidence.get("client_records") or [])
    metrics = evidence.get("metrics", {})
    connected = _redis_number(_redis_info_value(metrics, "connected_clients"))
    maxclients = _redis_number(_redis_info_value(metrics, "maxclients"))
    blocked = _redis_number(_redis_info_value(metrics, "blocked_clients"))
    buffer_limits = evidence.get("buffer_limits", {})

    problematic = _client_findings(records)
    buffer_group = next((group for group in problematic if group["max_qbuf"] >= _REDIS_CLIENT_QBUF_BYTES), None)
    output_group = next((group for group in problematic if group["max_omem"] >= _REDIS_CLIENT_OMEM_BYTES), None)
    if buffer_group:
        recommendations.append(
            RedisRemediation(
                "client",
                "Control input backpressure for offending clients",
                "high",
                (
                    f"CLIENT LIST: connection {buffer_group['kind']} `{buffer_group['identity']}` reported "
                    f"max qbuf={buffer_group['max_qbuf']} bytes.",
                ),
                "Find the owning service with CLIENT SETNAME, reduce request burst size, use bounded queues/backpressure, "
                "and confirm the client consumes responses promptly. Do not kill clients from this tool.",
                "Killing or throttling can interrupt work and lose in-flight context; relaxing input handling can "
                "transfer pressure to the application queue.",
            )
        )
    if output_group:
        output_limits = []
        hard_limit = None
        for class_name in ("normal", "replica", "pubsub"):
            value = buffer_limits.get(class_name)
            if value:
                output_limits.append(
                    f"{class_name} hard={value['hard_limit']}, soft={value['soft_limit']}, "
                    f"soft_seconds={value['soft_seconds']}"
                )
                if class_name == "normal":
                    hard_limit = _redis_number(value["hard_limit"])
        evidence_values = [
            f"CLIENT LIST: connection {output_group['kind']} `{output_group['identity']}` reported "
            f"max omem={output_group['max_omem']} bytes."
        ]
        if output_limits:
            evidence_values.append(f"CONFIG GET client-output-buffer-limit: {'; '.join(output_limits)}.")
        severity = "high" if hard_limit is None or output_group["max_omem"] >= hard_limit * 0.1 else "medium"
        recommendations.append(
            RedisRemediation(
                "client",
                "Fix slow consumers and output buffering",
                severity,
                tuple(evidence_values),
                "Instrument the client, unblock reads, reduce oversized responses/fan-out, and enforce bounded "
                "output. Treat buffer-limit changes as a last resort after finding the backpressure source.",
                "Raising limits can increase server memory and delay failure detection; fixing the consumer can "
                "change delivery semantics and requires error/retry handling.",
            )
        )

    idle_records = [
        record
        for record in records
        if (_redis_number(_client_record_value(record, "age")) or 0) >= _REDIS_CLIENT_LONG_AGE_SECONDS
        and (_redis_number(_client_record_value(record, "idle")) or 0) >= _REDIS_CLIENT_IDLE_SECONDS
    ]
    if idle_records:
        record = max(
            idle_records,
            key=lambda item: (
                _redis_number(_client_record_value(item, "age")) or 0,
                _redis_number(_client_record_value(item, "idle")) or 0,
            ),
        )
        recommendations.append(
            RedisRemediation(
                "client",
                "Fix idle or leaked connection pools",
                "medium",
                (
                    "CLIENT LIST: id="
                    f"{_client_record_value(record, 'id')} name={_client_record_value(record, 'name') or 'unnamed'} "
                    f"age={int(_redis_number(_client_record_value(record, 'age')) or 0)}s "
                    f"idle={int(_redis_number(_client_record_value(record, 'idle')) or 0)}s.",
                ),
                "Review pool min/idle/max settings, connection close paths, health checks, and deployment rolling "
                "behavior; fix leaks at the owner rather than killing connections from diagnostics.",
                "Aggressive idle closes can churn connections and lose prepared state; pool limits may mask an "
                "upstream bottleneck.",
            )
        )

    unnamed = sum(1 for record in records if not _client_record_value(record, "name"))
    if unnamed:
        recommendations.append(
            RedisRemediation(
                "client",
                "Name client connections for ownership",
                "low",
                (f"CLIENT LIST: {unnamed} of {len(records)} parsed connection(s) have no name.",),
                "Set stable CLIENT SETNAME values per service, pod, and deployment role in each client library.",
                "Names are diagnostic metadata only, but highly cardinal names can make aggregate triage harder "
                "and require client-library rollout.",
            )
        )

    if blocked is not None and blocked > 0:
        recommendations.append(
            RedisRemediation(
                "client",
                "Confirm blocking-command consumers",
                "medium",
                (f"INFO clients blocked_clients={int(blocked)}.",),
                "Map blocking clients to services and confirm BLPOP/BRPOP/XREAD-style behavior, timeouts, and "
                "consumer concurrency are intended.",
                "Removing blocking behavior can turn steady consumers into polling loops; adding consumers can "
                "increase memory and contention.",
            )
        )

    if connected is not None and maxclients is not None and maxclients > 0 and connected / maxclients >= 0.9:
        recommendations.append(
            RedisRemediation(
                "client",
                "Reduce connection churn or raise maxclients safely",
                "high",
                (
                    f"INFO clients connected_clients={int(connected)}; maxclients={int(maxclients)} "
                    f"({connected / maxclients * 100:.1f}% used).",
                ),
                "First tune pools, keepalive/idle close behavior, and fan-out per service. Only then evaluate a "
                "tested maxclients increase and pod resource impact.",
                "More file descriptors and memory increase resource use; pool changes can reduce concurrency and "
                "queue work in clients.",
            )
        )
    return recommendations


def _redis_remediation_topology_recommendations(evidence: dict[str, Any]) -> list[RedisRemediation]:
    recommendations: list[RedisRemediation] = []
    cluster_info = evidence.get("cluster_info", {})
    state = str(_redis_info_value(cluster_info, "cluster_state") or "").lower()
    failed = _redis_number(_redis_info_value(cluster_info, "cluster_slots_fail"))
    if state and state != "ok":
        recommendations.append(
            RedisRemediation(
                "topology",
                "Stabilize cluster health before performance tuning",
                "high",
                (
                    f"CLUSTER INFO cluster_state={state}; cluster_slots_fail={int(failed) if failed is not None else 'unavailable'}.",
                    "Do not attribute normal command latency to tuning while slot coverage is degraded.",
                ),
                "Diagnose failed/offline nodes and slot coverage with the cluster operator before command-pattern or "
                "memory tuning.",
                "Topology repair can move primaries and traffic; it must be coordinated to avoid failover storms "
                "or client redirection errors.",
            )
        )
    return recommendations


def _redis_remediations_from_evidence(evidence: dict[str, Any]) -> list[RedisRemediation]:
    recommendations: list[RedisRemediation] = []
    recommendations.extend(_redis_remediation_command_recommendations(evidence))
    recommendations.extend(_redis_remediation_memory_recommendations(evidence))
    recommendations.extend(_redis_remediation_persistence_recommendations(evidence))
    recommendations.extend(_redis_remediation_client_recommendations(evidence))
    recommendations.extend(_redis_remediation_topology_recommendations(evidence))
    return recommendations


def _format_redis_remediation_evidence(evidence: dict[str, Any]) -> list[str]:
    metrics = evidence.get("metrics", {})
    limit_bytes, limit_text = evidence.get("pod_memory_limit", (None, "absent"))
    sampled = evidence.get("bigkeys_sampled")
    records = len(evidence.get("client_records") or [])
    lines = [
        "Evidence sources:",
        "- SLOWLOG GET 25, SLOWLOG LEN, and CONFIG GET slowlog-max-len.",
        "- INFO memory/stats/persistence/replication and MEMORY STATS.",
        "- LATENCY HISTORY for known event classes plus LATENCY DOCTOR and CONFIG GET io-threads.",
        "- CLIENT LIST, CLIENT ID, INFO clients, CONFIG GET client-output-buffer-limit, and cluster topology reads.",
        f"- SCAN-based --bigkeys sampling: {'yes' if sampled is not None else 'not available'} "
        f"(keys sampled: {sampled if sampled is not None else 'unknown'}).",
        f"- Kubernetes memory limit: {limit_text} "
        f"({_format_redis_bytes(limit_bytes) if limit_bytes is not None else 'unavailable'}).",
        f"- CLIENT LIST records parsed: {records}.",
    ]
    cluster_info = evidence.get("cluster_info", {})
    if cluster_info:
        lines.append(
            "- Cluster mode: "
            f"{'yes' if evidence.get('cluster_mode') else 'no'}; cluster_state="
            f"{_redis_info_value(cluster_info, 'cluster_state') or 'unavailable'}."
        )
    histories = evidence.get("latency_histories", {})
    with_events = [name for name, events in histories.items() if events]
    if with_events:
        lines.append(f"- Latency histories with events: {', '.join(with_events)}.")
    if evidence.get("errors"):
        lines.extend(["Evidence gaps/errors:", *(f"- {error}" for error in evidence.get("errors", []))])
    return lines


def get_redis_remediation_recommendations(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    container: Optional[str] = None,
    port: Optional[int] = None,
    include_bigkeys: bool = True,
    sample_interval_seconds: float = 0.01,
    output_mode: str = "human",
) -> str:
    """Produce evidence-linked, advisory Redis/Valkey remediation recommendations.

    Uses only the existing read-only Redis diagnostics and the SCAN-based CLI
    big-key mode. It never changes Redis/Valkey state or Kubernetes resources.
    """
    tool = "get_redis_remediation_recommendations"
    try:
        output_mode = _redis_output_mode(output_mode)
    except ValueError as exc:
        return _redis_tool_error(tool, str(exc), "human")
    if pod and not namespace:
        return _redis_tool_error(
            tool,
            "A namespace is required when pod is specified.",
            output_mode,
        )
    try:
        interval = float(sample_interval_seconds)
    except (TypeError, ValueError):
        return _redis_tool_error(
            tool,
            "Invalid sample_interval_seconds: use a value between 0 and 1.",
            output_mode,
        )
    if interval < 0 or interval > 1:
        return _redis_tool_error(
            tool,
            "Invalid sample_interval_seconds: use a value between 0 and 1.",
            output_mode,
        )

    try:
        v1 = _load_kubernetes_client()
        all_pods = _redis_pods_for_analysis(v1, namespace, pod)
        pods = [candidate for candidate in all_pods if not _redis_pod_is_sentinel(candidate)]
        sentinel_pods = [
            candidate
            for candidate in all_pods
            if _redis_pod_is_sentinel(candidate) and (not pod or candidate.metadata.name == pod)
        ]
    except Exception as exc:
        return _redis_tool_error(
            tool,
            f"Error discovering or reading Redis/Valkey pods: {exc}",
            output_mode,
        )
    if not pods and not sentinel_pods:
        return _redis_tool_error(
            tool,
            "No Redis, Valkey, or Sentinel pods found. Use find_database_pods first "
            "to confirm the labels and namespace.",
            output_mode,
        )

    sections: list[str] = []
    errors: list[str] = []
    sentinel_sections: list[str] = []
    for pod_item in pods:
        context = _redis_result_context(pod_item, "")
        label = f"{context['namespace']}/{context['pod']}"
        try:
            cli = _detect_redis_cli(context["namespace"], context["pod"], container)
            evidence = _read_redis_remediation_evidence(
                context["namespace"],
                context["pod"],
                cli,
                context,
                container,
                port,
                include_bigkeys,
                interval,
            )
            evidence["pod_memory_limit"] = _redis_container_memory_limit(pod_item, container)
            recommendations = _redis_remediations_from_evidence(evidence)
            section = [
                f"### {label} (CLI: {cli})",
                f"- {_redis_context_label(context)}",
                *_format_redis_remediation_evidence(evidence),
                "",
                "Evidence-linked recommendations:",
            ]
            if recommendations:
                for recommendation in recommendations:
                    section.extend(recommendation.render())
            else:
                section.append(
                    "- No threshold-crossing recommendation was supported by this evidence. "
                    "No change was applied."
                )
            section.extend(
                [
                    "",
                    "All recommendations are advisory. The tool issued no remediation, mutation, or "
                    "configuration change.",
                    "No changes were made; this analysis is advisory only.",
                    "",
                ]
            )
            sections.append("\n".join(section))
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    for pod_item in sentinel_pods:
        context = _redis_result_context(pod_item, "")
        label = f"{context['namespace']}/{context['pod']}"
        try:
            cli = _detect_redis_cli(context["namespace"], context["pod"], container)
            snapshot = _redis_sentinel_snapshot(context["namespace"], context["pod"], cli, container, port)
            sentinel_sections.append(
                "\n".join(
                    [
                        f"### Sentinel {label} (CLI: {cli})",
                        f"- {_redis_context_label(context)}",
                        *(_format_sentinel_snapshot(snapshot)),
                        "",
                    ]
                )
            )
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    result = [
        "Redis/Valkey remediation recommendations (advisory only):",
        "",
        *"\n".join(sections).splitlines(),
    ]
    if sentinel_sections:
        result.extend(["Sentinel topology evidence:", *"\n".join(sentinel_sections).splitlines()])
    if errors:
        result.extend(["Skipped pods/evidence errors:", *(f"- {error}" for error in errors), ""])
    result.append("No changes were made; this analysis is advisory only.")
    return _redis_tool_output(
        tool,
        "\n".join(result),
        output_mode,
        metadata={"namespace": namespace, "pod": pod, "nodes_reported": len(sections)},
    )

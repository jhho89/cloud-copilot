from __future__ import annotations

import os
import re
import json
import logging
import shlex
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Optional
from typing import Any

from kubernetes import client, config, stream


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
            "Blocked Redis command. This tool permits only the read-only slow-log commands."
        )
    if operation == "CONFIG" and len(tokens) > 1 and tokens[1].upper() != "GET":
        raise RedisCommandBlockedError(
            "Blocked Redis command. CONFIG SET and other mutations are not permitted."
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
    if (
        operation == "CONFIG"
        and len(tokens) == 3
        and tokens[1].upper() == "GET"
        and tokens[2].lower() == "slowlog-max-len"
    ):
        return tokens

    raise RedisCommandNotAllowlistedError(
        "Redis command not allowlisted. This tool permits only SLOWLOG GET, "
        "SLOWLOG LEN, and CONFIG GET slowlog-max-len."
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


def _parse_config_get_max_len(output: str) -> Optional[int]:
    payload = _parse_redis_response(output)
    if isinstance(payload, list):
        if len(payload) == 2 and _format_redis_value(payload[0]).lower() == "slowlog-max-len":
            candidate = payload[1]
        elif payload and isinstance(payload[0], (list, tuple)):
            for pair in payload:
                if (
                    isinstance(pair, (list, tuple))
                    and len(pair) == 2
                    and _format_redis_value(pair[0]).lower() == "slowlog-max-len"
                ):
                    candidate = pair[1]
                    break
            else:
                return None
        else:
            return None
        try:
            return int(candidate)
        except (TypeError, ValueError):
            return None
    if isinstance(payload, dict):
        candidate = payload.get("slowlog-max-len")
        try:
            return int(candidate) if candidate is not None else None
        except (TypeError, ValueError):
            return None
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
    if context.get("role"):
        descriptors.append(f"role={context['role']}")
    if context.get("topology"):
        descriptors.append(context["topology"])
    if descriptors:
        label += f" ({'; '.join(descriptors)})"
    return label


def _redis_command_family(command: str) -> str:
    match = re.match(r"([A-Za-z]+)", command.strip())
    return match.group(1).upper() if match else "UNKNOWN"


def get_redis_slowlog(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    count: int = 25,
    min_duration_ms: Optional[float] = None,
    container: Optional[str] = None,
    port: Optional[int] = None,
) -> str:
    """Analyze Redis/Valkey slow-log entries using read-only commands only.

    This tool runs `SLOWLOG GET`, `SLOWLOG LEN`, and
    `CONFIG GET slowlog-max-len`. Credentials must already exist in the
    container environment (`REDISCLI_AUTH` or `VALKEYCLI_AUTH`); they are never
    added to argv by this tool.
    """
    if pod and not namespace:
        return "A namespace is required when pod is specified."
    try:
        requested_count = int(count)
    except (TypeError, ValueError):
        return "Invalid count: use an integer between 1 and 128."
    if requested_count < 1 or requested_count > 128:
        return "Invalid count: use an integer between 1 and 128."
    if min_duration_ms is not None:
        try:
            threshold = float(min_duration_ms)
        except (TypeError, ValueError):
            return "Invalid min_duration_ms: use a non-negative number of milliseconds."
        if threshold < 0:
            return "Invalid min_duration_ms: use a non-negative number of milliseconds."
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
            return (
                "No Redis or Valkey pods found. Use find_database_pods first "
                "to confirm the labels and namespace."
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
            return "\n".join(result)

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
        return "\n".join(result)
    except Exception as exc:
        return f"Error reading Redis/Valkey slow log: {exc}"
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

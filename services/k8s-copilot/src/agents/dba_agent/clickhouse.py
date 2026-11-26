from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional


CLICKHOUSE_STRUCTURED_SCHEMA_VERSION = "cloud-copilot.dba.clickhouse.v1"
_CLICKHOUSE_CONFIG_PATH = "/etc/clickhouse-client/copilot_dba.xml"
_CLICKHOUSE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_CLICKHOUSE_K8S_NAME_RE = re.compile(r"^[a-z0-9](?:[-.a-z0-9]*[a-z0-9])?$")
_CLICKHOUSE_SERVER_DATABASE_TYPES = {"clickhouse"}
_CLICKHOUSE_KEEPER_DATABASE_TYPES = {"clickhouse-keeper", "keeper"}
_CLICKHOUSE_DATABASE_TYPES = _CLICKHOUSE_SERVER_DATABASE_TYPES | _CLICKHOUSE_KEEPER_DATABASE_TYPES
_CLICKHOUSE_ALLOWED_TABLE_FUNCTIONS = {"numbers", "zeros", "values"}
_CLICKHOUSE_USER_SQL_START_RE = re.compile(r"^\s*(?:SELECT|WITH)\b", re.IGNORECASE)
_CLICKHOUSE_USER_SQL_COMMENT_RE = re.compile(r"--|/\*|\*/|#")
_CLICKHOUSE_USER_SQL_BLOCKED_WORDS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "ALTER",
    "CREATE",
    "DROP",
    "TRUNCATE",
    "RENAME",
    "EXCHANGE",
    "ATTACH",
    "DETACH",
    "OPTIMIZE",
    "SYSTEM",
    "KILL",
    "SET",
    "GRANT",
    "REVOKE",
    "BACKUP",
    "RESTORE",
    "UNDROP",
    "MOVE",
    "SETTINGS",
    "FORMAT",
    "INTO OUTFILE",
)
_CLICKHOUSE_QUERY_KINDS = {"Select", "Insert", "All"}
_CLICKHOUSE_SORT_KEYS = {
    "total_duration": "total_duration_ms",
    "read_bytes": "read_bytes",
    "memory": "peak_memory_usage",
}
_CLICKHOUSE_RELEVANT_PROFILE_EVENTS = (
    "ReadMarks",
    "SelectedMarks",
    "Marks",
    "DiskReadBytes",
    "DiskReadTimeMicroseconds",
    "FileOpen",
    "IOBufferReadBytes",
    "NetworkReceiveBytes",
    "NetworkSendBytes",
    "OSReadBytes",
    "OSWriteBytes",
)
_CLICKHOUSE_CLICKHOUSE_USER = "copilot_dba"
_CLICKHOUSE_MAX_MINUTES = 10_080
_CLICKHOUSE_MAX_LIMIT = 100
_CLICKHOUSE_QUERY_TEXT_LIMIT = 320


class ClickHouseUserSQLBlockedError(ValueError):
    """Raised when user SQL is outside the read-only ClickHouse guard."""


class ClickHouseReadOnlyProfileError(RuntimeError):
    """Raised when the remote ClickHouse user is not readonly=1."""


class ClickHouseCliUnavailableError(RuntimeError):
    """Raised when neither supported ClickHouse client invocation is present."""


@dataclass(frozen=True)
class ClickHouseSQLTemplate:
    template_id: str
    version: int
    description: str
    sql: str
    required_params: tuple[str, ...] = ()
    optional_params: tuple[str, ...] = ()


@dataclass
class ClickHouseQueryLogAggregate:
    namespace: str
    pod: str
    query_hash: str
    run_count: int = 0
    total_duration_ms: float = 0.0
    max_duration_ms: float = 0.0
    p50_duration_ms: float = 0.0
    p95_duration_ms: float = 0.0
    p99_duration_ms: float = 0.0
    read_rows: float = 0.0
    read_bytes: float = 0.0
    peak_memory_usage: float = 0.0
    total_memory_usage: float = 0.0
    result_rows: float = 0.0
    written_rows: float = 0.0
    exception_codes: set[int] | None = None
    exceptions: set[str] | None = None
    log_types: set[str] | None = None
    normalized_query: str = ""
    profile_events: dict[str, float] | None = None


CLICKHOUSE_SQL_TEMPLATES: dict[str, ClickHouseSQLTemplate] = {}


def _register_template(template: ClickHouseSQLTemplate) -> ClickHouseSQLTemplate:
    CLICKHOUSE_SQL_TEMPLATES[template.template_id] = template
    return template


_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_readonly_profile.v1",
        1,
        "Check the mounted client's read-only profile and server version.",
        "SELECT getSetting('readonly') AS readonly, version() AS version FORMAT JSONEachRow",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_logging.v1",
        1,
        "Read query-log enablement, flush interval, and table TTL evidence.",
        """SELECT
    (SELECT value FROM system.settings WHERE name = 'log_queries' LIMIT 1) AS query_logging,
    (SELECT value FROM system.server_settings WHERE name = 'log_flush_interval_milliseconds' LIMIT 1) AS flush_interval,
    engine AS query_log_engine,
    engine_full AS query_log_engine_full
FROM system.tables
WHERE database = 'system' AND name = 'query_log'
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_query_log.v1",
        1,
        "Aggregate recent query_log events by normalized query hash.",
        """SELECT
    normalized_query_hash AS query_hash,
    count() AS run_count,
    sum(query_duration_ms) AS total_duration_ms,
    max(query_duration_ms) AS max_duration_ms,
    avg(query_duration_ms) AS avg_duration_ms,
    quantileTDigest(0.50)(query_duration_ms) AS p50_duration_ms,
    quantileTDigest(0.95)(query_duration_ms) AS p95_duration_ms,
    quantileTDigest(0.99)(query_duration_ms) AS p99_duration_ms,
    sum(read_rows) AS read_rows,
    sum(read_bytes) AS read_bytes,
    max(memory_usage) AS peak_memory_usage,
    sum(memory_usage) AS total_memory_usage,
    sum(result_rows) AS result_rows,
    sum(written_rows) AS written_rows,
    groupUniqArray(type) AS log_types,
    groupUniqArray(exception_code) AS exception_codes,
    any(exception) AS exception,
    any(normalizeQuery(coalesce(query, ''))) AS normalized_query,
    sum(ProfileEvents['ReadMarks']) AS read_marks,
    sum(ProfileEvents['SelectedMarks']) AS selected_marks,
    sum(ProfileEvents['Marks']) AS marks
FROM system.query_log
WHERE event_date >= toDate({start_time:DateTime})
  AND event_date <= toDate({end_time:DateTime})
  AND event_time >= {start_time:DateTime}
  AND event_time <= {end_time:DateTime}
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
  AND user != 'copilot_dba'
  AND ({query_kind:String} = 'All' OR query_kind = {query_kind:String})
  AND (is_initial_query = 1 OR {include_distributed:UInt8} = 1)
  AND query_duration_ms >= {min_duration_ms:UInt64}
GROUP BY normalized_query_hash
ORDER BY CASE {sort_by:String}
    WHEN 'total_duration' THEN total_duration_ms
    WHEN 'read_bytes' THEN read_bytes
    WHEN 'memory' THEN peak_memory_usage
    ELSE total_duration_ms
END DESC, query_hash ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=(
            "start_time",
            "end_time",
            "min_duration_ms",
            "limit",
            "query_kind",
            "include_distributed",
            "sort_by",
        ),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_query_log_legacy.v1",
        1,
        "Aggregate query_log for server versions without normalized_query_hash or Map ProfileEvents.",
        """SELECT
    query_id AS query_hash,
    count() AS run_count,
    sum(query_duration_ms) AS total_duration_ms,
    max(query_duration_ms) AS max_duration_ms,
    avg(query_duration_ms) AS avg_duration_ms,
    quantileTDigest(0.50)(query_duration_ms) AS p50_duration_ms,
    quantileTDigest(0.95)(query_duration_ms) AS p95_duration_ms,
    quantileTDigest(0.99)(query_duration_ms) AS p99_duration_ms,
    sum(read_rows) AS read_rows,
    sum(read_bytes) AS read_bytes,
    max(memory_usage) AS peak_memory_usage,
    sum(memory_usage) AS total_memory_usage,
    sum(result_rows) AS result_rows,
    sum(written_rows) AS written_rows,
    groupUniqArray(type) AS log_types,
    groupUniqArray(exception_code) AS exception_codes,
    any(exception) AS exception,
    any(query) AS normalized_query
FROM system.query_log
WHERE event_date >= toDate({start_time:DateTime})
  AND event_date <= toDate({end_time:DateTime})
  AND event_time >= {start_time:DateTime}
  AND event_time <= {end_time:DateTime}
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
  AND user != 'copilot_dba'
  AND ({query_kind:String} = 'All' OR query_kind = {query_kind:String})
  AND (is_initial_query = 1 OR {include_distributed:UInt8} = 1)
  AND query_duration_ms >= {min_duration_ms:UInt64}
GROUP BY query_id
ORDER BY CASE {sort_by:String}
    WHEN 'total_duration' THEN total_duration_ms
    WHEN 'read_bytes' THEN read_bytes
    WHEN 'memory' THEN peak_memory_usage
    ELSE total_duration_ms
END DESC, query_hash ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=(
            "start_time",
            "end_time",
            "min_duration_ms",
            "limit",
            "query_kind",
            "include_distributed",
            "sort_by",
        ),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_processes.v1",
        1,
        "Read a bounded snapshot of running queries from system.processes.",
        """SELECT
    query_id,
    user,
    elapsed,
    query_kind,
    normalized_query_hash AS query_hash,
    read_rows,
    read_bytes,
    memory_usage,
    normalizeQuery(coalesce(query, '')) AS normalized_query
FROM system.processes
WHERE user != 'copilot_dba'
  AND ({query_kind:String} = 'All' OR query_kind = {query_kind:String})
ORDER BY elapsed DESC, query_id ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit", "query_kind"),
    )
)


def _clickhouse_output_mode(output_mode: object) -> str:
    if not isinstance(output_mode, str) or output_mode not in {"human", "json"}:
        raise ValueError("Invalid output_mode: use 'human' or 'json'.")
    return output_mode


def _clickhouse_tool_output(
    tool: str,
    report: str,
    output_mode: object = "human",
    *,
    metadata: Optional[dict[str, Any]] = None,
) -> str:
    mode = _clickhouse_output_mode(output_mode)
    if mode == "human":
        return report
    payload: dict[str, Any] = {
        "schema_version": CLICKHOUSE_STRUCTURED_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tool": tool,
        "status": "ok",
        "data": {"format": "markdown", "report": report},
    }
    if metadata is not None:
        payload["metadata"] = metadata
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _clickhouse_tool_error(tool: str, message: str, output_mode: object = "human") -> str:
    try:
        mode = _clickhouse_output_mode(output_mode)
    except ValueError as exc:
        return str(exc)
    if mode == "human":
        return message
    payload = {
        "schema_version": CLICKHOUSE_STRUCTURED_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tool": tool,
        "status": "error",
        "errors": [message],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _validate_clickhouse_identifier(value: object, field: str = "identifier") -> str:
    if not isinstance(value, str) or not _CLICKHOUSE_IDENTIFIER_RE.fullmatch(value):
        raise ClickHouseUserSQLBlockedError(
            f"Invalid {field}: use one unquoted alphanumeric identifier beginning with a letter or underscore."
        )
    return value


def _clamp_clickhouse_number(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError) as exc:
        raise ValueError("ClickHouse numeric arguments must be integers.") from exc
    return max(minimum, min(maximum, parsed))


def get_clickhouse_template(template_id: str) -> ClickHouseSQLTemplate:
    template = CLICKHOUSE_SQL_TEMPLATES.get(template_id)
    if template is None:
        raise KeyError(f"Unknown ClickHouse SQL template: {template_id}")
    return template


def render_clickhouse_template(
    template_id: str, params: Optional[Mapping[str, Any]] = None
) -> tuple[str, dict[str, str]]:
    """Return a registry SQL string and CLI query-parameter bindings.

    No parameter is interpolated into SQL. The CLI binds every value through
    its typed query-parameter mechanism.
    """
    template = get_clickhouse_template(template_id)
    supplied = dict(params or {})
    for name in template.required_params:
        if name not in supplied:
            raise ValueError(f"Missing ClickHouse template parameter: {name}")
    unknown = set(supplied) - set(template.required_params) - set(template.optional_params)
    if unknown:
        raise ValueError(f"Unknown ClickHouse template parameters: {', '.join(sorted(unknown))}")

    bindings: dict[str, str] = {}
    for name in template.required_params:
        value = supplied[name]
        if name == "limit":
            value = _clamp_clickhouse_number(value, 25, 1, _CLICKHOUSE_MAX_LIMIT)
        if name == "min_duration_ms":
            value = _clamp_clickhouse_number(value, 1000, 1, 3_600_000)
        if not isinstance(value, (str, int, float, bool)):
            raise ValueError(f"Invalid ClickHouse template parameter: {name}")
        if isinstance(value, bool):
            value = int(value)
        bindings[name] = str(value)
    return template.sql, bindings


def _guard_clickhouse_user_sql(
    sql: object,
    *,
    allow_cluster_all_replicas: bool = False,
    explain: bool = True,
) -> str:
    """Strictly validate and wrap one ad-hoc read-only ClickHouse statement."""
    if not isinstance(sql, str) or not sql.strip():
        raise ClickHouseUserSQLBlockedError("User SQL must be a non-empty string.")
    statement = sql.strip()
    if ";" in statement:
        raise ClickHouseUserSQLBlockedError("User SQL must contain exactly one statement and no semicolons.")
    if _CLICKHOUSE_USER_SQL_COMMENT_RE.search(statement):
        raise ClickHouseUserSQLBlockedError("User SQL comments are not allowed.")
    if not _CLICKHOUSE_USER_SQL_START_RE.match(statement):
        raise ClickHouseUserSQLBlockedError("Only one SELECT or WITH statement is allowed.")

    upper = statement.upper()
    for blocked in _CLICKHOUSE_USER_SQL_BLOCKED_WORDS:
        pattern = rf"\b{re.escape(blocked)}\b" if " " not in blocked else re.escape(blocked)
        if re.search(pattern, upper):
            raise ClickHouseUserSQLBlockedError(f"Blocked ClickHouse SQL construct: {blocked}.")
    if not allow_cluster_all_replicas and re.search(r"\bCLUSTER_ALL_REPLICAS\b", upper):
        raise ClickHouseUserSQLBlockedError(
            "clusterAllReplicas is opt-in and was not enabled for this request."
        )

    for match in re.finditer(
        r"(?i)\b(?:FROM|JOIN|TABLE\s+FUNCTION)\s+([A-Za-z_][A-Za-z0-9_$]*)\s*\(",
        statement,
    ):
        function = match.group(1)
        if function.lower() not in _CLICKHOUSE_ALLOWED_TABLE_FUNCTIONS:
            raise ClickHouseUserSQLBlockedError(
                f"ClickHouse table function is not allowlisted: {function}."
            )

    return f"EXPLAIN {statement}" if explain else statement


def _validate_clickhouse_config_path(path: object) -> str:
    if not isinstance(path, str) or not path.startswith("/"):
        raise ValueError("The ClickHouse client config must be an absolute POSIX path.")
    if path != path.strip() or "\x00" in path or "\\" in path:
        raise ValueError("The ClickHouse client config path must be a clean absolute POSIX path.")
    if any(ord(char) < 32 or ord(char) == 127 for char in path):
        raise ValueError("The ClickHouse client config path must not contain control characters.")
    components = path[1:].split("/")
    if any(component in {"", ".", ".."} for component in components):
        raise ValueError("The ClickHouse client config path must not contain '.', '..' or empty components.")
    return path


def _clickhouse_client_config_path() -> str:
    return _validate_clickhouse_config_path(
        os.getenv("DBA_CLICKHOUSE_CLIENT_CONFIG", _CLICKHOUSE_CONFIG_PATH)
    )


def _clickhouse_topology_hints(pod: Any) -> dict[str, Any]:
    labels = dict(getattr(getattr(pod, "metadata", None), "labels", None) or {})
    hints: dict[str, Any] = {}
    for label, key in (
        ("clickhouse.altinity.com/shard", "shard"),
        ("clickhouse.altinity.com/replica", "replica"),
        ("clickhouse.altinity.com/cluster", "cluster"),
        ("clickhouse.altinity.com/namespace", "altinity_namespace"),
        ("clickhouse-keeper.altinity.com/shard", "keeper_shard"),
        ("clickhouse-keeper.altinity.com/replica", "keeper_replica"),
        ("clickhouse-keeper.altinity.com/cluster", "keeper_cluster"),
        ("app.kubernetes.io/name", "app_name"),
        ("app.kubernetes.io/component", "component"),
        ("app.kubernetes.io/instance", "instance"),
        ("app.kubernetes.io/part-of", "part_of"),
        ("app.kubernetes.io/version", "app_version"),
    ):
        value = labels.get(label)
        if value:
            hints[key] = str(value)
    name = str(getattr(getattr(pod, "metadata", None), "name", None) or "")
    match = re.search(r"-(\d+)$", name)
    if match:
        hints["ordinal"] = int(match.group(1))
    owner_references = getattr(getattr(pod, "metadata", None), "owner_references", None) or []
    for owner in owner_references:
        if getattr(owner, "kind", None) == "StatefulSet" and getattr(owner, "name", None):
            hints["statefulset"] = str(owner.name)
    return hints


def _clickhouse_pod_kind(pod: Any) -> str:
    from .tools import _pod_identity

    identity = _pod_identity(pod)
    if "keeper" in identity:
        return "clickhouse-keeper"
    if "clickhouse" in identity:
        return "clickhouse"
    return "unknown"


def _clickhouse_query_argv(
    client_prefix: list[str],
    sql: str,
    params: Optional[Mapping[str, str]] = None,
    *,
    format: str = "JSONEachRow",
) -> list[str]:
    argv = [*client_prefix, "--config-file", _clickhouse_client_config_path()]
    if format:
        argv.extend(["--format", format])
    for name in sorted(params or {}):
        _validate_clickhouse_identifier(name, "query parameter name")
        argv.extend([f"--param_{name}", params[name]])
    argv.extend(["--query", sql])
    return argv


def _detect_clickhouse_client(
    namespace: str,
    pod_name: str,
    container: Optional[str] = None,
) -> list[str]:
    from .tools import _pod_exec

    candidates = [["clickhouse-client"], ["clickhouse", "client"]]
    errors: list[str] = []
    for prefix in candidates:
        try:
            output = _pod_exec(
                namespace,
                pod_name,
                [*prefix, "--version"],
                container,
                "clickhouse_client_detection",
                timeout_seconds=10,
            )
            if (output or "").strip():
                return prefix
            errors.append("empty --version output")
        except Exception as exc:
            errors.append(str(exc))
    raise ClickHouseCliUnavailableError(
        "Neither clickhouse-client nor the clickhouse binary's client subcommand is available."
    )


def _parse_json_each_row(output: object) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in str(output or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError("ClickHouse returned malformed JSONEachRow output.") from exc
        if not isinstance(value, dict):
            raise ValueError("ClickHouse JSONEachRow output must contain JSON objects.")
        rows.append(value)
    return rows


def _clickhouse_readonly_row(output: object) -> dict[str, Any]:
    rows = _parse_json_each_row(output)
    if len(rows) != 1:
        raise ClickHouseReadOnlyProfileError("The read-only profile check did not return one row.")
    return rows[0]


def _ensure_clickhouse_readonly(
    namespace: str,
    pod_name: str,
    client_prefix: list[str],
    container: Optional[str] = None,
) -> dict[str, Any]:
    from .tools import _pod_exec

    sql, _ = render_clickhouse_template("clickhouse_readonly_profile.v1")
    output = _pod_exec(
        namespace,
        pod_name,
        _clickhouse_query_argv(client_prefix, sql),
        container,
        "clickhouse_readonly_profile_check",
        timeout_seconds=15,
    )
    row = _clickhouse_readonly_row(output)
    readonly = row.get("readonly", row.get("getSetting('readonly')"))
    if isinstance(readonly, str):
        readonly = readonly.strip()
    if readonly != 1 and readonly != "1":
        raise ClickHouseReadOnlyProfileError(
            f"Refusing to run ClickHouse diagnostics: getSetting('readonly')={readonly!r}; expected 1."
        )
    return row


def _run_clickhouse_query(
    namespace: str,
    pod: str,
    template_id: str,
    params: Optional[Mapping[str, Any]] = None,
    container: Optional[str] = None,
) -> tuple[list[dict[str, Any]], str]:
    """Run one versioned registry query through the shared direct-argv helper."""
    from .tools import _pod_exec

    sql, bindings = render_clickhouse_template(template_id, params)
    client_prefix = _detect_clickhouse_client(namespace, pod, container)
    _ensure_clickhouse_readonly(namespace, pod, client_prefix, container)
    argv = _clickhouse_query_argv(client_prefix, sql, bindings)
    output = _pod_exec(
        namespace,
        pod,
        argv,
        container,
        "clickhouse_readonly_query",
        timeout_seconds=30,
    )
    return _parse_json_each_row(output), output


def _clickhouse_time_window(minutes: int) -> tuple[datetime, datetime]:
    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = end - timedelta(minutes=max(1, min(int(minutes), _CLICKHOUSE_MAX_MINUTES)))
    return start, end


def _clickhouse_number(row: Mapping[str, Any], *names: str, default: float = 0.0) -> float:
    for name in names:
        if name not in row or row[name] is None:
            continue
        try:
            return float(row[name])
        except (TypeError, ValueError):
            continue
    return default


def _clickhouse_string(row: Mapping[str, Any], *names: str, default: str = "") -> str:
    for name in names:
        value = row.get(name)
        if value is not None:
            return str(value)
    return default


def _clickhouse_query_text(value: object) -> str:
    text = " ".join(str(value or "").split())
    upper = text.upper()
    if any(re.search(rf"\b{re.escape(word)}\b", upper) for word in (
        "INSERT", "UPDATE", "DELETE", "ALTER", "CREATE", "DROP", "TRUNCATE", "RENAME",
        "EXCHANGE", "ATTACH", "DETACH", "OPTIMIZE", "SYSTEM", "KILL", "GRANT", "REVOKE",
        "BACKUP", "RESTORE", "UNDROP", "MOVE",
    )):
        return "[redacted write/DDL/maintenance query]"
    return text[:_CLICKHOUSE_QUERY_TEXT_LIMIT] + ("..." if len(text) > _CLICKHOUSE_QUERY_TEXT_LIMIT else "")


def parse_clickhouse_query_log_rows(
    namespace: str,
    pod: str,
    rows: list[dict[str, Any]],
) -> list[ClickHouseQueryLogAggregate]:
    """Normalize query_log rows across JSON/null and older column variants."""
    aggregates: list[ClickHouseQueryLogAggregate] = []
    for row in rows:
        profile_events = row.get("ProfileEvents") or row.get("profile_events") or {}
        events: dict[str, float] = {}
        if isinstance(profile_events, Mapping):
            for name in _CLICKHOUSE_RELEVANT_PROFILE_EVENTS:
                value = profile_events.get(name)
                if value is not None:
                    try:
                        events[name] = float(value)
                    except (TypeError, ValueError):
                        pass

        aggregate = ClickHouseQueryLogAggregate(
            namespace=namespace,
            pod=pod,
            query_hash=_clickhouse_string(row, "normalized_query_hash", "query_hash", "query_id"),
            run_count=int(_clickhouse_number(row, "run_count")),
            total_duration_ms=_clickhouse_number(row, "total_duration_ms"),
            max_duration_ms=_clickhouse_number(row, "max_duration_ms"),
            p50_duration_ms=_clickhouse_number(row, "p50_duration_ms", "quantile_50_duration_ms"),
            p95_duration_ms=_clickhouse_number(row, "p95_duration_ms", "quantile_95_duration_ms"),
            p99_duration_ms=_clickhouse_number(row, "p99_duration_ms", "quantile_99_duration_ms"),
            read_rows=_clickhouse_number(row, "read_rows", "rows_read"),
            read_bytes=_clickhouse_number(row, "read_bytes", "bytes_read"),
            peak_memory_usage=_clickhouse_number(
                row, "peak_memory_usage", "memory_usage", "max_memory_usage"
            ),
            total_memory_usage=_clickhouse_number(row, "total_memory_usage", "memory_usage"),
            result_rows=_clickhouse_number(row, "result_rows", "rows_result"),
            written_rows=_clickhouse_number(row, "written_rows"),
            exception_codes={
                int(float(code))
                for code in _clickhouse_sequence(row, "exception_codes")
                if str(code).strip() not in {"", "0", "None"}
            },
            exceptions={item for item in _clickhouse_sequence(row, "exception") if item},
            log_types={str(item) for item in _clickhouse_sequence(row, "log_types")},
            normalized_query=_clickhouse_query_text(row.get("normalized_query")),
            profile_events=events,
        )
        aggregates.append(aggregate)
    return aggregates


def _clickhouse_sequence(row: Mapping[str, Any], name: str) -> list[Any]:
    value = row.get(name)
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if isinstance(value, str) and value.startswith("["):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else [value]
        except json.JSONDecodeError:
            return [value]
    return [value]


def _clickhouse_marks_efficiency(aggregate: ClickHouseQueryLogAggregate) -> Optional[float]:
    events = aggregate.profile_events or {}
    marks = events.get("ReadMarks", events.get("SelectedMarks", events.get("Marks", 0.0)))
    if marks <= 0:
        return None
    return aggregate.read_rows / marks


def _clickhouse_remarkable_events(aggregate: ClickHouseQueryLogAggregate) -> list[str]:
    events = aggregate.profile_events or {}
    return [f"{name}={int(value)}" for name, value in events.items() if value]


def _clickhouse_query_log_template(version: str) -> str:
    major_minor = version.split(".")[:2]
    try:
        major, minor = (int(part) for part in major_minor)
    except ValueError:
        return "clickhouse_query_log_legacy.v1"
    # Old builds lack normalized_query_hash and Map-typed ProfileEvents. The
    # compatibility template still uses parameter binding and conservative
    # query_id grouping when the server cannot support the modern view.
    if (major, minor) < (21, 8):
        return "clickhouse_query_log_legacy.v1"
    return "clickhouse_query_log.v1"


def _clickhouse_logging_text(row: Mapping[str, Any]) -> str:
    enabled = _clickhouse_string(row, "query_logging", default="unknown")
    enabled_text = enabled if enabled.lower() in {"0", "1", "true", "false", "unknown", ""} else "unknown"
    flush = _clickhouse_string(row, "flush_interval", default="unknown") or "unknown"
    engine_full = _clickhouse_string(row, "query_log_engine_full", default="")
    ttl_match = re.search(r"(?i)\bTTL\b[^\n]+", engine_full)
    retention = ttl_match.group(0).strip() if ttl_match else "no TTL expression"
    return f"query_logging={enabled_text or 'unknown'}; flush_interval={flush}; retention={retention}"


def _clickhouse_recommendations(aggregates: list[ClickHouseQueryLogAggregate]) -> list[str]:
    recommendations: list[str] = []
    for aggregate in aggregates:
        label = f"{aggregate.namespace}/{aggregate.pod} query_hash={aggregate.query_hash}"
        evidence: list[str] = []
        marks = _clickhouse_marks_efficiency(aggregate)
        if aggregate.read_bytes >= 100 * 1024 * 1024:
            evidence.append(f"read_bytes={int(aggregate.read_bytes)}")
        if marks is not None and marks >= 100_000:
            evidence.append(f"rows_per_mark={marks:.1f}")
        if evidence:
            recommendations.append(
                f"- Review primary-key/range filtering for {label}. Evidence: {'; '.join(evidence)}. "
                "No change was applied."
            )
        if aggregate.peak_memory_usage >= 1024 * 1024 * 1024:
            recommendations.append(
                f"- Review concurrency and join/sort memory for {label}. Evidence: "
                f"peak_memory_usage={int(aggregate.peak_memory_usage)}. No change was applied."
            )
        if aggregate.exception_codes:
            recommendations.append(
                f"- Investigate failed executions for {label}. Evidence: exception_codes="
                f"{sorted(aggregate.exception_codes)}. No change was applied."
            )
    return recommendations


_CLICKHOUSE_INSTALLATION_GROUP = "clickhouse.altinity.com"
_CLICKHOUSE_INSTALLATION_VERSION = "v1"
_CLICKHOUSE_INSTALLATION_PLURAL = "clickhouseinstallations"
_CLICKHOUSE_MAX_INSTALLATION_NAMESPACES = 16


def _clickhouse_custom_objects(v1: Any) -> Any:
    from kubernetes import client

    return client.CustomObjectsApi(v1.api_client)


def _clickhouse_installation_access(
    v1: Any,
    db_type: str,
    namespace: Optional[str],
    installation_name: Optional[str],
    namespaces: set[str] | frozenset[str],
) -> tuple[list[str], list[str]]:
    """Optionally read Altinity ClickHouseInstallations and report access limits."""
    if db_type not in _CLICKHOUSE_SERVER_DATABASE_TYPES:
        return [], []
    try:
        custom = _clickhouse_custom_objects(v1)
        resources: list[dict[str, Any]] = []
        if installation_name:
            inferred_namespace = namespace
            if not inferred_namespace and len(namespaces) == 1:
                inferred_namespace = next(iter(namespaces))
            if inferred_namespace:
                if not _CLICKHOUSE_K8S_NAME_RE.fullmatch(installation_name):
                    return [], ["ClickHouseInstallation name is not a valid Kubernetes object name."]
                resource = custom.get_namespaced_custom_object(
                    group=_CLICKHOUSE_INSTALLATION_GROUP,
                    version=_CLICKHOUSE_INSTALLATION_VERSION,
                    namespace=inferred_namespace,
                    plural=_CLICKHOUSE_INSTALLATION_PLURAL,
                    name=installation_name,
                )
                resources = [resource]
            else:
                if not _CLICKHOUSE_K8S_NAME_RE.fullmatch(installation_name):
                    return [], ["ClickHouseInstallation name is not a valid Kubernetes object name."]
                for target_namespace in sorted(namespaces)[:_CLICKHOUSE_MAX_INSTALLATION_NAMESPACES]:
                    response = custom.list_namespaced_custom_object(
                        group=_CLICKHOUSE_INSTALLATION_GROUP,
                        version=_CLICKHOUSE_INSTALLATION_VERSION,
                        namespace=target_namespace,
                        plural=_CLICKHOUSE_INSTALLATION_PLURAL,
                    )
                    matches = [
                        resource
                        for resource in response.get("items", [])
                        if resource.get("metadata", {}).get("name") == installation_name
                    ]
                    if matches:
                        resources = matches
                        break
                if not resources:
                    return [], [f"ClickHouseInstallation {installation_name} was not found in accessible namespaces."]
        elif namespace:
            response = custom.list_namespaced_custom_object(
                group=_CLICKHOUSE_INSTALLATION_GROUP,
                version=_CLICKHOUSE_INSTALLATION_VERSION,
                namespace=namespace,
                plural=_CLICKHOUSE_INSTALLATION_PLURAL,
            )
            resources = response.get("items", [])
        else:
            for target_namespace in sorted(namespaces)[:_CLICKHOUSE_MAX_INSTALLATION_NAMESPACES]:
                response = custom.list_namespaced_custom_object(
                    group=_CLICKHOUSE_INSTALLATION_GROUP,
                    version=_CLICKHOUSE_INSTALLATION_VERSION,
                    namespace=target_namespace,
                    plural=_CLICKHOUSE_INSTALLATION_PLURAL,
                )
                resources.extend(response.get("items", []))
        lines = []
        for resource in resources[:_CLICKHOUSE_MAX_INSTALLATION_NAMESPACES]:
            metadata = resource.get("metadata", {})
            name = metadata.get("name", "unknown")
            resource_namespace = metadata.get("namespace", namespace or "unknown")
            lines.append(f"clickhouseinstallation={resource_namespace}/{name}")
        if installation_name and not lines:
            lines.append(f"clickhouseinstallation={namespace}/{installation_name} (not found)")
        return lines, []
    except FileNotFoundError as exc:
        return [], [f"ClickHouseInstallation access: CRD not found ({exc})."]
    except Exception as exc:
        message = str(exc)
        if "forbidden" in message.lower() or "403" in message:
            return [], [f"ClickHouseInstallation access denied (optional read-only CRD permission): {exc}"]
        if "404" in message or "not found" in message.lower():
            return [], [f"ClickHouseInstallation resource not found: {exc}"]
        return [], [f"ClickHouseInstallation access unavailable: {exc}"]


def _clickhouse_pod_candidates(
    v1: Any,
    namespace: Optional[str],
    pod: Optional[str],
    label_selector: Optional[str],
) -> list[Any]:
    from .tools import _pod_identity

    if pod:
        if not namespace:
            raise ValueError("A namespace is required when pod is specified.")
        return [v1.read_namespaced_pod(name=pod, namespace=namespace)]
    if namespace:
        kwargs: dict[str, Any] = {"namespace": namespace}
        if label_selector:
            kwargs["label_selector"] = label_selector
        pod_list = v1.list_namespaced_pod(**kwargs)
    else:
        kwargs = {"label_selector": label_selector} if label_selector else {}
        pod_list = v1.list_pod_for_all_namespaces(**kwargs)
    return [item for item in pod_list.items if _clickhouse_pod_kind(item) == "clickhouse"]


def _clickhouse_processes_report(
    namespace: str,
    pod: str,
    query_kind: str,
    limit: int,
    container: Optional[str],
) -> tuple[list[str], bool]:
    try:
        rows, _ = _run_clickhouse_query(
            namespace,
            pod,
            "clickhouse_processes.v1",
            {"limit": limit, "query_kind": query_kind},
            container,
        )
    except Exception as exc:
        return [f"- Running-query evidence unavailable: {exc}"], False
    lines = [f"- Running now: {len(rows)} bounded rows from system.processes."]
    for row in rows:
        elapsed = _clickhouse_number(row, "elapsed")
        query_id = _clickhouse_string(row, "query_id")
        read_bytes = _clickhouse_number(row, "read_bytes")
        memory = _clickhouse_number(row, "memory_usage")
        lines.append(
            f"  - query_id={query_id}; elapsed={elapsed:.3f}s; read_bytes={int(read_bytes)}; "
            f"memory_usage={int(memory)}; query={_clickhouse_query_text(row.get('normalized_query'))}"
        )
    return lines, True


def get_clickhouse_slow_queries(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    minutes: int = 60,
    min_duration_ms: int = 1000,
    limit: int = 25,
    sort_by: str = "total_duration",
    query_kind: str = "Select",
    output_mode: str = "human",
    *,
    container: Optional[str] = None,
    label_selector: Optional[str] = None,
    include_distributed: bool = False,
) -> str:
    """Analyze recent ClickHouse query_log evidence without mutating state."""
    tool = "get_clickhouse_slow_queries"
    mode = "human"
    try:
        mode = _clickhouse_output_mode(output_mode)
        minutes = _clamp_clickhouse_number(minutes, 60, 1, _CLICKHOUSE_MAX_MINUTES)
        min_duration_ms = _clamp_clickhouse_number(min_duration_ms, 1000, 1, 3_600_000)
        limit = _clamp_clickhouse_number(limit, 25, 1, _CLICKHOUSE_MAX_LIMIT)
        if sort_by not in _CLICKHOUSE_SORT_KEYS:
            raise ValueError("Invalid sort_by: use total_duration, read_bytes, or memory.")
        if not isinstance(query_kind, str) or query_kind not in _CLICKHOUSE_QUERY_KINDS:
            raise ValueError("Invalid query_kind: use Select, Insert, or All.")
        start, end = _clickhouse_time_window(minutes)
        from .tools import _load_kubernetes_client

        v1 = _load_kubernetes_client()
        pods = _clickhouse_pod_candidates(v1, namespace, pod, label_selector)
    except Exception as exc:
        return _clickhouse_tool_error(tool, f"Error preparing ClickHouse analysis: {exc}", mode)

    if not pods:
        return _clickhouse_tool_error(
            tool,
            "No ClickHouse server pods found. Use find_database_pods with 'clickhouse' to confirm labels.",
            mode,
        )

    sections: list[str] = []
    errors: list[str] = []
    all_recommendations: list[str] = []
    for pod_item in pods:
        target_namespace = pod_item.metadata.namespace
        target_pod = pod_item.metadata.name
        label = f"{target_namespace}/{target_pod}"
        try:
            profile_rows, _ = _run_clickhouse_query(
                target_namespace,
                target_pod,
                "clickhouse_readonly_profile.v1",
                container=container,
            )
            version = _clickhouse_string(profile_rows[0], "version", default="unknown")
            logging_rows, _ = _run_clickhouse_query(
                target_namespace,
                target_pod,
                "clickhouse_logging.v1",
                container=container,
            )
            logging_text = _clickhouse_logging_text(logging_rows[0]) if logging_rows else "query_log metadata unavailable"
            template_id = _clickhouse_query_log_template(version)
            params = {
                "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
                "min_duration_ms": min_duration_ms,
                "limit": limit,
                "query_kind": query_kind,
                "include_distributed": int(bool(include_distributed)),
                "sort_by": sort_by,
            }
            rows, _ = _run_clickhouse_query(
                target_namespace,
                target_pod,
                template_id,
                params,
                container,
            )
            aggregates = parse_clickhouse_query_log_rows(target_namespace, target_pod, rows)
            running_lines, _ = _clickhouse_processes_report(
                target_namespace, target_pod, query_kind, limit, container
            )
            recommendations = _clickhouse_recommendations(aggregates)
            all_recommendations.extend(recommendations)
            section = [
                f"### {label} (server {version})",
                f"- Evidence: system.query_log; {logging_text}; sort={sort_by}; initial_queries="
                f"{'no' if include_distributed else 'yes'}.",
                *running_lines,
                "",
            ]
            if not aggregates:
                section.append(
                    f"No matching {query_kind} queries >= {min_duration_ms}ms were found in the selected window."
                )
            for index, aggregate in enumerate(aggregates, 1):
                marks = _clickhouse_marks_efficiency(aggregate)
                exception_codes = sorted(aggregate.exception_codes or set())
                profile_text = ", ".join(_clickhouse_remarkable_events(aggregate)) or "none"
                marks_text = f"{marks:.1f}" if marks is not None else "unavailable"
                section.extend(
                    [
                        f"{index}. query_hash={aggregate.query_hash}; runs={aggregate.run_count}; "
                        f"total={aggregate.total_duration_ms / 1000:.3f}s; p95={aggregate.p95_duration_ms / 1000:.3f}s.",
                        f"   Read rows/bytes: {int(aggregate.read_rows)}/{int(aggregate.read_bytes)}; \n"
                        f"   rows/mark={marks_text}; marks/profile events: {profile_text}.",
                        f"   Memory peak/sum: {int(aggregate.peak_memory_usage)}/{int(aggregate.total_memory_usage)}; "
                        f"result_rows={int(aggregate.result_rows)}; written_rows={int(aggregate.written_rows)}.",
                        f"   Log types: {', '.join(sorted(aggregate.log_types or set())) or 'unknown'}; "
                        f"exception_codes={exception_codes or 'none'}.",
                    ]
                )
                if aggregate.exceptions:
                    section.append(f"   Exceptions: {' | '.join(sorted(aggregate.exceptions))[:400]}.")
                if aggregate.normalized_query:
                    section.extend(["   Normalized query:", f"   ```sql", f"   {aggregate.normalized_query}", "   ```"])
                section.append("")
            if recommendations:
                section.extend(["Evidence-linked advisory recommendations:"])
                section.extend(recommendations)
            else:
                section.append("No recommendation is supported by this pod's evidence.")
            section.extend(
                [
                    "",
                    "All recommendations are advisory. No query, configuration, or ClickHouse state was changed.",
                    "",
                ]
            )
            sections.append("\n".join(section))
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    if not sections and errors:
        return _clickhouse_tool_error(tool, "; ".join(errors), mode)
    report = [
        "ClickHouse slow and expensive query analysis (advisory only):",
        "",
        *"\n".join(sections).splitlines(),
    ]
    if errors:
        report.extend(["Skipped pods/evidence errors:", *(f"- {error}" for error in errors), ""])
    report.append("No changes were made; this analysis is advisory only.")
    return _clickhouse_tool_output(
        tool,
        "\n".join(report),
        mode,
        metadata={
            "namespace": namespace,
            "pod": pod,
            "minutes": minutes,
            "min_duration_ms": min_duration_ms,
            "sort_by": sort_by,
            "query_kind": query_kind,
            "include_distributed": include_distributed,
        },
    )


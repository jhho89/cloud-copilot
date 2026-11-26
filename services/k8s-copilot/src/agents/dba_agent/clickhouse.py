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
_CLICKHOUSE_SECRET_TEXT_RE = re.compile(
    r"(?i)\b(password|passwd|secret|token|credential|authorization|api[_-]?key)\s*[:=]\s*[^,;\s]+"
)
_CLICKHOUSE_CLICKHOUSE_USER = "copilot_dba"
_CLICKHOUSE_MAX_MINUTES = 10_080
_CLICKHOUSE_MAX_LIMIT = 100
_CLICKHOUSE_QUERY_TEXT_LIMIT = 320
_CLICKHOUSE_MAX_EVIDENCE_ROWS = 200
_CLICKHOUSE_MAX_REPLICATION_PODS = 16


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
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_topology.v1",
        1,
        "Read the replica-local view of configured cluster shard and replica membership.",
        """SELECT
    cluster,
    shard_num,
    replica_num,
    host_name,
    host_address,
    port,
    is_local,
    errors_count,
    slowdowns_count,
    estimated_recovery_time
FROM system.clusters
ORDER BY cluster, shard_num, replica_num
LIMIT 200
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_replicas.v1",
        1,
        "Read local replicated-table health, delay, queue size, membership, and recovery evidence.",
        """SELECT
    database,
    table,
    replica_name,
    replica_path,
    is_leader,
    is_readonly,
    is_session_expired,
    absolute_delay,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas,
    lost_parts,
    last_queue_update_exception,
    recovery_time
FROM system.replicas
ORDER BY database, table, replica_name
LIMIT 200
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_parts_summary.v1",
        1,
        "Aggregate active MergeTree parts by table and partition.",
        """SELECT
    database,
    table,
    partition,
    count() AS active_parts,
    sum(bytes_on_disk) AS total_bytes,
    sum(rows) AS total_rows,
    avg(bytes_on_disk) AS avg_part_bytes,
    max(bytes_on_disk) AS largest_part_bytes,
    max(modification_time) AS newest_part_time
FROM system.parts
WHERE active = 1
  AND (empty({database:String}) OR database = {database:String})
  AND (empty({table:String}) OR table = {table:String})
GROUP BY database, table, partition
ORDER BY active_parts DESC, total_bytes DESC, database ASC, table ASC, partition ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
        optional_params=("database", "table"),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_replication_queue.v1",
        1,
        "Read bounded replication-queue entries, retries, exceptions, and postponement reasons.",
        """SELECT
    database,
    table,
    replica_name,
    replica_path,
    node_name,
    type,
    size,
    attempts,
    last_exception,
    postpone_reason,
    last_postpone_time,
    last_attempt_time
FROM system.replication_queue
ORDER BY last_attempt_time DESC, database ASC, table ASC, node_name ASC
LIMIT 200
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_merges.v1",
        1,
        "Read a bounded snapshot of running merges.",
        """SELECT
    database,
    table,
    elapsed,
    progress,
    num_parts,
    rows_read,
    bytes_read_uncompressed,
    total_size_bytes_compressed,
    source_part_names,
    result_part_name
FROM system.merges
ORDER BY elapsed DESC, database ASC, table ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_distribution_queue.v1",
        1,
        "Read pending Distributed-table files, bytes, errors, and last-send evidence.",
        """SELECT
    database,
    table,
    address,
    errors_count,
    data_files,
    data_compressed_files,
    bytes_to_send,
    last_exception,
    last_time
FROM system.distribution_queue
ORDER BY bytes_to_send DESC, database ASC, table ASC
LIMIT 200
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_mutations.v1",
        1,
        "Read pending, old, and failed table mutations.",
        """SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    is_done,
    parts_to_do,
    latest_fail_time,
    latest_fail_reason
FROM system.mutations
WHERE (empty({database:String}) OR database = {database:String})
  AND (empty({table:String}) OR table = {table:String})
ORDER BY is_done ASC, create_time ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
        optional_params=("database", "table"),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_keeper_connection.v1",
        1,
        "Read the local Keeper/ZooKeeper connection state when the system table is present.",
        """SELECT
    name,
    host,
    port,
    is_connected,
    latency_ms,
    errors_count,
    timeout_count
FROM system.zookeeper_connection
ORDER BY name
LIMIT 200
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_detached_parts.v1",
        1,
        "Read detached MergeTree part metadata grouped by reason.",
        """SELECT
    database,
    table,
    partition,
    name,
    reason,
    disk_name
FROM system.detached_parts
WHERE (empty({database:String}) OR database = {database:String})
  AND (empty({table:String}) OR table = {table:String})
ORDER BY database ASC, table ASC, partition ASC, name ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
        optional_params=("database", "table"),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_part_log.v1",
        1,
        "Read recent part creation, merge, and mutation activity.",
        """SELECT
    database,
    table,
    event_type,
    count() AS event_count,
    sum(rows) AS rows_changed,
    sum(bytes) AS bytes_changed,
    avg(duration_ms) AS avg_duration_ms,
    max(event_time) AS latest_event_time
FROM system.part_log
WHERE event_date >= toDate(now() - INTERVAL 24 HOUR)
  AND event_time >= now() - INTERVAL 24 HOUR
  AND (empty({database:String}) OR database = {database:String})
  AND (empty({table:String}) OR table = {table:String})
GROUP BY database, table, event_type
ORDER BY latest_event_time DESC, event_count DESC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
        optional_params=("database", "table"),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_parts_errors.v1",
        1,
        "Read recent server errors that may explain merge or storage issues.",
        """SELECT
    name,
    code,
    text,
    value AS count,
    last_error_time,
    last_error_message
FROM system.errors
WHERE value > 0 AND last_error_time >= now() - INTERVAL 24 HOUR
ORDER BY last_error_time DESC, value DESC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_merge_tree_settings.v1",
        1,
        "Read MergeTree defaults used for too-many-parts comparisons.",
        """SELECT
    name,
    value,
    changed,
    description,
    type
FROM system.merge_tree_settings
WHERE name IN (
    'parts_to_delay_throw',
    'parts_to_throw_insert',
    'max_parts_to_delay_merge',
    'max_parts_to_merge_at_once',
    'min_bytes_for_wide_part',
    'min_rows_for_wide_part',
    'index_granularity'
)
ORDER BY name
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_table_metadata.v1",
        1,
        "Read table engine, TTL, key, and per-table settings evidence.",
        """SELECT
    database,
    table,
    engine,
    engine_full,
    partition_key,
    sorting_key
FROM system.tables
WHERE database != 'system'
  AND (empty({database:String}) OR database = {database:String})
  AND (empty({table:String}) OR table = {table:String})
ORDER BY database ASC, table ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
        optional_params=("database", "table"),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_async_metrics.v1",
        1,
        "Read selected memory and cache metrics.",
        """SELECT
    metric,
    value
FROM system.asynchronous_metrics
WHERE metric IN (
    'MemoryTracking',
    'MemoryTrackingForAllocations',
    'MemoryResident',
    'MemoryShared',
    'MemoryVirtual',
    'OSSMemoryAvailable',
    'OSSMemoryTotal',
    'MarkCacheBytes',
    'UncompressedCacheBytes',
    'CompressedReadBufferCacheBytes',
    'CacheSizeInBytes',
    'QueryCacheHits',
    'QueryCacheMisses',
    'QueryCacheEntries'
)
ORDER BY metric
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_metrics.v1",
        1,
        "Read bounded server metric counters from system.metrics.",
        """SELECT
    metric,
    value,
    description
FROM system.metrics
ORDER BY metric
LIMIT 200
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_resource_metrics.v1",
        1,
        "Read current memory, merge-pool, and query-cache metrics.",
        """SELECT
    metric,
    value
FROM system.metrics
WHERE metric IN (
    'MemoryTracking',
    'MemoryTrackingForAllocations',
    'BackgroundPoolTask',
    'BackgroundMergesAndMutationsPoolTask',
    'Merge',
    'Mutation',
    'QueryCacheHits',
    'QueryCacheMisses'
)
ORDER BY metric
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_workload_processes.v1",
        1,
        "Read bounded running queries with client attribution and normalized query text.",
        """SELECT
    query_id,
    user,
    elapsed,
    query_kind,
    normalized_query_hash AS query_hash,
    read_rows,
    read_bytes,
    memory_usage,
    client_name,
    http_user_agent,
    initial_address,
    normalizeQuery(coalesce(query, '')) AS normalized_query
FROM system.processes
WHERE user != 'copilot_dba'
ORDER BY memory_usage DESC, elapsed DESC, query_id ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_settings.v1",
        1,
        "Read selected concurrency-related settings as advisory evidence.",
        """SELECT
    name,
    value,
    changed
FROM system.settings
WHERE name IN (
    'max_threads',
    'max_concurrent_queries',
    'max_concurrent_queries_for_all_users',
    'max_concurrent_queries_for_user',
    'max_concurrent_select_queries',
    'max_concurrent_insert_queries',
    'max_io_threads',
    'max_sessions_for_user'
    )
ORDER BY name
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_server_settings.v1",
        1,
        "Read server memory and background-pool settings.",
        """SELECT
    name,
    value,
    type,
    changed
FROM system.server_settings
WHERE name IN (
    'max_server_memory_usage',
    'max_server_memory_usage_to_ram_ratio',
    'max_thread_pool_size',
    'background_pool_size',
    'background_merges_mutations_concurrency_scale',
    'background_merges_mutations_scheduling_policy',
    'max_uncompressed_cache_size',
    'mark_cache_size'
)
ORDER BY name
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_errors.v1",
        1,
        "Read bounded recent and cumulative server error counters.",
        """SELECT
    code AS error_code,
    name,
    value,
    last_error_time,
    last_error_text,
    remote
FROM system.errors
ORDER BY value DESC, name ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_disks.v1",
        1,
        "Read configured disk free space, total capacity, and keep-free thresholds.",
        """SELECT
    name,
    path,
    type,
    free_space,
    total_space,
    keep_free_space,
    is_read_only
FROM system.disks
ORDER BY name
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_storage_policies.v1",
        1,
        "Read storage-policy volume and tiering evidence.",
        """SELECT
    policy_name,
    volume_name,
    volume_index,
    disk_name,
    path,
    move_factor,
    prefer_not_to_merge,
    perform_ttl_move_on_insert
FROM system.storage_policies
ORDER BY policy_name ASC, volume_index ASC, disk_name ASC
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_largest_tables.v1",
        1,
        "Rank active-table storage by metadata in system.parts.",
        """SELECT
    database,
    table,
    count() AS active_parts,
    sum(bytes_on_disk) AS total_bytes,
    sum(rows) AS total_rows,
    avg(bytes_on_disk) AS avg_part_bytes
FROM system.parts
WHERE active = 1
  AND (empty({database:String}) OR database = {database:String})
  AND (empty({table:String}) OR table = {table:String})
GROUP BY database, table
ORDER BY total_bytes DESC, active_parts DESC, database ASC, table ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
        optional_params=("database", "table"),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_quotas_usage.v1",
        1,
        "Read bounded per-user/application quota usage and limits when available.",
        """SELECT
    quota_name,
    user_name,
    application_name,
    elapsed,
    duration,
    num_queries,
    max_queries,
    query_duration_ms,
    max_query_duration_ms,
    exceptions,
    max_exceptions,
    result_rows,
    max_result_rows,
    read_rows,
    max_read_rows,
    read_bytes,
    max_read_bytes
FROM system.quotas_usage
ORDER BY read_bytes DESC, num_queries DESC, user_name ASC
LIMIT 200
FORMAT JSONEachRow""",
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_workload_query_log.v1",
        1,
        "Aggregate recent initial-query load by user, client, HTTP user agent, and initial address.",
        """SELECT
    user,
    client_name,
    http_user_agent,
    initial_address,
    normalizeQuery(coalesce(any(query), '')) AS normalized_query,
    count() AS run_count,
    sum(query_duration_ms) AS total_duration_ms,
    max(query_duration_ms) AS max_duration_ms,
    avg(query_duration_ms) AS avg_duration_ms,
    quantileTDigest(0.95)(query_duration_ms) AS p95_duration_ms,
    sum(read_rows) AS read_rows,
    sum(read_bytes) AS read_bytes,
    sum(result_rows) AS result_rows,
    sum(written_rows) AS written_rows,
    sum(memory_usage) AS total_memory_usage,
    max(memory_usage) AS peak_memory_usage,
    groupUniqArray(exception_code) AS exception_codes,
    any(exception) AS exception,
    countIf(type = 'ExceptionWhileProcessing') AS error_count
FROM system.query_log
WHERE event_date >= toDate({start_time:DateTime})
  AND event_date <= toDate({end_time:DateTime})
  AND event_time >= {start_time:DateTime}
  AND event_time <= {end_time:DateTime}
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
  AND user != 'copilot_dba'
  AND is_initial_query = 1
GROUP BY user, client_name, http_user_agent, initial_address
ORDER BY total_duration_ms DESC, read_bytes DESC, run_count DESC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=(
            "start_time",
            "end_time",
            "limit",
        ),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_largest_partitions.v1",
        1,
        "Rank active partitions by metadata in system.parts.",
        """SELECT
    database,
    table,
    partition,
    count() AS active_parts,
    sum(bytes_on_disk) AS total_bytes,
    sum(rows) AS total_rows
FROM system.parts
WHERE active = 1
  AND (empty({database:String}) OR database = {database:String})
  AND (empty({table:String}) OR table = {table:String})
GROUP BY database, table, partition
ORDER BY total_bytes DESC, database ASC, table ASC, partition ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
        optional_params=("database", "table"),
    )
)
_register_template(
    ClickHouseSQLTemplate(
        "clickhouse_largest_columns.v1",
        1,
        "Rank column storage from system.parts_columns metadata only.",
        """SELECT
    database,
    table,
    column,
    sum(column_data_compressed_bytes) AS compressed_bytes,
    sum(column_data_uncompressed_bytes) AS uncompressed_bytes,
    max(rows) AS max_rows,
    if(sum(column_data_compressed_bytes) = 0, null, sum(column_data_uncompressed_bytes) / sum(column_data_compressed_bytes)) AS compression_ratio
FROM system.parts_columns
WHERE active = 1
  AND (empty({database:String}) OR database = {database:String})
  AND (empty({table:String}) OR table = {table:String})
GROUP BY database, table, column
ORDER BY compressed_bytes DESC, database ASC, table ASC, column ASC
LIMIT {limit:UInt64}
FORMAT JSONEachRow""",
        required_params=("limit",),
        optional_params=("database", "table"),
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


_CLICKHOUSE_MEMORY_CACHE_METRICS = (
    "MemoryTracking",
    "MemoryTrackingForAllocations",
    "MemoryResident",
    "MemoryShared",
    "MemoryVirtual",
    "OSSMemoryAvailable",
    "OSSMemoryTotal",
    "MarkCacheBytes",
    "UncompressedCacheBytes",
    "CompressedReadBufferCacheBytes",
    "CacheSizeInBytes",
)
_CLICKHOUSE_HIT_MISS_METRICS = ("QueryCacheHits", "QueryCacheMisses")
_CLICKHOUSE_PART_SETTINGS = (
    "parts_to_delay_throw",
    "parts_to_throw_insert",
    "max_parts_to_delay_merge",
    "max_parts_to_merge_at_once",
)


def _clickhouse_timestamp(value: object) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).replace("Z", "+00:00").strip()
    for candidate in (text, text.split(".")[0].replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _clickhouse_number_or_none(row: Mapping[str, Any], *names: str) -> Optional[float]:
    for name in names:
        if name not in row or row[name] is None or str(row[name]).strip() == "":
            continue
        try:
            return float(row[name])
        except (TypeError, ValueError):
            continue
    return None


def _clickhouse_metric_map(rows: list[dict[str, Any]]) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for row in rows:
        name = _clickhouse_string(row, "metric", "name")
        value = _clickhouse_number_or_none(row, "value")
        if name and value is not None:
            metrics[name] = value
    return metrics


def _clickhouse_setting_map(rows: list[dict[str, Any]]) -> dict[str, str]:
    return {
        _clickhouse_string(row, "name"): _clickhouse_string(row, "value")
        for row in rows
        if _clickhouse_string(row, "name")
    }


def _clickhouse_parse_engine_settings(engine_full: object) -> dict[str, str]:
    """Extract only known scalar engine SETTINGS; never parse SQL into a command."""
    text = str(engine_full or "")
    settings: dict[str, str] = {}
    match = re.search(r"(?i)\bSETTINGS\s+(.*)$", text)
    if not match:
        return settings
    for item in re.finditer(r"(?i)([a-z_][a-z0-9_]*)\s*=\s*('[^']*'|[^\s,]+)", match.group(1)):
        name = item.group(1).lower()
        value = item.group(2).strip("'")
        if name in _CLICKHOUSE_PART_SETTINGS or name in {"index_granularity", "min_bytes_for_wide_part"}:
            settings[name] = value
    return settings


def _clickhouse_container_memory_limit(
    pod: Any,
    selected_container: Optional[str] = None,
) -> tuple[Optional[int], str]:
    from .tools import _container_memory_limit

    return _container_memory_limit(pod, selected_container)


def _clickhouse_pvc_capacity(
    v1: Any,
    pod: Any,
    disk_path: str,
) -> tuple[Optional[int], str]:
    """Match one ClickHouse disk path to a mounted PVC and return its capacity."""
    if not disk_path.startswith("/"):
        return None, "not an absolute disk path"
    mounts: list[tuple[str, str]] = []
    for container in getattr(getattr(pod, "spec", None), "containers", None) or []:
        for volume_mount in getattr(container, "volume_mounts", None) or []:
            claim_name = getattr(volume_mount, "name", None)
            if claim_name:
                mounts.append((str(volume_mount.mount_path).rstrip("/"), str(claim_name)))
    if not mounts:
        return None, "no volume mounts found"
    matches = [
        (mount_path, claim_name)
        for mount_path, claim_name in mounts
        if disk_path == mount_path or disk_path.startswith(f"{mount_path}/")
    ]
    if not matches:
        return None, "disk path does not match a container volume mount"
    if len(matches) > 1:
        return None, f"ambiguous with {len(matches)} volume mounts"
    _, claim_name = matches[0]
    namespace = str(getattr(getattr(pod, "metadata", None), "namespace", "") or "")
    pod_name = str(getattr(getattr(pod, "metadata", None), "name", "") or "")
    if not namespace:
        return None, "pod namespace unavailable"
    claims = v1.list_namespaced_persistent_volume_claim(
        namespace=namespace,
        field_selector=f"metadata.name={claim_name}",
    )
    items = list(getattr(claims, "items", None) or [])
    if not items:
        return None, f"PVC {claim_name} not found"
    spec = getattr(getattr(items[0], "spec", None), "resources", None)
    storage = dict(getattr(spec, "requests", None) or {}).get("storage")
    if storage is None:
        return None, f"PVC {claim_name} storage request unavailable"
    from .tools import _memory_quantity

    return _memory_quantity(storage), f"{storage} on PVC {claim_name} in {namespace}/{pod_name}"


def _clickhouse_report_query(
    namespace: str,
    pod_name: str,
    template_id: str,
    params: dict[str, Any],
    container: Optional[str],
    missing_hint: str,
) -> list[dict[str, Any]]:
    try:
        rows, _ = _run_clickhouse_query(namespace, pod_name, template_id, params, container)
        return rows
    except Exception as exc:
        message = str(exc).lower()
        if "unknown table" in message or "not found" in message or "no such table" in message:
            raise RuntimeError(missing_hint) from exc
        if "unknown expression identifier" in message or "unknown identifier" in message:
            raise RuntimeError(missing_hint) from exc
        raise


def _clickhouse_bytes(value: Optional[float]) -> str:
    if value is None:
        return "unknown"
    value = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    scaled = value
    index = 0
    while scaled >= 1024 and index < len(units) - 1:
        scaled /= 1024
        index += 1
    return f"{scaled:.1f}{units[index]}"


def _clickhouse_table_label(database: str, table: str, partition: Optional[str] = None) -> str:
    base = f"{database or 'unknown'}.{table or 'unknown'}"
    return f"{base} partition={partition}" if partition else base


def _clickhouse_percent(value: Optional[float]) -> str:
    return f"{value * 100:.1f}%" if value is not None else "unavailable"


def _clickhouse_age_text(value: Optional[datetime]) -> str:
    if value is None:
        return "unknown"
    seconds = max(0.0, (datetime.now(timezone.utc) - value).total_seconds())
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def _clickhouse_limit_setting(value: object) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _clickhouse_parts_evidence(
    namespace: str,
    pod_name: str,
    version: str,
    database: Optional[str],
    table: Optional[str],
    limit: int,
    container: Optional[str],
) -> tuple[
    list[str],
    dict[str, str],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, float],
]:
    """Collect every parts/merge/mutation table independently so one gap does not erase the rest."""
    filters = {"database": database or "", "table": table or "", "limit": limit}
    errors: list[str] = []
    settings_rows: list[dict[str, Any]] = []
    parts: list[dict[str, Any]] = []
    merges: list[dict[str, Any]] = []
    mutations: list[dict[str, Any]] = []
    detached: list[dict[str, Any]] = []
    part_log: list[dict[str, Any]] = []
    server_errors: list[dict[str, Any]] = []
    pool_metrics: dict[str, float] = {}

    def query(template_id: str, missing_hint: str, params: dict[str, Any] | None = None):
        return _clickhouse_report_query(
            namespace,
            pod_name,
            template_id,
            params or {},
            container,
            missing_hint,
        )

    for template_id, hint in (
        ("clickhouse_merge_tree_settings.v1", "system.merge_tree_settings is unavailable."),
        ("clickhouse_parts_summary.v1", "system.parts is unavailable or cannot be read."),
        ("clickhouse_merges.v1", "system.merges is unavailable or uses an older column layout."),
        ("clickhouse_mutations.v1", "system.mutations is unavailable or cannot be read."),
        ("clickhouse_detached_parts.v1", "system.detached_parts is unavailable or cannot be read."),
        ("clickhouse_part_log.v1", "system.part_log is disabled or unavailable."),
        ("clickhouse_parts_errors.v1", "system.errors is unavailable or cannot be read."),
        ("clickhouse_resource_metrics.v1", "system.metrics is unavailable or cannot be read."),
    ):
        try:
            rows = query(template_id, hint, filters)
            if template_id == "clickhouse_merge_tree_settings.v1":
                settings_rows = rows
            elif template_id == "clickhouse_parts_summary.v1":
                parts = rows
            elif template_id == "clickhouse_merges.v1":
                merges = rows
            elif template_id == "clickhouse_mutations.v1":
                mutations = rows
            elif template_id == "clickhouse_detached_parts.v1":
                detached = rows
            elif template_id == "clickhouse_part_log.v1":
                part_log = rows
            elif template_id == "clickhouse_parts_errors.v1":
                server_errors = rows
            else:
                pool_metrics = _clickhouse_metric_map(rows)
        except Exception as exc:
            errors.append(f"{template_id}: {exc}")
    return errors, _clickhouse_setting_map(settings_rows), parts, merges, mutations, detached, part_log, server_errors, pool_metrics


def _clickhouse_table_settings(
    namespace: str,
    pod_name: str,
    database: Optional[str],
    table: Optional[str],
    limit: int,
    container: Optional[str],
) -> tuple[dict[tuple[str, str], dict[str, Any]], list[str]]:
    try:
        rows = _clickhouse_report_query(
            namespace,
            pod_name,
            "clickhouse_table_metadata.v1",
            {"database": database or "", "table": table or "", "limit": limit},
            container,
            "system.tables is unavailable or cannot be read.",
        )
        return {(row.get("database"), row.get("table")): row for row in rows}, []
    except Exception as exc:
        return {}, [f"clickhouse_table_metadata.v1: {exc}"]


def _clickhouse_parts_findings(
    parts: list[dict[str, Any]],
    table_metadata: dict[tuple[str, str], dict[str, Any]],
    settings: dict[str, str],
    merges: list[dict[str, Any]],
    mutations: list[dict[str, Any]],
    detached: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    findings: list[str] = []
    recommendations: list[str] = []
    if parts:
        small_partitions = [row for row in parts if (_clickhouse_number_or_none(row, "avg_part_bytes") or 0) <= 8 * 1024 * 1024]
        if small_partitions:
            smallest = min(small_partitions, key=lambda row: _clickhouse_number_or_none(row, "avg_part_bytes") or 0)
            ratio = len(small_partitions) / len(parts)
            findings.append(
                f"Small-part evidence: {len(small_partitions)}/{len(parts)} partitions have average active part size "
                f"<= 8.0MiB; smallest is {_clickhouse_table_label(str(smallest.get('database', '')), str(smallest.get('table', '')), str(smallest.get('partition', '')))} "
                f"with avg={_clickhouse_bytes(_clickhouse_number_or_none(smallest, 'avg_part_bytes'))}."
            )
            recommendations.append(
                f"Batch inserts or consolidate small parts for high-partition tables. Evidence: {ratio * 100:.1f}% of "
                "reported partitions average <= 8.0MiB per active part. Changes require load modeling and owner review; nothing was applied."
            )
        partitions = sorted(parts, key=lambda row: _clickhouse_number_or_none(row, "active_parts") or 0, reverse=True)
        for row in partitions[:5]:
            key = (row.get("database"), row.get("table"))
            metadata = table_metadata.get(key, {})
            per_table = _clickhouse_parse_engine_settings(metadata.get("engine_full"))
            delay = _clickhouse_limit_setting(
                per_table.get("parts_to_delay_throw", settings.get("parts_to_delay_throw"))
            )
            throw = _clickhouse_limit_setting(
                per_table.get("parts_to_throw_insert", settings.get("parts_to_throw_insert"))
            )
            active = _clickhouse_number_or_none(row, "active_parts") or 0
            if throw is not None and active >= throw:
                findings.append(
                    f"Too many parts at throw threshold: {_clickhouse_table_label(str(row.get('database', '')), str(row.get('table', '')), str(row.get('partition', '')))} "
                    f"has {int(active)} active parts; parts_to_throw_insert={throw}."
                )
                recommendations.append(
                    f"Investigate insert pressure before writes for {_clickhouse_table_label(str(row.get('database', '')), str(row.get('table', '')), str(row.get('partition', '')))}. "
                    f"Evidence: active_parts={int(active)} and parts_to_throw_insert={throw}. This is advisory; do not issue OPTIMIZE or SYSTEM commands from this agent."
                )
            elif delay is not None and active >= delay:
                findings.append(
                    f"Too many parts at delay threshold: {_clickhouse_table_label(str(row.get('database', '')), str(row.get('table', '')), str(row.get('partition', '')))} "
                    f"has {int(active)} active parts; parts_to_delay_throw={delay}."
                )

    for row in merges[:5]:
        progress = _clickhouse_number_or_none(row, "progress")
        elapsed = _clickhouse_number_or_none(row, "elapsed") or 0
        size = _clickhouse_number_or_none(row, "total_size_bytes_compressed")
        source_count = len(row.get("source_part_names") or [])
        findings.append(
            f"Running merge: {_clickhouse_table_label(str(row.get('database', '')), str(row.get('table', '')))} "
            f"progress={_clickhouse_percent(progress)}; elapsed={elapsed:.1f}s; source_parts={source_count}; "
            f"output_size={_clickhouse_bytes(size)}."
        )
        if elapsed >= 3600:
            recommendations.append(
                "Review merge concurrency and partition size before changing background pools. Evidence: a reported merge has "
                f"elapsed={elapsed:.1f}s. Pool changes can increase load; no setting was changed."
            )

    for row in mutations[:10]:
        create_time = _clickhouse_timestamp(row.get("create_time"))
        age = _clickhouse_age_text(create_time)
        parts_to_do = _clickhouse_number_or_none(row, "parts_to_do") or 0
        fail_reason = _clickhouse_string(row, "latest_fail_reason")
        done = row.get("is_done")
        if fail_reason:
            findings.append(
                f"Failed mutation: {_clickhouse_table_label(str(row.get('database', '')), str(row.get('table', '')))} "
                f"mutation_id={row.get('mutation_id')}; age={age}; parts_to_do={int(parts_to_do)}; latest_fail_reason={fail_reason}."
            )
            recommendations.append(
                f"Investigate the failed mutation with the table owner before retrying. Evidence: mutation_id={row.get('mutation_id')} "
                f"has parts_to_do={int(parts_to_do)} and latest_fail_reason={fail_reason}. No KILL or retry command was issued."
            )
        elif not done and (create_time is not None and (datetime.now(timezone.utc) - create_time).total_seconds() >= 6 * 3600):
            state = "stuck" if (datetime.now(timezone.utc) - create_time).total_seconds() >= 24 * 3600 else "old"
            findings.append(
                f"{state.capitalize()} mutation: {_clickhouse_table_label(str(row.get('database', '')), str(row.get('table', '')))} "
                f"mutation_id={row.get('mutation_id')}; age={age}; parts_to_do={int(parts_to_do)}."
            )
            recommendations.append(
                f"Review merge/mutation capacity and the mutation's table context. Evidence: mutation_id={row.get('mutation_id')} "
                f"is {age} old with parts_to_do={int(parts_to_do)}. No mutation was canceled, retried, or forced."
            )

    if detached:
        grouped: dict[tuple[str, str, str], int] = {}
        for row in detached:
            key = (
                str(row.get("database", "unknown")),
                str(row.get("table", "unknown")),
                str(row.get("reason", "unknown")),
            )
            grouped[key] = grouped.get(key, 0) + 1
        for (database, table, reason), count in sorted(grouped.items()):
            findings.append(
                f"Detached parts: {database}.{table} reason={reason or 'unknown'} count={count}."
            )
            recommendations.append(
                f"Inspect detached parts with the table owner and confirm recovery procedures. Evidence: {database}.{table} has "
                f"{count} detached part(s) with reason={reason or 'unknown'}. No ATTACH, DROP, or filesystem command was issued."
            )
    return findings, recommendations


def get_clickhouse_parts_report(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    database: Optional[str] = None,
    table: Optional[str] = None,
    output_mode: str = "human",
    *,
    container: Optional[str] = None,
    label_selector: Optional[str] = None,
) -> str:
    """Report active parts, merges, mutations, detached parts, and part activity."""
    tool = "get_clickhouse_parts_report"
    mode = "human"
    try:
        mode = _clickhouse_output_mode(output_mode)
        limit = _clamp_clickhouse_number(200, 200, 1, 1000)
        if database is not None:
            _validate_clickhouse_identifier(database, "database")
        if table is not None:
            _validate_clickhouse_identifier(table, "table")
        from .tools import _load_kubernetes_client

        v1 = _load_kubernetes_client()
        pods = _clickhouse_pod_candidates(v1, namespace, pod, label_selector)
    except Exception as exc:
        return _clickhouse_tool_error(tool, f"Error preparing ClickHouse parts analysis: {exc}", mode)
    if not pods:
        return _clickhouse_tool_error(
            tool,
            "No ClickHouse server pods found. Use find_database_pods with 'clickhouse' to confirm labels.",
            mode,
        )

    sections: list[str] = []
    errors: list[str] = []
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
            table_metadata, metadata_errors = _clickhouse_table_settings(
                target_namespace,
                target_pod,
                database,
                table,
                limit,
                container,
            )
            evidence_errors, settings, parts, merges, mutations, detached, part_log, server_errors, pool_metrics = _clickhouse_parts_evidence(
                target_namespace,
                target_pod,
                version,
                database,
                table,
                limit,
                container,
            )
            errors.extend(f"{label} {error}" for error in [*metadata_errors, *evidence_errors])
            findings, recommendations = _clickhouse_parts_findings(
                parts,
                table_metadata,
                settings,
                merges,
                mutations,
                detached,
            )
            section = [
                f"### {label} (server {version})",
                f"- Evidence: system.parts, system.merge_tree_settings, system.merges, system.mutations, "
                f"system.detached_parts, system.part_log (when enabled), and system.errors; limit={limit}.",
                f"- Background pool: BackgroundMergesAndMutationsPoolTask={int(pool_metrics.get('BackgroundMergesAndMutationsPoolTask', 0))}; "
                f"BackgroundPoolTask={int(pool_metrics.get('BackgroundPoolTask', 0))}.",
                "",
            ]
            if findings:
                section.extend(["Evidence findings:", *(f"- {finding}" for finding in findings)])
            else:
                section.append("No part-count, running-merge, mutation, or detached-part threshold was crossed in the bounded evidence.")
            section.append("")
            section.append("Part activity (last 24h):")
            if part_log:
                for row in part_log[:10]:
                    event_count = _clickhouse_number_or_none(row, "event_count") or 0
                    bytes_changed = _clickhouse_number_or_none(row, "bytes_changed")
                    bytes_text = _clickhouse_bytes(bytes_changed)
                    throughput_text = (
                        f"avg_bytes_per_second={bytes_changed / (24 * 3600):.1f}; " if bytes_changed is not None else ""
                    )
                    section.append(
                        f"- {_clickhouse_table_label(str(row.get('database', '')), str(row.get('table', '')))} "
                        f"event_type={row.get('event_type') or 'unknown'} count={int(event_count)}; "
                        f"rate_per_hour={event_count / 24:.1f}; "
                        f"rows={int(_clickhouse_number_or_none(row, 'rows_changed') or 0)}; "
                        f"bytes={bytes_text}; {throughput_text}"
                        f"avg_duration_ms={_clickhouse_number_or_none(row, 'avg_duration_ms') or 0:.1f}."
                    )
            else:
                section.append("- unavailable; part_log may be disabled, absent, or outside the retained window.")
            if server_errors:
                section.extend(["Recent server errors:", ""])
                for row in server_errors[:10]:
                    section.append(
                        f"- {row.get('name') or 'unknown'} code={row.get('code')} count={row.get('count')}; "
                        f"message={_clickhouse_string(row, 'last_error_message', 'text')[:240]}."
                    )
            if recommendations:
                section.extend(["Evidence-linked advisory recommendations:", *(f"- {item}" for item in recommendations)])
            else:
                section.append("No recommendation is supported by this pod's evidence.")
            section.extend(
                [
                    "",
                    "All recommendations are advisory. No mutation, merge, detach, DROP, ALTER, SYSTEM command, or "
                    "ClickHouse setting was changed.",
                    "",
                ]
            )
            sections.append("\n".join(section))
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    if not sections and errors:
        return _clickhouse_tool_error(tool, "; ".join(errors), mode)
    report = ["ClickHouse parts, merges, and mutations analysis (advisory only):", "", *"\n".join(sections).splitlines()]
    if errors:
        report.extend(["Skipped or partial evidence:", *(f"- {error}" for error in errors), ""])
    report.append("No changes were made; this analysis is advisory only.")
    return _clickhouse_tool_output(
        tool,
        "\n".join(report),
        mode,
        metadata={
            "namespace": namespace,
            "pod": pod,
            "database": database,
            "table": table,
            "limit": limit,
        },
    )


def _clickhouse_resource_evidence(
    namespace: str,
    pod_name: str,
    database: Optional[str],
    table: Optional[str],
    top_n: int,
    container: Optional[str],
) -> tuple[
    list[str],
    dict[str, float],
    dict[str, float],
    dict[str, str],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[tuple[str, str], dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    filters = {"database": database or "", "table": table or "", "limit": top_n}
    errors: list[str] = []
    async_metrics: dict[str, float] = {}
    metrics: dict[str, float] = {}
    server_settings: dict[str, str] = {}
    disks: list[dict[str, Any]] = []
    policies: list[dict[str, Any]] = []
    tables: dict[tuple[str, str], dict[str, Any]] = {}
    largest_tables: list[dict[str, Any]] = []
    largest_partitions: list[dict[str, Any]] = []
    largest_columns: list[dict[str, Any]] = []

    def query(template_id: str, missing_hint: str):
        return _clickhouse_report_query(
            namespace,
            pod_name,
            template_id,
            filters,
            container,
            missing_hint,
        )

    for template_id, hint in (
        ("clickhouse_async_metrics.v1", "system.asynchronous_metrics is unavailable."),
        ("clickhouse_resource_metrics.v1", "system.metrics is unavailable."),
        ("clickhouse_server_settings.v1", "system.server_settings is unavailable."),
        ("clickhouse_disks.v1", "system.disks is unavailable."),
        ("clickhouse_storage_policies.v1", "system.storage_policies is unavailable."),
        ("clickhouse_largest_tables.v1", "system.parts is unavailable."),
        ("clickhouse_largest_partitions.v1", "system.parts is unavailable."),
        ("clickhouse_largest_columns.v1", "system.parts_columns is unavailable or does not expose column-size columns."),
    ):
        try:
            rows = query(template_id, hint)
            if template_id == "clickhouse_async_metrics.v1":
                async_metrics = _clickhouse_metric_map(rows)
            elif template_id == "clickhouse_resource_metrics.v1":
                metrics = _clickhouse_metric_map(rows)
            elif template_id == "clickhouse_server_settings.v1":
                server_settings = _clickhouse_setting_map(rows)
            elif template_id == "clickhouse_disks.v1":
                disks = rows
            elif template_id == "clickhouse_storage_policies.v1":
                policies = rows
            elif template_id == "clickhouse_largest_tables.v1":
                largest_tables = rows
            elif template_id == "clickhouse_largest_partitions.v1":
                largest_partitions = rows
            else:
                largest_columns = rows
        except Exception as exc:
            errors.append(f"{template_id}: {exc}")
    try:
        metadata_rows = query("clickhouse_table_metadata.v1", "system.tables is unavailable.")
        tables = {(row.get("database"), row.get("table")): row for row in metadata_rows}
    except Exception as exc:
        errors.append(f"clickhouse_table_metadata.v1: {exc}")
    return errors, async_metrics, metrics, server_settings, disks, policies, tables, largest_tables, largest_partitions, largest_columns


def _clickhouse_memory_quantity(value: object) -> Optional[int]:
    from .tools import _memory_quantity

    return _memory_quantity(value)


def _clickhouse_resource_findings(
    async_metrics: dict[str, float],
    metrics: dict[str, float],
    server_settings: dict[str, str],
    disks: list[dict[str, Any]],
    tables: dict[tuple[str, str], dict[str, Any]],
    largest_tables: list[dict[str, Any]],
    largest_columns: list[dict[str, Any]],
    kubernetes_limit: Optional[int],
    top_n: int,
) -> tuple[list[str], list[str]]:
    findings: list[str] = []
    recommendations: list[str] = []
    tracking = async_metrics.get("MemoryTrackingForAllocations", async_metrics.get("MemoryTracking"))
    resident = async_metrics.get("MemoryResident")
    available = async_metrics.get("OSSMemoryAvailable")
    os_total = async_metrics.get("OSSMemoryTotal")
    server_limit = _clickhouse_memory_quantity(server_settings.get("max_server_memory_usage"))
    if tracking is not None:
        findings.append(f"MemoryTracking={int(tracking)}.")
    if resident is not None:
        findings.append(f"MemoryResident={int(resident)}.")
    if available is not None:
        findings.append(f"OSSMemoryAvailable={int(available)}.")
    if os_total is not None:
        findings.append(f"OSSMemoryTotal={int(os_total)}.")
    if server_limit is not None:
        findings.append(f"max_server_memory_usage={_clickhouse_bytes(server_limit)}.")
    if available is not None and server_limit is not None:
        findings.append(
            f"Memory headroom evidence: OSSMemoryAvailable={int(available)}; server max_server_memory_usage={_clickhouse_bytes(server_limit)}."
        )

    observed_values = [value for value in (tracking, resident) if value is not None]
    limit_values = [value for value in (server_limit, kubernetes_limit) if value is not None]
    if observed_values and limit_values:
        observed = max(observed_values)
        active_limit = min(limit_values)
        ratio = observed / active_limit
        findings.append(
            f"Memory usage ratio={ratio * 100:.1f}% against the lower of server limit="
            f"{_clickhouse_bytes(server_limit)} and Kubernetes limit={_clickhouse_bytes(kubernetes_limit)}."
        )
        if ratio >= 0.8:
            recommendations.append(
                f"Investigate concurrency and query memory before increasing limits. Evidence: observed={_clickhouse_bytes(observed)} "
                f"is {ratio * 100:.1f}% of {_clickhouse_bytes(active_limit)}. Consider external aggregation/sorting only after "
                "workload review; no server setting was changed."
            )

    hits = metrics.get("QueryCacheHits", async_metrics.get("QueryCacheHits"))
    misses = metrics.get("QueryCacheMisses", async_metrics.get("QueryCacheMisses"))
    if hits is not None or misses is not None:
        total = (hits or 0) + (misses or 0)
        hit_rate = hits / total if total and hits is not None else None
        findings.append(
            f"Query cache hits/misses={hits if hits is not None else 'unknown'}/"
            f"{misses if misses is not None else 'unknown'}; hit_rate={_clickhouse_percent(hit_rate)}."
        )
    for name in ("MarkCacheBytes", "UncompressedCacheBytes", "CompressedReadBufferCacheBytes", "CacheSizeInBytes"):
        value = async_metrics.get(name)
        if value is not None:
            findings.append(f"Cache {name}={_clickhouse_bytes(value)}.")

    for row in disks[:10]:
        name = str(row.get("name") or "unknown")
        free = _clickhouse_number_or_none(row, "free_space")
        total = _clickhouse_number_or_none(row, "total_space")
        keep_free = _clickhouse_number_or_none(row, "keep_free_space")
        used_ratio = (total - free) / total if free is not None and total and total > 0 else None
        findings.append(
            f"Disk {name}: free={_clickhouse_bytes(free)}/{_clickhouse_bytes(total)}={_clickhouse_percent(used_ratio) if used_ratio is not None else 'used unknown'}; "
            f"keep_free_space={_clickhouse_bytes(keep_free)}; path={row.get('path') or 'unknown'}."
        )
        if used_ratio is not None and used_ratio >= 0.85:
            recommendations.append(
                f"Review disk pressure and retention for ClickHouse disk {name}. Evidence: used_ratio={used_ratio * 100:.1f}%. "
                "Do not detach or delete parts from this agent; changes require owner approval."
            )
        if keep_free is not None and free is not None and free <= keep_free:
            recommendations.append(
                f"Disk {name} is at or below keep_free_space. Evidence: free={_clickhouse_bytes(free)}, "
                f"keep_free_space={_clickhouse_bytes(keep_free)}. Investigate retention and tiering with the storage owner; nothing was changed."
            )

    if largest_columns:
        top = largest_columns[0]
        compressed = _clickhouse_number_or_none(top, "compressed_bytes")
        uncompressed = _clickhouse_number_or_none(top, "uncompressed_bytes")
        compression = uncompressed / compressed if compressed else None
        compression_text = f"{compression:.2f}:1" if compression is not None else "unavailable"
        findings.append(
            f"Largest reported column: {top.get('database') or 'unknown'}.{top.get('table') or 'unknown'} "
            f"column={top.get('column') or 'unknown'}; compressed={_clickhouse_bytes(compressed)}; "
            f"compression_ratio={compression_text}; max_rows={int(_clickhouse_number_or_none(top, 'max_rows') or 0)}."
        )
        if compression is not None and compression < 2.0:
            recommendations.append(
                f"Review column codec and cardinality before changing storage. Evidence: compression_ratio={compression:.2f}:1 for "
                f"{top.get('column')}. Test codecs offline; no ALTER or data scan was performed."
            )

    for row in largest_tables[:top_n]:
        metadata = tables.get((row.get("database"), row.get("table")), {})
        has_ttl = bool(re.search(r"(?i)\bTTL\b", str(metadata.get("engine_full") or "")))
        total_bytes = _clickhouse_number_or_none(row, "total_bytes")
        if total_bytes is not None and total_bytes >= 100 * 1024 * 1024 * 1024 and not has_ttl:
            recommendations.append(
                f"Review retention or tiering for {row.get('database')}.{row.get('table')}. Evidence: total_bytes="
                f"{_clickhouse_bytes(total_bytes)} and no TTL expression in system.tables.engine_full. Data deletion requires owner approval."
            )
    return findings, recommendations


def get_clickhouse_resource_report(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    top_n: int = 20,
    output_mode: str = "human",
    *,
    container: Optional[str] = None,
    label_selector: Optional[str] = None,
) -> str:
    """Report ClickHouse memory, disks, storage tiers, and metadata-only storage leaders."""
    tool = "get_clickhouse_resource_report"
    mode = "human"
    try:
        mode = _clickhouse_output_mode(output_mode)
        top_n = _clamp_clickhouse_number(top_n, 20, 1, 100)
        from .tools import _load_kubernetes_client

        v1 = _load_kubernetes_client()
        pods = _clickhouse_pod_candidates(v1, namespace, pod, label_selector)
    except Exception as exc:
        return _clickhouse_tool_error(tool, f"Error preparing ClickHouse resource analysis: {exc}", mode)
    if not pods:
        return _clickhouse_tool_error(
            tool,
            "No ClickHouse server pods found. Use find_database_pods with 'clickhouse' to confirm labels.",
            mode,
        )

    sections: list[str] = []
    errors: list[str] = []
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
            evidence_errors, async_metrics, metrics, server_settings, disks, policies, tables, largest_tables, largest_partitions, largest_columns = _clickhouse_resource_evidence(
                target_namespace,
                target_pod,
                None,
                None,
                top_n,
                container,
            )
            errors.extend(f"{label} {error}" for error in evidence_errors)
            memory_limit, memory_limit_text = _clickhouse_container_memory_limit(pod_item, container)
            findings, recommendations = _clickhouse_resource_findings(
                async_metrics,
                metrics,
                server_settings,
                disks,
                tables,
                largest_tables,
                largest_columns,
                memory_limit,
                top_n,
            )
            findings.append(
                f"Kubernetes container memory limit={_clickhouse_bytes(memory_limit)} ({memory_limit_text})."
            )
            section = [
                f"### {label} (server {version})",
                f"- Evidence: system.asynchronous_metrics, system.metrics, system.server_settings, system.disks, "
                f"system.storage_policies, system.tables, system.parts, and system.parts_columns (when available).",
                "",
            ]
            if findings:
                section.extend(["Evidence findings:", *(f"- {finding}" for finding in findings)])
            else:
                section.append("No resource threshold was crossed in the available evidence.")
            if largest_tables:
                section.extend(["", f"Largest active tables (top {len(largest_tables)}):"])
                for row in largest_tables:
                    metadata = tables.get((row.get("database"), row.get("table")), {})
                    engine = metadata.get("engine", "unknown")
                    section.append(
                        f"- {_clickhouse_table_label(str(row.get('database', '')), str(row.get('table', '')))} "
                        f"engine={engine}; bytes={_clickhouse_bytes(_clickhouse_number_or_none(row, 'total_bytes'))}; "
                        f"rows={int(_clickhouse_number_or_none(row, 'total_rows') or 0)}; "
                        f"active_parts={int(_clickhouse_number_or_none(row, 'active_parts') or 0)}."
                    )
            if largest_partitions:
                section.extend(["", f"Largest active partitions (top {len(largest_partitions)}):"])
                for row in largest_partitions:
                    section.append(
                        f"- {_clickhouse_table_label(str(row.get('database', '')), str(row.get('table', '')), str(row.get('partition', '')))} "
                        f"bytes={_clickhouse_bytes(_clickhouse_number_or_none(row, 'total_bytes'))}; "
                        f"rows={int(_clickhouse_number_or_none(row, 'total_rows') or 0)}."
                    )
            if policies:
                section.extend(["", "Storage policies:"])
                for row in policies:
                    section.append(
                        f"- policy={row.get('policy_name') or 'unknown'}; volume={row.get('volume_name') or 'unknown'}; "
                        f"disk={row.get('disk_name') or 'unknown'}; move_factor={row.get('move_factor') or 'unknown'}."
                    )
            disks_with_paths = [row for row in disks if row.get("path")]
            if disks_with_paths:
                section.extend(["", "PVC capacity:"])
                for row in disks_with_paths:
                    disk_name = str(row.get("name") or "unknown")
                    capacity, capacity_text = _clickhouse_pvc_capacity(v1, pod_item, str(row.get("path")))
                    section.append(f"- {disk_name}: {_clickhouse_bytes(capacity)} ({capacity_text}).")
            if recommendations:
                section.extend(["Evidence-linked advisory recommendations:", *(f"- {item}" for item in recommendations)])
            else:
                section.append("No recommendation is supported by this pod's evidence.")
            section.extend(
                [
                    "",
                    "All recommendations are advisory. No configuration, disk, part, TTL, codec, cache, or Kubernetes "
                    "resource was changed.",
                    "",
                ]
            )
            sections.append("\n".join(section))
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    if not sections and errors:
        return _clickhouse_tool_error(tool, "; ".join(errors), mode)
    report = ["ClickHouse resource and storage analysis (advisory only):", "", *"\n".join(sections).splitlines()]
    if errors:
        report.extend(["Skipped or partial evidence:", *(f"- {error}" for error in errors), ""])
    report.append("No changes were made; this analysis is advisory only.")
    return _clickhouse_tool_output(
        tool,
        "\n".join(report),
        mode,
        metadata={"namespace": namespace, "pod": pod, "top_n": top_n},
    )

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


def _clickhouse_redacted_text(value: object, limit: int = 160) -> str:
    text = " ".join(str(value or "").split())
    text = _CLICKHOUSE_SECRET_TEXT_RE.sub(lambda match: f"{match.group(1)}=[redacted]", text)
    text = "".join(char if char.isprintable() or char.isspace() else " " for char in text)
    return text[:limit] + ("..." if len(text) > limit else "")


def _clickhouse_is_true(value: object) -> bool:
    if isinstance(value, bool):
        return value
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return str(value).strip().lower() in {"true", "yes", "on"}


def _clickhouse_identity_equal(actual: object, hint: object) -> bool:
    actual_text = str(actual or "").strip()
    hint_text = str(hint or "").strip()
    if not actual_text or not hint_text:
        return False
    try:
        return int(actual_text) == int(hint_text)
    except ValueError:
        return actual_text.lower() == hint_text.lower()


def _clickhouse_topology_evidence(
    hints: dict[str, Any],
    rows: list[dict[str, Any]],
    cluster: Optional[str],
) -> list[str]:
    lines = []
    local_rows = [row for row in rows if _clickhouse_is_true(row.get("is_local"))]
    if cluster:
        rows = [row for row in rows if str(row.get("cluster", "")) == cluster]
        if not rows:
            lines.append(f"- Requested runtime cluster {cluster!r} is not present in local system.clusters.")
    configured_clusters = sorted({str(row.get("cluster", "")) for row in rows if row.get("cluster")})
    lines.append(
        f"- Runtime clusters visible locally: {', '.join(configured_clusters) or 'none'}; "
        f"local server rows: {len(local_rows)}."
    )

    for row in local_rows:
        runtime_cluster = str(row.get("cluster", "unknown"))
        if cluster and runtime_cluster != cluster:
            lines.append(
                f"- Local runtime identity is cluster={runtime_cluster}, shard={row.get('shard_num')}, "
                f"replica={row.get('replica_num')}; it is outside requested cluster {cluster!r}."
            )
            continue
        shard_rows = [
            member
            for member in rows
            if str(member.get("cluster", "")) == runtime_cluster
            and str(member.get("shard_num", "")) == str(row.get("shard_num", ""))
        ]
        members = ", ".join(
            f"{_clickhouse_redacted_text(member.get('host_name'))}:"
            f"{_clickhouse_redacted_text(member.get('port'))}"
            f"{' (local)' if _clickhouse_is_true(member.get('is_local')) else ''}"
            for member in shard_rows[:8]
        )
        lines.append(
            f"- Runtime membership: cluster={runtime_cluster}; shard={row.get('shard_num')}; "
            f"local_replica={row.get('replica_num')}; configured_replicas={len(shard_rows)}; "
            f"members={members or 'unavailable'}."
        )

    label_cluster = hints.get("cluster")
    label_shard = hints.get("shard")
    label_replica = hints.get("replica")
    generic_hints = [
        f"statefulset={hints['statefulset']}" if hints.get("statefulset") else "",
        f"ordinal={hints['ordinal']}" if hints.get("ordinal") is not None else "",
        f"component={hints['component']}" if hints.get("component") else "",
        f"instance={hints['instance']}" if hints.get("instance") else "",
    ]
    lines.append(
        f"- Label hints: altinity_cluster={label_cluster or 'absent'}; shard={label_shard or 'absent'}; "
        f"replica={label_replica or 'absent'}; generic={', '.join(item for item in generic_hints if item) or 'none'}."
    )

    for local in local_rows:
        runtime_cluster = str(local.get("cluster", ""))
        mismatches: list[str] = []
        if label_cluster and not _clickhouse_identity_equal(runtime_cluster, label_cluster):
            mismatches.append(f"cluster label {label_cluster!r} != runtime {runtime_cluster!r}")
        if label_shard and not _clickhouse_identity_equal(local.get("shard_num"), label_shard):
            mismatches.append(f"shard label {label_shard!r} != runtime {local.get('shard_num')!r}")
        if label_replica and not _clickhouse_identity_equal(local.get("replica_num"), label_replica):
            mismatches.append(f"replica label {label_replica!r} != runtime {local.get('replica_num')!r}")
        if mismatches:
            lines.append(f"- Topology mismatch: {'; '.join(mismatches)}. Runtime system.clusters is authoritative.")
        else:
            lines.append("- Topology check: no label/runtime mismatch was observed for supplied hints.")
    return lines


def _clickhouse_replica_health_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- system.replicas: no replicated tables visible to this replica-local user."]
    lines = [f"- Replicated tables: {len(rows)}; active replicas reported: {sum(int(_clickhouse_number(row, 'active_replicas')) for row in rows)}."]
    readonly_count = 0
    expired_count = 0
    for row in rows:
        readonly = _clickhouse_is_true(row.get("is_readonly"))
        expired = _clickhouse_is_true(row.get("is_session_expired"))
        readonly_count += int(readonly)
        expired_count += int(expired)
        lines.append(
            f"  - {_clickhouse_redacted_text(row.get('database'))}.{_clickhouse_redacted_text(row.get('table'))} "
            f"replica={_clickhouse_redacted_text(row.get('replica_name'))}; readonly={'yes' if readonly else 'no'}; "
            f"session_expired={'yes' if expired else 'no'}; absolute_delay={int(_clickhouse_number(row, 'absolute_delay'))}s; "
            f"queue_size={int(_clickhouse_number(row, 'queue_size'))}; "
            f"inserts/merges={int(_clickhouse_number(row, 'inserts_in_queue'))}/"
            f"{int(_clickhouse_number(row, 'merges_in_queue'))}; "
            f"log_gap={int(_clickhouse_number(row, 'log_max_index') - _clickhouse_number(row, 'log_pointer'))}; "
            f"active/total={int(_clickhouse_number(row, 'active_replicas'))}/"
            f"{int(_clickhouse_number(row, 'total_replicas'))}; lost_parts={int(_clickhouse_number(row, 'lost_parts'))}; "
            f"recovery_time={row.get('recovery_time', 'unavailable')}."
        )
        exception = row.get("last_queue_update_exception")
        if exception:
            lines.append(f"    last_queue_update_exception={_clickhouse_query_text(exception)}")
    if readonly_count:
        lines.append(f"- Read-only replicated tables: {readonly_count}. Investigate Keeper state and table permissions; no action was taken.")
    if expired_count:
        lines.append(f"- Expired Keeper sessions: {expired_count}. Confirm Keeper connectivity before resuming writes; no action was taken.")
    delayed = [row for row in rows if _clickhouse_number(row, "absolute_delay") >= 30]
    queued = [row for row in rows if _clickhouse_number(row, "queue_size") >= 10]
    if delayed:
        lines.append(f"- Replication delay >=30s on {len(delayed)} table(s); compare each shard's replicas.")
    if queued:
        lines.append(f"- Replication queue >=10 on {len(queued)} table(s); review oldest entries and queue pressure.")
    return lines


def _clickhouse_queue_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- system.replication_queue: empty or unavailable."]
    lines = [f"- Replication queue entries (bounded): {len(rows)}."]
    for row in rows[:10]:
        lines.append(
            f"  - {_clickhouse_redacted_text(row.get('database'))}.{_clickhouse_redacted_text(row.get('table'))} "
            f"replica={_clickhouse_redacted_text(row.get('replica_name'))}; node={_clickhouse_redacted_text(row.get('node_name'))}; "
            f"type={_clickhouse_redacted_text(row.get('type'))}; size={int(_clickhouse_number(row, 'size'))}; "
            f"attempts={int(_clickhouse_number(row, 'attempts'))}; postpone_reason={_clickhouse_redacted_text(row.get('postpone_reason')) or 'none'}; "
            f"last_exception={_clickhouse_query_text(row.get('last_exception')) or 'none'}."
        )
    return lines


def _clickhouse_distribution_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- system.distribution_queue: no pending Distributed files visible or unavailable."]
    total_files = sum(int(_clickhouse_number(row, "data_files")) for row in rows)
    total_bytes = sum(_clickhouse_number(row, "bytes_to_send") for row in rows)
    lines = [f"- Distributed queue pressure: files={total_files}; bytes={int(total_bytes)} across {len(rows)} bounded row(s)."]
    for row in rows[:10]:
        lines.append(
            f"  - {_clickhouse_redacted_text(row.get('database'))}.{_clickhouse_redacted_text(row.get('table'))} "
            f"address={_clickhouse_redacted_text(row.get('address'))}; files={int(_clickhouse_number(row, 'data_files'))}; "
            f"compressed_files={int(_clickhouse_number(row, 'data_compressed_files'))}; bytes_to_send={int(_clickhouse_number(row, 'bytes_to_send'))}; "
            f"errors={int(_clickhouse_number(row, 'errors_count'))}; last_exception={_clickhouse_query_text(row.get('last_exception')) or 'none'}."
        )
    return lines


def _clickhouse_keeper_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- system.zookeeper_connection: unavailable or no connections visible."]
    lines = [f"- Keeper/ZooKeeper connections (bounded): {len(rows)}."]
    for row in rows:
        connected = _clickhouse_is_true(row.get("is_connected"))
        lines.append(
            f"  - name={_clickhouse_redacted_text(row.get('name'))}; host={_clickhouse_redacted_text(row.get('host'))}; "
            f"port={_clickhouse_redacted_text(row.get('port'))}; connected={'yes' if connected else 'no'}; "
            f"latency_ms={int(_clickhouse_number(row, 'latency_ms'))}; errors={int(_clickhouse_number(row, 'errors_count'))}; "
            f"timeouts={int(_clickhouse_number(row, 'timeout_count'))}."
        )
        if not connected:
            lines.append("    Lost Keeper session observed. This is replica-local evidence; confirm all Keeper hosts before changing anything.")
    return lines


def _clickhouse_replication_recommendations(
    replica_rows: list[dict[str, Any]],
    queue_rows: list[dict[str, Any]],
    distribution_rows: list[dict[str, Any]],
    keeper_rows: list[dict[str, Any]],
) -> list[str]:
    recommendations: list[str] = []
    for row in replica_rows:
        label = f"{row.get('database')}.{row.get('table')}"
        if _clickhouse_is_true(row.get("is_readonly")):
            recommendations.append(
                f"- Review read-only state for {label}; evidence: is_readonly=1. Check Keeper quorum and permissions before any change."
            )
        if _clickhouse_number(row, "absolute_delay") >= 30:
            recommendations.append(
                f"- Investigate replication lag for {label}; evidence: absolute_delay={int(_clickhouse_number(row, 'absolute_delay'))}s."
            )
        if _clickhouse_number(row, "queue_size") >= 10:
            recommendations.append(
                f"- Review replication backlog for {label}; evidence: queue_size={int(_clickhouse_number(row, 'queue_size'))}."
            )
        if _clickhouse_number(row, "lost_parts"):
            recommendations.append(
                f"- Investigate lost parts for {label}; evidence: lost_parts={int(_clickhouse_number(row, 'lost_parts'))}."
            )
    oldest = queue_rows[0] if queue_rows else None
    if oldest:
        recommendations.append(
            f"- Review oldest replication-queue entry {_clickhouse_redacted_text(oldest.get('node_name'))}; "
            f"evidence: attempts={int(_clickhouse_number(oldest, 'attempts'))}, "
            f"postpone_reason={_clickhouse_redacted_text(oldest.get('postpone_reason')) or 'none'}."
        )
    if sum(_clickhouse_number(row, "data_files") for row in distribution_rows) >= 10:
        recommendations.append(
            f"- Review Distributed queue pressure; evidence: pending files={sum(int(_clickhouse_number(row, 'data_files')) for row in distribution_rows)}."
        )
    if any(not _clickhouse_is_true(row.get("is_connected")) for row in keeper_rows):
        recommendations.append("- Investigate lost Keeper session; evidence: system.zookeeper_connection reports is_connected=false.")
    return recommendations


def _collect_clickhouse_replication_evidence(
    namespace: str,
    pod: str,
    container: Optional[str],
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    evidence: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    templates = (
        ("clusters", "clickhouse_topology.v1"),
        ("replicas", "clickhouse_replicas.v1"),
        ("queue", "clickhouse_replication_queue.v1"),
        ("distribution", "clickhouse_distribution_queue.v1"),
        ("keeper", "clickhouse_keeper_connection.v1"),
    )
    for key, template_id in templates:
        try:
            rows, _ = _run_clickhouse_query(namespace, pod, template_id, {}, container)
            evidence[key] = rows
        except Exception as exc:
            evidence[key] = []
            errors.append(f"{template_id}: {exc}")
    return evidence, errors


def _clickhouse_metrics_map(rows: list[dict[str, Any]]) -> dict[str, float]:
    return {str(row.get("metric", "")): _clickhouse_number(row, "value") for row in rows if row.get("metric")}


def _clickhouse_setting_map(rows: list[dict[str, Any]]) -> dict[str, str]:
    return {str(row.get("name", "")): _clickhouse_redacted_text(row.get("value")) for row in rows if row.get("name")}


def _clickhouse_workload_recommendations(
    metrics: dict[str, float],
    process_rows: list[dict[str, Any]],
    settings: dict[str, str],
    error_rows: list[dict[str, Any]],
    quota_rows: list[dict[str, Any]],
    query_rows: list[dict[str, Any]],
) -> list[str]:
    recommendations: list[str] = []
    for row in process_rows:
        if _clickhouse_number(row, "elapsed") >= 300:
            recommendations.append(
                f"- Review long-running query {row.get('query_id')}; evidence: elapsed={_clickhouse_number(row, 'elapsed'):.3f}s."
            )
        if _clickhouse_number(row, "memory_usage") >= 1024 * 1024 * 1024:
            recommendations.append(
                f"- Review memory pressure for query {row.get('query_id')}; evidence: memory_usage={int(_clickhouse_number(row, 'memory_usage'))}."
            )
    for row in quota_rows:
        for used, maximum in (
            ("num_queries", "max_queries"),
            ("read_bytes", "max_read_bytes"),
            ("read_rows", "max_read_rows"),
            ("exceptions", "max_exceptions"),
        ):
            max_value = _clickhouse_number(row, maximum)
            used_value = _clickhouse_number(row, used)
            if max_value > 0 and used_value >= max_value:
                recommendations.append(
                    f"- Review quota {_clickhouse_redacted_text(row.get('quota_name'))}; evidence: {used}={int(used_value)} "
                    f"reached max={int(max_value)}. Coordinate with the owner before changing quotas."
                )
    for name in ("max_concurrent_queries", "max_concurrent_queries_for_all_users", "max_concurrent_queries_for_user"):
        try:
            maximum = float(settings.get(name, "0"))
        except ValueError:
            continue
        if maximum > 0 and metrics.get("Query", 0.0) >= maximum:
            recommendations.append(
                f"- Review concurrency limits and client pool sizing; evidence: Query={int(metrics.get('Query', 0))}, {name}={int(maximum)}."
            )
    if error_rows:
        recommendations.append(
            f"- Review top server errors; evidence: system.errors reports {len(error_rows)} bounded row(s) with nonzero cumulative counts."
        )
    failed_queries = sum(int(_clickhouse_number(row, "error_count")) for row in query_rows)
    if failed_queries:
        recommendations.append(
            f"- Review recent failed query groups; evidence: error_count={failed_queries} in the selected query_log window."
        )
    return recommendations


def _collect_clickhouse_workload_evidence(
    namespace: str,
    pod: str,
    minutes: int,
    limit: int,
    container: Optional[str],
) -> tuple[dict[str, Any], list[str]]:
    evidence: dict[str, Any] = {}
    errors: list[str] = []
    optional_templates = (
        ("metrics", "clickhouse_metrics.v1", {}),
        ("processes", "clickhouse_workload_processes.v1", {"limit": limit}),
        ("settings", "clickhouse_settings.v1", {}),
        ("errors", "clickhouse_errors.v1", {"limit": limit}),
        ("quotas", "clickhouse_quotas_usage.v1", {}),
    )
    try:
        profile_rows, _ = _run_clickhouse_query(
            namespace, pod, "clickhouse_readonly_profile.v1", {}, container
        )
        evidence["profile"] = profile_rows
    except Exception as exc:
        evidence["profile"] = []
        errors.append(f"clickhouse_readonly_profile.v1: {exc}")
    for key, template_id, params in optional_templates:
        try:
            rows, _ = _run_clickhouse_query(namespace, pod, template_id, params, container)
            evidence[key] = rows
        except Exception as exc:
            evidence[key] = []
            errors.append(f"{template_id}: {exc}")
    try:
        start, end = _clickhouse_time_window(minutes)
        params = {
            "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
            "limit": limit,
        }
        rows, _ = _run_clickhouse_query(
            namespace, pod, "clickhouse_workload_query_log.v1", params, container
        )
        evidence["query_log"] = rows
    except Exception as exc:
        evidence["query_log"] = []
        errors.append(f"clickhouse_workload_query_log.v1: {exc}")
    return evidence, errors


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


def get_clickhouse_replication_report(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    cluster: Optional[str] = None,
    output_mode: str = "human",
    *,
    container: Optional[str] = None,
    label_selector: Optional[str] = None,
) -> str:
    """Report replica-local ClickHouse replication and topology evidence read-only."""
    tool = "get_clickhouse_replication_report"
    mode = "human"
    try:
        mode = _clickhouse_output_mode(output_mode)
        if cluster is not None and (not isinstance(cluster, str) or not cluster.strip()):
            raise ValueError("cluster must be a non-empty string or None.")
        from .tools import _load_kubernetes_client

        v1 = _load_kubernetes_client()
        pods = _clickhouse_pod_candidates(v1, namespace, pod, label_selector)
    except Exception as exc:
        return _clickhouse_tool_error(tool, f"Error preparing ClickHouse replication report: {exc}", mode)

    if not pods:
        return _clickhouse_tool_error(
            tool,
            "No ClickHouse server pods found. Use find_database_pods with 'clickhouse' to confirm labels.",
            mode,
        )
    if len(pods) > _CLICKHOUSE_MAX_REPLICATION_PODS:
        pods = pods[:_CLICKHOUSE_MAX_REPLICATION_PODS]

    sections: list[str] = []
    errors: list[str] = []
    for pod_item in pods[:_CLICKHOUSE_MAX_REPLICATION_PODS]:
        target_namespace = pod_item.metadata.namespace
        target_pod = pod_item.metadata.name
        label = f"{target_namespace}/{target_pod}"
        hints = _clickhouse_topology_hints(pod_item)
        try:
            evidence, evidence_errors = _collect_clickhouse_replication_evidence(
                target_namespace, target_pod, container
            )
            replica_rows = evidence.get("replicas", [])
            queue_rows = evidence.get("queue", [])
            distribution_rows = evidence.get("distribution", [])
            keeper_rows = evidence.get("keeper", [])
            recommendations = _clickhouse_replication_recommendations(
                replica_rows, queue_rows, distribution_rows, keeper_rows
            )
            section = [
                f"### {label} (replica-local)",
                *_clickhouse_topology_evidence(
                    hints, evidence.get("clusters", []), cluster
                ),
                *_clickhouse_replica_health_lines(replica_rows),
                *_clickhouse_queue_lines(queue_rows),
                *_clickhouse_distribution_lines(distribution_rows),
                *_clickhouse_keeper_lines(keeper_rows),
                "",
            ]
            if evidence_errors:
                section.extend(["Unreadable or malformed evidence:", *(f"- {error}" for error in evidence_errors), ""])
            if recommendations:
                section.extend(["Evidence-linked advisory recommendations:"])
                section.extend(recommendations)
            else:
                section.append("No replication recommendation is supported by this pod's evidence.")
            section.extend(
                [
                    "",
                    "This report is replica-local. Cluster-wide conclusions require repeating the check on each shard or replica; "
                    "Kubernetes labels are hints and system.clusters is the runtime evidence.",
                    "All recommendations are advisory. No query, configuration, Keeper state, or ClickHouse state was changed.",
                    "",
                ]
            )
            sections.append("\n".join(section))
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    if not sections and errors:
        return _clickhouse_tool_error(tool, "; ".join(errors), mode)
    report = [
        "ClickHouse replication and topology analysis (advisory only):",
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
            "cluster": cluster,
            "pods_reported": len(sections),
            "pods_limit": _CLICKHOUSE_MAX_REPLICATION_PODS,
            "label_selector": label_selector,
        },
    )


def get_clickhouse_workload_report(
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    minutes: int = 60,
    output_mode: str = "human",
    *,
    container: Optional[str] = None,
    label_selector: Optional[str] = None,
    limit: int = 25,
) -> str:
    """Report bounded ClickHouse workload, concurrency, and error evidence read-only."""
    tool = "get_clickhouse_workload_report"
    mode = "human"
    try:
        mode = _clickhouse_output_mode(output_mode)
        minutes = _clamp_clickhouse_number(minutes, 60, 1, _CLICKHOUSE_MAX_MINUTES)
        limit = _clamp_clickhouse_number(limit, 25, 1, _CLICKHOUSE_MAX_LIMIT)
        from .tools import _load_kubernetes_client

        v1 = _load_kubernetes_client()
        pods = _clickhouse_pod_candidates(v1, namespace, pod, label_selector)
    except Exception as exc:
        return _clickhouse_tool_error(tool, f"Error preparing ClickHouse workload report: {exc}", mode)

    if not pods:
        return _clickhouse_tool_error(
            tool,
            "No ClickHouse server pods found. Use find_database_pods with 'clickhouse' to confirm labels.",
            mode,
        )
    if len(pods) > _CLICKHOUSE_MAX_REPLICATION_PODS:
        pods = pods[:_CLICKHOUSE_MAX_REPLICATION_PODS]

    sections: list[str] = []
    errors: list[str] = []
    for pod_item in pods[:_CLICKHOUSE_MAX_REPLICATION_PODS]:
        target_namespace = pod_item.metadata.namespace
        target_pod = pod_item.metadata.name
        label = f"{target_namespace}/{target_pod}"
        try:
            evidence, evidence_errors = _collect_clickhouse_workload_evidence(
                target_namespace, target_pod, minutes, limit, container
            )
            profile_rows = evidence.get("profile", [])
            version = _clickhouse_string(profile_rows[0], "version", default="unknown") if profile_rows else "unknown"
            metrics = _clickhouse_metrics_map(evidence.get("metrics", []))
            process_rows = evidence.get("processes", [])
            settings = _clickhouse_setting_map(evidence.get("settings", []))
            error_rows = evidence.get("errors", [])
            quota_rows = evidence.get("quotas", [])
            query_rows = evidence.get("query_log", [])
            recommendations = _clickhouse_workload_recommendations(
                metrics, process_rows, settings, error_rows, quota_rows, query_rows
            )
            section = [
                f"### {label} (server {version})",
                "- Evidence: system.metrics, system.processes, system.settings, system.errors, "
                "system.quotas_usage, and system.query_log; each source is optional and bounded.",
            ]
            metric_names = (
                "Query",
                "Merge",
                "PoolTask",
                "BackgroundPoolTask",
                "BackgroundSchedulePoolTask",
                "BackgroundProcessingPoolTask",
                "TCPConnection",
                "HTTPConnection",
                "InterserverConnection",
                "MySQLConnection",
                "PostgreSQLConnection",
            )
            metrics_text = ", ".join(f"{name}={int(metrics.get(name, 0.0))}" for name in metric_names)
            section.append(f"- Server counters: {metrics_text}.")
            settings_text = (
                ", ".join(f"{name}={settings[name]}" for name in sorted(settings)) or "concurrency settings unavailable"
            )
            section.append(f"- Concurrency settings: {settings_text}.")
            section.append(f"- Running-query snapshot (bounded): {len(process_rows)} row(s).")
            for row in process_rows[:10]:
                section.append(
                    f"  - query_id={row.get('query_id')}; user={_clickhouse_redacted_text(row.get('user'))}; "
                    f"kind={_clickhouse_redacted_text(row.get('query_kind'))}; elapsed={_clickhouse_number(row, 'elapsed'):.3f}s; "
                    f"read_rows/bytes={int(_clickhouse_number(row, 'read_rows'))}/{int(_clickhouse_number(row, 'read_bytes'))}; "
                    f"memory_usage={int(_clickhouse_number(row, 'memory_usage'))}; "
                    f"client={_clickhouse_redacted_text(row.get('client_name')) or 'unknown'}; "
                    f"http_user_agent={_clickhouse_redacted_text(row.get('http_user_agent')) or 'unknown'}; "
                    f"initial_address={_clickhouse_redacted_text(row.get('initial_address')) or 'unknown'}; "
                    f"query={_clickhouse_query_text(row.get('normalized_query')) or 'unavailable'}."
                )
            section.append(f"- Recent load by attribution (bounded): {len(query_rows)} row(s).")
            for row in query_rows[:10]:
                section.append(
                    f"  - user={_clickhouse_redacted_text(row.get('user')) or 'unknown'}; "
                    f"client={_clickhouse_redacted_text(row.get('client_name')) or 'unknown'}; "
                    f"http_user_agent={_clickhouse_redacted_text(row.get('http_user_agent')) or 'unknown'}; "
                    f"initial_address={_clickhouse_redacted_text(row.get('initial_address')) or 'unknown'}; "
                    f"queries={int(_clickhouse_number(row, 'run_count'))}; total={_clickhouse_number(row, 'total_duration_ms') / 1000:.3f}s; "
                    f"p95={_clickhouse_number(row, 'p95_duration_ms') / 1000:.3f}s; read_bytes={int(_clickhouse_number(row, 'read_bytes'))}; "
                    f"peak_memory={int(_clickhouse_number(row, 'peak_memory_usage'))}; errors={int(_clickhouse_number(row, 'error_count'))}."
                )
            section.append(f"- Recent/cumulative server errors (bounded): {len(error_rows)} row(s).")
            for row in error_rows[:10]:
                section.append(
                    f"  - code={row.get('error_code')}; name={_clickhouse_redacted_text(row.get('name'))}; "
                    f"value={int(_clickhouse_number(row, 'value'))}; remote={'yes' if _clickhouse_is_true(row.get('remote')) else 'no'}; "
                    f"last_time={row.get('last_error_time', 'unknown')}; "
                    f"last_error_text={_clickhouse_query_text(row.get('last_error_text')) or 'none'}."
                )
            section.append(f"- Quota usage (bounded): {len(quota_rows)} row(s).")
            for row in quota_rows[:10]:
                section.append(
                    f"  - quota={_clickhouse_redacted_text(row.get('quota_name'))}; "
                    f"user={_clickhouse_redacted_text(row.get('user_name')) or 'unknown'}; "
                    f"application={_clickhouse_redacted_text(row.get('application_name')) or 'unknown'}; "
                    f"queries/max={int(_clickhouse_number(row, 'num_queries'))}/{row.get('max_queries', 'unlimited')}; "
                    f"read_bytes/max={int(_clickhouse_number(row, 'read_bytes'))}/{row.get('max_read_bytes', 'unlimited')}; "
                    f"exceptions/max={int(_clickhouse_number(row, 'exceptions'))}/{row.get('max_exceptions', 'unlimited')}."
                )
            section.append("")
            if evidence_errors:
                section.extend(["Unreadable or malformed evidence:", *(f"- {error}" for error in evidence_errors), ""])
            if recommendations:
                section.extend(["Evidence-linked advisory recommendations:"])
                section.extend(recommendations)
            else:
                section.append("No workload recommendation is supported by this pod's evidence.")
            section.extend(
                [
                    "",
                    "Use these counters to discuss pool sizing, per-user concurrency, and quotas with service owners. "
                    "Do not kill queries or change quotas or settings from this report.",
                    "All recommendations are advisory. No query, configuration, connection, quota, or ClickHouse state was changed.",
                    "",
                ]
            )
            sections.append("\n".join(section))
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    if not sections and errors:
        return _clickhouse_tool_error(tool, "; ".join(errors), mode)
    report = [
        "ClickHouse workload, concurrency, and error analysis (advisory only):",
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
            "limit": limit,
            "pods_reported": len(sections),
            "pods_limit": _CLICKHOUSE_MAX_REPLICATION_PODS,
            "label_selector": label_selector,
        },
    )


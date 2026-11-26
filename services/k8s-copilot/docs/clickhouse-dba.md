# ClickHouse DBA foundation

ClickHouse support in the DBA agent is diagnostic-only. The foundation provides discovery, a safe direct-argv query path, a versioned SQL template registry, and slow/expensive query analysis from `system.query_log` and `system.processes`. Replication/topology and workload/concurrency reporting use the same safety model. The DBA agent does not kill queries, change settings, alter tables, restart pods, or execute remediation.

## Discovery

`find_database_pods` supports `clickhouse`, `clickhouse-keeper`, and `keeper`. Matching is text-based over the pod name, labels, container names, and container images, so it covers Altinity Operator pods, Bitnami pods, generic Helm charts, and single-node StatefulSets. `keeper` also recognizes ZooKeeper-compatible pods by their `zookeeper` name, label, container, or image.

When StatefulSet ownership and an ordinal suffix are present, the report includes those topology hints. Altinity labels such as `clickhouse.altinity.com/shard`, `clickhouse.altinity.com/replica`, `clickhouse.altinity.com/cluster`, and the equivalent `clickhouse-keeper.altinity.com/*` labels are preserved when present. `app.kubernetes.io/*` labels are also reported. These values are hints, not authoritative role probes.

Altinity `ClickHouseInstallation` lookup is optional. If the operator grants `get`/`list` on the namespaced `clickhouseinstallations.clickhouse.altinity.com` resource, the discovery output includes installation evidence. If access is denied or the CRD is absent, discovery still reports pod evidence and explicitly reports the installation-access limitation.

## Client and credentials

Queries run through the shared pod/exec helper with fixed argv, no shell, no stdin, no TTY, enabled stderr capture, and a bounded request timeout. The tool first checks the known `clickhouse-client` binary and then the `clickhouse client` subcommand. It does not probe renamed binaries.

The client is expected to use a read-only Kubernetes Secret-mounted client configuration at `/etc/clickhouse-client/copilot_dba.xml` by default. Set `DBA_CLICKHOUSE_CLIENT_CONFIG` only to an alternative validated absolute path for a different mounted read-only config. The configuration contains the dedicated `copilot_dba` identity; the agent never puts usernames, passwords, tokens, or connection strings on argv and does not read environment credentials.

The client connects to localhost inside the selected pod. Do not set `--secure`, `--tls*`, `--host`, or `--port` flags. ClickHouse server TLS policy remains controlled by the server and mounted client configuration; this diagnostic path does not negotiate a remote TLS connection.

The profile must be exactly `readonly = 1`. The tool runs `SELECT getSetting('readonly')` before every registry query and refuses to continue for `0`, `2`, or any other value. Do not grant write, DDL, maintenance, or access-management privileges. Grants needed by the supplied registry templates are:

```sql
GRANT SELECT ON system.query_log TO copilot_dba;
GRANT SELECT ON system.processes TO copilot_dba;
GRANT SELECT ON system.tables TO copilot_dba;
GRANT SELECT ON system.columns TO copilot_dba;
GRANT SELECT ON system.data_skipping_indices TO copilot_dba;
GRANT SELECT ON system.projections TO copilot_dba;
GRANT SELECT ON system.settings TO copilot_dba;
GRANT SELECT ON system.server_settings TO copilot_dba;
GRANT SELECT ON system.clusters TO copilot_dba;
GRANT SELECT ON system.replicas TO copilot_dba;
GRANT SELECT ON system.replication_queue TO copilot_dba;
GRANT SELECT ON system.distribution_queue TO copilot_dba;
GRANT SELECT ON system.zookeeper_connection TO copilot_dba;
GRANT SELECT ON system.events TO copilot_dba;
GRANT SELECT ON system.metrics TO copilot_dba;
GRANT SELECT ON system.errors TO copilot_dba;
GRANT SELECT ON system.quotas_usage TO copilot_dba;
GRANT SELECT ON system.parts TO copilot_dba;
GRANT SELECT ON system.parts_columns TO copilot_dba;
GRANT SELECT ON system.merge_tree_settings TO copilot_dba;
GRANT SELECT ON system.merges TO copilot_dba;
GRANT SELECT ON system.mutations TO copilot_dba;
GRANT SELECT ON system.detached_parts TO copilot_dba;
GRANT SELECT ON system.part_log TO copilot_dba;
GRANT SELECT ON system.errors TO copilot_dba;
GRANT SELECT ON system.metrics TO copilot_dba;
GRANT SELECT ON system.asynchronous_metrics TO copilot_dba;
GRANT SELECT ON system.disks TO copilot_dba;
GRANT SELECT ON system.storage_policies TO copilot_dba;
```

`system.projections` is optional on older ClickHouse versions. If it is unavailable, table-design analysis continues with the other catalog evidence and reports the limitation.

Operators may grant SELECT on application tables for other read-only analysis, but the foundation does not require it. Keep the ClickHouse user's `readonly` profile set to `1` and no `allow_ddl` bypass. The client config should look like:

```xml
<clickhouse>
  <user>copilot_dba</user>
  <!-- Put the actual secret in a Kubernetes Secret-rendered read-only mount. -->
  <password>REPLACE_WITH_MOUNTED_SECRET</password>
</clickhouse>
```

Keep this file in a read-only Secret volume and rotate it by replacing the mounted secret according to your platform's volume-update behavior.

## Query safety

Registry SQL is named, versioned, and parameterized. Identifiers and parameter names are validated, numeric windows and limits are clamped, and values are passed through ClickHouse typed query parameters (`--param_*`). The registry does not interpolate user input or operator input into SQL.

The ad-hoc SQL guard accepts only one `SELECT` or `WITH` statement, wraps it in `EXPLAIN`, and rejects semicolons, comments, `SETTINGS`, `FORMAT`, `INTO OUTFILE`, and write/DDL/maintenance statement forms. It blocks `clusterAllReplicas` unless the caller explicitly opts in. External or side-effecting table functions are denied by default; only local generation functions `numbers`, `zeros`, and `values` are allowed.

Because no ad-hoc SQL tool is registered in this foundation, the guard is enforced by the internal ClickHouse helpers and covered by tests. Do not expose raw user SQL through a future tool without reusing this guard.

## Slow and expensive query analysis

`get_clickhouse_slow_queries` reads only `system.query_log` and a bounded `system.processes` snapshot. It defaults to the last 60 minutes, at least 1,000 ms duration, 25 aggregates, `SELECT` queries, and total duration ranking. Time windows are partition-pruned by `event_date` and bounded by `event_time`. The default view is initial queries and excludes `user = 'copilot_dba'`; `ExceptionWhileProcessing` and `QueryFinish` are included. Distributed/sub-query rows are optional and off by default.

Aggregation is by `normalized_query_hash`. The report includes run count, duration percentiles, read rows/bytes, peak and aggregate memory, result rows, written rows, exception codes/messages, and relevant ProfileEvents when the server supplies them. It reports rows per read/selected mark when marks are available. Query text is produced by `normalizeQuery`, truncated, and write/DDL/maintenance/access-management text is redacted rather than echoed.

The tool also reports whether `log_queries` is enabled, the query-log flush interval when available, and query-log TTL/retention from the table engine definition. Stale logging or a short TTL means evidence may be incomplete; the report does not infer missing data.

`system.processes` evidence is read-only and bounded. The agent never issues `KILL`, `SYSTEM`, `OPTIMIZE`, or any other maintenance or mutation command. Recommendations are advisory and always name the triggering measurement. Cluster-level conclusions require examining every shard; one pod's query log is not a whole-cluster result.

## Replication and topology analysis

`get_clickhouse_replication_report(namespace=None, pod=None, cluster=None, output_mode="human")` is replica-local. It reads bounded snapshots from `system.clusters`, `system.replicas`, `system.replication_queue`, `system.distribution_queue`, and `system.zookeeper_connection` where available. Each source can fail or be absent without hiding the others; the report names the limitation.

The report includes configured shard/replica membership, the local replica identity, read-only replicas, expired Keeper sessions, absolute replication delay, queue size, inserts/merges in queue, replication-log gap, active replica count, lost parts, errors, and recovery time where the server supplies them. It also lists the oldest replication-queue entries with retries, last exceptions, postponement reasons, and pending `Distributed` files and bytes.

Altinity labels and generic Kubernetes hints are displayed but never trusted as topology truth. When a label and `system.clusters` disagree, the report flags the mismatch and says that runtime evidence is authoritative. A single pod's report cannot prove cluster-wide health; repeat the check on every shard or replica, or use the existing opt-in `clusterAllReplicas` mechanism only when an operator has explicitly accepted its broader cost and access implications.

Recommendations are evidence-linked and advisory. The report does not request Keeper changes, permission changes, table mutations, restarts, or data repair.

## Workload, concurrency, and error analysis

`get_clickhouse_workload_report(namespace=None, pod=None, minutes=60, output_mode="human")` reads bounded evidence from `system.metrics`, `system.processes`, `system.errors`, `system.query_log`, and `system.quotas_usage` where readable. It also reads selected concurrency-related rows from `system.settings`. The window is clamped to 1-10,080 minutes and every source list is bounded.

The report shows query and merge counters; HTTP, TCP, interserver, MySQL, and PostgreSQL connection counters; pool/task counts; running queries; long-running or memory-heavy queries; recent and cumulative top errors; and quota usage against its limits. Recent load is grouped by user, client name, HTTP user agent, and initial address where those columns exist. Query text is produced by `normalizeQuery`, truncated, and write/DDL/maintenance/access-management text is redacted rather than echoed. Secret-shaped client attributes are redacted as well.

Use the report to discuss client pool sizing, per-user concurrency, quotas, and misbehaving consumers with service owners. Every recommendation names its triggering evidence. The tool never kills a query, changes a setting or quota, drops a connection, or takes any database or Kubernetes action.

## Parts, merges, and mutations

`get_clickhouse_parts_report` uses the same readonly profile check and bounded registry queries. It reads active-part aggregates from `system.parts`, MergeTree defaults from `system.merge_tree_settings`, table engine metadata from `system.tables`, running merges from `system.merges`, mutations from `system.mutations`, detached parts from `system.detached_parts`, recent activity from `system.part_log` when enabled, and recent failures from `system.errors`. Each source is treated independently; a missing or legacy table becomes an explicit evidence gap rather than an invented result.

The report compares per-partition active part counts with `parts_to_delay_throw` and `parts_to_throw_insert`, using a table override from `engine_full` when present and the server default otherwise. It reports average part size and partitions with an average active part of at most 8 MiB as small-part evidence. It also reports merge progress, elapsed time, source/output size, and background-pool task counts. Mutations are flagged as failed, old after six hours, or stuck after 24 hours, together with age, `parts_to_do`, and `latest_fail_reason`. Detached parts are grouped by reason. Part-log rows are bounded to the last 24 hours and cannot be used to invent a longer trend.

The tool may recommend insert batching, partition/granularity review, merge-pool review, investigation of stuck/failed mutations, or investigation of detached parts. It never runs or suggests issuing a mutation, `OPTIMIZE`, `DETACH`, `ATTACH`, `DROP`, `ALTER`, `SYSTEM`, or `KILL` command. Every recommendation is advisory and evidence-linked.

## Resource, disk, and storage forensics

`get_clickhouse_resource_report` reads selected values from `system.asynchronous_metrics`, `system.metrics`, `system.server_settings`, `system.disks`, and `system.storage_policies`. It reports tracking/resident memory when exposed, the lower of the ClickHouse server memory limit and the selected Kubernetes container memory limit, cache sizes where exposed, and query-cache hit rate only when hit and miss counters are available.

Storage reporting uses disk free/total space, `keep_free_space`, storage-policy volume and tiering metadata, and the PVC request matched by disk path to a pod volume mount. The disk and PVC values are different evidence layers: ClickHouse sees filesystem use while Kubernetes reports the configured PVC capacity. Missing mounts, ambiguous paths, or absent claims are reported as evidence gaps.

For size leaders, the report uses only metadata from `system.parts` and `system.parts_columns`. It ranks active tables and partitions by bytes, rows, and part count, and ranks columns by compressed bytes with a compression ratio from compressed versus uncompressed metadata when available. TTL presence comes from `system.tables.engine_full`; the tool does not scan application data.

The tool may recommend memory headroom investigation, external aggregation or sorting, column codecs, `LowCardinality`, retention/TTL review, storage tiering, or disk-pressure investigation. It never deletes, moves, detaches, or mutates data and never changes ClickHouse or Kubernetes configuration. Missing asynchronous metrics or older table layouts are reported as unavailable evidence rather than inferred.

## Metrics trends

`get_clickhouse_metrics_trends(namespace=None, pod=None, hours=24, project_id=None, output_mode="human")` is metrics-only for time-window questions. It first uses read-only pod metadata to recognize an existing ClickHouse native Prometheus endpoint (`<prometheus>` plus `prometheus.io/*` annotations, usually port 9363), an Altinity operator metrics endpoint, or an existing Google Managed Prometheus/GKE pipeline. It then reads matching Cloud Monitoring series.

The reader resolves native `ClickHouseProfileEvents_*`, `ClickHouseMetrics_*`, and `ClickHouseAsyncMetrics_*` families, plus Altinity `chi_clickhouse_event_*` and `chi_clickhouse_metric_*` aliases. It reports trends for query, SELECT, and INSERT rates, inserted rows, failed queries, query and connection concurrency, tracked memory, parts, running merges, replication delay/queue, read-only replicas, and disk free. Counter decreases are labeled restart/reset evidence and never become negative rates.

Set `DBA_CLICKHOUSE_METRICS_PROJECT_ID` when the existing project fallbacks do not identify the monitoring project. If no endpoint evidence and no matching series are discoverable, the tool returns a prerequisite rather than falling back to pod/exec or installing anything.

## Snapshot diffs

`get_clickhouse_snapshot_diff(namespace, pod, interval_minutes=5, output_mode="human")` reads selected numeric values from `system.events`, `system.metrics`, and `system.asynchronous_metrics`, waits 1-60 minutes, and reads the same bounded selection again. It reports deltas for queries, SELECT/INSERT queries, inserted and merged rows, failed queries, memory, parts, merges, replication delay/queue, read-only replicas, and disk free where those selected values are readable.

Each source is independent and failures remain evidence gaps. The snapshot contains only selected names and numeric values; it does not persist credentials, secrets, raw application data, or query text. The mandatory readonly profile check and direct-argv registry path are unchanged.

## Query plans and table design

`get_clickhouse_query_plan` accepts one validated `SELECT` or `WITH` statement and one of these modes: `indexes`, `actions`, `pipeline`, or `estimate`. It emits `EXPLAIN indexes = 1`, `EXPLAIN actions = 1`, `EXPLAIN PIPELINE`, or `EXPLAIN ESTIMATE` respectively. None of these modes executes the user query. The mandatory `readonly=1` check, direct argv, fixed timeouts, and audit logging remain unchanged.

`EXPLAIN indexes = 1` may require `SELECT` privilege on referenced application tables. If it fails with a privilege error, the tool reports the missing grant rather than inventing a plan. `EXPLAIN ESTIMATE` provides read estimates from table metadata and is still not an execution.

`recommend_clickhouse_table_design` combines `system.tables`, `system.columns`, `system.data_skipping_indices`, available `system.projections`, `EXPLAIN indexes = 1`, and an existing `system.query_log` aggregate when `normalized_query_hash` is supplied. It can use an explicit `database.table`, simple table references from the guarded query, or the table references in the normalized query associated with a hash.

The recommendation is advisory only. It can discuss sorting keys, partition pruning, data-skipping indexes, projections, `FINAL`, join order and memory, large `IN` subqueries, `LowCardinality` candidates, and codec/compression choices. Every suggestion names its evidence and cost/risk: table redesigns may require rebuild or backfill, indexes and projections add storage and merge work, codec choices can trade CPU for storage, and planner behavior must be verified. It does not read application data, generate `ALTER TABLE`, suggest a direct `CREATE INDEX`, run `OPTIMIZE`, or change any database state.

## Remediation recommendation correlation

`get_clickhouse_remediation_recommendations(namespace, pod, include_query_analysis=True, output_mode="human")` correlates the existing ClickHouse evidence for one server pod. It reuses the bounded slow-query, process, table-design/catalog, safe `EXPLAIN`, parts/merges/mutations, resource/storage, replication/topology, and workload collectors. It adds no SQL template and no new query form.

The tool emits advisory cards with a stable id, one of the `query`, `table_design`, `parts_merges`, `mutations`, `replication`, `memory`, `storage`, or `workload` categories, the triggering measurement, a suggested change, a risk/trade-off, a required approver, and `No change was applied.` It may collect `EXPLAIN indexes = 1` only for a complete, non-secret normalized `SELECT` or `WITH` statement found in existing query-log evidence; it does not execute that statement. When a normalized query is truncated, secret-shaped, not a guarded read-only form, or lacks a simple table target, the tool reports that as an evidence gap instead of running a plan.

Human output labels any operator-runnable command text as operator review/execution only; the agent does not execute commands. Mutation cards always identify mutation review as an operator-owned action. JSON mode uses the existing ClickHouse advisory envelope and includes structured recommendation cards plus evidence errors. Every recommendation requires the named owner approval, and one pod's evidence remains pod-local rather than a cluster-wide conclusion.

## Structured output

All ClickHouse tools accept `output_mode='human'` (the default, preserving existing Markdown) or `output_mode='json'`. JSON uses `schema_version: cloud-copilot.dba.clickhouse.v1`, `generated_at`, `tool`, `status`, `data.report`, and bounded metadata. Failures use `status: error` and an `errors` array. The envelope excludes credentials, secrets, command arguments, and raw application data; pass JSON verbatim to dashboards or automation.

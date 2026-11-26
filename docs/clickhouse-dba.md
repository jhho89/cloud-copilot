# ClickHouse DBA foundation

ClickHouse support in the DBA agent is diagnostic-only. The first PR provides discovery, a safe direct-argv query path, a versioned SQL template registry, and slow/expensive query analysis from `system.query_log` and `system.processes`. It does not kill queries, change settings, alter tables, or execute remediation.

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
GRANT SELECT ON system.settings TO copilot_dba;
GRANT SELECT ON system.server_settings TO copilot_dba;
```

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

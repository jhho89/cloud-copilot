# DBA agent read-only access

The DBA agent is diagnostic-only. It does not execute application SQL, create or alter database objects, mutate pods, or restart workloads. For MySQL/MariaDB, `EXPLAIN` is the only query execution supported and destructive statements are rejected before execution. For Redis/Valkey, only the explicit read-only diagnostic commands below are allowed. For ClickHouse, the agent executes only versioned registry SQL, checks `getSetting('readonly') = 1`, and treats all recommendations as advisory.

## Kubernetes RBAC

Bind the following role to the service account that runs the Kubernetes Copilot. Scope it to the database namespace rather than the cluster. For a multi-namespace deployment, create one role and binding per database namespace.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: dba-copilot-readonly
  namespace: database
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["pods/exec"]
    verbs: ["create", "get"]
  # Optional: discovery of Altinity ClickHouseInstallations. Omit this rule
  # when the agent should rely on pod labels alone; it reports denial instead
  # of failing the pod discovery result.
  - apiGroups: ["clickhouse.altinity.com"]
    resources: ["clickhouseinstallations"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: dba-copilot-readonly
  namespace: database
subjects:
  - kind: ServiceAccount
    name: cloud-copilot
    namespace: copilot
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: dba-copilot-readonly
```

Do not grant `secrets`, `deployments`, `statefulsets`, pod deletion, pod patching, or database write access. Keep cluster-wide pod discovery disabled unless there is a documented need for it. The optional `clickhouseinstallations` rule is read-only and namespaced; do not grant create, patch, update, delete, or cluster scope.

Where your Kubernetes distribution enforces `resourceNames` for `pods/exec`, list the database pod names in the `pods/exec` rule and omit `list`/`watch` if discovery is handled externally. Verify the restriction in your environment because enforcement of resource names for connect subresources varies.

## Streamed log mode

Streamed mode expects the database's slow log to appear in the selected container's `stdout` or `stderr`. If `slow_query_log=FILE`, configure the database image or a sidecar/collector to stream that file; table output is not supported. The default mode calls the Kubernetes pod-log API with a bounded time window and line tail. It does not use pod/exec.

## File-log prerequisites

File mode is disabled unless an operator configures an exact path:

```bash
# One path
DBA_SLOW_LOG_FILE_PATH=/var/log/mysql/slow.log

# Multiple selectable paths (semicolon-separated)
DBA_ALLOWED_SLOW_LOG_PATHS=/var/log/mysql/slow.log;/var/log/mariadb/slow.log
```

Path rules:

- Paths are absolute POSIX paths. Empty components, `.`, `..`, backslashes, NUL, trailing separators, and control characters are rejected.
- The agent does not expand shell expressions. The configured value is the literal pod path.
- File mode runs only `/usr/bin/tail --lines=<bounded count> -- <path>`. It does not run a shell or execute SQL.
- The database container must have `/usr/bin/tail` and the service-account identity must be allowed to exec into the selected pod and container.
- The path must be readable by the container user and should point to a regular file on a stable volume. If log rotation replaces the file by rename, configure rotation to recreate and reopen the log safely.
- The operator is responsible for ensuring the allowlisted path and its parents are not attacker-writable or replaced by symlinks. The remote exec does not perform a TOCTOU-free symlink check.
- Prefer a sidecar or collector streaming the file when it is available; use file mode only when pod logs cannot expose the data.

The pod/exec audit record contains the event time, namespace, pod, operation, fixed binary, and, for file mode, the configured path; it includes the container only when one is selected. It does not record SQL, query output, credentials, or command strings.

## Replica selection

Pass `namespace` and `pod_name` for a targeted investigation. When discovery is needed, use `namespace` plus a narrow `label_selector`; avoid cluster-wide scans in production.

For a replicated deployment, analyze the primary for write-path queries and replicas separately for read-path queries. Do not assume every replica has the same workload or slow-log contents. For file mode, selecting one pod at a time also limits exec fan-out; repeat the analysis on additional replicas only when their traffic pattern is relevant.

## Redis/Valkey slow-log mode

`find_database_pods` recognizes Redis and Valkey by pod name, label text, container name, or image. When a pod has a StatefulSet owner reference and an ordinal suffix such as `cache-2`, the tool reports the StatefulSet and ordinal as topology hints. Explicit role labels (`app.kubernetes.io/component`, `redis-role`, `valkey-role`, or provider-specific role labels) are reported when they name `master`, `primary`, `replica`, `slave`, or `sentinel`. These are hints, not authoritative topology probes.

The Redis path runs `valkey-cli --json` first and falls back to `redis-cli --json`. It does not try alternate or renamed binaries, does not probe credentials, and does not attempt to discover renamed commands. If neither known CLI binary exists in the selected pod/container, the tool fails clearly.

Commands are validated against a strict allowlist before execution:

- `SLOWLOG GET <count>`
- `SLOWLOG LEN`
- `CONFIG GET slowlog-max-len`

The blocked-command layer explicitly rejects `KEYS`, `DEBUG`, `CONFIG SET`, `LATENCY RESET`, `SLOWLOG RESET`, `CLIENT KILL`, `MONITOR`, `SAVE`, `FLUSHDB`, `FLUSHALL`, and destructive or maintenance commands. The CLI runs as direct argv over pod/exec; there is no shell for the Redis path.

Password authentication must already be present in the selected container. Prefer `REDISCLI_AUTH` or `VALKEYCLI_AUTH` populated from a Kubernetes Secret, or mount a read-only CLI configuration if ACL or transport settings require one. The tool does not accept passwords, usernames, or authentication tokens as arguments and does not add credentials to argv. Pod environment values are inherited by `valkey-cli`/`redis-cli`; the tool itself does not read them.

Redis/Valkey analysis performs four bounded pod/exec calls per selected pod: CLI detection with `--version`, `SLOWLOG GET`, `SLOWLOG LEN`, and `CONFIG GET slowlog-max-len`. Output is parsed as CLI JSON or RESP arrays, filtered by duration, sorted by total duration, and grouped by command family. Each entry is labeled with its pod and known role/topology. If `SLOWLOG LEN` equals `slowlog-max-len`, the report states that the slow log is saturated and older entries may have been evicted. All findings are advisory only; the tool changes no Redis or Valkey state.

## Redis/Valkey memory, big-key, and latency mode

Memory reports run the same known CLI with these allowlisted commands:

- `INFO memory`
- `INFO stats`
- `INFO persistence`
- `INFO replication`
- `MEMORY STATS`

Latency and client reports additionally allow `LATENCY HISTORY` for `command`, `expire-cycle`, `eviction-cycle`, `eviction-del`, `fork`, `aof-fsync`, and `rdb-save`, plus `LATENCY DOCTOR`, `CONFIG GET io-threads`, `CLIENT LIST`, `CLIENT ID`, `INFO clients`, `CONFIG GET client-output-buffer-limit`, `CLUSTER INFO`, `CLUSTER MYID`, `CLUSTER SHARDS`, `CLUSTER NODES`, `SENTINEL MASTERS`, `SENTINEL REPLICAS <name>`, and `SENTINEL SENTINELS <name>`. Latency reports correlate those events with slow-log, memory, command-rate, and persistence evidence. `io-threads` is context, not proof of causation.

Big-key analysis requires one namespace and pod. It invokes `valkey-cli --bigkeys` or `--memkeys` (preferring Valkey), falling back to `redis-cli`; the CLI uses `SCAN` internally. The agent does not issue `KEYS`, a manual `SCAN`, `DEBUG`, or any mutation command. The exec timeout is bounded and the CLI is throttled with `-i 0.01` by default. Because the analyzer iterates the keyspace, use it on a selected node and avoid simultaneous scans of every replica.

The tools re-read `INFO replication` at execution time. Reports label runtime role explicitly (including `unknown` when unavailable) and preserve namespace/pod attribution. Recommendations remain advisory; no memory setting, eviction policy, persistence setting, or key is changed.

## Redis/Valkey client and topology mode

`get_redis_client_report` uses the client and topology commands above. It reports `maxclients` headroom, blocked clients, and subscription counts from `INFO clients`; groups offending connections by `CLIENT SETNAME` (or address for unnamed clients); flags large `qbuf`, `omem`, long-lived/idle connections, and clients stuck in `MONITOR`; and explains likely application-side causes such as slow consumers, leaked pools, pub/sub backpressure, or accidental MONITOR use.

Topology is always observed at report execution time. `INFO replication` supplies runtime role, replica offsets, backlog size/use/headroom, and `master_replid`; `CLUSTER INFO`, `CLUSTER MYID`, and `CLUSTER SHARDS` identify cluster state, shard slots, and the current pod's node role. Sentinel pods are discovered from Redis/Valkey-related image, container name, pod name, or label text containing `sentinel`; `SENTINEL MASTERS`, `SENTINEL REPLICAS`, and `SENTINEL SENTINELS` provide each Sentinel's view of the current master, quorum, replica state, and peer Sentinels.

For cluster mode, client evidence is per node. A single shard's report does not describe the whole dataset or connection population; every shard master must be examined when a cluster-wide view is required, which increases pod/exec fan-out with shard count. Kubernetes labels remain hints, not authoritative role evidence. Reports explicitly mark missing, denied, or malformed topology data and remain advisory-only.

The shared pod/exec wrapper uses fixed argv, no stdin, no TTY, enabled stderr, a bounded request timeout, and the same audit event as MySQL file mode. The audit record does not include Redis commands, output, or credentials.

## Redis/Valkey metrics and snapshot mode

`get_redis_metrics_trends` uses metrics rather than pod/exec for time-window questions about hit rate, memory, evictions, connected clients, and ops/sec. It can use a `redis_exporter`/`valkey-exporter` sidecar identified by container/image text and `prometheus.io/scrape`, `prometheus.io/port`, and `prometheus.io/path` annotations, or an existing Google Managed Prometheus/GKE pipeline readable through the service's existing Cloud Monitoring integration. It does not create a new metrics integration or fall back to pod/exec. If no sidecar and no matching Redis series are discoverable, it reports the prerequisite clearly. Set `DBA_REDIS_METRICS_PROJECT_ID` when the default workload credentials do not identify the monitoring project.

`get_redis_snapshot_diff` makes two bounded, read-only INFO evidence collections on one selected pod, separated by 1-60 minutes. It reports selected deltas such as memory, fragmentation drift, eviction rate, hit/miss delta, instantaneous ops change, and connected-client change. It retains only selected numeric fields and runtime role evidence; it does not persist credentials, secrets, Redis commands, or raw output. Because the tool waits between snapshots, avoid long intervals and simultaneous fan-out without explicit agreement.

No Redis/Valkey metrics or snapshot tool changes configuration, resources, or data.

## Redis/Valkey structured output

Redis/Valkey tools accept `output_mode='human'` (the default, preserving existing Markdown) or `output_mode='json'`. JSON uses `schema_version: cloud-copilot.dba.redis.v1`, `tool`, `status`, `generated_at`, a Markdown `data.report`, and bounded metadata. Failures use `status: error` and an `errors` array. The envelope excludes credentials, command arguments, and raw CLI output. Pass JSON verbatim to dashboards or automation.

For common discovery labels, exporter prerequisites, TLS limitations, CLI availability, and Valkey release differences, see `services/k8s-copilot/docs/redis-valkey-dba.md`.

## Redis/Valkey remediation recommendations

`get_redis_remediation_recommendations` reuses the existing diagnostics and produces node-level, evidence-linked advisory cards for command patterns, memory, persistence, clients, and topology. It does not introduce a new Redis command. It reads the fixed slowlog, INFO/MEMORY, latency, client/topology, and `CONFIG GET` forms already described above. When `include_bigkeys` is enabled (the default), it invokes only the existing SCAN-based `--bigkeys` CLI mode for one selected pod with a bounded interval and timeout; it never issues `KEYS`, a manual `SCAN`, `DEBUG`, or a mutation command.

`get_clickhouse_remediation_recommendations` reuses the existing ClickHouse registry queries, catalog/table-design collectors, and safe `EXPLAIN` path. It introduces no query form and requires no additional RBAC. Recommendations are advisory cards only; operator command text, when present, is for operator execution and the agent never runs it.

## ClickHouse metrics and snapshot mode

`get_clickhouse_metrics_trends` reads existing Cloud Monitoring/Managed Prometheus series only. It recognizes a native ClickHouse Prometheus endpoint with `prometheus.io/*` annotations or port 9363, Altinity operator metrics, or an existing GMP/GKE pipeline carrying the `ClickHouseProfileEvents_*`, `ClickHouseMetrics_*`, `ClickHouseAsyncMetrics_*`, or `chi_clickhouse_*` families. It never falls back to pod/exec for trends and never installs an exporter. If no source is discoverable, it reports the prerequisite. Set `DBA_CLICKHOUSE_METRICS_PROJECT_ID` when the default workload credentials do not identify the monitoring project.

`get_clickhouse_snapshot_diff` makes two bounded reads of selected `system.events`, `system.metrics`, and `system.asynchronous_metrics` values 1-60 minutes apart through the existing versioned registry and readonly profile check. It retains only selected numeric fields and never persists credentials, secrets, raw application data, or query text.

No ClickHouse metrics or snapshot tool changes configuration, resources, or data.

## ClickHouse structured output

ClickHouse tools accept `output_mode='human'` (the default, preserving existing Markdown) or `output_mode='json'`. JSON uses `schema_version: cloud-copilot.dba.clickhouse.v1`, `generated_at`, `tool`, `status`, `data.report`, and bounded metadata. Failures use `status: error` and an `errors` array. The envelope excludes credentials, secrets, and raw application data. Pass JSON verbatim to dashboards or automation.

Each recommendation states the measurement that triggered it, a suggested change, and the risk or trade-off, and explicitly says that no change was applied. Evidence gaps are labeled rather than filled with assumptions. Cluster evidence is shard-local, so one pod's report does not describe a multi-shard cluster; Kubernetes role labels remain hints. The tool may also read Sentinel views for topology context when Sentinel pods are selected or discovered.

The report compares `INFO memory maxmemory` with the selected database container's Kubernetes memory limit when that limit is present, but it never changes a resource limit or Redis setting. Recommendations can include `SCAN` iteration, bounded collection reads, `UNLINK`, client pipelining, TTL review, eviction-policy trade-offs, defragmentation suitability, persistence/fork mitigation, replica-based backups, client ownership/backpressure, and cluster-health stabilization. All are advisory; the tool executes no remediation.

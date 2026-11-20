# DBA agent read-only access

The DBA agent is diagnostic-only. It does not execute application SQL, create or alter database objects, mutate pods, or restart workloads. For MySQL/MariaDB, `EXPLAIN` is the only query execution supported and destructive statements are rejected before execution. For Redis/Valkey, only the explicit read-only diagnostic commands below are allowed.

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

Do not grant `secrets`, `deployments`, `statefulsets`, pod deletion, pod patching, or database write access. Keep cluster-wide pod discovery disabled unless there is a documented need for it.

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

Latency reports additionally allow `LATENCY HISTORY` for `command`, `expire-cycle`, `eviction-cycle`, `eviction-del`, `fork`, `aof-fsync`, and `rdb-save`, plus `LATENCY DOCTOR` and `CONFIG GET io-threads`. They correlate those events with slow-log, memory, command-rate, and persistence evidence. `io-threads` is context, not proof of causation.

Big-key analysis requires one namespace and pod. It invokes `valkey-cli --bigkeys` or `--memkeys` (preferring Valkey), falling back to `redis-cli`; the CLI uses `SCAN` internally. The agent does not issue `KEYS`, a manual `SCAN`, `DEBUG`, or any mutation command. The exec timeout is bounded and the CLI is throttled with `-i 0.01` by default. Because the analyzer iterates the keyspace, use it on a selected node and avoid simultaneous scans of every replica.

The tools re-read `INFO replication` at execution time. Reports label runtime role explicitly (including `unknown` when unavailable) and preserve namespace/pod attribution. Recommendations remain advisory; no memory setting, eviction policy, persistence setting, or key is changed.

The shared pod/exec wrapper uses fixed argv, no stdin, no TTY, enabled stderr, a bounded request timeout, and the same audit event as MySQL file mode. The audit record does not include Redis commands, output, or credentials.

# DBA agent read-only access

The DBA agent is diagnostic-only. It does not execute application SQL, create or alter database objects, mutate pods, or restart workloads. `EXPLAIN` is the only query execution supported, and destructive statements are rejected before execution.

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

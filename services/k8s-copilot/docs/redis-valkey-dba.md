# Redis/Valkey DBA diagnostics

The DBA agent treats Redis and Valkey as read-only targets. It discovers pods, reads metrics for trends, and runs bounded point-in-time diagnostics. It never mutates Redis/Valkey state, Kubernetes resources, or client connections.

## Discovery

`find_database_pods` matches pod name, label text, container name, or image text containing `redis` or `valkey`. The following labels are common operator patterns:

| Operator or chart | Examples |
| --- | --- |
| Kubernetes recommendation | `app.kubernetes.io/name=redis` or `app.kubernetes.io/name=valkey`; `app.kubernetes.io/component=master|primary|replica|sentinel` |
| Redis and Valkey charts | `redis-role`, `valkey-role`, `redis.kubernetes.io/role`, or `valkey.kubernetes.io/role` |
| Custom deployments | Any operator-selected selector accepted by `find_database_pods(..., label_selector=...)` |

Role labels and StatefulSet ordinals are hints only. Runtime role is re-read from `INFO replication` for reports that exec into the selected pod. In cluster mode, one node's evidence is shard-local; every shard primary needs its own report.

For production, pass a `namespace` and `pod`, or a narrow label selector. Cluster-wide discovery is useful for triage but should not become routine in a large cluster.

## Authentication and transport

Credentials must already exist in the selected container. Prefer `REDISCLI_AUTH` or `VALKEYCLI_AUTH` sourced from a Kubernetes Secret. A mounted CLI configuration can be used when an operator's image supports it. The agent does not read the Secret, inspect pod environment values, accept a password argument, or add credentials to `argv`.

TLS is not negotiated by the DBA agent. The fixed `redis-cli`/`valkey-cli` invocation does not add `--tls`, certificate, or identity flags. For a TLS-only server, expose a local non-TLS proxy/sidecar for diagnostics, or extend the environment deliberately with an operator-approved client configuration. Do not place private keys or tokens in pod annotations.

## CLI behavior

The tools run `valkey-cli --json` first and then `redis-cli --json`. They do not probe alternate or renamed binaries and do not discover renamed commands. If neither known CLI exists, the report says so. Every command is validated against a fixed read-only allowlist before direct argv pod/exec; there is no remote shell for Redis/Valkey.

Commands include `SLOWLOG GET/LEN`, selected `INFO` sections, `MEMORY STATS`, selected `LATENCY` views, `CLIENT LIST/ID`, selected `CONFIG GET` values, selected `CLUSTER` views, and selected `SENTINEL` views. Destructive or maintenance forms are blocked. The audit record contains namespace, pod, operation, binary, and selected container; it does not contain Redis commands, responses, credentials, or SQL.

## Valkey differences

Valkey reports the same INFO categories used here, but field names, latency event classes, and defaults can vary by release. The client prefers `valkey-cli` and still treats Redis-compatible responses as evidence. Avoid assuming a feature or command is identical across Redis and Valkey versions; a report says `unavailable` when a field or command is absent rather than inferring it from the server's image name.

## Metrics trends

For questions about hit rate, memory, evictions, connected clients, and ops/sec over time, use `get_redis_metrics_trends`. Metrics are preferred so trend windows do not amplify load or rely on the instantaneous INFO counters alone.

Discovery accepts either of these sources:

1. A `redis_exporter`/`valkey-exporter` sidecar, identified by container name/image text plus scrape annotations:
   - `prometheus.io/scrape: "true"`
   - `prometheus.io/port: "9121"`
   - `prometheus.io/path: "/metrics"` (or the configured path)
2. An existing Google Managed Prometheus/GKE pipeline already exporting Redis metrics and readable through the project's existing Cloud Monitoring credentials.

If neither source is discoverable, or no matching series exists, the tool returns a clear prerequisite error. It does not fall back to pod/exec for trends. Set `DBA_REDIS_METRICS_PROJECT_ID` when workload credentials do not infer the monitoring project; pass `project_id` for a one-off selection. The service already installs `google-cloud-monitoring`; no new dependency is introduced.

The tool recognizes common `redis_exporter` names such as `redis_memory_used_bytes`, `redis_memory_max_bytes`, `redis_mem_fragmentation_ratio`, `redis_evicted_keys_total`, `redis_connected_clients`, `redis_commands_processed_total`, `redis_instantaneous_ops_per_sec`, `redis_keyspace_hits_total`, and `redis_keyspace_misses_total`. It normalizes labels such as `namespace` and `pod` and reports gaps explicitly. Counter decreases are treated as restart/reset evidence rather than negative rates.

## Snapshot diff

`get_redis_snapshot_diff` performs two point-in-time INFO evidence collections on one pod, separated by `interval_minutes` (1-60). The call waits for that interval, so coordinate long comparisons with the user and avoid fanning out across replicas.

It reports memory change, fragmentation drift, evicted-key delta and rate, hit/miss deltas and hit-ratio change, instantaneous ops/sec change, and connected-client/maxclients change. A process restart can reset cumulative counters; such gaps are reported as unavailable rather than as negative rates. The report retains only selected numeric INFO fields and runtime role evidence. It does not persist credentials, secrets, raw CLI output, or unrelated INFO fields.

## Structured output

Redis/Valkey diagnostic tools accept `output_mode='human'` (default) or `output_mode='json'`. Human output remains the existing Markdown report. JSON is a stable envelope:

```json
{
  "schema_version": "cloud-copilot.dba.redis.v1",
  "generated_at": "2026-01-01T00:00:00+00:00",
  "tool": "get_redis_memory_report",
  "status": "ok",
  "data": {
    "format": "markdown",
    "report": "Redis/Valkey memory analysis..."
  },
  "metadata": {}
}
```

Errors use `status: "error"` and an `errors` array instead of mixing failure text into a successful envelope. Metadata contains bounded attribution such as namespace, pod, window, and evidence counts. It does not contain credentials, command arguments, or raw CLI responses. Keep structured JSON verbatim for dashboards; use the human report only when a person is the intended consumer.

All reports are advisory. No change is applied.

## Idle cost assessment

For one-to-31-day idle-cost questions, use `collect_database_idle_assessment` instead of the point-in-time diagnostics. It reads Cloud Monitoring/Managed Prometheus series from an existing `redis_exporter` or `valkey-exporter`, aggregates counters and gauges hourly, and adds read-only Kubernetes ownership, PVC, backup, replication, node, and resource-envelope context. It does not use pod/exec for trends or install an exporter.

Connections alone never establish idleness. The assessment combines work-rate and CPU evidence with the configured cost-review eligibility marker and blocks replicas, primaries with backups, production workloads, unknown backups, and workloads without owner/environment markers. See [idle-database-cost.md](idle-database-cost.md) for IAM, threshold variables, and the stable JSON envelope.

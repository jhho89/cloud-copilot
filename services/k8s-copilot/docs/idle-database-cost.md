# Idle database cost engineering

`collect_database_idle_assessment` is a metrics-only cost assessment for MySQL, MariaDB, Redis, Valkey, and ClickHouse. It returns `schema_version=cloud-copilot.dba.idle-cost.v1`, preserves attribution for every matching workload in `data.workloads`, and is advisory. It does not use pod/exec as a metrics fallback, install an exporter, read Secrets, accept credentials, or mutate Kubernetes or database state.

## Prerequisites and IAM

The function reads an existing Google Managed Prometheus exporter through Cloud Monitoring. Operators must already run and scrape `mysqld_exporter`, `redis_exporter`, `valkey-exporter`, native ClickHouse Prometheus metrics, or Altinity operator metrics; the service never installs one. Sidecars, database-native metric ports, and `prometheus.io/*` annotations are discovery evidence only. The profile-specific recommendation is shown when no enabled exporter or matching series is found.

The caller needs read access to the configured Cloud Monitoring project, typically `roles/monitoring.viewer`, plus Kubernetes read access for pods, namespaces, PVCs, CronJobs, and nodes. Configure `DBA_DATABASE_METRICS_PROJECT_ID`; the service falls back to `DBA_CLICKHOUSE_METRICS_PROJECT_ID`, `DBA_REDIS_METRICS_PROJECT_ID`, `GOOGLE_CLOUD_PROJECT`, and `GCP_PROJECT_ID`, then inferred Application Default Credentials. Never place database credentials in annotations or function arguments.

## Profiles and evidence

A declarative profile controls aliases, counter/gauge kinds, required logical metrics, exporter identities, role labels, backup names, and the operator recommendation. Exporter-version differences are resolved by aliases before pod matching. The reader queries one Cloud Monitoring metric type at a time for all workload targets, then matches series labels to pods; it does not fan out per pod.

Counters and gauges are aggregated hourly. A counter decrease is treated as unavailable work-rate evidence and a restart, not as a negative rate. Missing series and data gaps remain explicit. Kubernetes container evidence comes from the named GKE types for CPU and memory utilization and network sent/received bytes.

## Classification

Thresholds are configurable and conservative:

| Variable | Default |
| --- | --- |
| `DBA_IDLE_ASSESSMENT_MIN_DAYS` | `7` |
| `DBA_IDLE_WORK_P95_OPS_PER_SEC` | `0.01` |
| `DBA_IDLE_CPU_P95_FRACTION` | `0.05` |
| `DBA_IDLE_MIN_ZERO_WORK_RATIO` | `0.90` |
| `DBA_IDLE_ACTIVE_THREADS_MEAN` | `1.05` (MySQL `Threads_running` is at least 1 while the exporter queries) |
| `DBA_IDLE_EXPORTER_BASELINE_OPS_PER_SEC` | `0` (subtracted from the work rate) |
| `DBA_IDLE_MIN_DATA_COVERAGE` | `0.8` (hours with data / expected hours) |
| `DBA_GKE_DISTRIBUTION` | unset (`Autopilot` or `Standard`; copied to `metadata.kubernetes`) |
| `DBA_GKE_CLUSTER_NAME`, `DBA_GKE_CLUSTER_LOCATION` | unset (pin billing rows to one cluster) |
| `DBA_COST_REVIEW_ELIGIBILITY_ANNOTATION` | `cloud-copilot.ai/cost-review` |

The exporter's own scrapes (`SHOW GLOBAL STATUS`, `INFO`, `PING`) count as database work, so on real clusters the default `0.01` ops/s threshold may never be met. Measure your exporter's idle rate and set `DBA_IDLE_EXPORTER_BASELINE_OPS_PER_SEC`; the default `0` is the safe direction (nothing is marked idle by accident).

Candidacy also needs at least `DBA_IDLE_MIN_DATA_COVERAGE` of the window to have samples, container CPU evidence, an explicit `primary` role label (an unlabeled pod might serve failover), no counter restarts, and no running backup job. Backup CronJobs must be named for, or labeled with, this database; suspended CronJobs do not count as a schedule. Savings are modeled only for `candidate` workloads. Set `DBA_GKE_DISTRIBUTION` because Cost Copilot never infers it, and set the cluster variables when one project hosts same-named workloads in several clusters.

The supported window is 1-31 days (default minimum 7). Classification is `active`, `candidate`, or `blocked`. Connections never establish idleness; work-rate p95, active MySQL threads, CPU utilization, and the zero-work ratio must all support it. A workload is a `candidate` only when those thresholds are met and a non-secret annotation or operator label marks it eligible for cost review.

Shutdown is never recommended for failover replicas, primaries with active backups, production workloads, or workloads lacking an owner or environment marker. Persistent data and unknown backup schedules are risks and block candidacy. Recommendations include database-operator, backup-owner, and service-owner approval requirements and are advisory only.

## ClickHouse clusters

ClickHouse is assessed as `metadata.assessment_unit="cluster"`. The reader recognizes native `ClickHouseProfileEvents_*`, `ClickHouseMetrics_*`, and `ClickHouseAsyncMetrics_*` names plus Altinity `chi_clickhouse_event_*` and `chi_clickhouse_metric_*` aliases for queries, SELECT and INSERT work, inserted rows, active queries, client connections, and running merges. Query work and inserted rows are both work, so Kafka or S3Queue ingestion is reported as active and `metadata.ingest_only` distinguishes insert-only traffic.

The cluster is `candidate` only when every member of every shard passes work, CPU, coverage, and safety checks. One busy replica makes the cluster active; one incomplete replica blocks it. `metadata.cluster_members` preserves shard/replica hints. Keeper or ZooKeeper pods attached by cluster/instance labels are `metadata.protected_members` and are protected cluster cost, not independent savings. The tool never models removing one replica as savings; replica-count right-sizing is an owner-approval item.

ClickHouse backup evidence combines matching CronJobs, sidecar/container names, and recent `system.backups` rows when the existing read-only exec path is available. `system.disks` and `system.clusters` supply object-storage disks and remote/cross-cluster consumers as `metadata.approval_risks`; those approvals are separate from compute savings. Missing evidence stays missing and is never fabricated.

## Output envelope

JSON is the default and is intended for Cost Copilot. Stable fields are `schema_version`, `generated_at`, `status`, `data.workloads[*]` attribution, classification, metrics, risks, owner, environment, and eligibility evidence, plus project, window, source, and missing-metric metadata. Each workload also carries a `kubernetes` envelope with node/controller context, requests and limits, PVCs, backup evidence, and node-pool information so downstream services need no Kubernetes access. Non-secret metadata may be added, but existing fields and the schema version remain stable.

Human output summarizes the same evidence for operators. All output avoids Secrets, credential values, and raw metric labels that could contain sensitive identity data.

## Orchestration with Cost Copilot

1. Ask DBA Agent for `collect_database_idle_assessment` and use `output_mode=json`.
2. If modeling needs missing node, namespace, or resource context, ask Diagnostic Agent. Cost Copilot has no Kubernetes access.
3. Pass the DBA JSON envelope verbatim to Cost Copilot's `estimate_database_cost_impact`. It models every workload in `data.workloads`, preserving separate attribution, protected costs, savings, and currency. For ClickHouse, read `result.cluster` for the single cluster decision; `result.workloads` remains per-pod transparency.
4. Render a combined report with idle evidence, risks, required approvals, cost attribution, and marginal savings. The result is advisory: explicit database, backup, and service-owner approval is required, and no shutdown or mutation is automatic.

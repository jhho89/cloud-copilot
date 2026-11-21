# Idle database cost engineering

`collect_database_idle_assessment` is a metrics-only cost assessment for MySQL, MariaDB, Redis, and Valkey. It returns `schema_version=cloud-copilot.dba.idle-cost.v1`, preserves workload attribution, and is advisory. It does not use pod/exec as a metrics fallback, install an exporter, read Secrets, accept credentials, or mutate Kubernetes or database state.

## Prerequisites and IAM

The function reads an existing Google Managed Prometheus exporter through Cloud Monitoring. Operators must already run and scrape `mysqld_exporter`, `redis_exporter`, or `valkey-exporter`; the service never installs one. Sidecars, database-native metric ports, and `prometheus.io/*` annotations are discovery evidence only. The profile-specific recommendation is `mysqld_exporter`, `redis_exporter`, or `valkey-exporter` when no enabled exporter or matching series is found.

The caller needs read access to the configured Cloud Monitoring project, typically `roles/monitoring.viewer`, plus Kubernetes read access for pods, namespaces, PVCs, CronJobs, and nodes. Configure `DBA_DATABASE_METRICS_PROJECT_ID`; the service falls back to `DBA_REDIS_METRICS_PROJECT_ID`, `GOOGLE_CLOUD_PROJECT`, and `GCP_PROJECT_ID`, then inferred Application Default Credentials. Never place database credentials in annotations or function arguments.

## Profiles and evidence

A declarative profile controls aliases, counter/gauge kinds, required logical metrics, exporter identities, role labels, backup names, and the operator recommendation. Exporter-version differences are resolved by aliases before pod matching. The reader queries one Cloud Monitoring metric type at a time for all workload targets, then matches series labels to pods; it does not fan out per pod.

Counters and gauges are aggregated hourly. A counter decrease is treated as unavailable work-rate evidence and a restart, not as a negative rate. Missing series and data gaps remain explicit. Kubernetes container evidence comes from the named GKE types for CPU and memory utilization and network sent/received bytes.

## Classification

Thresholds are configurable and conservative:

| Variable | Default |
| --- | --- |
| `DBA_IDLE_ASSESSMENT_MIN_DAYS` | `1` |
| `DBA_IDLE_WORK_P95_OPS_PER_SEC` | `0.01` |
| `DBA_IDLE_CPU_P95_FRACTION` | `0.05` |
| `DBA_IDLE_MIN_ZERO_WORK_RATIO` | `0.90` |
| `DBA_COST_REVIEW_ELIGIBILITY_ANNOTATION` | `cloud-copilot.ai/cost-review` |

The supported window is 1-31 days. Classification is `active`, `candidate`, or `blocked`. Connections never establish idleness; work-rate p95, active MySQL threads, CPU utilization, and the zero-work ratio must all support it. A workload is a `candidate` only when those thresholds are met and a non-secret annotation or operator label marks it eligible for cost review.

Shutdown is never recommended for failover replicas, primaries with active backups, production workloads, or workloads lacking an owner or environment marker. Persistent data and unknown backup schedules are risks and block candidacy. Recommendations include database-operator, backup-owner, and service-owner approval requirements and are advisory only.

## Output envelope

JSON is the default and is intended for Cost Copilot. Stable fields are `schema_version`, `generated_at`, `status`, `data.workloads[*]` attribution, classification, metrics, risks, owner, environment, and eligibility evidence, plus project, window, source, and missing-metric metadata. Each workload also carries a `kubernetes` envelope with node/controller context, requests and limits, PVCs, backup evidence, and node-pool information so downstream services need no Kubernetes access. Non-secret metadata may be added, but existing fields and the schema version remain stable.

Human output summarizes the same evidence for operators. All output avoids Secrets, credential values, and raw metric labels that could contain sensitive identity data.

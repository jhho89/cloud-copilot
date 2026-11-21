# BigQuery Billing Export Schema Assumptions

The Cost Copilot service reads a standard **detailed usage cost** billing export (v1) table.

## Table Location

Set these environment variables so the service can find the export:

- `BILLING_EXPORT_PROJECT` - the project that hosts the export dataset (falls back to `GOOGLE_CLOUD_PROJECT` or `GCP_PROJECT_ID`).
- `BILLING_EXPORT_DATASET` - the BigQuery dataset containing the export.
- `BILLING_EXPORT_TABLE` - the export table, named `gcp_billing_export_v1_<BILLING_ACCOUNT_ID>` by default.

## Expected Columns

| Column | Type | Notes |
| --- | --- | --- |
| `service.description` | `STRING` | Human-readable service name, e.g. "Compute Engine". |
| `sku.description` | `STRING` | SKU-level description. |
| `project.id` / `project.name` | `STRING` | The GCP project that incurred the cost. |
| `labels` | `REPEATED RECORD` with `key`/`value` `STRING` fields | User labels attached to usage. |
| `cost` | `FLOAT64` | Cost after discounts before taxes and adjustments. |
| `currency` | `STRING` | Billing currency code, e.g. `USD`. |
| `usage_start_time` | `TIMESTAMP` | Start of the usage interval. |
| `usage_end_time` | `TIMESTAMP` | End of the usage interval. |

## GKE Cost-Allocation System Labels

Database idle-impact analysis reads GKE cost-allocation labels from the same
`labels` field:

| Group key | System label |
| --- | --- |
| `cluster` | `goog-k8s-cluster-name` |
| `cluster_location` | `goog-k8s-cluster-location` |
| `namespace` | `goog-k8s-namespace` |
| `workload` | `goog-k8s-workload-name` |
| `workload_type` | `goog-k8s-workload-type` |
| `node_pool` | `goog-k8s-node-pool-name` |

The SQL extracts each label as a scalar subquery before grouping. This preserves
one billing row as one row and avoids multiplying cost by the number of labels.
The database query groups by all six labels, project, service, SKU, and currency,
then applies a caller-constrained date window. Missing or unassigned labels are
reported as `unlabeled`; exact workload savings require all six GKE labels.

If GKE cost allocation is disabled or labels are absent, database impact output
reports `billing_attribution.status = unavailable`. It does not infer node-level
spend. Project-level unlabeled storage, backup, load-balancer, and static-IP rows
can still be shown as visibility, but they are not assigned to a workload and are
never counted as savings.

Cost totals include a `currency` column and are grouped per currency. The
database tool never adds values across currencies. A 1-90 day observed window is
normalized to a 30-day projection using `30 / days`; date bounds remain
parameterized.

## DBA Envelope Contract

`estimate_database_cost_impact` accepts only JSON with:

```json
{
  "schema_version": "cloud-copilot.dba.idle-cost.v1",
  "generated_at": "2026-11-21T00:00:00Z",
  "status": "ok",
  "data": {"workloads": []},
  "metadata": {
    "project_id": "example-project",
    "window_days": 14,
    "metrics_source": "cloud_monitoring",
    "missing_metrics": []
  }
}
```

Each workload must include namespace, pod, database type, node, controller name
and type, metrics, classification, owner, environment, and eligibility evidence.
The tool validates and models every workload in the array; it does not stop after
the first item.
The minimum metrics are `p95_work_per_second`, `mean_active_threads`,
`p95_cpu_fraction`, and `zero_work_hour_ratio`.

Extra non-secret Kubernetes metadata may be preserved for modeling, especially
distribution (`Autopilot` or `Standard`), node name and pool, controller, PVCs,
backups, requests/capacity, and node-pool consolidation evidence. Useful modeling
fields include:

- `node_pool_consolidation.other_non_daemonset_workloads`
- `node_pool_consolidation.remaining_pods_fit_existing_nodes`
- `node_pool_consolidation.node_monthly_cost` (or `node_count` with attributed
  node-pool cost)
- `scheduled_shutdown.approved_off_hours_per_month`
- `scheduled_shutdown.shutdowns_per_month`
- `scheduled_shutdown.minimum_run_minutes`
- `scheduled_shutdown.restart_warmup_minutes`
- `cost_review.approved_idle_hours`

The tool defaults to conservative values of 15 minutes minimum run, 10 minutes
restart/warm-up, and one monthly shutdown when schedule evidence omits them.
It does not accept credentials or tokens as arguments.

## Time and Currency Normalization

- The service converts `usage_start_time` to a UTC calendar date via `DATE(usage_start_time)`.
- Date filters are parameterized with `DATE` parameters, so they are timezone-neutral in SQL.
- Costs are summed per currency; results never mix currencies. The `currency` column is returned with every grouped total.

## Permissions

The runtime service account needs:

- `roles/bigquery.jobUser` on the export project (or wherever jobs run).
- `roles/bigquery.dataViewer` on the export dataset.

The runtime does not need Kubernetes RBAC for this tool. The DBA envelope is the
only source of workload identity and evidence; the cost tool performs no
Kubernetes discovery and no mutations.

## Diagnostics

| Symptom | Cause | Fix |
| --- | --- | --- |
| "Billing export is not configured" | One of the three env vars above is missing. | Set the missing variable and redeploy. |
| "Billing export table not found" | Wrong table name or the export is disabled. | Verify the export and table id in Cloud Billing. |
| "BigQuery access denied" | Missing `bigquery.jobUser` or `dataViewer`. | Grant the roles listed above. |
| "no rows for the requested window" | The export is enabled but empty for that period. | Wait for data or widen the date range; exports can lag 24-48 hours. |

Credentials are never accepted in tool arguments; the service uses Application Default Credentials only.

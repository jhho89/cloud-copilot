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

## Time and Currency Normalization

- The service converts `usage_start_time` to a UTC calendar date via `DATE(usage_start_time)`.
- Date filters are parameterized with `DATE` parameters, so they are timezone-neutral in SQL.
- Costs are summed per currency; results never mix currencies. The `currency` column is returned with every grouped total.

## Permissions

The runtime service account needs:

- `roles/bigquery.jobUser` on the export project (or wherever jobs run).
- `roles/bigquery.dataViewer` on the export dataset.

## Diagnostics

| Symptom | Cause | Fix |
| --- | --- | --- |
| "Billing export is not configured" | One of the three env vars above is missing. | Set the missing variable and redeploy. |
| "Billing export table not found" | Wrong table name or the export is disabled. | Verify the export and table id in Cloud Billing. |
| "BigQuery access denied" | Missing `bigquery.jobUser` or `dataViewer`. | Grant the roles listed above. |
| "no rows for the requested window" | The export is enabled but empty for that period. | Wait for data or widen the date range; exports can lag 24-48 hours. |

Credentials are never accepted in tool arguments; the service uses Application Default Credentials only.

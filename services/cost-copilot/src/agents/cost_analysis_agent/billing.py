"""
BigQuery billing-export integration for the Cost Analysis agent.

This module queries a standard Cloud Billing export to BigQuery table. The
expected export is the *detailed usage cost* export (v1), whose table name
looks like ``gcp_billing_export_v1_<BILLING_ACCOUNT_ID>``. See
docs/BIGQUERY-BILLING-EXPORT.md for the schema assumptions.

Credentials are always obtained from Application Default Credentials (ADC)
or the ambient Cloud Run service account; this module never accepts tokens
or key material as arguments.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from typing import Any

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery

BILLING_EXPORT_SCHEMA = {
    "service": {"description": "STRING", "sku": {"description": "STRING"}},
    "project": {"id": "STRING", "name": "STRING"},
    "labels": "REPEATED RECORD with key/value STRING fields",
    "cost": "FLOAT64",
    "currency": "STRING",
    "usage_start_time": "TIMESTAMP",
    "usage_end_time": "TIMESTAMP",
}


class BillingExportError(Exception):
    """Base error for billing-export problems."""


class BillingExportNotConfigured(BillingExportError):
    """Raised when the billing export environment variables are missing."""


class BillingExportNotFound(BillingExportError):
    """Raised when the configured export table cannot be found."""


class BillingPermissionError(BillingExportError):
    """Raised when BigQuery access is denied for the configured table."""


class EmptyBillingDataset(BillingExportError):
    """Raised when the export exists but contains no rows in the window."""


def get_billing_export_target() -> tuple[str, str, str]:
    """Return ``(project, dataset, table)`` from environment variables."""
    project = os.getenv("BILLING_EXPORT_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BILLING_EXPORT_DATASET")
    table = os.getenv("BILLING_EXPORT_TABLE")
    if not project or not dataset or not table:
        missing = [
            name
            for name, value in (
                ("BILLING_EXPORT_PROJECT (or GOOGLE_CLOUD_PROJECT/GCP_PROJECT_ID)", project),
                ("BILLING_EXPORT_DATASET", dataset),
                ("BILLING_EXPORT_TABLE", table),
            )
            if not value
        ]
        raise BillingExportNotConfigured(
            "Billing export is not configured. Set the following environment "
            "variables: " + ", ".join(missing) + "."
        )
    return project, dataset, table


def billing_export_table_id() -> str:
    """Return the fully qualified BigQuery table id, without credentials."""
    project, dataset, table = get_billing_export_target()
    return f"{project}.{dataset}.{table}"


def _normalize_day(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            return None
    return None


def _utcnow() -> datetime:
    return datetime.now(UTC)


def normalize_window(days: int = 30, end_date: Any | None = None) -> tuple[date, date]:
    """Normalize a look-back window to inclusive UTC dates ``(start, end)``."""
    if not isinstance(days, int) or days <= 0:
        raise ValueError("days must be a positive integer")
    end = _normalize_day(end_date) or _utcnow().date()
    if end_date is not None and _normalize_day(end_date) is None:
        raise ValueError("end_date must be an ISO date string or date object")
    return end - timedelta(days=days - 1), end


def build_cost_breakdown_query(
    group_by: list[str],
    days: int = 30,
    end_date: Any | None = None,
) -> tuple[str, dict[str, Any]]:
    """Build a parameterized cost-breakdown query for the billing export."""
    allowed = {
        "service": "service.description",
        "sku": "sku.description",
        "project": "project.id",
        "date": "DATE(usage_start_time)",
    }
    keys = [k.strip().lower() for k in group_by]
    invalid = [k for k in keys if k not in allowed]
    if invalid:
        raise ValueError(f"Unsupported group-by field(s): {', '.join(invalid)}. Allowed: {', '.join(sorted(allowed))}")
    start, end = normalize_window(days, end_date)
    select_parts = [allowed[k] for k in keys]
    query = f"""
        SELECT
          {', '.join(select_parts)},
          currency,
          SUM(cost) AS total_cost
        FROM `{billing_export_table_id()}`
        WHERE DATE(usage_start_time) BETWEEN @start_date AND @end_date
        GROUP BY {', '.join(keys)}, currency
        ORDER BY total_cost DESC
        LIMIT 100
    """
    params = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    return query, params


def build_anomaly_query(days: int = 30) -> tuple[str, dict[str, Any]]:
    """Build a daily cost query for anomaly detection."""
    start, end = normalize_window(days)
    query = f"""
        SELECT
          DATE(usage_start_time) AS day,
          currency,
          SUM(cost) AS total_cost
        FROM `{billing_export_table_id()}`
        WHERE DATE(usage_start_time) BETWEEN @start_date AND @end_date
        GROUP BY day, currency
        ORDER BY day
    """
    params = {"start_date": start.isoformat(), "end_date": end.isoformat()}
    return query, params


def _redact(value: str, max_len: int = 6) -> str:
    """Return a short, non-reversible prefix for log-safe messages."""
    return value[:max_len] + "..." if len(value) > max_len else value


def _run_query(client: bigquery.Client, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """Run a parameterized query and return rows as dicts."""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(k, "DATE", v) for k, v in params.items()
        ]
    )
    try:
        rows = list(client.query(query, job_config=job_config).result())
    except Forbidden as exc:
        raise BillingPermissionError(
            "BigQuery access denied. Ensure the service account has "
            "roles/bigquery.jobUser on the billing project and "
            "roles/bigquery.dataViewer on the export dataset."
        ) from exc
    except NotFound as exc:
        raise BillingExportNotFound(
            "Billing export table not found. Verify the export exists and the "
            "table name matches the configured BILLING_EXPORT_TABLE."
        ) from exc
    return [dict(row) for row in rows]


def get_billing_client() -> bigquery.Client:
    """Create a BigQuery client using Application Default Credentials."""
    project, _, _ = get_billing_export_target()
    return bigquery.Client(project=project)


def _labels_to_map(labels: Any) -> dict[str, str]:
    if not labels:
        return {}
    result = {}
    for item in labels:
        if isinstance(item, dict) and "key" in item:
            result[item["key"]] = item.get("value", "")
    return result


def get_cost_breakdown(
    group_by: list[str],
    days: int = 30,
    end_date: Any | None = None,
    client: bigquery.Client | None = None,
) -> list[dict[str, Any]]:
    """Return grouped cost totals from the billing export."""
    if not group_by:
        raise ValueError("group_by must contain at least one field")
    group_by = [k.strip().lower() for k in group_by]
    if len(set(group_by)) != len(group_by):
        raise ValueError("group_by contains duplicate fields")
    if "label" in group_by:
        raise ValueError("label breakdown requires the unnest path; not yet implemented in this helper")
    query, params = build_cost_breakdown_query(group_by, days, end_date)
    keys = group_by
    client = client or get_billing_client()
    rows = _run_query(client, query, params)
    if not rows:
        raise EmptyBillingDataset(
            "Billing export contains no rows for the requested window. "
            "Check that the export is enabled and covers this period."
        )
    # Normalize label rows into a consistent shape.
    for row in rows:
        if "label" in keys and "label_value" in row:
            row["label"] = {row["label"]: row.pop("label_value", "")}
    return rows


def detect_anomalies(
    daily_costs: list[dict[str, Any]],
    threshold_pct: float = 30.0,
) -> list[dict[str, Any]]:
    """Detect simple percentage-change anomalies in daily costs.

    A day is anomalous when its cost differs from the mean of the previous
    days by more than ``threshold_pct`` percent (in either direction).
    """
    if threshold_pct <= 0:
        raise ValueError("threshold_pct must be positive")
    anomalies: list[dict[str, Any]] = []
    history: list[float] = []
    for row in sorted(daily_costs, key=lambda r: r["day"]):
        cost = float(row["total_cost"])
        currency = row.get("currency", "")
        if history:
            mean = sum(history) / len(history)
            if mean > 0:
                change_pct = abs(cost - mean) / mean * 100
                if change_pct > threshold_pct:
                    anomalies.append(
                        {
                            "day": row["day"],
                            "currency": currency,
                            "cost": cost,
                            "mean_prior": round(mean, 2),
                            "change_pct": round(change_pct, 2),
                            "direction": "spike" if cost > mean else "drop",
                        }
                    )
        history.append(cost)
    return anomalies


def detect_cost_anomalies(
    days: int = 30,
    threshold_pct: float = 30.0,
    client: bigquery.Client | None = None,
) -> list[dict[str, Any]]:
    """Query daily costs and detect anomalies."""
    query, params = build_anomaly_query(days)
    client = client or get_billing_client()
    rows = _run_query(client, query, params)
    if not rows:
        raise EmptyBillingDataset(
            "Billing export contains no rows for the requested window."
        )
    return detect_anomalies(rows, threshold_pct)


def format_cost_breakdown(rows: list[dict[str, Any]], group_by: list[str]) -> str:
    """Format grouped cost rows into a readable table."""
    if not rows:
        return "No cost data found for the requested period."
    keys = [k.strip().lower() for k in group_by]
    lines = ["## Cost Breakdown", ""]
    header = " | ".join(keys + ["currency", "total_cost"])
    lines.append(header)
    lines.append("-" * len(header))
    for row in rows:
        values = []
        for key in keys:
            value = row.get(key, "")
            if key == "label" and isinstance(value, dict):
                value = ", ".join(f"{k}={v}" for k, v in value.items())
            values.append(str(value))
        values.extend([str(row.get("currency", "")), f"{row.get('total_cost', 0):.2f}"])
        lines.append(" | ".join(values))
    return "\n".join(lines)

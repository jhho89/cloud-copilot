"""Database idle-cost impact modeling from a DBA-supplied envelope.

This tool is intentionally envelope-driven.  It never discovers Kubernetes
workloads and never changes Kubernetes or database state.  Billing is read
from the configured BigQuery billing export with Application Default
Credentials only.
"""

from __future__ import annotations

import copy
import json
import math
from datetime import UTC, datetime
from typing import Any

from google.cloud import bigquery

from .billing import (
    BillingExportError,
    BillingExportNotConfigured,
    EmptyBillingDataset,
    get_cost_breakdown,
    normalize_window,
)

IDLE_COST_SCHEMA = "cloud-copilot.dba.idle-cost.v1"
OUTPUT_SCHEMA = "cloud-copilot.cost.idle-impact.v1"
ELIGIBILITY_EVIDENCE = "cloud-copilot.ai/cost-review=eligible"
MONTH_HOURS = 730
DEFAULT_MINIMUM_RUN_MINUTES = 15
DEFAULT_RESTART_WARMUP_MINUTES = 10
DEFAULT_SHUTDOWNS_PER_MONTH = 1

_ATTRIBUTION_KEYS = ("cluster", "cluster_location", "namespace", "workload", "workload_type", "node_pool")
_GROUP_KEYS = (*_ATTRIBUTION_KEYS, "project", "service", "sku")


class IdleAssessmentError(ValueError):
    """Raised when a DBA envelope does not satisfy the v1 contract."""


def _require_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IdleAssessmentError(f"{field} must be a non-empty string")
    return value


def _require_number(value: Any, field: str, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise IdleAssessmentError(f"{field} must be a finite number")
    number = float(value)
    if number < minimum:
        raise IdleAssessmentError(f"{field} must be at least {minimum}")
    return number


def _parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise IdleAssessmentError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IdleAssessmentError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise IdleAssessmentError(f"{field} must include a timezone")
    return parsed


def parse_idle_assessment(raw: str | dict[str, Any]) -> dict[str, Any]:
    """Validate and deep-copy a DBA idle-cost envelope."""
    if isinstance(raw, str):
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise IdleAssessmentError(f"invalid JSON: {exc.msg}") from exc
    elif isinstance(raw, dict):
        envelope = raw
    else:
        raise IdleAssessmentError("idle_assessment must be a JSON object")

    if not isinstance(envelope, dict):
        raise IdleAssessmentError("idle_assessment must be a JSON object")
    if envelope.get("schema_version") != IDLE_COST_SCHEMA:
        raise IdleAssessmentError(f"schema_version must be {IDLE_COST_SCHEMA}")
    if envelope.get("status") != "ok":
        raise IdleAssessmentError("status must be ok")
    _parse_timestamp(envelope.get("generated_at"), "generated_at")

    data = envelope.get("data")
    metadata = envelope.get("metadata")
    if not isinstance(data, dict) or not isinstance(metadata, dict):
        raise IdleAssessmentError("data and metadata must be objects")

    workloads = data.get("workloads")
    if not isinstance(workloads, list) or not workloads:
        raise IdleAssessmentError("data.workloads must be a non-empty array")

    for index, workload in enumerate(workloads):
        prefix = f"data.workloads[{index}]"
        if not isinstance(workload, dict):
            raise IdleAssessmentError(f"{prefix} must be an object")
        for field in ("namespace", "pod", "db_type", "classification"):
            _require_text(workload.get(field), f"{prefix}.{field}")
        # An unscheduled or bare pod legitimately has no node or controller.
        for field in ("node_name", "controller_name", "controller_type"):
            if not isinstance(workload.get(field), str):
                raise IdleAssessmentError(f"{prefix}.{field} must be a string")
        metrics = workload.get("metrics")
        if not isinstance(metrics, dict):
            raise IdleAssessmentError(f"{prefix}.metrics must be an object")
        # DBA emits null for evidence it could not collect (no exporter, or no
        # thread metric for Redis); the key itself must still be present.
        for field in ("p95_work_per_second", "mean_active_threads", "p95_cpu_fraction", "zero_work_hour_ratio"):
            if field not in metrics:
                raise IdleAssessmentError(f"{prefix}.metrics.{field} is required")
            if metrics[field] is None:
                continue
            _require_number(metrics[field], f"{prefix}.metrics.{field}", 0.0)
            if field in {"p95_cpu_fraction", "zero_work_hour_ratio"} and float(metrics[field]) > 1.0:
                raise IdleAssessmentError(f"{prefix}.metrics.{field} must be between 0 and 1")
        if not isinstance(workload.get("risks", []), list):
            raise IdleAssessmentError(f"{prefix}.risks must be an array")
        if not isinstance(workload.get("labels", {}), dict):
            raise IdleAssessmentError(f"{prefix}.labels must be an object")

    _require_text(metadata.get("project_id"), "metadata.project_id")
    window_days = _require_number(metadata.get("window_days"), "metadata.window_days", 1)
    if not float(window_days).is_integer() or int(window_days) > 90:
        raise IdleAssessmentError("metadata.window_days must be an integer from 1 to 90")
    _require_text(metadata.get("metrics_source"), "metadata.metrics_source")
    missing_metrics = metadata.get("missing_metrics")
    if not isinstance(missing_metrics, list) or any(not isinstance(item, str) for item in missing_metrics):
        raise IdleAssessmentError("metadata.missing_metrics must be an array of strings")

    return copy.deepcopy(envelope)


def _month_factor(days: int) -> float:
    return 30.0 / days


def _is_labeled(value: Any) -> bool:
    return isinstance(value, str) and value != "unlabeled"


def _has_attribution_labels(row: dict[str, Any]) -> bool:
    return all(_is_labeled(row.get(key)) for key in _ATTRIBUTION_KEYS)


def _amount(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        return None
    return max(0.0, float(value))


def _cost_bucket(service: Any, sku: Any) -> str:
    text = f"{service} {sku}".lower()
    if any(term in text for term in ("snapshot", "backup")):
        return "backups_snapshots"
    if any(term in text for term in ("persistent", "disk", "storage")):
        return "persistent_storage"
    if "load balancing" in text or "forwarding rule" in text:
        return "load_balancers"
    if any(term in text for term in ("static ip", "external ip", "ip address")):
        return "static_ips"
    if "compute engine" in text or "kubernetes engine" in text:
        return "compute"
    return "other"


def _single_currency_total(rows: list[dict[str, Any]], field: str = "total_cost") -> dict[str, Any] | None:
    currencies = {row.get("currency") for row in rows}
    if len(currencies) != 1 or not next(iter(currencies), None):
        return None
    total = sum(_amount(row.get(field)) or 0.0 for row in rows)
    return {"currency": next(iter(currencies)), "amount": round(total, 6)}


def _add_amount(target: dict[str, float], currency: str | None, value: Any) -> None:
    amount = _amount(value)
    if amount is not None and currency:
        target[currency] = target.get(currency, 0.0) + amount


def _monthly(amount: float, days: int) -> float:
    return round(amount * _month_factor(days), 6)


def _cluster_pin(metadata: dict[str, Any]) -> dict[str, str]:
    kubernetes = metadata.get("kubernetes")
    if not isinstance(kubernetes, dict):
        return {}
    return {
        key: kubernetes[key]
        for key in ("cluster", "cluster_location")
        if isinstance(kubernetes.get(key), str) and kubernetes[key].strip()
    }


def _match_workload_rows(
    rows: list[dict[str, Any]], workload: dict[str, Any], project_id: str, metadata: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    workload_names = {workload["controller_name"], workload["pod"]}
    pin = _cluster_pin(metadata or {})
    return [
        row
        for row in rows
        if row.get("project") == project_id
        and _is_labeled(row.get("cluster"))
        and all(row.get(key) == value for key, value in pin.items())
        and row.get("namespace") == workload["namespace"]
        and row.get("workload") in workload_names
        and row.get("workload_type") == workload["controller_type"]
    ]


def _spans_multiple_clusters(rows: list[dict[str, Any]]) -> bool:
    return len({(row.get("cluster"), row.get("cluster_location")) for row in rows}) > 1


def _attribution_from_rows(
    rows: list[dict[str, Any]],
    workload: dict[str, Any],
    project_id: str,
    days: int,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not rows:
        return {
            "status": "unavailable",
            "reason": "no billing rows for this workload; GKE cost allocation may be disabled or labels may be absent",
            "currencies": [],
        }

    currencies = sorted({str(row.get("currency")) for row in rows if row.get("currency")})
    workload_rows = _match_workload_rows(rows, workload, project_id, metadata)
    if _spans_multiple_clusters(workload_rows):
        return {
            "status": "unavailable",
            "reason": (
                "workload name matches billing rows in more than one GKE cluster; "
                "set DBA_GKE_CLUSTER_NAME and DBA_GKE_CLUSTER_LOCATION so costs are not summed across clusters"
            ),
            "currencies": currencies,
        }
    if not workload_rows:
        return {
            "status": "unavailable",
            "reason": "no exact GKE workload attribution; node-level spend was not fabricated",
            "currencies": currencies,
        }

    attribution = _single_currency_total(workload_rows)
    if attribution is None:
        return {
            "status": "unavailable",
            "reason": "multiple currencies in the matched attribution window; currencies were not mixed",
            "currencies": currencies,
        }

    fully_labeled = all(_has_attribution_labels(row) for row in workload_rows)
    return {
        "status": "available" if fully_labeled else "partial",
        "observed_cost": attribution,
        "observed_window_days": days,
        "attributed_monthly_cost": {
            "currency": attribution["currency"],
            "amount": _monthly(attribution["amount"], days),
        },
        "currencies": currencies,
        "missing_labels": [] if fully_labeled else ["one or more GKE cost-allocation labels"],
    }


def _protected_costs(rows: list[dict[str, Any]], days: int) -> dict[str, Any]:
    totals = {
        "persistent_storage": {},
        "backups_snapshots": {},
        "load_balancers": {},
        "static_ips": {},
    }
    for row in rows:
        bucket = _cost_bucket(row.get("service"), row.get("sku"))
        if bucket in totals:
            _add_amount(totals[bucket], row.get("currency"), row.get("total_cost"))

    def shaped(values: dict[str, float]) -> dict[str, Any]:
        return {currency: round(total, 6) for currency, total in sorted(values.items())}

    shaped_totals = {key: shaped(value) for key, value in totals.items()}
    return {
        "window_observed": shaped_totals,
        "monthly_projection": {
            key: {currency: _monthly(value, days) for currency, value in totals.items()}
            for key, totals in totals.items()
        },
        "note": "These costs remain after compute changes and are not counted as compute savings.",
    }


def _compute_cost(rows: list[dict[str, Any]], days: int) -> dict[str, Any]:
    compute_rows = [row for row in rows if _cost_bucket(row.get("service"), row.get("sku")) == "compute"]
    total = _single_currency_total(compute_rows)
    if total is None:
        return {"status": "unavailable", "reason": "compute SKU attribution is unavailable or spans multiple currencies"}
    return {
        "status": "available",
        "observed_cost": total,
        "monthly_cost": {"currency": total["currency"], "amount": _monthly(total["amount"], days)},
    }


def _unattributed_costs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], float] = {}
    for row in rows:
        if _has_attribution_labels(row):
            continue
        key = (str(row.get("service", "unknown")), str(row.get("sku", "unknown")), str(row.get("currency", "")))
        grouped[key] = grouped.get(key, 0.0) + (_amount(row.get("total_cost")) or 0.0)
    return [
        {"service": service, "sku": sku, "currency": currency, "observed_cost": round(total, 6)}
        for (service, sku, currency), total in sorted(grouped.items())
    ]


def _nested(metadata: dict[str, Any], workload: dict[str, Any], key: str) -> dict[str, Any]:
    candidates = (
        metadata.get(key),
        metadata.get("kubernetes", {}).get(key) if isinstance(metadata.get("kubernetes"), dict) else None,
        workload.get(key),
    )
    return next((value for value in candidates if isinstance(value, dict)), {})


def _cost_review_eligible(workload: dict[str, Any]) -> bool:
    """Prefer DBA's explicit flag; the evidence string depends on a configurable annotation name."""
    flag = workload.get("cost_review_eligible")
    if isinstance(flag, bool):
        return flag
    return workload.get("eligibility_evidence") == ELIGIBILITY_EVIDENCE


def _zero_work_ratio(workload: dict[str, Any]) -> float:
    value = workload["metrics"].get("zero_work_hour_ratio")
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0


def _workload_node_pool(workload: dict[str, Any]) -> Any:
    kubernetes = workload.get("kubernetes")
    if isinstance(kubernetes, dict) and kubernetes.get("node_pool"):
        return kubernetes["node_pool"]
    return workload.get("node_pool")


def _approved_idle_hours(workload: dict[str, Any], metadata: dict[str, Any]) -> float | None:
    for source in (_nested(metadata, workload, "cost_review"), workload):
        if "approved_idle_hours" in source:
            return min(MONTH_HOURS, max(0.0, _require_number(source["approved_idle_hours"], "approved_idle_hours")))
    if _cost_review_eligible(workload):
        return MONTH_HOURS * _zero_work_ratio(workload)
    return None


def _autopilot_scenario(compute: dict[str, Any], workload: dict[str, Any], metadata: dict[str, Any], days: int) -> dict[str, Any]:
    if compute["status"] != "available":
        return {
            "name": "autopilot_idle_scale_down",
            "status": "unavailable",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": ["compute attribution"],
        }
    approved = _approved_idle_hours(workload, metadata)
    if approved is None:
        return {
            "name": "autopilot_idle_scale_down",
            "status": "requires_evidence",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": ["approved idle hours or eligible zero-work evidence"],
        }
    ratio = approved / MONTH_HOURS
    currency = compute["monthly_cost"]["currency"]
    savings = round(compute["monthly_cost"]["amount"] * ratio, 6)
    zero_ratio = _zero_work_ratio(workload)
    return {
        "name": "autopilot_idle_scale_down",
        "status": "estimate" if zero_ratio >= 0.99 else "conditional",
        "monthly_savings": {"currency": currency, "amount": savings},
        "approved_idle_hours": round(approved, 2),
        "note": "Autopilot pod-request compute is marginal. PVC and backup costs remain.",
    }


def _standard_scenario(compute: dict[str, Any], workload: dict[str, Any], metadata: dict[str, Any], rows: list[dict[str, Any]], days: int) -> dict[str, Any]:
    if compute["status"] != "available":
        return {
            "name": "node_consolidation",
            "status": "unavailable",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": ["compute attribution"],
            "note": "No marginal savings are inferred without billing attribution.",
        }
    evidence = _nested(metadata, workload, "node_pool_consolidation")
    missing = ["node_pool_consolidation.other_non_daemonset_workloads", "node_pool_consolidation.remaining_pods_fit_existing_nodes", "removable node monthly cost"]
    if not evidence:
        return {
            "name": "node_consolidation",
            "status": "requires_evidence",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": missing,
            "note": "Standard GKE workload attribution is not automatically savings.",
        }

    other = evidence.get("other_non_daemonset_workloads")
    fits = evidence.get("remaining_pods_fit_existing_nodes")
    if not isinstance(other, int) or isinstance(other, bool) or other < 0:
        return {
            "name": "node_consolidation",
            "status": "requires_evidence",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": [missing[0]],
        }
    if not isinstance(fits, bool):
        return {
            "name": "node_consolidation",
            "status": "requires_evidence",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": [missing[1]],
        }
    if other > 0 or not fits:
        return {
            "name": "node_consolidation",
            "status": "no_savings",
            "monthly_savings": {"currency": _currency_or_empty(compute), "amount": 0.0},
            "missing_evidence": [],
            "note": "The database is not a removable last workload or remaining pods do not fit.",
        }

    node_cost = _amount(evidence.get("node_monthly_cost"))
    currency = _currency_or_empty(compute)
    node_pool = _workload_node_pool(workload)
    if node_cost is None and _is_labeled(node_pool):
        pin = _cluster_pin(metadata)
        pool_rows = [
            row
            for row in rows
            if row.get("project") == metadata["project_id"]
            and row.get("node_pool") == node_pool
            and row.get("currency") == currency
            and all(row.get(key) == value for key, value in pin.items())
        ]
        pool_total = sum(_amount(row.get("total_cost")) or 0.0 for row in pool_rows)
        node_count = evidence.get("node_count")
        if pool_total > 0 and isinstance(node_count, int) and not isinstance(node_count, bool) and node_count > 0:
            node_cost = _monthly(pool_total, days) / node_count
    if node_cost is None or node_cost <= 0:
        return {
            "name": "node_consolidation",
            "status": "requires_evidence",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": [missing[2]],
        }
    return {
        "name": "node_consolidation",
        "status": "estimate",
        "monthly_savings": {"currency": currency, "amount": round(node_cost, 6)},
        "note": "Savings use the supplied removable-node cost and same-node-pool fit evidence.",
    }


def _currency_or_empty(compute: dict[str, Any]) -> str:
    if compute["status"] == "available":
        return compute["monthly_cost"]["currency"]
    return ""


def _schedule_scenario(compute: dict[str, Any], workload: dict[str, Any], metadata: dict[str, Any], days: int) -> dict[str, Any]:
    schedule = _nested(metadata, workload, "scheduled_shutdown")
    approved_idle = _approved_idle_hours(workload, metadata)
    approved = _amount(schedule.get("approved_off_hours_per_month")) or approved_idle
    missing: list[str] = []
    if approved is None:
        missing.append("approved off-hours")
    if not schedule:
        missing.append("scheduled_shutdown evidence")
    if compute["status"] != "available" or approved is None or not schedule:
        # An estimate without a schedule would present off-hours as planned
        # when nobody has defined them.
        return {
            "name": "scheduled_shutdown",
            "status": "requires_evidence",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": missing,
        }

    minimum_run = max(0.0, _amount(schedule.get("minimum_run_minutes")) or DEFAULT_MINIMUM_RUN_MINUTES)
    warmup = max(0.0, _amount(schedule.get("restart_warmup_minutes")) or DEFAULT_RESTART_WARMUP_MINUTES)
    shutdowns = schedule.get("shutdowns_per_month", DEFAULT_SHUTDOWNS_PER_MONTH)
    if isinstance(shutdowns, bool) or not isinstance(shutdowns, int) or shutdowns < 1:
        return {
            "name": "scheduled_shutdown",
            "status": "requires_evidence",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": ["shutdowns_per_month"],
        }

    block_hours = approved / shutdowns
    if block_hours < (minimum_run + warmup) / 60:
        return {
            "name": "scheduled_shutdown",
            "status": "no_savings",
            "monthly_savings": {"currency": _currency_or_empty(compute), "amount": 0.0},
            "missing_evidence": [],
            "note": "Off-hours blocks are shorter than the conservative minimum run and warm-up assumptions.",
        }
    currency = compute["monthly_cost"]["currency"]
    monthly_compute = compute["monthly_cost"]["amount"]
    gross = monthly_compute * approved / MONTH_HOURS
    overhead = monthly_compute / MONTH_HOURS * shutdowns * (minimum_run + warmup) / 60
    savings = max(0.0, gross - overhead)
    return {
        "name": "scheduled_shutdown",
        "status": "estimate",
        "monthly_savings": {"currency": currency, "amount": round(savings, 6)},
        "approved_off_hours_per_month": round(approved, 2),
        "minimum_run_minutes": minimum_run,
        "restart_warmup_minutes": warmup,
        "shutdowns_per_month": shutdowns,
        "note": "Gross off-hours savings are reduced by restart and warm-up running cost.",
    }


def _decommission_scenario(
    compute: dict[str, Any], workload: dict[str, Any], distribution: Any, node_scenario: dict[str, Any] | None
) -> dict[str, Any]:
    if compute["status"] != "available":
        return {
            "name": "permanent_decommission",
            "status": "unavailable",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": ["compute attribution"],
        }
    if distribution == "Standard":
        # Attributed Standard compute is a share of nodes that stay allocated
        # unless the node-consolidation evidence says a node can be removed.
        if node_scenario is None or node_scenario.get("status") != "estimate":
            return {
                "name": "permanent_decommission",
                "status": "requires_evidence",
                "monthly_savings": {"currency": "", "amount": 0.0},
                "missing_evidence": ["node_pool_consolidation evidence for removable nodes"],
                "note": "Standard GKE compute is not savings unless a node can be removed.",
            }
        savings = copy.deepcopy(node_scenario["monthly_savings"])
    elif distribution == "Autopilot":
        savings = copy.deepcopy(compute["monthly_cost"])
    else:
        return {
            "name": "permanent_decommission",
            "status": "requires_evidence",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": ["kubernetes.distribution (Autopilot or Standard)"],
        }
    return {
        "name": "permanent_decommission",
        "status": "requires_approval" if not _cost_review_eligible(workload) else "estimate",
        "monthly_savings": savings,
        "note": "Only marginal compute is savings. Persistent storage, backups, snapshots, load balancers, and static IPs remain.",
    }


def _confidence(attribution: dict[str, Any], missing_metrics: list[str], scenarios: list[dict[str, Any]], days: int) -> str:
    if attribution["status"] != "available":
        return "unavailable"
    if days < 7 or attribution.get("status") == "partial" or missing_metrics:
        return "low"
    if any(
        scenario.get("status") in {"requires_evidence", "requires_approval", "no_savings", "not_recommended"}
        for scenario in scenarios
    ):
        return "medium"
    if days >= 14:
        return "high"
    return "medium"


def _merge_missing_evidence(scenarios: list[dict[str, Any]]) -> list[str]:
    evidence: list[str] = []
    for scenario in scenarios:
        for item in scenario.get("missing_evidence", []):
            if item not in evidence:
                evidence.append(item)
    return evidence


def _model_workload(
    envelope: dict[str, Any], workload: dict[str, Any], rows: list[dict[str, Any]], days: int
) -> dict[str, Any]:
    """Model one workload in isolation so currency and attribution stay separate."""
    metadata = envelope["metadata"]
    project_id = metadata["project_id"]
    attribution = _attribution_from_rows(rows, workload, project_id, days, metadata)
    workload_rows = _match_workload_rows(rows, workload, project_id, metadata)
    if _spans_multiple_clusters(workload_rows):
        workload_rows = []
    compute = _compute_cost(workload_rows, days)
    protected = _protected_costs(workload_rows, days)

    distribution = metadata.get("kubernetes", {}).get("distribution") if isinstance(metadata.get("kubernetes"), dict) else None
    scenarios: list[dict[str, Any]] = []
    primary: dict[str, Any] | None = None
    if workload["classification"] != "candidate":
        # Savings are modeled only for workloads DBA judged safe to review.
        scenarios.append({
            "name": "not_a_candidate",
            "status": "not_recommended",
            "monthly_savings": {"currency": "", "amount": 0.0},
            "missing_evidence": [],
            "note": f"DBA classified this workload as {workload['classification']}; no savings are modeled.",
        })
    else:
        if distribution == "Autopilot":
            primary = _autopilot_scenario(compute, workload, metadata, days)
        elif distribution == "Standard":
            primary = _standard_scenario(compute, workload, metadata, rows, days)
        else:
            primary = {
                "name": "workload_attribution",
                "status": "requires_evidence",
                "monthly_savings": {"currency": "", "amount": 0.0},
                "missing_evidence": ["kubernetes.distribution (Autopilot or Standard)"],
            }
        scenarios.append(primary)
        scenarios.append(_schedule_scenario(compute, workload, metadata, days))
        scenarios.append(_decommission_scenario(compute, workload, distribution, primary if distribution == "Standard" else None))

    missing_evidence = _merge_missing_evidence(scenarios)
    if attribution["status"] == "unavailable":
        missing_evidence.insert(0, "GKE billing attribution")

    return {
        "source_workload": workload,
        "workload": {
            key: workload.get(key)
            for key in ("namespace", "pod", "db_type", "node_name", "controller_name", "controller_type", "owner", "environment")
        },
        "billing_attribution": {
            "status": attribution["status"],
            "system_labels": list(_ATTRIBUTION_KEYS),
            "detail": attribution,
        },
        "observed_cost": attribution.get("observed_cost"),
        "attributed_monthly_cost": attribution.get("attributed_monthly_cost"),
        "marginal_monthly_savings": scenarios[0].get("monthly_savings", {"currency": "", "amount": 0.0}),
        "compute_cost": compute,
        "protected_costs": protected,
        "remediation_scenarios": scenarios,
        "confidence": _confidence(attribution, metadata["missing_metrics"], scenarios, days),
        "missing_evidence": missing_evidence,
    }


def build_idle_cost_result(
    envelope: dict[str, Any], rows: list[dict[str, Any]], days: int
) -> dict[str, Any]:
    """Build a model result from validated envelope data and billing rows."""
    metadata = envelope["metadata"]
    project_id = metadata["project_id"]
    relevant_rows = [row for row in rows if row.get("project") == project_id]
    workload_results = [_model_workload(envelope, workload, relevant_rows, days) for workload in envelope["data"]["workloads"]]
    first = workload_results[0]

    return {
        "schema_version": OUTPUT_SCHEMA,
        "generated_at": envelope["generated_at"],
        "status": "ok",
        "project_id": project_id,
        "source_metadata": metadata,
        # Keep the first workload's legacy fields for existing callers. New
        # callers should consume workloads[], which models every DBA item.
        **first,
        "unattributed_costs": _unattributed_costs(relevant_rows),
        "workloads": workload_results,
        "limits": [
            "No Kubernetes discovery or mutation calls.",
            "Only workload-attributed GKE system-label rows are used for savings.",
            "Currencies are never mixed.",
        ],
    }


def _fetch_billing_rows(days: int, end_date: str | None, client: bigquery.Client | None) -> list[dict[str, Any]]:
    if client is None:
        from .billing import get_billing_client

        client = get_billing_client()
    return get_cost_breakdown(
        list(_GROUP_KEYS),
        days=days,
        end_date=end_date,
        client=client,
        limit=None,
    )


def format_idle_cost_markdown(result: dict[str, Any]) -> str:
    """Render a successful or failed result as compact operator-facing Markdown."""
    if result["status"] != "ok":
        return f"## Database Cost Impact Error\n\n{result['error']['message']}"

    lines = [
        "## Database Cost Impact",
        "",
        f"Project `{result['project_id']}`",
        "",
        "### Modeled Workloads",
        "",
    ]
    for index, item in enumerate(result["workloads"], 1):
        identity = item["workload"]
        attribution = item["billing_attribution"]
        lines.extend(
            [
                f"### {index}. {identity['namespace']}/{identity['pod']} ({identity['db_type']})",
                f"Attribution: **{attribution['status']}** | Confidence: **{item['confidence']}**",
            ]
        )
        monthly = item.get("attributed_monthly_cost")
        lines.append(f"Attributed monthly cost: {monthly['currency']} {monthly['amount']:,.2f}" if monthly else "Attributed monthly cost: unavailable")
        lines.extend(["", "Marginal savings scenarios:"])
        for scenario in item["remediation_scenarios"]:
            savings = scenario.get("monthly_savings") or {"currency": "", "amount": 0.0}
            lines.append(f"- **{scenario['name']}**: {scenario['status']} - {savings['currency']} {savings['amount']:,.2f}/month")
            if scenario.get("note"):
                lines.append(f"  - {scenario['note']}")
        if item["missing_evidence"]:
            lines.append("Missing evidence: " + "; ".join(item["missing_evidence"]))
        lines.append("")

    lines.extend(["", result["limits"][0], result["limits"][1]])
    return "\n".join(lines)


def estimate_database_cost_impact(
    idle_assessment: str,
    days: int = 30,
    end_date: str | None = None,
    project_id: str | None = None,
    output_mode: str = "markdown",
) -> str:
    """Estimate database cost impact from a DBA idle-cost envelope.

    The tool reads the configured BigQuery billing export only. It does not
    discover pods, inspect the cluster, or make mutation calls.
    """
    try:
        if not isinstance(days, int) or isinstance(days, bool) or not 1 <= days <= 90:
            raise ValueError("days must be an integer from 1 to 90")
        if output_mode not in {"markdown", "json"}:
            raise ValueError("output_mode must be 'markdown' or 'json'")
        envelope = parse_idle_assessment(idle_assessment)
        metadata = envelope["metadata"]
        selected_project = project_id or metadata["project_id"]
        if selected_project != metadata["project_id"]:
            raise ValueError("project_id must match metadata.project_id")
        normalize_window(days, end_date)
        try:
            rows = _fetch_billing_rows(days, end_date, None)
        except EmptyBillingDataset:
            rows = []
        except BillingExportNotConfigured as exc:
            result = {
                "schema_version": OUTPUT_SCHEMA,
                "generated_at": envelope["generated_at"],
                "status": "billing_unavailable",
                "error": {"code": "billing_export_not_configured", "message": str(exc)},
                "billing_attribution": {"status": "unavailable"},
            }
            return json.dumps(result, indent=2, sort_keys=True) if output_mode == "json" else format_idle_cost_markdown(result)
        except BillingExportError as exc:
            result = {
                "schema_version": OUTPUT_SCHEMA,
                "generated_at": envelope["generated_at"],
                "status": "billing_unavailable",
                "error": {"code": "billing_export_unavailable", "message": str(exc)},
                "billing_attribution": {"status": "unavailable"},
            }
            return json.dumps(result, indent=2, sort_keys=True) if output_mode == "json" else format_idle_cost_markdown(result)

        result = build_idle_cost_result(envelope, rows, days)
        return json.dumps(result, indent=2, sort_keys=True) if output_mode == "json" else format_idle_cost_markdown(result)
    except IdleAssessmentError as exc:
        message = f"Invalid DBA envelope: {exc}"
        error = {"schema_version": OUTPUT_SCHEMA, "status": "error", "error": {"code": "invalid_envelope", "message": message}}
        return json.dumps(error, indent=2, sort_keys=True) if output_mode == "json" else format_idle_cost_markdown(error)
    except (ValueError, TypeError) as exc:
        message = str(exc)
        error = {"schema_version": OUTPUT_SCHEMA, "status": "error", "error": {"code": "invalid_request", "message": message}}
        return json.dumps(error, indent=2, sort_keys=True) if output_mode == "json" else format_idle_cost_markdown(error)

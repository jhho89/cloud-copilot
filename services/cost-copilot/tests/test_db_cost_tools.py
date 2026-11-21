import datetime as dt
import inspect
import json
import os
import sys
from copy import deepcopy

import pytest

SRC = os.path.join(os.path.dirname(__file__), "..", "src", "agents")
sys.path.insert(0, SRC)

from cost_analysis_agent import billing, db_cost_tools
from cost_analysis_agent.db_cost_tools import (
    build_idle_cost_result,
    estimate_database_cost_impact,
    parse_idle_assessment,
)


@pytest.fixture
def export_env(monkeypatch):
    monkeypatch.setenv("BILLING_EXPORT_PROJECT", "proj")
    monkeypatch.setenv("BILLING_EXPORT_DATASET", "billing")
    monkeypatch.setenv("BILLING_EXPORT_TABLE", "gcp_billing_export_v1_0X9ABC-DEF123")


ENVELOPE = {
    "schema_version": "cloud-copilot.dba.idle-cost.v1",
    "generated_at": "2026-11-21T00:00:00Z",
    "status": "ok",
    "data": {
        "workloads": [
            {
                "namespace": "database",
                "pod": "mysql-0",
                "db_type": "mysql",
                "node_name": "gke-pool-1-abc",
                "controller_name": "mysql",
                "controller_type": "StatefulSet",
                "labels": {"app.kubernetes.io/name": "mysql"},
                "metrics": {
                    "p95_work_per_second": 0,
                    "mean_active_threads": 0.1,
                    "p95_cpu_fraction": 0.01,
                    "zero_work_hour_ratio": 0.97,
                },
                "classification": "candidate",
                "risks": ["persistent_data", "backup_schedule_unknown"],
                "owner": "platform-data",
                "environment": "staging",
                "eligibility_evidence": "cloud-copilot.ai/cost-review=eligible",
            }
        ]
    },
    "metadata": {
        "project_id": "example-project",
        "window_days": 14,
        "metrics_source": "cloud_monitoring",
        "missing_metrics": [],
        "kubernetes": {"distribution": "Standard"},
        "node_pool_consolidation": {
            "node_pool": "pool-1",
            "other_non_daemonset_workloads": 0,
            "remaining_pods_fit_existing_nodes": True,
            "node_monthly_cost": 50,
        },
        "scheduled_shutdown": {
            "approved_off_hours_per_month": 240,
            "shutdowns_per_month": 30,
            "minimum_run_minutes": 10,
            "restart_warmup_minutes": 5,
        },
    },
}


def _row(service="Compute Engine", sku="N1 Core", cost=20.0, **overrides):
    row = {
        "cluster": "prod-cluster",
        "cluster_location": "us-central1",
        "namespace": "database",
        "workload": "mysql",
        "workload_type": "StatefulSet",
        "node_pool": "pool-1",
        "project": "example-project",
        "service": service,
        "sku": sku,
        "currency": "USD",
        "total_cost": cost,
    }
    row.update(overrides)
    return row


def test_parse_idle_assessment_contract():
    parsed = parse_idle_assessment(json.dumps(ENVELOPE))
    assert parsed["data"]["workloads"][0]["pod"] == "mysql-0"
    assert parsed["metadata"]["kubernetes"]["distribution"] == "Standard"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda e: e.update(schema_version="cloud-copilot.dba.idle-cost.v0"),
        lambda e: e.update(status="degraded"),
        lambda e: e.update(generated_at="not-a-time"),
        lambda e: e["data"].update(workloads=[]),
        lambda e: e["data"]["workloads"][0]["metrics"].pop("mean_active_threads"),
        lambda e: e["data"]["workloads"][0]["metrics"].update(zero_work_hour_ratio=1.1),
        lambda e: e["metadata"].update(window_days=91),
        lambda e: e["metadata"].update(missing_metrics=[1]),
    ],
)
def test_parse_idle_assessment_rejects_malformed_or_old_envelopes(mutation):
    envelope = json.loads(json.dumps(ENVELOPE))
    mutation(envelope)
    with pytest.raises(db_cost_tools.IdleAssessmentError):
        parse_idle_assessment(envelope)


def test_billing_query_includes_gke_cost_allocation_labels(export_env):
    query, params = billing.build_cost_breakdown_query(
        ["cluster", "cluster_location", "namespace", "workload", "workload_type", "node_pool"],
        14,
        "2026-11-21",
    )
    for label in (
        "goog-k8s-cluster-name",
        "goog-k8s-cluster-location",
        "goog-k8s-namespace",
        "goog-k8s-workload-name",
        "goog-k8s-workload-type",
        "goog-k8s-node-pool-name",
    ):
        assert label in query
    assert "UNNEST(labels) AS label" in query
    assert "SUM(cost) AS total_cost" in query
    assert "currency" in query
    assert params == {"start_date": "2026-11-08", "end_date": "2026-11-21"}


def test_build_idle_result_separates_costs_and_savings():
    envelope = parse_idle_assessment(ENVELOPE)
    rows = [
        _row(sku="N1 Core", cost=20),
        _row(service="Compute Engine", sku="Persistent SSD", cost=5),
        _row(service="Cloud SQL", sku="Backup", cost=3),
        _row(service="Compute Engine", sku="N1 Core", cost=99, cluster="unlabeled", workload="unlabeled"),
    ]
    result = build_idle_cost_result(envelope, rows, 30)

    assert result["billing_attribution"]["status"] == "available"
    assert result["observed_cost"] == {"currency": "USD", "amount": 28.0}
    assert result["attributed_monthly_cost"] == {"currency": "USD", "amount": 28.0}
    assert result["compute_cost"]["monthly_cost"] == {"currency": "USD", "amount": 20.0}
    assert result["protected_costs"]["window_observed"]["persistent_storage"] == {"USD": 5.0}
    assert result["protected_costs"]["window_observed"]["backups_snapshots"] == {"USD": 3.0}
    assert result["unattributed_costs"][0]["observed_cost"] == 99.0
    assert result["missing_evidence"] == []


def test_build_idle_result_models_every_workload_separately():
    envelope = parse_idle_assessment(ENVELOPE)
    redis = deepcopy(envelope["data"]["workloads"][0])
    redis.update(pod="redis-0", db_type="redis", controller_name="redis")
    envelope["data"]["workloads"].append(redis)
    rows = [
        _row(cost=20),
        _row(service="Compute Engine", sku="Persistent SSD", cost=5),
        _row(workload="redis", node_pool="pool-2", cost=8),
        _row(workload="redis", node_pool="pool-2", service="Cloud SQL", sku="Backup", cost=2),
    ]

    result = build_idle_cost_result(envelope, rows, 30)

    assert [item["workload"]["pod"] for item in result["workloads"]] == ["mysql-0", "redis-0"]
    mysql, redis_result = result["workloads"]
    assert mysql["observed_cost"] == {"currency": "USD", "amount": 25.0}
    assert mysql["compute_cost"]["monthly_cost"] == {"currency": "USD", "amount": 20.0}
    assert mysql["protected_costs"]["window_observed"]["persistent_storage"] == {"USD": 5.0}
    assert redis_result["observed_cost"] == {"currency": "USD", "amount": 10.0}
    assert redis_result["compute_cost"]["monthly_cost"] == {"currency": "USD", "amount": 8.0}
    assert redis_result["protected_costs"]["window_observed"]["backups_snapshots"] == {"USD": 2.0}
    assert result["workload"]["pod"] == "mysql-0"
    assert result["marginal_monthly_savings"] == mysql["marginal_monthly_savings"]


def test_multi_workload_results_never_mix_currencies():
    envelope = parse_idle_assessment(ENVELOPE)
    redis = deepcopy(envelope["data"]["workloads"][0])
    redis.update(pod="redis-0", db_type="redis", controller_name="redis")
    envelope["data"]["workloads"].append(redis)
    rows = [
        _row(cost=20),
        _row(workload="redis", cost=8, currency="USD"),
        _row(workload="redis", cost=2, currency="EUR"),
    ]

    result = build_idle_cost_result(envelope, rows, 30)

    assert result["billing_attribution"]["status"] == "available"
    assert result["marginal_monthly_savings"]["currency"] == "USD"
    redis_result = result["workloads"][1]
    assert redis_result["billing_attribution"]["status"] == "unavailable"
    assert "currencies were not mixed" in redis_result["billing_attribution"]["detail"]["reason"]
    assert redis_result["marginal_monthly_savings"]["amount"] == 0.0


def test_autopilot_uses_approved_idle_hours_and_keeps_storage():
    envelope = parse_idle_assessment(ENVELOPE)
    envelope["metadata"]["kubernetes"]["distribution"] = "Autopilot"
    result = build_idle_cost_result(envelope, [_row(cost=20), _row(sku="Persistent SSD", cost=5)], 30)
    autopilot = result["remediation_scenarios"][0]
    assert autopilot["name"] == "autopilot_idle_scale_down"
    assert autopilot["monthly_savings"]["amount"] == 19.4
    assert result["protected_costs"]["monthly_projection"]["persistent_storage"] == {"USD": 5.0}


def test_standard_requires_last_removable_workload_and_fit_evidence():
    envelope = parse_idle_assessment(ENVELOPE)
    removable = build_idle_cost_result(envelope, [_row(cost=20)], 30)
    assert removable["remediation_scenarios"][0]["monthly_savings"]["amount"] == 50.0

    envelope["metadata"]["node_pool_consolidation"]["other_non_daemonset_workloads"] = 1
    blocked = build_idle_cost_result(envelope, [_row(cost=20)], 30)
    assert blocked["remediation_scenarios"][0]["status"] == "no_savings"
    assert blocked["remediation_scenarios"][0]["monthly_savings"]["amount"] == 0.0


def test_scheduled_shutdown_subtracts_conservative_overhead():
    envelope = parse_idle_assessment(ENVELOPE)
    result = build_idle_cost_result(envelope, [_row(cost=20)], 30)
    scenario = next(item for item in result["remediation_scenarios"] if item["name"] == "scheduled_shutdown")
    gross = 20 * 240 / 730
    overhead = 20 / 730 * 30 * (10 + 5) / 60
    assert scenario["monthly_savings"]["amount"] == round(gross - overhead, 6)


def test_scheduled_shutdown_blocks_short_run_length():
    envelope = parse_idle_assessment(ENVELOPE)
    envelope["metadata"]["scheduled_shutdown"]["approved_off_hours_per_month"] = 6
    envelope["metadata"]["scheduled_shutdown"]["shutdowns_per_month"] = 30
    result = build_idle_cost_result(envelope, [_row(cost=20)], 30)
    scenario = next(item for item in result["remediation_scenarios"] if item["name"] == "scheduled_shutdown")
    assert scenario["status"] == "no_savings"
    assert scenario["monthly_savings"]["amount"] == 0.0


def test_permanent_decommission_saves_only_compute():
    envelope = parse_idle_assessment(ENVELOPE)
    rows = [_row(cost=20), _row(sku="Persistent SSD", cost=5), _row(sku="Backup", cost=2)]
    result = build_idle_cost_result(envelope, rows, 30)
    scenario = next(item for item in result["remediation_scenarios"] if item["name"] == "permanent_decommission")
    assert scenario["monthly_savings"] == {"currency": "USD", "amount": 20.0}
    assert result["protected_costs"]["window_observed"]["backups_snapshots"] == {"USD": 2.0}


def test_missing_billing_attribution_is_unavailable():
    envelope = parse_idle_assessment(ENVELOPE)
    result = build_idle_cost_result(envelope, [_row(cluster="unlabeled", namespace="unlabeled", workload="unlabeled", workload_type="unlabeled", node_pool="unlabeled")], 30)
    assert result["billing_attribution"]["status"] == "unavailable"
    assert result["observed_cost"] is None
    assert result["attributed_monthly_cost"] is None
    assert result["confidence"] == "unavailable"
    assert "GKE billing attribution" in result["missing_evidence"]


def test_public_tool_normalizes_json_and_dates(monkeypatch):
    captured = {}

    def fake_fetch(days, end_date, client):
        captured["days"] = days
        captured["end_date"] = end_date
        return [_row(cost=20)]

    monkeypatch.setattr(db_cost_tools, "_fetch_billing_rows", fake_fetch)
    raw = json.dumps(ENVELOPE)
    result = json.loads(estimate_database_cost_impact(raw, days=14, end_date="2026-11-21", output_mode="json"))
    assert captured == {"days": 14, "end_date": "2026-11-21"}
    assert result["status"] == "ok"
    assert result["attributed_monthly_cost"]["amount"] == round(20 * 30 / 14, 6)


def test_public_tool_rejects_bad_days_output_and_project(monkeypatch):
    monkeypatch.setattr(db_cost_tools, "_fetch_billing_rows", lambda *args: [])
    raw = json.dumps(ENVELOPE)
    for kwargs in ({"days": 0}, {"days": 91}, {"days": True}):
        result = estimate_database_cost_impact(raw, **kwargs)
        assert result.startswith("## Database Cost Impact Error")
    result = json.loads(estimate_database_cost_impact(raw, project_id="other-project", output_mode="json"))
    assert result["status"] == "error"
    assert estimate_database_cost_impact(raw, output_mode="yaml").startswith("## Database Cost Impact Error")


def test_public_tool_reports_missing_attribution_without_fabrication(monkeypatch):
    monkeypatch.setattr(db_cost_tools, "_fetch_billing_rows", lambda *args: [_row(cluster="unlabeled", namespace="unlabeled", workload="unlabeled", workload_type="unlabeled", node_pool="unlabeled")])
    raw = json.dumps(ENVELOPE)
    result = json.loads(estimate_database_cost_impact(raw, output_mode="json"))
    assert result["billing_attribution"]["status"] == "unavailable"
    assert result["marginal_monthly_savings"]["amount"] == 0.0
    assert result["confidence"] == "unavailable"


def test_tool_has_no_credential_arguments_and_uses_adc(monkeypatch):
    signature = inspect.signature(estimate_database_cost_impact)
    names = set(signature.parameters)
    assert not names.intersection({"credential", "credentials", "token", "api_key"})

    class FakeClient:
        def query(self, query, job_config=None):
            class Job:
                def result(self):
                    return []
            return Job()

    created = {}
    def fake_client(project=None):
        created["project"] = project
        return FakeClient()
    monkeypatch.setattr(db_cost_tools.bigquery, "Client", fake_client)
    monkeypatch.setattr(billing, "get_billing_export_target", lambda: ("proj", "dataset", "table"))
    monkeypatch.setattr(db_cost_tools, "get_cost_breakdown", lambda *args, **kwargs: [])
    client = db_cost_tools._fetch_billing_rows(30, None, None)
    assert created == {"project": "proj"}


def test_billing_client_uses_application_default_credentials(monkeypatch):
    monkeypatch.setenv("BILLING_EXPORT_PROJECT", "billing-project")
    monkeypatch.setenv("BILLING_EXPORT_DATASET", "billing")
    monkeypatch.setenv("BILLING_EXPORT_TABLE", "export")
    created = {}
    def fake_client(project=None, credentials=None):
        created["arguments"] = (project, credentials)
        return object()
    monkeypatch.setattr(billing.bigquery, "Client", fake_client)
    billing.get_billing_client()
    assert created == {"arguments": ("billing-project", None)}

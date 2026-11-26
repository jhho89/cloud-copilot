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
    assert scenario["monthly_savings"] == {"currency": "USD", "amount": 50.0}
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


# --- Regression tests for review blockers -----------------------------------


def _envelope(**workload_overrides):
    envelope = deepcopy(ENVELOPE)
    envelope["data"]["workloads"][0].update(workload_overrides)
    return envelope


def _scenario(result, name):
    return next(item for item in result["remediation_scenarios"] if item["name"] == name)


def test_null_metrics_and_bare_pods_are_accepted():
    envelope = _envelope(controller_name="", controller_type="", node_name="")
    envelope["data"]["workloads"][0]["metrics"].update(
        p95_work_per_second=None, mean_active_threads=None, p95_cpu_fraction=None, zero_work_hour_ratio=None
    )
    parse_idle_assessment(envelope)
    result = build_idle_cost_result(envelope, [_row()], 30)
    assert result["workloads"][0]["billing_attribution"]["status"] == "unavailable"


def test_missing_metric_key_is_still_rejected():
    envelope = _envelope()
    del envelope["data"]["workloads"][0]["metrics"]["p95_cpu_fraction"]
    with pytest.raises(db_cost_tools.IdleAssessmentError):
        parse_idle_assessment(envelope)


def test_explicit_eligibility_flag_is_honoured_over_evidence_string():
    envelope = _envelope(eligibility_evidence="annotation=eligible", cost_review_eligible=True)
    envelope["metadata"]["kubernetes"] = {"distribution": "Autopilot"}
    result = build_idle_cost_result(envelope, [_row(service="Kubernetes Engine", sku="Autopilot Pod mCPU", cost=100.0)], 30)
    assert _scenario(result, "autopilot_idle_scale_down")["status"] == "conditional"
    assert _scenario(result, "permanent_decommission")["status"] == "estimate"
    envelope["data"]["workloads"][0]["cost_review_eligible"] = False
    result = build_idle_cost_result(envelope, [_row(service="Kubernetes Engine", sku="Autopilot Pod mCPU", cost=100.0)], 30)
    assert _scenario(result, "autopilot_idle_scale_down")["status"] == "requires_evidence"


def test_standard_node_pool_is_read_from_the_dba_kubernetes_block():
    envelope = _envelope(kubernetes={"node_pool": "pool-1"})
    del envelope["metadata"]["node_pool_consolidation"]["node_monthly_cost"]
    envelope["metadata"]["node_pool_consolidation"]["node_count"] = 2
    result = build_idle_cost_result(envelope, [_row(cost=200.0)], 30)
    scenario = _scenario(result, "node_consolidation")
    assert scenario["status"] == "estimate"
    assert scenario["monthly_savings"]["amount"] == 100.0


def test_standard_node_cost_without_pool_rows_requires_evidence_not_zero():
    envelope = _envelope(kubernetes={"node_pool": "unknown-pool"})
    del envelope["metadata"]["node_pool_consolidation"]["node_monthly_cost"]
    envelope["metadata"]["node_pool_consolidation"]["node_count"] = 2
    result = build_idle_cost_result(envelope, [_row(cost=200.0)], 30)
    assert _scenario(result, "node_consolidation")["status"] == "requires_evidence"


@pytest.mark.parametrize("classification", ["active", "blocked"])
def test_non_candidates_get_no_savings_scenarios(classification):
    result = build_idle_cost_result(_envelope(classification=classification), [_row()], 30)
    scenarios = result["workloads"][0]["remediation_scenarios"]
    assert [item["status"] for item in scenarios] == ["not_recommended"]
    assert scenarios[0]["monthly_savings"]["amount"] == 0.0
    assert result["workloads"][0]["confidence"] != "high"


def test_standard_decommission_is_only_savings_when_a_node_can_be_removed():
    envelope = _envelope()
    result = build_idle_cost_result(envelope, [_row(cost=20.0)], 30)
    assert _scenario(result, "permanent_decommission")["status"] in {"estimate", "requires_approval"}
    assert _scenario(result, "permanent_decommission")["monthly_savings"]["amount"] == 50.0
    envelope["metadata"]["node_pool_consolidation"]["other_non_daemonset_workloads"] = 3
    result = build_idle_cost_result(envelope, [_row(cost=20.0)], 30)
    decommission = _scenario(result, "permanent_decommission")
    assert decommission["status"] == "requires_evidence"
    assert decommission["monthly_savings"]["amount"] == 0.0


def test_decommission_requires_known_distribution():
    envelope = _envelope()
    del envelope["metadata"]["kubernetes"]
    result = build_idle_cost_result(envelope, [_row()], 30)
    assert _scenario(result, "permanent_decommission")["status"] == "requires_evidence"


def test_scheduled_shutdown_needs_schedule_evidence():
    envelope = _envelope()
    del envelope["metadata"]["scheduled_shutdown"]
    result = build_idle_cost_result(envelope, [_row()], 30)
    scenario = _scenario(result, "scheduled_shutdown")
    assert scenario["status"] == "requires_evidence"
    assert "scheduled_shutdown evidence" in scenario["missing_evidence"]


def test_same_workload_in_two_clusters_is_not_summed():
    rows = [_row(cluster="prod-cluster", cost=20.0), _row(cluster="staging-cluster", cost=30.0)]
    result = build_idle_cost_result(_envelope(), rows, 30)
    workload = result["workloads"][0]
    assert workload["billing_attribution"]["status"] == "unavailable"
    assert "more than one GKE cluster" in workload["billing_attribution"]["detail"]["reason"]
    assert workload["compute_cost"]["status"] == "unavailable"


def test_cluster_pin_selects_one_cluster():
    envelope = _envelope()
    envelope["metadata"]["kubernetes"].update(cluster="staging-cluster", cluster_location="us-central1")
    rows = [_row(cluster="prod-cluster", cost=20.0), _row(cluster="staging-cluster", cost=30.0)]
    workload = build_idle_cost_result(envelope, rows, 30)["workloads"][0]
    assert workload["billing_attribution"]["status"] == "available"
    assert workload["observed_cost"]["amount"] == 30.0


def _clickhouse_cluster_envelope():
    envelope = deepcopy(ENVELOPE)
    member = deepcopy(envelope["data"]["workloads"][0])
    member.update(pod="clickhouse-0", db_type="clickhouse", controller_name="clickhouse", controller_type="StatefulSet")
    replica = deepcopy(member)
    replica["pod"] = "clickhouse-1"
    envelope["data"]["workloads"] = [member, replica]
    envelope["metadata"]["assessment_unit"] = "cluster"
    envelope["metadata"]["cluster_members"] = [
        {"namespace": "database", "pod": "clickhouse-0", "controller_name": "clickhouse", "controller_type": "StatefulSet"},
        {"namespace": "database", "pod": "clickhouse-1", "controller_name": "clickhouse", "controller_type": "StatefulSet"},
    ]
    envelope["metadata"]["protected_members"] = [
        {"namespace": "database", "pod": "clickhouse-keeper-0", "controller_name": "clickhouse-keeper", "controller_type": "StatefulSet"},
    ]
    envelope["metadata"]["cluster_assessment"] = {"classification": "candidate", "classification_reasons": ["all replicas idle"]}
    envelope["metadata"]["dedicated_node_pool_consolidation"] = {
        "dedicated_node_pool": True,
        "taints_compatible": True,
        "other_non_daemonset_workloads": 0,
        "remaining_pods_fit_existing_nodes": True,
        "node_monthly_cost": 50.0,
    }
    return envelope


def test_clickhouse_cluster_attribution_and_savings_are_whole_cluster():
    envelope = parse_idle_assessment(_clickhouse_cluster_envelope())
    rows = [
        _row(workload="clickhouse", cost=20.0),
        _row(workload="clickhouse-keeper", service="Kubernetes Engine", cost=5.0),
        _row(workload="clickhouse", service="Compute Engine", sku="Persistent SSD", cost=3.0),
    ]
    result = build_idle_cost_result(envelope, rows, 30)

    cluster = result["cluster"]
    assert cluster["assessment_unit"] == "cluster"
    assert cluster["billing_attribution"]["detail"]["status"] == "available"
    assert cluster["observed_cost"]["amount"] == 28.0
    assert cluster["compute_cost"]["monthly_cost"]["amount"] == 20.0
    assert cluster["protected_costs"]["keeper_compute"]["window_observed"] == {"USD": 5.0}
    assert [item["pod"] for item in cluster["protected_members"]] == ["clickhouse-keeper-0"]
    assert all(item["remediation_scenarios"][0]["monthly_savings"]["amount"] == 0.0 for item in result["workloads"])
    single = _scenario(cluster, "single_replica_removal")
    assert single["status"] == "no_savings"
    assert single["monthly_savings"]["amount"] == 0.0
    assert _scenario(cluster, "dedicated_node_pool_consolidation")["monthly_savings"]["amount"] == 50.0
    assert _scenario(cluster, "whole_cluster_decommission")["monthly_savings"]["amount"] == 50.0


def test_clickhouse_active_member_blocks_cluster_savings():
    envelope = _clickhouse_cluster_envelope()
    envelope["data"]["workloads"][0]["classification"] = "active"
    envelope["metadata"]["cluster_assessment"] = {"classification": "active", "classification_reasons": ["one replica active"]}
    result = build_idle_cost_result(parse_idle_assessment(envelope), [_row(workload="clickhouse", cost=20.0)], 30)
    cluster = result["cluster"]
    assert _scenario(cluster, "not_a_cluster_candidate")["status"] == "not_recommended"
    assert cluster["marginal_monthly_savings"]["amount"] == 0.0
    assert _scenario(cluster, "single_replica_removal")["monthly_savings"]["amount"] == 0.0

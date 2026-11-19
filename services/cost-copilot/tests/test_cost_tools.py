import os
import sys
from unittest import mock

SRC = os.path.join(os.path.dirname(__file__), "..", "src", "agents")
sys.path.insert(0, SRC)

from cost_analysis_agent import tools
from cost_analysis_agent.billing import (
    BillingExportNotConfigured,
    EmptyBillingDataset,
)


def test_get_cost_breakdown_formats_rows():
    import os
    os.environ.setdefault("BILLING_EXPORT_PROJECT", "proj")
    os.environ.setdefault("BILLING_EXPORT_DATASET", "billing")
    os.environ.setdefault("BILLING_EXPORT_TABLE", "gcp_billing_export_v1_0X9ABC-DEF123")
    rows = [
        {"service": "Compute Engine", "date": "2024-03-10", "currency": "USD", "total_cost": 12.5},
        {"service": "Cloud Storage", "date": "2024-03-10", "currency": "USD", "total_cost": 3.25},
    ]
    with mock.patch("cost_analysis_agent.tools.query_cost_breakdown", return_value=rows):
        result = tools.get_cost_breakdown("proj", group_by=["service", "date"], days=7)
    assert "Cost Breakdown for proj (7 days)" in result
    assert "Compute Engine | 2024-03-10 | USD | 12.50" in result
    assert "Cloud Storage | 2024-03-10 | USD | 3.25" in result


def test_get_cost_breakdown_not_configured_message():
    with mock.patch("cost_analysis_agent.tools.query_cost_breakdown", side_effect=BillingExportNotConfigured("missing env")):
        result = tools.get_cost_breakdown("proj")
    assert "Billing Export Not Configured" in result
    assert "missing env" in result
    assert "BIGQUERY-BILLING-EXPORT.md" in result


def test_get_cost_breakdown_empty_dataset_message():
    with mock.patch("cost_analysis_agent.tools.query_cost_breakdown", side_effect=EmptyBillingDataset("no rows")):
        result = tools.get_cost_breakdown("proj")
    assert "No Cost Data for proj" in result
    assert "no rows" in result


def test_detect_billing_anomalies_reports_spikes():
    anomaly = {"day": "2024-03-10", "currency": "USD", "cost": 250.0, "mean_prior": 100.0, "change_pct": 150.0, "direction": "spike"}
    with mock.patch("cost_analysis_agent.tools.detect_cost_anomalies", return_value=[anomaly]):
        result = tools.detect_billing_anomalies("proj", days=14, threshold_pct=30)
    assert "Cost Anomalies for proj" in result
    assert "spike 150.0%" in result


def test_detect_billing_anomalies_none_detected():
    with mock.patch("cost_analysis_agent.tools.detect_cost_anomalies", return_value=[]):
        result = tools.detect_billing_anomalies("proj")
    assert "No Cost Anomalies Detected for proj" in result

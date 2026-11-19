import datetime as dt
import os
import sys

import pytest

SRC = os.path.join(os.path.dirname(__file__), "..", "src", "agents")
sys.path.insert(0, SRC)

from cost_analysis_agent import billing
from cost_analysis_agent.billing import (
    BillingExportNotConfigured,
    EmptyBillingDataset,
    build_anomaly_query,
    build_cost_breakdown_query,
    detect_anomalies,
    get_cost_breakdown,
    normalize_window,
)


@pytest.fixture
def export_env(monkeypatch):
    monkeypatch.setenv("BILLING_EXPORT_PROJECT", "proj")
    monkeypatch.setenv("BILLING_EXPORT_DATASET", "billing")
    monkeypatch.setenv("BILLING_EXPORT_TABLE", "gcp_billing_export_v1_0X9ABC-DEF123")


def test_billing_export_table_id(export_env):
    assert billing.billing_export_table_id() == "proj.billing.gcp_billing_export_v1_0X9ABC-DEF123"


def test_billing_export_not_configured(monkeypatch):
    for key in ("BILLING_EXPORT_PROJECT", "BILLING_EXPORT_DATASET", "BILLING_EXPORT_TABLE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
    with pytest.raises(BillingExportNotConfigured):
        billing.billing_export_table_id()


def test_normalize_window_dates():
    end = dt.date(2024, 3, 15)
    start, end_out = normalize_window(7, end)
    assert start == dt.date(2024, 3, 9)
    assert end_out == end


def test_normalize_window_string_end_date():
    start, end = normalize_window(1, "2024-03-15")
    assert start == end == dt.date(2024, 3, 15)


def test_normalize_window_invalid():
    with pytest.raises(ValueError):
        normalize_window(0)
    with pytest.raises(ValueError):
        normalize_window(30, "not-a-date")


def test_build_cost_breakdown_query(export_env):
    query, params = build_cost_breakdown_query(["service", "date"], 7, dt.date(2024, 3, 15))
    assert "service.description" in query
    assert "DATE(usage_start_time)" in query
    assert "proj.billing.gcp_billing_export_v1_0X9ABC-DEF123" in query
    assert params == {"start_date": "2024-03-09", "end_date": "2024-03-15"}


def test_build_cost_breakdown_query_invalid_field(export_env):
    with pytest.raises(ValueError):
        build_cost_breakdown_query(["bogus"])


def test_build_anomaly_query(export_env):
    query, params = build_anomaly_query(14)
    assert "SUM(cost) AS total_cost" in query
    assert "DATE(usage_start_time)" in query
    assert set(params) == {"start_date", "end_date"}


class _FakeJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class _FakeClient:
    def __init__(self, rows):
        self._rows = rows

    def query(self, query, job_config=None):
        self.last_query = query
        self.last_job_config = job_config
        return _FakeJob(self._rows)


def test_get_cost_breakdown_groups_and_shapes(export_env):
    client = _FakeClient([
        {"service": "Compute Engine", "date": dt.date(2024, 3, 10), "currency": "USD", "total_cost": 12.5},
        {"service": "Cloud Storage", "date": dt.date(2024, 3, 11), "currency": "USD", "total_cost": 3.25},
    ])
    rows = get_cost_breakdown(["service", "date"], days=7, end_date=dt.date(2024, 3, 15), client=client)
    assert rows[0]["service"] == "Compute Engine"
    assert rows[1]["total_cost"] == 3.25
    params = client.last_job_config.query_parameters
    assert {p.name: p.value for p in params} == {"start_date": dt.date(2024, 3, 9), "end_date": dt.date(2024, 3, 15)}


def test_get_cost_breakdown_empty_dataset(export_env):
    with pytest.raises(EmptyBillingDataset):
        get_cost_breakdown(["service"], days=7, client=_FakeClient([]))


def test_get_cost_breakdown_permission_error(export_env):
    class DeniedClient:
        def query(self, query, job_config=None):
            raise billing.Forbidden("denied")

    from cost_analysis_agent.billing import BillingPermissionError
    with pytest.raises(BillingPermissionError):
        get_cost_breakdown(["service"], days=7, client=DeniedClient())


def test_get_cost_breakdown_not_found(export_env):
    class NotFoundClient:
        def query(self, query, job_config=None):
            raise billing.NotFound("missing")

    from cost_analysis_agent.billing import BillingExportNotFound
    with pytest.raises(BillingExportNotFound):
        get_cost_breakdown(["service"], days=7, client=NotFoundClient())


def test_detect_anomalies_spikes_and_drops():
    base = dt.date(2024, 3, 1)
    rows = [
        {"day": base + dt.timedelta(days=i), "currency": "USD", "total_cost": 100.0}
        for i in range(5)
    ]
    rows.append({"day": base + dt.timedelta(days=5), "currency": "USD", "total_cost": 250.0})
    rows.append({"day": base + dt.timedelta(days=6), "currency": "USD", "total_cost": 10.0})
    anomalies = detect_anomalies(rows, threshold_pct=30)
    assert len(anomalies) == 2
    assert anomalies[0]["direction"] == "spike"
    assert anomalies[1]["direction"] == "drop"


def test_detect_anomalies_no_history_is_not_anomaly():
    rows = [{"day": dt.date(2024, 3, 1), "currency": "USD", "total_cost": 1000.0}]
    assert detect_anomalies(rows, threshold_pct=10) == []


def test_detect_anomalies_invalid_threshold():
    with pytest.raises(ValueError):
        detect_anomalies([], threshold_pct=0)

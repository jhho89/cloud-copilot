from __future__ import annotations

import inspect
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
K8S_AGENTS = REPO_ROOT / "services" / "k8s-copilot" / "src" / "agents"
if str(K8S_AGENTS) not in sys.path:
    sys.path.insert(0, str(K8S_AGENTS))

from dba_agent import clickhouse  # noqa: E402
from dba_agent.idle_metrics import MetricSample  # noqa: E402
from dba_agent import idle_metrics, tools  # noqa: E402


def _clickhouse_pod(
    name: str = "chi-clickhouse-0-0",
    namespace: str = "analytics",
    *,
    scrape: bool = True,
    altinity: bool = True,
) -> SimpleNamespace:
    labels = {"app.kubernetes.io/name": "clickhouse"}
    if altinity:
        labels["clickhouse.altinity.com/cluster"] = "prod"
    annotations = {"prometheus.io/scrape": "true"} if scrape else {}
    container = SimpleNamespace(
        name="clickhouse",
        image="altinity/clickhouse:23.8",
        ports=[SimpleNamespace(container_port=9363, name="prometheus")] if scrape else [],
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, namespace=namespace, labels=labels, annotations=annotations),
        spec=SimpleNamespace(containers=[container]),
    )


def _metric_point(timestamp: datetime, value: float) -> SimpleNamespace:
    return SimpleNamespace(interval=SimpleNamespace(end_time=timestamp), value=SimpleNamespace(double_value=value))


def test_clickhouse_metrics_endpoint_evidence_recovers_native_and_altinity_sources() -> None:
    pod = _clickhouse_pod()

    evidence = clickhouse._clickhouse_metrics_endpoint_evidence(pod)

    assert [item["source"] for item in evidence] == [
        "clickhouse_native_prometheus",
        "altinity_operator_metrics",
    ]
    assert evidence[0]["port"] == "9363"
    assert evidence[0]["scrape_enabled"] == "true"


def test_clickhouse_metric_aliases_resolve_native_and_altinity_gmp_suffixes(monkeypatch) -> None:
    descriptor = SimpleNamespace(type="prometheus.googleapis.com/ClickHouseProfileEvents_Query/counter")
    altinity = SimpleNamespace(type="prometheus.googleapis.com/chi_clickhouse_event_query/counter")
    gauge = SimpleNamespace(type="prometheus.googleapis.com/ClickHouseAsyncMetrics_MemoryTracking/gauge")
    series = SimpleNamespace(
        metric=SimpleNamespace(labels={"namespace": "analytics", "pod": "chi-clickhouse-0-0"}),
        resource=SimpleNamespace(labels={}),
        points=[
            _metric_point(datetime(2026, 1, 1, tzinfo=timezone.utc), 100),
            _metric_point(datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), 130),
        ],
    )

    class MonitoringClient:
        def __init__(self, descriptors):
            self.descriptors = descriptors

        def list_metric_descriptors(self, request):
            assert request["filter"].startswith('metric.type = starts_with("prometheus.googleapis.com/")')
            return self.descriptors

        def list_time_series(self, request):
            assert "/counter" in request["filter"] or "/gauge" in request["filter"]
            assert 'one_of("analytics")' in request["filter"]
            return [series]

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(idle_metrics, "load_cloud_monitoring_client", lambda: MonitoringClient([descriptor]))
    native = clickhouse._read_clickhouse_monitoring_series(
        "prod", start, start + timedelta(minutes=5), "analytics", "chi-clickhouse-0-0"
    )
    monkeypatch.setattr(idle_metrics, "load_cloud_monitoring_client", lambda: MonitoringClient([altinity]))
    altinity_metrics = clickhouse._read_clickhouse_monitoring_series(
        "prod", start, start + timedelta(minutes=5), "analytics", "chi-clickhouse-0-0"
    )
    monkeypatch.setattr(idle_metrics, "load_cloud_monitoring_client", lambda: MonitoringClient([gauge]))
    gauges = clickhouse._read_clickhouse_monitoring_series(
        "prod", start, start + timedelta(minutes=5), "analytics", "chi-clickhouse-0-0"
    )

    assert "queries_total" in native
    assert native["queries_total"]["metric_type"] == descriptor.type
    assert "queries_total" in altinity_metrics
    assert altinity_metrics["queries_total"]["metric_type"] == altinity.type
    assert "memory_tracking_bytes" in gauges
    assert gauges["memory_tracking_bytes"]["metric_type"] == gauge.type
    assert "ClickHouseProfileEvents_Query" in clickhouse.CLICKHOUSE_METRIC_ALIASES["queries_total"]
    assert "chi_clickhouse_event_query" in clickhouse.CLICKHOUSE_METRIC_ALIASES["queries_total"]


def test_clickhouse_counter_reset_is_restart_evidence_without_negative_rate() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    samples = [
        MetricSample(start, 100.0),
        MetricSample(start + timedelta(seconds=50), 0.0),
        MetricSample(start + timedelta(seconds=100), 20.0),
    ]

    change = clickhouse._clickhouse_metric_change(
        {"metric_type": "counter", "kind": "counter", "samples": samples}, 100.0
    )

    assert change["change"] == 20.0
    assert change["rate_per_second"] == 0.2
    assert change["restarts"] == 1
    assert "restart" in change["note"]


def test_clickhouse_trends_render_human_and_json(monkeypatch) -> None:
    pod = _clickhouse_pod()
    api = SimpleNamespace(
        list_namespaced_pod=lambda **_kwargs: SimpleNamespace(items=[pod]),
        read_namespaced_pod=lambda **kwargs: pod,
    )
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    metrics = {
        "queries_total": {
            "metric_type": "prometheus.googleapis.com/ClickHouseProfileEvents_Query/counter",
            "kind": "counter",
            "labels": {},
            "samples": [
                MetricSample(start, 100.0),
                MetricSample(start + timedelta(hours=1), 500.0),
            ],
        },
        "memory_tracking_bytes": {
            "metric_type": "prometheus.googleapis.com/ClickHouseAsyncMetrics_MemoryTracking/gauge",
            "kind": "gauge",
            "labels": {},
            "samples": [
                MetricSample(start, 1_000.0),
                MetricSample(start + timedelta(hours=1), 2_000.0),
            ],
        },
    }
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(clickhouse, "_resolve_clickhouse_metrics_project_id", lambda _requested: "prod")
    monkeypatch.setattr(clickhouse, "_read_clickhouse_monitoring_series", lambda *args, **kwargs: metrics)

    human = clickhouse.get_clickhouse_metrics_trends("analytics", "chi-clickhouse-0-0", hours=1)
    payload = json.loads(
        clickhouse.get_clickhouse_metrics_trends(
            "analytics", "chi-clickhouse-0-0", hours=1, output_mode="json"
        )
    )

    assert "queries_per_second: 0.111111" in human
    assert "Counter decreases observed" not in human
    assert "No changes were made" in human
    assert payload["schema_version"] == clickhouse.CLICKHOUSE_STRUCTURED_SCHEMA_VERSION
    assert payload["tool"] == "get_clickhouse_metrics_trends"
    assert payload["status"] == "ok"
    assert payload["metadata"]["endpoint_discovered"] is True
    assert payload["metadata"]["trends"]["queries_per_second"] == 400 / 3600
    assert payload["metadata"]["trends"]["memory_tracking_bytes"]["change"] == 1000


def test_clickhouse_metrics_missing_series_reports_prerequisite_without_pod_exec(monkeypatch) -> None:
    pod = _clickhouse_pod()
    api = SimpleNamespace(
        list_namespaced_pod=lambda **_kwargs: SimpleNamespace(items=[pod]),
        read_namespaced_pod=lambda **kwargs: pod,
    )
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(clickhouse, "_resolve_clickhouse_metrics_project_id", lambda _requested: "prod")
    monkeypatch.setattr(clickhouse, "_read_clickhouse_monitoring_series", lambda *args, **kwargs: {})

    def fail_exec(*args, **kwargs):
        raise AssertionError("pod/exec must not be used for metrics trends")

    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fail_exec)

    human = clickhouse.get_clickhouse_metrics_trends("analytics", "chi-clickhouse-0-0")
    payload = json.loads(clickhouse.get_clickhouse_metrics_trends("analytics", "chi-clickhouse-0-0", output_mode="json"))

    assert "pod/exec is not a trend fallback" in human
    assert payload["status"] == "error"
    assert "pod/exec is not a trend fallback" in payload["errors"][0]


def _snapshot_rows(position: str) -> list[dict[str, object]]:
    if position == "first":
        return [
            {"event": "Query", "value": 100},
            {"event": "InsertedRows", "value": 50},
            {"event": "MergedRows", "value": 10},
            {"metric": "Query", "value": 10},
            {"metric": "Connection", "value": 5},
            {"metric": "MemoryTracking", "value": 1_000},
            {"metric": "TotalPartsOfMergeTree", "value": 20},
            {"metric": "AbsoluteDelay", "value": 3},
            {"metric": "ReplicaQueueSize", "value": 8},
        ]
    return [
        {"event": "Query", "value": 120},
        {"event": "InsertedRows", "value": 60},
        {"event": "MergedRows", "value": 15},
        {"metric": "Query", "value": 12},
        {"metric": "Connection", "value": 7},
        {"metric": "MemoryTracking", "value": 1_200},
        {"metric": "TotalPartsOfMergeTree", "value": 18},
        {"metric": "AbsoluteDelay", "value": 7},
        {"metric": "ReplicaQueueSize", "value": 9},
    ]


def test_clickhouse_snapshot_diff_reads_selected_values_and_uses_bounded_wait(monkeypatch) -> None:
    pod = _clickhouse_pod(name="clickhouse-0")
    api = SimpleNamespace(
        list_namespaced_pod=lambda **_kwargs: SimpleNamespace(items=[pod]),
        read_namespaced_pod=lambda **kwargs: pod,
    )
    calls: list[str] = []
    sleeps: list[int] = []

    def fake_run(_namespace, _pod, template_id, params=None, container=None):
        calls.append(template_id)
        if template_id == "clickhouse_readonly_profile.v1":
            return [{"readonly": "1", "version": "23.8.1.1"}], ""
        position = "first" if len(calls) <= 3 else "second"
        if template_id == "clickhouse_snapshot_events.v1":
            return [row for row in _snapshot_rows(position) if "event" in row], ""
        return [row for row in _snapshot_rows(position) if "metric" in row], ""

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(clickhouse, "_run_clickhouse_query", fake_run)
    monkeypatch.setattr(clickhouse.time, "sleep", lambda seconds: sleeps.append(seconds))

    human = clickhouse.get_clickhouse_snapshot_diff("analytics", "clickhouse-0", interval_minutes=1)
    payload = json.loads(
        clickhouse.get_clickhouse_snapshot_diff(
            "analytics", "clickhouse-0", interval_minutes=1, output_mode="json"
        )
    )

    assert sleeps == [60, 60]
    assert calls.count("clickhouse_readonly_profile.v1") == 0
    assert "queries: 100 -> 120 (change=20, rate=0.333333/s)" in human
    assert "parts_count: 20 -> 18 (change=-2, rate=-0.0333333/s)" in human
    assert "contains no query text, credentials, secrets, or raw application data" in human
    assert payload["schema_version"] == clickhouse.CLICKHOUSE_STRUCTURED_SCHEMA_VERSION
    assert payload["tool"] == "get_clickhouse_snapshot_diff"
    assert payload["status"] == "ok"
    assert payload["metadata"]["interval_minutes"] == 1
    assert payload["metadata"]["snapshots"][0]["retained_sources"] == [
        "asynchronous_metrics",
        "events",
        "metrics",
    ]
    assert not {"password", "credential", "raw_output"} & set(payload["metadata"])


def test_clickhouse_tools_expose_structured_output_contract() -> None:
    tools = [
        "get_clickhouse_slow_queries",
        "get_clickhouse_query_plan",
        "recommend_clickhouse_table_design",
        "get_clickhouse_replication_report",
        "get_clickhouse_workload_report",
        "get_clickhouse_parts_report",
        "get_clickhouse_resource_report",
        "get_clickhouse_metrics_trends",
        "get_clickhouse_snapshot_diff",
        "get_clickhouse_remediation_recommendations",
    ]
    for name in tools:
        signature = inspect.signature(getattr(clickhouse, name))
        assert "output_mode" in signature.parameters
        assert signature.parameters["output_mode"].default == "human"


def test_documentation_and_agent_contracts_match_clickhouse_tools() -> None:
    doc = (REPO_ROOT / "services/k8s-copilot/docs/clickhouse-dba.md").read_text()
    readme = (REPO_ROOT / "services/k8s-copilot/README.md").read_text()
    rbac = (REPO_ROOT / "docs/dba-rbac.md").read_text()
    agent_source = (REPO_ROOT / "services/k8s-copilot/src/agents/dba_agent/agent.py").read_text()
    orchestrator = (REPO_ROOT / "services/k8s-copilot/src/agents/kubernetes_copilot/agent.py").read_text()

    for text in (doc, readme, rbac, agent_source, orchestrator):
        assert "get_clickhouse_metrics_trends" in text
        assert "get_clickhouse_snapshot_diff" in text
    for topic in (
        "cloud-copilot.dba.clickhouse.v1",
        "ClickHouseProfileEvents_",
        "chi_clickhouse_event_",
        "DBA_CLICKHOUSE_METRICS_PROJECT_ID",
        "system.asynchronous_metrics",
        "1-60 minutes",
    ):
        assert topic in doc

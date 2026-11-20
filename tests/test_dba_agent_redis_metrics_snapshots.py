from __future__ import annotations

import inspect
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
K8S_AGENTS = REPO_ROOT / "services" / "k8s-copilot" / "src" / "agents"
if str(K8S_AGENTS) not in sys.path:
    sys.path.insert(0, str(K8S_AGENTS))

from dba_agent import tools  # noqa: E402


def _redis_pod(
    name: str = "cache-0",
    namespace: str = "cache",
    *,
    exporter: bool = True,
) -> SimpleNamespace:
    containers = [SimpleNamespace(name="redis", image="valkey/valkey:8.0")]
    if exporter:
        containers.append(SimpleNamespace(name="metrics", image="oliver006/redis_exporter:v1.70.0"))
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace=namespace,
            labels={"app.kubernetes.io/name": "valkey"},
            annotations={
                "prometheus.io/scrape": "true",
                "prometheus.io/port": "9121",
                "prometheus.io/path": "/metrics",
            },
        ),
        spec=SimpleNamespace(containers=containers),
        status=SimpleNamespace(phase="Running"),
    )


def _redis_info(section: str, fields: dict[str, object]) -> str:
    return json.dumps({section.title(): fields})


def _metric_point(timestamp: datetime, value: float) -> SimpleNamespace:
    return SimpleNamespace(
        interval=SimpleNamespace(end_time=timestamp),
        value=SimpleNamespace(double_value=value),
    )


def test_redis_metrics_discovery_finds_exporter_sidecar_and_annotations() -> None:
    pod = _redis_pod()
    plain_pod = _redis_pod(name="cache-1", exporter=False)
    api = SimpleNamespace(list_namespaced_pod=lambda namespace: SimpleNamespace(items=[pod, plain_pod]))

    discovery = tools._redis_metrics_discovery(api, "cache", None)

    assert len(discovery["pods"]) == 2
    assert discovery["candidates"] == [
        {
            "namespace": "cache",
            "pod": "cache-0",
            "source": "redis-exporter-sidecar",
            "sidecars": [
                {
                    "container": "metrics",
                    "image": "oliver006/redis_exporter:v1.70.0",
                    "port": "9121",
                    "path": "/metrics",
                    "scrape": "true",
                }
            ],
        }
    ]
    assert tools._redis_metrics_sidecars(plain_pod) == []


def test_monitoring_series_normalizes_labels_and_sorts_samples(monkeypatch) -> None:
    descriptor = SimpleNamespace(type="prometheus.googleapis.com/redis_memory_used_bytes/gauge")
    other = SimpleNamespace(type="prometheus.googleapis.com/redis_unrelated/gauge")
    series = SimpleNamespace(
        metric=SimpleNamespace(labels={"namespace": "cache", "pod": "cache-0"}),
        resource=SimpleNamespace(labels={"cluster": "prod"}),
        points=[
            _metric_point(datetime(2026, 1, 1, tzinfo=timezone.utc), 20),
            _metric_point(datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), 30),
        ],
    )
    wrong_target = SimpleNamespace(
        metric=SimpleNamespace(labels={"namespace": "other", "pod": "other-0"}),
        resource=SimpleNamespace(labels={}),
        points=[_metric_point(datetime(2026, 1, 1, tzinfo=timezone.utc), 999)],
    )

    class MonitoringClient:
        def list_metric_descriptors(self, request):
            assert request["filter"].startswith("metric.type = starts_with")
            return [descriptor, other]

        def list_time_series(self, request):
            assert request["filter"] == 'metric.type = "prometheus.googleapis.com/redis_memory_used_bytes/gauge"'
            return [wrong_target, series]

    monkeypatch.setattr(tools, "_load_cloud_monitoring_client", MonitoringClient)
    metrics = tools._read_redis_monitoring_series(
        "prod",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 0, 15, tzinfo=timezone.utc),
        "cache",
        "cache-0",
    )

    assert set(metrics) == {"memory_used_bytes"}
    samples = metrics["memory_used_bytes"]["samples"]
    assert [sample.value for sample in samples] == [20, 30]
    assert metrics["memory_used_bytes"]["labels"]["cluster"] == "prod"


def test_redis_trends_calculate_rates_and_hit_ratio() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(seconds=300)
    samples = {
        "evicted_total": {
            "metric_type": "counter",
            "kind": "counter",
            "labels": {},
            "samples": [
                tools.RedisMetricSample(start, 10),
                tools.RedisMetricSample(end, 20),
            ],
        },
        "hits_total": {
            "metric_type": "counter",
            "kind": "counter",
            "labels": {},
            "samples": [
                tools.RedisMetricSample(start, 100),
                tools.RedisMetricSample(end, 120),
            ],
        },
        "misses_total": {
            "metric_type": "counter",
            "kind": "counter",
            "labels": {},
            "samples": [
                tools.RedisMetricSample(start, 50),
                tools.RedisMetricSample(end, 80),
            ],
        },
        "instantaneous_ops": {
            "metric_type": "gauge",
            "kind": "gauge",
            "labels": {},
            "samples": [
                tools.RedisMetricSample(start, 400),
                tools.RedisMetricSample(end, 500),
            ],
        },
    }

    trends = tools._redis_trends_from_metrics(samples, start, end)

    assert trends["eviction_rate_per_second"] == pytest.approx(10 / 300)
    assert trends["hit_rate_from_window_rates"] == pytest.approx(20 / 50)
    assert trends["ops_per_second_from_gauge"] == 500
    assert trends["memory_used_bytes"]["available"] is False


def test_get_redis_metrics_trends_human_and_json(monkeypatch) -> None:
    pod = _redis_pod()
    api = SimpleNamespace(
        list_namespaced_pod=lambda namespace: SimpleNamespace(items=[pod]),
        read_namespaced_pod=lambda name, namespace: pod,
    )
    metrics = {
        "connected_clients": {
            "metric_type": "prometheus.googleapis.com/redis_connected_clients/gauge",
            "kind": "gauge",
            "labels": {},
            "samples": [
                tools.RedisMetricSample(datetime(2026, 1, 1, tzinfo=timezone.utc), 20),
                tools.RedisMetricSample(datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), 30),
            ],
        }
    }
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(tools, "_redis_project_id", lambda requested: "prod")
    monkeypatch.setattr(
        tools,
        "_read_redis_monitoring_series",
        lambda *args: metrics,
    )

    human = tools.get_redis_metrics_trends("cache", "cache-0", minutes=5)
    payload = json.loads(tools.get_redis_metrics_trends("cache", "cache-0", minutes=5, output_mode="json"))

    assert "connected_clients: latest=30, change=10" in human
    assert "No changes were made" in human
    assert payload["schema_version"] == "cloud-copilot.dba.redis.v1"
    assert payload["tool"] == "get_redis_metrics_trends"
    assert payload["status"] == "ok"
    assert payload["metadata"]["sidecar_discovered"] is True
    assert "connected_clients" in payload["metadata"]["metrics_reported"]


def test_get_redis_metrics_trends_reports_metrics_prerequisite(monkeypatch) -> None:
    pod = _redis_pod(exporter=False)
    api = SimpleNamespace(
        list_namespaced_pod=lambda namespace: SimpleNamespace(items=[pod]),
        read_namespaced_pod=lambda name, namespace: pod,
    )
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(tools, "_redis_project_id", lambda requested: "prod")
    monkeypatch.setattr(
        tools,
        "_read_redis_monitoring_series",
        lambda *args: (_ for _ in ()).throw(tools.RedisMetricsUnavailableError("no series")),
    )

    human = tools.get_redis_metrics_trends("cache", "cache-0")
    payload = json.loads(tools.get_redis_metrics_trends("cache", "cache-0", output_mode="json"))

    assert human.startswith("Metrics unavailable:")
    assert payload["status"] == "error"
    assert "no series" in payload["errors"][0]


def test_redis_snapshot_delta_and_read_only_diff(monkeypatch) -> None:
    pod = _redis_pod(exporter=False)
    api = SimpleNamespace(read_namespaced_pod=lambda name, namespace: pod)
    sleep_seconds: list[int] = []
    calls: list[list[str]] = []
    fixtures = {
        ("INFO", "memory"): _redis_info(
            "Memory",
            {
                "used_memory": 1_000_000,
                "maxmemory": 2_000_000,
                "mem_fragmentation_ratio": 1.5,
            },
        ),
        ("INFO", "stats"): _redis_info(
            "Stats",
            {
                "evicted_keys": 10,
                "keyspace_hits": 80,
                "keyspace_misses": 20,
                "instantaneous_ops_per_sec": 100,
            },
        ),
        ("INFO", "replication"): _redis_info("Replication", {"role": "master"}),
        ("INFO", "persistence"): _redis_info("Persistence", {"latest_fork_usec": 1000}),
        ("INFO", "clients"): _redis_info("Clients", {"connected_clients": 20, "maxclients": 100}),
        ("MEMORY", "STATS"): _redis_info("Memory", {"fragmentation": 1.5}),
    }

    def fake_run(namespace, pod_name, cli, command, container=None, port=None):
        tools._validate_redis_command(command)
        calls.append(list(command))
        position = (len(calls) - 1) % 12
        fixture = dict(fixtures)
        if 6 <= position <= 11:
            fixture[("INFO", "memory")] = _redis_info(
                "Memory",
                {
                    "used_memory": 1_100_000,
                    "maxmemory": 2_000_000,
                    "mem_fragmentation_ratio": 1.4,
                },
            )
            fixture[("INFO", "stats")] = _redis_info(
                "Stats",
                {
                    "evicted_keys": 30,
                    "keyspace_hits": 90,
                    "keyspace_misses": 30,
                    "instantaneous_ops_per_sec": 140,
                },
            )
            fixture[("MEMORY", "STATS")] = _redis_info("Memory", {"fragmentation": 1.4})
            fixture[("INFO", "clients")] = _redis_info(
                "Clients",
                {"connected_clients": 25, "maxclients": 100},
            )
        return fixture[tuple(command)]

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: api)
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "valkey-cli")
    monkeypatch.setattr(tools, "_run_redis_command", fake_run)
    monkeypatch.setattr(tools.time, "sleep", lambda seconds: sleep_seconds.append(seconds))
    original_snapshot = tools._read_redis_snapshot
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    clock = iter([base_time, base_time + timedelta(seconds=60), base_time, base_time + timedelta(seconds=60)])

    def timed_snapshot(*args, **kwargs):
        snapshot, _, errors = original_snapshot(*args, **kwargs)
        return snapshot, next(clock), errors

    monkeypatch.setattr(tools, "_read_redis_snapshot", timed_snapshot)

    human = tools.get_redis_snapshot_diff("cache", "cache-0", interval_minutes=1)
    payload = json.loads(
        tools.get_redis_snapshot_diff("cache", "cache-0", interval_minutes=1, output_mode="json")
    )

    assert sleep_seconds == [60, 60]
    assert all(tools._validate_redis_command(command) == command for command in calls)
    assert "evictions: 20.0 (0.333333/s)" in human
    assert "fragmentation: -0.1" in human
    assert "memory: 100000.0 bytes" in human
    assert "hit ratio from hit/miss deltas: 0.5000" in human
    assert "connected clients: 5.0" in human
    assert "No changes were made" in human
    assert payload["metadata"]["interval_minutes"] == 1
    snapshot = payload["metadata"]["snapshots"][0]
    assert snapshot["deltas"]["eviction_rate_per_second"] == pytest.approx(20 / 60)
    assert snapshot["runtime_role"] == "master"
    assert "secret" not in json.dumps(payload)


def test_redis_tools_expose_structured_output_contract() -> None:
    redis_tools = [
        "analyze_redis_bigkeys",
        "get_redis_memory_report",
        "get_redis_snapshot_diff",
        "get_redis_client_report",
        "get_redis_latency_report",
        "get_redis_slowlog",
        "get_redis_remediation_recommendations",
        "get_redis_metrics_trends",
    ]
    for name in redis_tools:
        signature = inspect.signature(getattr(tools, name))
        assert "output_mode" in signature.parameters
        assert signature.parameters["output_mode"].default == "human"

    human_error = tools.get_redis_memory_report("cache", "cache-0", output_mode="yaml")
    json_error = json.loads(tools.get_redis_memory_report("cache", "cache-0", output_mode="json"))
    assert human_error == "Invalid output_mode: use 'human' or 'json'."
    assert json_error["schema_version"] == "cloud-copilot.dba.redis.v1"
    assert json_error["tool"] == "get_redis_memory_report"
    assert json_error["status"] == "error"


def test_documentation_and_agent_contracts_match_new_tools() -> None:
    redis_doc = (REPO_ROOT / "services/k8s-copilot/docs/redis-valkey-dba.md").read_text()
    readme = (REPO_ROOT / "services/k8s-copilot/README.md").read_text()
    rbac = (REPO_ROOT / "docs/dba-rbac.md").read_text()
    agent_source = (REPO_ROOT / "services/k8s-copilot/src/agents/dba_agent/agent.py").read_text()

    for text in (redis_doc, readme, rbac, agent_source):
        assert "get_redis_metrics_trends" in text
        assert "get_redis_snapshot_diff" in text
    for topic in (
        "app.kubernetes.io/component",
        "REDISCLI_AUTH",
        "VALKEYCLI_AUTH",
        "TLS",
        "valkey-cli",
        "redis-cli",
        "cloud-copilot.dba.redis.v1",
    ):
        assert topic in redis_doc

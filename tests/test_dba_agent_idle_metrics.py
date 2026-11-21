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

from dba_agent import idle_metrics  # noqa: E402


END_TIME = datetime(2026, 1, 3, 12, 0, tzinfo=timezone.utc)


def _timestamp(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, minute, tzinfo=timezone.utc)


def _point(timestamp: datetime, value: float) -> SimpleNamespace:
    return SimpleNamespace(
        interval=SimpleNamespace(end_time=timestamp),
        value=SimpleNamespace(double_value=value),
    )


def _labels(namespace: str, pod: str, container: str = "database") -> SimpleNamespace:
    return SimpleNamespace(labels={"namespace": namespace, "pod": pod, "container": container})


def _series(namespace: str, pod: str, container: str = "database", points: list[SimpleNamespace] | None = None):
    return SimpleNamespace(metric=_labels(namespace, pod, container), resource=SimpleNamespace(labels={"cluster": "test-cluster"}), points=points or [])


def _database_profile_pod(
    db_type: str,
    exporter: bool,
    *,
    eligible: bool = True,
    persistent: bool = False,
    owner: str = "platform-data",
    environment: str = "staging",
    role: str = "",
) -> SimpleNamespace:
    images = {"mysql": "mysql:8.0", "mariadb": "mariadb:11", "redis": "redis:7", "valkey": "valkey/valkey:8.0"}
    labels = {"app.kubernetes.io/name": db_type, "team": owner}
    if environment:
        labels["environment"] = environment
    if role:
        labels["app.kubernetes.io/component"] = role
    containers = [
        SimpleNamespace(
            name="database",
            image=images[db_type],
            resources=SimpleNamespace(requests={"cpu": "500m", "memory": "1Gi"}, limits={"cpu": "1", "memory": "2Gi"}),
        )
    ]
    if exporter:
        exporter_image = {"mysql": "prom/mysqld-exporter:v0.15", "mariadb": "prom/mysqld-exporter:v0.15", "redis": "oliver006/redis_exporter:v1.70", "valkey": "valkey-exporter:v1.7"}[db_type]
        containers.append(SimpleNamespace(name="metrics", image=exporter_image))
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"{db_type}-0",
            namespace="database",
            labels=labels,
            annotations={
                "cloud-copilot.ai/cost-review": "eligible" if eligible else "not-eligible",
                "prometheus.io/scrape": "true" if exporter else "false",
                "prometheus.io/port": "9104",
                "prometheus.io/path": "/metrics",
            },
            owner_references=[SimpleNamespace(kind="StatefulSet", name=db_type, controller=True)],
        ),
        spec=SimpleNamespace(
            node_name="gke-pool-1-abc",
            containers=containers,
            volumes=[SimpleNamespace(name="data", persistent_volume_claim=SimpleNamespace(claim_name=f"{db_type}-data"))] if persistent else [],
        ),
        status=SimpleNamespace(phase="Running"),
    )


class FakeCoreApi:
    def __init__(self, pods: list[SimpleNamespace], namespace: str = "database", namespace_labels: dict[str, str] | None = None) -> None:
        self.pods = pods
        self.namespace = namespace
        self.namespace_labels = {"environment": "staging"} if namespace_labels is None else namespace_labels

    def list_namespaced_pod(self, namespace: str):
        assert namespace == self.namespace
        return SimpleNamespace(items=self.pods)

    def list_pod_for_all_namespaces(self):
        return SimpleNamespace(items=self.pods)

    def read_namespace(self, namespace: str):
        return SimpleNamespace(metadata=SimpleNamespace(labels=self.namespace_labels))

    def list_persistent_volume_claim_for_all_namespaces(self):
        claims = []
        for pod in self.pods:
            for volume in getattr(pod.spec, "volumes", None) or []:
                claim_name = volume.persistent_volume_claim.claim_name
                claims.append(
                    SimpleNamespace(
                        metadata=SimpleNamespace(name=claim_name, namespace=self.namespace),
                        status=SimpleNamespace(phase="Bound"),
                        spec=SimpleNamespace(resources={"requests": {"storage": "10Gi"}}),
                    )
                )
        return SimpleNamespace(items=claims)

    def list_node(self):
        return SimpleNamespace(
            items=[
                SimpleNamespace(
                    metadata=SimpleNamespace(
                        name="gke-pool-1-abc",
                        labels={
                            "cloud.google.com/gke-nodepool": "db-pool",
                            "node.kubernetes.io/machine-type": "e2-standard-4",
                        },
                    )
                )
            ]
        )


class FakeBatchApi:
    def __init__(self, cronjobs: list[SimpleNamespace] | None = None) -> None:
        self.cronjobs = cronjobs or []

    def list_cron_job_for_all_namespaces(self):
        return SimpleNamespace(items=self.cronjobs)


class FakeMonitoringApi:
    def __init__(self, series: dict[str, list[SimpleNamespace]]) -> None:
        self.series = series
        self.descriptor_calls = 0
        self.series_calls = 0

    def list_metric_descriptors(self, request: dict):
        self.descriptor_calls += 1
        if request["filter"].startswith('metric.type = starts_with("prometheus.googleapis.com/') is False:
            return []
        return [
            SimpleNamespace(type=metric_type)
            for metric_type in self.series
            if metric_type.startswith("prometheus.googleapis.com/")
        ]

    def list_time_series(self, request: dict):
        self.series_calls += 1
        metric_type = request["filter"].split('"')[1]
        return self.series.get(metric_type, [])


def _profile_series(db_type: str) -> dict[str, list[SimpleNamespace]]:
    if db_type in {"mysql", "mariadb"}:
        aliases = {
            "mysql_global_status_queries": "counter",
            "mysql_global_status_threads_running": "gauge",
            "mysql_global_status_threads_connected": "gauge",
        }
    else:
        aliases = {
            "redis_commands_processed_total": "counter",
            "redis_connected_clients": "gauge",
        }
    series: dict[str, list[SimpleNamespace]] = {}
    for metric_name, kind in aliases.items():
        metric_type = f"prometheus.googleapis.com/{metric_name}/{kind}"
        if kind == "counter":
            points = []
            for day in (1, 2):
                for hour in range(4):
                    points.extend(
                        [
                            _point(_timestamp(day, hour), 0.0),
                            _point(_timestamp(day, hour, 30), 0.0),
                        ]
                    )
        else:
            points = [_point(_timestamp(1, hour), 0.01 if metric_name.endswith("running") else 2) for hour in range(4)]
            points.extend(_point(_timestamp(2, hour), 0.01 if metric_name.endswith("running") else 2) for hour in range(4))
        series[metric_type] = [_series("database", f"{db_type}-0", points=points)]
    return series


def _gke_series() -> dict[str, list[SimpleNamespace]]:
    types = {
        "kubernetes.io/container/cpu/request_utilization": 0.01,
        "kubernetes.io/container/cpu/limit_utilization": 0.005,
        "kubernetes.io/container/memory/request_utilization": 0.2,
        "kubernetes.io/container/memory/limit_utilization": 0.1,
        "kubernetes.io/container/network/sent_bytes_count": "counter",
        "kubernetes.io/container/network/received_bytes_count": "counter",
    }
    series = {}
    for metric_type, value in types.items():
        kind = "counter" if value == "counter" else "gauge"
        if kind == "counter":
            points = []
            for day in (1, 2):
                for hour in range(4):
                    points.extend([_point(_timestamp(day, hour), 0.0), _point(_timestamp(day, hour, 30), 0.0)])
        else:
            points = [_point(_timestamp(1, hour), value) for hour in range(4)]
            points.extend(_point(_timestamp(2, hour), value) for hour in range(4))
        series[metric_type] = [
            SimpleNamespace(
                metric=SimpleNamespace(labels={"namespace_name": "database", "pod_name": "mysql-0", "container_name": "database"}),
                resource=SimpleNamespace(labels={}),
                points=points,
            )
        ]
    return series


def _assessment_fixtures(
    db_type: str,
    *,
    exporter: bool = True,
    eligible: bool = True,
    persistent: bool = False,
    owner: str = "platform-data",
    environment: str = "staging",
    role: str = "",
    with_backup: bool = True,
    series: dict[str, list[SimpleNamespace]] | None = None,
):
    pod = _database_profile_pod(
        db_type,
        exporter,
        eligible=eligible,
        persistent=persistent,
        owner=owner,
        environment=environment,
        role=role,
    )
    core = FakeCoreApi([pod], namespace_labels={"environment": "staging"} if environment else {})
    backup = None
    if with_backup:
        backup = SimpleNamespace(
            metadata=SimpleNamespace(name=f"{db_type}-backup", namespace="database", labels={}),
            spec=SimpleNamespace(schedule="0 2 * * *"),
            status=SimpleNamespace(last_schedule_time=END_TIME - timedelta(hours=2)),
        )
    profile_series = series if series is not None else _profile_series(db_type)
    monitoring = FakeMonitoringApi({**profile_series, **_gke_series()})
    return pod, core, backup, monitoring


def _run_assessment(monkeypatch, db_type: str, *, days: int = 2, output_mode: str = "json", **fixture_options):
    pod, core, backup, monitoring = _assessment_fixtures(db_type, **fixture_options)
    monkeypatch.setattr(idle_metrics, "_load_kubernetes_client", lambda: core)
    monkeypatch.setattr(idle_metrics, "load_cloud_monitoring_client", lambda: monitoring)
    monkeypatch.setattr(idle_metrics.client, "BatchV1Api", lambda: FakeBatchApi([] if backup is None else [backup]))
    raw = idle_metrics.collect_database_idle_assessment(
        db_type,
        namespace="database",
        pod=f"{db_type}-0",
        days=days,
        project_id="example-project",
        output_mode=output_mode,
    )
    return raw, json.loads(raw) if output_mode == "json" else None


@pytest.mark.parametrize("db_type", ["mysql", "mariadb", "redis", "valkey"])
def test_builtin_profile_registry(db_type: str) -> None:
    profile = idle_metrics.DATABASE_PROFILES[db_type]
    assert profile.db_type == db_type
    assert profile.required_metrics
    assert all(name in profile.kinds for name in profile.required_metrics)
    assert profile.exporter_identities
    if db_type in {"mysql", "mariadb"}:
        assert profile.exporter_recommendation == "mysqld_exporter"
        assert profile.active_thread_metric == "active_threads"
    elif db_type == "redis":
        assert profile.exporter_recommendation == "redis_exporter"
    else:
        assert profile.exporter_recommendation == "valkey-exporter"


def test_mysql_assessment_is_candidate_with_existing_exporter(monkeypatch) -> None:
    raw, payload = _run_assessment(monkeypatch, "mysql", persistent=True)

    assert payload["schema_version"] == "cloud-copilot.dba.idle-cost.v1"
    assert payload["status"] == "ok"
    workload = payload["data"]["workloads"][0]
    assert workload["namespace"] == "database"
    assert workload["pod"] == "mysql-0"
    assert workload["classification"] == "candidate"
    assert workload["node_name"] == "gke-pool-1-abc"
    assert workload["controller_type"] == "StatefulSet"
    assert workload["controller_name"] == "mysql"
    assert workload["owner"] == "platform-data"
    assert workload["environment"] == "staging"
    assert workload["eligibility_evidence"] == "annotation=eligible"
    assert workload["metrics"]["p95_work_per_second"] == 0
    assert workload["metrics"]["mean_active_threads"] == 0.01
    assert workload["metrics"]["p95_cpu_fraction"] == 0.01
    assert workload["metrics"]["zero_work_hour_ratio"] == 1.0
    assert workload["metrics_prerequisite"] == "none"
    assert workload["exporters"][0]["source"] == "sidecar"
    assert workload["kubernetes"]["node_pool"] == "db-pool"
    assert workload["kubernetes"]["resource_envelope"]["requests"]
    assert workload["kubernetes"]["persistent_volume_claims"]["mounted"] == ["mysql-data"]
    assert payload["metadata"]["project_id"] == "example-project"
    assert payload["metadata"]["window_days"] == 2
    assert payload["metadata"]["metrics_source"] == "cloud_monitoring"
    assert payload["metadata"]["missing_metrics"] == []


def test_redis_alias_and_connection_does_not_establish_idleness(monkeypatch) -> None:
    series = {**_profile_series("redis"), **_gke_series()}
    work_type = "prometheus.googleapis.com/redis_commands_processed_total/counter"
    cpu_type = "kubernetes.io/container/cpu/request_utilization"
    series[work_type][0].points = []
    for day in (1, 2):
        for hour in range(4):
            series[work_type][0].points.extend(
                [
                    _point(_timestamp(day, hour), 0.0),
                    _point(_timestamp(day, hour, 30), 3600.0),
                ]
            )
    series[cpu_type][0].points = [_point(_timestamp(1, hour), 0.8) for hour in range(4)]
    series[cpu_type][0].points.extend(_point(_timestamp(2, hour), 0.8) for hour in range(4))

    raw, payload = _run_assessment(monkeypatch, "redis", series=series)

    assert payload["metadata"]["resolved_metrics"] == ["connected_clients", "work_total"]
    assert payload["data"]["workloads"][0]["classification"] == "active"


def test_valkey_assessment_uses_alternate_work_alias(monkeypatch) -> None:
    series = _profile_series("valkey")
    replacement = {
        "prometheus.googleapis.com/redis_total_commands_processed/counter": series.pop(
            "prometheus.googleapis.com/redis_commands_processed_total/counter"
        )
    }
    raw, payload = _run_assessment(monkeypatch, "valkey", series={**series, **replacement})

    assert payload["metadata"]["resolved_metrics"] == ["connected_clients", "work_total"]
    assert payload["data"]["workloads"][0]["db_type"] == "valkey"
    assert payload["metadata"]["exporter_recommendation"] == "valkey-exporter"


def test_missing_exporter_returns_profile_recommendation_and_no_exec(monkeypatch) -> None:
    raw, payload = _run_assessment(monkeypatch, "valkey", exporter=False, series={})

    workload = payload["data"]["workloads"][0]
    assert workload["metrics_prerequisite"] == "missing_exporter"
    assert workload["recommendation"].startswith("Install or enable an existing valkey-exporter")
    assert payload["metadata"]["exporter_discovered"] is False
    assert payload["metadata"]["metrics_prerequisite"] == "missing_exporter"
    assert set(payload["metadata"]["missing_metrics"]) == {"connected_clients", "work_total"}


def test_hourly_aggregation_and_counter_reset(monkeypatch) -> None:
    start = END_TIME - timedelta(hours=4)
    metric = {
        "kind": "counter",
        "samples": [
            idle_metrics.MetricSample(END_TIME - timedelta(hours=4), 0),
            idle_metrics.MetricSample(END_TIME - timedelta(hours=3), 100),
            idle_metrics.MetricSample(END_TIME - timedelta(hours=2), 50),
            idle_metrics.MetricSample(END_TIME - timedelta(hours=1), 50),
        ],
    }
    aggregate = idle_metrics.aggregate_hourly(metric, start, END_TIME)

    assert len(aggregate["values"]) == 2
    assert aggregate["restarts"] == 1
    assert aggregate["values"][0] == pytest.approx(100 / 3600)
    assert aggregate["values"][1:] == [0.0]


def test_metric_alias_label_matching_uses_namespace_and_pod(monkeypatch) -> None:
    aliases = {"work_total": ("redis_commands_processed_total", "redis_total_commands_processed")}
    kinds = {"work_total": "counter"}
    wrong_namespace = SimpleNamespace(
        metric=_labels("other", "redis-0"),
        points=[_point(_timestamp(1, 0), 100)],
    )
    matching = SimpleNamespace(
        metric=_labels("database", "redis-0"),
        points=[_point(_timestamp(1, 0), 100)],
    )
    monitoring = FakeMonitoringApi(
        {
            "prometheus.googleapis.com/redis_total_commands_processed/counter": [
                wrong_namespace,
                matching,
            ]
        }
    )
    monkeypatch.setattr(idle_metrics, "load_cloud_monitoring_client", lambda: monitoring)
    metrics = idle_metrics.read_monitoring_series(
        "example-project",
        _timestamp(1, 0),
        _timestamp(2, 0),
        [("database", "redis-0")],
        aliases,
        kinds,
    )

    assert list(metrics) == ["work_total"]
    assert metrics["work_total"]["metric_type"] == "prometheus.googleapis.com/redis_total_commands_processed/counter"


def test_missing_required_series_blocks_rather_than_marks_idle(monkeypatch) -> None:
    profile = idle_metrics.DATABASE_PROFILES["mysql"]
    metrics = {"work_total": {"samples": [idle_metrics.MetricSample(_timestamp(1, 0), 0)]}}
    classification, reasons = idle_metrics._classification(
        profile,
        metrics,
        {"p95": 0.0, "zero_work_hour_ratio": 1.0, "restarts": 0},
        0.0,
        0.0,
        idle_metrics.classification_thresholds(),
        [],
    )

    assert classification == "blocked"
    assert any("connected_clients" in reason for reason in reasons)


def test_replica_role_and_missing_owner_block_shutdown(monkeypatch) -> None:
    raw, payload = _run_assessment(
        monkeypatch,
        "redis",
        role="replica",
        owner="",
        environment="",
    )

    workload = payload["data"]["workloads"][0]
    assert workload["classification"] == "blocked"
    assert {"replication_failover_role", "ownership_missing", "environment_missing"} <= set(workload["risks"])
    assert workload["recommendation"].startswith("Do not recommend shutdown")


def test_not_eligible_and_active_backup_block_candidate(monkeypatch) -> None:
    raw, payload = _run_assessment(
        monkeypatch,
        "mysql",
        eligible=False,
        role="primary",
        persistent=True,
    )

    workload = payload["data"]["workloads"][0]
    assert workload["classification"] == "blocked"
    assert "not_cost_review_eligible" in workload["risks"]
    assert "primary_with_active_backup" in workload["risks"]


def test_human_output_and_invalid_parameters(monkeypatch) -> None:
    human, _ = _run_assessment(monkeypatch, "mysql", output_mode="human")
    assert "Database idle-cost assessment" in human
    assert "mysql-0 (mysql): candidate" in human

    payload = json.loads(idle_metrics.collect_database_idle_assessment("mongodb", days=2))
    assert payload["status"] == "error"
    assert "Unsupported database type" in payload["errors"][0]

    payload = json.loads(idle_metrics.collect_database_idle_assessment("mysql", days=32))
    assert payload["status"] == "error"
    assert "days must be between 1 and 31" in payload["errors"][0]

    monkeypatch.setenv("DBA_IDLE_ASSESSMENT_MIN_DAYS", "2")
    payload = json.loads(idle_metrics.collect_database_idle_assessment("mysql", days=1))
    assert payload["status"] == "error"
    assert "days must be between 1 and 31" in payload["errors"][0]


def test_idle_connections_with_nonzero_work_are_active(monkeypatch) -> None:
    profile = idle_metrics.DATABASE_PROFILES["redis"]
    work = {"p95": 0.2, "zero_work_hour_ratio": 0.0, "restarts": 0}
    classification, reasons = idle_metrics._classification(
        profile,
        {
            "work_total": {"samples": [idle_metrics.MetricSample(_timestamp(1, 0), 0)]},
            "connected_clients": {"samples": [idle_metrics.MetricSample(_timestamp(1, 0), 50)]},
        },
        work,
        None,
        0.2,
        idle_metrics.classification_thresholds(),
        [],
    )

    assert classification == "active"
    assert any("work p95" in reason for reason in reasons)


def test_assessment_only_reads_kubernetes_and_cloud_monitoring() -> None:
    source = inspect.getsource(idle_metrics)
    forbidden = (
        "connect_get_namespaced_pod_exec",
        "stream.stream",
        "read_namespaced_secret",
        "create_namespaced_",
        "patch_namespaced_",
        "replace_namespaced_",
        "delete_namespaced_",
    )
    assert not any(term in source for term in forbidden)
    signature = inspect.signature(idle_metrics.collect_database_idle_assessment)
    assert not any(name in {"credential", "password", "token", "secret"} for name in signature.parameters)

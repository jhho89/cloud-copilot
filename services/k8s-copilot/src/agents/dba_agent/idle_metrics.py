from __future__ import annotations

import json
import os
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional

from kubernetes import client, config


SCHEMA_VERSION = "cloud-copilot.dba.idle-cost.v1"
SUPPORTED_DATABASE_TYPES = ("mysql", "mariadb", "redis", "valkey", "clickhouse")
REDIS_DATABASE_TYPES = {"redis", "valkey"}

_GKE_CPU_REQUEST = "kubernetes.io/container/cpu/request_utilization"
_GKE_CPU_LIMIT = "kubernetes.io/container/cpu/limit_utilization"
_GKE_MEMORY_REQUEST = "kubernetes.io/container/memory/request_utilization"
_GKE_MEMORY_LIMIT = "kubernetes.io/container/memory/limit_utilization"
_GKE_NETWORK_SENT = "kubernetes.io/container/network/sent_bytes_count"
_GKE_NETWORK_RECEIVED = "kubernetes.io/container/network/received_bytes_count"


@dataclass(frozen=True)
class DatabaseMetricProfile:
    """Declarative mapping from an exporter family to idle-cost evidence."""

    db_type: str
    aliases: Mapping[str, tuple[str, ...]]
    kinds: Mapping[str, str]
    required_metrics: tuple[str, ...]
    work_metrics: tuple[str, ...]
    active_thread_metric: Optional[str]
    exporter_identities: tuple[str, ...]
    exporter_recommendation: str
    role_label_keys: tuple[str, ...] = field(default=())
    backup_names: tuple[str, ...] = field(default=("backup", "dump", "snapshot"))


@dataclass
class MetricSample:
    timestamp: datetime
    value: float
    labels: dict[str, str] = field(default_factory=dict)


MYSQL_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "work_total": (
        "mysql_global_status_queries",
        "mysql_global_status_questions",
        "mysql_global_status_commands_total",
    ),
    "active_threads": (
        "mysql_global_status_threads_running",
        "mysql_global_threads_running",
    ),
    "connected_clients": (
        "mysql_global_status_threads_connected",
        "mysql_global_threads_connected",
    ),
    "uptime": ("mysql_global_status_uptime",),
}
REDIS_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "work_total": (
        "redis_commands_processed_total",
        "redis_total_commands_processed",
        "redis_commands_total",
    ),
    "instantaneous_work_rate": (
        "redis_instantaneous_ops_per_sec",
        "redis_commands_per_second",
    ),
    "connected_clients": (
        "redis_connected_clients",
        "redis_clients_connected",
    ),
    "memory_used_bytes": (
        "redis_memory_used_bytes",
        "redis_used_memory_bytes",
    ),
    "maxmemory_bytes": (
        "redis_memory_max_bytes",
        "redis_maxmemory_bytes",
    ),
    "evicted_total": (
        "redis_evicted_keys_total",
        "redis_evicted_keys",
        "redis_total_keys_evicted",
    ),
    "hits_total": ("redis_keyspace_hits_total", "redis_keyspace_hits"),
    "misses_total": ("redis_keyspace_misses_total", "redis_keyspace_misses"),
}
MYSQL_METRIC_KINDS = {
    "work_total": "counter",
    "active_threads": "gauge",
    "connected_clients": "gauge",
    "uptime": "counter",
}
CLICKHOUSE_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "work_total": (
        "ClickHouseProfileEvents_Query",
        "chi_clickhouse_event_query",
        "chi_clickhouse_event_Query",
    ),
    "select_work_total": (
        "ClickHouseProfileEvents_SelectQuery",
        "chi_clickhouse_event_select_query",
        "chi_clickhouse_event_SelectQuery",
    ),
    "insert_work_total": (
        "ClickHouseProfileEvents_InsertQuery",
        "chi_clickhouse_event_insert_query",
        "chi_clickhouse_event_InsertQuery",
    ),
    "inserted_rows_total": (
        "ClickHouseProfileEvents_InsertedRows",
        "chi_clickhouse_event_inserted_rows",
        "chi_clickhouse_event_InsertedRows",
    ),
    "active_threads": (
        "ClickHouseMetrics_Query",
        "ClickHouseAsyncMetrics_Query",
        "chi_clickhouse_metric_query",
    ),
    "connected_clients": (
        "ClickHouseMetrics_Connection",
        "ClickHouseAsyncMetrics_Connection",
        "chi_clickhouse_metric_connection",
    ),
    "merges_running": (
        "ClickHouseMetrics_Merge",
        "ClickHouseAsyncMetrics_Merge",
        "chi_clickhouse_metric_merge",
    ),
}
CLICKHOUSE_METRIC_KINDS = {
    "work_total": "counter",
    "select_work_total": "counter",
    "insert_work_total": "counter",
    "inserted_rows_total": "counter",
    "active_threads": "gauge",
    "connected_clients": "gauge",
    "merges_running": "gauge",
}
REDIS_METRIC_KINDS = {
    "work_total": "counter",
    "instantaneous_work_rate": "gauge",
    "connected_clients": "gauge",
    "memory_used_bytes": "gauge",
    "maxmemory_bytes": "gauge",
    "evicted_total": "counter",
    "hits_total": "counter",
    "misses_total": "counter",
}

DATABASE_PROFILES: dict[str, DatabaseMetricProfile] = {
    "mysql": DatabaseMetricProfile(
        db_type="mysql",
        aliases=MYSQL_METRIC_ALIASES,
        kinds=MYSQL_METRIC_KINDS,
        required_metrics=("work_total", "active_threads", "connected_clients"),
        work_metrics=("work_total",),
        active_thread_metric="active_threads",
        exporter_identities=(
            "mysqld-exporter",
            "mysqld_exporter",
            "mysql-exporter",
            "mysql_exporter",
            "percona/mysqld_exporter",
            "prometheus/mysqld_exporter",
        ),
        exporter_recommendation="mysqld_exporter",
        role_label_keys=(
            "mysql-role",
            "mariadb-role",
            "app.kubernetes.io/component",
            "mysql.kubernetes.io/role",
        ),
        backup_names=("mysql-backup", "mariadb-backup", "backup", "dump", "xtrabackup"),
    ),
    "mariadb": DatabaseMetricProfile(
        db_type="mariadb",
        aliases=MYSQL_METRIC_ALIASES,
        kinds=MYSQL_METRIC_KINDS,
        required_metrics=("work_total", "active_threads", "connected_clients"),
        work_metrics=("work_total",),
        active_thread_metric="active_threads",
        exporter_identities=(
            "mysqld-exporter",
            "mysqld_exporter",
            "mysql-exporter",
            "mysql_exporter",
            "mariadb-exporter",
            "mariadb_exporter",
            "percona/mysqld_exporter",
        ),
        exporter_recommendation="mysqld_exporter",
        role_label_keys=(
            "mariadb-role",
            "mysql-role",
            "app.kubernetes.io/component",
            "mariadb.kubernetes.io/role",
        ),
        backup_names=("mariadb-backup", "mysql-backup", "backup", "dump", "xtrabackup"),
    ),
    "redis": DatabaseMetricProfile(
        db_type="redis",
        aliases=REDIS_METRIC_ALIASES,
        kinds=REDIS_METRIC_KINDS,
        required_metrics=("work_total", "connected_clients"),
        work_metrics=("work_total", "instantaneous_work_rate"),
        active_thread_metric=None,
        exporter_identities=(
            "redis-exporter",
            "redis_exporter",
            "valkey-exporter",
            "oliver006/redis_exporter",
        ),
        exporter_recommendation="redis_exporter",
        role_label_keys=(
            "redis-role",
            "app.kubernetes.io/component",
            "redis.kubernetes.io/role",
            "valkey.kubernetes.io/role",
        ),
    ),
    "valkey": DatabaseMetricProfile(
        db_type="valkey",
        aliases=REDIS_METRIC_ALIASES,
        kinds=REDIS_METRIC_KINDS,
        required_metrics=("work_total", "connected_clients"),
        work_metrics=("work_total", "instantaneous_work_rate"),
        active_thread_metric=None,
        exporter_identities=(
            "valkey-exporter",
            "valkey_exporter",
            "redis-exporter",
            "redis_exporter",
            "oliver006/redis_exporter",
        ),
        exporter_recommendation="valkey-exporter",
        role_label_keys=(
            "valkey-role",
            "redis-role",
            "app.kubernetes.io/component",
            "valkey.kubernetes.io/role",
        ),
    ),
    "clickhouse": DatabaseMetricProfile(
        db_type="clickhouse",
        aliases=CLICKHOUSE_METRIC_ALIASES,
        kinds=CLICKHOUSE_METRIC_KINDS,
        required_metrics=("work_total", "active_threads", "connected_clients", "merges_running"),
        work_metrics=("work_total", "insert_work_total", "inserted_rows_total"),
        active_thread_metric="active_threads",
        exporter_identities=(
            "altinity/metrics-exporter",
            "chi-",
        ),
        exporter_recommendation="native ClickHouse Prometheus endpoint or Altinity operator metrics",
        role_label_keys=(
            "clickhouse-role",
            "clickhouse.altinity.com/replica",
            "app.kubernetes.io/component",
        ),
        backup_names=("clickhouse-backup", "clickhouse_backup", "backup", "dump", "snapshot"),
    ),
}


class MetricsProjectMissingError(RuntimeError):
    """Raised when a Cloud Monitoring project cannot be safely resolved."""


def _load_kubernetes_client() -> client.CoreV1Api:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        config.load_kube_config(config_file=kubeconfig_path)
    return client.CoreV1Api()


def resolve_metrics_project_id(requested: Optional[str]) -> str:
    project = requested
    if not project:
        for variable in (
            "DBA_DATABASE_METRICS_PROJECT_ID",
        "DBA_CLICKHOUSE_METRICS_PROJECT_ID",
            "DBA_REDIS_METRICS_PROJECT_ID",
            "GOOGLE_CLOUD_PROJECT",
            "GCP_PROJECT_ID",
        ):
            project = os.getenv(variable)
            if project:
                break
    if project:
        return str(project)
    try:
        import google.auth

        _, inferred = google.auth.default()
    except Exception as exc:
        raise MetricsProjectMissingError(
            "A metrics project is not configured. Set DBA_DATABASE_METRICS_PROJECT_ID or provide project_id."
        ) from exc
    if not inferred:
        raise MetricsProjectMissingError(
            "A metrics project is not configured. Set DBA_DATABASE_METRICS_PROJECT_ID or provide project_id."
        )
    return str(inferred)


def load_cloud_monitoring_client() -> Any:
    from google.cloud import monitoring_v3

    return monitoring_v3.MetricServiceClient()


def normalize_metric_labels(
    metric_labels: Optional[Mapping[str, str]],
    resource_labels: Optional[Mapping[str, str]],
) -> dict[str, str]:
    labels = {str(key): str(value) for key, value in (metric_labels or {}).items()}
    labels.update({str(key): str(value) for key, value in (resource_labels or {}).items()})
    return labels


def metric_matches_target(
    labels: Mapping[str, str],
    targets: Iterable[tuple[str, str]],
    *,
    container_names: Optional[set[str]] = None,
) -> bool:
    target_pairs = set(targets)
    label_namespace = next(
        (
            str(labels.get(name, ""))
            for name in ("namespace", "namespace_name", "k8s_namespace", "k8s_pod_namespace")
            if labels.get(name)
        ),
        "",
    )
    label_pod = next(
        (
            str(labels.get(name, ""))
            for name in ("pod", "pod_name", "k8s_pod", "k8s_pod_name")
            if labels.get(name)
        ),
        "",
    )
    if label_pod:
        # Pod names repeat across namespaces (StatefulSet ordinals), so a pod
        # label without a namespace cannot be attributed safely.
        if not label_namespace or (label_namespace, label_pod) not in target_pairs:
            return False
    elif label_namespace:
        if not any(namespace == label_namespace for namespace, _ in target_pairs):
            return False
    else:
        return False
    label_container = next(
        (str(labels.get(name, "")) for name in ("container", "container_name") if labels.get(name)),
        "",
    )
    if label_container and container_names and label_container not in container_names:
        return False
    return True


def _typed_number(value: Any) -> Any:
    """Read a TypedValue number without mistaking unset proto-plus fields for 0.0."""
    raw = getattr(value, "_pb", None)
    if raw is not None and hasattr(raw, "WhichOneof"):
        kind = raw.WhichOneof("value")
        if kind in {"double_value", "int64_value"}:
            return getattr(value, kind)
        return None
    number = getattr(value, "double_value", None)
    if number is None:
        number = getattr(value, "int64_value", None)
    return number


def parse_metric_samples(points: Any, labels: Optional[Mapping[str, str]] = None) -> list[MetricSample]:
    samples: list[MetricSample] = []
    shared_labels = dict(labels or {})
    for point in points or []:
        interval = getattr(point, "interval", None)
        value = getattr(point, "value", None)
        end_time = getattr(interval, "end_time", None)
        number = _typed_number(value)
        if end_time is None or number is None or not isinstance(number, (int, float)):
            continue
        timestamp = end_time
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if getattr(timestamp, "tzinfo", None) is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        if isinstance(number, (int, float)) and math.isfinite(float(number)):
            samples.append(MetricSample(timestamp, float(number), shared_labels))
    samples.sort(key=lambda sample: sample.timestamp)
    return samples


def read_monitoring_series(
    project_id: str,
    start_time: datetime,
    end_time: datetime,
    targets: Iterable[tuple[str, str]],
    aliases: Mapping[str, tuple[str, ...]],
    kinds: Mapping[str, str],
    *,
    descriptor_prefix: str = "prometheus.googleapis.com/",
    known_metric_types: Optional[Mapping[str, str]] = None,
    container_names: Optional[set[str]] = None,
) -> dict[str, dict[str, Any]]:
    """Read one matching Cloud Monitoring metric type at a time.

    The caller supplies every namespace/pod target. This deliberately avoids
    issuing a query per pod, and Cloud Monitoring remains the only trend source.
    """

    targets = list(targets)
    if not targets:
        return {}
    monitoring_v3 = _monitoring_v3_module()
    monitoring_client = load_cloud_monitoring_client()
    interval = monitoring_v3.TimeInterval(
        {
            "start_time": {"seconds": int(start_time.timestamp())},
            "end_time": {"seconds": int(end_time.timestamp())},
        }
    )
    # Alias order is priority order. Exporters often expose several aliases of
    # one logical metric (for example Queries and Questions); mixing them in one
    # counter series produces false resets, so only the best present alias is used.
    alias_rank: dict[str, tuple[str, int]] = {}
    for logical, names in aliases.items():
        for rank, name in enumerate(names):
            alias_rank.setdefault(name, (logical, rank))

    candidates: list[tuple[int, str, str]] = []
    if alias_rank:
        for descriptor in monitoring_client.list_metric_descriptors(
            request={
                "name": f"projects/{project_id}",
                "filter": f'metric.type = starts_with("{descriptor_prefix}")',
            }
        ):
            metric_type = str(getattr(descriptor, "type", ""))
            parts = metric_type.split("/")
            if len(parts) < 3 or parts[-2] not in alias_rank:
                continue
            logical, rank = alias_rank[parts[-2]]
            if all(metric_type != existing for _, _, existing in candidates):
                candidates.append((rank, logical, metric_type))
    candidates.sort(key=lambda item: item[0])
    ordered: list[tuple[str, str]] = [(logical, metric_type) for _, logical, metric_type in candidates]
    for logical, metric_type in (known_metric_types or {}).items():
        if all(metric_type != existing for _, existing in ordered):
            ordered.append((logical, metric_type))

    namespaces = sorted({namespace for namespace, _ in targets})
    quoted = ", ".join('"' + namespace.replace("\\", "\\\\").replace('"', '\\"') + '"' for namespace in namespaces)
    namespace_filter = (
        f"(resource.labels.namespace = one_of({quoted}) OR resource.labels.namespace_name = one_of({quoted}))"
    )

    metrics_by_logical: dict[str, dict[str, Any]] = {}
    for logical, metric_type in ordered:
        if logical in metrics_by_logical:
            continue
        series_list = monitoring_client.list_time_series(
            request={
                "name": f"projects/{project_id}",
                "filter": f'metric.type = "{metric_type}" AND {namespace_filter}',
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )
        for series in series_list:
            labels = normalize_metric_labels(
                getattr(series.metric, "labels", None),
                getattr(getattr(series, "resource", None), "labels", None),
            )
            if not metric_matches_target(labels, targets, container_names=container_names):
                continue
            samples = parse_metric_samples(getattr(series, "points", None), labels)
            if not samples:
                continue
            existing = metrics_by_logical.setdefault(
                logical,
                {"metric_type": metric_type, "kind": kinds.get(logical, "gauge"), "labels": labels, "samples": []},
            )
            existing["samples"].extend(samples)
            existing["labels"] = labels

    for metric in metrics_by_logical.values():
        metric["samples"].sort(key=lambda sample: sample.timestamp)
    return metrics_by_logical


def _monitoring_v3_module() -> Any:
    from google.cloud import monitoring_v3

    return monitoring_v3


def _hour_start(timestamp: datetime) -> datetime:
    return timestamp.replace(minute=0, second=0, microsecond=0)


def aggregate_hourly(
    metric: Optional[dict[str, Any]],
    start_time: datetime,
    end_time: datetime,
) -> dict[str, Any]:
    """Aggregate samples into one value per hour.

    Counter resets produce no negative rate and are retained as restart evidence.
    """

    if not metric or not metric.get("samples"):
        return {"values": [], "restarts": 0, "hours_with_data": 0}
    kind = metric.get("kind", "gauge")
    samples = list(metric["samples"])
    restarts = 0
    if kind == "counter":
        # Counters are differenced per series. Interleaving distinct series
        # (different label sets) would look like constant resets.
        grouped: dict[tuple[tuple[str, str], ...], list[MetricSample]] = {}
        for sample in samples:
            grouped.setdefault(tuple(sorted(sample.labels.items())), []).append(sample)
        rate_by_hour: dict[datetime, float] = {}
        for series_samples in grouped.values():
            series_hours: dict[datetime, list[float]] = {}
            previous: Optional[MetricSample] = None
            for sample in series_samples:
                if previous is None:
                    previous = sample
                    continue
                elapsed = (sample.timestamp - previous.timestamp).total_seconds()
                if elapsed <= 0:
                    continue
                if sample.value < previous.value:
                    restarts += 1
                    previous = sample
                    continue
                rate = (sample.value - previous.value) / elapsed
                series_hours.setdefault(_hour_start(sample.timestamp), []).append(rate)
                previous = sample
            for hour, rates in series_hours.items():
                rate_by_hour[hour] = rate_by_hour.get(hour, 0.0) + sum(rates) / len(rates)
        hours = sorted(rate_by_hour)
        return {"values": [rate_by_hour[hour] for hour in hours], "restarts": restarts, "hours_with_data": len(hours)}
    values_by_hour: dict[datetime, list[float]] = {}
    for sample in samples:
        values_by_hour.setdefault(_hour_start(sample.timestamp), []).append(sample.value)
    hours = sorted(values_by_hour)
    values = [sum(values) / len(values) for values in (values_by_hour[hour] for hour in hours)]
    return {"values": values, "restarts": restarts, "hours_with_data": len(hours)}


def _percentile_95(values: list[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * 0.95) - 1)
    return ordered[index]


def _environment_number(name: str, default: float, *, minimum: Optional[float] = None) -> float:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        value = float(raw_value)
    except ValueError:
        return default
    if minimum is not None and value < minimum:
        return default
    return value


def classification_thresholds() -> dict[str, float]:
    return {
        "work_p95_ops_per_sec": _environment_number("DBA_IDLE_WORK_P95_OPS_PER_SEC", 0.01, minimum=0.0),
        "cpu_p95_fraction": _environment_number("DBA_IDLE_CPU_P95_FRACTION", 0.05, minimum=0.0),
        "min_zero_work_ratio": _environment_number("DBA_IDLE_MIN_ZERO_WORK_RATIO", 0.90, minimum=0.0),
        # Threads_running is at least 1 while the exporter itself is querying.
        "active_threads_mean": _environment_number("DBA_IDLE_ACTIVE_THREADS_MEAN", 1.05, minimum=0.0),
        # Ops/s produced by the exporter's own scrapes; subtracted from the work rate.
        "work_baseline_ops_per_sec": _environment_number("DBA_IDLE_EXPORTER_BASELINE_OPS_PER_SEC", 0.0, minimum=0.0),
        "min_data_coverage": min(1.0, _environment_number("DBA_IDLE_MIN_DATA_COVERAGE", 0.8, minimum=0.0)),
    }


DEFAULT_MINIMUM_WINDOW_DAYS = 7


def minimum_window_days() -> int:
    try:
        configured = int(os.getenv("DBA_IDLE_ASSESSMENT_MIN_DAYS", str(DEFAULT_MINIMUM_WINDOW_DAYS)))
    except ValueError:
        configured = DEFAULT_MINIMUM_WINDOW_DAYS
    return configured if 1 <= configured <= 31 else DEFAULT_MINIMUM_WINDOW_DAYS


def _container_identity(container: Any) -> str:
    return f"{getattr(container, 'name', '') or ''} {getattr(container, 'image', '') or ''}".lower()


def _is_exporter_container(container: Any, db_type: str) -> bool:
    profile = DATABASE_PROFILES.get(db_type)
    identity = _container_identity(container)
    return bool(profile and any(marker in identity for marker in profile.exporter_identities))


def _matches_profile(pod: Any, db_type: str) -> bool:
    containers = list(getattr(getattr(pod, "spec", None), "containers", None) or [])
    database_side = [container for container in containers if not _is_exporter_container(container, db_type)]
    if containers and not database_side:
        # An exporter-only pod (for example a standalone mysqld-exporter
        # Deployment) is not a database workload.
        return False
    metadata = getattr(pod, "metadata", None)
    labels = dict(getattr(metadata, "labels", None) or {})
    values = [str(getattr(metadata, "name", "") or "")]
    values.extend(str(key) for key in labels)
    values.extend(str(value) for value in labels.values())
    values.extend(_container_identity(container) for container in database_side)
    return db_type in " ".join(values).lower()


def _labels(pod: Any) -> dict[str, str]:
    return {str(key): str(value) for key, value in (getattr(getattr(pod, "metadata", None), "labels", None) or {}).items()}


def _annotations(pod: Any) -> dict[str, str]:
    return {str(key): str(value) for key, value in (getattr(getattr(pod, "metadata", None), "annotations", None) or {}).items()}


def _role(labels: Mapping[str, str], profile: DatabaseMetricProfile) -> str:
    aliases = {"master": "primary", "primary": "primary", "leader": "primary", "replica": "replica", "slave": "replica"}
    for key in profile.role_label_keys:
        value = str(labels.get(key, "")).strip().lower()
        if value in aliases:
            return aliases[value]
    return ""


def _controller_context(pod: Any) -> tuple[str, str]:
    """Return the workload kind/name GKE cost allocation reports.

    GKE labels Deployment-owned pods with the Deployment, not the intermediate
    ReplicaSet that ownerReferences point at.
    """
    metadata = getattr(pod, "metadata", None)
    for owner in getattr(metadata, "owner_references", None) or []:
        if not bool(getattr(owner, "controller", False)):
            continue
        kind = str(getattr(owner, "kind", "") or "")
        name = str(getattr(owner, "name", "") or "")
        if kind == "ReplicaSet":
            template_hash = str((getattr(metadata, "labels", None) or {}).get("pod-template-hash", ""))
            suffix = f"-{template_hash}"
            if template_hash and name.endswith(suffix) and len(name) > len(suffix):
                return "Deployment", name[: -len(suffix)]
        return kind, name
    return "", ""


def _all_container_names(pod: Any) -> set[str]:
    return {str(getattr(container, "name", "") or "") for container in getattr(getattr(pod, "spec", None), "containers", None) or []}


def _database_containers(pod: Any, db_type: str) -> list[Any]:
    containers = list(getattr(getattr(pod, "spec", None), "containers", None) or [])
    database_side = [container for container in containers if not _is_exporter_container(container, db_type)]
    matching = [container for container in database_side if db_type in _container_identity(container)]
    if matching:
        return matching
    return database_side[:1] if database_side else containers[:1]


def _exporter_discovery(pod: Any, profile: DatabaseMetricProfile) -> list[dict[str, str]]:
    """Find an existing exporter; no endpoint is probed and no exporter is installed."""

    discovered: list[dict[str, str]] = []
    annotations = _annotations(pod)
    scrape = str(annotations.get("prometheus.io/scrape", "")).lower() in {"true", "1", "yes"}
    port = str(annotations.get("prometheus.io/port", "")).strip()
    path = str(annotations.get("prometheus.io/path", "/metrics")).strip() or "/metrics"
    for container in getattr(getattr(pod, "spec", None), "containers", None) or []:
        name = str(getattr(container, "name", "") or "")
        image = str(getattr(container, "image", "") or "")
        combined = f"{name} {image}".lower()
        if any(identity in combined for identity in profile.exporter_identities):
            discovered.append(
                {
                    "source": "sidecar" if name not in {item.name for item in _database_containers(pod, profile.db_type)} else "database_container",
                    "container": name,
                    "image": image,
                    "port": port,
                    "path": path,
                    "scrape_enabled": str(scrape).lower(),
                }
            )
        ports = getattr(container, "ports", None) or []
        for port_item in ports:
            port_name = str(getattr(port_item, "name", "") or "").lower()
            if "metrics" in port_name or "prometheus" in port_name:
                discovered.append(
                    {
                        "source": "database_native_endpoint",
                        "container": name,
                        "image": image,
                        "port": str(getattr(port_item, "container_port", "") or ""),
                        "path": path,
                        "scrape_enabled": str(scrape).lower(),
                    }
                )
    if scrape and port:
        discovered.append({"source": "prometheus_annotations", "container": "", "image": "", "port": port, "path": path, "scrape_enabled": "true"})
    unique: list[dict[str, str]] = []
    for item in discovered:
        if item not in unique:
            unique.append(item)
    return unique


def _resource_value(value: object) -> str:
    return str(value) if value is not None else ""


def _container_envelope(pod: Any, db_type: str) -> dict[str, Any]:
    requests: dict[str, str] = {}
    limits: dict[str, str] = {}
    container_names: list[str] = []
    images: list[str] = []
    for container in _database_containers(pod, db_type):
        container_names.append(str(getattr(container, "name", "") or ""))
        images.append(str(getattr(container, "image", "") or ""))
        resources = getattr(container, "resources", None)
        for target, source_name in ((requests, "requests"), (limits, "limits")):
            values = dict(getattr(resources, source_name, None) or {})
            for key, value in values.items():
                formatted = _resource_value(value)
                if formatted:
                    target[str(key)] = formatted
    return {
        "container_names": container_names,
        "images": images,
        "requests": requests,
        "limits": limits,
    }


def _volume_claims(pod: Any) -> list[str]:
    claims: list[str] = []
    for volume in getattr(getattr(pod, "spec", None), "volumes", None) or []:
        claim = getattr(volume, "persistent_volume_claim", None)
        name = str(getattr(claim, "claim_name", "") or "")
        if name and name not in claims:
            claims.append(name)
    return claims


def _node_pool_information(pod: Any, nodes_by_name: Mapping[str, Any]) -> dict[str, str]:
    node_name = str(getattr(getattr(pod, "spec", None), "node_name", "") or "")
    node = nodes_by_name.get(node_name)
    labels = dict(getattr(getattr(node, "metadata", None), "labels", None) or {}) if node else {}
    return {
        "node_name": node_name,
        "node_pool": str(labels.get("cloud.google.com/gke-nodepool", "") or ""),
        "machine_type": str(labels.get("node.kubernetes.io/machine-type", "") or ""),
    }


def _first_label(labels: Mapping[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = str(labels.get(key, "")).strip()
        if value:
            return value
    return ""


def _owner_and_environment(pod: Any, namespace_labels: Mapping[str, Mapping[str, str]], namespace: str) -> tuple[str, str]:
    labels = _labels(pod)
    owner = _first_label(
        labels,
        (
            "cost-owner",
            "cloud-copilot.ai/owner",
            "owner",
            "team",
            "owner-team",
        ),
    )
    environment = _first_label(
        labels,
        ("environment", "env", "app.kubernetes.io/environment", "cloud-copilot.ai/environment"),
    )
    current = namespace_labels.get(namespace, {})
    owner = owner or _first_label(current, ("cost-owner", "cloud-copilot.ai/owner", "owner", "team"))
    environment = environment or _first_label(current, ("environment", "env", "cloud-copilot.ai/environment"))
    if environment:
        return owner, _normalize_environment(environment)
    tokens = _namespace_tokens(namespace)
    if tokens & _PRODUCTION_TOKENS:
        environment = "production"
    elif tokens & _NON_PRODUCTION_TOKENS:
        environment = "non-production"
    return owner, environment


_PRODUCTION_TOKENS = frozenset({"prod", "production", "prd", "live"})
_NON_PRODUCTION_TOKENS = frozenset({"dev", "development", "test", "testing", "staging", "stage", "stg", "sandbox", "qa", "uat"})


def _namespace_tokens(namespace: str) -> set[str]:
    # Whole-token matching: "latest-orders" is not "test", "backstage" is not "stage".
    return {token for token in re.split(r"[^a-z]+", namespace.lower()) if token}


def _normalize_environment(value: str) -> str:
    lowered = value.strip().lower()
    return "production" if lowered in _PRODUCTION_TOKENS else lowered


def _is_production(namespace: str, environment: str) -> bool:
    return _normalize_environment(environment) == "production" or bool(_namespace_tokens(namespace) & _PRODUCTION_TOKENS)


def _eligibility(pod: Any) -> tuple[str, str]:
    annotation_name = os.getenv("DBA_COST_REVIEW_ELIGIBILITY_ANNOTATION", "cloud-copilot.ai/cost-review")
    annotations = _annotations(pod)
    labels = _labels(pod)
    key = annotation_name.lower()
    for source_name, values in (("annotation", annotations), ("label", labels)):
        for candidate_key, value in values.items():
            if candidate_key.lower() == key:
                return source_name, str(value)
    return "", ""


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in {"eligible", "true", "yes", "1"}


def _data_gaps(start_time: datetime, end_time: datetime, observed_hours: int) -> int:
    first = _hour_start(start_time)
    expected = int(math.ceil((end_time - first).total_seconds() / 3600.0))
    return max(0, expected - observed_hours)


def _work_evidence(
    metrics: Mapping[str, dict[str, Any]],
    profile: DatabaseMetricProfile,
    start_time: datetime,
    end_time: datetime,
    baseline: float = 0.0,
) -> dict[str, Any]:
    hourly_values: list[float] = []
    restarts = 0
    observed_hours = 0
    for logical in profile.work_metrics:
        aggregate = aggregate_hourly(metrics.get(logical), start_time, end_time)
        restarts += int(aggregate["restarts"])
        observed_hours = max(observed_hours, int(aggregate["hours_with_data"]))
        if aggregate["values"]:
            # The exporter's own scrape traffic is counted as database work.
            hourly_values = [max(0.0, value - baseline) for value in aggregate["values"]]
            break
    p95 = _percentile_95(hourly_values)
    zero_ratio = sum(1 for value in hourly_values if value <= 0.0) / len(hourly_values) if hourly_values else 0.0
    return {
        "p95": p95,
        "zero_work_hour_ratio": zero_ratio,
        "restarts": restarts,
        "hours_with_data": observed_hours,
        "data_gaps": _data_gaps(start_time, end_time, observed_hours),
    }


def _clickhouse_insert_work_evidence(
    metrics: Mapping[str, dict[str, Any]],
    start_time: datetime,
    end_time: datetime,
    baseline: float,
) -> dict[str, Any]:
    """Treat inserts as work even when query counters are absent or zero."""
    restarts = 0
    observed_hours = 0
    for logical in ("insert_work_total", "inserted_rows_total"):
        aggregate = aggregate_hourly(metrics.get(logical), start_time, end_time)
        restarts += int(aggregate["restarts"])
        observed_hours = max(observed_hours, int(aggregate["hours_with_data"]))
        if not aggregate["values"]:
            continue
        values = [max(0.0, value - baseline) for value in aggregate["values"]]
        return {
            "p95": _percentile_95(values),
            "zero_work_hour_ratio": sum(1 for value in values if value <= 0.0) / len(values),
            "restarts": restarts,
            "hours_with_data": observed_hours,
            "data_gaps": _data_gaps(start_time, end_time, observed_hours),
        }
    return {"p95": None, "zero_work_hour_ratio": 0.0, "restarts": restarts, "hours_with_data": observed_hours, "data_gaps": 0}


def _active_thread_evidence(
    metrics: Mapping[str, dict[str, Any]],
    profile: DatabaseMetricProfile,
    start_time: datetime,
    end_time: datetime,
) -> Optional[float]:
    if not profile.active_thread_metric:
        return None
    values = aggregate_hourly(metrics.get(profile.active_thread_metric), start_time, end_time)["values"]
    return sum(values) / len(values) if values else None


def _p95_metric(logical: str, metrics: Mapping[str, dict[str, Any]], start_time: datetime, end_time: datetime) -> Optional[float]:
    return _percentile_95(aggregate_hourly(metrics.get(logical), start_time, end_time)["values"])


def _classification(
    profile: DatabaseMetricProfile,
    metrics: Mapping[str, dict[str, Any]],
    work: Mapping[str, Any],
    active_threads: Optional[float],
    cpu_p95: Optional[float],
    thresholds: Mapping[str, float],
    risks: Iterable[str],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    missing = [logical for logical in profile.required_metrics if logical not in metrics or not metrics[logical].get("samples")]
    if profile.db_type == "clickhouse" and "work_total" in missing and any(
        logical in metrics and metrics[logical].get("samples")
        for logical in ("insert_work_total", "inserted_rows_total")
    ):
        missing.remove("work_total")
    if missing:
        reasons.append(f"missing required metrics: {','.join(sorted(missing))}")
        return "blocked", reasons

    work_p95 = work.get("p95")
    idle_looking = True
    if work_p95 is None:
        reasons.append("no usable hourly work-rate samples")
        idle_looking = False
    elif work_p95 > thresholds["work_p95_ops_per_sec"]:
        reasons.append(f"work p95 {work_p95:.6g} ops/s is above idle threshold")
        idle_looking = False

    threads_threshold = thresholds.get("active_threads_mean", 1.05)
    if active_threads is not None and active_threads > threads_threshold:
        reasons.append(f"mean active threads {active_threads:.6g} is above idle threshold {threads_threshold:.6g}")
        idle_looking = False
    if cpu_p95 is not None and cpu_p95 > thresholds["cpu_p95_fraction"]:
        reasons.append(f"CPU p95 {cpu_p95:.6g} is above idle threshold")
        idle_looking = False
    zero_ratio = work.get("zero_work_hour_ratio", 0.0)
    if zero_ratio < thresholds["min_zero_work_ratio"]:
        reasons.append(f"zero-work ratio {work.get('zero_work_hour_ratio', 0.0):.2f} is below threshold")
        idle_looking = False
    if work.get("restarts"):
        reasons.append("counter restarts reduce usable work evidence")

    if not idle_looking:
        return "active", reasons

    hours_with_data = work.get("hours_with_data")
    if hours_with_data is not None:
        expected_hours = int(hours_with_data) + int(work.get("data_gaps", 0))
        coverage = int(hours_with_data) / expected_hours if expected_hours else 0.0
        if coverage < thresholds.get("min_data_coverage", 0.0):
            reasons.append(f"data coverage {coverage:.2f} is below threshold; idle evidence is incomplete")
            return "blocked", reasons
    if cpu_p95 is None:
        reasons.append("container CPU evidence is missing")
        return "blocked", reasons

    blocking_risks = {
        "replication_failover_role",
        "role_unknown",
        "primary_with_active_backup",
        "production_environment",
        "ownership_missing",
        "environment_missing",
        "not_cost_review_eligible",
        "backup_schedule_unknown",
        "counter_restarts_observed",
    }
    if any(risk in blocking_risks for risk in risks):
        reasons.append("operational safety evidence blocks shutdown candidacy")
        return "blocked", reasons
    reasons.extend(["hourly work, thread, and CPU thresholds are met", "cost-review eligibility evidence is present"])
    return "candidate", reasons


def _backup_evidence(
    cronjobs: Iterable[Any],
    namespace: str,
    pod_labels: Mapping[str, str],
    role: str,
    profile: DatabaseMetricProfile,
    controller_name: str = "",
) -> tuple[dict[str, Any], bool, bool]:
    """Return backup evidence, whether a schedule is known, and whether a backup is running now."""
    matches: list[dict[str, Any]] = []
    # Only these labels tie a CronJob to this database; the namespace alone does not.
    pod_identity_values = {
        str(pod_labels[key]).lower()
        for key in ("app.kubernetes.io/instance", "app.kubernetes.io/name", "app", "release")
        if pod_labels.get(key)
    }
    if controller_name:
        pod_identity_values.add(controller_name.lower())
    pod_identity_values.add(profile.db_type)
    for cronjob in cronjobs:
        metadata = getattr(cronjob, "metadata", None)
        spec = getattr(cronjob, "spec", None)
        cron_namespace = str(getattr(metadata, "namespace", "") or "")
        if cron_namespace != namespace:
            continue
        labels = dict(getattr(metadata, "labels", None) or {})
        cron_name = str(getattr(metadata, "name", "") or "").lower()
        cron_values = {str(value).lower() for value in labels.values()}
        is_backup = any(term in cron_name or term in cron_values for term in profile.backup_names)
        targets_pod = any(value and (value in cron_name or value in cron_values) for value in pod_identity_values)
        if not (is_backup and targets_pod):
            continue
        last_schedule = getattr(getattr(cronjob, "status", None), "last_schedule_time", None)
        matches.append(
            {
                "name": str(getattr(metadata, "name", "") or ""),
                "schedule": str(getattr(spec, "schedule", "") or ""),
                "suspended": bool(getattr(spec, "suspend", False)),
                "active_jobs": len(getattr(getattr(cronjob, "status", None), "active", None) or []),
                "last_schedule_time": last_schedule.isoformat() if hasattr(last_schedule, "isoformat") else "",
            }
        )
    known_schedule = any(item["schedule"] and not item["suspended"] for item in matches)
    active_backup = any(item["active_jobs"] for item in matches)
    return {"cronjobs": matches, "schedule_known": known_schedule, "active": active_backup}, known_schedule, active_backup


def _clickhouse_pod_identity(pod: Any) -> str:
    metadata = getattr(pod, "metadata", None)
    labels = dict(getattr(metadata, "labels", None) or {})
    values = [str(getattr(metadata, "name", "") or ""), *(f"{key}={value}" for key, value in labels.items())]
    for container in getattr(getattr(pod, "spec", None), "containers", None) or []:
        values.extend([str(getattr(container, "name", "") or ""), str(getattr(container, "image", "") or "")])
    return " ".join(values).lower()


def _clickhouse_is_keeper_pod(pod: Any) -> bool:
    return "keeper" in _clickhouse_pod_identity(pod) or "zookeeper" in _clickhouse_pod_identity(pod)


def _clickhouse_cluster_name(pod: Any) -> str:
    labels = _labels(pod)
    controller_type, controller_name = _controller_context(pod)
    for key in (
        "clickhouse.altinity.com/cluster",
        "clickhouse-keeper.altinity.com/cluster",
        "app.kubernetes.io/cluster",
        "app.kubernetes.io/instance",
    ):
        value = str(labels.get(key, "")).strip()
        if value:
            return value
    return controller_name or "default"


def _clickhouse_topology(pod: Any) -> dict[str, Any]:
    labels = _labels(pod)
    metadata = getattr(pod, "metadata", None)
    match = re.search(r"-(\d+)$", str(getattr(metadata, "name", "") or ""))
    result: dict[str, Any] = {"ordinal": int(match.group(1)) if match else None}
    for key, output in (
        ("clickhouse.altinity.com/shard", "shard"),
        ("clickhouse.altinity.com/replica", "replica"),
        ("clickhouse.altinity.com/cluster", "cluster"),
        ("clickhouse-keeper.altinity.com/shard", "keeper_shard"),
        ("clickhouse-keeper.altinity.com/replica", "keeper_replica"),
        ("clickhouse-keeper.altinity.com/cluster", "keeper_cluster"),
    ):
        if labels.get(key):
            result[output] = str(labels[key])
    return result


def _clickhouse_server_clusters(pods: Iterable[Any]) -> dict[tuple[str, str], list[Any]]:
    clusters: dict[tuple[str, str], list[Any]] = {}
    for pod in pods:
        namespace = str(getattr(getattr(pod, "metadata", None), "namespace", "") or "")
        clusters.setdefault((namespace, _clickhouse_cluster_name(pod)), []).append(pod)
    return clusters


def _clickhouse_keeper_members(
    pods: Iterable[Any],
    server_clusters: Mapping[tuple[str, str], list[Any]],
) -> list[dict[str, Any]]:
    """Return Keeper/ZooKeeper pods attached as protected cluster members."""
    protected: list[dict[str, Any]] = []
    keeper_by_namespace: dict[str, list[Any]] = {}
    for pod in pods:
        namespace = str(getattr(getattr(pod, "metadata", None), "namespace", "") or "")
        if _clickhouse_is_keeper_pod(pod):
            keeper_by_namespace.setdefault(namespace, []).append(pod)
    for namespace in sorted(keeper_by_namespace):
        for pod in keeper_by_namespace[namespace]:
            keeper_cluster = _clickhouse_cluster_name(pod)
            pod_labels = _labels(pod)
            pod_identities = {
                str(pod_labels.get(key, "")).strip().lower()
                for key in ("app.kubernetes.io/instance", "app.kubernetes.io/name", "app.kubernetes.io/part-of", "release")
                if pod_labels.get(key)
            }
            candidates = []
            for (candidate_namespace, cluster_name), members in server_clusters.items():
                if candidate_namespace != namespace:
                    continue
                first_labels = _labels(members[0])
                server_identities = {
                    str(first_labels.get(key, "")).strip().lower()
                    for key in ("app.kubernetes.io/instance", "app.kubernetes.io/name", "app.kubernetes.io/part-of", "release")
                    if first_labels.get(key)
                }
                if cluster_name == keeper_cluster or (pod_identities and pod_identities & server_identities):
                    candidates.append((cluster_name, members))
            if not candidates:
                continue
            controller_type, controller_name = _controller_context(pod)
            protected.append(
                {
                    "namespace": namespace,
                    "pod": str(getattr(getattr(pod, "metadata", None), "name", "") or ""),
                    "controller_name": controller_name,
                    "controller_type": controller_type,
                    "role": "protected_keeper",
                    "topology": _clickhouse_topology(pod),
                }
            )
    return protected


def _clickhouse_member_record(pod: Any) -> dict[str, Any]:
    controller_type, controller_name = _controller_context(pod)
    return {
        "namespace": str(getattr(getattr(pod, "metadata", None), "namespace", "") or ""),
        "pod": str(getattr(getattr(pod, "metadata", None), "name", "") or ""),
        "controller_name": controller_name,
        "controller_type": controller_type,
        "role": "cluster_member",
        "topology": _clickhouse_topology(pod),
    }


def _clickhouse_backup_sidecars(pods: Iterable[Any]) -> list[dict[str, str]]:
    evidence: list[dict[str, str]] = []
    for pod in pods:
        metadata = getattr(pod, "metadata", None)
        for container in getattr(getattr(pod, "spec", None), "containers", None) or []:
            combined = f"{getattr(container, 'name', '') or ''} {getattr(container, 'image', '') or ''}".lower()
            if "backup" in combined or "dump" in combined or "snapshot" in combined:
                evidence.append(
                    {
                        "namespace": str(getattr(metadata, "namespace", "") or ""),
                        "pod": str(getattr(metadata, "name", "") or ""),
                        "container": str(getattr(container, "name", "") or ""),
                        "image": str(getattr(container, "image", "") or ""),
                    }
                )
    return evidence


def _clickhouse_approval_risks(disks: Iterable[Mapping[str, Any]], clusters: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, str]]]:
    object_disks: list[dict[str, str]] = []
    cross_cluster: list[dict[str, str]] = []
    for disk in disks:
        disk_type = str(disk.get("type", "") or "")
        path = str(disk.get("path", "") or "")
        if any(term in f"{disk_type} {path}".lower() for term in ("s3", "gcs", "azure", "object", "bucket")):
            object_disks.append(
                {
                    "name": str(disk.get("name", "") or ""),
                    "type": disk_type,
                    "path": path,
                }
            )
    for cluster in clusters:
        if bool(cluster.get("is_local")):
            continue
        consumer = {
            "cluster": str(cluster.get("cluster", "") or ""),
            "host_name": str(cluster.get("host_name", "") or ""),
            "host_address": str(cluster.get("host_address", "") or ""),
        }
        if consumer not in cross_cluster:
            cross_cluster.append(consumer)
    return {"object_storage_disks": object_disks, "cross_cluster_consumers": cross_cluster}


def _clickhouse_readonly_idle_evidence(namespace: str, pod_name: str) -> dict[str, Any]:
    """Collect bounded, read-only catalog evidence when the exec path is configured."""
    from .clickhouse import _run_clickhouse_query

    evidence: dict[str, Any] = {
        "system_backups": {"status": "unavailable", "reason": "pod/exec or system.backups is unavailable", "rows": []},
        "system_disks": {"status": "unavailable", "reason": "pod/exec or system.disks is unavailable", "rows": []},
        "system_clusters": {"status": "unavailable", "reason": "pod/exec or system.clusters is unavailable", "rows": []},
    }
    for key, template in (
        ("system_backups", "clickhouse_backups.v1"),
        ("system_disks", "clickhouse_disks.v1"),
        ("system_clusters", "clickhouse_topology.v1"),
    ):
        try:
            rows, _ = _run_clickhouse_query(namespace, pod_name, template)
            evidence[key] = {"status": "available", "rows": rows[:20]}
        except Exception as exc:
            evidence[key] = {"status": "unavailable", "reason": str(exc), "rows": []}
    return evidence


def _clickhouse_cluster_work_evidence(
    metrics: Mapping[str, dict[str, Any]],
    profile: DatabaseMetricProfile,
    start_time: datetime,
    end_time: datetime,
    baseline: float,
) -> tuple[Mapping[str, Any], Mapping[str, Any], bool]:
    query_work = _work_evidence({"work_total": metrics.get("work_total")}, profile, start_time, end_time, baseline)
    insert_work = _clickhouse_insert_work_evidence(metrics, start_time, end_time, baseline)
    query_idle = query_work.get("p95") is None or float(query_work.get("p95") or 0.0) <= 0.0
    insert_active = insert_work.get("p95") is not None and float(insert_work.get("p95") or 0.0) > 0.0
    return query_work, insert_work, query_idle and insert_active


def _pvc_evidence(
    claims: Iterable[Any],
    claimed_names: Iterable[str],
) -> dict[str, Any]:
    wanted = set(claimed_names)
    evidence: list[dict[str, str]] = []
    for claim in claims:
        metadata = getattr(claim, "metadata", None)
        name = str(getattr(metadata, "name", "") or "")
        if name not in wanted:
            continue
        evidence.append(
            {
                "name": name,
                "phase": str(getattr(getattr(claim, "status", None), "phase", "") or ""),
                "storage": str((dict(getattr(getattr(claim, "spec", None), "resources", None) or {}).get("requests") or {}).get("storage", "")),
            }
        )
    return {"claims": evidence, "mounted": sorted(wanted)}


def _risk_fields(
    profile: DatabaseMetricProfile,
    namespace: str,
    owner: str,
    environment: str,
    role: str,
    persistent: bool,
    backup_known: bool,
    work_restarts: int,
    eligibility_source: str,
    eligibility_value: str,
    backup_active: bool = False,
) -> list[str]:
    risks: list[str] = []
    if persistent:
        risks.append("persistent_data")
    if not backup_known:
        risks.append("backup_schedule_unknown")
    if role == "replica":
        risks.append("replication_failover_role")
    elif role != "primary":
        # Without an explicit role label a failover-serving replica cannot be ruled out.
        risks.append("role_unknown")
    if backup_active:
        risks.append("primary_with_active_backup")
    if _is_production(namespace, environment):
        risks.append("production_environment")
    if not owner:
        risks.append("ownership_missing")
    if not environment:
        risks.append("environment_missing")
    if work_restarts:
        risks.append("counter_restarts_observed")
    if not eligibility_source or not _is_truthy(eligibility_value):
        risks.append("not_cost_review_eligible")
    return risks


def _filter_metrics(
    metrics: Mapping[str, dict[str, Any]],
    namespace: str,
    pod_name: str,
    container_names: set[str],
) -> dict[str, dict[str, Any]]:
    """Keep only the samples attributable to one namespace/pod."""
    filtered: dict[str, dict[str, Any]] = {}
    for logical, metric in metrics.items():
        filtered[logical] = dict(metric)
        filtered[logical]["samples"] = [
            sample
            for sample in metric.get("samples", [])
            if metric_matches_target(sample.labels, [(namespace, pod_name)], container_names=container_names)
        ]
    return filtered


def _workload_record(
    pod: Any,
    profile: DatabaseMetricProfile,
    metrics: Mapping[str, dict[str, Any]],
    kubernetes_metrics: Mapping[str, dict[str, Any]],
    start_time: datetime,
    end_time: datetime,
    thresholds: Mapping[str, float],
    namespace: str,
    pod_name: str,
    namespace_labels: Mapping[str, Mapping[str, str]],
    claims_by_namespace: Mapping[str, Iterable[Any]],
    backup_cronjobs: Iterable[Any],
    nodes_by_name: Mapping[str, Any],
) -> dict[str, Any]:
    labels = _labels(pod)
    role = _role(labels, profile)
    # Exporter sidecars report their own container name, so every container of
    # the pod is acceptable; namespace and pod name do the attribution.
    container_names = _all_container_names(pod)
    filtered = _filter_metrics(metrics, namespace, pod_name, container_names)
    pod_kubernetes_metrics = _filter_metrics(kubernetes_metrics, namespace, pod_name, container_names)
    work = _work_evidence(filtered, profile, start_time, end_time, thresholds.get("work_baseline_ops_per_sec", 0.0))
    if profile.db_type == "clickhouse":
        insert_work = _clickhouse_insert_work_evidence(filtered, start_time, end_time, thresholds.get("work_baseline_ops_per_sec", 0.0))
        if insert_work["p95"] is not None and (work["p95"] is None or insert_work["p95"] > work["p95"]):
            work = {**work, "p95": insert_work["p95"]}
    active_threads = _active_thread_evidence(filtered, profile, start_time, end_time)
    cpu_p95 = _p95_metric("cpu_request", pod_kubernetes_metrics, start_time, end_time)
    memory_p95 = _p95_metric("memory_request", pod_kubernetes_metrics, start_time, end_time)
    network_sent = _p95_metric("network_sent", pod_kubernetes_metrics, start_time, end_time)
    network_received = _p95_metric("network_received", pod_kubernetes_metrics, start_time, end_time)
    owner, environment = _owner_and_environment(pod, namespace_labels, namespace)
    eligibility_source, eligibility_value = _eligibility(pod)
    claimed_names = _volume_claims(pod)
    persistent = bool(claimed_names)
    controller_type, controller_name = _controller_context(pod)
    backup, backup_known, backup_active = _backup_evidence(backup_cronjobs, namespace, labels, role, profile, controller_name)
    risks = _risk_fields(
        profile,
        namespace,
        owner,
        environment,
        role,
        persistent,
        backup_known,
        int(work.get("restarts", 0)),
        eligibility_source,
        eligibility_value,
        backup_active,
    )
    classification, reasons = _classification(profile, filtered, work, active_threads, cpu_p95, thresholds, risks)
    workload_missing_metrics = sorted(
        logical for logical in profile.required_metrics if not filtered.get(logical, {}).get("samples")
    )
    node = _node_pool_information(pod, nodes_by_name)
    if classification == "candidate":
        recommendation = f"Advisory cost review only. Validate the {profile.exporter_recommendation} metrics with the database operator before any shutdown."
        approvals = ("database_operator", "backup_owner", "service_owner")
    else:
        recommendation = "Do not recommend shutdown. Collect the missing operational evidence or restore production traffic."
        approvals = ("database_operator",)
    workload_has_metrics = any(metric.get("samples") for metric in filtered.values())
    if not workload_has_metrics:
        recommendation = f"Install or enable an existing {profile.exporter_recommendation}; do not use pod/exec as a metrics fallback."
    eligibility_evidence = f"{eligibility_source}={eligibility_value}" if eligibility_source else ""
    return {
        "namespace": namespace,
        "pod": pod_name,
        "db_type": profile.db_type,
        "node_name": node["node_name"],
        "controller_name": controller_name,
        "controller_type": controller_type,
        "labels": labels,
        "metrics": {
            "p95_work_per_second": work.get("p95"),
            "mean_active_threads": active_threads,
            "p95_cpu_fraction": cpu_p95,
            "zero_work_hour_ratio": work.get("zero_work_hour_ratio"),
        },
        "classification": classification,
        "risks": risks,
        "owner": owner,
        "environment": environment,
        "eligibility_evidence": eligibility_evidence,
        "cost_review_eligible": bool(eligibility_source) and _is_truthy(eligibility_value),
        "missing_metrics": workload_missing_metrics,
        "recommendation": recommendation,
        "required_approvals": list(approvals),
        "classification_reasons": reasons,
        "metrics_prerequisite": "missing_exporter" if not workload_has_metrics else "none",
        "data_gaps_hours": work.get("data_gaps", 0),
        "exporters": _exporter_discovery(pod, profile),
        "kubernetes": {
            **node,
            "resource_envelope": _container_envelope(pod, profile.db_type),
            "persistent_volume_claims": _pvc_evidence(claims_by_namespace.get(namespace, []), claimed_names),
            "backup_evidence": backup,
        },
        "kubernetes_metrics": {
            "p95_memory_fraction": memory_p95,
            "p95_network_sent_bytes_per_second": network_sent,
            "p95_network_received_bytes_per_second": network_received,
        },
    }


def _operator_cluster_context() -> dict[str, str]:
    """Cluster facts the Kubernetes API cannot report, supplied by the operator.

    Cost Copilot uses them to match billing rows; without them it never infers
    a GKE distribution or picks between same-named workloads in two clusters.
    """
    context: dict[str, str] = {}
    distribution = os.getenv("DBA_GKE_DISTRIBUTION", "").strip().lower()
    if distribution in {"autopilot", "standard"}:
        context["distribution"] = distribution.capitalize()
    for key, variable in (("cluster", "DBA_GKE_CLUSTER_NAME"), ("cluster_location", "DBA_GKE_CLUSTER_LOCATION")):
        value = os.getenv(variable, "").strip()
        if value:
            context[key] = value
    return context


def _clickhouse_cluster_assessment(workloads: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = [str(workload.get("classification", "blocked")) for workload in workloads]
    if "active" in statuses:
        classification = "active"
        reasons = ["at least one ClickHouse replica has active work evidence"]
    elif statuses and all(status == "candidate" for status in statuses):
        classification = "candidate"
        reasons = ["every shard replica in the ClickHouse cluster meets idle thresholds"]
    else:
        classification = "blocked"
        reasons = sorted(
            {
                reason
                for workload in workloads
                for reason in workload.get("classification_reasons", [])
                if classification != "active" or reason
            }
        ) or ["no ClickHouse replica met the complete candidate evidence"]
    counts = {status: statuses.count(status) for status in sorted(set(statuses))}
    return {
        "classification": classification,
        "classification_reasons": reasons,
        "member_counts": counts,
        "member_classifications": [
            {
                "namespace": workload["namespace"],
                "pod": workload["pod"],
                "classification": workload["classification"],
                "classification_reasons": workload["classification_reasons"],
            }
            for workload in workloads
        ],
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _error_payload(message: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "status": "error",
        "errors": [message],
    }


def _render_human(payload: dict[str, Any]) -> str:
    if payload.get("status") != "ok":
        return "; ".join(str(error) for error in payload.get("errors", ["assessment failed"]))
    lines = ["Database idle-cost assessment (metrics-only; no Kubernetes or database mutation):"]
    workloads = payload.get("data", {}).get("workloads", [])
    if not workloads:
        lines.append("- No matching database pods were found.")
    for workload in workloads:
        lines.extend(
            [
                f"- {workload['namespace']}/{workload['pod']} ({workload['db_type']}): {workload['classification']}",
                f"  - Metrics: {workload['metrics']}",
                f"  - Risks: {', '.join(workload['risks']) or 'none'}",
                f"  - Recommendation: {workload['recommendation']}",
                f"  - Approvals: {', '.join(workload.get('required_approvals', []))}",
            ]
        )
    cluster = payload.get("metadata", {}).get("cluster_assessment", {})
    if payload.get("metadata", {}).get("assessment_unit") == "cluster":
        lines.extend(
            [
                f"- Cluster assessment unit: {payload['metadata'].get('cluster_id', 'unknown')} ({cluster.get('classification', 'unknown')})",
                f"- Cluster ingest-only: {payload['metadata'].get('ingest_only', False)}",
                f"- Protected members: {len(payload['metadata'].get('protected_members', []))}",
            ]
        )
    metadata = payload.get("metadata", {})
    lines.extend(
        [
            f"- Project: {metadata.get('project_id', 'unavailable')}; window: {metadata.get('window_days', 'unavailable')} days.",
            f"- Missing metrics: {', '.join(metadata.get('missing_metrics', [])) or 'none'}.",
            "Recommendations are advisory only and require operator approval before any change.",
        ]
    )
    return "\n".join(lines)


def collect_database_idle_assessment(
    db_type: str,
    namespace: Optional[str] = None,
    pod: Optional[str] = None,
    days: int = 14,
    project_id: Optional[str] = None,
    output_mode: str = "json",
) -> str:
    """Assess database idle cost from Cloud Monitoring and Kubernetes metadata only.

    The function discovers an existing exporter, never installs one, never
    reaches a database over pod/exec, and never reads or accepts credentials.
    """

    try:
        normalized_type = str(db_type or "").strip().lower()
        if normalized_type not in SUPPORTED_DATABASE_TYPES:
            raise ValueError("Unsupported database type: use mysql, mariadb, redis, or valkey.")
        if output_mode not in {"json", "human"}:
            raise ValueError("Invalid output_mode: use 'json' or 'human'.")
        try:
            window_days = int(days)
        except (TypeError, ValueError) as exc:
            raise ValueError("days must be an integer between 1 and 31.") from exc
        minimum_days = minimum_window_days()
        if window_days < minimum_days or window_days > 31:
            raise ValueError("days must be between 1 and 31.")
        if pod and not namespace:
            raise ValueError("A namespace is required when pod is specified.")

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=window_days)
        resolved_project = resolve_metrics_project_id(project_id)
        profile = DATABASE_PROFILES[normalized_type]
        core = _load_kubernetes_client()
        batch = client.BatchV1Api()
        if namespace:
            pods = core.list_namespaced_pod(namespace=namespace).items
        else:
            pods = core.list_pod_for_all_namespaces().items
        matching_pods = [
            item
            for item in pods
            if _matches_profile(item, normalized_type)
            and not (normalized_type == "clickhouse" and _clickhouse_is_keeper_pod(item))
        ]
        if pod:
            matching_pods = [
                item
                for item in matching_pods
                if str(getattr(item.metadata, "name", "")) == pod
                and str(getattr(item.metadata, "namespace", "")) == namespace
            ]
        targets = [
            (str(getattr(item.metadata, "namespace", "") or ""), str(getattr(item.metadata, "name", "") or ""))
            for item in matching_pods
        ]
        container_names: set[str] = set()
        for item in matching_pods:
            container_names.update(_all_container_names(item))
        metrics = read_monitoring_series(
            resolved_project,
            start_time,
            end_time,
            targets,
            profile.aliases,
            profile.kinds,
            container_names=container_names,
        )
        kubernetes_metric_types = {
            "cpu_request": _GKE_CPU_REQUEST,
            "cpu_limit": _GKE_CPU_LIMIT,
            "memory_request": _GKE_MEMORY_REQUEST,
            "memory_limit": _GKE_MEMORY_LIMIT,
            "network_sent": _GKE_NETWORK_SENT,
            "network_received": _GKE_NETWORK_RECEIVED,
        }
        kubernetes_metric_kinds = {"network_sent": "counter", "network_received": "counter"}
        kubernetes_metrics = read_monitoring_series(
            resolved_project,
            start_time,
            end_time,
            targets,
            {},
            kubernetes_metric_kinds,
            known_metric_types=kubernetes_metric_types,
        )

        namespaces = sorted({target[0] for target in targets})
        namespace_labels: dict[str, dict[str, str]] = {}
        for assessed_namespace in namespaces:
            try:
                metadata = getattr(core.read_namespace(assessed_namespace), "metadata", None)
                namespace_labels[assessed_namespace] = dict(getattr(metadata, "labels", None) or {})
            except Exception:
                namespace_labels[assessed_namespace] = {}
        try:
            claims = core.list_persistent_volume_claim_for_all_namespaces().items
        except Exception:
            claims = []
        claims_by_namespace: dict[str, list[Any]] = {}
        for claim in claims:
            claims_by_namespace.setdefault(str(getattr(getattr(claim, "metadata", None), "namespace", "") or ""), []).append(claim)
        try:
            backup_cronjobs = batch.list_cron_job_for_all_namespaces().items
        except Exception:
            backup_cronjobs = []
        try:
            nodes = core.list_node().items
        except Exception:
            nodes = []
        nodes_by_name = {str(getattr(getattr(node, "metadata", None), "name", "") or ""): node for node in nodes}

        thresholds = classification_thresholds()
        workloads = [
            _workload_record(
                item,
                profile,
                metrics,
                kubernetes_metrics,
                start_time,
                end_time,
                thresholds,
                namespace_value,
                pod_name,
                namespace_labels,
                claims_by_namespace,
                backup_cronjobs,
                nodes_by_name,
            )
            for item, (namespace_value, pod_name) in zip(matching_pods, targets, strict=True)
        ]
        cluster_id = ""
        cluster_assessment: dict[str, Any] = {}
        protected_members: list[dict[str, Any]] = []
        cluster_members: list[dict[str, Any]] = []
        ingest_only = False
        cluster_backup: dict[str, Any] = {}
        approval_risks: dict[str, list[dict[str, str]]] = {}
        if normalized_type == "clickhouse":
            server_clusters = _clickhouse_server_clusters(matching_pods)
            cluster_key = next(iter(server_clusters), ("", ""))
            cluster_id = ":".join(cluster_key) if cluster_key != ("", "") else ""
            server_pods = server_clusters.get(cluster_key, [])
            protected_members = _clickhouse_keeper_members(pods, server_clusters)
            cluster_members = [_clickhouse_member_record(item) for item in matching_pods]
            cluster_assessment = _clickhouse_cluster_assessment(workloads)
            query_evidence, insert_evidence, ingest_only = _clickhouse_cluster_work_evidence(
                metrics,
                profile,
                start_time,
                end_time,
                thresholds.get("work_baseline_ops_per_sec", 0.0),
            )
            exec_evidence: dict[str, Any] = {"status": "unavailable", "reason": "no ClickHouse server pod selected", "rows": []}
            if server_pods:
                first = server_pods[0]
                first_namespace = str(getattr(getattr(first, "metadata", None), "namespace", "") or "")
                first_pod = str(getattr(getattr(first, "metadata", None), "name", "") or "")
                exec_evidence = _clickhouse_readonly_idle_evidence(first_namespace, first_pod)
            backup_rows = exec_evidence.get("system_backups", {}).get("rows", []) if isinstance(exec_evidence.get("system_backups"), dict) else []
            disk_rows = exec_evidence.get("system_disks", {}).get("rows", []) if isinstance(exec_evidence.get("system_disks"), dict) else []
            topology_rows = exec_evidence.get("system_clusters", {}).get("rows", []) if isinstance(exec_evidence.get("system_clusters"), dict) else []
            backup_cronjobs_evidence, backup_schedule_known, backup_active = _backup_evidence(
                backup_cronjobs,
                cluster_key[0],
                dict(getattr(getattr(matching_pods[0], "metadata", None), "labels", None) or {}) if matching_pods else {},
                "",
                profile,
                _controller_context(matching_pods[0])[1] if matching_pods else "",
            )
            cluster_backup = {
                **backup_cronjobs_evidence,
                "sidecars": _clickhouse_backup_sidecars(matching_pods),
                "system_backups": {
                    "status": "available" if backup_rows else "unavailable",
                    "rows": backup_rows,
                    "reason": "" if backup_rows else "no recent system.backups rows were returned",
                },
            }
            approval_risks = _clickhouse_approval_risks(disk_rows, topology_rows)
            if not backup_schedule_known and not backup_rows:
                cluster_assessment["classification"] = "blocked"
                cluster_assessment["classification_reasons"] = sorted(
                    set(cluster_assessment["classification_reasons"]) | {"cluster backup schedule is unknown"}
                )
        # Per-workload gaps: a pod with no series is not covered by another pod's series.
        missing_metrics = sorted({logical for workload in workloads for logical in workload["missing_metrics"]})
        exporter_discovered = bool(workloads and any(_exporter_discovery(item, profile) for item in matching_pods))
        all_missing_exporter = bool(workloads) and all(
            workload["metrics_prerequisite"] == "missing_exporter" for workload in workloads
        )
        metrics_prerequisite = "missing_exporter" if all_missing_exporter else "none"
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _now_iso(),
            "status": "ok",
            "data": {"workloads": workloads},
            "metadata": {
                "project_id": resolved_project,
                "window_days": window_days,
                "metrics_source": "cloud_monitoring",
                "missing_metrics": missing_metrics,
                "data_gap_hours": [workload["data_gaps_hours"] for workload in workloads],
                "exporter_discovered": exporter_discovered,
                "exporter_recommendation": profile.exporter_recommendation,
                "metrics_prerequisite": metrics_prerequisite,
                "resolved_metrics": sorted(metrics),
                "pod_count": len(workloads),
                "kubernetes": _operator_cluster_context(),
            },
        }
        if normalized_type == "clickhouse":
            payload["metadata"].update(
                {
                    "assessment_unit": "cluster",
                    "cluster_id": cluster_id,
                    "cluster_members": cluster_members,
                    "protected_members": protected_members,
                    "ingest_only": ingest_only,
                    "cluster_assessment": cluster_assessment,
                    "cluster_work_evidence": {
                        "query": query_evidence,
                        "insert": insert_evidence,
                    },
                    "cluster_backup_evidence": cluster_backup,
                    "approval_risks": approval_risks,
                }
            )
        return json.dumps(payload, sort_keys=True, separators=(",", ":")) if output_mode == "json" else _render_human(payload)
    except Exception as exc:
        payload = _error_payload(str(exc))
        return json.dumps(payload, sort_keys=True, separators=(",", ":")) if output_mode == "json" else _render_human(payload)


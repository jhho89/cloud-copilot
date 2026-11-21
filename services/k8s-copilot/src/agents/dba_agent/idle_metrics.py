from __future__ import annotations

import json
import os
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional

from kubernetes import client, config


SCHEMA_VERSION = "cloud-copilot.dba.idle-cost.v1"
SUPPORTED_DATABASE_TYPES = ("mysql", "mariadb", "redis", "valkey")
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
        if not any(pod == label_pod for _, pod in targets):
            return False
    elif label_namespace:
        if not any(namespace == label_namespace for namespace, _ in targets):
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


def parse_metric_samples(points: Any) -> list[MetricSample]:
    samples: list[MetricSample] = []
    for point in points or []:
        interval = getattr(point, "interval", None)
        value = getattr(point, "value", None)
        end_time = getattr(interval, "end_time", None)
        number = getattr(value, "double_value", None)
        if number is None:
            number = getattr(value, "int64_value", None)
        if number is None:
            number = getattr(value, "distribution_value", None)
        if end_time is None or number is None or not isinstance(number, (int, float)):
            continue
        timestamp = end_time
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if getattr(timestamp, "tzinfo", None) is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        if isinstance(number, (int, float)) and math.isfinite(float(number)):
            samples.append(MetricSample(timestamp, float(number)))
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

    monitoring_v3 = _monitoring_v3_module()
    monitoring_client = load_cloud_monitoring_client()
    interval = monitoring_v3.TimeInterval(
        {
            "start_time": {"seconds": int(start_time.timestamp())},
            "end_time": {"seconds": int(end_time.timestamp())},
        }
    )
    aliases_by_name: dict[str, str] = {}
    for logical, names in aliases.items():
        for name in names:
            aliases_by_name.setdefault(name, logical)

    selected_types: list[str] = []
    descriptor_names = aliases_by_name
    if descriptor_names:
        for descriptor in monitoring_client.list_metric_descriptors(
            request={
                "name": f"projects/{project_id}",
                "filter": f'metric.type = starts_with("{descriptor_prefix}")',
            }
        ):
            metric_type = str(getattr(descriptor, "type", ""))
            parts = metric_type.split("/")
            if len(parts) < 3:
                continue
            logical = aliases_by_name.get(parts[-2])
            if logical and metric_type not in selected_types:
                selected_types.append(metric_type)
    if known_metric_types:
        selected_types.extend(metric_type for metric_type in known_metric_types.values() if metric_type not in selected_types)

    logical_by_type = {value: key for key, value in (known_metric_types or {}).items()}
    metrics_by_logical: dict[str, dict[str, Any]] = {}
    for metric_type in selected_types:
        descriptor_name = metric_type.split("/")[-2] if len(metric_type.split("/")) >= 3 else ""
        logical = logical_by_type.get(metric_type) or aliases_by_name.get(descriptor_name)
        if not logical:
            continue
        series_list = monitoring_client.list_time_series(
            request={
                "name": f"projects/{project_id}",
                "filter": f'metric.type = "{metric_type}"',
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
            samples = parse_metric_samples(getattr(series, "points", None))
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
    values_by_hour: dict[datetime, list[float]] = {}
    restarts = 0
    if kind == "counter":
        previous: Optional[MetricSample] = None
        for sample in samples:
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
            values_by_hour.setdefault(_hour_start(sample.timestamp), []).append(rate)
            previous = sample
    else:
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
    }


def minimum_window_days() -> int:
    try:
        configured = int(os.getenv("DBA_IDLE_ASSESSMENT_MIN_DAYS", "1"))
    except ValueError:
        configured = 1
    return configured if 1 <= configured <= 31 else 1


def _pod_identity(pod: Any) -> str:
    metadata = getattr(pod, "metadata", None)
    spec = getattr(pod, "spec", None)
    values = [str(getattr(metadata, "name", "") or "")]
    labels = dict(getattr(metadata, "labels", None) or {})
    values.extend(str(key) for key in labels)
    values.extend(str(value) for value in labels.values())
    for container in getattr(spec, "containers", None) or []:
        values.extend([str(getattr(container, "name", "") or ""), str(getattr(container, "image", "") or "")])
    return " ".join(values).lower()


def _matches_profile(pod: Any, db_type: str) -> bool:
    identity = _pod_identity(pod)
    if db_type in REDIS_DATABASE_TYPES:
        return db_type in identity
    return db_type in identity


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
    for owner in getattr(getattr(pod, "metadata", None), "owner_references", None) or []:
        if bool(getattr(owner, "controller", False)):
            return str(getattr(owner, "kind", "") or ""), str(getattr(owner, "name", "") or "")
    return "", ""


def _database_containers(pod: Any, db_type: str) -> list[Any]:
    matching = [container for container in getattr(getattr(pod, "spec", None), "containers", None) or [] if _matches_profile({"metadata": None, "spec": {"containers": [container]}}, db_type)]
    if matching:
        return matching
    containers = list(getattr(getattr(pod, "spec", None), "containers", None) or [])
    return containers[:1] if containers else []


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
            "app.kubernetes.io/managed-by",
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
    lowered = namespace.lower()
    if not environment:
        if "prod" in lowered:
            environment = "production"
        elif any(marker in lowered for marker in ("staging", "stage", "dev", "test", "sandbox")):
            environment = "non-production"
    return owner, environment


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


def _is_false(value: str) -> bool:
    return value.strip().lower() in {"not-eligible", "ineligible", "false", "no", "0"}


def _data_gaps(start_time: datetime, end_time: datetime, observed_hours: int) -> int:
    first = _hour_start(start_time)
    expected = int(math.ceil((end_time - first).total_seconds() / 3600.0))
    return max(0, expected - observed_hours)


def _work_evidence(
    metrics: Mapping[str, dict[str, Any]],
    profile: DatabaseMetricProfile,
    start_time: datetime,
    end_time: datetime,
) -> dict[str, Any]:
    hourly_values: list[float] = []
    restarts = 0
    observed_hours = 0
    for logical in profile.work_metrics:
        aggregate = aggregate_hourly(metrics.get(logical), start_time, end_time)
        restarts += int(aggregate["restarts"])
        observed_hours = max(observed_hours, int(aggregate["hours_with_data"]))
        if aggregate["values"]:
            hourly_values = aggregate["values"]
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

    if active_threads is not None and active_threads > thresholds["work_p95_ops_per_sec"]:
        reasons.append(f"active threads p95 {active_threads:.6g} is above idle threshold")
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

    blocking_risks = {
        "replication_failover_role",
        "primary_with_active_backup",
        "production_environment",
        "ownership_missing",
        "environment_missing",
        "not_cost_review_eligible",
        "backup_schedule_unknown",
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
) -> tuple[dict[str, Any], bool, bool]:
    matches: list[dict[str, str]] = []
    haystack = " ".join([namespace, *pod_labels.keys(), *pod_labels.values()]).lower()
    for cronjob in cronjobs:
        metadata = getattr(cronjob, "metadata", None)
        spec = getattr(cronjob, "spec", None)
        cron_namespace = str(getattr(metadata, "namespace", "") or "")
        if cron_namespace != namespace:
            continue
        labels = dict(getattr(metadata, "labels", None) or {})
        cron_haystack = " ".join([str(getattr(metadata, "name", "") or ""), *labels.keys(), *labels.values()]).lower()
        if any(term in cron_haystack for term in profile.backup_names) or "backup" in haystack:
            matches.append(
                {
                    "name": str(getattr(metadata, "name", "") or ""),
                    "schedule": str(getattr(spec, "schedule", "") or ""),
                    "last_schedule_time": str(getattr(getattr(getattr(cronjob, "status", None), "last_schedule_time", None), "isoformat", lambda: "")() or ""),
                }
            )
    known_schedule = bool(matches and any(item["schedule"] for item in matches))
    return {"cronjobs": matches, "schedule_known": known_schedule}, known_schedule, role == "primary"


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
) -> list[str]:
    risks: list[str] = []
    if persistent:
        risks.append("persistent_data")
    if not backup_known:
        risks.append("backup_schedule_unknown")
    if role == "replica":
        risks.append("replication_failover_role")
    if role == "primary" and backup_known:
        risks.append("primary_with_active_backup")
    if "prod" in namespace.lower() or environment.lower() == "production":
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
    container_names = {str(getattr(container, "name", "")) for container in _database_containers(pod, profile.db_type)}
    filtered = {}
    for logical, metric in metrics.items():
        filtered[logical] = dict(metric)
        filtered[logical]["samples"] = [
            sample
            for sample in metric.get("samples", [])
            if metric_matches_target(metric.get("labels", {}), [(namespace, pod_name)], container_names=container_names)
        ]
    work = _work_evidence(filtered, profile, start_time, end_time)
    active_threads = _active_thread_evidence(filtered, profile, start_time, end_time)
    cpu_p95 = _p95_metric("cpu_request", kubernetes_metrics, start_time, end_time)
    memory_p95 = _p95_metric("memory_request", kubernetes_metrics, start_time, end_time)
    network_sent = _p95_metric("network_sent", kubernetes_metrics, start_time, end_time)
    network_received = _p95_metric("network_received", kubernetes_metrics, start_time, end_time)
    owner, environment = _owner_and_environment(pod, namespace_labels, namespace)
    eligibility_source, eligibility_value = _eligibility(pod)
    claimed_names = _volume_claims(pod)
    persistent = bool(claimed_names)
    backup, backup_known, _ = _backup_evidence(backup_cronjobs, namespace, labels, role, profile)
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
    )
    classification, reasons = _classification(profile, filtered, work, active_threads, cpu_p95, thresholds, risks)
    controller_type, controller_name = _controller_context(pod)
    node = _node_pool_information(pod, nodes_by_name)
    if classification == "candidate":
        recommendation = f"Advisory cost review only. Validate the {profile.exporter_recommendation} metrics with the database operator before any shutdown."
        approvals = ("database_operator", "backup_owner", "service_owner")
    else:
        recommendation = "Do not recommend shutdown. Collect the missing operational evidence or restore production traffic."
        approvals = ("database_operator",)
    if not metrics:
        recommendation = f"Install or enable an existing {profile.exporter_recommendation}; do not use pod/exec as a metrics fallback."
    eligibility_evidence = f"{eligibility_source}={eligibility_value}" if eligibility_source else ""
    workload_has_metrics = any(metric.get("samples") for metric in filtered.values())
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
        matching_pods = [item for item in pods if _matches_profile(item, normalized_type)]
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
            container_names.update(str(getattr(container, "name", "")) for container in _database_containers(item, normalized_type))
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

        workloads = [
            _workload_record(
                item,
                profile,
                metrics,
                kubernetes_metrics,
                start_time,
                end_time,
                classification_thresholds(),
                namespace_value,
                pod_name,
                namespace_labels,
                claims_by_namespace,
                backup_cronjobs,
                nodes_by_name,
            )
            for item, (namespace_value, pod_name) in zip(matching_pods, targets, strict=True)
        ]
        missing_metrics = sorted(
            {
                logical
                for workload in workloads
                for logical in profile.required_metrics
                if logical not in metrics or not metrics[logical].get("samples")
            }
        )
        exporter_discovered = bool(workloads and any(_exporter_discovery(item, profile) for item in matching_pods))
        all_missing_exporter = all(workload["metrics_prerequisite"] == "missing_exporter" for workload in workloads)
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
            },
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")) if output_mode == "json" else _render_human(payload)
    except Exception as exc:
        payload = _error_payload(str(exc))
        return json.dumps(payload, sort_keys=True, separators=(",", ":")) if output_mode == "json" else _render_human(payload)


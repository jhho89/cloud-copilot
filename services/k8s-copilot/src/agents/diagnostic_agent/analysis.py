"""Evidence-based analysis of read-only Kubernetes diagnostic snapshots."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2, "info": 3}

CRASHLOOP_REASONS = {"CrashLoopBackOff", "Error", "BackOff"}
IMAGE_PULL_REASONS = {"ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError"}
PROBE_REASONS = {"Unhealthy", "ProbeWarning", "ProbeFailed"}
NODE_PRESSURE_CONDITIONS = {"MemoryPressure", "DiskPressure", "PIDPressure"}


@dataclass(frozen=True)
class Finding:
    """One prioritized diagnostic result and the API evidence behind it."""

    category: str
    title: str
    severity: str
    explanation: str
    evidence: list[str]
    additional_evidence: list[str]


def _value(container: Any, name: str, default: Any = None) -> Any:
    if container is None:
        return default
    return getattr(container, name, default)


def _namespace_name(metadata: Any) -> tuple[str, str]:
    return _value(metadata, "namespace", "") or "", _value(metadata, "name", "") or ""


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return None


def _format_time(value: Any) -> str:
    parsed = _timestamp(value)
    if not parsed:
        return "unknown time"
    return parsed.isoformat()


def _dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value is None:
        return {}
    try:
        return dict(value)
    except (TypeError, ValueError):
        return {}


def _quantity_millicores(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("m"):
            return float(text[:-1])
        return float(text) * 1000
    except ValueError:
        return None


def _quantity_bytes(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    multipliers = {
        "KI": 1024,
        "K": 1000,
        "MI": 1024**2,
        "M": 1000**2,
        "GI": 1024**3,
        "G": 1000**3,
        "TI": 1024**4,
        "T": 1000**4,
    }
    for suffix, multiplier in multipliers.items():
        if text.endswith(suffix):
            try:
                return float(text[: -len(suffix)]) * multiplier
            except ValueError:
                return None
    try:
        return float(text)
    except ValueError:
        return None


def _format_bytes(value: float | None) -> str:
    if value is None:
        return "unknown"
    for suffix, divisor in (("Gi", 1024**3), ("Mi", 1024**2), ("Ki", 1024)):
        if abs(value) >= divisor:
            return f"{value / divisor:.1f}{suffix}"
    return f"{value:.0f}B"


def _event_is_after(event: Any, when: datetime | None) -> bool:
    if not when:
        return False
    event_time = _timestamp(_value(event, "last_timestamp")) or _timestamp(
        _value(event, "event_time")
    )
    first_time = _timestamp(_value(event, "first_timestamp"))
    return any(value and value >= when for value in (event_time, first_time))


def _event_line(event: Any) -> str:
    at = _value(event, "last_timestamp") or _value(event, "event_time") or _value(
        event, "first_timestamp"
    )
    kind = _value(event, "type", "Unknown")
    reason = _value(event, "reason", "Unknown")
    message = _value(event, "message", "") or ""
    count = _value(event, "count")
    count_text = f" (count={count})" if count else ""
    return f"{_format_time(at)} {kind}/{reason}{count_text}: {message}"


def _pod_conditions(pod: Any) -> dict[str, tuple[str, str, str]]:
    result: dict[str, tuple[str, str, str]] = {}
    for condition in _value(_value(pod, "status"), "conditions", []) or []:
        result[_value(condition, "type", "")] = (
            str(_value(condition, "status", "")),
            _value(condition, "reason", "") or "",
            _value(condition, "message", "") or "",
        )
    return result


def _node_name(pod: Any) -> str:
    return _value(_value(pod, "spec"), "node_name", "") or ""


def _resource_requests(pod: Any) -> tuple[float | None, float | None]:
    cpu = 0.0
    memory = 0.0
    seen_cpu = False
    seen_memory = False
    for container in _value(_value(pod, "spec"), "containers", []) or []:
        requests = _dict(_value(_value(container, "resources"), "requests"))
        cpu_value = _quantity_millicores(requests.get("cpu"))
        memory_value = _quantity_bytes(requests.get("memory"))
        if cpu_value is not None:
            cpu += cpu_value
            seen_cpu = True
        if memory_value is not None:
            memory += memory_value
            seen_memory = True
    return (cpu if seen_cpu else None, memory if seen_memory else None)


def _pod_container_statuses(pod: Any) -> list[Any]:
    return _value(_value(pod, "status"), "container_statuses", []) or []


def _pod_total_restarts(pod: Any) -> int:
    return sum(_value(status, "restart_count", 0) or 0 for status in _pod_container_statuses(pod))


def _pod_state_line(pod: Any) -> str:
    parts: list[str] = []
    for status in _pod_container_statuses(pod):
        name = _value(status, "name", "?")
        restarts = _value(status, "restart_count", 0) or 0
        state = _value(status, "state")
        waiting = _value(state, "waiting")
        terminated = _value(state, "terminated")
        running = _value(state, "running")
        if waiting:
            parts.append(
                f"{name}: waiting={_value(waiting, 'reason', '?')} "
                f"message={_value(waiting, 'message', '') or 'none'} restarts={restarts}"
            )
        elif terminated:
            parts.append(
                f"{name}: terminated={_value(terminated, 'reason', '?')} "
                f"exit_code={_value(terminated, 'exit_code', '?')} restarts={restarts}"
            )
        elif running:
            parts.append(f"{name}: running restarts={restarts}")
        else:
            parts.append(f"{name}: state unknown restarts={restarts}")
    if not parts:
        return f"container statuses: none; phase={_value(_value(pod, 'status'), 'phase', 'unknown')}"
    return "container statuses: " + "; ".join(parts)


def _log_tail(logs: str | None, limit: int = 3) -> list[str]:
    if not logs:
        return []
    lines = [line.strip() for line in logs.splitlines() if line.strip()]
    return lines[-limit:]


def _pod_findings(
    pod: Any,
    events: list[Any],
    logs: str | None,
    metric: Mapping[str, Any] | None,
    nodes_by_name: Mapping[str, Any],
) -> list[Finding]:
    findings: list[Finding] = []
    namespace, name = _namespace_name(_value(pod, "metadata"))
    key = f"{namespace}/{name}"
    phase = _value(_value(pod, "status"), "phase", "Unknown")
    conditions = _pod_conditions(pod)
    base_evidence = [f"pod phase={phase}", _pod_state_line(pod)]
    event_lines = [_event_line(event) for event in sorted(events, key=_event_line)]
    ready_condition = conditions.get("Ready", ("Unknown", "", ""))

    if event_lines:
        base_evidence.extend(event_lines[-5:])
    if metric:
        base_evidence.append(
            f"metrics API evidence: cpu={metric.get('cpu_millicores', 'unknown')}m "
            f"memory={_format_bytes(metric.get('memory_bytes'))}"
        )
    if conditions:
        condition_lines = [
            f"{condition_type}={values[0]} reason={values[1] or 'none'}"
            for condition_type, values in conditions.items()
        ]
        base_evidence.append("pod conditions: " + "; ".join(condition_lines))
    log_lines = _log_tail(logs)
    if log_lines:
        base_evidence.extend(f"log: {line}" for line in log_lines)

    oom_terminated: Any | None = None
    waiting_states: list[tuple[Any, Any]] = []
    terminated_states: list[tuple[Any, Any]] = []
    for status in _pod_container_statuses(pod):
        state = _value(status, "state")
        waiting = _value(state, "waiting")
        terminated = _value(state, "terminated")
        last_terminated = _value(state, "last_state", None)
        last_terminated = _value(last_terminated, "terminated") if last_terminated else None
        if waiting:
            waiting_states.append((status, waiting))
        if terminated:
            terminated_states.append((status, terminated))
        if terminated and _value(terminated, "reason") == "OOMKilled":
            oom_terminated = terminated
        elif last_terminated and _value(last_terminated, "reason") == "OOMKilled":
            oom_terminated = last_terminated

    if oom_terminated:
        evidence = list(base_evidence)
        limits = [
                _quantity_bytes(_dict(_value(_value(container, "resources"), "limits")).get("memory"))
            for container in _value(_value(pod, "spec"), "containers", []) or []
        ]
        limits = [value for value in limits if value is not None]
        if limits:
            evidence.append(f"configured memory limit evidence: {[_format_bytes(v) for v in limits]}")
        if metric:
            evidence.append(
                f"current memory usage evidence: {_format_bytes(metric.get('memory_bytes'))}"
            )
        for node_name in filter(None, [_node_name(pod)]):
            node = nodes_by_name.get(node_name)
            node_conditions = {
                _value(condition, "type"): str(_value(condition, "status"))
                for condition in _value(_value(node, "status"), "conditions", []) or []
            }
            if node_conditions.get("MemoryPressure") == "True":
                evidence.append(f"node {node_name} reports MemoryPressure=True")
        findings.append(
            Finding(
                category="pod_failure",
                title=f"{key}: container terminated as OOMKilled",
                severity="high",
                explanation=(
                    "The runtime reports an OOM kill; this is direct evidence that at least one "
                    "container was terminated for exceeding its memory limit or available node "
                    "memory. Correlate usage, limits, and node pressure before changing resources."
                ),
                evidence=evidence,
                additional_evidence=[
                    "Previous container termination reason and exit code",
                    "Recent memory metrics and node memory pressure",
                    "Current logs for the last failed container",
                ],
            )
        )

    crash_waiting = any(_value(waiting, "reason") in CRASHLOOP_REASONS for _, waiting in waiting_states)
    crash_terminated = any(
        (_value(terminated, "exit_code") not in (0, None))
        for _, terminated in terminated_states
    )
    if crash_waiting or (crash_terminated and _pod_total_restarts(pod) > 0):
        evidence = list(base_evidence)
        findings.append(
            Finding(
                category="pod_failure",
                title=f"{key}: crash loop evidence",
                severity="high",
                explanation=(
                    "Container restart state and non-zero exits are consistent with a crashing "
                    "workload. Event and log timestamps can show whether restarts follow the same "
                    "failure."
                ),
                evidence=evidence,
                additional_evidence=[
                    "Startup and termination timestamps for repeated crashes",
                    "Previous-container logs",
                    "Rollout history if the failure is recent",
                ],
            )
        )

    pull_waiting = any(_value(waiting, "reason") in IMAGE_PULL_REASONS for _, waiting in waiting_states)
    pull_events = [event for event in events if _value(event, "reason") in {"Failed", "FailedPull", "BackOff"}]
    if pull_waiting or pull_events:
        evidence = list(base_evidence)
        if metric is not None and not metric:
            evidence.append("metrics are absent for this pod, which supports that it has not run")
        findings.append(
            Finding(
                category="image_pull",
                title=f"{key}: image pull or container configuration failure",
                severity="high",
                explanation=(
                    "The observed waiting state or pull events show the kubelet did not create a "
                    "running container. They do not prove whether the cause is image identity, "
                    "registry access, or configuration without those specific records."
                ),
                evidence=evidence,
                additional_evidence=[
                    "Full image reference and pull-secret visibility from the event message",
                    "Registry error or image manifest access",
                    "Container config error event if present",
                ],
            )
        )

    probe_events = [event for event in events if _value(event, "reason") in PROBE_REASONS]
    unready = any(not bool(_value(status, "ready", False)) for status in _pod_container_statuses(pod))
    if phase == "Running" and probe_events:
        severity = "high" if ready_condition[0] == "False" else "medium"
        findings.append(
            Finding(
                category="probe_failure",
                title=f"{key}: container probe failures",
                severity=severity,
                explanation=(
                    "Probe events show that Kubernetes observed failed health checks. The probe "
                    "message and logs identify the failing endpoint or process state."
                ),
                evidence=base_evidence,
                additional_evidence=[
                    "Probe path, port, delay, and threshold from the pod spec",
                    "Logs around the first and latest probe failure",
                    "Service endpoint readiness if traffic is affected",
                ],
            )
        )
    elif phase == "Running" and unready and ready_condition[0] == "False":
        findings.append(
            Finding(
                category="probe_failure",
                title=f"{key}: pod not Ready while running",
                severity="medium",
                explanation=(
                    "The Ready condition is False even though at least one container is running; "
                    "this is evidence of failed readiness or startup gating."
                ),
                evidence=base_evidence,
                additional_evidence=[
                    "Readiness and startup probe definitions",
                    "Recent probe events and application logs",
                ],
            )
        )

    if phase == "Pending":
        evidence = list(base_evidence)
        scheduling_events = [
            event
            for event in events
            if _value(event, "reason") in {"FailedScheduling", "SchedulingGated", "UnexpectedAdmissionError"}
        ]
        if scheduling_events:
            evidence.extend(_event_line(event) for event in scheduling_events)
        requests = _resource_requests(pod)
        if requests[0] is not None:
            evidence.append(f"pod CPU request evidence: {requests[0]:.0f}m")
        if requests[1] is not None:
            evidence.append(f"pod memory request evidence: {_format_bytes(requests[1])}")
        node_selector = _dict(_value(_value(pod, "spec"), "node_selector"))
        if node_selector:
            evidence.append(f"nodeSelector: {node_selector}")
        matching_nodes: list[str] = []
        for node_name, node in nodes_by_name.items():
            labels = _dict(_value(_value(node, "metadata"), "labels"))
            if node_selector and all(labels.get(key) == value for key, value in node_selector.items()):
                matching_nodes.append(node_name)
        if node_selector:
            evidence.append(
                f"visible nodes matching nodeSelector: {matching_nodes or 'none'}"
            )
        severity = "medium"
        title = f"{key}: pod remains Pending"
        explanation = (
            "The pod has not reached a node. Scheduler events, resource requests, and node "
            "constraints identify the actual gate."
        )
        if any(
            _value(event, "reason") == "FailedScheduling"
            or "Unschedulable" in str(_value(event, "message") or "")
            for event in scheduling_events
        ):
            severity = "high"
            title = f"{key}: scheduling failure"
            explanation = (
                "The scheduler explicitly reported the pod as Unschedulable. Use its message to "
                "distinguish capacity, affinity, taints, or admission gates."
            )
        findings.append(
            Finding(
                category="scheduling",
                title=title,
                severity=severity,
                explanation=explanation,
                evidence=evidence,
                additional_evidence=[
                    "Node allocatable and current allocation",
                    "Node taints versus pod tolerations",
                    "ResourceQuota status if pod creation is rejected",
                ],
            )
        )

    volume_events = [
        event
        for event in events
        if any(
            phrase in str(_value(event, "message") or "").lower()
            for phrase in ("persistentvolume", "pvc", "volume", "mount", "attach")
        )
    ]
    volume_waiting = any(
        _value(waiting, "reason") in {"ContainerCreating", "CreateContainerError"}
        for _, waiting in waiting_states
    )
    if volume_events and (volume_waiting or phase in {"Pending", "ContainerCreating"}):
        findings.append(
            Finding(
                category="storage",
                title=f"{key}: possible PVC or volume mount issue",
                severity="high",
                explanation=(
                    "Volume-related events coincide with a pod that has not reached a normal running "
                    "state. The PVC phase and volume events show whether provisioning, attachment, "
                    "or mounting is the relevant stage."
                ),
                evidence=base_evidence,
                additional_evidence=[
                    "PVC status, storage class, and access modes",
                    "PersistentVolume events and node attachment state",
                    "CSI driver events if the messages reference one",
                ],
            )
        )

    return findings


def _pvc_findings(pvcs: Iterable[Any]) -> list[Finding]:
    findings: list[Finding] = []
    for pvc in pvcs:
        namespace, name = _namespace_name(_value(pvc, "metadata"))
        phase = _value(_value(pvc, "status"), "phase", "Unknown")
        if phase in {"Pending", "Lost"}:
            evidence = [f"PVC phase={phase}"]
            conditions = _value(_value(pvc, "status"), "conditions", []) or []
            for condition in conditions:
                evidence.append(
                    f"condition={_value(condition, 'type')} status={_value(condition, 'status')} "
                    f"message={_value(condition, 'message') or 'none'}"
                )
            findings.append(
                Finding(
                    category="storage",
                    title=f"{namespace}/{name}: PVC is {phase}",
                    severity="high",
                    explanation=(
                        "The volume claim is not Bound, so workloads depending on it may remain in "
                        "Pending or ContainerCreating until the volume lifecycle completes."
                    ),
                    evidence=evidence,
                    additional_evidence=[
                        "StorageClass and provisioner events",
                        "PersistentVolume capacity and access modes",
                        "CSI driver events",
                    ],
                )
            )
    return findings


def _node_pressure_findings(nodes: Iterable[Any]) -> list[Finding]:
    findings: list[Finding] = []
    for node in nodes:
        name = _value(_value(node, "metadata"), "name", "?")
        conditions = _value(_value(node, "status"), "conditions", []) or []
        pressure = [
            condition
            for condition in conditions
            if _value(condition, "type") in NODE_PRESSURE_CONDITIONS
            and str(_value(condition, "status")).lower() == "true"
        ]
        if pressure:
            evidence = [
                f"node {_value(condition, 'type')}={_value(condition, 'status')} "
                f"reason={_value(condition, 'reason') or 'none'} "
                f"message={_value(condition, 'message') or 'none'}"
                for condition in pressure
            ]
            findings.append(
                Finding(
                    category="node_pressure",
                    title=f"{name}: node reports active pressure",
                    severity="high",
                    explanation=(
                        "A kubelet node condition is reporting memory, disk, or PID pressure. This "
                        "can affect scheduling and eviction; correlate it with pod failures on the "
                        "same node."
                    ),
                    evidence=evidence,
                    additional_evidence=[
                        "Node events and eviction records",
                        "Pods assigned to the affected node",
                        "Node resource metrics if the metrics API is available",
                    ],
                )
            )
    return findings


def _rollout_findings(
    pods: Iterable[Any],
    replica_sets: Iterable[Any],
    deployment_conditions: Iterable[Any] = (),
    events: Iterable[Any] = (),
    deployment_name: str | None = None,
) -> list[Finding]:
    replica_sets = sorted(
        replica_sets,
        key=lambda rs: _timestamp(_value(_value(rs, "metadata"), "creation_timestamp"))
        or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    if not replica_sets:
        if deployment_conditions:
            return [
                Finding(
                    category="rollout_correlation",
                    title="Rollout status is incomplete but no ReplicaSet is visible",
                    severity="low",
                    explanation=(
                        "Deployment conditions indicate rollout progress, but ReplicaSet history is "
                        "not available in this snapshot; timing correlation is therefore partial."
                    ),
                    evidence=[
                        f"condition={_value(condition, 'type')} status={_value(condition, 'status')} "
                        f"message={_value(condition, 'message') or 'none'}"
                        for condition in deployment_conditions
                    ],
                    additional_evidence=[
                        "ReplicaSet ownership and creation timestamps",
                        "Deployment revision annotations and events",
                    ],
                )
            ]
        return []
    newest = replica_sets[0]
    newest_time = _timestamp(_value(_value(newest, "metadata"), "creation_timestamp"))
    if not newest_time:
        return []
    annotations = _dict(_value(_value(newest, "metadata"), "annotations"))
    revision = annotations.get("deployment.kubernetes.io/revision", "unknown")
    affected = []
    for pod in pods:
        owner_name = ""
        for owner in _value(_value(pod, "metadata"), "owner_references", []) or []:
            if _value(owner, "kind") == "ReplicaSet":
                owner_name = _value(owner, "name", "")
                break
        if owner_name and owner_name != _value(_value(newest, "metadata"), "name"):
            continue
        namespace, name = _namespace_name(_value(pod, "metadata"))
        if _timestamp(_value(_value(pod, "metadata"), "creation_timestamp")):
            affected.append(f"{namespace}/{name}")

    # Deployment status conditions are usually on the Deployment object, not ReplicaSets.
    deployment_evidence = [
        f"condition={_value(condition, 'type')} status={_value(condition, 'status')} "
        f"message={_value(condition, 'message') or 'none'}"
        for condition in deployment_conditions
    ]
    if affected or deployment_evidence:
        affected_keys = set(affected)
        rollout_event_evidence = []
        for event in events:
            if _value(event, "type") != "Warning":
                continue
            involved = _value(event, "involved_object")
            event_key = (
                f"{_value(involved, 'namespace', '')}/{_value(involved, 'name', '')}"
            )
            if event_key in affected_keys:
                rollout_event_evidence.append(_event_line(event))
        rollout_event_evidence = rollout_event_evidence[-5:]
        finding = Finding(
            category="rollout_correlation",
            title=(
                f"Recent rollout (revision {revision}) overlaps current failure evidence"
                if affected
                else f"Rollout {deployment_name or 'workload'} has incomplete status conditions"
            ),
            severity="medium" if affected else "low",
            explanation=(
                "The newest ReplicaSet creation time overlaps failing pods or incomplete rollout "
                "conditions. This is timing correlation, not proof that the rollout caused the "
                "failure; compare the revision's template and prior revision when investigating."
            ),
            evidence=[
                (
                    f"newest ReplicaSet {_value(_value(newest, 'metadata'), 'name')} "
                    f"created={_format_time(newest_time)} revision={revision}"
                ),
                f"failing pods belonging to the newest ReplicaSet: {affected or 'none visible'}",
                *rollout_event_evidence,
                *(
                    [
                        f"condition={_value(condition, 'type')} "
                        f"status={_value(condition, 'status')} "
                        f"reason={_value(condition, 'reason') or 'none'} "
                        f"message={_value(condition, 'message') or 'none'}"
                        for condition in deployment_conditions
                    ]
                    if affected
                    else deployment_evidence
                ),
            ],
            additional_evidence=[
                "Pod template diff between newest and prior ReplicaSet",
                "Deployment revision events around the change",
                "Replica scaling and pod creation timestamps",
            ],
        )
        return [finding]
    return []


def _pod_matches_policy(pod: Any, policy: Any) -> bool:
    labels = _dict(_value(_value(pod, "metadata"), "labels"))
    selector = _dict(_value(policy, "spec", None) and _value(_value(policy, "spec"), "pod_selector"))
    match_labels = _dict(selector.get("match_labels"))
    return all(labels.get(key) == value for key, value in match_labels.items())


def _connectivity_findings(
    services: Iterable[Any],
    endpoints: Iterable[Any],
    pods: Iterable[Any],
    ingresses: Iterable[Any],
    network_policies: Iterable[Any],
    dns_services: Iterable[Any],
    include_dns: bool = True,
) -> list[Finding]:
    findings: list[Finding] = []
    pods = list(pods)
    services = list(services)
    endpoints_by_name = {
        _value(_value(endpoint, "metadata"), "name"): endpoint for endpoint in endpoints
    }
    dns_ready = False
    for dns_service in dns_services:
        dns_name = _value(_value(dns_service, "metadata"), "name", "")
        dns_endpoint = endpoints_by_name.get(dns_name)
        if dns_endpoint:
            subsets = _value(dns_endpoint, "subsets", []) or []
            for subset in subsets:
                addresses = _value(subset, "addresses", []) or []
                if addresses:
                    dns_ready = True
                    break
    if include_dns and not dns_ready:
        findings.append(
            Finding(
                category="dns",
                title="No ready cluster DNS endpoint is visible",
                severity="medium",
                explanation=(
                    "The Kubernetes API shows no ready endpoint behind the known cluster DNS "
                    "services. This is endpoint evidence, not proof that a workload's DNS query "
                    "failed; actual resolution must be measured by a client probe or DNS logs."
                ),
                evidence=[
                    "No ready addresses found for cluster DNS services from API-visible endpoints"
                ],
                additional_evidence=[
                    "DNS Deployment and CoreDNS logs",
                    "A read-only client DNS lookup from the affected workload context",
                ],
            )
        )

    for service in services:
        namespace, name = _namespace_name(_value(service, "metadata"))
        selector = _dict(_value(service, "spec") and _value(service, "spec").selector)
        endpoint = endpoints_by_name.get(name)
        ready_addresses = 0
        total_addresses = 0
        if endpoint:
            for subset in _value(endpoint, "subsets", []) or []:
                ready_addresses += len(_value(subset, "addresses", []) or [])
                total_addresses += len(_value(subset, "not_ready_addresses", []) or [])
        total_addresses += ready_addresses
        if selector and ready_addresses == 0:
            matching = []
            for pod in pods:
                pod_labels = _dict(_value(_value(pod, "metadata"), "labels"))
                if all(pod_labels.get(key) == value for key, value in selector.items()):
                    phase = _value(_value(pod, "status"), "phase", "Unknown")
                    pod_namespace, pod_name = _namespace_name(_value(pod, "metadata"))
                    matching.append(f"{pod_namespace}/{pod_name} phase={phase}")
            findings.append(
                Finding(
                    category="service",
                    title=f"{namespace}/{name}: service has no ready endpoints",
                    severity="high",
                    explanation=(
                        "The Service has a selector but the API-visible Endpoints object exposes no "
                        "ready addresses. This explains failed in-cluster service access without "
                        "identifying why pods are not Ready."
                    ),
                    evidence=[
                        f"service selector={selector}",
                        f"ready endpoints={ready_addresses}, total addresses={total_addresses}",
                        f"selector-matching pods: {matching or 'none visible'}",
                    ],
                    additional_evidence=[
                        "Pod readiness and probe events for selected pods",
                        "Service targetPort versus containerPort",
                        "Recent Service and Endpoints events",
                    ],
                )
            )

    for ingress in ingresses:
        namespace, name = _namespace_name(_value(ingress, "metadata"))
        backend_names = []
        rules = _value(ingress, "spec", None) and _value(_value(ingress, "spec"), "rules", []) or []
        for rule in rules:
            http = _value(rule, "http")
            for path in _value(http, "paths", []) or []:
                backend = _value(path, "backend")
                service_backend = _value(backend, "service")
                backend_name = _value(service_backend, "name")
                if backend_name:
                    backend_names.append(backend_name)
        for backend_name in backend_names:
            service = next(
                (
                    item
                    for item in services
                    if _value(_value(item, "metadata"), "name") == backend_name
                ),
                None,
            )
            endpoint = endpoints_by_name.get(backend_name)
            ready = False
            if endpoint:
                ready = any(
                    _value(subset, "addresses", []) for subset in _value(endpoint, "subsets", []) or []
                )
            if not service or not ready:
                findings.append(
                    Finding(
                        category="ingress",
                        title=f"{namespace}/{name}: ingress backend is not ready",
                        severity="high",
                        explanation=(
                            "The Ingress routes to a Service that is missing or has no ready "
                            "addresses in the observed API snapshot. This is routing evidence; the "
                            "underlying workload still needs diagnosis."
                        ),
                        evidence=[
                            f"ingress backend={backend_name}",
                            f"service exists={bool(service)}, ready endpoints={ready}",
                        ],
                        additional_evidence=[
                            "Ingress controller events and status",
                            "TLS secret existence if the Ingress references TLS",
                            "Service and pod readiness",
                        ],
                    )
                )

    for policy in network_policies:
        matched_pods = [pod for pod in pods if _pod_matches_policy(pod, policy)]
        if matched_pods:
            namespace, name = _namespace_name(_value(policy, "metadata"))
            pod_names = [
                f"{_value(_value(pod, 'metadata'), 'namespace')}/{_value(_value(pod, 'metadata'), 'name')}"
                for pod in matched_pods
            ]
            findings.append(
                Finding(
                    category="network_policy",
                    title=f"{namespace}/{name}: NetworkPolicy selects observed pods",
                    severity="info",
                    explanation=(
                        "A NetworkPolicy podSelector matches the workload labels. Policy presence "
                        "does not prove that traffic is blocked; include ingress/egress rules and "
                        "client labels when evaluating connectivity."
                    ),
                    evidence=[
                        f"matching pods: {pod_names}",
                        (
                            f"policy has ingress={bool(_value(_value(policy, 'spec'), 'ingress'))}, "
                            f"egress={bool(_value(_value(policy, 'spec'), 'egress'))}"
                        ),
                    ],
                    additional_evidence=[
                        "Full NetworkPolicy ingress, egress, and namespace selectors",
                        "CNI controller events or logs",
                        "A controlled connectivity probe",
                    ],
                )
            )
    return findings


def _quota_findings(quotas: Iterable[Any], events: Iterable[Any]) -> list[Finding]:
    findings: list[Finding] = []
    quota_events = [
        event for event in events if _value(event, "reason") in {"ResourceQuotaExceeded", "FailedCreate"}
    ]
    for quota in quotas:
        namespace, name = _namespace_name(_value(quota, "metadata"))
        hard = _dict(_value(_value(quota, "status"), "hard"))
        used = _dict(_value(_value(quota, "status"), "used"))
        exceeded = []
        for resource, hard_value in hard.items():
            if resource not in used or hard_value in (None, ""):
                continue
            try:
                if float(used[resource]) >= float(hard_value):
                    exceeded.append(f"{resource}: used={used[resource]} hard={hard_value}")
            except (TypeError, ValueError):
                continue
        if exceeded:
            findings.append(
                Finding(
                    category="resource_quota",
                    title=f"{namespace}/{name}: ResourceQuota is exhausted",
                    severity="high",
                    explanation=(
                        "The ResourceQuota status reports usage at or above its hard limit. If "
                        "creation events match this timing, quota admission is supported as a "
                        "failure contributor."
                    ),
                    evidence=[f"quota status: {exceeded}", *[_event_line(e) for e in quota_events]],
                    additional_evidence=[
                        "API creation-rejection events for the affected ReplicaSet",
                        "Namespace scope and quota resource names",
                    ],
                )
            )
    if quota_events and not findings:
        findings.append(
            Finding(
                category="resource_quota",
                title="Quota-related creation failure events are visible",
                severity="medium",
                explanation=(
                    "Creation-failure events mention quota or admission, but the visible quota "
                    "status does not show an exhausted hard limit in this snapshot."
                ),
                evidence=[_event_line(event) for event in quota_events],
                additional_evidence=["All ResourceQuota objects in the affected namespace"],
            )
        )
    return findings


def diagnose_failures(
    pods: Iterable[Any],
    events: Iterable[Any],
    nodes: Iterable[Any],
    pod_logs: Mapping[str, str],
    pod_metrics: Mapping[str, Mapping[str, Any]],
    pvcs: Iterable[Any] = (),
    replica_sets: Iterable[Any] = (),
    deployment_conditions: Iterable[Any] = (),
) -> list[Finding]:
    """Produce prioritized findings from API snapshots, without calling Kubernetes."""
    pods = list(pods)
    events = list(events)
    nodes = list(nodes)
    nodes_by_name = {
        _value(_value(node, "metadata"), "name"): node for node in nodes
    }
    events_by_pod: dict[str, list[Any]] = {}
    for event in events:
        involved = _value(event, "involved_object")
        involved_name = _value(involved, "name", "")
        involved_namespace = _value(involved, "namespace", "")
        events_by_pod.setdefault(f"{involved_namespace}/{involved_name}", []).append(event)

    findings: list[Finding] = []
    failed_pods: list[Any] = []
    for pod in pods:
        namespace, name = _namespace_name(_value(pod, "metadata"))
        key = f"{namespace}/{name}"
        findings.extend(
            _pod_findings(
                pod,
                events_by_pod.get(key, []),
                pod_logs.get(key),
                pod_metrics.get(key),
                nodes_by_name,
            )
        )
        if any(
            finding.category in {"pod_failure", "image_pull", "probe_failure", "scheduling", "storage"}
            and finding.title.startswith(key)
            for finding in findings
        ):
            failed_pods.append(pod)
    findings.extend(_pvc_findings(pvcs))
    findings.extend(_node_pressure_findings(nodes))
    findings.extend(
        _rollout_findings(
            failed_pods,
            replica_sets,
            deployment_conditions,
            events,
        )
    )
    return sort_findings(findings)


def diagnose_connectivity(
    services: Iterable[Any],
    endpoints: Iterable[Any],
    pods: Iterable[Any],
    ingresses: Iterable[Any],
    network_policies: Iterable[Any],
    dns_services: Iterable[Any],
    include_dns: bool = True,
) -> list[Finding]:
    """Analyze Service, Ingress, DNS endpoint, and NetworkPolicy signals."""
    return sort_findings(
        _connectivity_findings(
            services,
            endpoints,
            pods,
            ingresses,
            network_policies,
            dns_services if include_dns else [],
            include_dns,
        )
    )


def diagnose_quotas(quotas: Iterable[Any], events: Iterable[Any]) -> list[Finding]:
    return sort_findings(_quota_findings(quotas, events))


def sort_findings(findings: Iterable[Finding]) -> list[Finding]:
    """Order findings by severity, category, and stable title."""
    unique: dict[tuple[str, str], Finding] = {}
    for finding in findings:
        unique[(finding.category, finding.title)] = finding
    return sorted(
        unique.values(),
        key=lambda finding: (
            SEVERITY_ORDER.get(finding.severity, 9),
            finding.category,
            finding.title,
        ),
    )


def format_findings(findings: Iterable[Finding]) -> str:
    """Render findings as a concise, evidence-first Markdown report."""
    result = list(findings)
    if not result:
        return "No failure findings in the visible Kubernetes API snapshot."
    lines = ["## Prioritized Diagnostic Findings"]
    for index, finding in enumerate(result, start=1):
        lines.append(f"{index}. [{finding.severity.upper()}] {finding.title} ({finding.category})")
        lines.append(f"   Evidence: {'; '.join(finding.evidence)}")
        lines.append(f"   Explanation: {finding.explanation}")
        if finding.additional_evidence:
            lines.append(
                f"   Additional evidence to collect: {', '.join(finding.additional_evidence)}"
            )
    return "\n".join(lines)


def format_partial_visibility(sections: Iterable[tuple[str, str]]) -> str:
    """Render denied or unavailable checks explicitly instead of omitting them."""
    entries = list(sections)
    if not entries:
        return ""
    lines = ["", "## Partial Visibility"]
    for section, detail in entries:
        lines.append(f"- **{section}**: {detail}")
    return "\n".join(lines)

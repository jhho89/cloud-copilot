from kubernetes import config, client
from google.cloud import monitoring_v3
import time
import os
from typing import Any, Iterable, Mapping, Optional

from . import analysis


def _load_kubernetes_client() -> client.CoreV1Api:
    """Loads the in-cluster or kubeconfig client without performing API calls."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv("KUBECONFIG", os.path.expanduser("~/.kube/config"))
        config.load_kube_config(config_file=kubeconfig_path)
    return client.CoreV1Api()


def _load_kubernetes_clients() -> tuple[Any, Any, Any, Any]:
    core = _load_kubernetes_client()
    return (
        core,
        client.AppsV1Api(),
        client.NetworkingV1Api(),
        client.CustomObjectsApi(),
    )


def _is_forbidden(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    reason = str(getattr(exc, "reason", "") or "").lower()
    body = str(getattr(exc, "body", "") or "").lower()
    return status == 403 or "forbidden" in reason or "forbidden" in body


def _error_text(exc: Exception) -> str:
    status = getattr(exc, "status", None)
    reason = getattr(exc, "reason", None)
    body = getattr(exc, "body", None)
    detail = f" status={status} reason={reason}"
    if body:
        detail += f" body={body}"
    return detail.strip()


def _result(sections: list[str]) -> str:
    return "\n".join(section for section in sections if section)


def _safe_list(call: Any, denied: list[str], unavailable: list[str], section: str):
    try:
        return call()
    except Exception as exc:
        if _is_forbidden(exc):
            denied.append((section, f"RBAC denied the Kubernetes API read:{_error_text(exc)}"))
        else:
            unavailable.append((section, f"API read failed:{_error_text(exc)}"))
        return None


def _pod_snapshot(
    core: Any,
    namespace: Optional[str],
    pod_name: Optional[str],
    denied: list[tuple[str, str]],
    unavailable: list[tuple[str, str]],
) -> list[Any]:
    if pod_name:
        if not namespace:
            raise ValueError("pod_name requires namespace")
        pod = _safe_list(
            lambda: core.read_namespaced_pod(name=pod_name, namespace=namespace),
            denied,
            unavailable,
            "pods",
        )
        return [pod] if pod else []
    if namespace:
        result = _safe_list(
            lambda: core.list_namespaced_pod(namespace),
            denied,
            unavailable,
            "pods",
        )
    else:
        result = _safe_list(
            lambda: core.list_pod_for_all_namespaces(),
            denied,
            unavailable,
            "pods",
        )
    return result.items if result else []


def _events_snapshot(
    core: Any,
    namespace: Optional[str],
    denied: list[tuple[str, str]],
    unavailable: list[tuple[str, str]],
) -> list[Any]:
    if namespace:
        result = _safe_list(
            lambda: core.list_namespaced_event(namespace),
            denied,
            unavailable,
            "events",
        )
    else:
        result = _safe_list(
            lambda: core.list_event_for_all_namespaces(),
            denied,
            unavailable,
            "events",
        )
    return result.items if result else []


def _metrics_snapshot(
    custom: Any,
    namespace: Optional[str],
    denied: list[tuple[str, str]],
    unavailable: list[tuple[str, str]],
) -> dict[str, dict[str, Any]]:
    group = "metrics.k8s.io"
    version = "v1beta1"
    try:
        if namespace:
            raw = custom.list_namespaced_custom_object(group, version, namespace, "pods")
        else:
            raw = custom.list_cluster_custom_object(group, version, "pods")
    except Exception as exc:
        if _is_forbidden(exc):
            denied.append(("resource_usage", f"RBAC denied metrics.k8s.io:{_error_text(exc)}"))
        else:
            unavailable.append(("resource_usage", f"metrics.k8s.io read failed:{_error_text(exc)}"))
        return {}

    metrics: dict[str, dict[str, Any]] = {}
    for item in raw.get("items", []) if isinstance(raw, Mapping) else []:
        metadata = item.get("metadata", {})
        key = f"{metadata.get('namespace', '')}/{metadata.get('name', '')}"
        cpu_millicores = 0.0
        memory_bytes = 0.0
        for container in item.get("containers", []) or []:
            usage = container.get("usage", {}) or {}
            cpu = analysis._quantity_millicores(usage.get("cpu"))
            memory = analysis._quantity_bytes(usage.get("memory"))
            cpu_millicores += cpu or 0
            memory_bytes += memory or 0
        metrics[key] = {"cpu_millicores": cpu_millicores, "memory_bytes": memory_bytes}
    return metrics


def _pod_logs_snapshot(
    core: Any,
    pods: Iterable[Any],
    denied: list[tuple[str, str]],
    unavailable: list[tuple[str, str]],
    tail_lines: int,
    limit: int = 25,
) -> tuple[dict[str, str], bool]:
    logs: dict[str, str] = {}
    truncated = False
    read_count = 0
    for pod in pods:
        namespace = _value_safe(pod, "metadata", "namespace")
        name = _value_safe(pod, "metadata", "name")
        if not namespace or not name:
            continue
        if read_count >= limit:
            truncated = True
            break
        read_count += 1
        try:
            body = core.read_namespaced_pod_log(
                name=name,
                namespace=namespace,
                tail_lines=tail_lines,
                previous=False,
            )
            logs[f"{namespace}/{name}"] = body or ""
        except Exception as exc:
            if _is_forbidden(exc):
                denied.append(
                    ("pod_logs", f"RBAC denied logs for {namespace}/{name}:{_error_text(exc)}")
                )
                break
            else:
                unavailable.append(
                    ("pod_logs", f"log read failed for {namespace}/{name}:{_error_text(exc)}")
                )
    return logs, truncated


def _value_safe(container: Any, parent: str, child: str) -> Optional[str]:
    parent_value = getattr(container, parent, None)
    return getattr(parent_value, child, None)


def _network_snapshot(
    core: Any,
    networking: Any,
    namespace: Optional[str],
    denied: list[tuple[str, str]],
    unavailable: list[tuple[str, str]],
) -> tuple[list[Any], list[Any], list[Any], list[Any], list[Any], list[Any]]:
    def listed(call: Any, section: str) -> Any:
        result = _safe_list(call, denied, unavailable, section)
        return result.items if result else []

    if namespace:
        services = listed(lambda: core.list_namespaced_service(namespace), "services")
        endpoints = listed(lambda: core.list_namespaced_endpoints(namespace), "endpoints")
        ingresses = listed(lambda: networking.list_namespaced_ingress(namespace), "ingresses")
        policies = listed(
            lambda: networking.list_namespaced_network_policy(namespace), "network_policies"
        )
        quotas = listed(
            lambda: core.list_namespaced_resource_quota(namespace), "resource_quotas"
        )
    else:
        services = listed(lambda: core.list_service_for_all_namespaces(), "services")
        endpoints = listed(lambda: core.list_endpoints_for_all_namespaces(), "endpoints")
        ingresses = listed(lambda: networking.list_ingress_for_all_namespaces(), "ingresses")
        policies = listed(
            lambda: networking.list_network_policy_for_all_namespaces(), "network_policies"
        )
        quotas = listed(
            lambda: core.list_resource_quota_for_all_namespaces(), "resource_quotas"
        )
    dns_services = listed(lambda: core.list_namespaced_service("kube-system"), "cluster_dns")
    return services, endpoints, ingresses, policies, quotas, dns_services


def run_read_only_diagnostics(
    namespace: Optional[str] = None,
    pod_name: Optional[str] = None,
    include_logs: bool = True,
    include_connectivity: bool = True,
    log_tail_lines: int = 50,
) -> str:
    """Collect read-only Kubernetes evidence and return a prioritized report.

    Denied API reads are reported under Partial Visibility rather than treated
    as absence of evidence.
    """
    try:
        core, apps, networking, custom = _load_kubernetes_clients()
    except Exception as exc:
        return f"Error loading Kubernetes clients: {exc}"

    denied: list[tuple[str, str]] = []
    unavailable: list[tuple[str, str]] = []
    try:
        pods = _pod_snapshot(core, namespace, pod_name, denied, unavailable)
        events = _events_snapshot(core, namespace, denied, unavailable)
        nodes_result = _safe_list(
            lambda: core.list_node(), denied, unavailable, "nodes"
        )
        nodes = nodes_result.items if nodes_result else []
        metrics = _metrics_snapshot(custom, namespace, denied, unavailable)
    except ValueError as exc:
        return f"Invalid diagnostic request: {exc}"

    logs, logs_truncated = ({}, False)
    if include_logs:
        logs, logs_truncated = _pod_logs_snapshot(
            core, pods, denied, unavailable, tail_lines=log_tail_lines
        )

    replica_sets: list[Any] = []
    deployments: list[Any] = []
    rs_result = _safe_list(
        lambda: (
            apps.list_namespaced_replica_set(namespace)
            if namespace
            else apps.list_replica_set_for_all_namespaces()
        ),
        denied,
        unavailable,
        "rollout_history",
    )
    if rs_result:
        replica_sets = rs_result.items
    deploy_result = _safe_list(
        lambda: (
            apps.list_namespaced_deployment(namespace)
            if namespace
            else apps.list_deployment_for_all_namespaces()
        ),
        denied,
        unavailable,
        "rollout_history",
    )
    if deploy_result:
        deployments = deploy_result.items

    pvc_result = _safe_list(
        lambda: (
            core.list_namespaced_persistent_volume_claim(namespace)
            if namespace
            else core.list_persistent_volume_claim_for_all_namespaces()
        ),
        denied,
        unavailable,
        "persistent_volume_claims",
    )
    pvcs = pvc_result.items if pvc_result else []

    deployment_conditions: list[Any] = []
    if replica_sets and deployments:
        newest_name = getattr(getattr(replica_sets[0], "metadata", None), "name", None)
        deployment_owners = {
            owner.name
            for owner in getattr(getattr(replica_sets[0], "metadata", None), "owner_references", []) or []
            if getattr(owner, "kind", "") == "Deployment"
        }
        for deployment in deployments:
            deploy_name = getattr(getattr(deployment, "metadata", None), "name", None)
            if deploy_name == newest_name or deploy_name in deployment_owners:
                deployment_conditions = getattr(
                    getattr(deployment, "status", None), "conditions", []
                ) or []
                break

    findings = analysis.diagnose_failures(
        pods=pods,
        events=events,
        nodes=nodes,
        pod_logs=logs,
        pod_metrics=metrics,
        pvcs=pvcs,
        replica_sets=replica_sets,
        deployment_conditions=deployment_conditions,
    )

    if include_connectivity:
        services, endpoints, ingresses, policies, quotas, dns_services = _network_snapshot(
            core, networking, namespace, denied, unavailable
        )
        dns_denied = any(
            section in {"endpoints", "cluster_dns"} for section, _ in denied + unavailable
        )
        findings.extend(
            analysis.diagnose_connectivity(
                services=services,
                endpoints=endpoints,
                pods=pods,
                ingresses=ingresses,
                network_policies=policies,
                dns_services=dns_services,
                include_dns=not dns_denied,
            )
        )
        findings.extend(analysis.diagnose_quotas(quotas, events))

    findings = analysis.sort_findings(findings)
    sections = [analysis.format_findings(findings)]
    if logs_truncated:
        unavailable.append(
            ("pod_logs", "log collection stopped after the read limit of 25 pods; remaining pod logs were not read")
        )
    sections.append(analysis.format_partial_visibility(denied + unavailable))
    return _result(sections)


def check_network_and_quota_signals(namespace: Optional[str] = None) -> str:
    """Read Service, endpoint, Ingress, DNS, policy, and quota signals."""
    try:
        core, _, networking, _ = _load_kubernetes_clients()
    except Exception as exc:
        return f"Error loading Kubernetes clients: {exc}"

    denied: list[tuple[str, str]] = []
    unavailable: list[tuple[str, str]] = []
    services, endpoints, ingresses, policies, quotas, dns_services = _network_snapshot(
        core, networking, namespace, denied, unavailable
    )
    pods = []
    if namespace:
        pod_result = _safe_list(
            lambda: core.list_namespaced_pod(namespace), denied, unavailable, "pods"
        )
        pods = pod_result.items if pod_result else []
    else:
        pod_result = _safe_list(
            lambda: core.list_pod_for_all_namespaces(), denied, unavailable, "pods"
        )
        pods = pod_result.items if pod_result else []
    events = _events_snapshot(core, namespace, denied, unavailable)
    dns_denied = any(
        section in {"endpoints", "cluster_dns"} for section, _ in denied + unavailable
    )
    findings = analysis.diagnose_connectivity(
        services=services,
        endpoints=endpoints,
        pods=pods,
        ingresses=ingresses,
        network_policies=policies,
        dns_services=dns_services,
        include_dns=not dns_denied,
    )
    findings.extend(analysis.diagnose_quotas(quotas, events))
    return _result(
        [
            analysis.format_findings(findings),
            analysis.format_partial_visibility(denied + unavailable),
        ]
    )


def correlate_rollout_history(
    namespace: Optional[str] = None,
    deployment_name: Optional[str] = None,
) -> str:
    """Correlate failing pods and warning events with ReplicaSet timing."""
    try:
        core, apps, _, _ = _load_kubernetes_clients()
    except Exception as exc:
        return f"Error loading Kubernetes clients: {exc}"

    denied: list[tuple[str, str]] = []
    unavailable: list[tuple[str, str]] = []
    pods = _pod_snapshot(core, namespace, None, denied, unavailable)
    events = _events_snapshot(core, namespace, denied, unavailable)
    if deployment_name and namespace:
        deployment_result = _safe_list(
            lambda: apps.read_namespaced_deployment(deployment_name, namespace),
            denied,
            unavailable,
            "rollout_history",
        )
        deployment = deployment_result
        replica_sets = []
        matching_pods = []
        if deployment:
            selector = getattr(getattr(deployment, "spec", None), "selector", None)
            match_labels = getattr(selector, "match_labels", None) or {}
            label_selector = ",".join(f"{key}={value}" for key, value in match_labels.items())
            rs_result = _safe_list(
                lambda: apps.list_namespaced_replica_set(namespace, label_selector=label_selector),
                denied,
                unavailable,
                "rollout_history",
            )
            replica_sets = rs_result.items if rs_result else []
            rs_names = {
                getattr(getattr(rs, "metadata", None), "name", "")
                for rs in replica_sets
            }
            for pod in pods:
                owners = getattr(getattr(pod, "metadata", None), "owner_references", []) or []
                if any(getattr(owner, "name", "") in rs_names for owner in owners):
                    matching_pods.append(pod)
            pods = matching_pods
        conditions = getattr(getattr(deployment, "status", None), "conditions", []) or []
    else:
        rs_result = _safe_list(
            lambda: (
                apps.list_namespaced_replica_set(namespace)
                if namespace
                else apps.list_replica_set_for_all_namespaces()
            ),
            denied,
            unavailable,
            "rollout_history",
        )
        replica_sets = rs_result.items if rs_result else []
        conditions = []

    findings = analysis._rollout_findings(
        pods,
        replica_sets,
        deployment_conditions=conditions,
        events=events,
        deployment_name=deployment_name,
    )
    return _result(
        [
            analysis.format_findings(findings),
            analysis.format_partial_visibility(denied + unavailable),
        ]
    )

def get_cluster_info() -> str:
    """Gets information about the current Kubernetes cluster.

    Returns:
        A string containing cluster name, version, and endpoint information.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        config.load_kube_config(config_file=kubeconfig_path)

    try:
        v1 = client.CoreV1Api()
        version_api = client.VersionApi()

        # Get cluster version
        version_info = version_api.get_code()

        # Get current context info
        contexts, active_context = config.list_kube_config_contexts(config_file=os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config')))
        cluster_name = active_context['context']['cluster'] if active_context else "Unknown"

        # Get node count
        nodes = v1.list_node()
        node_count = len(nodes.items)

        info = [
            f"Cluster Name: {cluster_name}",
            f"Kubernetes Version: {version_info.git_version}",
            f"Nodes: {node_count}",
            f"Platform: {version_info.platform}"
        ]

        return "\n".join(info)
    except Exception as e:
        return f"Error retrieving cluster info: {str(e)}"


def list_namespaces() -> str:
    """Lists all namespaces in the cluster.

    Returns:
        A string containing all namespaces and their status.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        config.load_kube_config(config_file=kubeconfig_path)

    v1 = client.CoreV1Api()
    namespaces = v1.list_namespace()

    namespace_list = []
    for ns in namespaces.items:
        status = ns.status.phase
        age = ns.metadata.creation_timestamp
        namespace_list.append(f"- {ns.metadata.name} (Status: {status}, Created: {age})")

    return "\n".join(namespace_list) if namespace_list else "No namespaces found"


def list_all_pods(namespace: Optional[str] = None) -> str:
    """Lists all pods in the cluster or a specific namespace.

    Args:
        namespace: Optional namespace to filter pods. If None, lists all pods cluster-wide.

    Returns:
        A string containing pod information including name, namespace, status, and node.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        config.load_kube_config(config_file=kubeconfig_path)

    v1 = client.CoreV1Api()

    if namespace:
        pod_list = v1.list_namespaced_pod(namespace)
    else:
        pod_list = v1.list_pod_for_all_namespaces()

    pods = []
    for pod in pod_list.items:
        ns = pod.metadata.namespace
        name = pod.metadata.name
        phase = pod.status.phase
        node = pod.spec.node_name or "Unassigned"
        restarts = sum(cs.restart_count for cs in pod.status.container_statuses) if pod.status.container_statuses else 0

        pods.append(f"{ns}/{name} - Status: {phase}, Node: {node}, Restarts: {restarts}")

    if not pods:
        return f"No pods found{f' in namespace {namespace}' if namespace else ' in cluster'}"

    return "\n".join(pods)


def get_unhealthy_pods(namespace: Optional[str] = None) -> str:
    """Gets all pods that are not in a 'Running' or 'Succeeded' state.

    Args:
        namespace: Optional namespace to check. If None, checks all namespaces.

    Returns:
        A string containing a list of unhealthy pods and their status.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        config.load_kube_config(config_file=kubeconfig_path)

    v1 = client.CoreV1Api()

    if namespace:
        pod_list = v1.list_namespaced_pod(namespace)
    else:
        pod_list = v1.list_pod_for_all_namespaces()

    unhealthy_pods = []
    for pod in pod_list.items:
        if pod.status.phase not in ["Running", "Succeeded"]:
            container_statuses = []
            if pod.status.container_statuses:
                for cs in pod.status.container_statuses:
                    if cs.state.waiting:
                        container_statuses.append(f"Waiting: {cs.state.waiting.reason}")
                    elif cs.state.terminated:
                        container_statuses.append(f"Terminated: {cs.state.terminated.reason}")

            status_detail = ", ".join(container_statuses) if container_statuses else pod.status.phase
            ns = pod.metadata.namespace
            restarts = sum(cs.restart_count for cs in pod.status.container_statuses) if pod.status.container_statuses else 0
            unhealthy_pods.append(f"{ns}/{pod.metadata.name} - Status: {status_detail}, Restarts: {restarts}")

    if not unhealthy_pods:
        return f"No unhealthy pods found{f' in namespace {namespace}' if namespace else ' in cluster'}."

    return "\n".join(unhealthy_pods)


def get_cluster_metrics(project_id: Optional[str] = None) -> str:
    """Gets cluster-level metrics from Cloud Monitoring.

    Args:
        project_id: GCP project ID. If None, uses default project.

    Returns:
        A string containing cluster CPU and memory metrics.
    """
    try:
        client = monitoring_v3.MetricServiceClient()

        if not project_id:
            import google.auth
            _, project_id = google.auth.default()

        project_name = f"projects/{project_id}"

        # Get the last 5 minutes of data
        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval({
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": (seconds - 300), "nanos": nanos},
        })

        results = []

        # CPU usage metric
        cpu_filter = 'metric.type="kubernetes.io/container/cpu/core_usage_time" AND resource.type="k8s_container"'
        cpu_results = client.list_time_series(
            request={
                "name": project_name,
                "filter": cpu_filter,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )

        cpu_values = []
        for result in cpu_results:
            if result.points:
                cpu_values.append(result.points[0].value.double_value)

        if cpu_values:
            results.append(f"Average CPU usage: {sum(cpu_values) / len(cpu_values):.2f} cores")

        # Memory usage metric
        mem_filter = 'metric.type="kubernetes.io/container/memory/used_bytes" AND resource.type="k8s_container"'
        mem_results = client.list_time_series(
            request={
                "name": project_name,
                "filter": mem_filter,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )

        mem_values = []
        for result in mem_results:
            if result.points:
                mem_values.append(result.points[0].value.int64_value)

        if mem_values:
            avg_mem_gb = sum(mem_values) / len(mem_values) / (1024**3)
            results.append(f"Average Memory usage: {avg_mem_gb:.2f} GB")

        return "\n".join(results) if results else "No metrics data available"

    except Exception as e:
        return f"Error retrieving cluster metrics: {str(e)}"


def get_node_resources() -> str:
    """Gets resource usage information for all nodes in the cluster.

    Returns:
        A string containing node resource information including CPU and memory capacity/usage.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        config.load_kube_config(config_file=kubeconfig_path)

    v1 = client.CoreV1Api()

    try:
        nodes = v1.list_node()

        node_info = []
        for node in nodes.items:
            name = node.metadata.name
            status = "Ready" if any(c.type == "Ready" and c.status == "True" for c in node.status.conditions) else "NotReady"

            # Get capacity
            capacity = node.status.capacity
            cpu_capacity = capacity.get('cpu', 'Unknown')
            memory_capacity_bytes = capacity.get('memory', '0')
            memory_capacity_gb = int(memory_capacity_bytes.replace('Ki', '')) / (1024 * 1024) if 'Ki' in memory_capacity_bytes else 0

            # Get allocatable
            allocatable = node.status.allocatable
            cpu_allocatable = allocatable.get('cpu', 'Unknown')
            memory_allocatable_bytes = allocatable.get('memory', '0')
            memory_allocatable_gb = int(memory_allocatable_bytes.replace('Ki', '')) / (1024 * 1024) if 'Ki' in memory_allocatable_bytes else 0

            # Get pod count
            field_selector = f"spec.nodeName={name}"
            pods_on_node = v1.list_pod_for_all_namespaces(field_selector=field_selector)
            pod_count = len(pods_on_node.items)

            node_info.append(
                f"Node: {name}\n"
                f"  Status: {status}\n"
                f"  CPU: {cpu_allocatable}/{cpu_capacity} allocatable\n"
                f"  Memory: {memory_allocatable_gb:.1f}GB/{memory_capacity_gb:.1f}GB allocatable\n"
                f"  Pods: {pod_count}"
            )

        return "\n\n".join(node_info) if node_info else "No nodes found"
    except Exception as e:
        return f"Error retrieving node resources: {str(e)}"


def get_pod_events(namespace: str, pod_name: str) -> str:
    """Gets Kubernetes events for a specific pod.

    Args:
        namespace: The namespace of the pod.
        pod_name: The name of the pod.

    Returns:
        A string containing recent events for the pod.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        config.load_kube_config(config_file=kubeconfig_path)

    v1 = client.CoreV1Api()

    field_selector = f"involvedObject.name={pod_name}"
    events = v1.list_namespaced_event(namespace, field_selector=field_selector)

    if not events.items:
        return f"No events found for pod '{pod_name}' in namespace '{namespace}'."

    event_lines = []
    for event in sorted(events.items, key=lambda x: x.last_timestamp or x.event_time, reverse=True)[:10]:
        timestamp = event.last_timestamp or event.event_time
        event_lines.append(f"{timestamp} - {event.type}: {event.reason} - {event.message}")

    return "\n".join(event_lines)


def get_namespace_resources(namespace: str) -> str:
    """Gets total resource allocation (requests) for a namespace.

    Calculates the sum of CPU and memory requests across all pods in the namespace.
    This is useful for cost estimation and capacity planning.

    Args:
        namespace: The namespace to analyze

    Returns:
        Resource allocation summary including total CPU cores, memory GB, storage GB, and pod count
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        config.load_kube_config(config_file=kubeconfig_path)

    v1 = client.CoreV1Api()

    try:
        # Get all pods in namespace
        pods = v1.list_namespaced_pod(namespace)

        total_cpu_millicores = 0
        total_memory_bytes = 0
        total_storage_bytes = 0
        pod_count = len(pods.items)

        # Sum up CPU and memory requests from all containers in all pods
        for pod in pods.items:
            if pod.spec.containers:
                for container in pod.spec.containers:
                    if container.resources and container.resources.requests:
                        # Parse CPU (can be in cores like "1" or millicores like "100m")
                        cpu_request = container.resources.requests.get('cpu', '0')
                        if cpu_request:
                            if isinstance(cpu_request, str):
                                if cpu_request.endswith('m'):
                                    total_cpu_millicores += int(cpu_request[:-1])
                                else:
                                    total_cpu_millicores += int(float(cpu_request) * 1000)

                        # Parse memory (can be in various units: Mi, Gi, M, G, Ki, K)
                        memory_request = container.resources.requests.get('memory', '0')
                        if memory_request:
                            if isinstance(memory_request, str):
                                memory_str = memory_request.upper()
                                if memory_str.endswith('KI'):
                                    total_memory_bytes += int(memory_str[:-2]) * 1024
                                elif memory_str.endswith('K'):
                                    total_memory_bytes += int(memory_str[:-1]) * 1000
                                elif memory_str.endswith('MI'):
                                    total_memory_bytes += int(memory_str[:-2]) * 1024 * 1024
                                elif memory_str.endswith('M'):
                                    total_memory_bytes += int(memory_str[:-1]) * 1000 * 1000
                                elif memory_str.endswith('GI'):
                                    total_memory_bytes += int(memory_str[:-2]) * 1024 * 1024 * 1024
                                elif memory_str.endswith('G'):
                                    total_memory_bytes += int(memory_str[:-1]) * 1000 * 1000 * 1000
                                else:
                                    total_memory_bytes += int(memory_str)

        # Get persistent volumes in namespace
        pvcs = v1.list_namespaced_persistent_volume_claim(namespace)
        for pvc in pvcs.items:
            if pvc.spec.resources and pvc.spec.resources.requests:
                storage_request = pvc.spec.resources.requests.get('storage', '0')
                if storage_request:
                    if isinstance(storage_request, str):
                        storage_str = storage_request.upper()
                        if storage_str.endswith('GI'):
                            total_storage_bytes += int(storage_str[:-2]) * 1024 * 1024 * 1024
                        elif storage_str.endswith('G'):
                            total_storage_bytes += int(storage_str[:-1]) * 1000 * 1000 * 1000
                        elif storage_str.endswith('MI'):
                            total_storage_bytes += int(storage_str[:-2]) * 1024 * 1024
                        elif storage_str.endswith('M'):
                            total_storage_bytes += int(storage_str[:-1]) * 1000 * 1000

        # Convert to human-readable units
        total_cpu_cores = total_cpu_millicores / 1000
        total_memory_gb = total_memory_bytes / (1024 ** 3)
        total_storage_gb = total_storage_bytes / (1024 ** 3)

        # Get cluster info to determine if it's Autopilot
        try:
            # Try to detect Autopilot by checking for Autopilot-specific annotations or node labels
            nodes = v1.list_node()
            is_autopilot = False
            if nodes.items:
                first_node = nodes.items[0]
                if first_node.metadata.labels:
                    is_autopilot = 'cloud.google.com/gke-nodepool' in first_node.metadata.labels and \
                                 'autopilot' in str(first_node.metadata.labels).lower()
            cluster_type = "GKE Autopilot" if is_autopilot else "GKE Standard"
        except:
            cluster_type = "Unknown"
            is_autopilot = True  # Default to Autopilot for cost estimation

        result = f"""## Resource Allocation for Namespace: {namespace}

**Cluster Type**: {cluster_type}
**Pod Count**: {pod_count}

### Resource Requests Summary:
- **CPU**: {total_cpu_cores:.2f} cores ({total_cpu_millicores} millicores)
- **Memory**: {total_memory_gb:.2f} GB ({total_memory_bytes / (1024**2):.2f} MiB)
- **Storage (PVCs)**: {total_storage_gb:.2f} GB

### For Cost Estimation:
Use these values with Cost Copilot:
- namespace: {namespace}
- cpu_cores: {total_cpu_cores:.2f}
- memory_gb: {total_memory_gb:.2f}
- storage_gb: {total_storage_gb:.2f}
- pod_count: {pod_count}
- is_autopilot: {is_autopilot}

**Note**: These are resource *requests*, not actual usage. Actual usage may be lower.
"""
        return result

    except Exception as e:
        return f"Error getting namespace resources: {str(e)}"


def get_cluster_resources() -> str:
    """Gets total resource allocation (requests) for the entire cluster.

    Calculates the sum of CPU and memory requests across all namespaces and pods.
    This is useful for cluster-wide cost estimation and capacity planning.

    Returns:
        Cluster-wide resource allocation summary including total CPU, memory, storage, pods, and nodes
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
        config.load_kube_config(config_file=kubeconfig_path)

    v1 = client.CoreV1Api()

    try:
        # Get all pods in cluster
        pods = v1.list_pod_for_all_namespaces()

        total_cpu_millicores = 0
        total_memory_bytes = 0
        total_storage_bytes = 0
        pod_count = len(pods.items)

        # Sum up CPU and memory requests from all containers in all pods
        for pod in pods.items:
            if pod.spec.containers:
                for container in pod.spec.containers:
                    if container.resources and container.resources.requests:
                        # Parse CPU
                        cpu_request = container.resources.requests.get('cpu', '0')
                        if cpu_request:
                            if isinstance(cpu_request, str):
                                if cpu_request.endswith('m'):
                                    total_cpu_millicores += int(cpu_request[:-1])
                                else:
                                    total_cpu_millicores += int(float(cpu_request) * 1000)

                        # Parse memory
                        memory_request = container.resources.requests.get('memory', '0')
                        if memory_request:
                            if isinstance(memory_request, str):
                                memory_str = memory_request.upper()
                                if memory_str.endswith('KI'):
                                    total_memory_bytes += int(memory_str[:-2]) * 1024
                                elif memory_str.endswith('K'):
                                    total_memory_bytes += int(memory_str[:-1]) * 1000
                                elif memory_str.endswith('MI'):
                                    total_memory_bytes += int(memory_str[:-2]) * 1024 * 1024
                                elif memory_str.endswith('M'):
                                    total_memory_bytes += int(memory_str[:-1]) * 1000 * 1000
                                elif memory_str.endswith('GI'):
                                    total_memory_bytes += int(memory_str[:-2]) * 1024 * 1024 * 1024
                                elif memory_str.endswith('G'):
                                    total_memory_bytes += int(memory_str[:-1]) * 1000 * 1000 * 1000
                                else:
                                    total_memory_bytes += int(memory_str)

        # Get all persistent volumes across all namespaces
        try:
            all_pvcs = v1.list_persistent_volume_claim_for_all_namespaces()
            for pvc in all_pvcs.items:
                if pvc.spec.resources and pvc.spec.resources.requests:
                    storage_request = pvc.spec.resources.requests.get('storage', '0')
                    if storage_request:
                        if isinstance(storage_request, str):
                            storage_str = storage_request.upper()
                            if storage_str.endswith('GI'):
                                total_storage_bytes += int(storage_str[:-2]) * 1024 * 1024 * 1024
                            elif storage_str.endswith('G'):
                                total_storage_bytes += int(storage_str[:-1]) * 1000 * 1000 * 1000
                            elif storage_str.endswith('MI'):
                                total_storage_bytes += int(storage_str[:-2]) * 1024 * 1024
                            elif storage_str.endswith('M'):
                                total_storage_bytes += int(storage_str[:-1]) * 1000 * 1000
        except Exception as e:
            print(f"Warning: Could not get PVCs: {e}")

        # Get node count
        nodes = v1.list_node()
        node_count = len(nodes.items)

        # Detect cluster type
        is_autopilot = False
        if nodes.items:
            first_node = nodes.items[0]
            if first_node.metadata.labels:
                is_autopilot = 'cloud.google.com/gke-nodepool' in first_node.metadata.labels and \
                             'autopilot' in str(first_node.metadata.labels).lower()
        cluster_type = "GKE Autopilot" if is_autopilot else "GKE Standard"

        # Convert to human-readable units
        total_cpu_cores = total_cpu_millicores / 1000
        total_memory_gb = total_memory_bytes / (1024 ** 3)
        total_storage_gb = total_storage_bytes / (1024 ** 3)

        result = f"""## Cluster-Wide Resource Allocation

**Cluster Type**: {cluster_type}
**Node Count**: {node_count}
**Total Pods**: {pod_count}

### Total Resource Requests Across All Namespaces:
- **CPU**: {total_cpu_cores:.2f} cores ({total_cpu_millicores} millicores)
- **Memory**: {total_memory_gb:.2f} GB ({total_memory_bytes / (1024**2):.2f} MiB)
- **Storage (PVCs)**: {total_storage_gb:.2f} GB

### Average Per Pod:
- **CPU per pod**: {(total_cpu_cores / pod_count) if pod_count > 0 else 0:.3f} cores
- **Memory per pod**: {(total_memory_gb / pod_count) if pod_count > 0 else 0:.3f} GB

### For Cost Estimation:
Use these values with Cost Copilot:
- total_cpu_cores: {total_cpu_cores:.2f}
- total_memory_gb: {total_memory_gb:.2f}
- total_storage_gb: {total_storage_gb:.2f}
- total_pods: {pod_count}
- node_count: {node_count}
- is_autopilot: {is_autopilot}

**Note**: These are resource *requests*, not actual usage. Actual usage may be lower.
"""
        return result

    except Exception as e:
        return f"Error getting cluster resources: {str(e)}"

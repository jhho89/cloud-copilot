from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
K8S_AGENTS = REPO_ROOT / "services" / "k8s-copilot" / "src" / "agents"
if str(K8S_AGENTS) not in sys.path:
    sys.path.insert(0, str(K8S_AGENTS))

from diagnostic_agent import analysis, tools

NOW = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)


def metadata(
    name: str,
    namespace: str = "app",
    created: datetime = NOW - timedelta(minutes=10),
    labels: dict[str, str] | None = None,
    owners: list[SimpleNamespace] | None = None,
):
    return SimpleNamespace(
        name=name,
        namespace=namespace,
        creation_timestamp=created,
        labels=labels or {},
        owner_references=owners or [],
        annotations={"deployment.kubernetes.io/revision": "7"},
    )


def condition(kind: str, status: str, reason: str = "", message: str = ""):
    return SimpleNamespace(
        type=kind,
        status=status,
        reason=reason,
        message=message,
        last_transition_time=NOW - timedelta(minutes=5),
    )


def container_status(
    name: str = "app",
    ready: bool = True,
    restart_count: int = 0,
    waiting: SimpleNamespace | None = None,
    terminated: SimpleNamespace | None = None,
    last_terminated: SimpleNamespace | None = None,
):
    return SimpleNamespace(
        name=name,
        ready=ready,
        restart_count=restart_count,
        state=SimpleNamespace(
            waiting=waiting,
            running=None if waiting or terminated else SimpleNamespace(started_at=NOW),
            terminated=terminated,
        ),
        last_state=SimpleNamespace(terminated=last_terminated),
    )


def pod(
    name: str,
    namespace: str = "app",
    phase: str = "Running",
    node_name: str = "node-a",
    labels: dict[str, str] | None = None,
    statuses: list[SimpleNamespace] | None = None,
    conditions: list[SimpleNamespace] | None = None,
    owners: list[SimpleNamespace] | None = None,
    created: datetime = NOW - timedelta(minutes=10),
):
    return SimpleNamespace(
        metadata=metadata(name, namespace, created, labels, owners),
        spec=SimpleNamespace(
            node_name=node_name,
            containers=[
                SimpleNamespace(
                    name="app",
                    image="registry.example.com/app:1.2",
                    resources=SimpleNamespace(
                        requests={"cpu": "250m", "memory": "128Mi"},
                        limits={"memory": "128Mi"},
                    ),
                )
            ],
            node_selector={"disktype": "ssd"},
            tolerations=[],
        ),
        status=SimpleNamespace(
            phase=phase,
            container_statuses=statuses or [container_status()],
            conditions=conditions or [condition("Ready", "True")],
        ),
    )


def event(name: str, reason: str, message: str, at: datetime, kind: str = "Warning"):
    return SimpleNamespace(
        involved_object=SimpleNamespace(kind="Pod", name=name, namespace="app"),
        type=kind,
        reason=reason,
        message=message,
        count=3,
        first_timestamp=at,
        last_timestamp=at,
        event_time=at,
    )


def node(name: str, conditions: list[SimpleNamespace] | None = None):
    return SimpleNamespace(
        metadata=metadata(name, "", NOW - timedelta(days=30), {"disktype": "ssd"}),
        spec=SimpleNamespace(taints=[]),
        status=SimpleNamespace(
            conditions=conditions
            or [
                condition("Ready", "True"),
                condition("MemoryPressure", "False"),
            ],
            capacity={"cpu": "4", "memory": "16Gi"},
            allocatable={"cpu": "3800m", "memory": "15Gi"},
        ),
    )


def replica_set(
    name: str,
    created: datetime,
    revision: str,
    deployment: str = "web",
):
    owner = SimpleNamespace(kind="Deployment", name=deployment)
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace="app",
            creation_timestamp=created,
            owner_references=[owner],
            annotations={"deployment.kubernetes.io/revision": revision},
        ),
        spec=SimpleNamespace(),
        status=SimpleNamespace(replicas=1, ready_replicas=1),
    )


def service(name: str, selector: dict[str, str] | None = None):
    return SimpleNamespace(
        metadata=metadata(name),
        spec=SimpleNamespace(selector=selector or {}),
    )


def endpoints(name: str, ready: int = 0, not_ready: int = 0):
    return SimpleNamespace(
        metadata=metadata(name),
        subsets=[
            SimpleNamespace(
                addresses=[SimpleNamespace(ip=f"10.0.0.{index}") for index in range(ready)],
                not_ready_addresses=[
                    SimpleNamespace(ip=f"10.1.0.{index}") for index in range(not_ready)
                ],
            )
        ],
    )


def test_oomkilled_includes_limits_usage_and_node_pressure() -> None:
    crashed_node = node(
        "node-a",
        [condition("Ready", "True"), condition("MemoryPressure", "True")],
    )
    target = pod(
        "web",
        statuses=[
            container_status(
                terminated=SimpleNamespace(reason="OOMKilled", exit_code=137),
                last_terminated=SimpleNamespace(reason="OOMKilled", exit_code=137),
            )
        ],
    )
    findings = analysis.diagnose_failures(
        [target],
        [],
        [crashed_node],
        {"app/web": "panic: worker stopped\n"},
        {"app/web": {"cpu_millicores": 250, "memory_bytes": 256 * 1024 * 1024}},
    )

    finding = next(item for item in findings if item.category == "pod_failure")
    assert "OOMKilled" in finding.title
    assert "MemoryPressure=True" in " ".join(finding.evidence)
    assert "128.0Mi" in " ".join(finding.evidence)
    assert "256.0Mi" in " ".join(finding.evidence)


def test_crashloop_backoff_reports_restarts_events_and_logs() -> None:
    target = pod(
        "api",
        statuses=[
            container_status(
                waiting=SimpleNamespace(reason="CrashLoopBackOff", message="back-off"),
                restart_count=5,
            )
        ],
    )
    findings = analysis.diagnose_failures(
        [target],
        [event("api", "BackOff", "Back-off restarting failed container", NOW)],
        [node("node-a")],
        {"app/api": "Traceback (most recent call last)\nconfig invalid\n"},
        {},
    )

    finding = next(item for item in findings if item.category == "pod_failure")
    assert "crash loop" in finding.title.lower()
    assert "CrashLoopBackOff" in " ".join(finding.evidence)
    assert "restarts=5" in " ".join(finding.evidence)
    assert "config invalid" in " ".join(finding.evidence)


def test_image_pull_failure_uses_waiting_state_and_events() -> None:
    target = pod(
        "worker",
        statuses=[
            container_status(
                waiting=SimpleNamespace(
                    reason="ImagePullBackOff",
                    message="pull access denied",
                )
            )
        ],
    )
    findings = analysis.diagnose_failures(
        [target],
        [event("worker", "Failed", "pull access denied", NOW)],
        [node("node-a")],
        {},
        {},
    )

    finding = next(item for item in findings if item.category == "image_pull")
    assert "image pull" in finding.title.lower()
    assert "pull access denied" in " ".join(finding.evidence)


def test_pending_pod_reports_scheduler_message_and_node_constraints() -> None:
    target = pod("batch", phase="Pending")
    findings = analysis.diagnose_failures(
        [target],
        [event("batch", "FailedScheduling", "0/1 nodes available: Insufficient cpu", NOW)],
        [node("node-a")],
        {},
        {},
    )

    finding = next(item for item in findings if item.category == "scheduling")
    assert finding.severity == "high"
    assert "Insufficient cpu" in " ".join(finding.evidence)
    assert "nodeSelector" in " ".join(finding.evidence)


def test_pvc_pending_and_mount_event_are_reported() -> None:
    target = pod(
        "db",
        phase="Pending",
        statuses=[
            container_status(
                waiting=SimpleNamespace(reason="ContainerCreating", message="mount failed")
            )
        ],
    )
    pvc = SimpleNamespace(
        metadata=metadata("data", "app"),
        spec=SimpleNamespace(resources=SimpleNamespace(requests={"storage": "10Gi"})),
        status=SimpleNamespace(phase="Pending", conditions=[]),
    )
    findings = analysis.diagnose_failures(
        [target],
        [event("db", "FailedAttachVolume", "Multi-Attach error for volume", NOW)],
        [node("node-a")],
        {},
        {},
        pvcs=[pvc],
    )

    assert [item.title for item in findings if item.category == "storage"]
    assert "PVC is Pending" in " ".join(item.title for item in findings)


def test_probe_failure_and_node_pressure_are_distinct_findings() -> None:
    target = pod(
        "frontend",
        statuses=[container_status(ready=False)],
        conditions=[condition("Ready", "False")],
    )
    pressured = node(
        "node-b",
        [condition("Ready", "True"), condition("DiskPressure", "True")],
    )
    findings = analysis.diagnose_failures(
        [target],
        [event("frontend", "Unhealthy", "Liveness probe failed: HTTP 500", NOW)],
        [node("node-a"), pressured],
        {},
        {},
    )

    probe = next(item for item in findings if item.category == "probe_failure")
    pressure = next(item for item in findings if item.category == "node_pressure")
    assert "HTTP 500" in " ".join(probe.evidence)
    assert "DiskPressure=True" in " ".join(pressure.evidence)


def test_rollout_timing_is_correlated_only_for_failing_pods() -> None:
    old_rs = replica_set("web-old", NOW - timedelta(hours=2), "6")
    new_rs = replica_set("web-new", NOW - timedelta(minutes=10), "7")
    owner = SimpleNamespace(kind="ReplicaSet", name="web-new")
    failed = pod(
        "web-new",
        statuses=[container_status(waiting=SimpleNamespace(reason="CrashLoopBackOff"), restart_count=2)],
        owners=[owner],
        created=NOW - timedelta(minutes=9),
    )
    healthy = pod(
        "old-web",
        owners=[SimpleNamespace(kind="ReplicaSet", name="web-old")],
        created=NOW - timedelta(hours=1),
    )
    findings = analysis.diagnose_failures(
        [failed, healthy],
        [event("web-new", "BackOff", "Back-off restarting failed container", NOW)],
        [node("node-a")],
        {},
        {},
        replica_sets=[old_rs, new_rs],
    )

    rollout = next(item for item in findings if item.category == "rollout_correlation")
    assert "revision 7" in rollout.title
    assert "not proof that the rollout caused" in rollout.explanation

    healthy_findings = analysis.diagnose_failures(
        [healthy], [], [node("node-a")], {}, {}, replica_sets=[old_rs, new_rs]
    )
    assert not [item for item in healthy_findings if item.category == "rollout_correlation"]


def test_connectivity_and_quota_findings_are_prioritized() -> None:
    app_pod = pod("web", labels={"app": "web"})
    policy = SimpleNamespace(
        metadata=metadata("deny-ingress"),
        spec=SimpleNamespace(
            pod_selector=SimpleNamespace(match_labels={"app": "web"}),
            ingress=[],
            egress=[],
        ),
    )
    ingress = SimpleNamespace(
        metadata=metadata("public"),
        spec=SimpleNamespace(
            rules=[
                SimpleNamespace(
                    http=SimpleNamespace(
                        paths=[
                            SimpleNamespace(
                                backend=SimpleNamespace(service=SimpleNamespace(name="web"))
                            )
                        ]
                    )
                )
            ]
        ),
    )
    quota = SimpleNamespace(
        metadata=metadata("app-quota"),
        status=SimpleNamespace(
            hard={"requests.cpu": "1"},
            used={"requests.cpu": "1"},
        ),
    )
    findings = analysis.diagnose_connectivity(
        [service("web", {"app": "web"})],
        [endpoints("kube-dns", ready=1)],
        [app_pod],
        [ingress],
        [policy],
        [service("kube-dns")],
    )
    findings.extend(
        analysis.diagnose_quotas(
            [quota],
            [event("web", "FailedCreate", "exceeded quota: requests.cpu", NOW)],
        )
    )

    titles = {item.category: item for item in findings}
    assert titles["service"].severity == "high"
    assert titles["ingress"].severity == "high"
    assert titles["resource_quota"].severity == "high"
    assert titles["network_policy"].severity == "info"
    assert "dns" not in titles
    report = analysis.format_findings(findings)
    assert "1. [HIGH]" in report and "3. [INFO]" in report


class DeniedRead(Exception):
    status = 403
    reason = "Forbidden"
    body = '{"reason":"Forbidden"}'


def denied_error() -> Exception:
    return DeniedRead("forbidden")


def fake_apis_all_denied():
    def deny(*args, **kwargs):
        raise denied_error()

    core = SimpleNamespace(
        list_pod_for_all_namespaces=deny,
        list_namespaced_pod=deny,
        list_event_for_all_namespaces=deny,
        list_namespaced_event=deny,
        list_node=deny,
        list_persistent_volume_claim_for_all_namespaces=deny,
        list_namespaced_persistent_volume_claim=deny,
        list_service_for_all_namespaces=deny,
        list_namespaced_service=deny,
        list_endpoints_for_all_namespaces=deny,
        list_namespaced_endpoints=deny,
        list_resource_quota_for_all_namespaces=deny,
        list_namespaced_resource_quota=deny,
        read_namespaced_pod_log=deny,
    )
    apps = SimpleNamespace(
        list_replica_set_for_all_namespaces=deny,
        list_namespaced_replica_set=deny,
        list_deployment_for_all_namespaces=deny,
        list_namespaced_deployment=deny,
    )
    networking = SimpleNamespace(
        list_ingress_for_all_namespaces=deny,
        list_namespaced_ingress=deny,
        list_network_policy_for_all_namespaces=deny,
        list_namespaced_network_policy=deny,
    )
    custom = SimpleNamespace(list_cluster_custom_object=deny, list_namespaced_custom_object=deny)
    return core, apps, networking, custom


def test_all_denied_reads_are_explicitly_reported(monkeypatch) -> None:
    monkeypatch.setattr(tools, "_load_kubernetes_clients", fake_apis_all_denied)

    report = tools.run_read_only_diagnostics()

    assert "No failure findings" in report
    assert "## Partial Visibility" in report
    for section in (
        "pods",
        "events",
        "nodes",
        "resource_usage",
        "rollout_history",
        "persistent_volume_claims",
        "services",
        "network_policies",
        "resource_quotas",
        "cluster_dns",
    ):
        assert section in report


def fake_apis_log_denied():
    app = pod("web")
    list_result = SimpleNamespace(items=[app])
    core = SimpleNamespace(
        list_pod_for_all_namespaces=lambda *args, **kwargs: list_result,
        list_event_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(items=[]),
        list_node=lambda *args, **kwargs: SimpleNamespace(items=[node("node-a")]),
        list_persistent_volume_claim_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(
            items=[]
        ),
        list_service_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(items=[]),
        list_namespaced_service=lambda *args, **kwargs: SimpleNamespace(
            items=[service("kube-dns")]
        ),
        list_endpoints_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(items=[]),
        list_resource_quota_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(items=[]),
        read_namespaced_pod_log=lambda *args, **kwargs: (_ for _ in ()).throw(denied_error()),
    )
    apps = SimpleNamespace(
        list_replica_set_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(items=[]),
        list_deployment_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(items=[]),
    )
    networking = SimpleNamespace(
        list_ingress_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(items=[]),
        list_network_policy_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(items=[]),
    )
    custom = SimpleNamespace(list_cluster_custom_object=lambda *args, **kwargs: {"items": []})
    return core, apps, networking, custom


def test_log_denial_is_partial_visibility(monkeypatch) -> None:
    monkeypatch.setattr(tools, "_load_kubernetes_clients", fake_apis_log_denied)

    report = tools.run_read_only_diagnostics()

    assert "pod_logs" in report
    assert "RBAC denied logs for app/web" in report


def fake_apis_rollout():
    deployment = SimpleNamespace(
        metadata=metadata("web"),
        spec=SimpleNamespace(selector=SimpleNamespace(match_labels={"app": "web"})),
        status=SimpleNamespace(
            conditions=[condition("Available", "False", "MinimumReplicasUnavailable")]
        ),
    )
    old_rs = replica_set("web-old", NOW - timedelta(hours=1), "6")
    new_rs = replica_set("web-new", NOW - timedelta(minutes=10), "7")
    failed = pod(
        "web-new",
        statuses=[container_status(waiting=SimpleNamespace(reason="CrashLoopBackOff"), restart_count=1)],
        owners=[SimpleNamespace(kind="ReplicaSet", name="web-new")],
        created=NOW - timedelta(minutes=9),
    )
    core = SimpleNamespace(
        list_namespaced_pod=lambda *args, **kwargs: SimpleNamespace(items=[failed]),
        list_event_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(
            items=[event("web-new", "BackOff", "Back-off restarting failed container", NOW)]
        ),
        list_namespaced_event=lambda *args, **kwargs: SimpleNamespace(
            items=[event("web-new", "BackOff", "Back-off restarting failed container", NOW)]
        ),
        list_node=lambda *args, **kwargs: SimpleNamespace(items=[node("node-a")]),
        list_persistent_volume_claim_for_all_namespaces=lambda *args, **kwargs: SimpleNamespace(
            items=[]
        ),
        list_namespaced_persistent_volume_claim=lambda *args, **kwargs: SimpleNamespace(items=[]),
    )
    apps = SimpleNamespace(
        read_namespaced_deployment=lambda *args, **kwargs: deployment,
        list_namespaced_replica_set=lambda *args, **kwargs: SimpleNamespace(items=[old_rs, new_rs]),
    )
    return core, apps


def test_rollout_history_tool_uses_replicaset_and_deployment_timing(monkeypatch) -> None:
    core, apps = fake_apis_rollout()
    monkeypatch.setattr(
        tools, "_load_kubernetes_clients", lambda: (core, apps, SimpleNamespace(), SimpleNamespace())
    )

    report = tools.correlate_rollout_history("app", "web")

    assert "revision 7" in report
    assert "MinimumReplicasUnavailable" in report
    assert "not proof that the rollout caused" in report

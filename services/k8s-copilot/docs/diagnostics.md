# Kubernetes Diagnostics

The diagnostic agent uses read-only Kubernetes API reads to produce a prioritized, evidence-backed report. It does not create, update, delete, execute commands in pods, or change workload configuration.

## Primary Tools

- `run_read_only_diagnostics` collects pods, pod events and conditions, recent container logs, node conditions, PVCs, ReplicaSet and Deployment rollout history, and `metrics.k8s.io` usage. It can also collect Service, Endpoints, Ingress, DNS endpoint, NetworkPolicy, and ResourceQuota signals.
- `check_network_and_quota_signals` focuses on the networking and quota surface.
- `correlate_rollout_history` compares failing pods and warning events with ReplicaSet creation and revision metadata.

## What The Report Contains

Each finding has a severity, category, concise explanation, direct evidence, and additional evidence that would strengthen or narrow the diagnosis. Severity is a triage order, not proof of root cause. Rollout overlap is explicitly labeled as timing correlation rather than causation.

The analysis covers:

- CrashLoopBackOff and repeated non-zero container exits.
- `OOMKilled` termination, memory limits, current usage, and `MemoryPressure`.
- Image pull and container configuration failures, including pull-secret evidence in events.
- Pending pods, `FailedScheduling`, resource requests, node selectors, taints, and allocatable capacity.
- PVC, PersistentVolume, CSI, attach, and mount failures.
- Liveness, readiness, and startup probe failures.
- Memory, disk, and PID pressure on node conditions.
- ResourceQuota exhaustion and quota-related creation failures.
- Service selector/endpoint readiness, Ingress backend readiness, cluster DNS endpoint availability, and NetworkPolicy selector presence.

## Partial Visibility

Every read is handled independently. If RBAC denies a pod, event, log, node, metrics, PVC, rollout, Service, endpoint, Ingress, NetworkPolicy, DNS, or ResourceQuota read, the report includes that check under **Partial Visibility** with the API status. A denial is never interpreted as absence of a problem. The same rule applies when the metrics API is unavailable or the log read limit is reached.

## Evidence Boundaries

- Kubernetes endpoints show whether a Service has ready addresses; they do not prove that a client's DNS query or network path succeeded.
- NetworkPolicy presence does not prove traffic is blocked without rule details and a controlled probe.
- Rollout timing correlation does not prove causation without comparing the revision template or revision events.
- Resource usage is available only when the `metrics.k8s.io` API is installed and RBAC allows the read.

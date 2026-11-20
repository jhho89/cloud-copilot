from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
K8S_AGENTS = REPO_ROOT / "services" / "k8s-copilot" / "src" / "agents"
if str(K8S_AGENTS) not in sys.path:
    sys.path.insert(0, str(K8S_AGENTS))

from dba_agent import tools  # noqa: E402


CLIENT_LIST_JSON = json.dumps(
    [
        {
            "id": 101,
            "addr": "10.0.0.10:41010",
            "name": "orders-worker",
            "age": 3600,
            "idle": 900,
            "flags": "N",
            "db": 0,
            "sub": 0,
            "psub": 0,
            "ssub": 0,
            "qbuf": 1048576,
            "qbuf-free": 0,
            "omem": 10485760,
        },
        {
            "id": 102,
            "addr": "10.0.0.10:41011",
            "name": "orders-worker",
            "age": 3600,
            "idle": 900,
            "flags": "N",
            "db": 0,
            "sub": 0,
            "psub": 0,
            "ssub": 0,
            "qbuf": 0,
            "qbuf-free": 0,
            "omem": 0,
        },
        {
            "id": 103,
            "addr": "10.0.0.11:41020",
            "name": "",
            "age": 90000,
            "idle": 700,
            "flags": "O",
            "db": 0,
            "sub": 120,
            "psub": 0,
            "ssub": 0,
            "qbuf": 0,
            "qbuf-free": 0,
            "omem": 0,
        },
    ]
)
INFO_CLIENTS = json.dumps(
    {
        "Clients": {
            "connected_clients": 95,
            "maxclients": 100,
            "blocked_clients": 1,
            "pubsub_clients": 2,
        }
    }
)
REDIS_INFO_REPLICATION = json.dumps(
    {
        "Replication": {
            "role": "master",
            "master_replid": "8f1e08d3a1c24b7f9dd5f2f6e2b1a7c3",
            "master_repl_offset": 2000000,
            "repl_backlog_size": 1048576,
            "repl_backlog_histlen": 262144,
            "repl_backlog_active": 1,
        }
    }
)
VALKEY_INFO_REPLICATION = json.dumps(
    {
        "Replication": {
            "role": "replica",
            "master_host": "redis-0.redis",
            "master_port": 6379,
            "master_link_status": "up",
            "master_replid": "8f1e08d3a1c24b7f9dd5f2f6e2b1a7c3",
            "master_repl_offset": 3000000,
            "slave_repl_offset": 500000,
            "repl_backlog_size": 1048576,
            "repl_backlog_histlen": 1048576,
            "repl_backlog_active": 1,
        }
    }
)
CLUSTER_INFO = json.dumps(
    {
        "Cluster": {
            "cluster_enabled": 1,
            "cluster_state": "ok",
            "cluster_slots_assigned": 16384,
            "cluster_slots_fail": 0,
            "cluster_known_nodes": 6,
            "cluster_size": 3,
        }
    }
)
CLUSTER_SHARDS = json.dumps(
    [
        {
            "slots": [0, 5460],
            "nodes": [
                {"node-id": "node-a", "role": "master", "endpoint": "10.0.0.1", "port": 6379},
                {"node-id": "node-b", "role": "replica", "endpoint": "10.0.0.2", "port": 6379},
            ],
        }
    ]
)
OUTPUT_BUFFER_LIMIT = '["client-output-buffer-limit", "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"]'
SENTINEL_MASTERS = json.dumps(
    [
        {
            "name": "mymaster",
            "ip": "10.0.0.1",
            "port": "6379",
            "flags": "master",
            "quorum": "2",
            "num-other-sentinels": "0",
            "num-slaves": "1",
        }
    ]
)
SENTINEL_REPLICAS = json.dumps(
    [
        {
            "name": "10.0.0.2:6379",
            "ip": "10.0.0.2",
            "port": "6379",
            "flags": "slave,s_down,disconnected",
        }
    ]
)
SENTINEL_PEERS = json.dumps(
    [
        {
            "name": "sentinel-1",
            "ip": "10.0.0.3",
            "port": "26379",
            "flags": "sentinel",
        }
    ]
)


def _redis_pod(
    name: str = "cache-0",
    namespace: str = "cache",
    image: str = "valkey/valkey:8.0",
    labels: dict[str, str] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            namespace=namespace,
            labels=labels
            or {
                "app.kubernetes.io/name": "valkey",
                "app.kubernetes.io/component": "primary",
            },
            owner_references=[SimpleNamespace(kind="StatefulSet", name="cache")],
        ),
        spec=SimpleNamespace(containers=[SimpleNamespace(name="redis", image=image)]),
        status=SimpleNamespace(phase="Running", pod_ip="10.0.0.1"),
    )


def test_redis_client_topology_allowlist_accepts_only_fixed_read_only_forms() -> None:
    assert tools._validate_redis_command(["CLIENT", "LIST"]) == ["CLIENT", "LIST"]
    assert tools._validate_redis_command(["CLIENT", "ID"]) == ["CLIENT", "ID"]
    assert tools._validate_redis_command(["INFO", "clients"]) == ["INFO", "clients"]
    assert tools._validate_redis_command(["INFO", "replication"]) == ["INFO", "replication"]
    assert tools._validate_redis_command(["CONFIG", "GET", "client-output-buffer-limit"]) == [
        "CONFIG",
        "GET",
        "client-output-buffer-limit",
    ]
    for command in (
        ["CLUSTER", "INFO"],
        ["CLUSTER", "MYID"],
        ["CLUSTER", "SHARDS"],
        ["CLUSTER", "NODES"],
        ["SENTINEL", "MASTERS"],
        ["SENTINEL", "REPLICAS", "mymaster"],
        ["SENTINEL", "SENTINELS", "mymaster"],
    ):
        assert tools._validate_redis_command(command) == command


@pytest.mark.parametrize(
    "command",
    [
        ["CLIENT", "KILL", "ID", "101"],
        ["CLIENT", "SETNAME", "bad"],
        ["CLUSTER", "RESET"],
        ["SENTINEL", "FAILOVER", "mymaster"],
        ["SENTINEL", "REPLICAS", "bad name"],
        ["CONFIG", "SET", "client-output-buffer-limit", "normal 0 0 0"],
    ],
)
def test_redis_client_topology_allowlist_rejects_mutations(command: list[str]) -> None:
    with pytest.raises(Exception):
        tools._validate_redis_command(command)


def test_parse_client_list_supports_json_resp_and_bulk_rows() -> None:
    records, error = tools._parse_client_list(CLIENT_LIST_JSON)
    assert error is None
    assert len(records) == 3
    assert tools._client_record_value(records[0], "name") == "orders-worker"

    resp = "*1\r\n$32\r\nid=5 addr=10.0.0.4:5000 name=api\r\n"
    records, error = tools._parse_client_list(resp)
    assert error is None
    assert records == [{"id": "5", "addr": "10.0.0.4:5000", "name": "api"}]

    records, error = tools._parse_client_list("not-a-client-list")
    assert records == []
    assert "missing a key=value field" in error


def test_client_findings_groups_named_connections_and_ranks_impact() -> None:
    records, _ = tools._parse_client_list(CLIENT_LIST_JSON)
    findings = tools._client_findings(records)

    assert findings[0]["identity"] == "orders-worker"
    assert findings[0]["kind"] == "connection-name"
    assert findings[0]["max_qbuf"] == 1048576
    assert findings[0]["max_omem"] == 10485760
    assert findings[1]["identity"] == "10.0.0.11:41020"
    assert findings[1]["monitor"] is True
    assert findings[1]["unnamed"] is True
    assert "no CLIENT SETNAME" in " ".join(findings[1]["reasons"])


def test_get_redis_client_report_rereads_role_at_execution_time(monkeypatch) -> None:
    pod = _redis_pod(labels={"app.kubernetes.io/name": "valkey", "app.kubernetes.io/component": "primary"})

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def fake_run(namespace, pod_name, cli, command, container=None, port=None):
        fixtures = {
            ("CONFIG", "GET", "client-output-buffer-limit"): OUTPUT_BUFFER_LIMIT,
            ("INFO", "clients"): INFO_CLIENTS,
            ("INFO", "replication"): VALKEY_INFO_REPLICATION,
            ("CLIENT", "LIST"): CLIENT_LIST_JSON,
            ("CLIENT", "ID"): 103,
            ("CLUSTER", "INFO"): json.dumps({"Cluster": {"cluster_enabled": 0}}),
        }
        return fixtures[tuple(command)]

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "valkey-cli")
    monkeypatch.setattr(tools, "_run_redis_command", fake_run)

    result = tools.get_redis_client_report("cache", "cache-0")

    assert "metadata-role=primary" in result
    assert "Runtime role: replica" in result
    assert "Runtime role was re-read from INFO replication during this report." in result
    assert "replication lag evidence: 2500000 bytes" in result
    assert "Replication backlog: size=1048576, used=1048576, headroom=0 bytes." in result
    assert "backlog is full" in result
    assert "maxclients headroom: 5 connections (95.0% used)" in result
    assert "connection-name `orders-worker`" in result
    assert "omem reached 10485760 bytes" in result
    assert "stuck in MONITOR mode" in result
    assert "Use `CLIENT SETNAME`" in result
    assert "Recommendations are advisory" in result
    assert "No changes were made" in result


def test_get_redis_client_report_attributes_cluster_shard_from_live_topology(monkeypatch) -> None:
    pod = _redis_pod()

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def fake_run(namespace, pod_name, cli, command, container=None, port=None):
        fixtures = {
            ("CONFIG", "GET", "client-output-buffer-limit"): OUTPUT_BUFFER_LIMIT,
            ("INFO", "clients"): json.dumps({"Clients": {"connected_clients": 2, "maxclients": 100, "blocked_clients": 0}}),
            ("INFO", "replication"): REDIS_INFO_REPLICATION,
            ("CLIENT", "LIST"): json.dumps([]),
            ("CLIENT", "ID"): 1,
            ("CLUSTER", "INFO"): CLUSTER_INFO,
            ("CLUSTER", "MYID"): "node-a",
            ("CLUSTER", "SHARDS"): CLUSTER_SHARDS,
        }
        return fixtures[tuple(command)]

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "redis-cli")
    monkeypatch.setattr(tools, "_run_redis_command", fake_run)

    result = tools.get_redis_client_report("cache", "cache-0")

    assert "Cluster mode: yes" in result
    assert "cluster_state=ok" in result
    assert "CLUSTER MYID: node-a" in result
    assert "Current pod is attributed to shard 1 as master." in result
    assert "slots=0-5460" in result
    assert "node-b (replica, 10.0.0.2:6379)" in result
    assert "require checking every shard master" in result


def test_get_redis_client_report_reads_sentinel_quorum_and_failover(monkeypatch) -> None:
    sentinel_pod = _redis_pod(
        name="sentinel-0",
        image="redis:7.2",
        labels={"app.kubernetes.io/name": "redis", "app.kubernetes.io/component": "sentinel"},
    )

    class DiscoveryApi:
        def list_namespaced_pod(self, namespace):
            return SimpleNamespace(items=[sentinel_pod])

    def fake_run(namespace, pod_name, cli, command, container=None, port=None):
        fixtures = {
            ("SENTINEL", "MASTERS"): SENTINEL_MASTERS,
            ("SENTINEL", "REPLICAS", "mymaster"): SENTINEL_REPLICAS,
            ("SENTINEL", "SENTINELS", "mymaster"): SENTINEL_PEERS,
        }
        return fixtures[tuple(command)]

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "redis-cli")
    monkeypatch.setattr(tools, "_run_redis_command", fake_run)

    result = tools.get_redis_client_report("cache")

    assert "Sentinel-managed topology:" in result
    assert "Master `mymaster`: primary at 10.0.0.1:6379" in result
    assert "quorum=2" in result
    assert "quorum 2 is not currently reachable" in result
    assert "Failure or disconnect flag is present; this can lead to failover." in result
    assert "s_down" in result
    assert "Peer Sentinel observations: 1" in result
    assert "No changes were made" in result


def test_get_redis_client_report_reports_malformed_client_and_sentinel_responses(monkeypatch) -> None:
    pod = _redis_pod(name="cache-1", image="redis:7.2")
    sentinel_pod = _redis_pod(
        name="sentinel-1",
        image="redis:7.2",
        labels={"app.kubernetes.io/name": "redis", "app.kubernetes.io/component": "sentinel"},
    )

    class DiscoveryApi:
        def list_namespaced_pod(self, namespace):
            return SimpleNamespace(items=[pod, sentinel_pod])

    def fake_run(namespace, pod_name, cli, command, container=None, port=None):
        if pod_name == "sentinel-1":
            return "{not json"
        return "not-a-client-list"

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "redis-cli")
    monkeypatch.setattr(tools, "_run_redis_command", fake_run)

    result = tools.get_redis_client_report("cache")

    assert "CLIENT LIST response is missing a key=value field." in result
    assert "Runtime role: unknown" in result
    assert "Sentinel evidence errors:" in result
    assert "SENTINEL MASTERS: Invalid JSON response" in result
    assert "No changes were made" in result


def test_get_redis_client_report_reports_rbac_denial(monkeypatch) -> None:
    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            raise RuntimeError("pods/exec is forbidden")

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())

    result = tools.get_redis_client_report("cache", "cache-0")

    assert result.startswith("Error discovering or reading Redis/Valkey pods:")
    assert "pods/exec is forbidden" in result
    assert "No changes were made" not in result

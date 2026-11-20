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


INFO_MEMORY = json.dumps(
    {
        "Memory": {
            "used_memory": 1_073_741_824,
            "used_memory_human": "1.00G",
            "maxmemory": 1_073_741_824,
            "maxmemory_policy": "volatile-lru",
            "mem_fragmentation_ratio": 1.7,
            "mem_allocator": "jemalloc",
            "activedefrag_running": 0,
        }
    }
)
INFO_STATS = json.dumps(
    {
        "Stats": {
            "evicted_keys": 12,
            "expired_keys": 1500,
            "keyspace_hits": 900,
            "keyspace_misses": 100,
            "instantaneous_ops_per_sec": 2400,
        }
    }
)
INFO_PERSISTENCE = json.dumps(
    {
        "Persistence": {
            "loading": 0,
            "rdb_bgsave_in_progress": 0,
            "rdb_last_bgsave_status": "ok",
            "aof_enabled": 0,
            "aof_rewrite_in_progress": 0,
            "latest_fork_usec": 250_000,
        }
    }
)
INFO_REPLICATION = json.dumps(
    {
        "Replication": {
            "role": "master",
            "connected_slaves": 1,
            "master_replid": "8f1e08d3a1c24b7f9dd5f2f6e2b1a7c3",
            "master_repl_offset": 2_000_000,
            "repl_backlog_size": 1_048_576,
            "repl_backlog_histlen": 262_144,
            "repl_backlog_active": 1,
        }
    }
)
MEMORY_STATS = json.dumps({"fragmentation": 1.7, "fragmentation.bytes": 268_435_456, "keys.count": 250_000})
INFO_CLIENTS = json.dumps(
    {
        "Clients": {
            "connected_clients": 95,
            "maxclients": 100,
            "blocked_clients": 1,
        }
    }
)
CLIENT_LIST = json.dumps(
    [
        {
            "id": index,
            "addr": f"10.0.0.20:{42000 + index}",
            "name": "orders-worker" if index == 11 else "",
            "age": 172_800 if index == 11 else 120,
            "idle": 900 if index == 11 else 2,
            "flags": "N",
            "sub": 0,
            "psub": 0,
            "ssub": 0,
            "qbuf": 1_048_576 if index == 11 else 0,
            "omem": 10_485_760 if index == 11 else 0,
            "cmd": "get" if index < 10 else "set",
        }
        for index in range(12)
    ]
)
BUFFER_LIMITS = '["client-output-buffer-limit", "normal 268435456 67108864 60"]'
CLUSTER_INFO = json.dumps({"Cluster": {"cluster_enabled": 1, "cluster_state": "ok", "cluster_slots_fail": 0}})
BIGKEYS_OUTPUT = (
    "Biggest hash found 'user:profile' has 100000 fields\n"
    "Biggest list found 'queue:jobs' has 100000 items\n"
    "Sampled 3000 keys in the keyspace!\n"
)
SECRET = "redis-secret-do-not-leak"


@pytest.mark.parametrize(
    "command",
    [
        ["KEYS", "*"],
        ["SCAN", "0"],
        ["DEBUG", "SLEEP", "0"],
        ["CONFIG", "SET", "maxmemory", "1Gi"],
        ["LATENCY", "RESET"],
        ["SLOWLOG", "RESET"],
        ["CLIENT", "KILL", "ID", "1"],
        ["SAVE"],
        ["FLUSHDB"],
        ["FLUSHALL"],
    ],
)
def test_recommendation_path_cannot_issue_blocked_redis_commands(command: list[str]) -> None:
    with pytest.raises(Exception):
        tools._validate_redis_command(command)


def _redis_pod() -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name="cache-0",
            namespace="cache",
            labels={"app.kubernetes.io/name": "valkey", "app.kubernetes.io/component": "primary"},
            owner_references=[SimpleNamespace(kind="StatefulSet", name="cache")],
        ),
        spec=SimpleNamespace(
            containers=[
                SimpleNamespace(
                    name="redis",
                    image="valkey/valkey:8.0",
                    resources=SimpleNamespace(limits={"memory": "2Gi"}),
                )
            ]
        ),
        status=SimpleNamespace(phase="Running"),
    )


def _test_run_redis_command(command: list[str], captured: list[list[str]]) -> str:
    tools._validate_redis_command(command)
    captured.append(list(command))
    fixtures = {
        ("SLOWLOG", "GET", "25"): json.dumps(
            [
                ["1", 1790000000, 3_000_000, ["KEYS", "*"], "10.0.0.2:51000", "jobs-worker"],
                ["2", 1790000001, 250_000, ["HGETALL", "user:profile"], "10.0.0.3:51001", "profile-worker"],
                ["3", 1790000002, 250_000, ["DEL", "queue:jobs"], "10.0.0.4:51002", "cleanup-worker"],
            ]
        ),
        ("SLOWLOG", "LEN"): "128",
        ("CONFIG", "GET", "slowlog-max-len"): '["slowlog-max-len", "128"]',
        ("INFO", "memory"): INFO_MEMORY,
        ("INFO", "stats"): INFO_STATS,
        ("INFO", "persistence"): INFO_PERSISTENCE,
        ("INFO", "replication"): INFO_REPLICATION,
        ("MEMORY", "STATS"): MEMORY_STATS,
        ("INFO", "clients"): INFO_CLIENTS,
        ("CONFIG", "GET", "client-output-buffer-limit"): BUFFER_LIMITS,
        ("CLIENT", "LIST"): CLIENT_LIST,
        ("CLIENT", "ID"): 12,
        ("CLUSTER", "INFO"): CLUSTER_INFO,
        ("LATENCY", "HISTORY", "fork"): "[[1790000010, 250]]",
        ("LATENCY", "DOCTOR"): "fork observed; check persistence timing.",
        ("CONFIG", "GET", "io-threads"): '["io-threads", "2"]',
    }
    return fixtures[tuple(command)]


def test_get_redis_remediation_recommendations_builds_evidence_linked_advice(monkeypatch) -> None:
    pod = _redis_pod()
    commands: list[list[str]] = []
    argvs: list[list[str]] = []

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def fake_exec(namespace, pod_name, argv, container, operation, timeout_seconds):
        argvs.append(list(argv))
        assert namespace == "cache" and pod_name == "cache-0"
        assert argv[0] == "valkey-cli"
        assert argv[1:] == ["--bigkeys", "-i", "0.01"]
        assert operation == "redis_scan_based_key_analysis"
        assert timeout_seconds == 120
        return BIGKEYS_OUTPUT

    monkeypatch.setenv("VALKEYCLI_AUTH", SECRET)
    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "valkey-cli")
    monkeypatch.setattr(
        tools,
        "_run_redis_command",
        lambda namespace, pod_name, cli, command, container=None, port=None: _test_run_redis_command(
            command, commands
        ),
    )
    monkeypatch.setattr(tools, "_pod_exec", fake_exec)

    result = tools.get_redis_remediation_recommendations("cache", "cache-0")

    for command in commands:
        tools._validate_redis_command(command)
    assert argvs == [["valkey-cli", "--bigkeys", "-i", "0.01"]]
    assert SECRET not in result
    assert "--pass" not in result
    assert "-a" not in result

    assert "SCAN-based --bigkeys sampling: yes (keys sampled: 3000)." in result
    assert "Kubernetes memory limit: 2Gi on container redis (2.0GiB)." in result
    assert "Replace KEYS with cursor-paginated SCAN" in result
    assert "SLOWLOG GET 25 returned 1 entr(ies); max duration 3000.000ms" in result
    assert "Bound HGETALL collection reads" in result
    assert "matching hash key `user:profile` has 100000 fields" in result
    assert "Use UNLINK for large deletions" in result
    assert "matching list key `queue:jobs` has 100000 items" in result
    assert "Evaluate client-side pipelining or batching" in result
    assert "last-command histogram was GET=10 of 12." in result
    assert "Recover logical memory headroom" in result
    assert "used_memory=1.0GiB is 100.0% of maxmemory=1.0GiB." in result
    assert "Choose an intentional eviction policy" in result
    assert "evicted_keys=12" in result
    assert "Audit TTL hygiene" in result
    assert "expired_keys=1500" in result
    assert "Evaluate active defragmentation" in result
    assert "fragmentation ratio=1.7" in result
    assert "Reduce persistence fork and copy-on-write impact" in result
    assert "latest_fork_usec=250000" in result
    assert "Review the RDB recovery window" in result
    assert "Move backup pressure off the primary" in result
    assert "connected_slaves=1" in result
    assert "Control input backpressure for offending clients" in result
    assert "`orders-worker` reported max qbuf=1048576 bytes." in result
    assert "Fix slow consumers and output buffering" in result
    assert "max omem=10485760 bytes." in result
    assert "Fix idle or leaked connection pools" in result
    assert "age=172800s idle=900s." in result
    assert "Name client connections for ownership" in result
    assert "11 of 12 parsed connection(s) have no name." in result
    assert "Confirm blocking-command consumers" in result
    assert "blocked_clients=1." in result
    assert "Reduce connection churn or raise maxclients safely" in result
    assert "(95.0% used)." in result

    assert result.count("No change was applied.") >= 17
    assert "All recommendations are advisory" in result
    assert "No changes were made; this analysis is advisory only." in result


def test_recommendation_engine_reports_unhealthy_cluster_and_gaps(monkeypatch) -> None:
    pod = _redis_pod()

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "redis-cli")
    monkeypatch.setattr(tools, "_run_redis_command", lambda *args, **kwargs: "[]")
    monkeypatch.setattr(tools, "_pod_exec", lambda *args, **kwargs: "")

    result = tools.get_redis_remediation_recommendations("cache", "cache-0", include_bigkeys=False)

    assert "SCAN-based --bigkeys sampling: not available" in result
    assert "Evidence gaps/errors:" in result
    assert "Stabilize cluster health before performance tuning" not in result
    assert "No threshold-crossing recommendation was supported by this evidence. No change was applied." in result
    assert "All recommendations are advisory" in result


def test_redis_remediation_recommendations_flag_unhealthy_cluster(monkeypatch) -> None:
    pod = _redis_pod()

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def run(command, *args, **kwargs):
        if tuple(command) == ("CLUSTER", "INFO"):
            return json.dumps({"Cluster": {"cluster_enabled": 1, "cluster_state": "fail", "cluster_slots_fail": 12}})
        return "[]"

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "redis-cli")
    monkeypatch.setattr(tools, "_run_redis_command", lambda namespace, pod_name, cli, command, container=None, port=None: run(command))
    monkeypatch.setattr(tools, "_pod_exec", lambda *args, **kwargs: "")

    result = tools.get_redis_remediation_recommendations("cache", "cache-0", include_bigkeys=False)

    assert "Stabilize cluster health before performance tuning" in result
    assert "cluster_state=fail; cluster_slots_fail=12." in result
    assert "No change was applied." in result


def test_recommendations_include_bigkeys_false_runs_no_keyspace_scan(monkeypatch) -> None:
    pod = _redis_pod()

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "redis-cli")
    monkeypatch.setattr(tools, "_run_redis_command", lambda *args, **kwargs: "[]")

    def unexpected_exec(*args, **kwargs):
        raise AssertionError("A keyspace scan must not run when include_bigkeys is false.")

    monkeypatch.setattr(tools, "_pod_exec", unexpected_exec)
    result = tools.get_redis_remediation_recommendations("cache", "cache-0", include_bigkeys=False)

    assert "SCAN-based --bigkeys sampling: not available" in result
    assert "No changes were made; this analysis is advisory only." in result


def test_invalid_sample_interval_is_rejected() -> None:
    assert "Invalid sample_interval_seconds" in tools.get_redis_remediation_recommendations(
        "cache",
        "cache-0",
        sample_interval_seconds=1.5,
    )

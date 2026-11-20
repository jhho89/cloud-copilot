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


REDIS_INFO_MEMORY = json.dumps(
    {
        "Memory": {
            "used_memory": 10485760,
            "used_memory_human": "10.00M",
            "maxmemory": 0,
            "maxmemory_human": "0B",
            "maxmemory_policy": "noeviction",
            "mem_fragmentation_ratio": 1.82,
            "mem_allocator": "libc",
            "activedefrag_running": 0,
            "total_system_memory": 17179869184,
        }
    }
)
REDIS_INFO_STATS = json.dumps(
    {
        "Stats": {
            "evicted_keys": 20,
            "keyspace_hits": 980,
            "keyspace_misses": 120,
            "instantaneous_ops_per_sec": 2100,
        }
    }
)
REDIS_INFO_PERSISTENCE = json.dumps(
    {
        "Persistence": {
            "loading": 0,
            "rdb_bgsave_in_progress": 1,
            "rdb_last_bgsave_status": "ok",
            "aof_enabled": 1,
            "aof_rewrite_in_progress": 0,
            "aof_last_bgrewrite_status": "ok",
            "aof_last_write_status": "ok",
            "latest_fork_usec": 875000,
        }
    }
)
VALKEY_INFO_REPLICATION = json.dumps({"Replication": {"role": "master"}})
VALKEY_MEMORY_STATS = json.dumps(
    {
        "peak.allocated": 12582912,
        "dataset.bytes": 8388608,
        "fragmentation": 1.75,
        "fragmentation.bytes": 3670016,
        "keys.count": 190000,
    }
)

REDIS_BIGKEYS_OUTPUT = """\
[00.00%] Biggest string seen 'cache:banner' has 4096 bytes
[12.50%] Biggest list seen 'queue:jobs' has 120000 items
[100.00%] sampled 8 keys
-------- SUMMARY -------
Sampled 8 keys in the keyspace!
Total key length in bytes is 2048 (avg len 256.00)
Biggest string found 'cache:banner' has 4096 bytes
Biggest   list found 'queue:jobs' has 120000 items
"""

VALKEY_MEMKEYS_OUTPUT = """\
[00.00%] Biggest string seen 'session:blob' has 1048576 bytes
[100.00%] sampled 24 keys
-------- SUMMARY -------
Sampled 24 keys in the keyspace!
Biggest string found 'session:blob' has 1048576 bytes
Biggest   hash found 'user:profile' has 64000 fields
"""


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
            labels=labels or {"app.kubernetes.io/name": "valkey", "app.kubernetes.io/component": "primary"},
            owner_references=[SimpleNamespace(kind="StatefulSet", name="cache")],
        ),
        spec=SimpleNamespace(containers=[SimpleNamespace(name="redis", image=image)]),
        status=SimpleNamespace(phase="Running"),
    )


def test_redis_allowlist_accepts_read_only_memory_and_latency_commands() -> None:
    assert tools._validate_redis_command(["INFO", "memory"]) == ["INFO", "memory"]
    assert tools._validate_redis_command(["MEMORY", "STATS"]) == ["MEMORY", "STATS"]
    assert tools._validate_redis_command(["LATENCY", "DOCTOR"]) == ["LATENCY", "DOCTOR"]
    assert tools._validate_redis_command(["LATENCY", "HISTORY", "fork"]) == [
        "LATENCY",
        "HISTORY",
        "fork",
    ]
    assert tools._validate_redis_command(["CONFIG", "GET", "io-threads"]) == [
        "CONFIG",
        "GET",
        "io-threads",
    ]


@pytest.mark.parametrize(
    "command",
    [
        ["KEYS", "*"],
        ["SCAN", "0"],
        ["DEBUG", "SLEEP", "0"],
        ["CONFIG", "SET", "maxmemory", "100000"],
        ["LATENCY", "RESET"],
        ["SLOWLOG", "RESET"],
        ["CLIENT", "KILL", "id", "1"],
        ["SAVE"],
        ["FLUSHDB"],
        ["FLUSHALL"],
    ],
)
def test_redis_allowlist_rejects_manual_scan_debug_and_mutations(command: list[str]) -> None:
    with pytest.raises(Exception):
        tools._validate_redis_command(command)


def test_redis_cli_argv_expands_allowlist_without_exposing_credentials(monkeypatch) -> None:
    monkeypatch.setenv("VALKEYCLI_AUTH", "super-secret")
    argv = tools._redis_cli_argv("valkey-cli", ["MEMORY", "STATS"])
    assert argv == ["valkey-cli", "--json", "MEMORY", "STATS"]
    assert "super-secret" not in argv
    assert "--pass" not in argv
    assert "-a" not in argv


def test_parse_info_sections_supports_redis_json_and_valkey_resp() -> None:
    json_fields = tools._parse_info_sections(REDIS_INFO_MEMORY)
    resp_fields = tools._parse_info_sections(
        "# Memory\r\nused_memory_human:10.00M\r\nmaxmemory_policy:noeviction\r\n"
    )

    assert json_fields["used_memory_human"] == "10.00M"
    assert json_fields["maxmemory_policy"] == "noeviction"
    assert resp_fields["used_memory_human"] == "10.00M"


def test_get_redis_memory_report_interprets_pressure_and_fork(monkeypatch) -> None:
    pod = _redis_pod()
    commands = []

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def fake_run(namespace, pod_name, cli, command, container=None, port=None):
        commands.append(list(command))
        fixtures = {
            ("INFO", "memory"): REDIS_INFO_MEMORY,
            ("INFO", "stats"): REDIS_INFO_STATS,
            ("INFO", "persistence"): REDIS_INFO_PERSISTENCE,
            ("INFO", "replication"): VALKEY_INFO_REPLICATION,
            ("MEMORY", "STATS"): VALKEY_MEMORY_STATS,
        }
        return fixtures[tuple(command)]

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "valkey-cli")
    monkeypatch.setattr(tools, "_run_redis_command", fake_run)

    result = tools.get_redis_memory_report("cache", "cache-0")

    assert commands == [
        ["INFO", "memory"],
        ["INFO", "stats"],
        ["INFO", "persistence"],
        ["INFO", "replication"],
        ["MEMORY", "STATS"],
    ]
    assert "### cache/cache-0 (CLI: valkey-cli)" in result
    assert "runtime-role=master" in result
    assert "maxmemory: 0" in result
    assert "maxmemory_policy: noeviction" in result
    assert "no in-process limit" in result
    assert "writes fail instead of evicting data" in result
    assert "mem_fragmentation_ratio=1.82 is high" in result
    assert "libc" in result
    assert "latest_fork_usec=875000 (875.0ms)" in result
    assert "copy-on-write" in result
    assert "they have not been applied" in result
    assert "No changes were made" in result


def test_get_redis_memory_report_reports_unavailable_evidence_and_unknown_role(monkeypatch) -> None:
    pod = _redis_pod(name="redis-1", image="redis:7.2")

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def failing_run(namespace, pod_name, cli, command, container=None, port=None):
        raise RuntimeError('ERR unknown command "INFO"')

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "redis-cli")
    monkeypatch.setattr(tools, "_run_redis_command", failing_run)

    result = tools.get_redis_memory_report("cache", "redis-1")

    assert "cache/redis-1" in result
    assert "runtime-role=unknown" in result
    assert "memory evidence is unavailable" in result
    assert "INFO memory:" in result
    assert "No changes were made" in result


def test_get_redis_memory_reports_rbac_denial_without_recommendation(monkeypatch) -> None:
    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            raise RuntimeError("pods is forbidden")

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())

    result = tools.get_redis_memory_report("cache", "cache-0")

    assert result.startswith("Error discovering or reading Redis/Valkey pods:")
    assert "pods is forbidden" in result
    assert "Recommendations" not in result


def test_get_redis_memory_report_reports_no_pods(monkeypatch) -> None:
    class DiscoveryApi:
        def list_namespaced_pod(self, namespace):
            return SimpleNamespace(items=[])

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())

    result = tools.get_redis_memory_report("cache")

    assert result.startswith("No Redis or Valkey pods found.")


def test_parse_bigkeys_output_supports_redis_and_valkey_formats() -> None:
    redis_findings, redis_sampled = tools._parse_bigkeys_output(REDIS_BIGKEYS_OUTPUT)
    valkey_findings, valkey_sampled = tools._parse_bigkeys_output(VALKEY_MEMKEYS_OUTPUT)

    assert redis_sampled == 8
    assert redis_findings == [
        {"key_type": "string", "key": "cache:banner", "size": 4096, "size_unit": "bytes"},
        {"key_type": "list", "key": "queue:jobs", "size": 120000, "size_unit": "items"},
    ]
    assert valkey_sampled == 24
    assert valkey_findings == [
        {"key_type": "string", "key": "session:blob", "size": 1048576, "size_unit": "bytes"},
        {"key_type": "hash", "key": "user:profile", "size": 64000, "size_unit": "fields"},
    ]


def test_analyze_redis_bigkeys_uses_only_scan_based_cli_mode(monkeypatch) -> None:
    pod = _redis_pod()
    captured = {}

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def fake_exec(namespace, pod_name, argv, container, operation, timeout_seconds, target_path=None):
        captured.update(
            {"argv": argv, "operation": operation, "timeout": timeout_seconds, "namespace": namespace}
        )
        return REDIS_BIGKEYS_OUTPUT

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "redis-cli")
    monkeypatch.setattr(
        tools,
        "_run_redis_command",
        lambda *args, **kwargs: json.dumps({"Replication": {"role": "master"}}),
    )
    monkeypatch.setattr(tools, "_pod_exec", fake_exec)

    result = tools.analyze_redis_bigkeys("cache", "cache-0", mode="bigkeys")

    assert captured["argv"] == ["redis-cli", "--bigkeys", "-i", "0.01"]
    assert captured["operation"] == "redis_scan_based_key_analysis"
    assert captured["timeout"] == 120
    assert "runtime-role=master" in result
    assert "queue:jobs" in result
    assert "120000 items" in result
    assert "Keys sampled by the CLI: 8" in result
    assert "The analyzer itself iterates the keyspace" in result
    assert "No KEYS, manual SCAN, DEBUG, or mutation command was used" in result
    assert "No changes were made" in result


def test_analyze_redis_bigkeys_supports_valkey_memkeys(monkeypatch) -> None:
    pod = _redis_pod()
    captured = {}

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def fake_exec(namespace, pod_name, argv, container, operation, timeout_seconds, target_path=None):
        captured["argv"] = argv
        return VALKEY_MEMKEYS_OUTPUT

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "valkey-cli")
    monkeypatch.setattr(
        tools,
        "_run_redis_command",
        lambda *args, **kwargs: json.dumps({"Replication": {"role": "replica"}}),
    )
    monkeypatch.setattr(tools, "_pod_exec", fake_exec)

    result = tools.analyze_redis_bigkeys("cache", "cache-1", mode="memkeys")

    assert captured["argv"] == ["valkey-cli", "--memkeys", "-i", "0.01"]
    assert "runtime-role=replica" in result
    assert "session:blob" in result
    assert "1048576 bytes" in result


def test_analyze_redis_bigkeys_requires_one_pod_and_reports_rbac_denial(monkeypatch) -> None:
    assert "namespace and pod are required" in tools.analyze_redis_bigkeys()
    assert "namespace and pod are required" in tools.analyze_redis_bigkeys("cache")

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            raise RuntimeError("pods/exec is forbidden")

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())

    result = tools.analyze_redis_bigkeys("cache", "cache-0")

    assert result.startswith("Error analyzing Redis/Valkey big keys in cache/cache-0:")
    assert "pods/exec is forbidden" in result


def test_get_redis_latency_report_correlates_events_and_io_threads(monkeypatch) -> None:
    pod = _redis_pod()
    commands = []

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def fake_run(namespace, pod_name, cli, command, container=None, port=None):
        commands.append(list(command))
        key = tuple(command)
        fixtures = {
            ("INFO", "memory"): REDIS_INFO_MEMORY,
            ("INFO", "stats"): REDIS_INFO_STATS,
            ("INFO", "persistence"): REDIS_INFO_PERSISTENCE,
            ("INFO", "replication"): VALKEY_INFO_REPLICATION,
            ("MEMORY", "STATS"): VALKEY_MEMORY_STATS,
            ("LATENCY", "HISTORY", "fork"): "[[1790000000, 220]]",
            ("LATENCY", "HISTORY", "eviction-cycle"): "[[1790000010, 95]]",
            ("LATENCY", "HISTORY", "command"): "[]",
            ("LATENCY", "DOCTOR"): "Dave, I have observed latency spikes related to fork.",
            ("CONFIG", "GET", "io-threads"): '["io-threads", "4"]',
            ("SLOWLOG", "GET", "25"): '[["1", 1790000020, 3500, ["SET", "session:blob"]]]',
            ("SLOWLOG", "LEN"): "25",
            ("CONFIG", "GET", "slowlog-max-len"): '["slowlog-max-len", "25"]',
        }
        return fixtures[key]

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "valkey-cli")
    monkeypatch.setattr(tools, "_run_redis_command", fake_run)

    result = tools.get_redis_latency_report("cache", "cache-0")

    assert commands[0:5] == [
        ["INFO", "memory"],
        ["INFO", "stats"],
        ["INFO", "persistence"],
        ["INFO", "replication"],
        ["MEMORY", "STATS"],
    ]
    assert ["LATENCY", "HISTORY", "fork"] in commands
    assert ["LATENCY", "DOCTOR"] in commands
    assert ["CONFIG", "GET", "io-threads"] in commands
    assert "runtime-role=master" in result
    assert "fork: 1 event(s); latest 220.000ms" in result
    assert "eviction history plus evicted_keys>0" in result.lower()
    assert "instantaneous_ops_per_sec: 2100" in result
    assert "Slow log is saturated" in result
    assert "SET (1 entries, 3.500ms)" in result
    assert "io-threads=4" in result
    assert "this value does not prove a cause" in result
    assert "Dave, I have observed latency spikes" in result
    assert "they have not been applied" in result
    assert "No changes were made" in result


def test_parse_latency_history_supports_resp_and_empty_responses() -> None:
    events = tools._parse_latency_history("*1\r\n*2\r\n:1790000000\r\n:20\r\n")
    assert events == [{"timestamp": 1790000000, "duration_ms": 20.0}]
    assert tools._parse_latency_history("[]") == []
    assert tools._parse_latency_history("") == []


def test_get_redis_latency_report_labels_unavailable_role_and_evidence(monkeypatch) -> None:
    pod = _redis_pod(name="redis-2", image="redis:7.2")

    class DiscoveryApi:
        def read_namespaced_pod(self, name, namespace):
            return pod

    def failing_run(namespace, pod_name, cli, command, container=None, port=None):
        raise RuntimeError("LATENCY extension unavailable")

    monkeypatch.setattr(tools, "_load_kubernetes_client", lambda: DiscoveryApi())
    monkeypatch.setattr(tools, "_detect_redis_cli", lambda *args, **kwargs: "redis-cli")
    monkeypatch.setattr(tools, "_run_redis_command", failing_run)

    result = tools.get_redis_latency_report("cache", "redis-2")

    assert "cache/redis-2" in result
    assert "runtime-role=unknown" in result
    assert "io-threads: unavailable" in result
    assert "LATENCY HISTORY fork: " in result
    assert "LATENCY DOCTOR: " in result
    assert "No changes were made" in result

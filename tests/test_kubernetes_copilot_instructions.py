from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATOR = REPO_ROOT / "services" / "k8s-copilot" / "src" / "agents" / "kubernetes_copilot" / "agent.py"


def test_database_idle_cost_orchestration_instructions() -> None:
    instruction = ORCHESTRATOR.read_text(encoding="utf-8")

    assert "collect_database_idle_assessment" in instruction
    assert "output_mode='json'" in instruction
    assert "diagnostic_agent" in instruction
    assert "estimate_database_cost_impact" in instruction
    assert "Cost Copilot cannot discover pods" in instruction
    assert "process only that supplied envelope" in instruction
    assert "per-workload cost attribution" in instruction
    assert "never add or convert currencies" in instruction
    assert "advisory" in instruction
    assert "no automatic shutdown or mutation" in instruction

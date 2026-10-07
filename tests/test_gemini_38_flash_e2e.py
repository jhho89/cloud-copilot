"""End-to-end coverage for the Gemini 3.8 Flash upgrade.

Each production agent is loaded and run through the ADK in-memory runner.
Outbound generateContent calls are served by a local Gemini stand-in so the
test checks the real agent graph, the model id ADK resolves, and the URL the
Google GenAI client actually requests.

Requires the k8s-copilot and cost-copilot Python dependencies plus pytest.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
K8S_AGENTS = REPO_ROOT / "services" / "k8s-copilot" / "src" / "agents"
COST_AGENTS = REPO_ROOT / "services" / "cost-copilot" / "src" / "agents"
EXPECTED_MODEL = "gemini-3.8-flash"
STUB_REPLY = "upgrade-ok"

_captured_paths: list[str] = []


class _GeminiStub(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        _captured_paths.append(self.path)
        payload = {
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [{"text": STUB_REPLY}],
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


@pytest.fixture(scope="module")
def gemini_stub():
    os.environ["GOOGLE_API_KEY"] = "test-key"
    os.environ["COST_COPILOT_URL"] = "http://127.0.0.1:9"
    os.environ.pop("GOOGLE_GENAI_USE_VERTEXAI", None)
    os.environ.pop("GOOGLE_GENAI_USE_ENTERPRISE", None)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _GeminiStub)
    port = server.server_address[1]
    os.environ["GOOGLE_GEMINI_BASE_URL"] = f"http://127.0.0.1:{port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()


def _load_agents():
    for path in (str(K8S_AGENTS), str(COST_AGENTS)):
        if path not in sys.path:
            sys.path.insert(0, path)

    from cost_analysis_agent.agent import CostAnalysisAgent
    from cost_copilot.agent import CostCopilot
    from cost_discovery_agent.agent import CostDiscoveryAgent
    from diagnostic_agent.agent import DiagnosticAgent
    from investigator_agent.agent import InvestigatorAgent
    from kubernetes_copilot.agent import KubernetesCopilot
    from remediation_advisor_agent.agent import RemediationAdvisorAgent

    return [
        DiagnosticAgent,
        InvestigatorAgent,
        RemediationAdvisorAgent,
        KubernetesCopilot,
        CostDiscoveryAgent,
        CostAnalysisAgent,
        CostCopilot,
    ]


def _texts(event) -> list[str]:
    content = getattr(event, "content", None)
    parts = getattr(content, "parts", None) or []
    return [part.text for part in parts if getattr(part, "text", None)]


def _run_turn(agent) -> list[str]:
    from google.adk.runners import InMemoryRunner
    from google.genai import types

    async def _run() -> list[str]:
        runner = InMemoryRunner(agent=agent, app_name=agent.name)
        session = await runner.session_service.create_session(
            app_name=agent.name,
            user_id="e2e-user",
            session_id=f"e2e-{agent.name}",
        )
        texts: list[str] = []
        message = types.Content(
            role="user",
            parts=[types.Part(text="Reply with the stub sentence only.")],
        )
        async for event in runner.run_async(
            user_id="e2e-user",
            session_id=session.id,
            new_message=message,
        ):
            texts.extend(_texts(event))
        return texts

    return asyncio.run(_run())


def test_repository_no_longer_references_gemini_2_5():
    forbidden = ("gemini-2.5", "Gemini 2.5")
    offenders: list[str] = []
    scan_roots = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "services",
        REPO_ROOT / "docs",
    ]
    for root in scan_roots:
        files = [root] if root.is_file() else root.rglob("*")
        for path in files:
            if not path.is_file():
                continue
            if path.suffix.lower() not in {".py", ".md", ".ts", ".tsx", ".json", ".yml", ".yaml"}:
                continue
            if ".test." in path.name or "node_modules" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if any(token in text for token in forbidden):
                offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []


def test_every_agent_calls_gemini_3_8_flash(gemini_stub):
    del gemini_stub
    agents = _load_agents()
    assert agents, "expected the production copilot agents"

    for agent in agents:
        assert agent.model == EXPECTED_MODEL
        resolved = agent.canonical_model
        assert resolved.model == EXPECTED_MODEL
        assert type(resolved).__name__ == "Gemini"

        _captured_paths.clear()
        texts = _run_turn(agent)

        assert _captured_paths, f"{agent.name} did not call Gemini"
        assert all(
            path == f"/v1beta/models/{EXPECTED_MODEL}:generateContent"
            for path in _captured_paths
        ), _captured_paths
        assert STUB_REPLY in texts


def test_orchestrators_keep_sub_agents_on_gemini_3_8_flash(gemini_stub):
    del gemini_stub
    from google.adk.agents.llm_agent import LlmAgent

    agents = {agent.name: agent for agent in _load_agents()}

    k8s_sub_agents = agents["kubernetes_copilot"].sub_agents
    assert [agent.name for agent in k8s_sub_agents[:3]] == [
        "diagnostic_agent",
        "investigator_agent",
        "remediation_advisor_agent",
    ]
    assert k8s_sub_agents[-1].name == "cost_copilot"
    for sub_agent in k8s_sub_agents:
        if isinstance(sub_agent, LlmAgent):
            assert sub_agent.model == EXPECTED_MODEL

    cost_sub_agents = agents["cost_copilot"].sub_agents
    assert [agent.name for agent in cost_sub_agents] == [
        "cost_discovery_agent",
        "cost_analysis_agent",
    ]
    for sub_agent in cost_sub_agents:
        assert isinstance(sub_agent, LlmAgent)
        assert sub_agent.model == EXPECTED_MODEL

# Cloud Copilot Dashboard

The dashboard is the operational entry point for the Cloud Copilot agents. It discovers configured services through A2A agent cards, exposes one read-only chat entry point, and shows operations records only when a backend feed is configured.

## Commands

```bash
npm install
npm run dev
npm test
npm run build
```

## Environment

| Variable | Purpose | Default |
| --- | --- | --- |
| `VITE_K8S_COPILOT_URL` | Kubernetes and DBA A2A service base URL | `https://k8s.agents.hunt3r.dev` |
| `VITE_COST_COPILOT_URL` | Cost A2A service base URL | `https://cost.agents.hunt3r.dev` |
| `VITE_DASHBOARD_API_URL` | Optional operations-feed base URL | unset |
| `VITE_DASHBOARD_ROLE` | Explicit console role (`viewer` or `operator`) | `viewer` |

Agent discovery reads `GET {baseUrl}/.well-known/agent-card.json` with a five-second timeout. Cards are rechecked with the refresh control. A card can be online but individual skills can still be unavailable; the dashboard only asserts service/card reachability.

## Read-only routing and roles

The unified chat classifies database, cost, and Kubernetes questions and can also target a selected agent. DBA Copilot shares the Kubernetes service URL and sends its request to the same A2A endpoint; the backend's root agent routes database diagnostics to `dba_agent`.

`VITE_DASHBOARD_ROLE` is an explicit deployment configuration, not authentication or server-side authorization. `viewer` can use Kubernetes and Cost read paths; `operator` can additionally use DBA diagnostics. Every action is labeled read-only and the chat does not expose write or approval tools.

## Operations feed

Set `VITE_DASHBOARD_API_URL` only when a backend can provide records. Without it, the dashboard shows an unconfigured state rather than sample data. See [docs/OPERATIONS.md](docs/OPERATIONS.md) for the response contract.

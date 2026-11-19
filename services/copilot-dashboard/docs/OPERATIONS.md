# Dashboard operations contracts

## Agent card discovery

For every configured service, the dashboard requests:

```http
GET {baseUrl}/.well-known/agent-card.json
```

The service must return an A2A agent card. Missing `name` or `url`, a non-2xx response, a network error, or a timeout marks that target unavailable and records the error. The dashboard displays a `checking` state until discovery settles.

## Chat requests

The dashboard currently assumes A2A JSON-RPC 2.0 at the configured service root:

```http
POST {baseUrl}/
Content-Type: application/json

{
  "jsonrpc": "2.0",
  "id": "dashboard-generated-id",
  "method": "message/send",
  "params": {
    "message": {
      "role": "user",
      "messageId": "dashboard-generated-id",
      "parts": [{ "kind": "text", "text": "User request" }]
    }
  }
}
```

The client extracts text recursively from the `result` payload and surfaces JSON-RPC `error.message`, HTTP failures, and empty responses as chat errors. If a deployment uses a different A2A transport or endpoint, update `sendAgentMessage` and this contract; do not silently proxy requests through an undocumented endpoint.

## Operations activity feed

With `VITE_DASHBOARD_API_URL` set, the dashboard requests:

```http
GET {baseUrl}/api/dashboard/activity
```

Expected JSON:

```json
{
  "generatedAt": "2026-01-01T00:00:00.000Z",
  "scopes": [
    { "type": "cluster", "id": "prod-us-1", "label": "prod-us-1" },
    { "type": "project", "id": "acme-prod", "label": "Acme Production" }
  ],
  "investigations": [],
  "recommendations": [],
  "approvals": []
}
```

Each activity record uses:

```json
{
  "id": "activity-id",
  "scope": { "type": "cluster", "id": "prod-us-1", "label": "prod-us-1" },
  "title": "Investigation or recommendation title",
  "status": "open | in_progress | complete | pending | approved | rejected",
  "updatedAt": "2026-01-01T00:00:00.000Z",
  "summary": "Optional human-readable summary"
}
```

Records missing `id`, `scope`, `title`, `status`, or `updatedAt` are ignored. Empty arrays mean the backend explicitly has no records and are displayed as empty. A failed request shows unavailable and never falls back to generated history.

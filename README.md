# Cloud AI Copilots

> Intelligent multi-agent platform for Kubernetes management and cloud cost optimization

Built with [Google Agent Development Kit (ADK)](https://github.com/google/adk) and the A2A (Agent-to-Agent) protocol for seamless inter-agent communication.

[![Live Demo](https://img.shields.io/badge/Live-Demo-blue)](https://cloudcopilot-dashboard.run.app)
[![Cloud Run](https://img.shields.io/badge/Google%20Cloud-Run-4285F4?logo=google-cloud)](https://cloud.google.com/run)
[![ADK](https://img.shields.io/badge/Google-ADK-4285F4?logo=google)](https://google.github.io/adk-docs/)

## 🎯 Overview

Cloud AI Copilots is a multi-agent platform that combines specialized AI agents for comprehensive cloud infrastructure management. The platform consists of three integrated services:

1. **K8s Copilot** - Kubernetes troubleshooting and management
2. **Cost Copilot** - GCP cost analysis and optimization  
3. **Unified Dashboard** - Central management UI

## ✨ Features

### K8s Copilot
- 🔍 **Multi-agent diagnostics** - Automated cluster health checks and resource monitoring
- 🕵️ **Intelligent investigation** - Root cause analysis for pod failures and deployment issues
- 💡 **Remediation suggestions** - Actionable fix recommendations with kubectl commands
- 💰 **Integrated cost analysis** - Namespace and cluster cost estimation via A2A

### Cost Copilot
- 💸 **Idle resource detection** - Find unused VMs, disks, IPs, and load balancers
- 📊 **Cost optimization** - GCP billing analysis and recommendations
- 🎯 **K8s cost estimation** - Calculate namespace and cluster costs from resource requests
- 📈 **Cost comparison** - Compare costs across multiple namespaces

### Unified Dashboard
- 🎨 **Modern UI** - Built with TanStack Start, React, and Tailwind CSS
- ⚡ **Instant loading** - Static configuration for fast page loads
- 🔗 **Agent discovery** - View all available agents and their capabilities
- 📱 **Responsive design** - Works on desktop and mobile devices

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Cloud AI Copilots                        │
│                  (Microservices Architecture)                │
└─────────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┬────────────────┐
        │                 │                 │                │
   ┌────▼────┐      ┌─────▼──────┐   ┌────▼────────┐  ┌────▼────┐
   │ K8s     │ A2A  │ Cost       │   │ Unified     │  │  User   │
   │ Copilot │◄────►│ Copilot    │   │ Dashboard   │  │         │
   │ :8000   │      │ :8001      │   │ :3000       │  │         │
   └─────────┘      └────────────┘   └─────────────┘  └─────────┘
        │                 │                 │
   ┌────▼────┐      ┌─────▼──────┐
   │   GKE   │      │ GCP Billing│
   │ Cluster │      │ & Assets   │
   └─────────┘      └────────────┘
```

### A2A Integration

The K8s Copilot and Cost Copilot communicate via the A2A (Agent-to-Agent) protocol:

```
User: "What's the cost of my namespace?"
  │
  ├─► K8s Copilot (Diagnose namespace resources)
  │     ├─► DiagnosticAgent.get_namespace_resources()
  │     └─► Returns: 2.0 cores, 8.0 GB RAM, 4 pods
  │
  └─► Cost Copilot (via A2A)
        ├─► CostAnalysisAgent.estimate_namespace_cost()
        └─► Returns: $69.92/month breakdown
```

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Node.js 18+
- Google Cloud SDK
- Kubernetes cluster access (for K8s Copilot)
- GCP project with billing enabled (for Cost Copilot)
- Gemini API key

### 1. Clone the Repository

```bash
git clone https://github.com/m3rryqold/cloud-copilot.git
cd cloud-copilot
```

### 2. Set Up K8s Copilot

```bash
cd services/k8s-copilot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create .env file
echo "GOOGLE_API_KEY=your-gemini-api-key" > .env
echo "COST_COPILOT_URL=http://localhost:8001" >> .env

# Start the agent
adk web src/agents --port 8000 --host 0.0.0.0
```

Visit: http://localhost:8000

### 3. Set Up Cost Copilot

```bash
cd services/cost-copilot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create .env file
echo "GOOGLE_API_KEY=your-gemini-api-key" > .env
echo "GCP_PROJECT_ID=your-gcp-project-id" >> .env

# Start the A2A server
python start_a2a.py
```

A2A endpoint: http://localhost:8001

### 4. Set Up Unified Dashboard

```bash
cd services/unified-dashboard
npm install

# Create .env.local file
cat > .env.local <<ENVEOF
VITE_K8S_COPILOT_URL=http://localhost:8000
VITE_COST_COPILOT_URL=http://localhost:8001
ENVEOF

# Start the dashboard
npm run dev
```

Visit: http://localhost:3000

## 📖 Usage Examples

### K8s Copilot

```
💬 "Check the health of my cluster"
💬 "Why is my pod in CrashLoopBackOff?"
💬 "Show me the cost of the production namespace"
💬 "Compare costs between staging and production namespaces"
```

### Cost Copilot

```
💬 "Find all idle VMs in my project"
💬 "Show me unattached disks that are costing money"
💬 "Estimate the cost of my Kubernetes cluster"
💬 "What are my top 10 most expensive services?"
```

### Unified Dashboard

- Browse available agents and their capabilities
- View agent architecture and communication flows
- Quick access to agent web UIs
- Responsive design for mobile and desktop

## 🛠️ Technology Stack

### Backend (Python)
- **Google ADK** - Agent framework
- **Gemini 3.8 Flash** - Large language model
- **Kubernetes Python Client** - K8s API interaction
- **Google Cloud Client Libraries** - GCP services
- **Uvicorn** - ASGI server

### Frontend (TypeScript)
- **TanStack Start** - Full-stack React framework
- **React 18** - UI library
- **TypeScript** - Type safety
- **Tailwind CSS** - Styling
- **shadcn/ui** - Component library
- **Vite** - Build tool

## 📂 Project Structure

```
cloudcopilot/
├── services/
│   ├── k8s-copilot/          # Kubernetes management agent
│   │   ├── src/agents/
│   │   ├── requirements.txt
│   │   └── .env
│   ├── cost-copilot/         # Cost optimization agent
│   │   ├── src/agents/
│   │   ├── start_a2a.py
│   │   ├── requirements.txt
│   │   └── .env
│   └── unified-dashboard/    # Web dashboard
│       ├── src/
│       ├── public/
│       ├── package.json
│       └── .env.local
├── docs/                     # Documentation
├── README.md                 # This file
├── CLAUDE.md                 # Development context
└── .gitignore
```

## 🔧 Configuration

### K8s Copilot (.env)
```bash
GOOGLE_API_KEY=your-gemini-api-key
COST_COPILOT_URL=http://localhost:8001  # or Cloud Run URL
```

### Cost Copilot (.env)
```bash
GOOGLE_API_KEY=your-gemini-api-key
GCP_PROJECT_ID=your-gcp-project-id
PUBLIC_HOST=localhost
PUBLIC_PORT=8001
PROTOCOL=http
```

### Unified Dashboard (.env.local)
```bash
VITE_K8S_COPILOT_URL=http://localhost:8000
VITE_COST_COPILOT_URL=http://localhost:8001
```

## 🚢 Deployment

### Google Cloud Run

```bash
# Deploy all services via Cloud Build
gcloud builds submit --config cloudbuild.yaml --project=your-project-id

# Services will be deployed to:
# - https://k8s-copilot-xxx.run.app
# - https://cost-copilot-xxx.run.app
# - https://unified-dashboard-xxx.run.app
```

## 🧪 Testing

### Verify A2A Connection

```bash
# Check Cost Copilot agent card
curl http://localhost:8001/.well-known/agent-card.json | python3 -m json.tool

# Should return agent metadata with skills list
```

### Test Namespace Cost Query

```
1. Start both K8s Copilot and Cost Copilot
2. In K8s Copilot UI: "Tell me the cost of my default namespace"
3. Expected: Cost breakdown with cores, memory, and estimated monthly cost
```

## 📝 Development

See [CLAUDE.md](CLAUDE.md) for detailed development context and guidelines.

### Adding New Tools

**K8s Copilot:**
```python
# In services/k8s-copilot/src/agents/diagnostic_agent/tools.py
@tool
def your_new_tool(param: str) -> str:
    """Tool description for the LLM."""
    return result
```

**Cost Copilot:**
```python
# In services/cost-copilot/src/agents/cost_analysis_agent/tools.py
@tool
def your_new_cost_tool(project_id: str) -> dict:
    """Tool description for the LLM."""
    return result
```

**Dashboard:**
```typescript
// In services/unified-dashboard/src/config/agents.ts
export const AGENT_CONFIGS: AgentConfig[] = [
  // Add your new agent configuration
]
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Built with [Google Agent Development Kit (ADK)](https://github.com/google/adk)
- Powered by [Gemini 3.8 Flash](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash)
- UI components from [shadcn/ui](https://ui.shadcn.com/)

## 📬 Support

For issues and questions:
- Create an issue in this repository
- Review the [A2A Integration docs](docs/A2A-INTEGRATION.md)

---

**Built with ❤️ using Google ADK and A2A Protocol**

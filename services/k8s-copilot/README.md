# Kubernetes Copilot

**AI-Powered Kubernetes Troubleshooting with Multi-Agent Intelligence**

An intelligent multi-agent system built with Google's Agent Development Kit (ADK) that automatically diagnoses, investigates, and provides remediation guidance for Kubernetes cluster issues. Reduce troubleshooting time from hours to minutes with AI-powered cluster analysis.

[![Cloud Run](https://img.shields.io/badge/Google%20Cloud-Run-4285F4?logo=google-cloud)](https://cloud.google.com/run)
[![ADK](https://img.shields.io/badge/Google-ADK-4285F4?logo=google)](https://google.github.io/adk-docs/)
[![Gemini](https://img.shields.io/badge/Gemini-3.8%20Flash-8E75B2?logo=google-gemini)](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash)

---

## Overview

Kubernetes troubleshooting is complex and time-consuming, requiring engineers to correlate information across logs, metrics, events, and configurations. **Kubernetes Copilot** uses a team of specialized AI agents to automatically analyze your cluster, identify issues, and provide actionable remediation guidance.

---

## 🤖 Agent Architecture

```
┌─────────────────────────────────────────────────────────┐
│           kubernetes_copilot (Orchestrator)             │
│         Coordinates 5 specialized sub-agents            │
└────────────────────┬────────────────────────────────────┘
                     │
                     │ delegates to
                     │
        ┌────────────┼────────────┬──────────────┐
        │            │            │              │
   ┌────▼───────┐ ┌─▼──────────┐ ┌─▼───────────┐ ┌▼────────────┐
   │ diagnostic │ │investigator│ │    dba      │ │ remediation │
   │  agent     │ │   agent    │ │   agent     │ │ advisor     │
   │  7 tools   │ │  3 tools   │ │  4 tools    │ │  3 tools    │
   └────────────┘ └────────────┘ └─────────────┘ └─────────────┘
```

### 1. **kubernetes_copilot** (Intelligent Orchestrator)
Coordinates specialized sub-agents to solve complex troubleshooting workflows. Uses ADK's sub-agent pattern to delegate tasks to the right specialist and synthesize results into comprehensive reports.

### 2. **diagnostic_agent** (Cluster Health & Configuration)
**Tools:**
- `get_cluster_info()` - Cluster name, version, node count
- `list_namespaces()` - All namespaces with status
- `list_all_pods(namespace)` - All pods (cluster-wide or by namespace)
- `get_unhealthy_pods(namespace)` - Find failing pods (cluster-wide or by namespace)
- `get_cluster_metrics()` - CPU/memory via Cloud Monitoring
- `get_node_resources()` - Node capacity and usage
- `get_pod_events()` - Kubernetes events for specific pod

### 3. **investigator_agent** (Root Cause Analyzer)
**Tools:**
- `get_pod_logs()` - Pod logs with tail support
- `get_pod_description()` - Detailed pod configuration
- `analyze_deployment_history()` - Deployment changes and rollout status

### 4. **remediation_advisor_agent** (Fix Suggester)
**Tools:**
- `suggest_fix()` - Remediation strategies for common errors
- `generate_kubectl_commands()` - Ready-to-run kubectl commands
- `generate_yaml_patch()` - Configuration patches

### 5. **dba_agent** (Database Logs & Slow Queries)
**Tools:**
- `find_database_pods()` - Discover MySQL/MariaDB pods by image, container name, or labels
- `get_mysql_slow_queries()` - Read recent MySQL/MariaDB slow-query entries from pod logs
- `get_mysql_query_plan()` - Run `EXPLAIN` for a single read-only MySQL statement
- `recommend_mysql_indexes()` - Analyze a MySQL query plan and suggest indexes without applying them

Slow-query analysis reads the container's `stdout`/`stderr` by default. A read-only file mode is also available when an operator allowlists an absolute slow-log path; it uses a shell-free `tail` exec and emits audit metadata without SQL or credentials. See [docs/dba-rbac.md](../docs/dba-rbac.md) for RBAC, file prerequisites, and replica-selection guidance.

For query-plan analysis, the database agent runs a validated `EXPLAIN` statement inside the MySQL/MariaDB pod and can use the result plus `SHOW INDEX` output to recommend indexes. It does not create or alter indexes.

---

## 🏗️ System Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     ADK Web Interface                         │
│              (FastAPI + Uvicorn Server)                       │
│                    Port 8000                                  │
└────────────────────────┬─────────────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────────────┐
│                  6 AI Agents (Gemini 3.8 Flash)               │
│ kubernetes_copilot | diagnostic | investigator | dba |       │
│ remediation                                                  │
│ 17 tools total across all specialized agents                 │
└────────────────────────┬─────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    ┌────▼─────┐  ┌─────▼──────┐  ┌────▼─────────┐
    │Kubernetes│  │Cloud       │  │Google Cloud  │
    │API       │  │Monitoring  │  │Secrets & IAM │
    └──────────┘  └────────────┘  └──────────────┘
```

**Key Capabilities:**
- **Intelligent Orchestration** - Uses ADK sub-agent pattern for proper task delegation
- **Cluster-wide visibility** - No namespace restrictions, full cluster analysis
- **Comprehensive diagnostics** - From cluster info to node resources
- **Smart troubleshooting** - Correlates pod issues with node resources
- **Automated remediation** - Generates kubectl commands and YAML patches

**How it works:**
The orchestrator delegates tasks to specialized agents based on the workflow:
1. **Diagnose** → diagnostic_agent finds issues
2. **Investigate** → investigator_agent analyzes root causes
3. **Remediate** → remediation_advisor_agent suggests fixes
4. **Report** → orchestrator synthesizes findings

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Gemini API key ([Get one](https://aistudio.google.com/app/apikey))

### Install & Run

```bash
# Clone and setup
git clone <repo-url> && cd k8s-copilot
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Configure (create .env file)
echo "GOOGLE_API_KEY=your-key-here" > .env

# Run
adk web src/agents --port 8000

# Open browser: http://localhost:8000
```

### Use the Agents

Select an agent and ask questions:
- "What cluster am I connected to?"
- "Show me all namespaces in my cluster"
- "Check my entire cluster for issues"
- "List all pods with their status"
- "What's the resource usage on my nodes?"
- "How do I fix OOMKilled errors?"
- "Investigate pod xyz in namespace default"
- "Find slow MySQL queries from the last 5 minutes"
- "Show MySQL queries slower than 2 seconds in the orders namespace"
- "Explain this SQL query and recommend indexes"
- "What indexes should I add for this SELECT query?"

---

## 🐳 Deploy to Cloud Run

### Prerequisites

1. **Create an Artifact Registry repository:**
```bash
gcloud artifacts repositories create k8s-copilot-repo \
  --repository-format=docker \
  --location=europe-west1 \
  --project=your-project-id
```

2. **Create secrets in Google Secret Manager:**
```bash
# Gemini API key
echo -n "your-api-key" | gcloud secrets create GOOGLE_API_KEY \
  --data-file=- \
  --project=your-project-id

# GKE cluster kubeconfig (for cluster access)
gcloud container clusters get-credentials your-cluster-name \
  --zone=your-zone \
  --project=your-project-id

kubectl config view --flatten --minify > kubeconfig.yaml

gcloud secrets create gke-kubeconfig \
  --data-file=kubeconfig.yaml \
  --project=your-project-id

rm kubeconfig.yaml  # Clean up
```

### Deploy with Cloud Build

1. Initialize git and push to remote repository:
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin your-repo-url
git push -u origin main
```

2. In Google Cloud Console:
   - Navigate to **Cloud Run**
   - Click **"Create Service"**
   - Select **"Set up with Cloud Build"**
   - Connect your repository
   - Cloud Build will use `cloudbuild.yaml` to build and deploy

The service will be deployed to `europe-west1` with:
- **256MB RAM** (cost-optimized)
- **0-3 instances** (scales to zero when idle)
- **GKE cluster access** via mounted kubeconfig secret

---

## 📁 Project Structure

```
k8s-copilot/
├── src/agents/
│   ├── kubernetes_copilot/       # Master orchestrator (9 tools)
│   │   ├── agent.py              # Main agent definition
│   │   └── knowledge_base.py    # Remediation knowledge
│   ├── diagnostic_agent/         # Health monitoring (3 tools)
│   │   ├── agent.py
│   │   └── tools.py
│   ├── investigator_agent/       # Log analysis (3 tools)
│   │   ├── agent.py
│   │   └── tools.py
│   └── remediation_advisor_agent/ # Fix suggestions (3 tools)
│       ├── agent.py
│       ├── tools.py
│       └── knowledge_base.py
├── Dockerfile                    # Container configuration
├── cloudbuild.yaml              # Cloud Build deployment config
├── requirements.txt             # Python dependencies
├── test_agents.py               # Comprehensive test suite
└── .env                         # Environment variables (API keys)
```

---

## 🎬 Example Workflows

### Complete Cluster Diagnosis
**Agent:** kubernetes_copilot
**Query:** "Check my cluster for problems"

**Workflow:**
1. Scans for unhealthy pods across all namespaces
2. Retrieves cluster metrics (CPU, memory utilization)
3. Analyzes pod logs and events for error patterns
4. Suggests fixes with ready-to-run kubectl commands
5. Generates YAML patches for configuration issues

### Investigating Specific Issues
**Agent:** investigator_agent
**Query:** "Why is my pod crashing in the default namespace?"

**Workflow:**
1. Retrieves pod logs and recent events
2. Analyzes deployment history for recent changes
3. Identifies root cause (e.g., OOMKilled, misconfiguration)
4. Provides detailed diagnostic information

---

## 🔧 Configuration

### Environment Variables
- `GOOGLE_API_KEY` - Gemini API key (required)
- `GOOGLE_CLOUD_PROJECT` - GCP project (for deployment)
- `PORT` - Server port (default: 8000)

### ADK Commands
```bash
# Basic
adk web src/agents

# With auto-reload
adk web src/agents --reload

# Custom port
adk web src/agents --port 8080
```

---

## 🧪 Testing

```bash
python test_agents.py
```

Verifies:
- ✅ All agents load correctly
- ✅ Tools properly configured
- ✅ Type annotations valid
- ✅ Knowledge base works

---

## 🐛 Troubleshooting

### 503 Model Overloaded
Wait 1-2 minutes (Gemini API rate limit), then retry.

### Check Logs
```bash
tail -f adk_web.log
```

### Common Issues
- Missing `GOOGLE_API_KEY` in .env
- Agent files must expose `root_agent` variable
- Use `Optional[str]` for nullable parameters

---

## 📊 Supported Errors

| Error | Detection | Solutions |
|-------|-----------|-----------|
| OOMKilled | Memory exhaustion | Increase limits, HPA |
| CrashLoopBackOff | Repeated crashes | Fix config, probes |
| ImagePullBackOff | Pull failures | Registry auth |
| Pending | Scheduling issues | Scale cluster |

---

## 🔑 Key Features

- **Multi-Agent Architecture**: 5 specialized AI agents working in coordination
- **Comprehensive Diagnostics**: 15 tools covering cluster config, monitoring, investigation, remediation, and database slow-query analysis
- **Cluster-Wide Visibility**: No namespace restrictions - analyze entire cluster at once
- **Cloud-Native**: Integrates with Kubernetes API and Google Cloud Monitoring
- **Production-Ready**: Deploy to Cloud Run with IAM, secrets management, and logging
- **Intelligent Analysis**: Powered by Gemini 3.8 Flash for accurate troubleshooting
- **Cost-Optimized**: Scales to zero, 256MB RAM, efficient resource usage

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

---

## 📄 License

This project is open source and available under the MIT License.

---

## 🙏 Acknowledgments

Built with:
- [Google Agent Development Kit (ADK)](https://google.github.io/adk-docs/)
- [Gemini 3.8 Flash](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash)
- [Google Cloud Run](https://cloud.google.com/run)
- [Kubernetes Python Client](https://github.com/kubernetes-client/python)
- [Google Cloud Monitoring](https://cloud.google.com/monitoring)

---

## 📞 Support

For questions, issues, or feature requests, please open an issue on GitHub.

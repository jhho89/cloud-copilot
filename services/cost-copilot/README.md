# Cost Copilot Service

AI-powered GCP cost optimization using multi-agent collaboration

## Overview

The Cost Copilot service uses Google ADK agents to help you discover wasted resources and reduce cloud costs. It combines resource discovery with cost analysis to provide actionable, quantified recommendations.

## Architecture

```
Cost Copilot (Orchestrator)
├── CostDiscoveryAgent    - Finds idle VMs, unattached disks, unused IPs
└── CostAnalysisAgent     - VM rightsizing, CUDs, storage optimization
```

### Agents

**1. Cost Copilot (Root Agent)**
- Orchestrates discovery and analysis
- Synthesizes recommendations
- Prioritizes optimizations by impact

**2. Cost Discovery Agent**
- Lists accessible GCP projects
- Scans compute resources (VMs, disks, IPs)
- Finds idle resources (< 5% CPU utilization)
- Identifies unattached disks and idle static IPs
- Estimates waste in dollars

**3. Cost Analysis Agent**
- Analyzes VM rightsizing opportunities (Recommender API)
- Suggests Committed Use Discounts (25-55% savings)
- Reviews storage optimization strategies
- Calculates total potential savings
- Queries the BigQuery billing export for real cost breakdowns and anomalies

## Features

✅ **Resource Discovery**
- Idle VM detection (CPU monitoring)
- Unattached disk scanning
- Idle static IP identification
- Multi-project support

✅ **Cost Analysis**
- VM rightsizing recommendations
- CUD opportunity analysis
- Storage class suggestions
- Savings estimation
- BigQuery billing-export cost breakdowns by service, SKU, project and date
- Daily cost anomaly detection

✅ **Actionable Recommendations**
- Dollar-quantified savings
- Prioritized action plans
- Specific resource names
- Risk assessment

## Quick Start

### Local Development

```bash
# 1. Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Set up environment
cp .env.example .env
# Edit .env and add GOOGLE_API_KEY

# Set the BigQuery billing export target (see docs/BIGQUERY-BILLING-EXPORT.md)
# BILLING_EXPORT_PROJECT=your-project-id
# BILLING_EXPORT_DATASET=your_billing_dataset
# BILLING_EXPORT_TABLE=gcp_billing_export_v1_0X9ABC-DEF123

# 3. Authenticate with GCP (for ADC)
gcloud auth application-default login

# 4. Run the service
adk web src/agents --port 8001 --host 0.0.0.0
```

Access at: http://localhost:8001

### Required GCP Permissions

The service account needs:
- `roles/compute.viewer` - View compute resources
- `roles/monitoring.viewer` - Access metrics
- `roles/recommender.viewer` - Get optimization recommendations
- `roles/billing.viewer` - Access billing data
- `roles/resourcemanager.projectViewer` - List projects
- `roles/bigquery.jobUser` and `roles/bigquery.dataViewer` - Query the billing export

### Example Queries

```
"Help me reduce my GCP costs"
"Show me all idle resources in project hunt3r"
"Find unattached disks"
"What are my VM rightsizing opportunities?"
"Suggest committed use discounts"
"Estimate my potential cost savings"
```

## Tools Reference

### Cost Discovery Tools

- `list_accessible_projects()` - List all accessible GCP projects
- `scan_compute_resources(project_id)` - Overview of VMs, disks, IPs
- `find_idle_resources(project_id, days=7)` - VMs with < 5% CPU
- `list_unattached_disks(project_id)` - Orphaned persistent disks
- `list_idle_static_ips(project_id)` - Unused IP addresses

### Cost Analysis Tools

- `get_billing_account_for_project(project_id)` - Verify billing access
- `get_cost_breakdown(project_id, group_by, days, end_date)` - Query the BigQuery billing export
- `detect_billing_anomalies(project_id, days, threshold_pct)` - Daily cost anomaly detection
- `analyze_vm_rightsizing(project_id)` - Recommender API suggestions
- `suggest_committed_use_discounts(project_id)` - CUD opportunities
- `estimate_cost_savings(summary)` - Calculate potential savings
- `analyze_storage_costs(project_id)` - Storage optimization guide

## Deployment

### Cloud Run

```bash
# Build and deploy
gcloud builds submit --config ../../cloudbuild.yaml

# Or deploy individually
gcloud run deploy cost-optimizer \
  --source . \
  --region europe-west1 \
  --memory 512Mi \
  --min-instances 0 \
  --max-instances 3 \
  --set-env-vars GOOGLE_API_KEY=${GOOGLE_API_KEY}
```

### A2A Integration

This service can be called by other agents (like K8s Copilot) via A2A protocol:

```python
from google.adk.a2a import RemoteAgent

# In another service
cost_copilot_url = os.getenv("COST_COPILOT_URL")
CostCopilotRemote = RemoteAgent(cost_copilot_url)

# Use as sub-agent
KubernetesCopilot = Agent(
    sub_agents=[..., CostCopilotRemote]
)
```

## Limitations

- **Cost Breakdown**: Requires BigQuery billing export (see docs/BIGQUERY-BILLING-EXPORT.md)
- **Recommender API**: Needs 24-48 hours of data collection
- **Zone/Region Scanning**: Limited to top 10 zones for performance
- **Read-Only**: No automated actions (recommendations only)

## Future Enhancements

- [ ] CostActionAgent for safe automation
- [ ] Cloud Storage analysis tools
- [ ] GKE cost breakdown
- [ ] Cost alerts and notifications
- [ ] Historical cost trends

## Contributing

Part of the [CloudCopilot](../../README.md) platform.

## License

MIT License - See [../../LICENSE](../../LICENSE)

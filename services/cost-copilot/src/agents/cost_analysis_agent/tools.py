"""
Cost Analysis Agent Tools
Analyzes GCP billing data and provides cost optimization recommendations
"""

import os
from typing import List, Optional

from google.cloud import billing_v1, recommender_v1

from .billing import (
    BillingExportError,
    BillingExportNotConfigured,
    EmptyBillingDataset,
    build_cost_breakdown_query,
    detect_cost_anomalies,
    format_cost_breakdown,
)
from .billing import (
    get_cost_breakdown as query_cost_breakdown,
)


def get_current_project() -> str:
    """Gets the current GCP project ID."""
    project_id = os.getenv('GCP_PROJECT_ID') or os.getenv('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        import google.auth
        _, project_id = google.auth.default()
    return project_id or "unknown"


def get_billing_account_for_project(project_id: str | None = None) -> str:
    """
    Gets the billing account ID associated with a project.
    Required for querying Cloud Billing API.
    """
    if not project_id:
        project_id = get_current_project()

    try:
        client = billing_v1.CloudBillingClient()
        name = f"projects/{project_id}"
        project_billing_info = client.get_project_billing_info(name=name)

        if not project_billing_info.billing_enabled:
            return f"Billing is not enabled for project {project_id}"

        billing_account = project_billing_info.billing_account_name
        return f"Billing account for {project_id}: {billing_account}"

    except Exception as e:
        return f"Error getting billing account: {e!s}\nEnsure service account has roles/billing.viewer role."


def get_cost_breakdown(
    project_id: str | None = None,
    group_by: list[str] | None = None,
    days: int = 30,
    end_date: str | None = None,
) -> str:
    """
    Queries the BigQuery billing export and returns grouped cost totals.

    Supported group-by fields: service, sku, project, date.
    The label breakdown is not exposed through this tool yet.
    """
    if not project_id:
        project_id = get_current_project()

    try:
        fields = group_by or ["service"]
        rows = query_cost_breakdown(fields, days=days, end_date=end_date)
        query, params = build_cost_breakdown_query(fields, days=days, end_date=end_date)
        result = f"## Cost Breakdown for {project_id} ({days} days)\n\n"
        result += format_cost_breakdown(rows, fields)
        return result
    except BillingExportNotConfigured as exc:
        return (
            f"## Billing Export Not Configured\n\n{exc}\n\n"
            "Enable the detailed usage cost export in Cloud Billing and set "
            "BILLING_EXPORT_PROJECT, BILLING_EXPORT_DATASET and "
            "BILLING_EXPORT_TABLE. See docs/BIGQUERY-BILLING-EXPORT.md."
        )
    except EmptyBillingDataset as exc:
        return (
            f"## No Cost Data for {project_id}\n\n{exc}\n\n"
            "Exports can take 24-48 hours to populate after they are enabled."
        )
    except BillingExportError as exc:
        return f"## Billing Export Error\n\n{exc}"
    except ValueError as exc:
        return f"## Invalid Request\n\n{exc}"
    except Exception as exc:  # pragma: no cover - defensive
        return f"## Unexpected Error\n\n{exc}"


def detect_billing_anomalies(
    project_id: str | None = None,
    days: int = 30,
    threshold_pct: float = 30.0,
) -> str:
    """Detect cost anomalies over the look-back window using daily totals."""
    if not project_id:
        project_id = get_current_project()
    try:
        anomalies = detect_cost_anomalies(days=days, threshold_pct=threshold_pct)
        if not anomalies:
            return f"## No Cost Anomalies Detected for {project_id}\n\nDaily costs stayed within {threshold_pct}% of the running mean."
        lines = [f"## Cost Anomalies for {project_id}", ""]
        for anomaly in anomalies:
            lines.append(
                f"- {anomaly['day']}: {anomaly['currency']} {anomaly['cost']:.2f} "
                f"({anomaly['direction']} {anomaly['change_pct']:.1f}% vs mean {anomaly['mean_prior']:.2f})"
            )
        return "\n".join(lines)
    except BillingExportNotConfigured as exc:
        return f"## Billing Export Not Configured\n\n{exc}"
    except EmptyBillingDataset as exc:
        return f"## No Cost Data for Anomaly Detection\n\n{exc}"
    except BillingExportError as exc:
        return f"## Billing Export Error\n\n{exc}"
    except ValueError as exc:
        return f"## Invalid Request\n\n{exc}"


def analyze_vm_rightsizing(project_id: str | None = None) -> str:
    """
    Gets VM rightsizing recommendations from Recommender API.
    Suggests downsizing overprovisioned VMs to save costs.
    """
    if not project_id:
        project_id = get_current_project()

    try:
        client = recommender_v1.RecommenderClient()

        # Recommender parent format: projects/{project}/locations/{location}/recommenders/{recommender}
        # For VM rightsizing: google.compute.instance.MachineTypeRecommender

        recommendations = []
        locations = ["us-central1", "us-east1", "europe-west1", "asia-east1"]  # Common locations

        for location in locations:
            parent = f"projects/{project_id}/locations/{location}/recommenders/google.compute.instance.MachineTypeRecommender"

            try:
                request = recommender_v1.ListRecommendationsRequest(parent=parent)
                for recommendation in client.list_recommendations(request=request):
                    recommendations.append({
                        "name": recommendation.name.split("/")[-1],
                        "description": recommendation.description,
                        "primary_impact": recommendation.primary_impact,
                        "state": recommendation.state_info.state.name,
                        "location": location
                    })
            except Exception:
                # Skip locations with no recommendations or permission issues
                continue

        if not recommendations:
            return f"No VM rightsizing recommendations found for project {project_id}.\nEither VMs are already optimally sized, or Recommender API needs more time to collect data (usually 24-48 hours)."

        result = "## VM Rightsizing Recommendations\n\n"
        result += f"Project: {project_id}\n\n"
        result += f"Found {len(recommendations)} recommendation(s):\n\n"

        for rec in recommendations:
            result += f"### {rec['name']}\n"
            result += f"**Description**: {rec['description']}\n"
            result += f"**Location**: {rec['location']}\n"
            result += f"**State**: {rec['state']}\n"

            if rec['primary_impact']:
                if hasattr(rec['primary_impact'], 'cost_projection'):
                    cost_projection = rec['primary_impact'].cost_projection
                    if hasattr(cost_projection, 'cost'):
                        result += f"**Estimated Savings**: {cost_projection.cost}\n"

            result += "\n"

        result += "💡 Apply these recommendations to reduce costs while maintaining performance.\n"
        return result

    except Exception as e:
        return f"Error analyzing VM rightsizing: {e!s}\nEnsure service account has roles/recommender.viewer role."


def suggest_committed_use_discounts(project_id: str | None = None) -> str:
    """
    Suggests Committed Use Discounts (CUD) based on usage patterns.
    CUDs can save 25-55% for predictable workloads.
    """
    if not project_id:
        project_id = get_current_project()

    try:
        client = recommender_v1.RecommenderClient()

        recommendations = []
        locations = ["us-central1", "us-east1", "europe-west1"]

        for location in locations:
            # CUD Recommender
            parent = f"projects/{project_id}/locations/{location}/recommenders/google.compute.commitment.UsageCommitmentRecommender"

            try:
                request = recommender_v1.ListRecommendationsRequest(parent=parent)
                for recommendation in client.list_recommendations(request=request):
                    recommendations.append({
                        "name": recommendation.name.split("/")[-1],
                        "description": recommendation.description,
                        "primary_impact": recommendation.primary_impact,
                        "location": location
                    })
            except Exception:
                continue

        if not recommendations:
            return f"""## Committed Use Discounts (CUD) Analysis

Project: {project_id}

**No CUD recommendations found.**

This could mean:
1. Usage patterns don't justify CUDs yet (needs stable workloads)
2. You already have CUDs in place
3. Not enough historical data (Recommender needs 7+ days)

**What are CUDs?**
- Commit to use resources for 1 or 3 years
- Save 25-55% on predictable workloads
- Best for stable production environments

**Manual CUD Planning:**
1. Go to Console → Compute Engine → Committed use discounts
2. Analyze usage patterns in Billing Reports
3. Purchase commitments for baseline workloads

**Ideal for**: VMs running 24/7, databases, production clusters
"""

        result = "## Committed Use Discount Recommendations\n\n"
        result += f"Project: {project_id}\n\n"
        result += f"Found {len(recommendations)} CUD opportunity/opportunities:\n\n"

        total_potential_savings = 0
        for rec in recommendations:
            result += f"### {rec['name']}\n"
            result += f"**Description**: {rec['description']}\n"
            result += f"**Location**: {rec['location']}\n"
            result += "\n"

        result += "💡 Committed Use Discounts save 25-55% for predictable workloads.\n"
        result += "Review these recommendations in Cloud Console for detailed cost impact.\n"

        return result

    except Exception as e:
        return f"Error analyzing CUD opportunities: {e!s}"


def estimate_cost_savings(recommendations_summary: str) -> str:
    """
    Estimates potential monthly cost savings from a set of recommendations.
    This is a simplified calculator based on common optimization scenarios.
    """

    result = "## Cost Savings Estimation\n\n"
    result += "Based on common optimization scenarios:\n\n"

    savings_scenarios = {
        "Idle VMs (stopped)": {
            "savings_per_vm": 50,
            "description": "Average $50/month per small VM stopped"
        },
        "Unattached disks (deleted)": {
            "savings_per_100gb": 4,
            "description": "$0.04/GB/month for PD-Standard"
        },
        "Idle static IPs (released)": {
            "savings_per_ip": 3.5,
            "description": "~$3.50/month per idle IP"
        },
        "VM rightsizing (25% smaller)": {
            "savings_per_vm": 15,
            "description": "Average 25% savings per downsized VM"
        },
        "Committed Use Discounts": {
            "savings_percentage": "25-55%",
            "description": "Save 25-55% on committed workloads"
        }
    }

    for scenario, details in savings_scenarios.items():
        result += f"**{scenario}**\n"
        result += f"  - {details['description']}\n"

    result += "\n**Example Monthly Savings**:\n"
    result += "- Stop 5 idle VMs: ~$250/month\n"
    result += "- Delete 500GB unattached disks: ~$20/month\n"
    result += "- Release 10 idle IPs: ~$35/month\n"
    result += "- Rightsize 10 VMs: ~$150/month\n"
    result += "- **Total potential**: ~$455/month (~$5,460/year)\n\n"

    result += "💡 **Action Plan**:\n"
    result += "1. Start with low-risk optimizations (idle IPs, unattached disks)\n"
    result += "2. Stop non-production VMs during off-hours\n"
    result += "3. Rightsize underutilized VMs\n"
    result += "4. Consider CUDs for stable workloads\n"

    return result


def analyze_storage_costs(project_id: str | None = None) -> str:
    """
    Analyzes storage usage and suggests cheaper storage classes.
    Helps optimize Cloud Storage, Persistent Disk costs.
    """
    if not project_id:
        project_id = get_current_project()

    return f"""## Storage Cost Optimization

Project: {project_id}

### Storage Class Recommendations:

**Cloud Storage**:
- **Standard**: Frequently accessed data (> 1/month) → $0.020/GB/month
- **Nearline**: Accessed < 1/month → $0.010/GB/month (50% savings)
- **Coldline**: Accessed < 1/quarter → $0.004/GB/month (80% savings)
- **Archive**: Accessed < 1/year → $0.0012/GB/month (94% savings)

**Persistent Disks**:
- **PD-Standard** (HDD): $0.040/GB/month - For sequential I/O
- **PD-Balanced** (SSD): $0.100/GB/month - General purpose
- **PD-SSD**: $0.170/GB/month - High performance
- **PD-Extreme**: $0.125/GB/month - Highest IOPS

### Optimization Strategies:

1. **Lifecycle Policies** (Cloud Storage):
   - Auto-transition objects to colder storage after N days
   - Delete old log files, backups after retention period

2. **Snapshot Management**:
   - Delete snapshots older than 90 days (unless required)
   - Use incremental snapshots (automatic in GCP)

3. **Disk Rightsizing**:
   - Use PD-Standard instead of SSD for non-performance-critical workloads
   - Reduce disk sizes to actual usage (leave 20% buffer)

4. **Check Unattached Resources**:
   - Use Cost Discovery Agent's `list_unattached_disks()` tool
   - Delete orphaned disks

### Example Savings:
- Move 1TB from Standard to Nearline: Save $10/month
- Downgrade 500GB PD-SSD to PD-Standard: Save $65/month
- Delete old snapshots (100GB): Save $2-8/month

For detailed storage analysis, check GCP Storage Transfer Service and Cloud Storage Insights.
"""

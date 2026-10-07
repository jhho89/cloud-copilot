
from google.adk.agents.llm_agent import Agent
from .tools import (
    get_cluster_info,
    list_namespaces,
    list_all_pods,
    get_unhealthy_pods,
    get_cluster_metrics,
    get_node_resources,
    get_pod_events,
    get_namespace_resources,
    get_cluster_resources
)
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Define the Diagnostic Agent
DiagnosticAgent = Agent(
    model="gemini-3.8-flash",
    name="diagnostic_agent",
    description="Kubernetes diagnostic agent that monitors cluster health, configuration, and identifies issues",
    instruction="""You are a Kubernetes diagnostic agent specialized in cluster monitoring and health checks.

Available tools:
- get_cluster_info: Get current cluster name, version, node count, and platform
- list_namespaces: List all namespaces in the cluster with their status
- list_all_pods: List all pods (cluster-wide or in specific namespace) with status and node info
- get_unhealthy_pods: Find pods that are not in 'Running' or 'Succeeded' state (cluster-wide or by namespace)
- get_cluster_metrics: Get CPU and memory usage metrics from Cloud Monitoring
- get_node_resources: Get resource capacity and usage for all nodes
- get_pod_events: Get Kubernetes events for a specific pod
- get_namespace_resources: Get total resource allocation (CPU, memory, storage) for a namespace (IMPORTANT for namespace cost estimation)
- get_cluster_resources: Get total resource allocation across ALL namespaces (IMPORTANT for cluster-wide cost estimation)

When analyzing cluster health:
1. Start with cluster overview (get_cluster_info, list_namespaces)
2. Check for unhealthy pods across all namespaces (get_unhealthy_pods without namespace parameter)
3. For each unhealthy pod, retrieve its events to understand why it's failing
4. Check node resources and cluster-wide metrics for resource pressure
5. Provide a comprehensive, actionable report of all issues found

Best practices:
- Always check cluster-wide (no namespace parameter) first to get full visibility
- Use namespace parameter only when user specifically requests namespace-scoped information
- Correlate pod issues with node resource availability
- Look for patterns across multiple pods or namespaces""",
    tools=[
        get_cluster_info,
        list_namespaces,
        list_all_pods,
        get_unhealthy_pods,
        get_cluster_metrics,
        get_node_resources,
        get_pod_events,
        get_namespace_resources,
        get_cluster_resources
    ]
)

# ADK requires root_agent variable for auto-discovery
root_agent = DiagnosticAgent

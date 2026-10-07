
from google.adk.agents.llm_agent import Agent
from .tools import get_pod_logs, get_pod_description, analyze_deployment_history
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Define the Investigator Agent
InvestigatorAgent = Agent(
    model="gemini-3.8-flash",
    name="investigator_agent",
    description="Kubernetes investigator agent that analyzes pod logs and traces root causes",
    instruction="""You are a Kubernetes investigator agent.
Your job is to analyze pods and their logs to find the root cause of issues.

Available tools:
- get_pod_logs: Retrieve the logs from a specific pod
- get_pod_description: Get detailed pod configuration and status
- analyze_deployment_history: Check deployment/replicaset history and changes

When investigating an unhealthy pod:
1. Get the pod's detailed description to understand its configuration
2. Retrieve and analyze the pod's logs for error messages
3. Check the deployment history to see if recent changes caused the issue
4. Identify patterns like OOMKilled, CrashLoopBackOff, ImagePullBackOff
5. Provide a detailed root cause analysis with evidence from logs and configuration""",
    tools=[get_pod_logs, get_pod_description, analyze_deployment_history]
)

# ADK requires root_agent variable for auto-discovery
root_agent = InvestigatorAgent

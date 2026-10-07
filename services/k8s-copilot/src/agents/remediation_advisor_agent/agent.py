
from google.adk.agents.llm_agent import Agent
from .tools import suggest_fix, generate_kubectl_commands, generate_yaml_patch
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Define the Remediation Advisor Agent
RemediationAdvisorAgent = Agent(
    model="gemini-3.8-flash",
    name="remediation_advisor_agent",
    description="Kubernetes remediation advisor that suggests fixes and provides kubectl commands",
    instruction="""You are a Kubernetes remediation advisor agent.
Your job is to suggest specific fixes for Kubernetes issues with actionable commands.

Available tools:
- suggest_fix: Get remediation suggestions for common error patterns
- generate_kubectl_commands: Generate specific kubectl commands to fix issues
- generate_yaml_patch: Generate YAML patches for configuration changes

When providing remediation advice:
1. Analyze the error message and root cause
2. Use suggest_fix to get general remediation strategies
3. Generate specific kubectl commands the user can run
4. If configuration changes are needed, generate YAML patches
5. Explain WHY each fix works and what it does
6. Prioritize fixes by impact and ease of implementation""",
    tools=[suggest_fix, generate_kubectl_commands, generate_yaml_patch]
)

# ADK requires root_agent variable for auto-discovery
root_agent = RemediationAdvisorAgent

/**
 * Agent Configuration
 * Static configuration for AI agents in the Cloud AI Copilots platform
 *
 * Environment Variables:
 * - VITE_K8S_COPILOT_URL: K8s Copilot base URL (default: Cloud Run production)
 * - VITE_COST_COPILOT_URL: Cost Copilot base URL (default: Cloud Run production)
 * - VITE_DASHBOARD_ROLE: Explicit console role for the deployment (`viewer` or `operator`)
 */

import type { ToolCapability } from '../types/agent'

export interface AgentConfig {
  id: string
  name: string
  displayName: string
  description: string
  baseUrl: string
  webUrl: string
  features: string[]
  architecture: {
    type: string
    description: string
  }
  requiredCapability: ToolCapability
  agentKind: 'kubernetes' | 'database' | 'cost'
}

// Get URLs from environment or use Cloud Run defaults
const K8S_COPILOT_URL = import.meta.env.VITE_K8S_COPILOT_URL || 'https://k8s.agents.hunt3r.dev'
const COST_COPILOT_URL = import.meta.env.VITE_COST_COPILOT_URL || 'https://cost.agents.hunt3r.dev'

export const AGENT_CONFIGS: AgentConfig[] = [
  {
    id: 'k8s-copilot',
    name: 'k8s-copilot',
    displayName: 'K8s Copilot',
    description: 'Intelligent Kubernetes management agent powered by Gemini 3.8 Flash. Provides natural language interface for cluster operations, monitoring, and troubleshooting.',
    baseUrl: K8S_COPILOT_URL,
    webUrl: `${K8S_COPILOT_URL}/dev-ui`,
    features: [
      'Cluster health monitoring',
      'Pod and deployment management',
      'Real-time troubleshooting',
      'MySQL and MariaDB slow-query logs',
      'MySQL query plans and index recommendations',
      'Resource optimization',
      'Multi-cluster support'
    ],
    architecture: {
      type: 'A2A Protocol + ADK Web UI',
      description: 'Built with Google Agent Development Kit (ADK), exposing both A2A protocol for agent-to-agent communication and a web-based chat interface for human interaction.'
    },
    requiredCapability: 'kubernetes:read',
    agentKind: 'kubernetes'
  },
  {
    id: 'dba-copilot',
    name: 'dba-copilot',
    displayName: 'DBA Copilot',
    description: 'Reads MySQL/MariaDB slow-query evidence and reports read-only Redis/Valkey diagnostics, metrics trends, and snapshot deltas without changing databases.',
    baseUrl: K8S_COPILOT_URL,
    webUrl: `${K8S_COPILOT_URL}/dev-ui`,
    features: [
      'MySQL and MariaDB slow-query logs',
      'Recent database diagnostics',
      'Query plan guidance',
      'Redis/Valkey metrics trends and snapshot diffs',
      'Read-only database inspection'
    ],
    architecture: {
      type: 'A2A endpoint shared with K8s Copilot',
      description: 'Uses the same K8s Copilot A2A service as a specialized database diagnostic target.'
    },
    requiredCapability: 'database:read',
    agentKind: 'database'
  },
  {
    id: 'cost-copilot',
    name: 'cost-copilot',
    displayName: 'Cost Optimization Copilot',
    description: 'AI-powered cloud cost optimization agent. Analyzes GCP resource usage, identifies cost-saving opportunities, and provides actionable recommendations.',
    baseUrl: COST_COPILOT_URL,
    webUrl: COST_COPILOT_URL,
    features: [
      'Cost analysis and breakdown',
      'Idle resource detection',
      'Budget forecasting',
      'Resource rightsizing recommendations',
      'Multi-project cost tracking'
    ],
    architecture: {
      type: 'A2A Protocol + ADK Web UI',
      description: 'Built with Google Agent Development Kit (ADK), exposing both A2A protocol for agent-to-agent communication and a web-based chat interface for human interaction.'
    },
    requiredCapability: 'cloud:cost:read',
    agentKind: 'cost'
  },
]

// Get agent config by ID
export function getAgentConfig(id: string): AgentConfig | undefined {
  return AGENT_CONFIGS.find(config => config.id === id)
}

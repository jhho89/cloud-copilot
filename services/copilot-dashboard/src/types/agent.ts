/**
 * A2A Agent Types
 * Based on the A2A protocol specification
 */

export interface AgentSkill {
  id: string
  name: string
  description: string
  tags: string[]
}

export interface AgentCapabilities {
  streaming?: boolean
  pushNotifications?: boolean
  extensions?: Array<{ uri: string } | string>
  [key: string]: unknown
}

export interface AgentCard {
  name: string
  description: string
  url: string
  version: string
  protocolVersion: string
  capabilities: AgentCapabilities
  skills: AgentSkill[]
  defaultInputModes: string[]
  defaultOutputModes: string[]
  preferredTransport: string
  supportsAuthenticatedExtendedCard: boolean
}

export type AgentStatus = 'checking' | 'online' | 'unavailable'

export interface Agent {
  id: string
  name: string
  url: string  // A2A URL
  webUrl?: string  // Web UI URL (for user to visit)
  status: AgentStatus
  checkedAt?: string
  card?: AgentCard
  error?: string
}

export type ToolCapability =
  | 'kubernetes:read'
  | 'database:read'
  | 'cloud:cost:read'

export interface DashboardActivityScope {
  type: 'cluster' | 'project'
  id: string
  label: string
}

export type ActivityKind = 'investigation' | 'recommendation' | 'approval'

export interface DashboardActivityRecord {
  id: string
  kind: ActivityKind
  scope: DashboardActivityScope
  title: string
  status: string
  updatedAt: string
  summary?: string
}

export interface DashboardActivityResponse {
  generatedAt: string
  scopes: DashboardActivityScope[]
  investigations: DashboardActivityRecord[]
  recommendations: DashboardActivityRecord[]
  approvals: DashboardActivityRecord[]
}

export interface AgentMessage {
  role: 'user' | 'assistant'
  content: string
  timestamp: Date
}

export interface ChatSession {
  id: string
  agentId?: string
  messages: AgentMessage[]
  createdAt: Date
}

import { AGENT_CONFIGS } from '../config/agents'
import { canInvoke, getRoleById, type DashboardRole } from '../config/roles'

export type TargetAgentId = (typeof AGENT_CONFIGS)[number]['id']

const DBA_PATTERNS = [
  /\bdba\b/i,
  /\bdatabase\b/i,
  /\bdb\b/i,
  /\bmysql\b/i,
  /\bmariadb\b/i,
  /\bpostgres\b/i,
  /\bsql\b/i,
  /\bslow[- ]?quer/i,
  /\bindex recommendation/i,
]

const COST_PATTERNS = [
  /\bcost\b/i,
  /\bbilling\b/i,
  /\bbudget\b/i,
  /\bidle resources?\b/i,
  /\bsavings\b/i,
  /\brightsiz/i,
]

export function routeMessage(
  message: string,
  requestedAgentId?: TargetAgentId,
): TargetAgentId {
  if (requestedAgentId) return requestedAgentId
  if (DBA_PATTERNS.some((pattern) => pattern.test(message))) return 'dba-copilot'
  if (COST_PATTERNS.some((pattern) => pattern.test(message))) return 'cost-copilot'
  return 'k8s-copilot'
}

export interface MessageRoute {
  agentId: TargetAgentId
  allowed: boolean
}

export function routeMessageForRole(
  message: string,
  roleId: DashboardRole['id'],
  requestedAgentId?: TargetAgentId,
): MessageRoute {
  const agentId = routeMessage(message, requestedAgentId)
  const agent = AGENT_CONFIGS.find((config) => config.id === agentId)
  const role = getRoleById(roleId)

  return {
    agentId,
    allowed: Boolean(agent && canInvoke(role, agent.requiredCapability)),
  }
}

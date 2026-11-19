import type { ToolCapability } from '../types/agent'

export type DashboardRoleId = 'viewer' | 'operator'

export interface DashboardRole {
  id: DashboardRoleId
  label: string
  description: string
  capabilities: ToolCapability[]
}

export const ROLE_CONFIGS: DashboardRole[] = [
  {
    id: 'viewer',
    label: 'Viewer',
    description: 'Can read Kubernetes and cost findings. Database log inspection is disabled.',
    capabilities: ['kubernetes:read', 'cloud:cost:read'],
  },
  {
    id: 'operator',
    label: 'Operator',
    description: 'Can read Kubernetes, database diagnostics, and cost findings.',
    capabilities: ['kubernetes:read', 'database:read', 'cloud:cost:read'],
  },
]

export const DEFAULT_ROLE_ID =
  (import.meta.env.VITE_DASHBOARD_ROLE as DashboardRoleId | undefined) ?? 'viewer'

export function getRoleById(roleId: string): DashboardRole {
  return ROLE_CONFIGS.find((role) => role.id === roleId) ?? ROLE_CONFIGS[0]
}

export function canInvoke(role: DashboardRole, capability: ToolCapability): boolean {
  return role.capabilities.includes(capability)
}

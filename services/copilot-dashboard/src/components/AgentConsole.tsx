import { useCallback, useEffect, useState } from 'react'
import { Boxes, CloudCog, DatabaseZap, RefreshCw, ShieldCheck } from 'lucide-react'
import { AGENT_CONFIGS } from '../config/agents'
import { DEFAULT_ROLE_ID, getRoleById, type DashboardRoleId } from '../config/roles'
import { AgentCard } from './AgentCard'
import { UnifiedChat } from './UnifiedChat'
import { ActivityPanel } from './ActivityPanel'
import { discoverAgents } from '../lib/agent-client'
import { fetchActivityBundle, type ActivityState } from '../lib/dashboard-api'
import type { TargetAgentId } from '../lib/routing'
import type { Agent } from '../types/agent'

interface AgentConsoleProps {
  roleId?: DashboardRoleId
  agentFetchImpl?: typeof fetch
  activityFetchImpl?: typeof fetch
}

const QUICK_ACTIONS: Array<{
  id: string
  label: string
  prompt: string
  target: TargetAgentId
}> = [
  {
    id: 'cluster-health',
    label: 'Check cluster health',
    prompt: 'Check cluster health and summarize any issues.',
    target: 'k8s-copilot',
  },
  {
    id: 'slow-queries',
    label: 'Read recent slow queries',
    prompt: 'Read recent MySQL slow-query logs from the last five minutes.',
    target: 'dba-copilot',
  },
  {
    id: 'cost-review',
    label: 'Review cost savings',
    prompt: 'Analyze cloud costs and identify idle resources.',
    target: 'cost-copilot',
  },
]

function formatTime(value?: string): string {
  if (!value) return 'not yet'
  const time = new Date(value)
  if (Number.isNaN(time.getTime())) return 'unknown time'
  return time.toLocaleTimeString()
}

export function AgentConsole({
  roleId = DEFAULT_ROLE_ID,
  agentFetchImpl = fetch,
  activityFetchImpl = fetch,
}: AgentConsoleProps) {
  const role = getRoleById(roleId)
  const [agents, setAgents] = useState<Agent[]>(
    AGENT_CONFIGS.map((config) => ({
      id: config.id,
      name: config.displayName,
      url: config.baseUrl,
      webUrl: config.webUrl,
      status: 'checking',
    })),
  )
  const [checkedAt, setCheckedAt] = useState<string>()
  const [isChecking, setIsChecking] = useState(false)
  const [draft, setDraft] = useState('')
  const [activity, setActivity] = useState<ActivityState>({ source: 'loading' })
  const [selectedScopeId, setSelectedScopeId] = useState('all')

  const checkAgents = useCallback(async () => {
    setIsChecking(true)
    try {
      const result = await discoverAgents(AGENT_CONFIGS, agentFetchImpl)
      setAgents(result.agents)
      setCheckedAt(result.checkedAt)
    } finally {
      setIsChecking(false)
    }
  }, [agentFetchImpl])

  const refreshActivity = useCallback(async () => {
    setActivity(await fetchActivityBundle(import.meta.env.VITE_DASHBOARD_API_URL, activityFetchImpl))
  }, [activityFetchImpl])

  useEffect(() => {
    let active = true

    checkAgents().then(() => {
      if (active) refreshActivity()
    })

    return () => {
      active = false
    }
  }, [checkAgents, refreshActivity])

  useEffect(() => {
    if (activity.response?.scopes.length && activity.response.scopes.every((scope) => scope.id !== selectedScopeId)) {
      setSelectedScopeId(activity.response.scopes[0].id)
    }
  }, [activity.response, selectedScopeId])

  return (
    <main className="min-h-screen bg-slate-900 px-4 py-8 text-slate-100 sm:px-6 lg:px-8">
      <div className="mx-auto w-full max-w-7xl space-y-6">
        <header className="rounded-lg border border-slate-700 bg-slate-800/50 p-6">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="min-w-0">
              <h1 className="text-3xl font-black text-white sm:text-4xl">Cloud Copilot Console</h1>
              <p className="mt-2 max-w-3xl text-slate-300">
                Live agent availability, a unified read-only chat, and operational records for Kubernetes, DBA, and cost work.
              </p>
            </div>
            <div className="flex flex-col items-start gap-2 sm:items-end">
              <span className="inline-flex items-center gap-2 rounded-full border border-emerald-500/40 bg-emerald-500/10 px-3 py-1 text-sm font-medium text-emerald-300">
                <ShieldCheck size={16} aria-hidden="true" />
                Safe by default
              </span>
              <button
                type="button"
                onClick={checkAgents}
                disabled={isChecking}
                className="inline-flex items-center gap-2 rounded border border-slate-600 px-3 py-2 text-sm text-slate-200 hover:bg-slate-700 disabled:text-slate-500"
              >
                <RefreshCw className={isChecking ? 'animate-spin' : undefined} size={14} aria-hidden="true" />
                Refresh agents
                <span className="rounded-full bg-black/20 px-2 py-0.5 text-xs">Read-only</span>
              </button>
              <p className="text-xs text-slate-500">Cards checked {formatTime(checkedAt)}</p>
            </div>
          </div>
        </header>

        <section aria-labelledby="agents-heading">
          <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 id="agents-heading" className="text-2xl font-bold text-white">Agent availability</h2>
              <p className="text-sm text-slate-400">Discovered from live A2A agent cards.</p>
            </div>
            <span className="text-sm text-slate-400">Role: {role.label}</span>
          </div>
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {agents.map((agent) => (
              <AgentCard key={agent.id} agent={agent} />
            ))}
          </div>
        </section>

        <section aria-labelledby="quick-actions-heading" className="rounded-lg border border-slate-700 bg-slate-800/50 p-5">
          <h2 id="quick-actions-heading" className="text-xl font-bold text-white">Read-only quick actions</h2>
          <div className="mt-4 grid gap-3 md:grid-cols-3">
            {QUICK_ACTIONS.map((action) => {
              const config = AGENT_CONFIGS.find((agent) => agent.id === action.target)
              const allowed = Boolean(config && role.capabilities.includes(config.requiredCapability))
              const agent = agents.find((candidate) => candidate.id === action.target)
              const available = agent?.status === 'online'

              return (
                <button
                  key={action.id}
                  type="button"
                  disabled={!allowed || !available}
                  onClick={() => setDraft(action.prompt)}
                  title={allowed ? undefined : `Requires a role with ${config?.requiredCapability}`}
                  className="min-w-0 rounded-lg border border-slate-600 bg-slate-900/60 p-4 text-left transition-colors hover:border-cyan-500/50 hover:bg-slate-900 disabled:cursor-not-allowed disabled:border-slate-700 disabled:text-slate-500"
                >
                  <div className="flex items-center gap-2 text-slate-300">
                    {action.id === 'cluster-health' && <Boxes size={16} aria-hidden="true" />}
                    {action.id === 'slow-queries' && <DatabaseZap size={16} aria-hidden="true" />}
                    {action.id === 'cost-review' && <CloudCog size={16} aria-hidden="true" />}
                    <span className="text-xs font-semibold uppercase tracking-wide">
                      {action.id === 'cluster-health' ? 'Kubernetes' : action.id === 'slow-queries' ? 'DBA' : 'Cost'}
                    </span>
                  </div>
                  <div className="mt-2 break-words font-semibold text-white">{action.label}</div>
                  <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
                    <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-emerald-300">
                      Read-only
                    </span>
                    {!allowed && <span>Role restricted</span>}
                    {allowed && !available && <span>Agent unavailable</span>}
                  </div>
                </button>
              )
            })}
          </div>
        </section>

        <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
          <UnifiedChat
            agents={agents}
            role={role}
            draft={draft}
            onDraftChange={setDraft}
            fetchImpl={agentFetchImpl}
          />
          <ActivityPanel
            state={activity}
            selectedScopeId={selectedScopeId}
            onScopeChange={setSelectedScopeId}
            onRefresh={refreshActivity}
            isLoading={false}
          />
        </div>
      </div>
    </main>
  )
}

import type { Agent } from '../types/agent'
import { getAgentStats } from '../lib/agent-client'

interface AgentCardProps {
  agent: Agent
}

const STATUS_META = {
  checking: { label: 'Checking', dot: 'bg-amber-400', ring: 'border-amber-500/40' },
  online: { label: 'Online', dot: 'bg-emerald-400', ring: 'border-emerald-500/40' },
  unavailable: { label: 'Unavailable', dot: 'bg-red-400', ring: 'border-red-500/40' },
} as const

function formatCheckedAt(checkedAt?: string): string {
  if (!checkedAt) return 'Awaiting first check'
  const time = new Date(checkedAt)
  if (Number.isNaN(time.getTime())) return 'Unknown check time'
  return `Last checked ${time.toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })}`
}

export function AgentCard({ agent }: AgentCardProps) {
  const meta = STATUS_META[agent.status]
  const stats = agent.card ? getAgentStats(agent.card) : null

  return (
    <article className={`min-w-0 rounded-lg border bg-slate-800/50 p-5 ${meta.ring}`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="truncate text-xl font-bold text-white">
            {agent.card?.name || agent.name}
          </h3>
          <div className="mt-1 flex items-center gap-2 text-sm text-slate-300">
            <span className={`h-2 w-2 rounded-full ${meta.dot}`} aria-hidden="true" />
            <span>{meta.label}</span>
            <span aria-hidden="true">&middot;</span>
            <time dateTime={agent.checkedAt}>{formatCheckedAt(agent.checkedAt)}</time>
          </div>
        </div>
        <span className="rounded-full border border-slate-600 px-3 py-1 text-xs font-medium text-slate-200">
          Read-only
        </span>
      </div>

      {agent.card ? (
        <>
          <p className="mt-3 line-clamp-3 break-words text-sm text-slate-300">
            {agent.card.description}
          </p>
          {stats && (
            <dl className="mt-3 grid grid-cols-2 gap-2 text-sm">
              <div className="rounded border border-slate-700 bg-slate-900/50 p-2">
                <dt className="text-slate-400">Skills</dt>
                <dd className="text-white">{stats.agents}</dd>
              </div>
              <div className="rounded border border-slate-700 bg-slate-900/50 p-2">
                <dt className="text-slate-400">Extensions</dt>
                <dd className="text-white">{stats.tools}</dd>
              </div>
            </dl>
          )}
        </>
      ) : agent.error ? (
        <div className="mt-3 rounded border border-red-500/40 bg-red-950/40 p-3 text-sm text-red-200">
          {agent.error}
        </div>
      ) : (
        <p className="mt-3 text-sm text-slate-400">Reading the live agent card...</p>
      )}
    </article>
  )
}

import { AlertTriangle, Clock, Database, FileText, RefreshCw } from 'lucide-react'
import { recordsForScope, type ActivityState } from '../lib/dashboard-api'
import type { ActivityKind } from '../types/agent'

interface ActivityPanelProps {
  state: ActivityState
  selectedScopeId: string
  onScopeChange: (value: string) => void
  onRefresh: () => void
  isLoading: boolean
}

const KIND_META: Record<ActivityKind, { label: string; className: string }> = {
  investigation: { label: 'Investigation', className: 'border-cyan-500/40 bg-cyan-500/10 text-cyan-300' },
  recommendation: { label: 'Recommendation', className: 'border-blue-500/40 bg-blue-500/10 text-blue-300' },
  approval: { label: 'Approval', className: 'border-purple-500/40 bg-purple-500/10 text-purple-300' },
}

function formatTime(value: string): string {
  const time = new Date(value)
  if (Number.isNaN(time.getTime())) return value
  return time.toLocaleString()
}

export function ActivityPanel({
  state,
  selectedScopeId,
  onScopeChange,
  onRefresh,
  isLoading,
}: ActivityPanelProps) {
  const response = state.response
  const records = response ? recordsForScope(response, selectedScopeId) : []

  return (
    <section aria-labelledby="operations-heading" className="rounded-lg border border-slate-700 bg-slate-800/50 p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 id="operations-heading" className="text-xl font-bold text-white">Operations activity</h2>
          <p className="text-sm text-slate-400">
            Recent investigations, recommendations, and approvals by cluster or project.
          </p>
        </div>
        <button
          type="button"
          onClick={onRefresh}
          disabled={isLoading}
          className="inline-flex items-center gap-2 rounded border border-slate-600 px-3 py-2 text-sm text-slate-200 hover:bg-slate-700 disabled:text-slate-500"
        >
          <RefreshCw size={14} aria-hidden="true" />
          Refresh
          <span className="rounded-full bg-black/20 px-2 py-0.5 text-xs">Read-only</span>
        </button>
      </div>

      {state.source === 'unconfigured' ? (
        <div className="mt-4 rounded border border-amber-500/40 bg-amber-950/30 p-4 text-amber-200">
          <div className="mb-1 flex items-center gap-2 font-semibold">
            <AlertTriangle size={16} aria-hidden="true" />
            Activity feed not configured
          </div>
          <p className="text-sm">{state.error}</p>
        </div>
      ) : state.source === 'unavailable' ? (
        <div className="mt-4 rounded border border-red-500/40 bg-red-950/30 p-4 text-red-200">
          <div className="mb-1 flex items-center gap-2 font-semibold">
            <AlertTriangle size={16} aria-hidden="true" />
            Activity feed unavailable
          </div>
          <p className="text-sm">{state.error}</p>
        </div>
      ) : (
        <>
          <div className="mt-4 grid gap-3 sm:grid-cols-2">
            <label className="text-sm text-slate-300">
              Cluster or project
              <select
                value={selectedScopeId}
                onChange={(event) => onScopeChange(event.target.value)}
                className="mt-1 w-full rounded border border-slate-600 bg-slate-900 px-3 py-2 text-white"
              >
                <option value="all">All scopes</option>
                {response?.scopes.map((scope) => (
                  <option key={scope.id} value={scope.id}>
                    {scope.label}
                  </option>
                ))}
              </select>
            </label>
            <div className="rounded border border-slate-700 bg-slate-900/50 p-3 text-sm text-slate-300">
              <div className="flex items-center gap-2 font-semibold text-white">
                <Database size={14} aria-hidden="true" />
                Backend feed
              </div>
              <p className="mt-1">
                {response
                  ? `Updated ${formatTime(response.generatedAt)}`
                  : 'Waiting for the operations feed'}
              </p>
            </div>
          </div>

          {records.length === 0 ? (
            <div className="mt-4 rounded border border-slate-600 bg-slate-900/50 p-4 text-sm text-slate-300">
              No investigations, recommendations, or approvals are reported for this scope.
            </div>
          ) : (
            <ul className="mt-4 space-y-3">
              {records.map((record) => (
                <li key={`${record.kind}-${record.id}`} className="min-w-0 rounded border border-slate-700 bg-slate-900/50 p-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className={`rounded-full border px-2 py-1 text-xs font-medium ${KIND_META[record.kind].className}`}>
                      {KIND_META[record.kind].label}
                    </span>
                    <span className="truncate text-sm text-slate-300">{record.scope.label}</span>
                    <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-xs text-emerald-300">
                      Read-only
                    </span>
                  </div>
                  <h3 className="mt-2 break-words text-base font-semibold text-white">{record.title}</h3>
                  <div className="mt-1 flex items-center gap-2 text-sm text-slate-400">
                    <FileText size={14} aria-hidden="true" />
                    <span>{record.status}</span>
                    <span aria-hidden="true">&middot;</span>
                    <Clock size={14} aria-hidden="true" />
                    <time dateTime={record.updatedAt}>{formatTime(record.updatedAt)}</time>
                  </div>
                  {record.summary && (
                    <p className="mt-2 break-words text-sm text-slate-300">{record.summary}</p>
                  )}
                </li>
              ))}
            </ul>
          )}
        </>
      )}
    </section>
  )
}

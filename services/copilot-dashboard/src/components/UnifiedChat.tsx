import { useMemo, useState } from 'react'
import { Lock, Send, ShieldCheck, Loader2 } from 'lucide-react'
import { AGENT_CONFIGS } from '../config/agents'
import type { DashboardRole } from '../config/roles'
import { routeMessageForRole, type TargetAgentId } from '../lib/routing'
import { sendAgentMessage } from '../lib/agent-client'
import type { Agent } from '../types/agent'

interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  isError?: boolean
}

interface UnifiedChatProps {
  agents: Agent[]
  role: DashboardRole
  draft: string
  onDraftChange: (value: string) => void
  fetchImpl?: typeof fetch
}

function createId(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`
}

export function UnifiedChat({
  agents,
  role,
  draft,
  onDraftChange,
  fetchImpl = fetch,
}: UnifiedChatProps) {
  const [selectedTarget, setSelectedTarget] = useState<'auto' | TargetAgentId>('auto')
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [isSending, setIsSending] = useState(false)

  const route = useMemo(
    () => routeMessageForRole(draft || 'cluster health', role.id, selectedTarget === 'auto' ? undefined : selectedTarget),
    [draft, role.id, selectedTarget],
  )
  const target = AGENT_CONFIGS.find((config) => config.id === route.agentId)
  const agent = agents.find((candidate) => candidate.id === route.agentId)

  async function handleSubmit(event: React.FormEvent) {
    event.preventDefault()
    const message = draft.trim()
    if (!message || isSending || !target) return

    if (!route.allowed) {
      setMessages((current) => [
        ...current,
        {
          id: createId('blocked'),
          role: 'assistant',
          content: `Your ${role.label} role does not allow ${target.displayName}.`,
          isError: true,
        },
      ])
      return
    }

    if (agent?.status !== 'online') {
      setMessages((current) => [
        ...current,
        {
          id: createId('unavailable'),
          role: 'assistant',
          content: `${target.displayName} is unavailable. Check the live agent card and try again later.`,
          isError: true,
        },
      ])
      return
    }

    onDraftChange('')
    setIsSending(true)
    setMessages((current) => [...current, { id: createId('user'), role: 'user', content: message }])

    try {
      const response = await sendAgentMessage(target, message, fetchImpl)
      setMessages((current) => [
        ...current,
        { id: createId('reply'), role: 'assistant', content: response },
      ])
    } catch (error) {
      setMessages((current) => [
        ...current,
        {
          id: createId('error'),
          role: 'assistant',
          content: error instanceof Error ? error.message : 'Agent request failed.',
          isError: true,
        },
      ])
    } finally {
      setIsSending(false)
    }
  }

  return (
    <section aria-labelledby="unified-chat-heading" className="rounded-lg border border-slate-700 bg-slate-800/50 p-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 id="unified-chat-heading" className="text-xl font-bold text-white">Unified chat</h2>
          <p className="text-sm text-slate-400">
            Routes to Kubernetes, DBA, or Cost agents based on your request.
          </p>
        </div>
        <span className="inline-flex items-center gap-2 rounded-full border border-emerald-500/40 bg-emerald-500/10 px-3 py-1 text-xs font-medium text-emerald-300">
          <ShieldCheck size={14} aria-hidden="true" />
          Read-only
        </span>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-2">
        <label className="text-sm text-slate-300">
          Target
          <select
            value={selectedTarget}
            onChange={(event) => setSelectedTarget(event.target.value as 'auto' | TargetAgentId)}
            className="mt-1 w-full rounded border border-slate-600 bg-slate-900 px-3 py-2 text-white"
          >
            <option value="auto">Auto-route</option>
            {AGENT_CONFIGS.map((config) => {
              const allowed = role.capabilities.includes(config.requiredCapability)
              return (
                <option key={config.id} value={config.id} disabled={!allowed}>
                  {config.displayName}
                  {allowed ? '' : ' (role restricted)'}
                </option>
              )
            })}
          </select>
        </label>
        <div className="rounded border border-slate-700 bg-slate-900/50 p-3 text-sm text-slate-300">
          <div className="font-semibold text-white">Route preview</div>
          <div className="mt-1 truncate">{target?.displayName}</div>
          {!route.allowed && (
            <div className="mt-1 inline-flex items-center gap-1 text-amber-300">
              <Lock size={14} aria-hidden="true" />
              Blocked by {role.label} role
            </div>
          )}
        </div>
      </div>

      <div className="mt-4 min-h-32 space-y-3 rounded border border-slate-700 bg-slate-900/50 p-4">
        {messages.length === 0 ? (
          <p className="text-sm text-slate-400">
            No messages sent. Ask a read-only diagnostic question to begin.
          </p>
        ) : (
          messages.map((message) => (
            <div
              key={message.id}
              className={`max-w-full break-words rounded border p-3 text-sm ${
                message.role === 'user'
                  ? 'border-cyan-500/40 bg-cyan-500/10 text-cyan-100'
                  : message.isError
                    ? 'border-red-500/40 bg-red-950/30 text-red-200'
                    : 'border-slate-600 bg-slate-800 text-slate-100'
              }`}
            >
              <div className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-400">
                {message.role}
              </div>
              {message.content}
            </div>
          ))
        )}
        {isSending && (
          <div className="flex items-center gap-2 text-sm text-slate-400">
            <Loader2 className="animate-spin" size={16} aria-hidden="true" />
            Waiting for agent response
          </div>
        )}
      </div>

      <form onSubmit={handleSubmit} className="mt-4 flex flex-col gap-3 sm:flex-row">
        <label className="min-w-0 flex-1 text-sm text-slate-300">
          Request
          <textarea
            value={draft}
            onChange={(event) => onDraftChange(event.target.value)}
            rows={3}
            placeholder="Check the health of the production cluster"
            className="mt-1 w-full resize-y rounded border border-slate-600 bg-slate-900 px-3 py-2 text-white placeholder:text-slate-500"
          />
        </label>
        <button
          type="submit"
          disabled={!draft.trim() || isSending}
          className="inline-flex items-center justify-center gap-2 self-start rounded bg-cyan-600 px-4 py-2 font-semibold text-white transition-colors hover:bg-cyan-700 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
        >
          <Send size={16} aria-hidden="true" />
          Send
          <span className="rounded-full bg-black/20 px-2 py-0.5 text-xs">Read-only</span>
        </button>
      </form>
      <p className="mt-2 text-xs text-slate-500">
        Console role: {role.label}. Requests are routed read-only; no write or approval tools are exposed.
      </p>
    </section>
  )
}

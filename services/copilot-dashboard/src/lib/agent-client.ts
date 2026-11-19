import type {
  Agent,
  AgentCard,
} from '../types/agent'
import type { AgentConfig } from '../config/agents'

const CARD_PATH = '/.well-known/agent-card.json'

function createRequestId(): string {
  return `dashboard-${Date.now()}-${Math.random().toString(16).slice(2)}`
}

export function cardUrl(baseUrl: string): string {
  return `${baseUrl.replace(/\/+$/, '')}${CARD_PATH}`
}

export async function fetchAgentCard(
  baseUrl: string,
  fetchImpl: typeof fetch = fetch,
): Promise<AgentCard> {
  const response = await fetchImpl(cardUrl(baseUrl), {
    cache: 'no-store',
    headers: { accept: 'application/json' },
    signal: AbortSignal.timeout(5000),
  })

  if (!response.ok) {
    throw new Error(`Agent card request failed (${response.status})`)
  }

  const card = (await response.json()) as AgentCard
  if (!card?.name || !card?.url) {
    throw new Error('Agent card is missing required fields')
  }

  return card
}

export function discoverAgent(
  config: AgentConfig,
  result: PromiseSettledResult<AgentCard>,
  checkedAt: string,
): Agent {
  if (result.status === 'fulfilled') {
    return {
      id: config.id,
      name: config.displayName,
      url: config.baseUrl,
      webUrl: config.webUrl,
      status: 'online',
      checkedAt,
      card: result.value,
    }
  }

  return {
    id: config.id,
    name: config.displayName,
    url: config.baseUrl,
    webUrl: config.webUrl,
    status: 'unavailable',
      checkedAt,
    error: result.reason instanceof Error ? result.reason.message : 'Agent card unavailable',
  }
}

export async function discoverAgents(
  configs: AgentConfig[],
  fetchImpl: typeof fetch = fetch,
): Promise<{ agents: Agent[]; checkedAt: string }> {
  const checkedAt = new Date().toISOString()
  const results = await Promise.allSettled(
    configs.map((config) => fetchAgentCard(config.baseUrl, fetchImpl)),
  )

  return {
    agents: configs.map((config, index) => discoverAgent(config, results[index], checkedAt)),
    checkedAt,
  }
}

export function getAgentStats(card: AgentCard): { agents: number; tools: number } {
  return {
    agents: card.skills?.length ?? 0,
    tools: card.capabilities?.extensions?.length ?? 0,
  }
}

function collectText(value: unknown): string {
  if (typeof value === 'string') return value
  if (Array.isArray(value)) return value.map(collectText).filter(Boolean).join('\n')
  if (value && typeof value === 'object') {
    return Object.values(value).map(collectText).filter(Boolean).join('\n')
  }
  return ''
}

export async function sendAgentMessage(
  config: AgentConfig,
  message: string,
  fetchImpl: typeof fetch = fetch,
): Promise<string> {
  const response = await fetchImpl(`${config.baseUrl.replace(/\/+$/, '')}/`, {
    method: 'POST',
    headers: {
      accept: 'application/json',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: createRequestId(),
      method: 'message/send',
      params: {
        message: {
          role: 'user',
          messageId: createRequestId(),
          parts: [{ kind: 'text', text: message }],
        },
      },
    }),
  })

  if (!response.ok) {
    throw new Error(`Agent request failed (${response.status})`)
  }

  const payload = (await response.json()) as {
    result?: unknown
    error?: { message?: string }
  }
  if (payload.error?.message) {
    throw new Error(payload.error.message)
  }

  const text = collectText(payload.result).trim()
  if (!text) {
    throw new Error('Agent returned an empty response')
  }

  return text
}

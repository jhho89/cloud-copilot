import { useState } from 'react'
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { UnifiedChat } from './UnifiedChat'
import { getAgentConfig } from '../config/agents'
import { getRoleById } from '../config/roles'
import type { Agent, AgentCard } from '../types/agent'

const card: AgentCard = {
  name: 'DBA Copilot',
  description: 'Database diagnostics',
  url: 'https://k8s.example.com',
  version: '1.0.0',
  protocolVersion: '1.0',
  capabilities: {},
  skills: [],
  defaultInputModes: [],
  defaultOutputModes: [],
  preferredTransport: 'HTTP',
  supportsAuthenticatedExtendedCard: false,
}

const dbaAgent: Agent = {
  id: 'dba-copilot',
  name: 'DBA Copilot',
  url: 'https://k8s.example.com',
  webUrl: 'https://k8s.example.com/dev-ui',
  status: 'online',
  checkedAt: '2026-01-01T12:00:00Z',
  card,
}

function ChatHarness({
  agentOverrides,
  roleId,
  fetchImpl,
}: {
  agentOverrides: Partial<Agent>
  roleId: 'viewer' | 'operator'
  fetchImpl?: typeof fetch
}) {
  const [draft, setDraft] = useState('')

  return (
    <UnifiedChat
      agents={[{ ...dbaAgent, ...agentOverrides }]}
      role={getRoleById(roleId)}
      draft={draft}
      onDraftChange={setDraft}
      fetchImpl={fetchImpl}
    />
  )
}

function renderChat(
  overrides: Partial<Agent> = {},
  roleId: 'viewer' | 'operator' = 'operator',
  fetchImpl?: typeof fetch,
) {
  return render(<ChatHarness agentOverrides={overrides} roleId={roleId} fetchImpl={fetchImpl} />)
}

async function submit(
  message: string,
  options: { waitForFetch?: boolean; fetchImpl?: ReturnType<typeof vi.fn> } = {},
) {
  fireEvent.change(screen.getByLabelText('Request'), { target: { value: message } })
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: /send/i }))
  })
  if (options.waitForFetch) {
    await vi.waitFor(() => expect(options.fetchImpl).toHaveBeenCalled())
  }
}

describe('unified chat safety and routing', () => {
  afterEach(cleanup)

  it('blocks a DBA request when the role lacks capability', async () => {
    const fetchImpl = vi.fn()
    renderChat({ status: 'unavailable' }, 'viewer', fetchImpl as unknown as typeof fetch)
    await submit('Show recent MySQL slow queries')

    expect(screen.getByText('Your Viewer role does not allow DBA Copilot.')).toBeTruthy()
    expect(fetchImpl).not.toHaveBeenCalled()
  })

  it('does not send to an unavailable agent', async () => {
    const fetchImpl = vi.fn()
    renderChat({ status: 'unavailable' }, 'operator', fetchImpl as unknown as typeof fetch)
    await submit('Show recent MySQL slow queries')

    expect(screen.getByText(/DBA Copilot is unavailable/i)).toBeTruthy()
    expect(fetchImpl).not.toHaveBeenCalled()
  })

  it('sends an allowed request using the configured A2A URL', async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ result: { content: [{ text: 'No slow queries found' }] } }), {
        status: 200,
      }),
    )
    renderChat({}, 'operator', fetchImpl as unknown as typeof fetch)
    await submit('Show recent MySQL slow queries', { waitForFetch: true, fetchImpl })

    await vi.waitFor(() => expect(screen.getByText('No slow queries found')).toBeTruthy())
    expect(fetchImpl.mock.calls[0][0]).toBe(`${getAgentConfig('dba-copilot')?.baseUrl}/`)
  })
})

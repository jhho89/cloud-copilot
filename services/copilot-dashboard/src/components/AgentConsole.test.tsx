import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { AgentConsole } from './AgentConsole'
import type { AgentCard } from '../types/agent'

const card: AgentCard = {
  name: 'Test Service',
  description: 'Live service card',
  url: 'https://service.example.com',
  version: '1.0.0',
  protocolVersion: '1.0',
  capabilities: {},
  skills: [],
  defaultInputModes: [],
  defaultOutputModes: [],
  preferredTransport: 'HTTP',
  supportsAuthenticatedExtendedCard: false,
}

describe('agent console discovery', () => {
  afterEach(cleanup)

  it('shows online cards and the activity unavailable state', async () => {
    const agentFetchImpl = vi.fn().mockImplementation(async () =>
      new Response(JSON.stringify(card), { status: 200 }),
    )
    render(<AgentConsole roleId="operator" agentFetchImpl={agentFetchImpl} />)

    expect(await screen.findAllByText('Live service card')).toHaveLength(3)
    expect(await screen.findByText('Activity feed not configured')).toBeTruthy()
    expect(agentFetchImpl).toHaveBeenCalledTimes(3)
    await vi.waitFor(() => expect(screen.getAllByText('Online')).toHaveLength(3))
  })

  it('marks every configured agent unavailable when discovery fails', async () => {
    const agentFetchImpl = vi.fn().mockRejectedValue(new Error('Discovery failed'))
    render(<AgentConsole roleId="operator" agentFetchImpl={agentFetchImpl} />)

    expect(await screen.findAllByText('Unavailable')).toHaveLength(3)
    expect(screen.getAllByText('Discovery failed')).toHaveLength(3)
  })
})

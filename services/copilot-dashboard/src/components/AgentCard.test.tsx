import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { AgentCard } from './AgentCard'
import type { Agent, AgentCard as AgentCardData } from '../types/agent'

const card: AgentCardData = {
  name: 'K8s Copilot',
  description: 'Kubernetes diagnostics',
  url: 'https://k8s.example.com',
  version: '1.0.0',
  protocolVersion: '1.0',
  capabilities: {},
  skills: [{ id: 'diagnose', name: 'Diagnose', description: 'Diagnose', tags: [] }],
  defaultInputModes: [],
  defaultOutputModes: [],
  preferredTransport: 'HTTP',
  supportsAuthenticatedExtendedCard: false,
}

function agent(overrides: Partial<Agent>): Agent {
  return {
    id: 'k8s-copilot',
    name: 'K8s Copilot',
    url: 'https://k8s.example.com',
    status: 'checking',
    ...overrides,
  }
}

describe('agent availability card', () => {
  afterEach(cleanup)

  it('shows a loading state before the agent card resolves', () => {
    render(<AgentCard agent={agent({})} />)
    expect(screen.getByText('Checking')).toBeTruthy()
    expect(screen.getByText('Reading the live agent card...')).toBeTruthy()
  })

  it('shows live availability and last-checked time', () => {
    render(<AgentCard agent={agent({ status: 'online', checkedAt: '2026-01-01T12:00:00Z', card })} />)
    expect(screen.getByText('Online')).toBeTruthy()
    expect(screen.getByText(/Last checked/)).toBeTruthy()
    expect(screen.getAllByText('Read-only')[0]).toBeTruthy()
  })

  it('shows an unavailable state and error', () => {
    render(<AgentCard agent={agent({ status: 'unavailable', error: 'Agent card request failed (503)' })} />)
    expect(screen.getByText('Unavailable')).toBeTruthy()
    expect(screen.getByText('Agent card request failed (503)')).toBeTruthy()
  })
})

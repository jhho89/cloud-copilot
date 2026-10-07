import type { ComponentType } from 'react'
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getAgentConfig } from './config/agents'

const routeState = vi.hoisted(() => ({ id: 'k8s-copilot' }))

vi.mock('@tanstack/react-router', () => ({
  createFileRoute: () => (options: { component: ComponentType }) => ({
    useParams: () => ({ id: routeState.id }),
    component: options.component,
  }),
  useRouter: () => ({ history: { back: () => undefined } }),
}))

import { Route } from './routes/agents.$id'

function renderAgentDetail() {
  const Page = (Route as unknown as { component: ComponentType }).component
  return render(<Page />)
}

describe('Gemini 3.8 Flash dashboard copy', () => {
  beforeEach(() => {
    routeState.id = 'k8s-copilot'
  })

  afterEach(() => {
    cleanup()
  })

  it('describes the Kubernetes copilot as Gemini 3.8 Flash', () => {
    const agent = getAgentConfig('k8s-copilot')
    expect(agent?.description).toContain('Gemini 3.8 Flash')
    expect(agent?.description).not.toContain('Gemini 2.5')
  })

  it('shows Gemini 3.8 Flash on the agent detail page', () => {
    renderAgentDetail()

    expect(
      screen.getByText(/powered by Gemini 3\.8 Flash/i),
    ).toBeTruthy()
    expect(
      screen.getByText('Gemini 3.8 Flash processes requests with specialized tools'),
    ).toBeTruthy()
    expect(screen.queryByText(/Gemini 2\.5/)).toBeNull()
  })

  it('keeps the missing-agent state when the id is unknown', () => {
    routeState.id = 'missing-agent'
    renderAgentDetail()

    expect(screen.getByText('Agent Not Found')).toBeTruthy()
    expect(screen.queryByText(/Gemini 3\.8 Flash processes requests/)).toBeNull()
  })
})

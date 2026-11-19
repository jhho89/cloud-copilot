import { describe, expect, it } from 'vitest'
import { routeMessage, routeMessageForRole } from './routing'

describe('unified chat routing', () => {
  it('routes database questions to the DBA agent', () => {
    expect(routeMessage('Show recent MySQL slow queries')).toBe('dba-copilot')
  })

  it('routes cost questions to the cost agent', () => {
    expect(routeMessage('Find idle resources and cost savings')).toBe('cost-copilot')
  })

  it('routes remaining questions to the Kubernetes agent', () => {
    expect(routeMessage('Why is the frontend deployment crashing?')).toBe('k8s-copilot')
  })

  it('blocks DBA requests for the viewer role', () => {
    const route = routeMessageForRole('Show recent MySQL slow queries', 'viewer')
    expect(route).toEqual({ agentId: 'dba-copilot', allowed: false })
  })

  it('allows DBA requests for the operator role', () => {
    const route = routeMessageForRole('Show recent MySQL slow queries', 'operator')
    expect(route).toEqual({ agentId: 'dba-copilot', allowed: true })
  })
})

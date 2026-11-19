import { describe, expect, it, vi } from 'vitest'
import { fetchActivityBundle, recordsForScope } from './dashboard-api'
import type { DashboardActivityResponse } from '../types/agent'

describe('dashboard activity API', () => {
  it('reports an unconfigured backend without inventing data', async () => {
    const state = await fetchActivityBundle(undefined)
    expect(state.source).toBe('unconfigured')
    expect(state.error).toContain('VITE_DASHBOARD_API_URL')
    expect(state.response).toBeUndefined()
  })

  it('reports an unavailable backend when the request fails', async () => {
    const fetchImpl = vi.fn().mockResolvedValue(new Response('not found', { status: 404 }))
    const state = await fetchActivityBundle('https://ops.example.com', fetchImpl)

    expect(state.source).toBe('unavailable')
    expect(state.error).toContain('404')
    expect(state.response).toBeUndefined()
  })

  it('shows an empty result when a configured backend has no records', async () => {
    const emptyResponse: DashboardActivityResponse = {
      generatedAt: '2026-01-01T00:00:00Z',
      scopes: [
        { type: 'cluster', id: 'cluster-a', label: 'Cluster A' },
      ],
      investigations: [],
      recommendations: [],
      approvals: [],
    }
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(emptyResponse), { status: 200 }),
    )
    const state = await fetchActivityBundle('https://ops.example.com', fetchImpl)

    expect(state.source).toBe('loaded')
    expect(recordsForScope(emptyResponse, 'cluster-a')).toEqual([])
  })
})

import type { DashboardActivityRecord, DashboardActivityResponse } from '../types/agent'

export type ActivitySource = 'loading' | 'unconfigured' | 'unavailable' | 'loaded'

export interface ActivityState {
  source: ActivitySource
  response?: DashboardActivityResponse
  error?: string
}

export function normalizeRecords(
  records: unknown,
  kind: DashboardActivityRecord['kind'],
): DashboardActivityRecord[] {
  if (!Array.isArray(records)) return []

  return records.flatMap((record) => {
    const item = record as Partial<DashboardActivityRecord> & {
      scope?: Partial<DashboardActivityRecord['scope']>
    }
    if (!item?.id || !item?.title || !item?.status || !item?.updatedAt || !item?.scope) {
      return []
    }

    return [{
      id: item.id,
      kind,
      scope: {
        type: item.scope.type === 'project' ? 'project' : 'cluster',
        id: item.scope.id ?? 'unknown',
        label: item.scope.label ?? item.scope.id ?? 'Unknown',
      },
      title: item.title,
      status: item.status,
      updatedAt: item.updatedAt,
      summary: item.summary,
    }]
  })
}

export async function fetchActivityBundle(
  baseUrl: string | undefined,
  fetchImpl: typeof fetch = fetch,
): Promise<ActivityState> {
  if (!baseUrl) {
    return {
      source: 'unconfigured',
      error: 'Set VITE_DASHBOARD_API_URL to enable the operations feed.',
    }
  }

  try {
    const response = await fetchImpl(
      `${baseUrl.replace(/\/+$/, '')}/api/dashboard/activity`,
      {
        cache: 'no-store',
        headers: { accept: 'application/json' },
        signal: AbortSignal.timeout(5000),
      },
    )
    if (!response.ok) {
      throw new Error(`Operations feed request failed (${response.status})`)
    }

    const payload = (await response.json()) as Partial<DashboardActivityResponse>
    const scopes = Array.isArray(payload.scopes) ? payload.scopes : []
    return {
      source: 'loaded',
      response: {
        generatedAt: payload.generatedAt ?? new Date().toISOString(),
        scopes,
        investigations: normalizeRecords(payload.investigations, 'investigation'),
        recommendations: normalizeRecords(payload.recommendations, 'recommendation'),
        approvals: normalizeRecords(payload.approvals, 'approval'),
      },
    }
  } catch (error) {
    return {
      source: 'unavailable',
      error: error instanceof Error ? error.message : 'Operations feed unavailable',
    }
  }
}

export function recordsForScope(
  response: DashboardActivityResponse,
  scopeId: string,
): DashboardActivityRecord[] {
  const all = [
    ...response.investigations,
    ...response.recommendations,
    ...response.approvals,
  ]
  return all
    .filter((record) => scopeId === 'all' || record.scope.id === scopeId)
    .sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime())
}

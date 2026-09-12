import type { RunEvent } from '../../shared/api/types.ts'

/** Only use the gateway's safe projection, never raw provider output or another run's events. */
export function failureSummary(events: RunEvent[], runId: string): string | null {
  const failures = events
    .filter(
      (event) =>
        event.data.run_id === runId &&
        event.data.durability === 'durable' &&
        /\.(failed|rejected|timed_out)$/.test(event.event),
    )
    .sort((a, b) => Number(b.data.sequence) - Number(a.data.sequence))
  for (const event of failures) {
    const data = event.data.data
    if (!data || typeof data !== 'object' || Array.isArray(data)) continue
    const summary = (data as Record<string, unknown>).safe_summary
    if (typeof summary === 'string' && summary.trim()) return summary
  }
  return null
}

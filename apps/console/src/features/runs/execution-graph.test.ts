import { test } from 'node:test'
import assert from 'node:assert/strict'
import { executionNodes, executionDuration } from './execution-graph.ts'
import type { RunEvent } from '../../shared/api/types.ts'
function event(sequence: number, source: string, type: string): RunEvent {
  return {
    id: `cursor${sequence}`,
    event: type,
    data: {
      event_id: `event${sequence}`,
      sequence,
      durability: 'durable',
      occurred_at: `2026-09-12T00:00:0${sequence}Z`,
      data: {
        source_kind: 'model_turn',
        source_id: source,
        source_projection_version: sequence,
        safe_summary: null,
      },
    },
  }
}
test('interleaved calls are grouped by identity, deduplicated and ordered by sequence', () => {
  const start = event(1, 'a', 'model.started')
  const end = event(3, 'a', 'model.completed')
  const nodes = executionNodes([end, event(2, 'b', 'model.started'), start, end])
  assert.equal(nodes.length, 2)
  assert.equal(nodes[0]!.sourceId, 'a')
  assert.equal(nodes[0]!.events.length, 2)
  assert.equal(nodes[0]!.latest.event, 'model.completed')
  assert.equal(executionDuration(nodes[0]!), '2.0 秒')
})
test('partial history does not invent a start time; run and live events are not execution nodes', () => {
  const end = event(3, 'a', 'model.completed')
  const run = event(4, 'run', 'run.completed')
  run.data.data = { source_kind: 'run', source_id: 'run' }
  const live = event(2, 'a', 'model.delta')
  live.data.durability = 'live_only'
  const nodes = executionNodes([end, run, live])
  assert.equal(nodes.length, 1)
  assert.equal(executionDuration(nodes[0]!), '—')
})

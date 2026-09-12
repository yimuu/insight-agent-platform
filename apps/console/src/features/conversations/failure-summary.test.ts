import { test } from 'node:test'
import assert from 'node:assert/strict'
import { failureSummary } from './failure-summary.ts'
import type { RunEvent } from '../../shared/api/types.ts'

const event = (run: string, type: string, summary: unknown, sequence = 1): RunEvent => ({
  id: `cursor-${sequence}`,
  event: type,
  data: { run_id: run, durability: 'durable', sequence, data: { safe_summary: summary } },
})

test('failure summary uses the latest safe failed event from the requested run', () => {
  assert.equal(
    failureSummary(
      [
        event('run-a', 'model.failed', '模型输出超出约束。'),
        event('run-a', 'node.failed', null, 2),
        event('run-b', 'model.failed', '另一会话的错误', 3),
        event('run-a', 'model.completed', '成功摘要', 4),
      ],
      'run-a',
    ),
    '模型输出超出约束。',
  )
})

test('absent, malformed and live summaries do not invent a diagnosis', () => {
  const live = event('run-a', 'model.failed', '瞬时文本')
  live.data.durability = 'live_only'
  assert.equal(
    failureSummary([live, event('run-a', 'model.failed', { secret: 'x' })], 'run-a'),
    null,
  )
  assert.equal(failureSummary([], 'run-a'), null)
})

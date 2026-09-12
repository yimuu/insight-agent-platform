import test from 'node:test'
import assert from 'node:assert/strict'
import { configurationAlias, destinationLabel } from './presentation.ts'

test('generated aliases fit the existing contract without asking for a technical identifier', () => {
  for (const kind of ['source', 'model'] as const) {
    const first = configurationAlias(kind)
    assert.match(first, /^[a-z][a-z0-9._-]{0,63}$/)
    assert.notEqual(configurationAlias(kind), first)
  }
})

test('friendly names identify exact hosts and do not trust misleading host substrings', () => {
  const base = {
    destination_digest: '',
    endpoint_identity_digest: '',
    protocol: 'open_ai_responses' as const,
    region: 'cn-beijing',
  }
  assert.equal(
    destinationLabel({ ...base, base_url: 'https://dashscope.aliyuncs.com:443/compatible-mode' }),
    '阿里百炼 · 北京',
  )
  assert.equal(
    destinationLabel({ ...base, base_url: 'https://dashscope.aliyuncs.com.example.test' }),
    'dashscope.aliyuncs.com.example.test · cn-beijing',
  )
})

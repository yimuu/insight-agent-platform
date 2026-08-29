import test from 'node:test'
import assert from 'node:assert/strict'
import { REDACTED, discoverTaskIds, newReceipt, redact, safeJson } from './security.ts'

test('redaction removes sensitive values recursively without mutating safe metadata', () => {
  const source = {
    trace_id: '0123456789abcdef0123456789abcdef',
    nested: { access_token: 'bearer-secret', safe_prompt_key: 'approval.release', raw_body: 'private' },
    items: [{ credential: 'credential-value', state: 'waiting' }],
  }
  assert.deepEqual(redact(source), {
    trace_id: source.trace_id,
    nested: { access_token: REDACTED, safe_prompt_key: 'approval.release', raw_body: REDACTED },
    items: [{ credential: REDACTED, state: 'waiting' }],
  })
  const rendered = safeJson(source)
  assert.equal(rendered.includes('bearer-secret'), false)
  assert.equal(rendered.includes('credential-value'), false)
  assert.equal(rendered.includes('private'), false)
})

test('task discovery extracts exact durable task identifiers once', () => {
  const interaction = 'int_01234567-89ab-7cde-8fab-0123456789ab'
  const approval = 'apv_abcdef01-2345-7abc-9def-0123456789ab'
  assert.deepEqual(discoverTaskIds({ interaction, nested: [approval, interaction] }), [interaction, approval])
})

test('receipts are printable, scoped, and unique', () => {
  const first = newReceipt('Task Approve / Version 7')
  const second = newReceipt('Task Approve / Version 7')
  assert.match(first, /^console-task-approve-version-7-[0-9a-f-]{36}$/)
  assert.notEqual(first, second)
})

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { executionValueSections } from './execution-value.ts'

test('model inputs show distinct message roles and collapse system instructions', () => {
  const sections = executionValueSections(
    {
      messages: [
        {
          role: 'platform',
          parts: [{ kind: 'text', value: '系统规则' }],
          source: { source_id: 'internal-id' },
        },
        { role: 'user', parts: [{ kind: 'text', value: '证明勾股定理' }] },
      ],
    },
    true,
  )
  assert.deepEqual(sections, [
    { label: '系统指令', text: '系统规则', collapsed: true },
    { label: '用户输入', text: '证明勾股定理', collapsed: false },
  ])
  assert.ok(!JSON.stringify(sections).includes('internal-id'))
})

test('model result displays its response and usage, not observation envelope', () => {
  const sections = executionValueSections(
    {
      structured_output: { value: { answer: '具体证明' } },
      usage: { input_tokens: 10, output_tokens: null },
      observation: { provider_response_digest: 'digest' },
      tool_intents: [],
      finish_reason: 'completed',
    },
    true,
  )
  assert.equal(sections[0]?.text, '具体证明')
  assert.ok(sections.some((section) => section.text.includes('输出 未报告')))
  assert.ok(!JSON.stringify(sections).includes('digest'))
  assert.deepEqual(executionValueSections({ message: '不同的原始输入' }, false), [
    { label: '用户消息', text: '不同的原始输入' },
  ])
})

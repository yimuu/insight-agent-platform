import assert from 'node:assert/strict'
import test from 'node:test'

import { invokeCapability } from './graph.mjs'

test('the exact LangGraph graph returns one bounded typed result', async () => {
  assert.equal(await invokeCapability('bounded request'), 'langgraph: bounded request')
  await assert.rejects(() => invokeCapability(''), /Too small/)
  await assert.rejects(() => invokeCapability('x'.repeat(257)), /Too big/)
})

import test from 'node:test'
import assert from 'node:assert/strict'
import { liveAnswer } from './live-answer.ts'
test('structured previews expose only the root answer as it arrives', () => {
  assert.equal(liveAnswer('{"answer":"你好'), '你好')
  assert.equal(liveAnswer('{"answer":"你好\\n世界"}'), '你好\n世界')
  assert.equal(liveAnswer('{"trace":{"answer":"hidden"},"answer":"shown'), 'shown')
  assert.equal(liveAnswer('{"description":"answer","answer":"yes"}'), 'yes')
  assert.equal(liveAnswer('{"ans'), '')
  assert.equal(liveAnswer('plain answer'), 'plain answer')
})
test('partial JSON escapes never create replacement characters', () => {
  assert.equal(liveAnswer('{"answer":"x\\u4f'), 'x')
  assert.equal(liveAnswer('{"answer":"x\\u4f60'), 'x你')
  assert.equal(liveAnswer('{"answer":"\\ud83d'), '')
  assert.equal(liveAnswer('{"answer":"\\ud83d\\ude00'), '😀')
  assert.equal(liveAnswer('{"answer":"quote\\"'), 'quote"')
})

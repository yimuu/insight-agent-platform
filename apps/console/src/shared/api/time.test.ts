import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { utcTimestamp } from './time.ts'

const contract = JSON.parse(
  await readFile(
    new URL(
      '../../../../../contracts/platform-v1/schemas/nominal/utc-timestamp.schema.json',
      import.meta.url,
    ),
    'utf8',
  ),
)

test('browser deadlines satisfy the owning UtcTimestamp contract without changing the instant', () => {
  for (const input of [
    '2026-09-07T00:00:00.000Z',
    '2026-09-07T00:00:00.001Z',
    '2026-09-07T00:00:00.123Z',
    '2024-02-29T23:59:59.999Z',
  ]) {
    const instant = new Date(input)
    const timestamp = utcTimestamp(instant)
    assert.match(timestamp, new RegExp(contract.pattern))
    assert.equal(timestamp.length, contract.minLength)
    assert.equal(Buffer.byteLength(timestamp), contract['x-platform-max-bytes'])
    assert.equal(Date.parse(timestamp), instant.getTime())
    assert.equal(new RegExp(contract.pattern).test(instant.toISOString()), false)
  }
  assert.throws(() => utcTimestamp(new Date(NaN)), RangeError)
  assert.throws(() => utcTimestamp(new Date('+010000-01-01T00:00:00.000Z')), RangeError)
})

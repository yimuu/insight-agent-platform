import test from 'node:test'
import assert from 'node:assert/strict'
import { readTokenFile, tokenExpiry } from './session.ts'

test('session import is bounded and distinguishes a private token from metadata containing file paths', () => {
  assert.equal(readTokenFile('  opaque-token\n'), 'opaque-token')
  assert.equal(readTokenFile('{"access_token":"opaque-token"}'), 'opaque-token')
  assert.throws(() => readTokenFile('{"token_file":"/private/session-token"}'), /请选择令牌文件/)
  for (const text of ['', 'two tokens', 'x'.repeat(32_769)])
    assert.throws(() => readTokenFile(text))
})
test('expiry hints accept only a finite integer timestamp and never authenticate a token', () => {
  const token = (value: unknown) =>
    'unsigned.' + Buffer.from(JSON.stringify(value)).toString('base64url') + '.unsigned'
  assert.equal(tokenExpiry(token({ exp: 2_000_000_000 })), 2_000_000_000_000)
  for (const value of [{ exp: '2000000000' }, { exp: -1 }, { exp: 1.5 }, {}, null])
    assert.equal(tokenExpiry(token(value)), null)
  assert.equal(tokenExpiry('opaque-token'), null)
})

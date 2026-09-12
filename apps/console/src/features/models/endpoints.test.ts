import test from 'node:test'
import assert from 'node:assert/strict'
import { modelEndpoint, validEndpoint, MODEL_SERVICES } from './endpoints.ts'
test('service URLs normalize to the canonical provider prefix before credentials are imported', () => {
  assert.deepEqual(modelEndpoint(MODEL_SERVICES[0].url), {
    scheme: 'https',
    host: 'dashscope.aliyuncs.com',
    port: 443,
    base_path: '/compatible-mode',
  })
  assert.deepEqual(modelEndpoint('https://api.openai.com/v1'), {
    scheme: 'https',
    host: 'api.openai.com',
    port: 443,
    base_path: '/',
  })
  assert.equal(validEndpoint(modelEndpoint('https://custom.example:8443/inference/v1')), true)
  for (const url of [
    'http://api.example.com',
    'https://localhost',
    'https://127.0.0.1',
    'https://[::1]',
    'https://user:secret@api.example.com',
    'https://api.example.com/?token=secret',
    'https://api.example.com/a/../v1',
    'https://api.example.com/v1/responses',
    'https://api.example.com/a%2fb',
    'https://api.example.com/a//b',
  ])
    assert.throws(() => modelEndpoint(url), url)
})

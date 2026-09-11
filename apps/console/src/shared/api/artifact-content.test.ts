import assert from 'node:assert/strict'
import test from 'node:test'
import { readExactArtifact, bytesDigest } from './artifact-content.ts'
import { PlatformClient, PlatformProblem } from './client.ts'

const id = 'art_01950000-0000-7000-8000-000000000003'
const client = new PlatformClient('https://platform.example', 'token')
test('authorized Artifact download checks current exact metadata, byte digest and empty content', async (context) => {
  for (const text of ['authorized body', '']) {
    const body = new TextEncoder().encode(text)
    const ref = {
      artifact_id: id,
      content_digest: await bytesDigest(body),
      byte_length: body.length,
      media_type: 'text/plain',
      classification: 'internal',
      display_name: null,
    }
    const paths = []
    context.mock.method(globalThis, 'fetch', async (url, init) => {
      paths.push(new URL(url).pathname)
      assert.equal(init.cache, 'no-store')
      return url.endsWith('/content')
        ? new Response(body, {
            headers: {
              'content-length': String(body.length),
              'content-type': ref.media_type,
              etag: `"${ref.content_digest}"`,
            },
          })
        : new Response(JSON.stringify({ artifact_id: id, state: 'ready', content: ref }))
    })
    assert.equal(await (await readExactArtifact(client, ref, { maximumBytes: 65536 })).text(), text)
    assert.deepEqual(paths, [`/v1/artifacts/${id}`, `/v1/artifacts/${id}/content`])
    context.mock.restoreAll()
  }
})

test('Artifact current denial, digest drift and stream overrun do not produce a downloadable Blob', async (context) => {
  const body = new TextEncoder().encode('exact')
  const ref = {
    artifact_id: id,
    content_digest: await bytesDigest(body),
    byte_length: body.length,
    media_type: 'text/plain',
    classification: 'internal',
    display_name: null,
  }
  for (const failure of ['metadata-denied', 'body-denied', 'digest', 'overrun', 'length']) {
    let bodyCalls = 0
    context.mock.method(globalThis, 'fetch', async (url) => {
      if (!url.endsWith('/content'))
        return failure === 'metadata-denied'
          ? new Response(JSON.stringify({ code: 'permission_denied', retryable: false }), {
              status: 403,
            })
          : new Response(JSON.stringify({ artifact_id: id, state: 'ready', content: ref }))
      bodyCalls++
      if (failure === 'body-denied')
        return new Response(JSON.stringify({ code: 'permission_denied', retryable: false }), {
          status: 403,
        })
      return new Response(failure === 'digest' ? 'wrong' : 'too-long', {
        headers: {
          ...(failure === 'length' ? {} : { 'content-length': '5' }),
          'content-type': 'text/plain',
          etag: `"${ref.content_digest}"`,
        },
      })
    })
    await assert.rejects(
      readExactArtifact(client, ref, { maximumBytes: 65536 }),
      failure.endsWith('denied')
        ? (error) => error instanceof PlatformProblem && error.status === 403
        : undefined,
    )
    assert.equal(bodyCalls, failure === 'metadata-denied' ? 0 : 1)
    context.mock.restoreAll()
  }
})

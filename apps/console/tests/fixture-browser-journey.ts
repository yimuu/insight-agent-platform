// Explicit synthetic transport entry. The real Gateway journey never installs this interception.
// Uploaded browser bytes still reach the fixture, which checks hashes and recompiles source
// with the actual Rust WASM. This is browser fixture evidence, not Gateway/Postgres evidence.
import { runGatewayJourney } from './real-gateway-journey.ts'

async function configureSyntheticBrowser({ client, consoleOrigin, gatewayOrigin }) {
  await client.call('Network.enable')
  client.on('Fetch.requestPaused', async ({ requestId, request, networkId }) => {
    let status = 500
    try {
      const url = new URL(request.url)
      if (url.origin !== 'https://objects.example')
        throw new Error('unexpected synthetic object origin')
      if (request.method === 'OPTIONS') status = 204
      else if (request.method === 'PUT') {
        const body =
          request.postData ??
          (await client.call('Network.getRequestPostData', { requestId: networkId })).postData
        if (typeof body !== 'string') throw new Error('missing browser upload bytes')
        const response = await fetch(new URL(`/__fixture/objects${url.pathname}`, gatewayOrigin), {
          method: 'PUT',
          body,
          headers: { 'content-type': 'application/json' },
        })
        await response.arrayBuffer()
        status = response.status
      } else status = 405
    } catch {
      // A failed synthetic upload must fail the UI publication, never look successful.
      status = 500
    }
    await client.call('Fetch.fulfillRequest', {
      requestId,
      responseCode: status,
      responseHeaders: [
        { name: 'access-control-allow-origin', value: consoleOrigin },
        { name: 'access-control-allow-methods', value: 'PUT, OPTIONS' },
        { name: 'access-control-allow-headers', value: 'content-type, content-length' },
        { name: 'content-length', value: '0' },
      ],
    })
  })
  await client.call('Fetch.enable', {
    patterns: [{ urlPattern: 'https://objects.example/*', requestStage: 'Request' }],
  })
}

runGatewayJourney({ configureSyntheticBrowser }).catch((error) => {
  process.stderr.write(
    `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
  )
  process.exit(process.exitCode ?? 1)
})

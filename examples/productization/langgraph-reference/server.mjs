import { appendFileSync, readFileSync } from 'node:fs'
import { createServer } from 'node:https'

import { invokeCapability } from './graph.mjs'

function required(name) {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

function boundedBody(request, maximum = 4096) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    request.on('data', (chunk) => {
      size += chunk.length
      if (size > maximum) {
        reject(new Error('request body exceeds LangGraph reference limit'))
        request.destroy()
        return
      }
      chunks.push(chunk)
    })
    request.on('end', () => resolve(Buffer.concat(chunks)))
    request.on('error', reject)
  })
}

function trace(record) {
  const path = process.env.INSIGHT_REMOTE_FIXTURE_TRACE_PATH
  if (path) {
    appendFileSync(path, `${JSON.stringify({schema_version: 1, ...record})}\n`, {
      encoding: 'utf8',
      mode: 0o600,
    })
  }
}

const packageContract = JSON.parse(
  readFileSync(new URL('./package.json', import.meta.url), 'utf8'),
)

const port = Number.parseInt(required('INSIGHT_REMOTE_FIXTURE_PORT'), 10)
if (!Number.isInteger(port) || port < 1 || port > 65_535) {
  throw new Error('INSIGHT_REMOTE_FIXTURE_PORT is invalid')
}

const server = createServer(
  {
    cert: readFileSync(required('INSIGHT_REMOTE_FIXTURE_CERT_PATH')),
    key: readFileSync(required('INSIGHT_REMOTE_FIXTURE_KEY_PATH')),
    minVersion: 'TLSv1.2',
  },
  async (request, response) => {
    try {
      if (request.method !== 'POST' || request.url !== '/v1/framework') {
        response.writeHead(404, {'content-type': 'application/json', 'cache-control': 'no-store'})
        response.end(JSON.stringify({error: 'not_found'}))
        return
      }
      const body = JSON.parse((await boundedBody(request)).toString('utf8'))
      const databaseEnvironmentPresent = Object.keys(process.env).some(
        (name) =>
          name === 'DATABASE_URL' ||
          name.startsWith('PLATFORM_POSTGRES') ||
          name.startsWith('INSIGHT_DATABASE'),
      )
      trace({
        kind: 'langgraph_capability_request',
        framework: 'langgraph-js',
        framework_version: packageContract.dependencies['@langchain/langgraph'],
        method: request.method,
        path: request.url,
        database_environment_present: databaseEnvironmentPresent,
      })
      if (databaseEnvironmentPresent || typeof body?.message !== 'string') {
        throw new Error('invalid LangGraph reference request')
      }
      const message = await invokeCapability(body.message)
      response.writeHead(200, {
        'content-type': 'application/json',
        'cache-control': 'no-store',
        'content-encoding': 'identity',
      })
      response.end(JSON.stringify({message}))
    } catch {
      trace({kind: 'langgraph_request_rejected', method: request.method, path: request.url})
      if (!response.headersSent) {
        response.writeHead(400, {'content-type': 'application/json', 'cache-control': 'no-store'})
      }
      response.end(JSON.stringify({error: 'invalid_langgraph_request'}))
    }
  },
)

server.listen(port, '127.0.0.1', () => {
  process.stdout.write(
    `${JSON.stringify({
      schema_version: 1,
      status: 'ready',
      framework: 'langgraph-js',
      framework_version: packageContract.dependencies['@langchain/langgraph'],
      port,
    })}\n`,
  )
})

function shutdown() {
  server.close(() => process.exit(0))
}
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)

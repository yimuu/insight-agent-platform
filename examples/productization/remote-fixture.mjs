import { appendFileSync, readFileSync } from 'node:fs'
import { createServer } from 'node:https'

function required(name) {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

function boundedBody(request, maximum = 1_048_576) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    request.on('data', (chunk) => {
      size += chunk.length
      if (size > maximum) {
        reject(new Error('request body exceeds fixture limit'))
        request.destroy()
        return
      }
      chunks.push(chunk)
    })
    request.on('end', () => resolve(Buffer.concat(chunks)))
    request.on('error', reject)
  })
}

function sse(response, event, value) {
  response.write(`event: ${event}\n`)
  response.write(`data: ${JSON.stringify(value)}\n\n`)
}

function trace(record) {
  const path = process.env.INSIGHT_REMOTE_FIXTURE_TRACE_PATH
  if (path) appendFileSync(path, `${JSON.stringify({schema_version: 1, ...record})}\n`, {encoding: 'utf8', mode: 0o600})
}

async function handleModel(request, response) {
  const body = JSON.parse((await boundedBody(request)).toString('utf8'))
  const modelMatches = body.model === 'fixture-model-2026-08'
  const streamEnabled = body.stream === true
  trace({kind: 'model_request', method: request.method, path: request.url, model_matches: modelMatches, stream_enabled: streamEnabled})
  if (!modelMatches || !streamEnabled) {
    response.writeHead(400, {'content-type': 'application/json'})
    response.end(JSON.stringify({error: {type: 'invalid_request_error'}}))
    return
  }
  const serialized = JSON.stringify(body)
  if (serialized.includes('fixture-timeout')) return
  const text = serialized.includes('fixture-output-limit')
    ? 'x'.repeat(32_768)
    : JSON.stringify({answer: 'deterministic streamed model response'})
  response.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-store',
    'content-encoding': 'identity',
  })
  sse(response, 'response.created', {type: 'response.created', response: {}})
  sse(response, 'response.output_text.delta', {
    type: 'response.output_text.delta',
    delta: text,
  })
  sse(response, 'response.completed', {
    type: 'response.completed',
    response: {
      status: 'completed',
      model: 'fixture-model-2026-08',
      system_fingerprint: 'productization-fixture-v1',
      output: [{
        id: 'msg_productization_1',
        type: 'message',
        status: 'completed',
        role: 'assistant',
        content: [{type: 'output_text', text}],
      }],
      usage: {input_tokens: 7, output_tokens: 5, total_tokens: 12},
    },
  })
  response.end()
}

async function handleCapability(request, response) {
  const body = JSON.parse((await boundedBody(request)).toString('utf8'))
  trace({kind: 'capability_request', method: request.method, path: request.url})
  if (JSON.stringify(body).includes('fixture-capability-timeout')) return
  response.writeHead(200, {
    'content-type': 'application/json',
    'cache-control': 'no-store',
    'content-encoding': 'identity',
  })
  response.end(JSON.stringify(body))
}

async function handleFrameworkCapability(request, response) {
  const body = JSON.parse((await boundedBody(request, 4096)).toString('utf8'))
  const databaseEnvironmentPresent = Object.keys(process.env).some((name) =>
    name === 'DATABASE_URL' || name.startsWith('PLATFORM_POSTGRES') || name.startsWith('INSIGHT_DATABASE'),
  )
  trace({
    kind: 'framework_capability_request',
    method: request.method,
    path: request.url,
    database_environment_present: databaseEnvironmentPresent,
  })
  if (databaseEnvironmentPresent || typeof body?.message !== 'string' || body.message.length < 1 || body.message.length > 256) {
    response.writeHead(400, {'content-type': 'application/json', 'cache-control': 'no-store'})
    response.end(JSON.stringify({error: 'invalid_framework_request'}))
    return
  }
  response.writeHead(200, {
    'content-type': 'application/json',
    'cache-control': 'no-store',
    'content-encoding': 'identity',
  })
  response.end(JSON.stringify({message: `framework: ${body.message}`}))
}

async function handleMcp(request, response) {
  const body = JSON.parse((await boundedBody(request)).toString('utf8'))
  const method = body.method
  trace({kind: 'mcp_request', method: request.method, path: request.url, rpc_method: method})
  if (method === 'notifications/initialized') {
    response.writeHead(202, {'cache-control': 'no-store'})
    response.end()
    return
  }
  const id = body.id
  let result
  if (method === 'initialize') {
    result = {
      protocolVersion: '2025-11-25',
      capabilities: {tools: {}, resources: {}},
      serverInfo: {name: 'insight-productization-fixture', version: '1.0.0'},
    }
  } else if (method === 'tools/list') {
    result = {tools: [{
      name: 'fixture_lookup',
      description: 'Return a bounded deterministic productization value.',
      inputSchema: {
        type: 'object',
        properties: {message: {type: 'string'}},
        required: ['message'],
        additionalProperties: false,
      },
    }]}
  } else if (method === 'resources/list') {
    result = {resources: [{
      name: 'productization-resource',
      uri: 'mcp://insight.fixture/productization',
      mimeType: 'text/plain',
      description: 'Bounded MCP resource metadata.',
    }]}
  } else if (method === 'tools/call') {
    if (JSON.stringify(body.params).includes('fixture-mcp-remote-error')) {
      response.writeHead(200, {'content-type': 'application/json', 'cache-control': 'no-store'})
      response.end(JSON.stringify({jsonrpc: '2.0', id, error: {code: -32001, message: 'fixture remote error'}}))
      return
    }
    result = {message: body.params?.arguments?.message}
  } else {
    response.writeHead(200, {'content-type': 'application/json', 'cache-control': 'no-store'})
    response.end(JSON.stringify({jsonrpc: '2.0', id, error: {code: -32601, message: 'method not found'}}))
    return
  }
  const headers = {'content-type': 'application/json', 'cache-control': 'no-store', 'content-encoding': 'identity'}
  if (method === 'initialize') headers['mcp-session-id'] = 'productization-mcp-session-v1'
  response.writeHead(200, headers)
  response.end(JSON.stringify({jsonrpc: '2.0', id, result}))
}

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
      if (request.method === 'POST' && request.url === '/v1/responses') {
        await handleModel(request, response)
        return
      }
      if (request.method === 'POST' && request.url === '/v1/capability') {
        await handleCapability(request, response)
        return
      }
      if (request.method === 'POST' && request.url === '/v1/framework') {
        await handleFrameworkCapability(request, response)
        return
      }
      if (request.method === 'POST' && request.url === '/mcp') {
        await handleMcp(request, response)
        return
      }
      trace({kind: 'route_rejected', method: request.method, path: request.url})
      response.writeHead(404, {'content-type': 'application/json'})
      response.end(JSON.stringify({error: 'not_found'}))
    } catch {
      trace({kind: 'request_rejected', method: request.method, path: request.url})
      if (!response.headersSent) {
        response.writeHead(400, {'content-type': 'application/json'})
      }
      response.end(JSON.stringify({error: 'invalid_request'}))
    }
  },
)

server.listen(port, '127.0.0.1', () => {
  process.stdout.write(`${JSON.stringify({schema_version: 1, status: 'ready', port})}\n`)
})

function shutdown() {
  server.close(() => process.exit(0))
}
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)

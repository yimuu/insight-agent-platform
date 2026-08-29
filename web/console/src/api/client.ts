import { parseEventStream } from './sse.ts'
import type {
  ApiProblemShape,
  ArtifactView,
  AuthorityResponse,
  DeploymentView,
  JsonObject,
  OperationView,
  ResourceView,
  RunEvent,
  RunView,
  TaskView,
} from './types.ts'

const MAX_JSON_BYTES = 2 * 1024 * 1024
const MAX_SSE_PAGE_BYTES = 34 * 1024 * 1024

export class PlatformProblem extends Error {
  readonly status: number
  readonly code: string
  readonly traceId: string | null
  readonly retryable: boolean

  constructor(
    status: number,
    code: string,
    traceId: string | null,
    retryable: boolean,
    message: string,
  ) {
    super(message)
    this.name = 'PlatformProblem'
    this.status = status
    this.code = code
    this.traceId = traceId
    this.retryable = retryable
  }
}

function normalizeOrigin(raw: string): string {
  const parsed = new URL(raw.trim())
  if (!['http:', 'https:'].includes(parsed.protocol) || parsed.username || parsed.password) {
    throw new Error('Endpoint must be an HTTP(S) origin without embedded credentials')
  }
  parsed.pathname = parsed.pathname.replace(/\/v1\/?$/, '').replace(/\/$/, '')
  parsed.search = ''
  parsed.hash = ''
  return parsed.toString().replace(/\/$/, '')
}

async function boundedText(response: Response, maximumBytes = MAX_JSON_BYTES): Promise<string> {
  const declared = Number(response.headers.get('content-length') ?? 0)
  if (declared > maximumBytes) throw new Error(`response_too_large: response exceeded ${maximumBytes} bytes`)
  if (!response.body) return ''

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let size = 0
  let text = ''
  while (true) {
    const next = await reader.read()
    if (next.done) break
    size += next.value.byteLength
    if (size > maximumBytes) {
      await reader.cancel()
      throw new Error(`response_too_large: response exceeded ${maximumBytes} bytes`)
    }
    text += decoder.decode(next.value, { stream: true })
  }
  return text + decoder.decode()
}

async function decodeProblem(response: Response): Promise<PlatformProblem> {
  const traceId = response.headers.get('trace-id')
  let body: ApiProblemShape = {}
  try {
    body = JSON.parse(await boundedText(response)) as ApiProblemShape
  } catch {
    // Do not expose an unbounded or non-contract server body.
  }
  return new PlatformProblem(
    response.status,
    body.code ?? `http_${response.status}`,
    body.trace_id ?? traceId,
    body.retryable === true,
    body.detail ?? body.message ?? body.title ?? `Platform request failed with HTTP ${response.status}`,
  )
}

export class PlatformClient {
  readonly origin: string
  private readonly accessToken: string

  constructor(endpoint: string, accessToken: string) {
    this.origin = normalizeOrigin(endpoint)
    this.accessToken = accessToken
  }

  private async request<T>(path: string, init: RequestInit = {}): Promise<AuthorityResponse<T>> {
    const headers = new Headers(init.headers)
    headers.set('Accept', 'application/json')
    if (this.accessToken) headers.set('Authorization', `Bearer ${this.accessToken}`)
    if (init.body) headers.set('Content-Type', 'application/json')

    const response = await fetch(`${this.origin}/v1${path}`, {
      ...init,
      headers,
      cache: 'no-store',
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
    })
    if (!response.ok) throw await decodeProblem(response)
    const text = await boundedText(response)
    return {
      data: (text ? JSON.parse(text) : null) as T,
      etag: response.headers.get('etag'),
      traceId: response.headers.get('trace-id'),
    }
  }

  async readiness(): Promise<boolean> {
    const response = await fetch(`${this.origin}/readyz`, {
      cache: 'no-store',
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
    })
    return response.ok
  }

  getRun(id: string) { return this.request<RunView>(`/runs/${encodeURIComponent(id)}`) }
  getRunResult(id: string) { return this.request<JsonObject>(`/runs/${encodeURIComponent(id)}/result`) }
  getTask(id: string) { return this.request<TaskView>(`/tasks/${encodeURIComponent(id)}`) }
  getArtifact(id: string) { return this.request<ArtifactView>(`/artifacts/${encodeURIComponent(id)}`) }
  getOperation(id: string) { return this.request<OperationView>(`/operations/${encodeURIComponent(id)}`) }
  getResource(noun: string, id: string) { return this.request<ResourceView>(`/${encodeURIComponent(noun)}/${encodeURIComponent(id)}`) }
  getDeployment(noun: string, resourceId: string, deploymentId: string) {
    return this.request<DeploymentView>(`/${encodeURIComponent(noun)}/${encodeURIComponent(resourceId)}/deployments/${encodeURIComponent(deploymentId)}`)
  }

  async getRunEvents(id: string, cursor?: string): Promise<RunEvent[]> {
    const headers = new Headers({ Accept: 'text/event-stream' })
    if (this.accessToken) headers.set('Authorization', `Bearer ${this.accessToken}`)
    if (cursor) headers.set('Last-Event-ID', cursor)
    const response = await fetch(`${this.origin}/v1/runs/${encodeURIComponent(id)}/events`, {
      headers,
      cache: 'no-store',
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
    })
    if (!response.ok) throw await decodeProblem(response)
    return parseEventStream(await boundedText(response, MAX_SSE_PAGE_BYTES))
  }

  taskAction(id: string, action: 'submit-input' | 'approve' | 'reject' | 'cancel', etag: string, receipt: string, body?: JsonObject) {
    return this.request<TaskView>(`/tasks/${encodeURIComponent(id)}:${action}`, {
      method: 'POST',
      headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      body: body ? JSON.stringify(body) : undefined,
    })
  }

  runAction(id: string, action: 'pause' | 'resume' | 'cancel', etag: string, receipt: string) {
    return this.request<RunView>(`/runs/${encodeURIComponent(id)}:${action}`, {
      method: 'POST',
      headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
    })
  }

  async downloadArtifact(id: string): Promise<{ blob: Blob; mediaType: string; etag: string | null }> {
    const headers = new Headers()
    if (this.accessToken) headers.set('Authorization', `Bearer ${this.accessToken}`)
    const response = await fetch(`${this.origin}/v1/artifacts/${encodeURIComponent(id)}/content`, {
      headers,
      cache: 'no-store',
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
    })
    if (!response.ok) throw await decodeProblem(response)
    const declared = Number(response.headers.get('content-length') ?? 0)
    if (!Number.isSafeInteger(declared) || declared < 1 || declared > 1_073_741_824) {
      throw new Error('invalid_content_length: Artifact download did not provide a valid bounded length')
    }
    const blob = await response.blob()
    if (blob.size !== declared) throw new Error('content_length_mismatch: Artifact download was incomplete')
    return { blob, mediaType: response.headers.get('content-type') ?? 'application/octet-stream', etag: response.headers.get('etag') }
  }
}

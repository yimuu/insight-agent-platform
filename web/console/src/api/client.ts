import { parseEventStream } from './sse.ts'
import type {
  ApiProblemShape,
  AgentAuthoringProfile,
  AgentSummary,
  ArtifactView,
  AuthorityResponse,
  DeploymentView,
  JsonObject,
  ListPage,
  OperationView,
  PrepareArtifactUploadResponse,
  PublishResourceResponse,
  ResourceView,
  RunEvent,
  RunSummary,
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

  getAgentAuthoringProfile() { return this.request<AgentAuthoringProfile>('/agent-authoring-profile') }
  listAgents(cursor?: string) {
    const query = new URLSearchParams({ page_size: '25' })
    if (cursor) query.set('cursor', cursor)
    return this.request<ListPage<AgentSummary>>(`/agents?${query}`)
  }
  listRuns(filters: { agentId?: string; state?: string; cursor?: string } = {}) {
    const query = new URLSearchParams({ page_size: '25' })
    if (filters.agentId) query.set('agent_id', filters.agentId)
    if (filters.state) query.set('state', filters.state)
    if (filters.cursor) query.set('cursor', filters.cursor)
    return this.request<ListPage<RunSummary>>(`/runs?${query}`)
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

  createAgent(body: JsonObject, receipt: string) {
    return this.request<ResourceView>('/agents', {
      method: 'POST', headers: { 'Idempotency-Key': receipt }, body: JSON.stringify(body),
    })
  }

  updateAgent(id: string, body: JsonObject, etag: string, receipt: string) {
    return this.request<ResourceView>(`/agents/${encodeURIComponent(id)}/draft`, {
      method: 'PUT', headers: { 'If-Match': etag, 'Idempotency-Key': receipt }, body: JSON.stringify(body),
    })
  }

  validateAgent(id: string, etag: string, receipt: string) {
    return this.request<OperationView>(`/agents/${encodeURIComponent(id)}/draft:validate`, {
      method: 'POST', headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
    })
  }

  publishAgent(id: string, body: JsonObject, etag: string, receipt: string) {
    return this.request<PublishResourceResponse>(`/agents/${encodeURIComponent(id)}/draft:publish`, {
      method: 'POST', headers: { 'If-Match': etag, 'Idempotency-Key': receipt }, body: JSON.stringify(body),
    })
  }

  createAgentDeployment(id: string, body: JsonObject, etag: string, receipt: string) {
    return this.request<DeploymentView>(`/agents/${encodeURIComponent(id)}/deployments`, {
      method: 'POST', headers: { 'If-Match': etag, 'Idempotency-Key': receipt }, body: JSON.stringify(body),
    })
  }

  activateAgentDeployment(id: string, deploymentId: string, etag: string, receipt: string) {
    return this.request<ResourceView>(`/agents/${encodeURIComponent(id)}/deployments/${encodeURIComponent(deploymentId)}:activate`, {
      method: 'POST', headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
    })
  }

  createRun(body: JsonObject, receipt: string) {
    return this.request<RunView>('/runs', {
      method: 'POST', headers: { 'Idempotency-Key': receipt }, body: JSON.stringify(body),
    })
  }

  prepareArtifactUpload(body: JsonObject, receipt: string) {
    return this.request<PrepareArtifactUploadResponse>('/artifacts:prepare-upload', {
      method: 'POST', headers: { 'Idempotency-Key': receipt }, body: JSON.stringify(body),
    })
  }

  completeArtifactUpload(id: string, body: JsonObject, etag: string, receipt: string) {
    return this.request<JsonObject>(`/artifacts/${encodeURIComponent(id)}:complete-upload`, {
      method: 'POST', headers: { 'If-Match': etag, 'Idempotency-Key': receipt }, body: JSON.stringify(body),
    })
  }

  async putArtifactObject(target: string, bytes: Uint8Array, mediaType: string): Promise<void> {
    const url = new URL(target)
    if (url.protocol !== 'https:' || url.username || url.password || url.hash) {
      throw new Error('invalid_upload_target: Artifact upload authority returned an unsafe target')
    }
    const response = await fetch(url, {
      method: 'PUT',
      headers: { 'Content-Type': mediaType, 'Content-Length': String(bytes.byteLength) },
      body: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer,
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
    })
    if (!response.ok) throw new Error('artifact_upload_failed: Signed object upload was not accepted')
  }

  async waitOperation(id: string, timeoutMilliseconds = 120_000): Promise<AuthorityResponse<OperationView>> {
    const deadline = Date.now() + timeoutMilliseconds
    while (Date.now() < deadline) {
      const operation = await this.getOperation(id)
      if (['succeeded', 'failed', 'cancelled', 'timed_out'].includes(operation.data.state)) return operation
      await new Promise((resolve) => window.setTimeout(resolve, 250))
    }
    throw new Error('operation_pending: Background work is still running; resume from server authority')
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

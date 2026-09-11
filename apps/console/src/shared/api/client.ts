import { parseBindingResolution, parseDependencyPage } from './authoring-query.ts'
import type {
  CompiledModelConfiguration,
  ExactModelCredential,
  ModelConfigurationCatalog,
  ModelConfigurationInput,
  ModelDeclaration,
  ModelDefault,
  ModelResourceKind,
  ModelResourceNoun,
  ModelResourcePage,
  ModelCredentialMetadata,
  ModelConnectionObservation,
  ModelQuotaLimits,
  ModelQuotaView,
} from './model-types.ts'
import type { BindingSelections, DependencyFilters } from './authoring-query.ts'
import { followRunEventPages, readRunEventStream, RunEventTransportError } from './sse.ts'
import type { RunEventFollowOptions, RunEventPageOptions } from './sse.ts'
import type {
  ApiProblemShape,
  AgentAuthoringProfile,
  AgentSummary,
  ArtifactView,
  AuthorityResponse,
  DeploymentView,
  ExactDeploymentRef,
  JsonObject,
  ListPage,
  OperationView,
  PrepareArtifactUploadResponse,
  PublishResourceResponse,
  ResourceView,
  ResourceVersionView,
  RunEvent,
  RunSummary,
  RunView,
  RunDefinition,
  RunValueMetadata,
  TaskForm,
  TaskView,
} from './types.ts'

const MAX_JSON_BYTES = 2 * 1024 * 1024

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
  if (declared > maximumBytes)
    throw new Error(`response_too_large: response exceeded ${maximumBytes} bytes`)
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
    body.detail ??
      body.message ??
      body.title ??
      `Platform request failed with HTTP ${response.status}`,
  )
}

export class PlatformClient {
  readonly origin: string
  private accessToken: string
  private readonly sessionAbort = new AbortController()
  private authenticationRequired?: () => void

  onAuthenticationRequired(listener?: () => void) {
    this.authenticationRequired = listener
  }

  dispose() {
    this.sessionAbort.abort()
    this.accessToken = ''
  }
  private signal(signal?: AbortSignal | null) {
    return signal ? AbortSignal.any([this.sessionAbort.signal, signal]) : this.sessionAbort.signal
  }

  constructor(endpoint: string, accessToken: string) {
    this.origin = normalizeOrigin(endpoint)
    this.accessToken = accessToken
  }

  private async request<T>(
    path: string,
    init: RequestInit = {},
    decode?: (text: string) => T,
    expectedStatus?: number,
  ): Promise<AuthorityResponse<T>> {
    this.sessionAbort.signal.throwIfAborted()
    const headers = new Headers(init.headers)
    headers.set('Accept', 'application/json')
    if (this.accessToken) headers.set('Authorization', `Bearer ${this.accessToken}`)
    if (init.body) headers.set('Content-Type', 'application/json')

    const response = await fetch(`${this.origin}/v1${path}`, {
      ...init,
      signal: this.signal(init.signal),
      headers,
      cache: 'no-store',
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
    })
    if (!response.ok) {
      if (response.status === 401) this.authenticationRequired?.()
      throw await decodeProblem(response)
    }
    if (expectedStatus !== undefined && response.status !== expectedStatus) {
      await response.body?.cancel()
      throw new Error(
        'unexpected_response_status: Public operation returned an unsupported success status',
      )
    }
    const text = await boundedText(response)
    this.sessionAbort.signal.throwIfAborted()
    return {
      data: decode ? decode(text) : ((text ? JSON.parse(text) : null) as T),
      etag: response.headers.get('etag'),
      traceId: response.headers.get('trace-id'),
    }
  }

  async readiness(): Promise<boolean> {
    const response = await fetch(`${this.origin}/readyz`, {
      signal: this.signal(),
      cache: 'no-store',
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
    })
    return response.ok
  }

  getAgentAuthoringProfile(options: { signal?: AbortSignal } = {}) {
    return this.request<AgentAuthoringProfile>('/agent-authoring-profile', {
      signal: options.signal,
    })
  }
  listAuthoringDependencies(filters: DependencyFilters, options: { signal?: AbortSignal } = {}) {
    const query = new URLSearchParams({ kind: filters.kind, page_size: '25' })
    if (filters.environment) query.set('environment', filters.environment)
    if (filters.interfaceContractDigest)
      query.set('interface_contract_digest', filters.interfaceContractDigest)
    if (filters.cursor) query.set('cursor', filters.cursor)
    return this.request(`/agent-authoring-dependencies?${query}`, options, (text) =>
      parseDependencyPage(text, filters),
    )
  }
  resolveAgentBindings(request: BindingSelections, options: { signal?: AbortSignal } = {}) {
    return this.request(
      '/agent-authoring-bindings:resolve',
      { ...options, method: 'POST', body: JSON.stringify(request) },
      (text) => parseBindingResolution(text, request),
    )
  }
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

  getRun(id: string, options: { signal?: AbortSignal } = {}) {
    return this.request<RunView>(`/runs/${encodeURIComponent(id)}`, { signal: options.signal })
  }
  getRunDefinition(id: string, options: { signal?: AbortSignal } = {}) {
    return this.request<RunDefinition>(`/runs/${encodeURIComponent(id)}/definition`, options)
  }
  getRunResult(id: string, options: { signal?: AbortSignal } = {}) {
    return this.request<JsonObject>(`/runs/${encodeURIComponent(id)}/result`, {
      signal: options.signal,
    })
  }
  listRunValues(
    id: string,
    filters: { nodeId?: string; cursor?: string } = {},
    options: { signal?: AbortSignal } = {},
  ) {
    const query = new URLSearchParams({ page_size: '25' })
    if (filters.nodeId) query.set('node_id', filters.nodeId)
    if (filters.cursor) query.set('cursor', filters.cursor)
    return this.request<ListPage<RunValueMetadata>>(
      `/runs/${encodeURIComponent(id)}/values?${query}`,
      options,
    )
  }
  getRunValueContent(runId: string, valueId: string, options: { signal?: AbortSignal } = {}) {
    return this.request<JsonObject>(
      `/runs/${encodeURIComponent(runId)}/values/${encodeURIComponent(valueId)}/content`,
      options,
    )
  }
  listTasks(
    filters: {
      purpose?: 'respondable' | 'viewable'
      state?: string
      kind?: string
      runId?: string
      cursor?: string
    } = {},
    options: { signal?: AbortSignal } = {},
  ) {
    const query = new URLSearchParams({ page_size: '25' })
    if (filters.purpose) query.set('purpose', filters.purpose)
    if (filters.state) query.set('state', filters.state)
    if (filters.kind) query.set('kind', filters.kind)
    if (filters.runId) query.set('run_id', filters.runId)
    if (filters.cursor) query.set('cursor', filters.cursor)
    return this.request<ListPage<TaskView>>(`/tasks?${query}`, options)
  }
  getTask(
    id: string,
    options: { signal?: AbortSignal; purpose?: 'respondable' | 'viewable' } = {},
  ) {
    const query = new URLSearchParams({ purpose: options.purpose ?? 'respondable' })
    return this.request<TaskView>(`/tasks/${encodeURIComponent(id)}?${query}`, {
      signal: options.signal,
    })
  }
  getTaskForm(id: string, options: { signal?: AbortSignal } = {}) {
    return this.request<TaskForm>(`/tasks/${encodeURIComponent(id)}/form`, options)
  }
  getArtifact(id: string, options: { signal?: AbortSignal } = {}) {
    return this.request<ArtifactView>(`/artifacts/${encodeURIComponent(id)}`, options)
  }
  getResourceVersion(
    noun: string,
    id: string,
    versionId: string,
    options: { signal?: AbortSignal } = {},
  ) {
    return this.request<ResourceVersionView>(
      `/${encodeURIComponent(noun)}/${encodeURIComponent(id)}/versions/${encodeURIComponent(versionId)}`,
      options,
    )
  }
  getOperation(id: string) {
    return this.request<OperationView>(`/operations/${encodeURIComponent(id)}`)
  }
  getResource(noun: string, id: string, options: { signal?: AbortSignal } = {}) {
    return this.request<ResourceView>(
      `/${encodeURIComponent(noun)}/${encodeURIComponent(id)}`,
      options,
    )
  }
  getDeployment(
    noun: string,
    resourceId: string,
    deploymentId: string,
    options: { signal?: AbortSignal } = {},
  ) {
    return this.request<DeploymentView>(
      `/${encodeURIComponent(noun)}/${encodeURIComponent(resourceId)}/deployments/${encodeURIComponent(deploymentId)}`,
      options,
    )
  }

  createAgent(body: JsonObject, receipt: string) {
    return this.request<ResourceView>('/agents', {
      method: 'POST',
      headers: { 'Idempotency-Key': receipt },
      body: JSON.stringify(body),
    })
  }

  getModelConfiguration(options: { signal?: AbortSignal } = {}) {
    return this.request<ModelConfigurationCatalog>('/model-configuration', options)
  }
  listModelResources(
    kind: ModelResourceKind,
    after?: string,
    options: { signal?: AbortSignal } = {},
  ) {
    const query = new URLSearchParams({ kind })
    if (after) query.set('after', after)
    return this.request<ModelResourcePage>(`/model-configuration/resources?${query}`, options)
  }
  getModelQuota(deploymentId: string) {
    return this.request<ModelQuotaView>(`/model-quotas/${encodeURIComponent(deploymentId)}`)
  }
  setModelQuota(
    model: ExactDeploymentRef,
    limits: ModelQuotaLimits,
    etag: string,
    receipt: string,
  ) {
    return this.request<ModelQuotaView>(
      `/model-quotas/${encodeURIComponent(model.deployment_id)}`,
      {
        method: 'PUT',
        body: JSON.stringify({ schema_version: 1, model_deployment: model, limits }),
        headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      },
    )
  }
  getModelDefault(options: { signal?: AbortSignal } = {}) {
    return this.request<ModelDefault>('/model-default', options)
  }
  probeModel(installationDigest: string, model: import('./types.ts').ExactDeploymentRef) {
    return this.request<ModelConnectionObservation>('/model-configuration:probe', {
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        installation_digest: installationDigest,
        model_deployment: model,
      }),
    })
  }
  getModelCredential(id: string) {
    return this.request<ModelCredentialMetadata>(`/model-credentials/${encodeURIComponent(id)}`)
  }
  revokeModelCredential(id: string, generation: number, etag: string, receipt: string) {
    return this.request<ModelCredentialMetadata>(
      `/model-credentials/${encodeURIComponent(id)}:revoke`,
      {
        method: 'POST',
        body: JSON.stringify({ schema_version: 1, expected_generation: generation }),
        headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      },
    )
  }
  setModelDefault(body: JsonObject, etag: string, receipt: string) {
    return this.request<ModelDefault>('/model-default', {
      method: 'PUT',
      body: JSON.stringify(body),
      headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
    })
  }
  importModelCredential(operationId: string, providerId: string, apiKey: string) {
    return this.request<{ schema_version: 1; binding: ExactModelCredential }>(
      '/model-credentials',
      {
        method: 'POST',
        body: JSON.stringify({
          schema_version: 1,
          operation_id: operationId,
          provider_id: providerId,
          api_key: apiKey,
        }),
      },
      undefined,
      200,
    )
  }
  declareModelConfiguration(input: ModelConfigurationInput, installationDigest: string) {
    return this.request<ModelDeclaration>('/model-configuration:declare', {
      method: 'POST',
      body: JSON.stringify({ schema_version: 1, installation_digest: installationDigest, input }),
    })
  }
  compileModelConfiguration(
    input: ModelConfigurationInput,
    installationDigest: string,
    artifact: import('./types.ts').ArtifactRef,
  ) {
    return this.request<CompiledModelConfiguration>('/model-configuration:compile', {
      method: 'POST',
      body: JSON.stringify({
        schema_version: 1,
        installation_digest: installationDigest,
        input,
        artifact,
      }),
    })
  }
  createModelResource(noun: ModelResourceNoun, body: JsonObject, receipt: string) {
    return this.request<ResourceView>(
      `/${noun}`,
      { method: 'POST', body: JSON.stringify(body), headers: { 'Idempotency-Key': receipt } },
      undefined,
      201,
    )
  }
  updateModelResource(
    noun: ModelResourceNoun,
    id: string,
    body: JsonObject,
    etag: string,
    receipt: string,
  ) {
    return this.request<ResourceView>(
      `/${noun}/${encodeURIComponent(id)}/draft`,
      {
        method: 'PUT',
        body: JSON.stringify(body),
        headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      },
      undefined,
      200,
    )
  }
  validateModelResource(noun: ModelResourceNoun, id: string, etag: string, receipt: string) {
    return this.request<OperationView>(
      `/${noun}/${encodeURIComponent(id)}/draft:validate`,
      { method: 'POST', headers: { 'If-Match': etag, 'Idempotency-Key': receipt } },
      undefined,
      202,
    )
  }
  publishModelResource(
    noun: ModelResourceNoun,
    id: string,
    body: JsonObject,
    etag: string,
    receipt: string,
  ) {
    return this.request<PublishResourceResponse>(
      `/${noun}/${encodeURIComponent(id)}/draft:publish`,
      {
        method: 'POST',
        body: JSON.stringify(body),
        headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      },
      undefined,
      201,
    )
  }
  createModelDeployment(
    noun: ModelResourceNoun,
    id: string,
    body: JsonObject,
    etag: string,
    receipt: string,
  ) {
    return this.request<DeploymentView>(
      `/${noun}/${encodeURIComponent(id)}/deployments`,
      {
        method: 'POST',
        body: JSON.stringify(body),
        headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      },
      undefined,
      201,
    )
  }
  activateModelDeployment(
    noun: ModelResourceNoun,
    id: string,
    deploymentId: string,
    etag: string,
    receipt: string,
  ) {
    return this.request<ResourceView>(
      `/${noun}/${encodeURIComponent(id)}/deployments/${encodeURIComponent(deploymentId)}:activate`,
      { method: 'POST', headers: { 'If-Match': etag, 'Idempotency-Key': receipt } },
      undefined,
      200,
    )
  }

  updateAgent(id: string, body: JsonObject, etag: string, receipt: string) {
    return this.request<ResourceView>(`/agents/${encodeURIComponent(id)}/draft`, {
      method: 'PUT',
      headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      body: JSON.stringify(body),
    })
  }

  validateAgent(id: string, etag: string, receipt: string) {
    return this.request<OperationView>(`/agents/${encodeURIComponent(id)}/draft:validate`, {
      method: 'POST',
      headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
    })
  }

  publishAgent(id: string, body: JsonObject, etag: string, receipt: string) {
    return this.request<PublishResourceResponse>(
      `/agents/${encodeURIComponent(id)}/draft:publish`,
      {
        method: 'POST',
        headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
        body: JSON.stringify(body),
      },
    )
  }

  createAgentDeployment(id: string, body: JsonObject, etag: string, receipt: string) {
    return this.request<DeploymentView>(`/agents/${encodeURIComponent(id)}/deployments`, {
      method: 'POST',
      headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      body: JSON.stringify(body),
    })
  }

  activateAgentDeployment(id: string, deploymentId: string, etag: string, receipt: string) {
    return this.request<ResourceView>(
      `/agents/${encodeURIComponent(id)}/deployments/${encodeURIComponent(deploymentId)}:activate`,
      {
        method: 'POST',
        headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      },
    )
  }

  createRun(body: JsonObject, receipt: string) {
    return this.request<RunView>('/runs', {
      method: 'POST',
      headers: { 'Idempotency-Key': receipt },
      body: JSON.stringify(body),
    })
  }

  prepareArtifactUpload(body: JsonObject, receipt: string) {
    return this.request<PrepareArtifactUploadResponse>('/artifacts:prepare-upload', {
      method: 'POST',
      headers: { 'Idempotency-Key': receipt },
      body: JSON.stringify(body),
    })
  }

  completeArtifactUpload(id: string, body: JsonObject, etag: string, receipt: string) {
    return this.request<JsonObject>(`/artifacts/${encodeURIComponent(id)}:complete-upload`, {
      method: 'POST',
      headers: { 'If-Match': etag, 'Idempotency-Key': receipt },
      body: JSON.stringify(body),
    })
  }

  async putArtifactObject(target: string, bytes: Uint8Array, mediaType: string): Promise<void> {
    const url = new URL(target)
    if (url.protocol !== 'https:' || url.username || url.password || url.hash) {
      throw new Error('invalid_upload_target: Artifact upload authority returned an unsafe target')
    }
    const response = await fetch(url, {
      method: 'PUT',
      signal: this.signal(),
      headers: { 'Content-Type': mediaType, 'Content-Length': String(bytes.byteLength) },
      body: bytes.buffer.slice(
        bytes.byteOffset,
        bytes.byteOffset + bytes.byteLength,
      ) as ArrayBuffer,
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
    })
    if (!response.ok)
      throw new Error('artifact_upload_failed: Signed object upload was not accepted')
  }

  async waitOperation(
    id: string,
    timeoutMilliseconds = 120_000,
  ): Promise<AuthorityResponse<OperationView>> {
    const deadline = Date.now() + timeoutMilliseconds
    while (Date.now() < deadline) {
      const operation = await this.getOperation(id)
      if (['succeeded', 'failed', 'cancelled', 'timed_out'].includes(operation.data.state))
        return operation
      await new Promise((resolve) => window.setTimeout(resolve, 250))
    }
    throw new Error(
      'operation_pending: Background work is still running; resume from server authority',
    )
  }

  async getRunEvents(
    id: string,
    cursor?: string,
    options: RunEventPageOptions = {},
  ): Promise<RunEvent[]> {
    const headers = new Headers({ Accept: 'text/event-stream' })
    if (this.accessToken) headers.set('Authorization', `Bearer ${this.accessToken}`)
    if (cursor) headers.set('Last-Event-ID', cursor)
    let response: Response
    try {
      response = await fetch(`${this.origin}/v1/runs/${encodeURIComponent(id)}/events`, {
        headers,
        signal: this.signal(options.signal),
        cache: 'no-store',
        credentials: 'omit',
        redirect: 'error',
        referrerPolicy: 'no-referrer',
      })
    } catch {
      this.signal(options.signal).throwIfAborted()
      throw new RunEventTransportError()
    }
    if (!response.ok) {
      if (response.status === 401) this.authenticationRequired?.()
      throw await decodeProblem(response)
    }
    return readRunEventStream(response, options)
  }

  followRunEvents(id: string, options: RunEventFollowOptions): Promise<void> {
    return followRunEventPages(
      (cursor, pageOptions) => this.getRunEvents(id, cursor, pageOptions),
      { ...options, signal: this.signal(options.signal) },
    )
  }

  taskAction(
    id: string,
    action: 'submit-input' | 'approve' | 'reject' | 'cancel',
    etag: string,
    receipt: string,
    body?: JsonObject,
    options: { signal?: AbortSignal } = {},
  ) {
    return this.request<TaskView>(`/tasks/${encodeURIComponent(id)}:${action}`, {
      ...options,
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

  signalRun(
    id: string,
    key: string,
    body: JsonObject,
    receipt: string,
    options: { signal?: AbortSignal } = {},
  ) {
    return this.request<null>(
      `/runs/${encodeURIComponent(id)}/signals/${encodeURIComponent(key)}`,
      {
        ...options,
        method: 'POST',
        headers: { 'Idempotency-Key': receipt },
        body: JSON.stringify(body),
      },
      undefined,
      204,
    )
  }

  async downloadArtifact(
    id: string,
    options: { signal?: AbortSignal; maximumBytes?: number } = {},
  ): Promise<{ blob: Blob; mediaType: string; etag: string | null }> {
    const headers = new Headers()
    if (this.accessToken) headers.set('Authorization', `Bearer ${this.accessToken}`)
    const response = await fetch(`${this.origin}/v1/artifacts/${encodeURIComponent(id)}/content`, {
      headers,
      signal: this.signal(options.signal),
      cache: 'no-store',
      credentials: 'omit',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
    })
    if (!response.ok) {
      if (response.status === 401) this.authenticationRequired?.()
      throw await decodeProblem(response)
    }
    const lengthHeader = response.headers.get('content-length')
    const declared = Number(lengthHeader)
    const maximum = Math.min(options.maximumBytes ?? 1_073_741_824, 1_073_741_824)
    if (
      lengthHeader === null ||
      !/^\d+$/.test(lengthHeader) ||
      !Number.isSafeInteger(declared) ||
      declared < 0 ||
      declared > maximum
    ) {
      await response.body?.cancel()
      throw new Error(
        'invalid_content_length: Artifact download did not provide a valid bounded length',
      )
    }
    if (!response.body) throw new Error('artifact_content_missing')
    const reader = response.body.getReader()
    const chunks: Uint8Array<ArrayBuffer>[] = []
    let received = 0
    try {
      while (true) {
        const part = await reader.read()
        if (part.done) break
        received += part.value.byteLength
        if (received > declared || received > maximum)
          throw new Error('content_length_mismatch: Artifact download exceeded its exact bound')
        chunks.push(new Uint8Array(part.value))
      }
    } catch (error) {
      await reader.cancel()
      throw error
    } finally {
      reader.releaseLock()
    }
    const blob = new Blob(chunks, {
      type: response.headers.get('content-type') ?? 'application/octet-stream',
    })
    if (blob.size !== declared)
      throw new Error('content_length_mismatch: Artifact download was incomplete')
    return {
      blob,
      mediaType: response.headers.get('content-type') ?? 'application/octet-stream',
      etag: response.headers.get('etag'),
    }
  }
}

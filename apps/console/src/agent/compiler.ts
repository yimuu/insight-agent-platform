// Thin adapter for the shared Rust compiler. No parsing, lowering or compiler defaults live here.
export type AgentExecutionKind = 'deterministic' | 'model_chat' | 'full_plan' | 'framework_graph'
export type RequiredAgentFeature = 'model' | 'context' | 'remote-capability' | 'mcp' | 'sandbox'
type Json = null | boolean | number | string | Json[] | { [key: string]: Json }

export interface ExactVersionRef {
  revision_id: string
  resource_kind: 'policy_revision'
  semantic_digest: string
}

export interface ExactDeploymentRef {
  deployment_id: string
  resource_kind: 'model_deployment' | 'policy_deployment'
  deployment_digest: string
}

export interface ExactPolicyBinding {
  deployment: ExactDeploymentRef & { resource_kind: 'policy_deployment' }
  revision: ExactVersionRef
}

export interface AgentCompilerProfile {
  default_deadline_seconds: number
  default_environment: string
  policy_versions: ExactVersionRef[]
  deployment_policies: ExactPolicyBinding[]
  execution_profile: ExactPolicyBinding
  model_loop: {
    maximum_rounds: number
    maximum_capability_calls: number
    maximum_parallel_calls_per_round: number
    token_budget: number
  }
}

export interface AgentAuthoringProfileEnvelope extends AgentCompilerProfile {
  schema_version: 1
  models: unknown[]
  profile_digest: string
}

export interface ResolvedAgentBindings {
  deployment_features?: import('./authoring-query.ts').DeploymentFeatures[]
  slots?: Json[]
  model: null | {
    manifest_ref: string
    deployment: ExactDeploymentRef & { resource_kind: 'model_deployment' }
    selection_policy: ExactPolicyBinding
  }
}

export interface AgentCompilerInput {
  manifest: string
  manifestPath?: string
  plan?: string
  inputSchema: string
  outputSchema: string
  profile: AgentCompilerProfile
  bindings: ResolvedAgentBindings
}

export interface CompiledAgent {
  name: string
  executionKind: AgentExecutionKind
  sourceBundle: string
  sourceBundleDigest: string
  sourceMap: string
  sourceMapDigest: string
  canonicalManifest: string
  manifestDigest: string
  contractDigest: string
  resourceIntent: Json
  typedPlan: string
  typedPlanDigest: string
  deploymentIntent: Json
  requiredFeatures: RequiredAgentFeature[]
  lifecyclePlan: Json
}


export class AgentCompilerError extends Error {
  readonly code: string
  constructor(code: string, detail: string) {
    super(detail)
    this.name = 'AgentCompilerError'
    this.code = code
  }
}

function executionKind(value: unknown): AgentExecutionKind {
  if (value === 'deterministic' || value === 'model_chat' || value === 'full_plan' || value === 'framework_graph') return value
  throw new AgentCompilerError('compiler_internal', 'The shared compiler returned an unsupported execution kind.')
}

type Operation = 'inspect_sources' | 'inspect' | 'compile' | 'compile_frozen' | 'digest' | 'build_expression'
const encoder = new TextEncoder()
const decoder = new TextDecoder('utf-8', { fatal: true })

async function wasm(operation: Operation, input: unknown, signal?: AbortSignal): Promise<unknown> {
  signal?.throwIfAborted()
  const bytes = encoder.encode(JSON.stringify(input))
  return new Promise((resolve, reject) => {
    let worker: Worker
    try { worker = new Worker(new URL('./compiler.worker.ts', import.meta.url), { type: 'module' }) }
    catch { reject(new AgentCompilerError('compiler_unavailable', 'The shared compiler could not be initialized.')); return }
    const timer = setTimeout(() => {
      done()
      reject(new AgentCompilerError('compiler_limit_exceeded', 'Compilation exceeded its time budget.'))
    }, 30_000)
    const onAbort = () => { done(); reject(new DOMException('Compilation cancelled', 'AbortError')) }
    const done = () => { clearTimeout(timer); signal?.removeEventListener('abort', onAbort); worker.terminate() }
    signal?.addEventListener('abort', onAbort, { once: true })
    worker.addEventListener('message', (event: MessageEvent<{ bytes?: Uint8Array; failed?: boolean }>) => {
      done()
      if (event.data.failed || !event.data.bytes) {
        reject(new AgentCompilerError('compiler_unavailable', 'The shared compiler could not complete the request.'))
        return
      }
      try { resolve(JSON.parse(decoder.decode(event.data.bytes))) }
      catch { reject(new AgentCompilerError('compiler_internal', 'The shared compiler returned an invalid response.')) }
    }, { once: true })
    worker.addEventListener('error', () => { done(); reject(new AgentCompilerError('compiler_unavailable', 'The shared compiler failed.')) }, { once: true })
    worker.postMessage({ operation, bytes }, [bytes.buffer])
  })
}

function accepted(value: unknown): Record<string, unknown> {
  const response = value as { outcome?: string; diagnostics?: { code: string; safe_detail: string }[] }
  if (response.outcome === 'rejected') {
    const diagnostic = response.diagnostics?.[0]
    throw new AgentCompilerError(diagnostic?.code ?? 'compiler_internal', diagnostic?.safe_detail ?? 'Compilation failed.')
  }
  if (response.outcome !== 'compiled' && response.outcome !== 'inspected') {
    throw new AgentCompilerError('compiler_internal', 'The shared compiler returned an unsupported response.')
  }
  return response as Record<string, unknown>
}

export async function inspectAgentManifest(source: string): Promise<{
  executionKind: AgentExecutionKind; modelRef: string | null; inputSchemaPath: string; outputSchemaPath: string; planPath: string | null
}> {
  const response = accepted(await wasm('inspect', { schema_version: 1, manifest: source }))
  const resolution = response.resolution as { execution_kind: AgentExecutionKind; model_ref: string | null; input_schema_path: string; output_schema_path: string; plan_path: string | null }
  return { executionKind: executionKind(resolution.execution_kind), modelRef: resolution.model_ref,
    inputSchemaPath: resolution.input_schema_path, outputSchemaPath: resolution.output_schema_path, planPath: resolution.plan_path }
}

export async function digestJson(value: Json): Promise<string> {
  const result = await wasm('digest', value) as { Ok?: string; Err?: string }
  if (!result.Ok) throw new AgentCompilerError(result.Err ?? 'compiler_internal', 'Canonical digest could not be computed.')
  return result.Ok
}

/** The owning Rust builder computes stack depth and semantic identity. */
export async function rebuildExpression(expression: { [key: string]: Json }, signal?: AbortSignal): Promise<{ [key: string]: Json }> {
  const response = await wasm('build_expression', { schema_version: 1, input_ports: expression.input_ports, instructions: expression.instructions, output_schema_digest: expression.output_schema_digest }, signal) as { outcome?: string; schema_version?: number; expression?: { [key: string]: Json }; diagnostics?: { code: string; safe_detail: string }[] }
  if (response.outcome === 'rejected') { const diagnostic = response.diagnostics?.[0]; throw new AgentCompilerError(diagnostic?.code ?? 'agent_compile_failed', diagnostic?.safe_detail ?? 'Rust could not build this expression.') }
  if (response.outcome !== 'built' || response.schema_version !== 1 || !response.expression) throw new AgentCompilerError('compiler_internal', 'The shared expression builder returned an invalid response.')
  // Unknown authored fields are retained for the complete compiler diagnostic.
  return { ...expression, ...response.expression }
}

export async function verifyAgentAuthoringProfile(profile: AgentAuthoringProfileEnvelope): Promise<void> {
  const { profile_digest, ...payload } = profile
  if (await digestJson(payload as unknown as Json) !== profile_digest) {
    throw new AgentCompilerError('agent_binding_not_ready', 'Authoring profile digest does not match its exact bindings.')
  }
}

export type AgentSourceInput = Pick<AgentCompilerInput, 'manifest' | 'manifestPath' | 'inputSchema' | 'outputSchema' | 'plan'>
export interface AgentSourceSnapshot { manifest_path: string; files: Record<string, string> }

/** Capture a source-only snapshot before any dependency query. Rust owns all source semantics. */
export async function inspectAgentSources(input: AgentSourceInput, signal?: AbortSignal) {
  const inspected = await inspectAgentManifest(input.manifest)
  const manifestPath = input.manifestPath ?? 'agent.yaml'
  const files: Record<string, string> = Object.create(null)
  const file = (path: string, text: string) => {
    if (Object.hasOwn(files, path) && files[path] !== text) throw new AgentCompilerError('source_bundle_invalid', 'One source path was provided with conflicting file contents.')
    files[path] = text
  }
  file(manifestPath, input.manifest)
  file(inspected.inputSchemaPath, input.inputSchema)
  file(inspected.outputSchemaPath, input.outputSchema)
  if (inspected.planPath) {
    if (input.plan === undefined) throw new AgentCompilerError('source_reference_missing', 'The referenced Plan source has not been supplied.')
    file(inspected.planPath, input.plan)
  } else if (input.plan !== undefined) throw new AgentCompilerError('source_bundle_invalid', 'The manifest does not reference the supplied Plan source.')
  const sources: AgentSourceSnapshot = { manifest_path: manifestPath, files }
  accepted(await wasm('inspect_sources', { schema_version: 1, sources }, signal))
  return { sources, inspected }
}

export async function compileCapturedAgentSources(sources: AgentSourceSnapshot, profile: AgentCompilerProfile, bindings: ResolvedAgentBindings): Promise<CompiledAgent> {
  const { default_deadline_seconds, default_environment, policy_versions, deployment_policies, execution_profile, model_loop } = profile
  const response = accepted(await wasm('compile', {
    schema_version: 1,
    sources,
    profile: { default_deadline_seconds, default_environment, policy_versions, deployment_policies, execution_profile, model_loop },
    bindings,
  }))
  return compilationResult(response)
}

export async function compileAgentManifest(input: AgentCompilerInput): Promise<CompiledAgent> {
  const { sources } = await inspectAgentSources(input)
  return compileCapturedAgentSources(sources, input.profile, input.bindings)
}

/** Validate an exact persisted package with the owning full-bundle compiler ABI. */
export async function compileFrozenSourceBundle(source: string, signal?: AbortSignal): Promise<CompiledAgent> {
  if (encoder.encode(source).length > 8_388_608) throw new AgentCompilerError('compiler_limit_exceeded', 'Source bundle exceeds its byte limit.')
  const result = compilationResult(accepted(await wasm('compile_frozen', JSON.parse(source), signal)))
  if (result.sourceBundle !== source) throw new AgentCompilerError('source_bundle_invalid', 'The persisted package is not the exact canonical source bundle.')
  return result
}

function compilationResult(response: Record<string, unknown>): CompiledAgent {
  const result = response.compilation as {
    source_bundle_bytes: number[]; source_bundle_digest: string;
    source_map_bytes: number[]; source_map_digest: string;
    compiled: { name: string; execution_kind: AgentExecutionKind; canonical_manifest_bytes: number[];
      manifest_digest: string; resource_intent: Json & { contract_digest: string }; typed_plan_bytes: number[];
      typed_plan_digest: string; deployment_intent: Json; required_features: RequiredAgentFeature[]; lifecycle_plan: Json }
  }
  const compiled = result.compiled
  if (!Array.isArray(result.source_map_bytes) || !result.source_map_bytes.every((byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255) || !/^sha256:[0-9a-f]{64}$/.test(result.source_map_digest)) throw new AgentCompilerError('compiler_internal', 'The shared compiler returned no valid source map.')
  return {
    name: compiled.name, executionKind: executionKind(compiled.execution_kind),
    sourceBundle: decoder.decode(new Uint8Array(result.source_bundle_bytes)), sourceBundleDigest: result.source_bundle_digest,
    sourceMap: decoder.decode(new Uint8Array(result.source_map_bytes)), sourceMapDigest: result.source_map_digest,
    canonicalManifest: decoder.decode(new Uint8Array(compiled.canonical_manifest_bytes)), manifestDigest: compiled.manifest_digest,
    contractDigest: compiled.resource_intent.contract_digest, resourceIntent: compiled.resource_intent,
    typedPlan: decoder.decode(new Uint8Array(compiled.typed_plan_bytes)), typedPlanDigest: compiled.typed_plan_digest,
    deploymentIntent: compiled.deployment_intent, requiredFeatures: compiled.required_features, lifecyclePlan: compiled.lifecycle_plan,
  }
}

export async function compilerConformanceProjection(compiled: CompiledAgent): Promise<Json> {
  return {
    canonical_manifest: compiled.canonicalManifest, contract_digest: compiled.contractDigest,
    deployment_intent_digest: await digestJson(compiled.deploymentIntent), execution_kind: compiled.executionKind,
    lifecycle_plan_digest: await digestJson(compiled.lifecyclePlan), manifest_digest: compiled.manifestDigest,
    name: compiled.name, required_features: compiled.requiredFeatures,
    resource_intent_digest: await digestJson(compiled.resourceIntent), typed_plan: compiled.typedPlan,
    typed_plan_digest: compiled.typedPlanDigest,
  }
}

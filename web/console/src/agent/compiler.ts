import { isAlias, isMap, isScalar, parseDocument, visit } from 'yaml'

const API_VERSION = 'insight.platform/v1'
const MANIFEST_KIND = 'Agent'
const CLOSED_SCHEMA_PROFILE = 'insight.closed-json-schema/1'
const FAILURE_REFERENCE =
  'urn:insight:platform:v1:nominal:Failure@sha256:3a33a7ec0f81aa4a535b30113784bd6e4d620165fcc8d45f49c029d7124fdf62'
const MAX_MANIFEST_BYTES = 1_048_576
const MAX_SCHEMA_BYTES = 262_144
const MAX_INSTRUCTION_BYTES = 16_384
const utf8 = new TextEncoder()

export type AgentExecutionKind = 'deterministic' | 'model_chat'
export type RequiredAgentFeature = 'model'
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

export interface ResolvedAgentBindings {
  model: null | {
    manifest_ref: string
    deployment: ExactDeploymentRef & { resource_kind: 'model_deployment' }
    selection_policy: ExactPolicyBinding
  }
}

export interface AgentCompilerInput {
  manifest: string
  inputSchema: string
  outputSchema: string
  profile: AgentCompilerProfile
  bindings: ResolvedAgentBindings
}

interface ClosedJsonSchema {
  schema_version: 1
  profile: typeof CLOSED_SCHEMA_PROFILE
  schema: Json
  canonical_digest: string
}

export interface CompiledAgent {
  name: string
  executionKind: AgentExecutionKind
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
  readonly code:
    | 'agent_manifest_invalid'
    | 'agent_reference_missing'
    | 'agent_binding_not_ready'
    | 'agent_compile_failed'

  constructor(
    code:
      | 'agent_manifest_invalid'
      | 'agent_reference_missing'
      | 'agent_binding_not_ready'
      | 'agent_compile_failed',
    detail: string,
  ) {
    super(detail)
    this.name = 'AgentCompilerError'
    this.code = code
  }
}

export async function compileAgentManifest(input: AgentCompilerInput): Promise<CompiledAgent> {
  if (utf8.encode(input.manifest).byteLength > MAX_MANIFEST_BYTES || input.manifest.includes('\r')) {
    fail('agent_manifest_invalid', 'manifest is too large or uses non-canonical newlines')
  }
  const manifest = parseManifest(input.manifest)
  validateProfile(input.profile)
  const inputSchema = await compileSchema(input.inputSchema, 'input schema')
  const outputSchema = await compileSchema(input.outputSchema, 'output schema')
  requireObjectContract(inputSchema.schema, 'input schema')
  requireObjectContract(outputSchema.schema, 'output schema')
  const errorSchemaValue: Json = {
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    $ref: FAILURE_REFERENCE,
  }
  const errorSchema: ClosedJsonSchema = {
    schema_version: 1,
    profile: CLOSED_SCHEMA_PROFILE,
    schema: errorSchemaValue,
    canonical_digest: await digestJson(errorSchemaValue),
  }

  const displayName = manifest.metadata.displayName ?? defaultDisplayName(manifest.metadata.name)
  validateDisplayName(displayName)
  const deadlineSeconds = manifest.spec.limits?.deadlineSeconds ?? input.profile.default_deadline_seconds
  const environment = manifest.spec.publish?.environment ?? input.profile.default_environment
  validateDeadline(deadlineSeconds)
  validateStableName(environment, 'publish environment')

  if (
    manifest.spec.execution.kind === 'deterministic' &&
    inputSchema.canonical_digest !== outputSchema.canonical_digest
  ) {
    fail('agent_compile_failed', 'deterministic execution requires identical schema digests')
  }

  const normalizedManifest: Json = {
    apiVersion: API_VERSION,
    kind: MANIFEST_KIND,
    metadata: { displayName, name: manifest.metadata.name },
    spec: {
      execution: { kind: manifest.spec.execution.kind },
      input: {
        classification: manifest.spec.input.classification,
        schema: manifest.spec.input.schema,
      },
      instructions: manifest.spec.instructions,
      limits: { deadlineSeconds },
      model: manifest.spec.model,
      output: { schema: manifest.spec.output.schema },
      publish: { environment },
    },
  }
  const canonicalManifest = canonicalJson(normalizedManifest)
  const manifestDigest = await digestBytes(utf8.encode(canonicalManifest))
  const contractDigest = await digestJson({
    error_schema_digest: errorSchema.canonical_digest,
    input_schema_digest: inputSchema.canonical_digest,
    output_schema_digest: outputSchema.canonical_digest,
    schema_version: 1,
  })

  let slots: Json[] = []
  let requiredFeatures: RequiredAgentFeature[] = []
  let typedPlanValue: Json
  if (manifest.spec.execution.kind === 'deterministic') {
    if (input.bindings.model !== null) {
      fail('agent_binding_not_ready', 'deterministic execution cannot bind a model')
    }
    typedPlanValue = deterministicPlan(contractDigest, outputSchema.canonical_digest)
  } else {
    const model = manifest.spec.model
    if (model === null || input.bindings.model === null) {
      fail('agent_reference_missing', 'model_chat model reference is unresolved')
    }
    validateResolvedModel(model.ref, input.bindings.model)
    const requirementDigest = await digestJson({
      kind: 'model',
      manifest_ref: model.ref,
      schema_version: 1,
    })
    slots = [
      {
        requirement_digest: requirementDigest,
        slot_id: 'primary_model',
        target: {
          candidates: [input.bindings.model.deployment as unknown as Json],
          kind: 'model',
          selection_policy: input.bindings.model.selection_policy as unknown as Json,
        },
      },
    ]
    requiredFeatures = ['model']
    typedPlanValue = modelChatPlan(
      contractDigest,
      inputSchema.canonical_digest,
      outputSchema.canonical_digest,
      requirementDigest,
      input.profile.model_loop,
    )
  }
  const typedPlan = canonicalJson(typedPlanValue)
  const typedPlanDigest = await digestBytes(utf8.encode(typedPlan))

  const policyVersions = [...input.profile.policy_versions].sort((a, b) =>
    compareStrings(a.revision_id, b.revision_id),
  )
  rejectDuplicate(policyVersions.map((item) => item.revision_id), 'policy revision')
  const deploymentPolicies = [...input.profile.deployment_policies].sort((a, b) =>
    compareStrings(a.deployment.deployment_id, b.deployment.deployment_id),
  )
  rejectDuplicate(
    deploymentPolicies.map((item) => item.deployment.deployment_id),
    'policy deployment',
  )
  const resourceIntent: Json = {
    author_instructions: manifest.spec.instructions,
    authoring_artifact: {
      byte_length: utf8.encode(canonicalManifest).byteLength,
      classification: 'internal',
      content_digest: manifestDigest,
      display_name: `${manifest.metadata.name}.agent.json`,
      media_type: 'application/json',
      purpose: 'authoring_document',
    },
    contract_digest: contractDigest,
    dependency_versions: [],
    display_name: displayName,
    error_schema: errorSchema as unknown as Json,
    input_schema: inputSchema as unknown as Json,
    output_schema: outputSchema as unknown as Json,
    policy_versions: policyVersions as unknown as Json,
    typed_plan_artifact: {
      byte_length: utf8.encode(typedPlan).byteLength,
      classification: 'internal',
      content_digest: typedPlanDigest,
      display_name: `${manifest.metadata.name}.plan.json`,
      media_type: 'application/json',
      purpose: 'typed_plan',
    },
  }
  const deploymentIntent: Json = {
    default_deadline_seconds: deadlineSeconds,
    entry_node_id: 'start',
    entry_node_kind: 'start',
    environment,
    execution_profile: input.profile.execution_profile as unknown as Json,
    policies: deploymentPolicies as unknown as Json,
    slots,
  }
  return {
    name: manifest.metadata.name,
    executionKind: manifest.spec.execution.kind,
    canonicalManifest,
    manifestDigest,
    contractDigest,
    resourceIntent,
    typedPlan,
    typedPlanDigest,
    deploymentIntent,
    requiredFeatures,
    lifecyclePlan: lifecyclePlan(),
  }
}

export async function compilerConformanceProjection(compiled: CompiledAgent): Promise<Json> {
  return {
    canonical_manifest: compiled.canonicalManifest,
    contract_digest: compiled.contractDigest,
    deployment_intent_digest: await digestJson(compiled.deploymentIntent),
    execution_kind: compiled.executionKind,
    lifecycle_plan_digest: await digestJson(compiled.lifecyclePlan),
    manifest_digest: compiled.manifestDigest,
    name: compiled.name,
    required_features: compiled.requiredFeatures,
    resource_intent_digest: await digestJson(compiled.resourceIntent),
    typed_plan: compiled.typedPlan,
    typed_plan_digest: compiled.typedPlanDigest,
  }
}

interface Manifest {
  metadata: { name: string; displayName: string | null }
  spec: {
    execution: { kind: AgentExecutionKind }
    instructions: string | null
    model: { ref: string } | null
    input: { schema: string; classification: 'public' | 'internal' | 'confidential' | 'restricted' }
    output: { schema: string }
    limits: { deadlineSeconds: number } | null
    publish: { environment: string } | null
  }
}

function parseManifest(source: string): Manifest {
  const document = parseDocument(source, {
    merge: false,
    schema: 'core',
    strict: true,
    uniqueKeys: true,
    version: '1.2',
  })
  if (document.errors.length > 0 || document.contents === null) {
    fail('agent_manifest_invalid', 'manifest is not strict YAML 1.2 JSON-compatible input')
  }
  visit(document, (_key, node) => {
    if (typeof node !== 'object' || node === null) return
    if (isAlias(node) || ('anchor' in node && typeof node.anchor === 'string')) {
      fail('agent_manifest_invalid', 'YAML anchors and aliases are forbidden')
    }
    if ('tag' in node && typeof node.tag === 'string') {
      fail('agent_manifest_invalid', 'explicit YAML tags are forbidden')
    }
    if (isScalar(node) && node.type === 'PLAIN' && invalidPlainScalar(node.source ?? '')) {
      fail('agent_manifest_invalid', 'manifest contains a non-JSON implicit scalar')
    }
    if (isMap(node)) {
      for (const item of node.items) {
        if (!isScalar(item.key) || item.key.value === '<<') {
          fail('agent_manifest_invalid', 'mapping keys must be strings and merge keys are forbidden')
        }
      }
    }
  })
  const value = document.toJS({ maxAliasCount: 0 }) as unknown
  const root = closedObject(value, ['apiVersion', 'kind', 'metadata', 'spec'], 'manifest')
  exact(root.apiVersion, API_VERSION, 'apiVersion')
  exact(root.kind, MANIFEST_KIND, 'kind')
  const metadata = closedObject(root.metadata, ['name', 'displayName'], 'metadata', ['displayName'])
  const name = requiredString(metadata.name, 'metadata.name')
  validateStableName(name, 'metadata.name')
  const displayName = optionalString(metadata.displayName, 'metadata.displayName')
  if (displayName !== null) validateDisplayName(displayName)
  const spec = closedObject(
    root.spec,
    ['execution', 'instructions', 'model', 'input', 'output', 'limits', 'publish'],
    'spec',
    ['instructions', 'model', 'limits', 'publish'],
  )
  const execution = closedObject(spec.execution, ['kind'], 'spec.execution')
  const kind = requiredString(execution.kind, 'spec.execution.kind')
  if (kind !== 'deterministic' && kind !== 'model_chat') {
    fail('agent_manifest_invalid', 'execution kind is not supported')
  }
  const instructions = optionalString(spec.instructions, 'spec.instructions')
  const modelValue = spec.model
  let model: { ref: string } | null = null
  if (modelValue !== undefined && modelValue !== null) {
    const object = closedObject(modelValue, ['ref'], 'spec.model')
    model = { ref: requiredString(object.ref, 'spec.model.ref') }
  }
  const input = closedObject(spec.input, ['schema', 'classification'], 'spec.input')
  const inputPath = requiredString(input.schema, 'spec.input.schema')
  validateRelativePath(inputPath, 'input schema')
  const classification = requiredString(input.classification, 'spec.input.classification')
  if (!['public', 'internal', 'confidential', 'restricted'].includes(classification)) {
    fail('agent_manifest_invalid', 'input classification is not supported')
  }
  const output = closedObject(spec.output, ['schema'], 'spec.output')
  const outputPath = requiredString(output.schema, 'spec.output.schema')
  validateRelativePath(outputPath, 'output schema')
  let limits: { deadlineSeconds: number } | null = null
  if (spec.limits !== undefined && spec.limits !== null) {
    const object = closedObject(spec.limits, ['deadlineSeconds'], 'spec.limits')
    const deadlineSeconds = requiredInteger(object.deadlineSeconds, 'spec.limits.deadlineSeconds')
    validateDeadline(deadlineSeconds)
    limits = { deadlineSeconds }
  }
  let publish: { environment: string } | null = null
  if (spec.publish !== undefined && spec.publish !== null) {
    const object = closedObject(spec.publish, ['environment'], 'spec.publish')
    const environment = requiredString(object.environment, 'spec.publish.environment')
    validateStableName(environment, 'publish environment')
    publish = { environment }
  }
  if (kind === 'deterministic') {
    if (instructions !== null || model !== null) {
      fail('agent_manifest_invalid', 'deterministic execution forbids instructions and model')
    }
  } else {
    if (instructions === null || model === null) {
      fail('agent_manifest_invalid', 'model_chat requires instructions and model')
    }
    if (
      instructions.length === 0 ||
      utf8.encode(instructions).byteLength > MAX_INSTRUCTION_BYTES ||
      instructions.includes('\0')
    ) {
      fail('agent_manifest_invalid', 'instructions are outside their closed bounds')
    }
    rejectSensitiveLiteral(instructions)
    validateModelRef(model.ref)
  }
  return {
    metadata: { name, displayName },
    spec: {
      execution: { kind },
      instructions,
      model,
      input: {
        schema: inputPath,
        classification: classification as Manifest['spec']['input']['classification'],
      },
      output: { schema: outputPath },
      limits,
      publish,
    },
  }
}

function invalidPlainScalar(value: string): boolean {
  const lower = value.toLowerCase()
  if (['~', '.nan', '.inf', '+.inf', '-.inf'].includes(lower)) return true
  if (/^\d{4}-\d{2}-\d{2}(?:$|[tT ])/.test(value)) return true
  const first = value.charAt(0)
  const hasNumericPrefix = /[0-9+\-.]/.test(first)
  if (!hasNumericPrefix) return false
  const unsigned = value.startsWith('-') ? value.slice(1) : value
  return (
    value.startsWith('+') ||
    value.includes('_') ||
    /^(?:[-+])?0[xob]/i.test(value) ||
    value.endsWith('.') ||
    (unsigned.length > 1 && unsigned.startsWith('0') && /[0-9]/.test(unsigned.charAt(1)))
  )
}

async function compileSchema(source: string, name: string): Promise<ClosedJsonSchema> {
  if (utf8.encode(source).byteLength > MAX_SCHEMA_BYTES) {
    fail('agent_compile_failed', `${name} exceeds its byte limit`)
  }
  detectDuplicateJsonKeys(source, name)
  let value: unknown
  try {
    value = JSON.parse(source)
  } catch {
    fail('agent_compile_failed', `${name} is not strict JSON`)
  }
  validateClosedSchema(value, '$', new Set())
  const schema = value as Json
  return {
    schema_version: 1,
    profile: CLOSED_SCHEMA_PROFILE,
    schema,
    canonical_digest: await digestJson(schema),
  }
}

function requireObjectContract(schema: Json, name: string): void {
  if (!plainObject(schema) || schema.type !== 'object') {
    fail('agent_compile_failed', `${name} root must have type object`)
  }
}

function detectDuplicateJsonKeys(source: string, name: string): void {
  const document = parseDocument(source, { schema: 'json', strict: true, uniqueKeys: true, version: '1.2' })
  if (document.errors.length > 0) fail('agent_compile_failed', `${name} contains duplicate keys`)
}

function validateClosedSchema(value: unknown, path: string, refs: Set<string>): void {
  const schema = closedObject(
    value,
    [
      '$schema',
      '$ref',
      '$defs',
      'type',
      'properties',
      'required',
      'additionalProperties',
      'items',
      'oneOf',
      'const',
      'enum',
      'minimum',
      'maximum',
      'minLength',
      'maxLength',
      'minItems',
      'maxItems',
      'description',
      'x-platform-max-bytes',
      'x-platform-classification',
    ],
    path,
    [
      '$schema',
      '$ref',
      '$defs',
      'type',
      'properties',
      'required',
      'additionalProperties',
      'items',
      'oneOf',
      'const',
      'enum',
      'minimum',
      'maximum',
      'minLength',
      'maxLength',
      'minItems',
      'maxItems',
      'description',
      'x-platform-max-bytes',
      'x-platform-classification',
    ],
  )
  if (schema.$ref !== undefined) {
    const ref = requiredString(schema.$ref, `${path}.$ref`)
    if (!ref.startsWith('#/$defs/') && !ref.startsWith('urn:insight:platform:v1:nominal:')) {
      fail('agent_compile_failed', `${path} has a remote or unknown schema ref`)
    }
    if (refs.has(ref)) fail('agent_compile_failed', `${path} has a recursive schema ref`)
  }
  if (schema.type !== undefined) {
    const type = requiredString(schema.type, `${path}.type`)
    if (!['null', 'boolean', 'integer', 'number', 'string', 'array', 'object'].includes(type)) {
      fail('agent_compile_failed', `${path} has an unknown schema type`)
    }
    if (type === 'object') {
      if (schema.additionalProperties !== false || !plainObject(schema.properties)) {
        fail('agent_compile_failed', `${path} object must be closed`)
      }
      const properties = schema.properties as Record<string, unknown>
      for (const [key, child] of Object.entries(properties)) {
        validateClosedSchema(child, `${path}.properties.${key}`, refs)
      }
      if (!Array.isArray(schema.required) || !schema.required.every((item) => typeof item === 'string')) {
        fail('agent_compile_failed', `${path}.required must be a string array`)
      }
    } else if (type === 'string') {
      boundedIntegerPair(schema, 'minLength', 'maxLength', path)
      requiredInteger(schema['x-platform-max-bytes'], `${path}.x-platform-max-bytes`)
    } else if (type === 'array') {
      boundedIntegerPair(schema, 'minItems', 'maxItems', path)
      validateClosedSchema(schema.items, `${path}.items`, refs)
    } else if (type === 'number' || type === 'integer') {
      if (typeof schema.minimum !== 'number' || typeof schema.maximum !== 'number') {
        fail('agent_compile_failed', `${path} number must be bounded`)
      }
    }
  }
  if (schema.$defs !== undefined) {
    if (!plainObject(schema.$defs)) fail('agent_compile_failed', `${path}.$defs must be an object`)
    for (const [key, child] of Object.entries(schema.$defs as Record<string, unknown>)) {
      validateClosedSchema(child, `${path}.$defs.${key}`, refs)
    }
  }
  if (schema.oneOf !== undefined) {
    if (!Array.isArray(schema.oneOf) || schema.oneOf.length < 2) {
      fail('agent_compile_failed', `${path}.oneOf must be a non-empty tagged union`)
    }
    for (const [index, child] of schema.oneOf.entries()) {
      validateClosedSchema(child, `${path}.oneOf.${index}`, refs)
    }
  }
}

function boundedIntegerPair(
  object: Record<string, unknown>,
  minimum: string,
  maximum: string,
  path: string,
): void {
  const min = requiredInteger(object[minimum], `${path}.${minimum}`)
  const max = requiredInteger(object[maximum], `${path}.${maximum}`)
  if (min > max) fail('agent_compile_failed', `${path} has inverted bounds`)
}

function deterministicPlan(contract: string, schema: string): Json {
  return {
    dependency_slots: {},
    entry_node_id: 'start',
    interface_contract_digest: contract,
    nodes: {
      finish: { kind: 'return', value: { schema_digest: schema, source: 'run_input' } },
      start: { kind: 'start', next: 'finish' },
    },
    plan_version: 5,
  }
}

function modelChatPlan(
  contract: string,
  inputSchema: string,
  outputSchema: string,
  requirement: string,
  limits: AgentCompilerProfile['model_loop'],
): Json {
  const output: Json = {
    port_id: 'response',
    producer_node_id: 'model',
    schema_digest: outputSchema,
    source: 'node_output',
  }
  return {
    dependency_slots: { primary_model: { kind: 'model', requirement_digest: requirement } },
    entry_node_id: 'start',
    interface_contract_digest: contract,
    nodes: {
      finish: { kind: 'return', value: output },
      model: {
        capability_slot_ids: [],
        input: { schema_digest: inputSchema, source: 'run_input' },
        kind: 'model_loop',
        maximum_capability_calls: limits.maximum_capability_calls,
        maximum_parallel_calls_per_round: limits.maximum_parallel_calls_per_round,
        maximum_rounds: limits.maximum_rounds,
        model_route: null,
        model_slot_id: 'primary_model',
        output,
        resume: 'finish',
        skill_slot_ids: [],
        token_budget: limits.token_budget,
      },
      start: { kind: 'start', next: 'model' },
    },
    plan_version: 5,
  }
}

function lifecyclePlan(): Json {
  return {
    schema_version: 1,
    steps: [
      { kind: 'upload_authoring_artifact', ordinal: 1, requires: [] },
      { kind: 'upload_typed_plan_artifact', ordinal: 2, requires: [] },
      {
        kind: 'materialize_agent_document',
        ordinal: 3,
        requires: ['authoring_artifact', 'typed_plan_artifact'],
      },
      { kind: 'upsert_draft', ordinal: 4, requires: ['agent_document'] },
      { kind: 'validate_draft', ordinal: 5, requires: ['agent_resource'] },
      { kind: 'publish_revisions', ordinal: 6, requires: ['validation_operation'] },
      { kind: 'create_deployment', ordinal: 7, requires: ['published_revisions'] },
      { kind: 'activate_deployment', ordinal: 8, requires: ['agent_deployment'] },
      { kind: 'verify_active_binding', ordinal: 9, requires: ['active_binding'] },
    ],
  }
}

function validateProfile(profile: AgentCompilerProfile): void {
  validateDeadline(profile.default_deadline_seconds)
  validateStableName(profile.default_environment, 'profile environment')
  const limits = profile.model_loop
  for (const value of [
    limits.maximum_rounds,
    limits.maximum_capability_calls,
    limits.maximum_parallel_calls_per_round,
    limits.token_budget,
  ]) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      fail('agent_binding_not_ready', 'model loop limits must be positive safe integers')
    }
  }
  for (const version of profile.policy_versions) validatePolicyVersion(version)
  for (const binding of [...profile.deployment_policies, profile.execution_profile]) {
    validatePolicyBinding(binding)
  }
}

function validateResolvedModel(
  reference: string,
  binding: NonNullable<ResolvedAgentBindings['model']>,
): void {
  if (binding.manifest_ref !== reference || binding.deployment.resource_kind !== 'model_deployment') {
    fail('agent_binding_not_ready', 'resolved model does not match the manifest')
  }
  validateDigest(binding.deployment.deployment_digest, 'model deployment digest')
  validateResourceId(binding.deployment.deployment_id, 'mdep', 'model deployment')
  validatePolicyBinding(binding.selection_policy)
}

function validatePolicyBinding(binding: ExactPolicyBinding): void {
  if (binding.deployment.resource_kind !== 'policy_deployment') {
    fail('agent_binding_not_ready', 'policy binding has the wrong deployment kind')
  }
  validateResourceId(binding.deployment.deployment_id, 'pdep', 'policy deployment')
  validateDigest(binding.deployment.deployment_digest, 'policy deployment digest')
  validatePolicyVersion(binding.revision)
}

function validatePolicyVersion(version: ExactVersionRef): void {
  if (version.resource_kind !== 'policy_revision') {
    fail('agent_binding_not_ready', 'policy version has the wrong kind')
  }
  validateResourceId(version.revision_id, 'prev', 'policy revision')
  validateDigest(version.semantic_digest, 'policy revision digest')
}

function validateResourceId(value: string, prefix: string, name: string): void {
  const pattern = new RegExp(`^${prefix}_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
  if (!pattern.test(value)) fail('agent_binding_not_ready', `${name} ID is invalid`)
}

function validateDigest(value: string, name: string): void {
  if (!/^sha256:[0-9a-f]{64}$/.test(value)) fail('agent_binding_not_ready', `${name} is invalid`)
}

function validateDeadline(value: number): void {
  if (!Number.isInteger(value) || value < 1 || value > 3600) {
    fail('agent_manifest_invalid', 'deadlineSeconds must be between 1 and 3600')
  }
}

function validateStableName(value: string, name: string): void {
  if (!/^[a-z][a-z0-9-]{0,62}$/.test(value)) {
    fail('agent_manifest_invalid', `${name} is not a stable lowercase name`)
  }
}

function validateDisplayName(value: string): void {
  const hasControlCharacter = [...value].some((character) => {
    const codePoint = character.codePointAt(0) ?? 0
    return codePoint <= 0x1f || codePoint === 0x7f
  })
  if (value.length === 0 || [...value].length > 255 || hasControlCharacter) {
    fail('agent_manifest_invalid', 'displayName is outside its closed bounds')
  }
}

function validateRelativePath(value: string, name: string): void {
  if (
    value.length === 0 ||
    value.startsWith('/') ||
    value.includes('\\') ||
    value.includes('\0') ||
    value.split('/').some((part) => part.length === 0 || part === '.' || part === '..')
  ) {
    fail('agent_manifest_invalid', `${name} must be a normalized project-relative path`)
  }
}

function validateModelRef(value: string): void {
  if (value.startsWith('project/')) {
    validateStableName(value.slice('project/'.length), 'model ref')
    return
  }
  validateResourceId(value, 'mdep', 'model deployment')
}

function rejectSensitiveLiteral(value: string): void {
  const normalized = value.toLowerCase()
  if (
    ['http://', 'https://', 'postgres://', 'postgresql://', 'mysql://', 'mongodb://', 'sh -c', 'bash -c'].some(
      (needle) => normalized.includes(needle),
    ) ||
    value.includes('$(')
  ) {
    fail('agent_manifest_invalid', 'instructions contain an endpoint, database URL, or shell command')
  }
}

function defaultDisplayName(value: string): string {
  return value
    .split('-')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function closedObject(
  value: unknown,
  allowed: readonly string[],
  path: string,
  optional: readonly string[] = [],
): Record<string, unknown> {
  if (!plainObject(value)) fail('agent_manifest_invalid', `${path} must be an object`)
  const object = value as Record<string, unknown>
  for (const key of Object.keys(object)) {
    if (!allowed.includes(key)) fail('agent_manifest_invalid', `${path}.${key} is unknown`)
  }
  for (const key of allowed) {
    if (!optional.includes(key) && !(key in object)) {
      fail('agent_manifest_invalid', `${path}.${key} is required`)
    }
  }
  return object
}

function plainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function requiredString(value: unknown, path: string): string {
  if (typeof value !== 'string') fail('agent_manifest_invalid', `${path} must be a string`)
  return value
}

function optionalString(value: unknown, path: string): string | null {
  if (value === undefined || value === null) return null
  return requiredString(value, path)
}

function requiredInteger(value: unknown, path: string): number {
  if (!Number.isSafeInteger(value)) fail('agent_manifest_invalid', `${path} must be a safe integer`)
  return value as number
}

function exact(value: unknown, expected: string, path: string): void {
  if (value !== expected) fail('agent_manifest_invalid', `${path} has the wrong value`)
}

function rejectDuplicate(values: string[], name: string): void {
  if (new Set(values).size !== values.length) fail('agent_binding_not_ready', `duplicate ${name}`)
}

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0
}

function canonicalJson(value: Json): string {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return JSON.stringify(value)
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) {
      fail('agent_compile_failed', 'canonical JSON contains an invalid number')
    }
    return JSON.stringify(value)
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  const entries = Object.entries(value).sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(',')}}`
}

async function digestJson(value: Json): Promise<string> {
  return digestBytes(utf8.encode(canonicalJson(value)))
}

async function digestBytes(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', Uint8Array.from(bytes).buffer))
  return `sha256:${[...digest].map((byte) => byte.toString(16).padStart(2, '0')).join('')}`
}

function fail(
  code:
    | 'agent_manifest_invalid'
    | 'agent_reference_missing'
    | 'agent_binding_not_ready'
    | 'agent_compile_failed',
  detail: string,
): never {
  throw new AgentCompilerError(code, detail)
}

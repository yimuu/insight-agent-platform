import { parseDocument, stringify } from 'yaml'
import { inspectAgentManifest } from './compiler.ts'
import type { AgentExecutionKind, ResolvedAgentBindings } from './compiler.ts'

export const MAX_EDITOR_SOURCE_BYTES = 1_048_576
export const MAX_EDITOR_BUNDLE_BYTES = 8_388_608
const encoder = new TextEncoder()

export interface AgentFormFields {
  name: string
  displayName: string
  executionKind: AgentExecutionKind
  instructions: string
  modelAlias: string
  classification: string
  deadline: string
  environment: string
  inputSchemaPath: string
  outputSchemaPath: string
  planPath: string
}

export function buildFormManifest(values: AgentFormFields): string {
  return stringify({
    apiVersion: 'insight.platform/v1', kind: 'Agent',
    metadata: { name: values.name, displayName: values.displayName || undefined },
    spec: {
      execution: (values.executionKind === 'full_plan' || values.executionKind === 'framework_graph')
        ? { kind: values.executionKind, plan: values.planPath }
        : { kind: values.executionKind },
      instructions: values.executionKind === 'deterministic' ? null : values.instructions || null,
      model: values.executionKind === 'model_chat' ? { ref: values.modelAlias } : null,
      input: { schema: values.inputSchemaPath, classification: values.classification },
      output: { schema: values.outputSchemaPath },
      limits: values.deadline ? { deadlineSeconds: Number(values.deadline) } : null,
      publish: values.environment ? { environment: values.environment } : null,
    },
  }, { lineWidth: 0 })
}

/** Field projection follows a successful Rust inspection; this does not validate or lower a Plan. */
export async function manifestFormFields(manifest: string): Promise<AgentFormFields> {
  const inspected = await inspectAgentManifest(manifest)
  const document = parseDocument(manifest).toJS() as {
    metadata: { name: string; displayName?: string }
    spec: { instructions?: string; input: { classification: string }; limits?: { deadlineSeconds: number }; publish?: { environment: string } }
  }
  return {
    name: document.metadata.name,
    displayName: document.metadata.displayName ?? '',
    executionKind: inspected.executionKind,
    instructions: document.spec.instructions ?? '',
    modelAlias: inspected.modelRef ?? '',
    classification: document.spec.input.classification,
    deadline: document.spec.limits ? String(document.spec.limits.deadlineSeconds) : '',
    environment: document.spec.publish?.environment ?? '',
    inputSchemaPath: inspected.inputSchemaPath,
    outputSchemaPath: inspected.outputSchemaPath,
    planPath: inspected.planPath ?? 'plan.json',
  }
}

function editorJson(text: string, label: string, maximumBytes = MAX_EDITOR_SOURCE_BYTES): unknown {
  if (encoder.encode(text).byteLength > maximumBytes) throw new Error(`${label} exceeds its editor byte limit.`)
  let value: unknown
  try { value = JSON.parse(text) } catch { throw new Error(`${label} must contain complete JSON.`) }
  // JSON.parse alone discards duplicate keys. Preserve strict source intent before transport.
  const syntax = parseDocument(text, { uniqueKeys: true, strict: true })
  if (syntax.errors.length) throw new Error(`${label} contains duplicate keys or invalid JSON structure.`)
  return value
}

export function exactSlotBindings(text: string): NonNullable<ResolvedAgentBindings['slots']> {
  const value = editorJson(text, 'Exact slot bindings')
  if (!Array.isArray(value)) throw new Error('Exact slot bindings must be a JSON array.')
  // Owning Rust types validate every entry, digest, target, and Plan relationship.
  return value as NonNullable<ResolvedAgentBindings['slots']>
}

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

export interface EditableAgentSources {
  manifest: string
  manifestPath: string
  fields: AgentFormFields
  inputSchema: string
  outputSchema: string
  plan: string
  slotBindings: string
  modelBinding: ResolvedAgentBindings['model']
  compilerProfileDigest: string
}

/** Import editable files; publication always recompiles against the connected tenant's profile. */
export async function readEditableSourceBundle(text: string): Promise<EditableAgentSources> {
  const bundle = editorJson(text, 'Source bundle', MAX_EDITOR_BUNDLE_BYTES)
  if (!object(bundle) || bundle.schema_version !== 1 || !object(bundle.sources)
    || typeof bundle.compiler_semantic_identity !== 'string' || typeof bundle.compile_policy_inputs_digest !== 'string'
    || !object(bundle.profile) || typeof bundle.sources.manifest_path !== 'string' || !object(bundle.sources.files) || !object(bundle.bindings)) {
    throw new Error('The file is not a supported complete Agent source bundle.')
  }
  const files = bundle.sources.files
  const source = (path: string) => {
    const content = files[path]
    if (typeof content !== 'string') throw new Error(`The source bundle is missing ${path}.`)
    if (encoder.encode(content).byteLength > MAX_EDITOR_SOURCE_BYTES) throw new Error(`Source ${path} exceeds 1 MiB.`)
    return content
  }
  const manifest = source(bundle.sources.manifest_path)
  const fields = await manifestFormFields(manifest)
  const expected = new Set([bundle.sources.manifest_path, fields.inputSchemaPath, fields.outputSchemaPath])
  if ((fields.executionKind === 'full_plan' || fields.executionKind === 'framework_graph')) expected.add(fields.planPath)
  if (Object.keys(files).some((path) => !expected.has(path))) throw new Error('The source bundle contains unreferenced files; they cannot be discarded during import.')
  const slotBindings = JSON.stringify(bundle.bindings.slots ?? [], null, 2)
  exactSlotBindings(slotBindings)
  return {
    manifest, manifestPath: bundle.sources.manifest_path, fields,
    inputSchema: source(fields.inputSchemaPath),
    outputSchema: source(fields.outputSchemaPath),
    plan: (fields.executionKind === 'full_plan' || fields.executionKind === 'framework_graph') ? source(fields.planPath) : '',
    slotBindings,
    modelBinding: (bundle.bindings.model ?? null) as ResolvedAgentBindings['model'],
    compilerProfileDigest: bundle.compile_policy_inputs_digest,
  }
}

/** A bounded, non-authoritative outline. Unknown kinds remain visible for Rust diagnostics. */
export function planNodeOutline(text: string): { nodes: { id: string; kind: string; entry: boolean }[]; truncated: boolean } | null {
  if (!text.trim() || encoder.encode(text).byteLength > MAX_EDITOR_SOURCE_BYTES) return null
  try {
    const plan: unknown = JSON.parse(text)
    if (!object(plan) || !object(plan.nodes)) return null
    const entries = Object.entries(plan.nodes)
    return {
      nodes: entries.slice(0, 128).map(([id, node]) => ({ id, kind: object(node) && typeof node.kind === 'string' ? node.kind : 'Unspecified', entry: id === plan.entry_node_id })),
      truncated: entries.length > 128,
    }
  } catch { return null }
}

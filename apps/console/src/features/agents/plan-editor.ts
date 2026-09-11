import type { Json } from '../../shared/api/types.ts'
import { object } from '../../shared/schema/tree.ts'
import type { JsonObject, TreeSchema } from '../../shared/schema/tree.ts'

export interface NodeEditorDescriptor {
  schema_version: number
  plan_version: number
  compiler_semantic_identity: string
  draft_only: boolean
  nodes: { kind: string; template: JsonObject }[]
  templates: Record<string, Json>
  choices: Record<string, Json[]>
}
export function readNodeEditorDescriptor(text: string): NodeEditorDescriptor {
  if (new TextEncoder().encode(text).length > 131_072)
    throw new Error('Node editing descriptor exceeds its limit.')
  const value = JSON.parse(text) as NodeEditorDescriptor
  if (
    value.schema_version !== 1 ||
    value.plan_version !== 6 ||
    !value.draft_only ||
    !/^sha256:[0-9a-f]{64}$/.test(value.compiler_semantic_identity) ||
    !Array.isArray(value.nodes) ||
    value.nodes.some((node) => !object(node.template) || node.template.kind !== node.kind) ||
    !object(value.templates) ||
    !object(value.choices)
  )
    throw new Error('Unsupported owning node editing descriptor.')
  return value
}
const node = (
  schema: JsonObject,
  properties: Record<string, TreeSchema> = {},
  items?: TreeSchema,
  alternatives: TreeSchema[] = [],
  additional?: TreeSchema,
): TreeSchema => ({ schema, properties, items, alternatives, additional })
const generic = () => node({})

/** UI shapes are editing hints derived from Rust values. They are not Plan validation. */
export function draftFields(
  descriptor: NodeEditorDescriptor,
  template: Json,
  current: Json,
  field = '',
  depth = 0,
  variant = false,
): TreeSchema {
  if (depth > 32) return generic()
  const shape = (t: Json, c: Json = t, name = '') => draftFields(descriptor, t, c, name, depth + 1)
  const choices = (name: string, nullable = false) => {
    const values = descriptor.choices[name]
    if (!values) return undefined
    if (values.every((v) => !object(v)))
      return node({ enum: [...(nullable ? [null] : []), ...values] })
    const alternatives = values.map((value) => {
      const result = draftFields(descriptor, value, value, field, depth + 1, true)
      if (object(value))
        for (const tag of ['source', 'kind', 'op'])
          if (typeof value[tag] === 'string') result.properties[tag] = node({ const: value[tag] })
      return result
    })
    return node({}, {}, undefined, [...(nullable ? [node({ type: 'null' })] : []), ...alternatives])
  }
  const isPort =
    (object(current) && ['run_input', 'node_output'].includes(String(current.source))) ||
    (object(template) && ['run_input', 'node_output'].includes(String(template.source)))
  if (!variant) {
    if (isPort || ['model_route', 'candidate_route', 'payload'].includes(field))
      return (
        choices('port', ['model_route', 'candidate_route', 'payload'].includes(field)) ?? generic()
      )
    if (field === 'instruction') return choices('instruction') ?? generic()
    if (field === 'failure_policy') return choices('map_failure_policy') ?? generic()
    if (field === 'definition')
      return choices('human_definition') ?? shape(descriptor.templates.human_definition, current)
    if (field === 'eligibility_rule') return choices('eligibility_rule', true) ?? generic()
    if (field === 'policy') return choices('join_policy') ?? generic()
    if (field === 'remainder') return choices('remainder', true) ?? generic()
    if (field === 'cancellation_policy') return choices('cancellation_policy') ?? generic()
    if (field === 'quorum')
      return node({}, {}, undefined, [node({ type: 'null' }), node({ type: 'integer' })])
    if (
      field === 'kind' &&
      typeof template === 'string' &&
      descriptor.choices.dependency_kind?.includes(template)
    )
      return choices('dependency_kind') ?? generic()
  }
  if (Array.isArray(template) || Array.isArray(current)) {
    const arrays: Record<string, string> = {
      assignments: 'assignment',
      ordered_arms: 'branch_arm',
      carried_ports: 'loop_carried_port',
      input_ports: 'port',
      instructions: 'instruction',
    }
    const element = arrays[field]
      ? descriptor.templates[arrays[field]]
      : ['legs', 'skill_slot_ids', 'capability_slot_ids', 'ordered_fields'].includes(field)
        ? ''
        : Array.isArray(template)
          ? template[0]
          : undefined
    return node(
      { type: 'array', maxItems: 4096 },
      {},
      element === undefined ? generic() : shape(element, element, arrays[field] ?? ''),
    )
  }
  if (object(template) || object(current)) {
    const t = object(template) ? template : {}
    const c = object(current) ? current : {}
    if (field === 'schema') return generic()
    const map =
      field === 'handlers'
        ? node({ type: 'string' })
        : field === 'dependency_slots'
          ? shape(descriptor.templates.dependency_slot)
          : field === 'schema_documents'
            ? shape(descriptor.templates.schema_document)
            : undefined
    if (map)
      return node(
        { type: 'object', additionalProperties: true },
        Object.fromEntries(
          Object.entries(c).map(([key, value]) => [
            key,
            field === 'handlers'
              ? map
              : shape(
                  field === 'dependency_slots'
                    ? descriptor.templates.dependency_slot
                    : descriptor.templates.schema_document,
                  value,
                ),
          ]),
        ),
        undefined,
        [],
        map,
      )
    const props = Object.fromEntries(
      [...new Set([...Object.keys(t), ...Object.keys(c)])].map((key) => [
        key,
        shape(t[key] ?? c[key] ?? null, c[key] ?? t[key] ?? null, key),
      ]),
    )
    return node({ type: 'object', required: Object.keys(t), additionalProperties: false }, props)
  }
  const value = template ?? current
  if (value === null || value === undefined) return generic()
  return node({ type: typeof value === 'number' ? 'number' : typeof value })
}

export function sourceLocations(
  sourceMap: string | undefined,
  nodeId: string,
): {
  file: string
  source_pointer: string
  line: number
  column: number
  kind: string
  ir_pointer: string
}[] {
  if (!sourceMap) return []
  try {
    const map = JSON.parse(sourceMap)
    if (map.schema_version !== 1 || !Array.isArray(map.entries)) return []
    return map.entries
      .filter((entry: { target?: { node_id?: string } }) => entry.target?.node_id === nodeId)
      .slice(0, 256)
      .flatMap(
        (entry: {
          source?: { file?: unknown; source_pointer?: unknown; line?: unknown; column?: unknown }
          target: { kind?: unknown; ir_pointer?: unknown }
        }) => {
          const s = entry.source
          const t = entry.target
          return s &&
            typeof s.file === 'string' &&
            typeof s.source_pointer === 'string' &&
            Number.isSafeInteger(s.line) &&
            Number(s.line) > 0 &&
            Number.isSafeInteger(s.column) &&
            Number(s.column) > 0 &&
            typeof t.kind === 'string' &&
            typeof t.ir_pointer === 'string'
            ? [
                {
                  file: s.file,
                  source_pointer: s.source_pointer,
                  line: Number(s.line),
                  column: Number(s.column),
                  kind: t.kind,
                  ir_pointer: t.ir_pointer,
                },
              ]
            : []
        },
      )
  } catch {
    return []
  }
}

/** Port references are suggestions only; literal application data is not traversed. */
export function referencedPorts(value: Json): { keys: (string | number)[]; value: JsonObject }[] {
  const result: { keys: (string | number)[]; value: JsonObject }[] = []
  const visit = (item: Json, keys: (string | number)[]) => {
    if (keys.length > 32 || result.length >= 4096) return
    if (Array.isArray(item)) item.forEach((v, i) => visit(v, [...keys, i]))
    else if (object(item)) {
      if (
        ['run_input', 'node_output'].includes(String(item.source)) &&
        typeof item.schema_digest === 'string'
      ) {
        result.push({ keys, value: item })
        return
      }
      Object.entries(item).forEach(([key, child]) => {
        if (!(item.op === 'literal' && key === 'value') && key !== 'schema_documents')
          visit(child, [...keys, key])
      })
    }
  }
  visit(value, [])
  return result
}
export function replaceAt(value: Json, keys: (string | number)[], replacement: Json): Json {
  if (!keys.length) return replacement
  const [head, ...tail] = keys
  if (Array.isArray(value) && typeof head === 'number')
    return value.map((child, i) => (i === head ? replaceAt(child, tail, replacement) : child))
  if (object(value) && typeof head === 'string' && Object.hasOwn(value, head))
    return { ...value, [head]: replaceAt(value[head], tail, replacement) }
  throw new Error('The selected source field changed. Select it again.')
}

export function authoredExpressions(
  value: Json,
): { keys: (string | number)[]; value: JsonObject }[] {
  const result: { keys: (string | number)[]; value: JsonObject }[] = []
  const visit = (item: Json, keys: (string | number)[]) => {
    if (keys.length > 32 || result.length >= 4096) return
    if (Array.isArray(item)) item.forEach((child, i) => visit(child, [...keys, i]))
    else if (object(item)) {
      if (Object.hasOwn(item, 'expression_version') && Object.hasOwn(item, 'instructions')) {
        result.push({ keys, value: item })
        return
      }
      Object.entries(item).forEach(([key, child]) => {
        if (!(item.op === 'literal' && key === 'value') && key !== 'schema_documents')
          visit(child, [...keys, key])
      })
    }
  }
  visit(value, [])
  return result
}

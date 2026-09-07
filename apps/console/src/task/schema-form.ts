import type { ClosedJsonSchema, Json } from '../api/types.ts'
export type SchemaObject = { [key: string]: Json }
export type TaskFormSchema = SchemaObject | ClosedJsonSchema

// These are renderer capacity limits, not an alternative schema or authorization contract.
export const FORM_LIMITS = { schemaBytes: 262_144, depth: 8, nodes: 256, fields: 64, items: 64, choices: 128, controls: 512, text: 8_192, responseBytes: 65_536 } as const
const closedProfile = 'insight.closed-json-schema/1'
const mcpProfile = 'mcp.form-json-schema/2025-11-25'
const encoder = new TextEncoder()
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value)
const own = (value: object, key: string) => Object.hasOwn(value, key)
export const childPath = (path: string, key: string | number) => `${path}/${String(key).replaceAll('~', '~0').replaceAll('/', '~1')}`
export const fieldName = (path: string) => path || 'Response'

export interface FormNode {
  type: 'object' | 'array' | 'string' | 'number' | 'integer' | 'boolean'
  title?: string
  description?: string
  properties?: { name: string; required: boolean; node: FormNode }[]
  items?: FormNode
  minItems?: number
  maxItems?: number
  uniqueItems?: boolean
  minLength?: number
  maxLength?: number
  maxBytes?: number
  minimum?: number
  maximum?: number
  exclusiveMinimum?: number
  exclusiveMaximum?: number
  choices?: (string | number | boolean)[]
  choiceNames?: string[]
}
export interface SchemaAnalysis { node?: FormNode; issues: string[] }

export function schemaIdentity(schema: TaskFormSchema, digest: string): string {
  try { return `${digest}\n${JSON.stringify(schema)}` } catch { return `${digest}\ninvalid-schema` }
}

export function analyzeTaskSchema(input: TaskFormSchema, digest: string): SchemaAnalysis {
  const issues: string[] = []
  const issue = (path: string, text: string) => { if (issues.length < 20) issues.push(`${fieldName(path)}: ${text}`) }
  let bytes: number
  try { bytes = encoder.encode(JSON.stringify(input)).length } catch { return { issues: ['Response schema is not JSON.'] } }
  if (bytes > FORM_LIMITS.schemaBytes) return { issues: [`Response schema exceeds this form’s ${FORM_LIMITS.schemaBytes}-byte limit.`] }
  if (!digest) issue('', 'The frozen response schema digest is missing.')
  if (!record(input)) return { issues: ['Response schema must be an object.'] }
  let profile = closedProfile
  let raw: unknown = input
  if (own(input, 'schema')) {
    if (input.schema_version !== 1) issue('', 'Unsupported schema envelope version.')
    if (input.profile !== closedProfile && input.profile !== mcpProfile) issue('', 'Unsupported schema profile.')
    else profile = input.profile
    if (input.canonical_digest !== digest) issue('', 'Schema envelope does not match the frozen response schema digest.')
    for (const key of Object.keys(input)) if (!['schema_version', 'profile', 'schema', 'canonical_digest'].includes(key)) issue('', `Unsupported schema envelope field: ${key}.`)
    raw = input.schema
  }
  const mcp = profile === mcpProfile
  if (mcp && bytes > 65_536) issue('', 'MCP form schema exceeds its byte limit.')
  let count = 0
  const parse = (value: unknown, path: string, depth: number): FormNode | undefined => {
    if (++count > FORM_LIMITS.nodes || depth > FORM_LIMITS.depth) { issue(path, 'Schema exceeds this form’s complexity limit.'); return }
    if (!record(value)) { issue(path, 'Schema must be an object.'); return }
    const type = value.type
    if (!['object', 'array', 'string', 'number', 'integer', 'boolean'].includes(type as string)) {
      const capability = ['$ref', '$defs', 'oneOf', 'anyOf', 'allOf', 'const'].find((key) => own(value, key))
      issue(path, capability ? `Unsupported schema capability: ${capability}.` : 'Unsupported or missing schema type.')
      return
    }
    const node: FormNode = { type: type as FormNode['type'] }
    const allowed = new Set(['type', 'title', 'description', 'x-platform-classification'])
    if (depth === 0) { allowed.add('$schema'); allowed.add('$id') }
    if (mcp && type !== 'object') allowed.add('default') // A suggestion is never automatically submitted.
    if (type === 'object') ['properties', 'required', 'additionalProperties'].forEach((key) => allowed.add(key))
    if (type === 'array') ['items', 'minItems', 'maxItems', 'uniqueItems'].forEach((key) => allowed.add(key))
    if (type === 'string') ['minLength', 'maxLength', 'x-platform-max-bytes'].forEach((key) => allowed.add(key))
    if (type === 'number' || type === 'integer') ['minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum'].forEach((key) => allowed.add(key))
    if (type !== 'object' && type !== 'array') { allowed.add('enum'); if (mcp) allowed.add('enumNames') }
    for (const key of Object.keys(value)) if (!allowed.has(key)) issue(path, `Unsupported schema capability: ${key}.`)
    if (own(value, '$schema') && value.$schema !== 'https://json-schema.org/draft/2020-12/schema' && !(mcp && value.$schema === 'https://json-schema.org/draft/2020-12/schema#')) issue(path, 'Unsupported JSON Schema dialect.')
    for (const key of ['title', 'description'] as const) {
      if (!own(value, key)) continue
      if (typeof value[key] !== 'string' || encoder.encode(value[key]).length > FORM_LIMITS.text) issue(path, `Invalid or oversized ${key}.`)
      else node[key] = value[key]
    }
    if (own(value, 'enum')) {
      if (!Array.isArray(value.enum) || value.enum.length < 1 || value.enum.length > FORM_LIMITS.choices || !value.enum.every((item) => {
        if (type === 'string') return typeof item === 'string' && encoder.encode(item).length <= FORM_LIMITS.text
        if (type === 'boolean') return typeof item === 'boolean'
        return typeof item === 'number' && Number.isFinite(item) && (type !== 'integer' || Number.isSafeInteger(item))
      }) || new Set(value.enum.map((item) => JSON.stringify(item))).size !== value.enum.length) issue(path, 'Enum must contain distinct bounded values of the field’s type.')
      else node.choices = value.enum as FormNode['choices']
    }
    if (own(value, 'enumNames')) {
      if (!node.choices || !Array.isArray(value.enumNames) || value.enumNames.length !== node.choices.length || !value.enumNames.every((item) => typeof item === 'string' && encoder.encode(item).length <= FORM_LIMITS.text)) issue(path, 'enumNames must match the enum choices.')
      else node.choiceNames = value.enumNames as string[]
    }
    const bound = (key: string, fallback: number | undefined, ceiling: number, positive = false): number | undefined => {
      const candidate = own(value, key) ? value[key] : fallback
      if (candidate === undefined) { issue(path, `Missing ${key} bound.`); return }
      if (!Number.isSafeInteger(candidate) || (candidate as number) < (positive ? 1 : 0) || (candidate as number) > ceiling) { issue(path, `${key} exceeds this form’s supported range (maximum ${ceiling}).`); return }
      return candidate as number
    }
    if (type === 'object') {
      if (mcp && depth !== 0) issue(path, 'Nested objects are unsupported in the MCP form profile.')
      if (value.additionalProperties !== false && !(mcp && depth === 0 && !own(value, 'additionalProperties'))) issue(path, 'Object must have additionalProperties: false.')
      if (!mcp && !own(value, 'required')) issue(path, 'Object must explicitly declare required fields, including an empty list.')
      if (!record(value.properties)) issue(path, 'Object properties must be provided.')
      else {
        const entries = Object.entries(value.properties)
        if (entries.length > FORM_LIMITS.fields) issue(path, `Object exceeds this form’s ${FORM_LIMITS.fields}-field limit.`)
        const required = own(value, 'required') ? value.required : []
        if (!Array.isArray(required) || !required.every((item) => typeof item === 'string' && own(value.properties as object, item)) || new Set(required).size !== required.length) issue(path, 'Required fields must be distinct declared properties.')
        node.properties = entries.slice(0, FORM_LIMITS.fields).flatMap(([name, schema]) => {
          if (encoder.encode(name).length > 256) issue(path, 'Property name exceeds this form’s limit.')
          const child = parse(schema, childPath(path, name), depth + 1)
          return child ? [{ name, required: Array.isArray(required) && required.includes(name), node: child }] : []
        })
      }
    }
    if (type === 'array') {
      node.items = parse(value.items, childPath(path, 'items'), depth + 1)
      node.minItems = bound('minItems', mcp ? 0 : undefined, FORM_LIMITS.items)
      node.maxItems = bound('maxItems', mcp ? node.items?.choices?.length : undefined, FORM_LIMITS.items, true)
      if (node.minItems !== undefined && node.maxItems !== undefined && node.minItems > node.maxItems) issue(path, 'minItems exceeds maxItems.')
      if (own(value, 'uniqueItems') && typeof value.uniqueItems !== 'boolean') issue(path, 'uniqueItems must be boolean.')
      node.uniqueItems = mcp || value.uniqueItems === true
      if (mcp && (node.items?.type !== 'string' || !node.items.choices || (node.maxItems ?? 0) > node.items.choices.length)) issue(path, 'MCP arrays require bounded string enum choices.')
    }
    if (type === 'string') {
      node.minLength = bound('minLength', mcp ? 0 : undefined, FORM_LIMITS.text)
      node.maxLength = bound('maxLength', mcp ? FORM_LIMITS.text : undefined, FORM_LIMITS.text, true)
      node.maxBytes = bound('x-platform-max-bytes', mcp ? FORM_LIMITS.text : undefined, FORM_LIMITS.text, true)
      if (node.minLength !== undefined && node.maxLength !== undefined && node.minLength > node.maxLength) issue(path, 'minLength exceeds maxLength.')
    }
    if (type === 'number' || type === 'integer') {
      for (const key of ['minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum'] as const) {
        if (!own(value, key)) continue
        const limit = value[key]
        if (typeof limit !== 'number' || !Number.isFinite(limit) || Math.abs(limit) > Number.MAX_SAFE_INTEGER || (type === 'integer' && !Number.isSafeInteger(limit))) issue(path, `Unsupported ${key} bound.`)
        else node[key] = limit
      }
      if (node.minimum !== undefined && node.maximum !== undefined && node.minimum > node.maximum) issue(path, 'minimum exceeds maximum.')
    }
    return node
  }
  const node = parse(raw, '', 0)
  if (node?.type !== 'object') issue('', 'Response root must be an object.')
  const controls = (item: FormNode): number => 1 + (item.properties?.reduce((total, property) => total + controls(property.node), 0) ?? 0) + (item.items ? (item.maxItems ?? 0) * controls(item.items) : 0)
  if (node && controls(node) > FORM_LIMITS.controls) issue('', `Schema exceeds this form’s ${FORM_LIMITS.controls}-control capacity.`)
  return issues.length ? { issues } : { node, issues }
}

export interface FormDraft { included: boolean; raw: string; overflow?: boolean; children: Record<string, FormDraft>; items: FormDraft[] }
export function createDraft(node: FormNode, included = true): FormDraft {
  return { included, raw: '', children: Object.fromEntries((node.properties ?? []).map((property) => [property.name, createDraft(property.node, property.required)])), items: [] }
}
export function changeRaw(draft: FormDraft, raw: string): FormDraft {
  // Keep only a bounded draft but remember overflow; never submit a silently truncated prefix.
  const limit = FORM_LIMITS.text * 2
  return { ...draft, raw: raw.slice(0, limit), overflow: raw.length > limit }
}
export interface DraftValidation { value?: Json; errors: Record<string, string> }
export function validateDraft(node: FormNode, draft: FormDraft): DraftValidation {
  const errors: Record<string, string> = Object.create(null) as Record<string, string>
  const fail = (path: string, message: string) => { errors[path] ??= message }
  const visit = (field: FormNode, state: FormDraft, path: string): Json | undefined => {
    if (!state.included) return undefined
    if (state.overflow) fail(path, 'Input exceeded this form’s field limit. Edit this field before submitting.')
    if (field.type === 'object') return Object.fromEntries((field.properties ?? []).flatMap((property) => {
      const child = state.children[property.name]
      const nextPath = childPath(path, property.name)
      if (property.required && !child.included) fail(nextPath, 'This field is required.')
      const value = visit(property.node, child, nextPath)
      return value === undefined ? [] : [[property.name, value]]
    }))
    if (field.type === 'array') {
      if (state.items.length < (field.minItems ?? 0)) fail(path, `Add at least ${field.minItems} item(s).`)
      if (state.items.length > (field.maxItems ?? 0)) fail(path, `Use at most ${field.maxItems} item(s).`)
      const values = state.items.map((item, index) => visit(field.items!, item, childPath(path, index)) ?? null)
      if (field.uniqueItems) {
        const seen = new Set<string>()
        values.forEach((item, index) => {
          const key = canonicalJson(item)
          if (seen.has(key)) fail(childPath(path, index), 'Each array item must be unique.')
          seen.add(key)
        })
      }
      return values
    }
    let value: string | number | boolean = state.raw
    if (field.choices) {
      const index = Number(state.raw)
      if (!/^(0|[1-9][0-9]*)$/.test(state.raw) || !Number.isSafeInteger(index) || index >= field.choices.length) { fail(path, 'Select a value.'); return }
      value = field.choices[index]
    } else if (field.type === 'boolean') {
      if (state.raw !== 'true' && state.raw !== 'false') { fail(path, 'Select Yes or No.'); return }
      value = state.raw === 'true'
    } else if (field.type === 'number' || field.type === 'integer') {
      if (state.raw.length > 128 || !/^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/.test(state.raw)) { fail(path, 'Enter a JSON number.'); return }
      value = Number(state.raw)
    }
    if (typeof value === 'string') {
      const length = [...value].length
      if ([...value].some((character) => { const point = character.codePointAt(0)!; return point >= 0xd800 && point <= 0xdfff })) fail(path, 'Text contains invalid Unicode.')
      if (length < (field.minLength ?? 0)) fail(path, `Use at least ${field.minLength} character(s).`)
      if (length > (field.maxLength ?? FORM_LIMITS.text)) fail(path, `Use at most ${field.maxLength} character(s).`)
      if (encoder.encode(value).length > (field.maxBytes ?? FORM_LIMITS.text)) fail(path, `Text must fit within ${field.maxBytes} UTF-8 bytes.`)
    }
    if (typeof value === 'number') {
      if (!Number.isFinite(value) || Math.abs(value) > Number.MAX_SAFE_INTEGER) fail(path, 'Enter a finite number within the interoperable range.')
      if (field.type === 'integer' && !Number.isSafeInteger(value)) fail(path, 'Enter a safe integer.')
      if (field.minimum !== undefined && value < field.minimum) fail(path, `Use a value of at least ${field.minimum}.`)
      if (field.maximum !== undefined && value > field.maximum) fail(path, `Use a value of at most ${field.maximum}.`)
      if (field.exclusiveMinimum !== undefined && value <= field.exclusiveMinimum) fail(path, `Use a value greater than ${field.exclusiveMinimum}.`)
      if (field.exclusiveMaximum !== undefined && value >= field.exclusiveMaximum) fail(path, `Use a value less than ${field.exclusiveMaximum}.`)
    }
    return value
  }
  const value = visit(node, draft, '')
  if (value === undefined) fail('', 'A response is required.')
  if (encoder.encode(JSON.stringify(value)).length > FORM_LIMITS.responseBytes) fail('', `Response exceeds this form’s ${FORM_LIMITS.responseBytes}-byte limit.`)
  return { value: Object.keys(errors).length ? undefined : value, errors }
}

function canonicalJson(value: Json): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (value !== null && typeof value === 'object') return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`
  return JSON.stringify(value)
}

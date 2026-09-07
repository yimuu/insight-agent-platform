import type { Json } from '../api/types.ts'
export type JsonObject = { [key: string]: Json }

// Editing capacity follows the Task input port. These checks never grant permission or
// replace the owning Rust schema/instance validator.
export const TREE_LIMITS = { bytes: 65_536, depth: 32, items: 4_096, properties: 1_024, stringBytes: 65_536, schemaBytes: 262_144, schemaNodes: 16_384, schemaDepth: 128 } as const
export const object = (value: unknown): value is JsonObject => value !== null && typeof value === 'object' && !Array.isArray(value)
export const pointer = (path: string, key: string | number) => `${path}/${String(key).replaceAll('~', '~0').replaceAll('/', '~1')}`
export const equal = (a: Json, b: Json): boolean => JSON.stringify(sorted(a)) === JSON.stringify(sorted(b))
const sorted = (v: Json): Json => Array.isArray(v) ? v.map(sorted) : object(v) ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, sorted(v[k])])) : v
const bytes = (v: string) => new TextEncoder().encode(v).length
export type NominalSchemas = ReadonlyMap<string, JsonObject>
export interface TreeSchema { schema: JsonObject; properties: Record<string, TreeSchema>; items?: TreeSchema; additional?: TreeSchema; alternatives: TreeSchema[] }

/** Resolve schema positions only. Application values in const/enum/default are opaque. */
export function treeSchema(input: JsonObject, nominals: NominalSchemas = new Map()): TreeSchema {
  if (bytes(JSON.stringify(input)) > TREE_LIMITS.schemaBytes) throw new Error('Schema exceeds the form byte limit.')
  let nodes = 0
  const rootIds = new WeakMap<object, number>()
  const rootId = (root: object) => { let id = rootIds.get(root); if (id === undefined) { id = nextRoot++; rootIds.set(root, id) }; return id }
  let nextRoot = 0
  const visit = (value: Json, root: JsonObject, depth: number, references: Set<string>): TreeSchema => {
    if (++nodes > TREE_LIMITS.schemaNodes || depth > TREE_LIMITS.schemaDepth) throw new Error('Schema exceeds the bounded tree capacity.')
    if (!object(value)) throw new Error('Unsupported non-object schema.')
    let schema = value
    if ('$ref' in schema) {
      if (typeof schema.$ref !== 'string') throw new Error('Invalid schema reference.')
      const ref = schema.$ref
      const identity = `${rootId(root)}:${ref}`
      if (references.has(identity)) throw new Error('Recursive schema references cannot be edited.')
      let target: Json | undefined
      let targetRoot = root
      if (ref.startsWith('#/$defs/')) {
        const key = ref.slice(8).replaceAll('~1', '/').replaceAll('~0', '~')
        if (ref.slice(8).includes('/') || /~(?![01])/.test(ref.slice(8))) throw new Error('Unsupported local schema pointer.')
        target = object(root.$defs) ? root.$defs[key] : undefined
      } else { target = nominals.get(ref); if (object(target)) targetRoot = target }
      if (!object(target)) throw new Error(`Unknown local or exact pinned schema reference: ${ref.slice(0, 200)}`)
      const resolved = visit(target, targetRoot, depth + 1, new Set([...references, identity]))
      const { $ref: ignored, ...siblings } = schema
      void ignored
      if (Object.keys(siblings).every((k) => ['$schema', '$id', '$defs', 'title', 'description', 'x-platform-classification'].includes(k))) return { ...resolved, schema: { ...resolved.schema, ...siblings } }
      schema = { ...resolved.schema, ...siblings }
      // Already resolved children retain the correct root for nominal references.
      const overlay = visit(siblings, root, depth + 1, references)
      return { schema, properties: { ...resolved.properties, ...overlay.properties }, items: overlay.items ?? resolved.items, additional: overlay.additional ?? resolved.additional, alternatives: overlay.alternatives.length ? overlay.alternatives : resolved.alternatives }
    }
    const allowed = new Set(['$schema', '$id', '$defs', 'type', 'title', 'description', 'properties', 'required', 'additionalProperties', 'items', 'minItems', 'maxItems', 'uniqueItems', 'minLength', 'maxLength', 'minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'multipleOf', 'enum', 'const', 'oneOf', 'anyOf', 'allOf', 'enumNames', 'default', 'format', 'pattern', 'x-platform-max-bytes', 'x-platform-classification'])
    const unknown = Object.keys(schema).find((key) => !allowed.has(key))
    if (unknown) throw new Error(`Unsupported schema capability: ${unknown}`)
    if (schema.type !== undefined && !['object', 'array', 'string', 'integer', 'number', 'boolean', 'null'].includes(String(schema.type))) throw new Error('Unsupported schema type.')
    // Platform nominal allOf schemas are displayed through their common typed object.
    if (Array.isArray(schema.allOf)) {
      const branches = schema.allOf.map((s) => visit(s, root, depth + 1, references))
      const { allOf: ignored, ...base } = schema; void ignored
      const main = visit(base, root, depth + 1, references)
      return branches.reduce((a, b) => ({ ...a, schema: { ...b.schema, ...a.schema, required: [...new Set([...(Array.isArray(a.schema.required) ? a.schema.required : []), ...(Array.isArray(b.schema.required) ? b.schema.required : [])])] }, properties: { ...b.properties, ...a.properties }, items: a.items ?? b.items, alternatives: a.alternatives.length ? a.alternatives : b.alternatives }), main)
    }
    const alternatives = schema.oneOf ?? schema.anyOf
    return { schema, properties: object(schema.properties) ? Object.fromEntries(Object.entries(schema.properties).map(([k, v]) => [k, visit(v, root, depth + 1, references)])) : {},
      items: object(schema.items) ? visit(schema.items, root, depth + 1, references) : undefined,
      additional: object(schema.additionalProperties) ? visit(schema.additionalProperties, root, depth + 1, references) : undefined,
      alternatives: Array.isArray(alternatives) ? alternatives.map((s) => visit(s, root, depth + 1, references)) : [] }
  }
  return visit(input, input, 0, new Set())
}

export function initialValue(node: TreeSchema, depth = 0): Json {
  if (depth > TREE_LIMITS.depth) return null
  const s = node.schema
  if (Object.hasOwn(s, 'const')) return structuredClone(s.const)
  if (Array.isArray(s.enum) && s.enum.length) return structuredClone(s.enum[0])
  if (node.alternatives.length) return initialValue(node.alternatives[0], depth + 1)
  if (s.type === 'object') return Object.fromEntries(Object.entries(node.properties).filter(([k]) => Array.isArray(s.required) && s.required.includes(k)).map(([k, n]) => [k, initialValue(n, depth + 1)]))
  if (s.type === 'array') return []
  if (s.type === 'boolean') return false
  if (s.type === 'number' || s.type === 'integer') return typeof s.minimum === 'number' ? s.minimum : 0
  if (s.type === 'string') return ''
  return null
}

export function branchIndex(node: TreeSchema, value: Json): number {
  return node.alternatives.findIndex((branch) => {
    const s = branch.schema
    if (Object.hasOwn(s, 'const')) return equal(s.const, value)
    if (Array.isArray(s.enum)) return s.enum.some((choice) => equal(choice, value))
    if (object(value) && s.type === 'object') {
      const tags = Object.entries(branch.properties).filter(([, child]) => Object.hasOwn(child.schema, 'const'))
      return tags.every(([k, child]) => Object.hasOwn(value, k) && equal(child.schema.const, value[k]))
    }
    return typeMatches(s.type, value)
  })
}
const typeMatches = (type: Json | undefined, value: Json): boolean => type === undefined || (type === 'null' ? value === null : type === 'object' ? object(value) : type === 'array' ? Array.isArray(value) : type === 'integer' ? typeof value === 'number' && Number.isInteger(value) : typeof value === type)

export function valueBounds(value: Json, limit = TREE_LIMITS.bytes): Record<string, string> {
  const errors: Record<string, string> = {}
  let items = 0; let properties = 0
  const visit = (v: Json, path: string, depth: number) => {
    if (depth > TREE_LIMITS.depth) { errors[path] = 'Response exceeds depth 32.'; return }
    if (Array.isArray(v)) { items += v.length; if (items > TREE_LIMITS.items) { errors[path] = 'Response exceeds 4096 array items.'; return }; v.forEach((child, i) => visit(child, pointer(path, i), depth + 1)) }
    else if (object(v)) { properties += Object.keys(v).length; if (properties > TREE_LIMITS.properties) { errors[path] = 'Response exceeds 1024 properties.'; return }; Object.entries(v).forEach(([k, child]) => visit(child, pointer(path, k), depth + 1)) }
    else if (typeof v === 'string' && (bytes(v) > TREE_LIMITS.stringBytes || [...v].some((c) => { const n = c.codePointAt(0)!; return n >= 0xd800 && n <= 0xdfff }))) errors[path] = 'Text exceeds its UTF-8 limit or contains invalid Unicode.'
    else if (typeof v === 'number' && (!Number.isFinite(v) || Math.abs(v) > Number.MAX_SAFE_INTEGER)) errors[path] = 'Use a finite interoperable JSON number.'
  }
  visit(value, '', 0)
  if (bytes(JSON.stringify(value)) > limit) errors[''] = `Response exceeds ${limit} UTF-8 bytes.`
  return errors
}

export function validateTree(node: TreeSchema, value: Json): Record<string, string> {
  const errors = valueBounds(value)
  const visit = (n: TreeSchema, v: Json, path: string, depth: number) => {
    if (depth > TREE_LIMITS.depth) return
    const s = n.schema
    const fail = (message: string) => { errors[path] ??= message }
    if (!typeMatches(s.type, v)) { fail(`Expected ${String(s.type)}.`); return }
    if (Object.hasOwn(s, 'const') && !equal(s.const, v)) fail('Value must equal the schema constant.')
    if (Array.isArray(s.enum) && !s.enum.some((c) => equal(c, v))) fail('Select a declared enum value.')
    if (n.alternatives.length) { const index = branchIndex(n, v); if (index < 0) fail('Select a schema variant.'); else visit(n.alternatives[index], v, path, depth + 1) }
    if (object(v)) {
      if (Array.isArray(s.required)) for (const key of s.required) if (typeof key === 'string' && !Object.hasOwn(v, key)) errors[pointer(path, key)] = 'This field is required.'
      for (const [k, child] of Object.entries(v)) { const spec = n.properties[k] ?? n.additional; if (spec) visit(spec, child, pointer(path, k), depth + 1); else if (s.additionalProperties === false) errors[pointer(path, k)] = 'This property is not declared by the schema.' }
    }
    if (Array.isArray(v)) {
      if (typeof s.minItems === 'number' && v.length < s.minItems) fail(`Add at least ${s.minItems} items.`)
      if (typeof s.maxItems === 'number' && v.length > s.maxItems) fail(`Use at most ${s.maxItems} items.`)
      if (s.uniqueItems === true && new Set(v.map((item) => JSON.stringify(sorted(item)))).size !== v.length) fail('Array items must be unique.')
      if (n.items) v.forEach((item, i) => visit(n.items!, item, pointer(path, i), depth + 1))
    }
    if (typeof v === 'string') {
      if (typeof s.minLength === 'number' && [...v].length < s.minLength) fail(`Use at least ${s.minLength} characters.`)
      if (typeof s.maxLength === 'number' && [...v].length > s.maxLength) fail(`Use at most ${s.maxLength} characters.`)
      if (typeof s['x-platform-max-bytes'] === 'number' && bytes(v) > s['x-platform-max-bytes']) fail(`Text exceeds ${s['x-platform-max-bytes']} UTF-8 bytes.`)
      // format/pattern are deliberately left to the server's exact nominal/profile validator.
    }
    if (typeof v === 'number') {
      if (typeof s.minimum === 'number' && v < s.minimum) fail(`Use a value of at least ${s.minimum}.`)
      if (typeof s.maximum === 'number' && v > s.maximum) fail(`Use a value of at most ${s.maximum}.`)
      if (typeof s.exclusiveMinimum === 'number' && v <= s.exclusiveMinimum) fail(`Use a value greater than ${s.exclusiveMinimum}.`)
      if (typeof s.exclusiveMaximum === 'number' && v >= s.exclusiveMaximum) fail(`Use a value less than ${s.exclusiveMaximum}.`)
      // Decimal multipleOf uses the owning validator, avoiding floating point false rejections.
    }
  }
  visit(node, value, '', 0)
  return errors
}

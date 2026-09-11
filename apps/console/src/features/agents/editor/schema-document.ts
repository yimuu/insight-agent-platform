import { parseDocument } from 'yaml'
export type SchemaObject = Record<string, unknown>
export const isObject = (value: unknown): value is SchemaObject =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
export function parseSchema(source: string): SchemaObject {
  if (new TextEncoder().encode(source).byteLength > 1_048_576)
    throw new Error('Schema 超过 1 MiB 限制。')
  const value: unknown = JSON.parse(source)
  if (!isObject(value) || parseDocument(source, { uniqueKeys: true }).errors.length)
    throw new Error('Schema 必须是无重复字段的 JSON 对象。')
  return value
}
export function fieldTableSupported(schema: SchemaObject): boolean {
  return (
    schema.type === 'object' &&
    isObject(schema.properties) &&
    !['$ref', 'oneOf', 'anyOf', 'allOf', 'if', 'dependentSchemas', 'patternProperties'].some(
      (key) => key in schema,
    )
  )
}
export function changeField(
  schema: SchemaObject,
  name: string,
  nextName: string,
  field: SchemaObject | null,
  required: boolean,
): SchemaObject {
  if (!fieldTableSupported(schema)) throw new Error('此 Schema 请使用高级编辑。')
  const properties = schema.properties as SchemaObject
  if (field && (!nextName.trim() || (name !== nextName && Object.hasOwn(properties, nextName))))
    throw new Error('字段名称不能为空或重复。')
  const entries = Object.entries(properties).filter(([key]) => key !== name)
  if (field) entries.push([nextName, field])
  const requiredFields = Array.isArray(schema.required)
    ? schema.required.filter((key) => key !== name)
    : []
  if (field && required) requiredFields.push(nextName)
  // Clone only the edited paths: descriptions, extensions and unknown constraints remain intact.
  return { ...schema, properties: Object.fromEntries(entries), required: requiredFields }
}
export function newField(type = 'string'): SchemaObject {
  if (type === 'object') return { type, properties: {}, required: [], additionalProperties: false }
  if (type === 'array') return { type, items: newField(), minItems: 0, maxItems: 16 }
  if (type === 'string')
    return { type, minLength: 0, maxLength: 1024, 'x-platform-max-bytes': 4096 }
  if (type === 'number' || type === 'integer')
    return { type, minimum: -1_000_000, maximum: 1_000_000 }
  return { type }
}

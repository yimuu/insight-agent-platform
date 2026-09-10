export function id(value: unknown, prefix: string): value is string {
  return (
    typeof value === 'string' &&
    new RegExp(
      `^${prefix}_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
    ).test(value)
  )
}
export function uuid4(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)
  )
}
export function positive(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0
}
export function sha(value: unknown): value is string {
  return typeof value === 'string' && /^sha256:[0-9a-f]{64}$/.test(value)
}
export function closed(value: unknown, fields: string[]): boolean {
  return (
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.keys(value).length === fields.length &&
    fields.every((field) => Object.hasOwn(value, field))
  )
}
export function exactModel(value: unknown): boolean {
  if (!closed(value, ['deployment_id', 'resource_kind', 'deployment_digest'])) return false
  const model = value as Record<string, unknown>
  return (
    model.resource_kind === 'model_deployment' &&
    id(model.deployment_id, 'mdep') &&
    sha(model.deployment_digest)
  )
}

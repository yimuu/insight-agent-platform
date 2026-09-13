import { constants, closeSync, fstatSync, openSync, readFileSync } from 'node:fs'
import { createPrivateKey } from 'node:crypto'
import { isAbsolute } from 'node:path'

export interface LocalIdentityConfigV1 {
  schema_version: 1
  listen_host: string
  listen_port: number
  public_origin: string
  database_url: string
  principal_id: string
  issuer_key_pem: string
  session: {
    schema_version: 1
    issuer: string
    audience: 'insight.platform/v1'
    key_id: string
    tenant_id: string
    subject: string
    principal_kind: 'tenant_admin'
  }
}

function invalid(): never {
  throw new Error('Invalid local identity configuration')
}
function fields(value: unknown, names: string[]): value is Record<string, unknown> {
  return (
    !!value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.keys(value).length === names.length &&
    Object.keys(value).every((key) => names.includes(key))
  )
}
export function checkedIdentityConfig(value: unknown): LocalIdentityConfigV1 {
  if (
    !fields(value, [
      'schema_version',
      'listen_host',
      'listen_port',
      'public_origin',
      'database_url',
      'principal_id',
      'issuer_key_pem',
      'session',
    ])
  )
    invalid()
  if (
    value.schema_version !== 1 ||
    !['127.0.0.1', '0.0.0.0'].includes(String(value.listen_host)) ||
    !Number.isInteger(value.listen_port) ||
    Number(value.listen_port) < 1024 ||
    Number(value.listen_port) > 65535
  )
    invalid()
  for (const key of ['public_origin', 'database_url', 'principal_id', 'issuer_key_pem']) {
    if (typeof value[key] !== 'string' || !value[key] || value[key].length > 8192) invalid()
  }
  let origin, database
  try {
    origin = new URL(String(value.public_origin))
    database = new URL(String(value.database_url))
  } catch {
    invalid()
  }
  if (
    !['http:', 'https:'].includes(origin.protocol) ||
    origin.origin !== value.public_origin ||
    !['127.0.0.1', 'localhost'].includes(origin.hostname) ||
    !['postgres:', 'postgresql:'].includes(database.protocol) ||
    database.username !== 'insight_local_identity_dev' ||
    !database.password ||
    database.search ||
    database.hash
  )
    invalid()
  const identity = value.session
  if (
    !fields(identity, [
      'schema_version',
      'issuer',
      'audience',
      'key_id',
      'tenant_id',
      'subject',
      'principal_kind',
    ]) ||
    identity.schema_version !== 1 ||
    identity.audience !== 'insight.platform/v1' ||
    identity.principal_kind !== 'tenant_admin'
  )
    invalid()
  for (const key of ['issuer', 'key_id', 'tenant_id', 'subject']) {
    if (typeof identity[key] !== 'string' || !identity[key] || identity[key].length > 2048)
      invalid()
  }
  if (
    !/^prn_[0-9a-f-]{36}$/.test(String(value.principal_id)) ||
    !/^ten_[0-9a-f-]{36}$/.test(String(identity.tenant_id))
  )
    invalid()
  try {
    if (createPrivateKey(String(value.issuer_key_pem)).asymmetricKeyType !== 'rsa') invalid()
  } catch {
    invalid()
  }
  return value as unknown as LocalIdentityConfigV1
}
export function loadIdentityConfig(path: string): LocalIdentityConfigV1 {
  if (!isAbsolute(path)) invalid()
  const descriptor = openSync(
    path,
    constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
  )
  try {
    const metadata = fstatSync(descriptor)
    if (!metadata.isFile() || metadata.size > 32768 || (metadata.mode & 0o077) !== 0) invalid()
    return checkedIdentityConfig(JSON.parse(readFileSync(descriptor, 'utf8')))
  } finally {
    closeSync(descriptor)
  }
}

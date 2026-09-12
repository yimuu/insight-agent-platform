import { createHash, randomBytes, randomUUID, scrypt, sign, timingSafeEqual } from 'node:crypto'
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { Pool, type PoolClient } from 'pg'
import type { LocalIdentityConfigV1 } from './identity-config.ts'
import { identitySchemaInventory, verifyIdentitySchema } from './identity-schema.ts'

const cookieName = 'insight_session'
const lifetimeSeconds = 8 * 60 * 60
const prefix = '/_console/v1/auth/'
const maximumBody = 4096
class Rejected extends Error {
  readonly status: number
  readonly code: string
  constructor(status: number, code: string) {
    super(code)
    this.status = status
    this.code = code
  }
}
function digest(value: string) {
  return createHash('sha256').update(value).digest('hex')
}
function sessionToken(request: IncomingMessage): string | null {
  const values = (request.headers.cookie ?? '')
    .split(';')
    .map((value) => value.trim())
    .filter((value) => value.startsWith(`${cookieName}=`))
  if (values.length !== 1) return null
  const value = values[0].slice(cookieName.length + 1)
  return /^[A-Za-z0-9_-]{43}$/.test(value) ? value : null
}
function reply(response: ServerResponse, status: number, value?: unknown) {
  response.writeHead(status, {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store',
    'X-Content-Type-Options': 'nosniff',
  })
  response.end(value === undefined ? undefined : JSON.stringify(value))
}
function setCookie(
  response: ServerResponse,
  origin: string,
  token: string,
  seconds = lifetimeSeconds,
) {
  response.setHeader(
    'Set-Cookie',
    `${cookieName}=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${seconds}${origin.startsWith('https:') ? '; Secure' : ''}`,
  )
}
async function body(request: IncomingMessage): Promise<Record<string, unknown>> {
  if (request.headers['content-type']?.split(';')[0].trim() !== 'application/json')
    throw new Rejected(400, 'invalid_input')
  const parts: Buffer[] = []
  let length = 0
  for await (const part of request) {
    length += part.length
    if (length > maximumBody) throw new Rejected(413, 'request_too_large')
    parts.push(part)
  }
  let value
  try {
    value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(parts)))
  } catch {
    throw new Rejected(400, 'invalid_input')
  }
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Rejected(400, 'invalid_input')
  return value
}
function credentials(input: Record<string, unknown>, setup: boolean) {
  const fields = setup
    ? ['schema_version', 'email', 'password', 'display_name']
    : ['schema_version', 'email', 'password']
  if (
    input.schema_version !== 1 ||
    Object.keys(input).length !== fields.length ||
    Object.keys(input).some((key) => !fields.includes(key)) ||
    typeof input.email !== 'string' ||
    !/^[^\s@]{1,128}@[^\s@]{1,125}$/.test(input.email) ||
    input.email.length > 254 ||
    typeof input.password !== 'string' ||
    input.password.length < 12 ||
    input.password.length > 256 ||
    (setup &&
      (typeof input.display_name !== 'string' ||
        input.display_name.trim().length < 1 ||
        input.display_name.length > 128))
  ) {
    throw new Rejected(400, 'invalid_input')
  }
  return {
    email: input.email.toLowerCase(),
    password: input.password,
    displayName: setup ? String(input.display_name).trim() : '',
  }
}
function derive(password: string, salt: Buffer): Promise<Buffer> {
  return new Promise((resolve, reject) =>
    scrypt(password, salt, 64, { N: 32768, r: 8, p: 1, maxmem: 64 * 1024 * 1024 }, (error, key) =>
      error ? reject(error) : resolve(key),
    ),
  )
}

async function createSession(transaction: PoolClient) {
  // Caller holds the owner row lock; parallel logins cannot exceed the installation's bound.
  await transaction.query(
    'DELETE FROM insight_platform.local_console_sessions WHERE expires_at <= clock_timestamp()',
  )
  await transaction.query(
    'DELETE FROM insight_platform.local_console_sessions WHERE session_digest IN (SELECT session_digest FROM insight_platform.local_console_sessions ORDER BY created_at DESC, session_digest OFFSET 31)',
  )
  const token = randomBytes(32).toString('base64url')
  const result = await transaction.query(
    "INSERT INTO insight_platform.local_console_sessions(session_digest,created_at,expires_at) VALUES ($1,statement_timestamp(),statement_timestamp()+interval '8 hours') RETURNING expires_at",
    [digest(token)],
  )
  return { token, expiresAt: result.rows[0].expires_at as Date }
}
export async function startIdentityServer(
  config: LocalIdentityConfigV1,
  inventory = identitySchemaInventory(),
) {
  const pool = new Pool({
    connectionString: config.database_url,
    max: 4,
    connectionTimeoutMillis: 3000,
    statement_timeout: 5000,
    idleTimeoutMillis: 30000,
  })
  pool.on('error', () => {
    /* Individual requests report the closed unavailable error. */
  })
  try {
    await verifyIdentitySchema(pool, inventory)
  } catch {
    await pool.end()
    throw new Error('Local identity schema unavailable or incompatible')
  }
  let hashing = 0
  let closing = false
  const origins = new Set([config.public_origin])
  const alternate = new URL(config.public_origin)
  alternate.hostname = alternate.hostname === 'localhost' ? '127.0.0.1' : 'localhost'
  origins.add(alternate.origin)
  const current = async (request: IncomingMessage) => {
    const token = sessionToken(request)
    if (!token) return null
    const result = await pool.query(
      'SELECT o.display_name,s.expires_at FROM insight_platform.local_console_sessions s JOIN insight_platform.local_console_owner o ON o.singleton=s.owner WHERE s.session_digest=$1 AND s.expires_at>clock_timestamp() AND o.principal_id=$2',
      [digest(token), config.principal_id],
    )
    return result.rows[0] ?? null
  }
  const handle = async (request: IncomingMessage, response: ServerResponse) => {
    if (closing) throw new Rejected(503, 'temporarily_unavailable')
    if (request.url === '/health/ready' && request.method === 'GET') {
      await pool.query('SELECT 1')
      reply(response, 200, { ready: true })
      return
    }
    if (request.url === '/internal/token' && request.method === 'GET') {
      const session = await current(request)
      if (!session) throw new Rejected(401, 'authentication_required')
      const now = Math.floor(Date.now() / 1000)
      const header = Buffer.from(
        JSON.stringify({ alg: 'RS256', typ: 'JWT', kid: config.session.key_id }),
      ).toString('base64url')
      const payload = Buffer.from(
        JSON.stringify({
          iss: config.session.issuer,
          aud: config.session.audience,
          sub: config.session.subject,
          jti: `browser-${randomUUID()}`,
          iat: now,
          exp: Math.min(now + 900, Math.floor(new Date(session.expires_at).getTime() / 1000)),
          tenant_id: config.session.tenant_id,
          principal_kind: config.session.principal_kind,
          authn_strength: 'single_factor',
        }),
      ).toString('base64url')
      const message = `${header}.${payload}`
      reply(response, 200, {
        access_token: `${message}.${sign('RSA-SHA256', Buffer.from(message), config.issuer_key_pem).toString('base64url')}`,
      })
      return
    }
    if (request.url === `${prefix}session` && request.method === 'GET') {
      const owner = await pool.query(
        'SELECT principal_id FROM insight_platform.local_console_owner WHERE singleton',
      )
      if (owner.rows.length && owner.rows[0].principal_id !== config.principal_id)
        throw new Rejected(503, 'identity_mismatch')
      const session = await current(request)
      reply(response, 200, {
        schema_version: 1,
        authentication: 'local_owner',
        setup_required: owner.rows.length === 0,
        authenticated: !!session,
        display_name: session?.display_name ?? null,
        expires_at: session ? new Date(session.expires_at).toISOString() : null,
      })
      return
    }
    if (
      request.method !== 'POST' ||
      ![`${prefix}setup`, `${prefix}login`, `${prefix}logout`].includes(request.url ?? '')
    )
      throw new Rejected(404, 'not_found')
    if (!origins.has(request.headers.origin ?? '')) throw new Rejected(403, 'origin_rejected')
    if (request.url === `${prefix}logout`) {
      const token = sessionToken(request)
      if (token)
        await pool.query(
          'DELETE FROM insight_platform.local_console_sessions WHERE session_digest=$1',
          [digest(token)],
        )
      setCookie(response, config.public_origin, '', 0)
      reply(response, 204)
      return
    }
    const setup = request.url === `${prefix}setup`
    const input = credentials(await body(request), setup)
    if (hashing >= 2) throw new Rejected(429, 'capacity_exhausted')
    hashing++
    let transaction: PoolClient | undefined
    try {
      transaction = await pool.connect()
      await transaction.query('BEGIN')
      if (setup) {
        const salt = randomBytes(32)
        const key = await derive(input.password, salt)
        try {
          const result = await transaction.query(
            'INSERT INTO insight_platform.local_console_owner(singleton,principal_id,email,display_name,password_salt,password_hash) VALUES (true,$1,$2,$3,$4,$5) ON CONFLICT (singleton) DO NOTHING RETURNING singleton',
            [config.principal_id, input.email, input.displayName, salt, key],
          )
          if (!result.rowCount) throw new Rejected(409, 'owner_already_configured')
        } finally {
          key.fill(0)
        }
      } else {
        const result = await transaction.query(
          'SELECT *,clock_timestamp() AS now FROM insight_platform.local_console_owner WHERE singleton FOR UPDATE',
        )
        const owner = result.rows[0]
        if (!owner || owner.principal_id !== config.principal_id)
          throw new Rejected(401, 'invalid_credentials')
        if (owner.locked_until && owner.locked_until > owner.now)
          throw new Rejected(429, 'login_temporarily_locked')
        const key = await derive(input.password, owner.password_salt)
        let valid
        try {
          valid = timingSafeEqual(key, owner.password_hash) && input.email === owner.email
        } finally {
          key.fill(0)
        }
        if (!valid) {
          const attempts = owner.locked_until ? 1 : owner.failed_attempts + 1
          await transaction.query(
            "UPDATE insight_platform.local_console_owner SET failed_attempts=$1,locked_until=CASE WHEN $1>=10 THEN clock_timestamp()+interval '5 minutes' ELSE NULL END WHERE singleton",
            [attempts],
          )
          await transaction.query('COMMIT')
          throw new Rejected(401, 'invalid_credentials')
        }
        await transaction.query(
          'UPDATE insight_platform.local_console_owner SET failed_attempts=0,locked_until=NULL WHERE singleton',
        )
      }
      const session = await createSession(transaction)
      await transaction.query('COMMIT')
      setCookie(response, config.public_origin, session.token)
      reply(response, setup ? 201 : 200, { schema_version: 1 })
    } catch (error) {
      await transaction?.query('ROLLBACK').catch(() => {})
      throw error
    } finally {
      transaction?.release()
      hashing--
    }
  }
  const server = createServer(
    { maxHeaderSize: 8192, requestTimeout: 15000, headersTimeout: 10000 },
    (request, response) => {
      void handle(request, response).catch((error) => {
        if (!response.headersSent)
          reply(response, error instanceof Rejected ? error.status : 503, {
            code: error instanceof Rejected ? error.code : 'temporarily_unavailable',
          })
        else response.destroy()
      })
    },
  )
  server.maxConnections = 64
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(config.listen_port, config.listen_host, resolve)
  })
  return {
    origin: `http://${config.listen_host}:${config.listen_port}`,
    close: async () => {
      closing = true
      await new Promise<void>((resolve) => {
        server.close(() => resolve())
        server.closeIdleConnections()
      })
      await pool.end()
    },
  }
}

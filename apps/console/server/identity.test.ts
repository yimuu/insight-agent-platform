import test from 'node:test'
import assert from 'node:assert/strict'
import { generateKeyPairSync, createPublicKey, verify } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { Pool } from 'pg'
import { startIdentityServer } from './identity-server.ts'
import { checkedIdentityConfig } from './identity-config.ts'

const databaseUrl = process.env.INSIGHT_IDENTITY_TEST_DATABASE_URL
test(
  'local identity persists sessions, arbitrates setup, throttles login and has no business DML',
  { skip: !databaseUrl },
  async () => {
    const inventory = JSON.parse(
      readFileSync(
        new URL(
          '../../../crates/adapters/platform-postgres/schema-inventory.json',
          import.meta.url,
        ),
        'utf8',
      ),
    )
    const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 })
    const config = checkedIdentityConfig({
      schema_version: 1,
      listen_host: '127.0.0.1',
      listen_port: 43271,
      public_origin: 'http://127.0.0.1:8088',
      database_url: databaseUrl,
      principal_id: 'prn_0198f1c3-8f49-7c3e-b1f3-773c28367b90',
      issuer_key_pem: privateKey.export({ type: 'pkcs8', format: 'pem' }),
      session: {
        schema_version: 1,
        issuer: 'https://local.insight.platform/test',
        audience: 'insight.platform/v1',
        key_id: 'test',
        tenant_id: 'ten_0198f1c3-8f49-7c3e-b1f3-773c28367b90',
        subject: 'administrator:test',
        principal_kind: 'tenant_admin',
      },
    })
    const pool = new Pool({ connectionString: databaseUrl, max: 1 })
    let server = await startIdentityServer(config, inventory)
    const auth = async (
      action: string,
      input?: unknown,
      cookie?: string,
      origin = config.public_origin,
    ) =>
      fetch(`${server.origin}${action.startsWith('/') ? action : '/_console/v1/auth/' + action}`, {
        method: input === undefined ? 'GET' : 'POST',
        headers: {
          origin,
          ...(input ? { 'Content-Type': 'application/json' } : {}),
          ...(cookie ? { cookie } : {}),
        },
        body: input === undefined ? undefined : JSON.stringify(input),
        redirect: 'error',
      })
    const input = {
      schema_version: 1,
      email: 'owner@example.test',
      password: 'a-long-test-password',
      display_name: '测试管理员',
    }
    const login = { schema_version: 1, email: input.email, password: input.password }
    const cookieOf = (response: Response) => {
      const cookie = response.headers.get('set-cookie')!
      assert.match(cookie, /HttpOnly/)
      assert.match(cookie, /SameSite=Strict/)
      assert.match(cookie, /Max-Age=28800/)
      assert.ok(!cookie.includes('eyJ'))
      return cookie.split(';')[0]
    }
    try {
      assert.equal(
        (await (await auth('session')).json()).setup_required,
        true,
        'use a freshly provisioned identity test database',
      )
      assert.equal((await auth('setup', input, undefined, 'https://foreign.example')).status, 403)
      const setup = await Promise.all([auth('setup', input), auth('setup', input)])
      assert.deepEqual(setup.map((response) => response.status).sort(), [201, 409])
      const initial = cookieOf(setup.find((response) => response.status === 201)!)
      const session = await (await auth('session', undefined, initial)).json()
      assert.equal(session.authenticated, true)
      assert.equal(session.display_name, input.display_name)
      assert.equal('access_token' in session, false)
      const assertion = await (await auth('/internal/token', undefined, initial)).json()
      const [header, claims, signature] = assertion.access_token.split('.')
      assert.ok(
        verify(
          'RSA-SHA256',
          Buffer.from(`${header}.${claims}`),
          createPublicKey(privateKey),
          Buffer.from(signature, 'base64url'),
        ),
      )
      const decoded = JSON.parse(Buffer.from(claims, 'base64url').toString())
      assert.equal(decoded.sub, config.session.subject)
      assert.equal(decoded.tenant_id, config.session.tenant_id)
      assert.ok(decoded.exp - decoded.iat <= 900)
      for (let index = 0; index < 10; index++)
        assert.equal(
          (await auth('login', { ...login, password: 'incorrect-long-password' })).status,
          401,
        )
      assert.equal((await auth('login', login)).status, 429)
      const owner = (
        await pool.query(
          'SELECT failed_attempts,password_hash,password_salt FROM insight_platform.local_console_owner',
        )
      ).rows[0]
      assert.equal(owner.failed_attempts, 10)
      assert.equal(owner.password_hash.length, 64)
      assert.equal(owner.password_salt.length, 32)
      assert.ok(!owner.password_hash.includes(Buffer.from(input.password)))
      await pool.query(
        "UPDATE insight_platform.local_console_owner SET locked_until=clock_timestamp()-interval '1 second'",
      )
      let last = ''
      for (let index = 0; index < 33; index++) {
        const response = await auth('login', login)
        assert.equal(response.status, 200)
        last = cookieOf(response)
      }
      assert.equal(
        Number(
          (await pool.query('SELECT count(*) FROM insight_platform.local_console_sessions')).rows[0]
            .count,
        ),
        32,
      )
      assert.equal((await auth('/internal/token', undefined, initial)).status, 401)
      await server.close()
      server = await startIdentityServer(config, inventory)
      assert.equal((await (await auth('session', undefined, last)).json()).authenticated, true)
      assert.equal((await auth('logout', {}, last)).status, 204)
      assert.equal((await auth('/internal/token', undefined, last)).status, 401)
      await assert.rejects(
        pool.query('SELECT * FROM insight_platform.principals'),
        /permission denied/,
      )
      await assert.rejects(
        pool.query('CREATE TABLE insight_platform.unauthorized(value text)'),
        /permission denied/,
      )
      await assert.rejects(
        pool.query('DELETE FROM insight_platform.local_console_owner'),
        /permission denied/,
      )
      const corrupted = structuredClone(inventory)
      corrupted.constraints = corrupted.constraints.filter(
        (row: { table: string; name: string }) =>
          !(row.table === 'local_console_owner' && row.name === 'local_console_owner_pkey'),
      )
      await assert.rejects(
        startIdentityServer({ ...config, listen_port: 43272 }, corrupted),
        /schema/,
      )
    } finally {
      await server.close()
      await pool.end()
    }
  },
)

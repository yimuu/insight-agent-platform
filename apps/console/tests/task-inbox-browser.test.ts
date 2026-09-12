import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { createServer } from 'node:http'
import test from 'node:test'
import { browserAvailable, eventually, withConsoleBrowser } from './browser-test-client.ts'

const taskId = (suffix) => `int_0198f1c3-8f49-7c3e-b1f3-773c28367b${suffix}`
const editable = taskId('90')
const oauth = taskId('91')
const mismatched = taskId('92')
const slow = taskId('93')
const approval = 'apv_0198f1c3-8f49-7c3e-b1f3-773c28367b94'
const schema = {
  $schema: 'https://json-schema.org/draft/2020-12/schema',
  type: 'object',
  additionalProperties: false,
  properties: {
    message: {
      title: 'Message',
      type: 'string',
      minLength: 1,
      maxLength: 128,
      'x-platform-max-bytes': 512,
    },
    approved: { title: 'Confirmed', type: 'boolean' },
  },
  required: ['message', 'approved'],
}
const canonical = (value) =>
  Array.isArray(value)
    ? `[${value.map(canonical).join(',')}]`
    : value && typeof value === 'object'
      ? `{${Object.keys(value)
          .sort()
          .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
          .join(',')}}`
      : JSON.stringify(value)
const digest = `sha256:${createHash('sha256').update(canonical(schema)).digest('hex')}`
const envelope = {
  schema_version: 1,
  profile: 'insight.closed-json-schema/1',
  schema,
  canonical_digest: digest,
}

test(
  'Task inbox/form browser: empty authorized pages, frozen binding, exact submissions, conflicts and identity fencing',
  { skip: !browserAvailable, timeout: 45_000 },
  async () => {
    const states = new Map()
    const requests = []
    const mutations = []
    let behavior = 'retryable'
    let cursorExpired = false
    let denied = false
    let releaseSlow
    const view = (id) => ({
      schema_version: 2,
      allowed_actions:
        states.get(id)?.state && states.get(id).state !== 'pending'
          ? []
          : id === oauth
            ? []
            : id === approval
              ? ['approve', 'reject']
              : ['submit_input', 'cancel'],
      task_id: id,
      task_kind:
        id === approval ? 'approval' : id === oauth ? 'external_authorization' : 'human_work',
      state: states.get(id)?.state ?? 'pending',
      generation: 1,
      version: states.get(id)?.version ?? 1,
      safe_prompt_key:
        id === editable
          ? 'editable-task-private-canary'
          : id === slow
            ? 'slow-task-private-canary'
            : id === approval
              ? 'approval'
              : id === oauth
                ? 'oauth'
                : 'mismatch',
      response_schema_digest: id === approval || id === oauth ? null : digest,
      owner: { kind: 'run', run_id: 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b95' },
      deadline: '2030-01-01T00:00:00.000000Z',
      responded_at: null,
      created_at: '2026-09-01T00:00:00.000000Z',
      updated_at: '2026-09-01T00:00:00.000000Z',
      etag: `"${id}-${states.get(id)?.version ?? 1}"`,
    })
    const form = (id) => ({
      schema_version: 2,
      allowed_actions: view(id).allowed_actions,
      etag: view(id).etag,
      safe_prompt_key: view(id).safe_prompt_key,
      task_id: id,
      generation: 1,
      version: (states.get(id)?.version ?? 1) + (id === mismatched ? 1 : 0),
      response_schema: envelope,
      response_schema_digest: digest,
    })
    const send = (response, status, data) => {
      response.writeHead(status, {
        'content-type': 'application/json',
        'cache-control': 'no-store',
        ...(data.etag ? { etag: data.etag } : {}),
      })
      response.end(JSON.stringify(data))
    }
    const problem = (response, status, code, detail, retryable = false) =>
      send(response, status, { code, detail, retryable })
    const api = createServer(async (request, response) => {
      if (request.url.startsWith('/v1/agents?')) {
        response.setHeader('content-type', 'application/json')
        response.end(JSON.stringify({ schema_version: 1, items: [], next_cursor: null }))
        return
      }
      const url = new URL(request.url, 'http://fixture')
      requests.push({
        path: url.pathname,
        query: Object.fromEntries(url.searchParams),
        auth: request.headers.authorization,
      })
      if (url.pathname === '/readyz') {
        response.end('ready')
        return
      }
      if (url.pathname === '/v1/tasks') {
        if (denied) return problem(response, 403, 'permission_denied', 'Task access was revoked.')
        if (request.headers.authorization === 'Bearer actor-two-fixture')
          return send(response, 200, { schema_version: 1, items: [], next_cursor: null })
        if (!url.searchParams.has('cursor'))
          return send(response, 200, {
            schema_version: 1,
            items: [],
            next_cursor: 'opaque+/next==',
          })
        if (cursorExpired)
          return problem(response, 400, 'cursor_expired', 'Task cursor expired. Refresh the inbox.')
        return send(response, 200, {
          schema_version: 1,
          items: [editable, oauth, approval, mismatched, slow].map(view),
          next_cursor: null,
        })
      }
      const path = url.pathname.slice('/v1/tasks/'.length)
      if (request.method === 'GET' && path.endsWith('/form')) {
        const id = path.slice(0, -5)
        if (id === oauth)
          return problem(
            response,
            409,
            'task_form_unavailable',
            'This Task has no frozen response form.',
          )
        if (id === approval)
          return problem(
            response,
            403,
            'permission_denied',
            'Approval-only principal cannot read form content.',
          )
        if (id === slow) {
          releaseSlow = () => send(response, 200, form(id))
          return
        }
        return send(response, 200, form(id))
      }
      if (request.method === 'GET' && [editable, oauth, approval, mismatched, slow].includes(path))
        return send(
          response,
          200,
          url.searchParams.get('purpose') === 'viewable'
            ? { ...view(path), allowed_actions: [] }
            : view(path),
        )
      if (request.method === 'POST') {
        const [id, action] = path.split(':')
        const chunks = []
        for await (const chunk of request) chunks.push(chunk)
        const raw = Buffer.concat(chunks).toString()
        mutations.push({
          id,
          action,
          etag: request.headers['if-match'],
          receipt: request.headers['idempotency-key'],
          body: raw ? JSON.parse(raw) : null,
        })
        if (action === 'submit-input' && behavior === 'retryable')
          return problem(response, 503, 'service_unavailable', 'Retry this response later.', true)
        if (action === 'submit-input' && behavior === 'conflict') {
          states.set(id, { state: 'responded', version: 2 })
          return problem(response, 409, 'invalid_state_transition', 'Another response won.')
        }
        states.set(id, {
          state:
            action === 'approve' ? 'approved' : action === 'cancel' ? 'cancelled' : 'responded',
          version: (states.get(id)?.version ?? 1) + 1,
        })
        return send(response, 200, view(id))
      }
      problem(response, 404, 'not_found', 'Fixture resource unavailable.')
    })
    await withConsoleBrowser(api, async (browser) => {
      const text = () => browser.evaluate('document.body.innerText')
      const open = async (id) => {
        if (
          await browser.evaluate(
            `!![...document.querySelectorAll('button')].find(node => node.textContent.includes('返回待办任务'))`,
          )
        )
          await browser.click('← 返回待办任务')
        await browser.evaluate(
          `document.querySelector('summary') && [...document.querySelectorAll('summary')].find(node => node.textContent === '通过任务 ID 查找')?.click()`,
        )
        await browser.field('任务 ID', id)
        await browser.click('打开')
      }
      await browser.connect('actor-one-fixture')
      await browser.evaluate(
        `[...document.querySelectorAll('nav a')].find(node => node.textContent.includes('待办任务')).click()`,
      )
      await browser.wait(
        `document.body.innerText.includes('本页没有可见任务')`,
        'empty page with continuation',
      )
      await browser.click('下一页任务')
      await browser.wait(
        `document.body.innerText.includes('editable-task-private-canary')`,
        'next authorized page',
      )
      assert.equal(requests.find((entry) => entry.query.cursor)?.query.cursor, 'opaque+/next==')
      await browser.click('打开 editable-task-private-canary')
      await browser.wait(
        `!!document.querySelector('[data-ui~=task-schema-form] textarea')`,
        'frozen editable form',
      )
      await browser.field('Message（必填）', 'response-private-canary')
      await browser.field('Confirmed（必填）', 'false')
      await browser.field('回复数据分类', 'confidential')
      await browser.click('提交回复')
      await browser.wait(
        `document.body.innerText.includes('Retry this response later.') && !document.querySelector('[data-ui~=task-schema-form] button[type="submit"]').disabled`,
        'retryable failure',
      )
      assert.deepEqual(mutations[0].body, {
        classification: 'confidential',
        schema_digest: digest,
        value: { kind: 'inline', value: { message: 'response-private-canary', approved: false } },
      })
      assert.equal(mutations[0].etag, `"${editable}-1"`)
      behavior = 'conflict'
      await browser.click('提交回复')
      await browser.wait(
        `document.body.innerText.includes('Another response won.') && document.body.innerText.includes('已重新读取任务')`,
        'conflict refresh',
      )
      assert.equal(mutations.length, 2)
      assert.equal(mutations[0].receipt, mutations[1].receipt)
      assert.equal(
        await browser.evaluate(`document.querySelector('[data-ui~=task-schema-form]') === null`),
        true,
      )
      assert.match(await text(), /已回复/)

      states.set(editable, { state: 'pending', version: 3 })
      behavior = 'accept'
      await open(editable)
      await browser.wait(
        `!!document.querySelector('[data-ui~=task-schema-form] textarea')`,
        'new generation form',
      )
      assert.equal(
        await browser.evaluate(
          `document.querySelector('[data-ui~=task-schema-form] textarea').value`,
        ),
        '',
      )
      await browser.field('Message（必填）', 'fresh-response')
      await browser.field('Confirmed（必填）', 'true')
      await browser.click('提交回复')
      await browser.wait(
        `document.body.innerText.includes('操作已提交。')`,
        'accepted typed response',
      )
      assert.equal(mutations.length, 3)
      assert.equal(mutations[2].etag, `"${editable}-3"`)
      assert.notEqual(mutations[2].receipt, mutations[0].receipt)

      await open(oauth)
      await browser.wait(
        `document.body.innerText.includes('授权流程')`,
        'OAuth uses its authorization flow',
      )
      assert.equal(await browser.evaluate('document.querySelectorAll("textarea").length'), 0)
      assert.equal(
        requests.some((request) => request.path === `/v1/tasks/${oauth}/form`),
        false,
      )
      assert.equal(
        await browser.evaluate(
          `[...document.querySelectorAll('button')].some(button => button.textContent === '取消')`,
        ),
        false,
      )
      await open(approval)
      await browser.wait(
        `document.body.innerText.includes('请审阅此审批请求并选择操作。')`,
        'approval without response form',
      )
      assert.equal(
        requests.some((request) => request.path === `/v1/tasks/${approval}/form`),
        false,
      )
      await browser.click('批准')
      await browser.wait(
        `document.body.innerText.includes('操作已提交。')`,
        'approval action preserved',
      )
      assert.equal(mutations.at(-1).body, null)
      await open(mismatched)
      await browser.wait(
        `document.body.innerText.includes('加载表单时任务已变更')`,
        'mismatched frozen version rejected',
      )
      assert.equal(await browser.evaluate('document.querySelectorAll("textarea").length'), 0)

      await browser.click('← 返回待办任务')
      await browser.field('任务范围', 'viewable')
      await browser.click('应用筛选')
      await open(editable)
      await browser.wait(
        `document.body.innerText.includes('editable-task-private-canary')`,
        'safe viewable metadata',
      )
      assert.equal(
        await browser.evaluate(`document.querySelector('[data-ui~=task-schema-form]') === null`),
        true,
      )
      assert.equal(
        await browser.evaluate(
          `[...document.querySelectorAll('button')].some(button => ['批准','拒绝','取消'].includes(button.textContent))`,
        ),
        false,
      )
      assert.ok(
        requests.some(
          (entry) => entry.path === `/v1/tasks/${editable}` && entry.query.purpose === 'viewable',
        ),
      )
      await browser.click('← 返回待办任务')
      await browser.field('任务范围', 'respondable')
      await browser.click('应用筛选')
      await open(slow)
      await eventually(() => releaseSlow, 'pending old Task form')
      denied = true
      // Returning to the list cancels the pending detail read; a fresh list read
      // must still observe revocation and must not revive that late form.
      await browser.click('← 返回待办任务')
      await browser.click('刷新任务')
      await browser.wait(
        `document.body.innerText.includes('Task access was revoked.')`,
        'revocation clears protected state',
      )
      releaseSlow()
      assert.equal((await text()).includes('editable-task-private-canary'), false)
      assert.equal((await text()).includes('slow-task-private-canary'), false)
      assert.equal(await browser.evaluate('document.querySelectorAll("textarea").length'), 0)
      assert.equal(
        await browser.evaluate(
          `document.querySelector('input[placeholder="int_… 或 apv_…"]').value`,
        ),
        '',
      )

      denied = false
      cursorExpired = true
      await browser.click('刷新任务')
      await browser.wait(
        `document.body.innerText.includes('本页没有可见任务')`,
        'explicit list refresh',
      )
      const firstPages = requests.filter(
        (entry) => entry.path === '/v1/tasks' && !entry.query.cursor,
      ).length
      await browser.click('下一页任务')
      await browser.wait(
        `document.body.innerText.includes('Task cursor expired.')`,
        'cursor failure surfaced',
      )
      assert.equal(
        requests.filter((entry) => entry.path === '/v1/tasks' && !entry.query.cursor).length,
        firstPages,
      )
      await browser.field('任务类型', 'human_work')
      await browser.field('关联运行 ID（可选）', 'run_filter')
      await browser.click('应用筛选')
      await eventually(
        () =>
          requests.some(
            (entry) =>
              entry.query.kind === 'human_work' &&
              entry.query.run_id === 'run_filter' &&
              !entry.query.cursor,
          ),
        'filters reset continuation',
      )
      await browser.connect('actor-two-fixture')
      await browser.wait(`document.body.innerText.includes('暂无待办任务')`, 'new identity inbox')
      assert.equal(
        await browser.evaluate(`document.querySelector('input[placeholder="全部运行"]').value`),
        '',
      )
      assert.equal(
        await browser.evaluate(
          `JSON.stringify({...sessionStorage,...localStorage}).includes('response-private-canary')`,
        ),
        false,
      )
    })
  },
)

import { fieldValue } from './browser-fields.ts'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import test from 'node:test'
import { browserAvailable, withConsoleBrowser } from './browser-test-client.ts'
const digest = (c) => `sha256:${c.repeat(64)}`
const resourceId = 'mpr_0198f1c3-8f49-7c3e-b1f3-773c28367b90'
const deployment = {
  deployment_id: 'mdep_0198f1c3-8f49-7c3e-b1f3-773c28367b91',
  resource_kind: 'model_deployment',
  deployment_digest: digest('a'),
}
const policy = {
  deployment: {
    ...deployment,
    deployment_id: 'pdep_0198f1c3-8f49-7c3e-b1f3-773c28367b92',
    resource_kind: 'policy_deployment',
  },
  revision: {
    revision_id: 'prev_0198f1c3-8f49-7c3e-b1f3-773c28367b93',
    resource_kind: 'policy_revision',
    semantic_digest: digest('b'),
  },
}
const selections = {
  schema_version: 1,
  slots: [
    {
      slot_id: 'model',
      requirement_digest: digest('c'),
      interface_contract_digest: null,
      target: {
        kind: 'model',
        candidates: [{ kind: 'active', resource_id: resourceId, environment: 'dev' }],
        selection_policy: policy,
      },
    },
  ],
}
const runId = 'run_0198f1c3-8f49-7c3e-b1f3-773c28367b94'
const valueId = 'val_0198f1c3-8f49-7c3e-b1f3-773c28367b95'
const artifactValueId = 'val_0198f1c3-8f49-7c3e-b1f3-773c28367b96'
const artifactId = 'art_0198f1c3-8f49-7c3e-b1f3-773c28367b97'
const artifactRef = {
  artifact_id: artifactId,
  content_digest: digest('e'),
  byte_length: 5,
  media_type: 'text/plain',
  classification: 'internal',
  display_name: 'authorized-artifact-reference-canary',
}
const value = {
  schema_version: 1,
  run_id: runId,
  value_id: valueId,
  node_id: null,
  classification: 'internal',
  schema_digest: digest('d'),
  content_digest: digest('e'),
  storage_kind: 'inline',
}

test(
  'authoring lookup and Run values use current read authority, empty continuations, explicit content and scope clearing',
  { skip: !browserAvailable, timeout: 35_000 },
  async () => {
    const requests = []
    let deny = false
    let releaseContent
    let signalAttempts = 0
    const send = (response, data, status = 200) => {
      response.writeHead(status, { 'content-type': 'application/json' })
      response.end(JSON.stringify(data))
    }
    const api = createServer(async (request, response) => {
      if (request.url.startsWith('/v1/agents?')) {
        response.setHeader('content-type', 'application/json')
        response.end(JSON.stringify({ schema_version: 1, items: [], next_cursor: null }))
        return
      }
      const url = new URL(request.url, 'http://fixture')
      const chunks = []
      for await (const part of request) chunks.push(part)
      requests.push({
        path: url.pathname,
        query: Object.fromEntries(url.searchParams),
        method: request.method,
        headers: request.headers,
        body: chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : null,
      })
      if (url.pathname === '/readyz') {
        response.end('ready')
        return
      }
      if (url.pathname === '/v1/agent-authoring-dependencies') {
        if (deny)
          return send(
            response,
            {
              code: 'permission_denied',
              detail: 'Authoring lookup permission was revoked.',
              retryable: false,
            },
            403,
          )
        return send(response, {
          schema_version: 1,
          items: url.searchParams.has('cursor')
            ? [
                {
                  schema_version: 1,
                  kind: 'model',
                  resource_id: resourceId,
                  environment: 'dev',
                  deployment,
                  interface_contract_digest: digest('f'),
                  contract_match: null,
                  call_authorized: false,
                },
              ]
            : [],
          next_cursor: url.searchParams.has('cursor') ? null : 'opaque+/discovery==',
        })
      }
      if (url.pathname === '/v1/agent-authoring-bindings:resolve')
        return send(response, {
          schema_version: 1,
          slots: [
            {
              slot_id: 'model',
              resolution: {
                kind: 'resolved',
                binding: {
                  slot_id: 'model',
                  requirement_digest: digest('c'),
                  target: { kind: 'model', candidates: [deployment], selection_policy: policy },
                },
                observed_contract_digests: [digest('f')],
                deployment_features: [],
                contract_match: null,
                call_authorized: false,
              },
            },
          ],
        })
      if (url.pathname === `/v1/runs/${runId}`)
        return send(response, {
          schema_version: 1,
          run_id: runId,
          state: 'running',
          version: 1,
          deadline: '2030-01-01T00:00:00Z',
          etag: `"${runId}-1"`,
        })
      if (url.pathname === `/v1/runs/${runId}/signals/approval`) {
        if (++signalAttempts === 1)
          return send(
            response,
            {
              code: 'capacity_unavailable',
              detail: 'Retry this same signal intent.',
              retryable: true,
            },
            503,
          )
        response.writeHead(204)
        response.end()
        return
      }
      if (url.pathname === `/v1/runs/${runId}/events`) {
        response.writeHead(200, {
          'content-type': 'text/event-stream',
          'x-insight-run-replay-floor': '10',
          'x-insight-run-high-water': '10',
          'x-insight-history-truncated': 'true',
        })
        response.end()
        return
      }
      if (url.pathname === `/v1/runs/${runId}/values`)
        return send(response, {
          schema_version: 1,
          items: url.searchParams.has('cursor')
            ? [value, { ...value, value_id: artifactValueId, storage_kind: 'artifact' }]
            : [],
          next_cursor: url.searchParams.has('cursor') ? null : 'opaque+/values==',
        })
      if (url.pathname === `/v1/runs/${runId}/values/${artifactValueId}/content`)
        return send(response, {
          ...value,
          value_id: artifactValueId,
          storage_kind: 'artifact',
          value: { kind: 'artifact', artifact: artifactRef },
        })
      if (url.pathname === `/v1/artifacts/${artifactId}`)
        return send(
          response,
          {
            code: 'permission_denied',
            detail: 'Artifact body permission was revoked.',
            retryable: false,
          },
          403,
        )
      if (url.pathname === `/v1/runs/${runId}/values/${valueId}/content`) {
        if (request.headers.authorization === 'Bearer wait-content') {
          releaseContent = () =>
            send(response, {
              ...value,
              value: { kind: 'inline', value: { message: 'late-body-canary' } },
            })
          return
        }
        return send(response, {
          ...value,
          value: {
            kind: 'inline',
            value: {
              message: 'explicit-content-canary',
              prompt: 'authorized-prompt-canary',
              tool_output: 'authorized-tool-canary',
            },
          },
        })
      }
      return send(response, { code: 'resource_not_found', retryable: false }, 404)
    })
    await withConsoleBrowser(api, async (browser) => {
      await browser.connect('query-actor')
      await browser.click('创建智能体')
      await browser.field('任务类型', 'full_plan')
      await browser.evaluate(`document.querySelector('[data-ui~=authoring-bindings]').open = true`)
      await browser.field('依赖选择 JSON', JSON.stringify(selections))
      await browser.click('查找部署')
      await browser.wait(
        `document.body.innerText.includes('本页没有可见部署')`,
        'empty discovery with cursor',
      )
      await browser.click('下一页依赖')
      await browser.wait(
        `document.body.innerText.includes(${JSON.stringify(resourceId)})`,
        'discovered deployment',
      )
      assert.match(
        await browser.evaluate('document.body.innerText'),
        /契约匹配： 未检查 · 调用授权： 否/,
      )
      await browser.field('目标依赖槽 ID', 'model')
      await browser.click('使用精确部署')
      const updated = JSON.parse(await browser.evaluate(fieldValue('依赖选择 JSON')))
      assert.equal(updated.slots[0].target.candidates[0].kind, 'exact')
      await browser.click('解析依赖选择')
      await browser.wait(
        `document.body.innerText.includes('请检查契约匹配')`,
        'readonly resolution',
      )
      await browser.click('应用已解析绑定')
      const slots = JSON.parse(await browser.evaluate(fieldValue('精确依赖绑定 JSON')))
      assert.deepEqual(slots[0].target.candidates, [deployment])
      assert.equal(slots[0].binding_digest, undefined)
      const post = requests.find((entry) => entry.method === 'POST')
      assert.equal(post.path, '/v1/agent-authoring-bindings:resolve')
      assert.equal(post.headers['idempotency-key'], undefined)
      assert.equal(post.headers['if-match'], undefined)
      deny = true
      await browser.click('查找部署')
      await browser.wait(
        `document.body.innerText.includes('Authoring lookup permission was revoked.')`,
        'authoring permission cleared',
      )
      assert.deepEqual(JSON.parse(await browser.evaluate(fieldValue('依赖选择 JSON'))).slots, [])
      assert.equal(
        await browser.evaluate(
          `document.querySelectorAll('[data-ui~=authoring-bindings] [data-ui~=nested-panel]').length`,
        ),
        0,
      )
      await browser.evaluate(
        `[...document.querySelectorAll('nav button')].find(node => node.textContent.includes('运行记录')).click()`,
      )
      await browser.field('通过 ID 打开运行', runId)
      await browser.click('打开')
      await browser.wait(
        `document.body.innerText.includes('部分早期历史')`,
        'retention initial page visible',
      )
      await browser.evaluate(
        `[...document.querySelectorAll('summary')].find(node => node.textContent === '高级运行诊断').click()`,
      )
      await browser.evaluate(
        `[...document.querySelectorAll('details')].find(node => node.querySelector('summary')?.textContent === '发送运行信号').open = true`,
      )
      await browser.field('信号名称', 'approval')
      await browser.click('Send signal')
      await browser.wait(
        `document.body.innerText.includes('Retry this same signal intent.')`,
        'retryable signal failure',
      )
      await browser.click('Send signal')
      await browser.wait(
        `document.body.innerText.includes('信号已接受')`,
        'same intent signal accepted',
      )
      const signals = requests.filter((entry) => entry.path.includes('/signals/'))
      assert.equal(signals.length, 2)
      assert.equal(signals[0].headers['idempotency-key'], signals[1].headers['idempotency-key'])
      assert.equal(signals[0].headers['if-match'], undefined)
      assert.deepEqual(signals[0].body, { payload: null })
      assert.ok(
        requests.filter((entry) => entry.path === `/v1/runs/${runId}`).length >= 2,
        'accepted signal refreshes Run authority',
      )
      assert.equal(
        requests.some((entry) => entry.path.includes('/values')),
        false,
      )
      await browser.click('查询运行值')
      await browser.wait(`document.body.innerText.includes('本页没有运行值')`, 'empty value page')
      await browser.click('下一页运行值')
      await browser.wait(
        `document.body.innerText.includes(${JSON.stringify(valueId)})`,
        'value metadata',
      )
      assert.equal(
        requests.some((entry) => entry.path.endsWith('/content')),
        false,
      )
      await browser.click(`读取内容 ${valueId}`)
      await browser.wait(
        `document.body.innerText.includes('explicit-content-canary')`,
        'explicit value content',
      )
      assert.match(await browser.evaluate('document.body.innerText'), /authorized-prompt-canary/)
      assert.match(await browser.evaluate('document.body.innerText'), /authorized-tool-canary/)
      await browser.click('清除显示内容')
      assert.equal(
        (await browser.evaluate('document.body.innerText')).includes('explicit-content-canary'),
        false,
      )
      await browser.click(`读取内容 ${artifactValueId}`)
      await browser.wait(
        `document.body.textContent.includes('authorized-artifact-reference-canary')`,
        'explicit Artifact value reference',
      )
      assert.equal(
        requests.some((entry) => entry.path.startsWith(`/v1/artifacts/${artifactId}`)),
        false,
        'reference does not implicitly download bytes',
      )
      await browser.click('下载授权文件内容')
      await browser.wait(
        `document.body.innerText.includes('Artifact body permission was revoked.')`,
        'current Artifact read denial',
      )
      assert.equal(
        (await browser.evaluate('document.body.innerText')).includes(
          'authorized-artifact-reference-canary',
        ),
        false,
      )
      assert.equal(
        requests.some((entry) => entry.path === `/v1/artifacts/${artifactId}/content`),
        false,
      )
      await browser.field('节点执行 ID', 'nod_changed')
      assert.equal(
        await browser.evaluate(
          `document.querySelector('[data-ui~=run-values]').innerText.includes(${JSON.stringify(valueId)})`,
        ),
        false,
      )
      await browser.connect('wait-content')
      assert.equal(
        await browser.evaluate(fieldValue('信号名称')),
        undefined,
        'subject change removes sensitive signal fields',
      )
      await browser.field('通过 ID 打开运行', runId)
      await browser.click('打开')
      await browser.wait(`!!document.querySelector('[data-ui~=run-values]')`, 'new subject values')
      await browser.evaluate(
        `[...document.querySelectorAll('summary')].find(node => node.textContent === '高级运行诊断').click()`,
      )
      await browser.click('查询运行值')
      await browser.wait(`document.body.innerText.includes('下一页运行值')`, 'new subject page')
      await browser.click('下一页运行值')
      await browser.wait(
        `document.body.innerText.includes(${JSON.stringify(valueId)})`,
        'new subject metadata',
      )
      await browser.click(`读取内容 ${valueId}`)
      await new Promise<void>((resolve) => setTimeout(resolve, 40))
      assert.equal(typeof releaseContent, 'function')
      await browser.connect('other-actor')
      releaseContent()
      assert.equal(
        (await browser.evaluate('document.body.innerText')).includes('late-body-canary'),
        false,
      )
      assert.equal(
        await browser.evaluate(`!!document.querySelector('[data-ui~=run-values]')`),
        false,
      )
    })
  },
)

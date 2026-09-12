import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { createServer } from 'node:http'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import { browserAvailable, withConsoleBrowser } from './browser-test-client.ts'
import { fieldValue } from './browser-fields.ts'

const corpus = JSON.parse(
  readFileSync(
    new URL('../../../contracts/product-experience/agent-compiler/v2/corpus.json', import.meta.url),
    'utf8',
  ),
)
const model = corpus.cases.find((item) => item.expected.execution_kind === 'model_chat').bindings
  .model
const canonical = (value: unknown): string =>
  Array.isArray(value)
    ? `[${value.map(canonical).join(',')}]`
    : value !== null && typeof value === 'object'
      ? `{${Object.keys(value)
          .sort()
          .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
          .join(',')}}`
      : JSON.stringify(value)
const profile = {
  schema_version: 1,
  ...corpus.profile,
  models: [
    {
      alias: model.manifest_ref,
      deployment: model.deployment,
      selection_policy: model.selection_policy,
    },
  ],
}
profile.profile_digest = `sha256:${createHash('sha256').update(canonical(profile)).digest('hex')}`

test(
  'Chinese connection distinguishes authentication and permission; model wizard preserves fields, validates and highlights YAML',
  { skip: !browserAvailable, timeout: 35_000 },
  async () => {
    let access = 200
    let reads = 0
    let writes = 0
    const api = createServer((request, response) => {
      response.setHeader('content-type', 'application/json')
      if (request.url === '/readyz') {
        response.end('{}')
        return
      }
      if (request.method !== 'GET') writes++
      if (request.url.startsWith('/v1/agents?')) {
        reads++
        response.statusCode = access
        response.end(
          JSON.stringify(
            access === 200
              ? { schema_version: 1, items: [], next_cursor: null }
              : {
                  code: access === 401 ? 'authentication_required' : 'permission_denied',
                  retryable: false,
                },
          ),
        )
        return
      }
      if (request.url === '/v1/agent-authoring-profile') {
        response.end(JSON.stringify(profile))
        return
      }
      response.statusCode = 404
      response.end('{"code":"not_found","retryable":false}')
    })
    await withConsoleBrowser(api, async (browser) => {
      access = 401
      await browser.field('访问令牌', 'expired-fixture')
      await browser.click('进入工作空间')
      await browser.wait(
        `document.body.innerText.includes('会话无效或已过期')`,
        '401 connection feedback',
      )
      assert.equal(await browser.evaluate(`!!document.querySelector('nav')`), false)
      access = 403
      await browser.click('进入工作空间')
      await browser.wait(
        `document.body.innerText.includes('无权执行此操作')`,
        '403 connection feedback',
      )
      assert.equal(
        await browser.evaluate(`document.body.innerText.includes('还没有智能体')`),
        false,
      )
      access = 200
      await browser.connect('wizard-fixture')
      await browser.wait(`document.body.innerText.includes('还没有智能体')`, 'automatic empty list')
      assert.ok(reads >= 4)
      assert.equal(
        await browser.evaluate(
          `!!document.querySelector('input[type="password"],input[type="url"]')`,
        ),
        false,
      )
      await browser.click('创建智能体')
      await browser.evaluate(`document.querySelector('[data-ui~=editor] details summary').click()`)
      await browser.field('资源标识', 'wizard-model')
      await browser.field('智能体名称', '中文助手')
      await browser.click('下一步')
      await browser.click('下一步')
      await browser.wait(
        `document.body.innerText.includes('请选择模型并填写任务指令')`,
        'missing model blocks next step',
      )
      await browser.wait(
        `${fieldValue('模型')} !== undefined && [...document.querySelectorAll('option')].some(option => option.value === ${JSON.stringify(model.manifest_ref)})`,
        'current model profile',
      )
      await browser.field('模型', model.manifest_ref)
      await browser.field('任务指令', '用中文回答，将结果放入 answer 字段。')
      await browser.click('下一步')
      const inputTable = `document.querySelector('section[aria-label="输入字段"]')`
      await browser.evaluate(
        `[...${inputTable}.querySelectorAll('button')].find(button => button.textContent.includes('添加字段')).click()`,
      )
      await browser.field('field_1 字段名称', 'message')
      await browser.evaluate(
        `document.querySelector('[aria-label="field_1 字段名称"]').dispatchEvent(new FocusEvent('blur', {bubbles:true})); document.querySelector('[aria-label="field_1 字段名称"]').dispatchEvent(new FocusEvent('focusout', {bubbles:true}))`,
      )
      await browser.click('下一步')
      await browser.wait(
        `document.body.innerText.includes('字段名称不能为空或重复')`,
        'duplicate field blocked',
      )
      await browser.field('field_1 字段名称', 'context')
      await browser.evaluate(
        `document.querySelector('[aria-label="field_1 字段名称"]').dispatchEvent(new FocusEvent('focusout', {bubbles:true}))`,
      )
      await browser.wait(
        `!!document.querySelector('[aria-label="context 字段名称"]')`,
        'field rename',
      )
      await browser.field('context 说明', '补充背景')
      await browser.evaluate(`document.querySelector('[aria-label="context 必填"]').click()`)
      await browser.call('Emulation.setDeviceMetricsOverride', {
        width: 390,
        height: 844,
        deviceScaleFactor: 1,
        mobile: true,
      })
      assert.equal(
        await browser.evaluate(`document.documentElement.scrollWidth <= innerWidth`),
        true,
        'narrow page does not overflow',
      )
      await browser.call('Emulation.clearDeviceMetricsOverride')
      await browser.click('下一步')
      await browser.click('校验配置')
      await browser.wait(
        `document.body.innerText.includes('wizard-model 校验通过')`,
        'real Rust model-chat compilation',
      )
      await browser.click('高级 YAML')
      await browser.wait(
        `!!document.querySelector('[data-code-editor="agent.yaml"] .cm-line span')`,
        'YAML syntax highlight',
      )
      const yaml = await browser.evaluate(fieldValue('agent.yaml'))
      assert.match(yaml, /model_chat/)
      await browser.field('agent.yaml', '# 用户备注\n' + yaml)
      await browser.click('分步表单')
      await browser.wait(`${fieldValue('智能体名称')} === '中文助手'`, 'YAML to form')
      await browser.field('智能体名称', '保留备注的助手')
      await browser.click('高级 YAML')
      await browser.wait(
        `String(${fieldValue('agent.yaml')}).includes('保留备注的助手')`,
        'form to YAML',
      )
      assert.match(await browser.evaluate(fieldValue('agent.yaml')), /# 用户备注/)
      const schema = JSON.parse(await browser.evaluate(fieldValue('输入 Schema JSON')))
      assert.equal(schema.properties.context.description, '补充背景')
      assert.equal(schema.required.includes('context'), false)
      assert.equal(schema.properties.message.maxLength, 16384)
      assert.equal(writes, 0, 'view changes and validation do not publish')
      await browser.click('返回列表')
      access = 403
      await browser.click('刷新')
      await browser.wait(
        `document.body.innerText.includes('无权执行此操作')`,
        '403 keeps authenticated session',
      )
      assert.equal(await browser.evaluate(`!!document.querySelector('nav')`), true)
      access = 401
      await browser.click('刷新')
      await browser.wait(
        `!!document.querySelector('input[type="password"]')`,
        '401 revokes session',
      )
      assert.equal(await browser.evaluate(`!!document.querySelector('[data-ui~=editor]')`), false)
      assert.equal(
        await browser.evaluate(
          `Object.keys(localStorage).length + Object.keys(sessionStorage).length`,
        ),
        0,
      )
    })
  },
)

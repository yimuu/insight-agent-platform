import assert from 'node:assert/strict'
import { existsSync } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { build } from 'vite'
import react from '@vitejs/plugin-react'
import { browserBinary, withConsoleBrowser } from './browser-test-client.ts'

const enabled = process.env.INSIGHT_TASK_FORM_BROWSER === '1' && existsSync(browserBinary)

test(
  'Task schema form: real browser typed fields, array errors, pending submission, scope reset and unsupported schema',
  { skip: !enabled, timeout: 60_000 },
  async () => {
    const bundleRoot = await mkdtemp(join(tmpdir(), 'insight-task-form-browser-'))
    let requests = 0
    const api = createServer((_request, response) => {
      requests++
      response.writeHead(404)
      response.end()
    })
    try {
      await build({
        configFile: false,
        root: fileURLToPath(new URL('./task-form-fixture', import.meta.url)),
        plugins: [react()],
        logLevel: 'error',
        build: { outDir: bundleRoot, emptyOutDir: true },
      })
      await withConsoleBrowser(
        api,
        async (browser) => {
          const fixture = 'window.taskFormFixture'
          const submitted = () => browser.evaluate(`${fixture}.submitted`)
          const text = () => browser.evaluate('document.body.innerText')
          const change = (patch) => browser.evaluate(`${fixture}.configure(${patch})`)
          await browser.click('提交回复')
          await browser.wait(
            `document.querySelectorAll('[role="alert"]').length >= 4`,
            'field errors',
          )
          assert.deepEqual(await submitted(), [])
          assert.equal(await browser.evaluate('document.querySelectorAll("img").length'), 0)
          await browser.field('Name（必填）', '😀😀😀')
          assert.match(await text(), /UTF-8 字节/)
          await browser.field('Name（必填）', 'ok')
          await browser.field('Amount（必填）', '2')
          await browser.field('Approved（必填）', 'false')
          await browser.click('添加 Colors 项')
          await browser.field('Colors 第 1（必填）', '0')
          await browser.click('添加 Colors 项')
          await browser.field('Colors 第 2（必填）', '0')
          await browser.wait(
            `document.body.innerText.includes('数组元素不能重复。')`,
            'duplicate error',
          )
          assert.equal(
            await browser.evaluate(
              `[...document.querySelectorAll('button')].find(node => node.textContent === '添加 Colors 项').disabled`,
            ),
            true,
          )
          await browser.click('提交回复')
          assert.deepEqual(await submitted(), [])
          await browser.field('Colors 第 2（必填）', '1')
          await browser.evaluate(
            `[...document.querySelectorAll('label')].find(label => label.textContent === '包含 Details').querySelector('input').click()`,
          )
          await browser.field('Reply（必填）', 'done')
          await browser.click('提交回复')
          await browser.wait(
            `document.querySelector('button[type="submit"]').textContent === '提交中…'`,
            'pending submit',
          )
          assert.equal(
            await browser.evaluate(`document.querySelector('button[type="submit"]').disabled`),
            true,
          )
          await browser.evaluate(
            `document.querySelector('form').dispatchEvent(new Event('submit', {bubbles: true, cancelable: true}))`,
          )
          assert.deepEqual(await submitted(), [
            {
              name: 'ok',
              amount: 2,
              approved: false,
              colors: ['red', 'blue'],
              details: { reply: 'done' },
            },
          ])
          await browser.evaluate(
            `${fixture}.settle('response_rejected: frozen schema validation failed')`,
          )
          await browser.wait(
            `document.body.innerText.includes('response_rejected: frozen schema validation failed')`,
            'server rejection',
          )
          await change(`{behavior:'accept'}`)
          await browser.click('提交回复')
          await browser.wait(
            `${fixture}.submitted.length === 2 && !document.querySelector('button[type="submit"]').disabled`,
            'accepted response',
          )
          await change(`{disabled:true}`)
          await browser.wait(
            `document.querySelector('button[type="submit"]').disabled`,
            'external disabled',
          )
          await browser.evaluate(
            `document.querySelector('form').dispatchEvent(new Event('submit', {bubbles: true, cancelable: true}))`,
          )
          assert.equal((await submitted()).length, 2)

          await change(`{disabled:false, behavior:'pending'}`)
          await browser.click('提交回复')
          await browser.wait(`${fixture}.submitted.length === 3`, 'third submission pending')
          await change(`{digest:'sha256:'+'b'.repeat(64)}`)
          await browser.wait(
            `document.querySelector('textarea').value === '' && !document.querySelector('button[type="submit"]').disabled`,
            'digest resets draft',
          )
          await browser.evaluate(`${fixture}.settle('OLD TASK ERROR')`)
          assert.equal((await text()).includes('OLD TASK ERROR'), false)
          await browser.field('Name（必填）', 'new')
          await change(`{schema:structuredClone(${fixture}.initialSchema)}`)
          await browser.wait(
            `document.querySelector('textarea').value === 'new'`,
            'same schema content retains draft',
          )
          await change(`{subject:'task-b'}`)
          await browser.wait(
            `document.querySelector('textarea').value === ''`,
            'caller Task identity resets draft',
          )

          await change(
            `{schema:{...${fixture}.initialSchema,properties:{link:{type:'string',$ref:'https://invalid.example/schema'}}}}`,
          )
          await browser.wait(
            `document.body.innerText.includes('Unknown local or exact pinned schema reference')`,
            'unsupported reference message',
          )
          assert.equal(
            await browser.evaluate(`document.querySelector('button[type="submit"]').disabled`),
            true,
          )
          assert.equal((await submitted()).length, 3)
          assert.equal(requests, 0)
          const schema = `${fixture}.initialSchema`
          await change(
            `{schema:{schema_version:1,profile:'insight.closed-json-schema/1',canonical_digest:'wrong',schema:${schema}}}`,
          )
          await browser.wait(
            `document.body.innerText.includes('Unsupported or mismatched response schema envelope')`,
            'digest mismatch message',
          )
          assert.equal(
            await browser.evaluate(`document.querySelector('button[type="submit"]').disabled`),
            true,
          )

          await change(
            `{behavior:'accept', schema:{type:'object',properties:{message:{$ref:'#/$defs/Message'}, choice:{oneOf:[{type:'object',properties:{kind:{type:'string',const:'accept'},count:{type:'integer',minimum:1,maximum:10}},required:['kind','count'],additionalProperties:false},{type:'object',properties:{kind:{type:'string',const:'decline'}},required:['kind'],additionalProperties:false}]}, maybe:{oneOf:[{type:'string',minLength:0,maxLength:100,'x-platform-max-bytes':100},{type:'null'}]}, literal:{const:{$ref:'#/$defs/Literal',message:'<script>not HTML</script>'}}},required:['message','choice','maybe','literal'],additionalProperties:false,$defs:{Message:{type:'string',minLength:1,maxLength:20000,'x-platform-max-bytes':20000},Literal:{const:{$ref:'#/$defs/Literal'}}}}}`,
          )
          await browser.wait(
            `document.body.innerText.includes('表单字段、必填项') && !document.querySelector('button[type="submit"]').disabled`,
            'typed fallback ready',
          )
          await browser.field('message（必填）', 'local definition value')
          await browser.field('count（必填）', '')
          await browser.click('提交回复')
          assert.equal(
            (await submitted()).length,
            3,
            'invalid numeric draft must not submit its previous number',
          )
          await browser.field('count（必填）', '3')
          await browser.field('maybe（必填） 分支', '1')
          await browser.click('提交回复')
          await browser.wait(`${fixture}.submitted.length === 4`, 'typed fallback response')
          assert.deepEqual((await submitted())[3], {
            message: 'local definition value',
            choice: { kind: 'accept', count: 3 },
            maybe: null,
            literal: { $ref: '#/$defs/Literal', message: '<script>not HTML</script>' },
          })
          assert.equal(
            await browser.evaluate(
              `document.querySelectorAll('[data-ui~=task-schema-form] script').length`,
            ),
            0,
          )
          await browser.field('choice（必填） 分支', '1')
          await browser.click('提交回复')
          await browser.wait(`${fixture}.submitted.length === 5`, 'tagged union second branch')
          assert.deepEqual((await submitted())[4].choice, { kind: 'decline' })
          assert.equal(requests, 0, 'schema references never trigger network requests')
        },
        { bundleRoot, readyExpression: `!!document.querySelector('[data-ui~=task-schema-form]')` },
      )
    } finally {
      await rm(bundleRoot, { recursive: true, force: true })
    }
  },
)

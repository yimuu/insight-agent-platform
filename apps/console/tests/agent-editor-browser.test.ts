import { fieldValue } from './browser-fields.ts'
import './wasm-worker-fixture.ts'
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { createServer } from 'node:http'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { compileAgentManifest } from '../src/shared/compiler/compiler.ts'
import { buildFormManifest, manifestFormFields } from '../src/features/agents/editor.ts'
import { materializeDocument } from '../src/features/agents/publication.ts'
import { browserAvailable, eventually, withConsoleBrowser } from './browser-test-client.ts'

const corpusRoot = new URL(
  '../../../contracts/product-experience/agent-compiler/v2/',
  import.meta.url,
)
const text = (name) => readFileSync(new URL(name, corpusRoot), 'utf8')
const canonical = (value) =>
  Array.isArray(value)
    ? `[${value.map(canonical).join(',')}]`
    : value && typeof value === 'object'
      ? `{${Object.keys(value)
          .sort()
          .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
          .join(',')}}`
      : JSON.stringify(value)

test(
  'Agent editor validates Full Plan and framework graphs in actual browser WASM, preserves sources and refuses missing packages',
  {
    skip: !browserAvailable ? 'Requires an explicit Console bundle and Chrome executable' : false,
    timeout: 30_000,
  },
  async () => {
    const corpus = JSON.parse(text('corpus.json'))
    const input = {
      manifest: text('deterministic.yaml'),
      inputSchema: text('schema-message.json'),
      outputSchema: text('schema-message.json'),
      profile: corpus.profile,
      bindings: { model: null, slots: [] },
    }
    const seed = await compileAgentManifest(input)
    const full = await compileAgentManifest({
      ...input,
      manifest: buildFormManifest({
        ...(await manifestFormFields(seed.canonicalManifest)),
        executionKind: 'full_plan',
        planPath: 'plans/flow.json',
      }),
      plan: seed.typedPlan,
    })
    const profile = { schema_version: 1, ...corpus.profile, models: [] }
    profile.profile_digest = `sha256:${createHash('sha256').update(canonical(profile)).digest('hex')}`
    const workspace = mkdtempSync(join(tmpdir(), 'insight-editor-source-fixture-'))
    const inputPath = join(workspace, 'import.sources.json')
    writeFileSync(inputPath, full.sourceBundle)
    let mutationCalls = 0
    let denyContent = false
    const sourceId = 'art_01950000-0000-7000-8000-000000000003'
    const planId = 'art_01950000-0000-7000-8000-000000000004'
    const agentId = 'agt_01950000-0000-7000-8000-000000000001'
    const versionId = 'arev_01950000-0000-7000-8000-000000000002'
    const reference = (id, digest, content) => ({
      artifact_id: id,
      content_digest: digest,
      byte_length: Buffer.byteLength(content),
      media_type: 'application/json',
      classification: 'internal',
      display_name: null,
    })
    const sourceRef = reference(sourceId, full.sourceBundleDigest, full.sourceBundle)
    const planRef = reference(planId, full.typedPlanDigest, full.typedPlan)
    const document = materializeDocument(full, sourceRef, planRef)
    const api = createServer((request, response) => {
      if (request.url === '/readyz') {
        response.end('ready')
        return
      }
      if (request.method !== 'GET') mutationCalls++
      const send = (status, body, headers = {}) => {
        const encoded = JSON.stringify(body)
        response.writeHead(status, {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(encoded),
          ...headers,
        })
        response.end(encoded)
      }
      if (request.url === '/v1/agent-authoring-profile') {
        send(200, profile)
        return
      }
      if (request.url.startsWith('/v1/agents?')) {
        send(200, {
          schema_version: 1,
          next_cursor: null,
          items: [
            {
              agent_id: agentId,
              name: 'editable',
              display_name: 'Complete source Agent',
              state: 'draft',
              environment: null,
              published_at: null,
            },
            {
              agent_id: 'agt_missing',
              name: 'missing',
              display_name: 'Missing source Agent',
              state: 'draft',
              environment: null,
              published_at: null,
            },
          ],
        })
        return
      }
      if (request.url === `/v1/agents/${agentId}`) {
        send(
          200,
          {
            resource_id: agentId,
            resource_kind: 'agent',
            etag: '"agent-v1"',
            draft: { display_name: 'Complete source Agent', document },
          },
          { etag: '"agent-v1"' },
        )
        return
      }
      if (request.url === `/v1/agents/${agentId}/versions/${versionId}`) {
        const etag = `"${versionId}-${full.typedPlanDigest.slice(7)}"`
        send(
          200,
          {
            schema_version: 1,
            resource_id: agentId,
            resource_kind: 'agent',
            resource_version_id: versionId,
            revision_no: 1,
            content_digest: full.typedPlanDigest,
            artifact_id: planId,
            payload: { document, validation: {} },
            etag,
            created_at: '2026-09-06T00:00:00.000000Z',
          },
          { etag },
        )
        return
      }
      if (request.url === '/v1/agents/agt_missing') {
        send(200, {
          resource_id: 'agt_missing',
          etag: '"agent-v1"',
          draft: { document: { spec: {} } },
        })
        return
      }
      if (request.url === `/v1/artifacts/${sourceId}`) {
        send(200, {
          artifact_id: sourceId,
          state: 'ready',
          purpose: 'authoring_document',
          expected_size_bytes: Buffer.byteLength(full.sourceBundle),
          content: sourceRef,
        })
        return
      }
      if (request.url === `/v1/artifacts/${planId}`) {
        send(200, { artifact_id: planId, state: 'ready', purpose: 'typed_plan', content: planRef })
        return
      }
      if (request.url === `/v1/artifacts/${sourceId}/content`) {
        if (denyContent) {
          send(403, {
            code: 'permission_denied',
            detail: 'Source content permission was revoked.',
            retryable: false,
          })
          return
        }
        response.writeHead(200, {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(full.sourceBundle),
          etag: `"${full.sourceBundleDigest}"`,
        })
        response.end(full.sourceBundle)
        return
      }
      send(404, { code: 'not_found', detail: 'Fixture resource unavailable', retryable: false })
    })
    try {
      await withConsoleBrowser(api, async (browser) => {
        await browser.connect('editor-fixture-token')
        await browser.click('创建智能体')
        await browser.field('任务类型', 'deterministic')
        await browser.click('下一步')
        await browser.click('下一步')
        await browser.click('下一步')
        await browser.click('校验配置')
        await browser.wait(
          `document.body.innerText.includes('校验通过，依赖已解析')`,
          'validated deterministic starting point',
        )
        await browser.click('编辑已校验 Plan')
        await browser.wait(
          `(() => {
            if (${fieldValue('任务类型')} !== 'full_plan') return false;
            try { return JSON.parse(${fieldValue('Plan JSON')}).plan_version === 6; }
            catch { return false; }
          })()`,
          'explicit conversion of a Rust-compiled Plan',
        )
        assert.equal(JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).plan_version, 6)
        // Exercise replacement beyond CodeMirror's rendered viewport. Native DOM
        // select-all can leave unrendered source behind when inserting new text.
        const longPlan = JSON.stringify(
          { rows: Array.from({ length: 500 }, (_, index) => index) },
          null,
          2,
        )
        await browser.field('Plan JSON', longPlan)
        assert.equal(await browser.evaluate(fieldValue('Plan JSON')), longPlan)
        await browser.field('Plan JSON', '{}')
        assert.equal(await browser.evaluate(fieldValue('Plan JSON')), '{}')
        await browser.field('任务类型', 'full_plan')
        await browser.wait(
          `typeof (${fieldValue('Plan JSON')}) === 'string' && ${fieldValue('精确依赖绑定 JSON')} === '[]'`,
          'Full Plan editor controls',
        )
        await browser.field('Plan JSON', '{}')
        await browser.click('校验配置')
        await browser.wait(
          `!!document.querySelector('[role="alert"]')`,
          'Rust invalid Plan diagnostics',
        )
        assert.equal(await browser.evaluate(fieldValue('任务类型')), 'full_plan')
        assert.equal(await browser.evaluate(fieldValue('Plan JSON')), '{}')

        await browser.upload('input[type="file"][accept=".json,application/json"]', inputPath)
        await browser.wait(
          `document.body.innerText.includes('已导入源码与精确依赖')`,
          'complete source bundle import',
        )
        assert.equal(await browser.evaluate(fieldValue('Plan JSON')), seed.typedPlan)
        assert.equal(await browser.evaluate(fieldValue('输入 Schema JSON')), input.inputSchema)
        assert.equal(await browser.evaluate(fieldValue('输出 Schema JSON')), input.outputSchema)
        await browser.click('分步表单')
        await browser.wait(`${fieldValue('任务类型')} === 'full_plan'`, 'Full Plan Form projection')
        assert.equal(await browser.evaluate(fieldValue('Plan 路径')), 'plans/flow.json')
        await browser.click('高级 YAML')
        await browser.wait(`typeof (${fieldValue('agent.yaml')}) === 'string'`, 'YAML editing')
        assert.match(await browser.evaluate(fieldValue('agent.yaml')), /full_plan/)

        await browser.field('精确依赖绑定 JSON', '[{"slot_id":"missing-owner"}]')
        await browser.click('校验配置')
        await browser.wait(`!!document.querySelector('[role="alert"]')`, 'Rust slot diagnostics')
        assert.equal(
          await browser.evaluate(fieldValue('精确依赖绑定 JSON')),
          '[{"slot_id":"missing-owner"}]',
        )
        await browser.field('精确依赖绑定 JSON', '[]')
        await browser.click('校验配置')
        await browser.wait(
          `document.body.innerText.includes('校验通过，依赖已解析')`,
          'actual WASM Full Plan validation',
        )
        await browser.call('Browser.setDownloadBehavior', {
          behavior: 'allow',
          downloadPath: workspace,
        })
        await browser.click('导出源码包')
        const exportedPath = join(workspace, 'agent.sources.json')
        await eventually(() => existsSync(exportedPath), 'validated complete bundle download')
        const exported = JSON.parse(readFileSync(exportedPath, 'utf8'))
        assert.equal(exported.sources.files['plans/flow.json'], seed.typedPlan)
        assert.deepEqual(exported.bindings.slots ?? [], [])
        assert.equal(JSON.parse(exported.sources.files['plans/flow.json']).plan_version, 6)
        await browser.click('导出编译源码映射')
        await eventually(
          () => existsSync(join(workspace, 'source-map.json')),
          'immutable Rust source map download',
        )
        assert.equal(
          JSON.parse(readFileSync(join(workspace, 'source-map.json'), 'utf8')).typed_plan_digest,
          full.typedPlanDigest,
        )
        assert.match(await browser.evaluate('document.body.innerText'), /编译源码位置/)

        const editorDescriptor = JSON.parse(
          readFileSync(
            new URL('../../../contracts/platform-v1/agent-node-editor.v1.json', import.meta.url),
            'utf8',
          ),
        )
        for (const [index, entry] of editorDescriptor.nodes.entries()) {
          const id = `draft_${index}`
          await browser.field('新节点 ID', id)
          await browser.field('新节点类型', entry.kind)
          await browser.click('添加节点')
          await browser.wait(
            `${fieldValue('当前节点')} === ${JSON.stringify(id)}`,
            `selected ${entry.kind} node`,
          )
          assert.deepEqual(
            JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).nodes[id],
            entry.template,
          )
          assert.ok(
            await browser.evaluate(
              `document.querySelector('[data-field-path="/nodes/${id}"]').querySelectorAll('input,select,textarea').length > 0`,
            ),
            entry.kind,
          )
          if (entry.kind === 'compute') {
            await browser.click('重建表达式 assignments/0/expression')
            await browser.wait(
              `JSON.parse(${fieldValue('Plan JSON')}).nodes.${id}.assignments[0].expression.semantic_digest !== ${JSON.stringify(entry.template.assignments[0].expression.semantic_digest)}`,
              'Rust expression rebuild updates the source',
            )
            assert.equal(
              JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).nodes[id].assignments[0]
                .expression.maximum_stack_depth,
              1,
            )
          }
          if (entry.kind === 'branch') {
            await browser.click('添加 ordered_arms（必填） 项')
            assert.equal(
              JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).nodes[id].ordered_arms
                .length,
              2,
            )
            await browser.field(
              'otherwise（必填）',
              Object.keys(JSON.parse(seed.typedPlan).nodes)[0],
            )
            assert.equal(
              JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).nodes[id].otherwise,
              Object.keys(JSON.parse(seed.typedPlan).nodes)[0],
            )
          }
          if (entry.kind === 'join') {
            await browser.field('policy（必填）', '2')
            await browser.field('quorum（必填） 分支', '1')
            await browser.field('quorum（必填）', '2')
            assert.equal(
              JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).nodes[id].quorum,
              2,
            )
            assert.equal(
              JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).nodes[id].policy,
              'quorum',
            )
          }
          if (entry.kind === 'loop') {
            await browser.field('maximum_iterations（必填）', '4')
            await browser.click('添加 carried_ports（必填） 项')
            assert.equal(
              JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).nodes[id]
                .maximum_iterations,
              4,
            )
            assert.equal(
              JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).nodes[id].carried_ports
                .length,
              2,
            )
          }
          await browser.click(`删除节点 ${id}`)
          assert.equal(
            JSON.parse(await browser.evaluate(fieldValue('Plan JSON'))).nodes[id],
            undefined,
          )
        }

        const supportedYaml = await browser.evaluate(fieldValue('agent.yaml'))
        await browser.field('agent.yaml', supportedYaml.replace('full_plan', 'future_template'))
        await browser.click('分步表单')
        await browser.wait(
          `!!document.querySelector('[role="alert"]')`,
          'unsupported YAML refuses Form fallback',
        )
        assert.match(await browser.evaluate(fieldValue('agent.yaml')), /future_template/)
        await browser.field('agent.yaml', supportedYaml)
        await browser.click('分步表单')
        await browser.wait(`${fieldValue('任务类型')} === 'full_plan'`, 'restored supported Form')

        const typed = JSON.parse(seed.typedPlan)
        const descriptor = {
          adapter: 'langgraph-static-typed-ports',
          semantic_version: 1,
          state: 'single_assignment_exact_ports',
          checkpoint_owner: 'platform_run',
          ir_abi: 6,
        }
        const adapterDigest = `sha256:${createHash('sha256').update(canonical(descriptor)).digest('hex')}`
        const graph = {
          schema_version: 1,
          dialect: 'lang_graph_static_typed_ports_v1',
          adapter_semantic_identity: adapterDigest,
          entry_node_id: typed.entry_node_id,
          nodes: typed.nodes,
          dependency_slots: typed.dependency_slots,
          schema_documents: typed.schema_documents,
        }
        await browser.field('任务类型', 'framework_graph')
        await browser.field('Plan JSON', JSON.stringify(graph))
        await browser.click('高级 YAML')
        await browser.click('校验配置')
        await browser.wait(
          `document.body.innerText.includes('校验通过，依赖已解析')`,
          'actual WASM static framework validation',
        )
        assert.match(await browser.evaluate(fieldValue('agent.yaml')), /framework_graph/)
        assert.match(await browser.evaluate('document.body.innerText'), /Platform 节点/)
        await browser.click('高级 YAML')
        await browser.wait(
          `(${fieldValue('agent.yaml')}).includes('framework_graph')`,
          'framework Form/YAML round trip',
        )
        await browser.field('Plan JSON', 'def graph(): pass')
        await browser.click('校验配置')
        await browser.wait(
          `!!document.querySelector('[role="alert"]')`,
          'Python code is not a static framework export',
        )
        assert.equal(await browser.evaluate(fieldValue('Plan JSON')), 'def graph(): pass')

        await browser.click('返回列表')
        await browser.click('刷新')
        await browser.wait(
          `document.body.innerText.includes('Complete source Agent')`,
          'editable Agent list',
        )
        await browser.evaluate(
          `[...document.querySelectorAll('[data-ui~=agent-row]')].find(row => row.textContent.includes('Complete source Agent')).querySelector('button').click()`,
        )
        await browser.wait(
          `document.body.innerText.includes('已加载完整源码') && !!(${fieldValue('Plan JSON')})`,
          'existing Agent complete source recovery',
        )
        assert.equal(await browser.evaluate(fieldValue('Plan JSON')), seed.typedPlan)
        await browser.evaluate(
          `[...document.querySelectorAll('details')].find(node => node.querySelector('summary')?.textContent === '恢复已发布版本（高级）').open = true`,
        )
        await browser.field('已发布智能体 ID', agentId)
        await browser.field('已发布版本 ID', versionId)
        await browser.field('Plan JSON', '{"local":"unsaved draft"}')
        denyContent = true
        await browser.click('恢复已发布源码')
        await browser.wait(
          `document.body.innerText.includes('当前账户无权执行此操作')`,
          'source 403 leaves current editor intact',
        )
        assert.equal(await browser.evaluate(fieldValue('Plan JSON')), '{"local":"unsaved draft"}')
        denyContent = false
        await browser.click('恢复已发布源码')
        await browser.wait(
          `document.body.innerText.includes('已恢复已发布版本的完整源码和精确绑定') && (${fieldValue('Plan JSON')}) === ${JSON.stringify(seed.typedPlan)}`,
          'exact published source recovery in browser',
        )
        assert.equal(await browser.evaluate(fieldValue('Plan JSON')), seed.typedPlan)
        await browser.click('返回列表')
        await browser.evaluate(
          `[...document.querySelectorAll('[data-ui~=agent-row]')].find(row => row.textContent.includes('Missing source Agent')).querySelector('button').click()`,
        )
        await browser.wait(
          `document.body.textContent.includes('recompile_required')`,
          'missing source rejection',
        )
        assert.equal(await browser.evaluate(`!!document.querySelector('[data-ui~=editor]')`), false)
        assert.equal(mutationCalls, 0)
      })
    } finally {
      rmSync(workspace, { recursive: true, force: true })
    }
  },
)

import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Agents.module.css'
const cx = classNames(sharedStyles, localStyles)
import { CodeEditor } from '../../shared/ui/CodeEditor'
import { AgentWizard } from './editor/AgentWizard'
import { parseSchema } from './editor/schema-document'
import { exactFeatureSelections, exactResolvedFeatures } from '../../shared/api/authoring-query.ts'

import { restorePublishedSources } from './restore'
import { PlanEditor } from './PlanEditor'
import { readExactArtifact } from '../../shared/api/artifact-content'

import { AuthoringBindingsPanel } from './AuthoringBindingsPanel'
import { useEffect, useMemo, useRef, useState } from 'react'

import { PlatformClient } from '../../shared/api/client'

import {
  inspectAgentSources,
  compileCapturedAgentSources,
  compileAgentManifest,
  compileFrozenSourceBundle,
  inspectAgentManifest,
  verifyAgentAuthoringProfile,
} from '../../shared/compiler/compiler'
import type {
  AgentExecutionKind,
  CompiledAgent,
  ResolvedAgentBindings,
} from '../../shared/compiler/compiler'
import {
  updateFormManifest,
  exactSlotBindings,
  manifestFormFields,
  MAX_EDITOR_BUNDLE_BYTES,
  MAX_EDITOR_SOURCE_BYTES,
  planNodeOutline,
  readEditableSourceBundle,
} from './editor'
import type { AgentFormFields, EditableAgentSources } from './editor'

import { publishCompiledAgent } from './publication'
import type { PublicationStage } from './publication'
import type {
  AgentAuthoringProfile,
  AgentSummary,
  ArtifactRef,
  ResourceView,
} from '../../shared/api/types'

import { Icon } from '../../shared/ui/Icon'
import { Status, Metric } from '../../shared/ui/console-ui'
import { errorNotice, formatTime } from '../../shared/ui/feedback'
import type { Notice } from '../../shared/ui/feedback'
import { DEFAULT_INPUT_SCHEMA, DEFAULT_OUTPUT_SCHEMA } from './default-schemas'

export function Agents({
  client,
  active = true,
  report,
  onRun,
  onChat,
}: {
  client: PlatformClient | null
  active?: boolean
  report: (notice: Notice | null) => void
  onRun: (agent: AgentSummary) => void
  onChat?: (agent: AgentSummary) => void
}) {
  const editorElement = useRef<HTMLElement>(null)
  const ensureValidFields = () => {
    const invalid = editorElement.current?.querySelector<HTMLElement>('[aria-invalid="true"]')
    if (invalid) {
      invalid.focus()
      throw new Error('请修正字段表格中的错误后再继续。')
    }
  }
  const [search, setSearch] = useState('')
  const [pageNumber, setPageNumber] = useState(1)
  const [agents, setAgents] = useState<AgentSummary[]>([])
  const [cursor, setCursor] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [editor, setEditor] = useState(false)
  const [step, setStep] = useState(0)
  const [loaded, setLoaded] = useState(false)
  const [listFailed, setListFailed] = useState(false)
  const [mode, setMode] = useState<'form' | 'yaml'>('form')
  const [existing, setExisting] = useState<ResourceView | null>(null)
  const [compiled, setCompiled] = useState<CompiledAgent | null>(null)
  const [stage, setStage] = useState<PublicationStage | null>(null)
  const [name, setName] = useState('hello-agent')
  const [displayName, setDisplayName] = useState('Hello Agent')
  const [executionKind, setExecutionKind] = useState<AgentExecutionKind>('model_chat')
  const [instructions, setInstructions] = useState('Respond with a concise typed answer.')
  const [modelAlias, setModelAlias] = useState('')
  const [classification, setClassification] = useState('internal')
  const [deadline, setDeadline] = useState('')
  const [environment, setEnvironment] = useState('')
  const [inputSchema, setInputSchema] = useState(DEFAULT_INPUT_SCHEMA)
  const [outputSchema, setOutputSchema] = useState(DEFAULT_OUTPUT_SCHEMA)
  const [yaml, setYaml] = useState('')
  const [inputSchemaPath, setInputSchemaPath] = useState('input.schema.json')
  const [outputSchemaPath, setOutputSchemaPath] = useState('output.schema.json')
  const [planPath, setPlanPath] = useState('plan.json')
  const [plan, setPlan] = useState('')
  const [slotBindings, setSlotBindings] = useState('[]')
  const [manifestPath, setManifestPath] = useState('agent.yaml')
  const [exactModel, setExactModel] = useState<ResolvedAgentBindings['model']>(null)
  const [sourceProfileDigest, setSourceProfileDigest] = useState<string | null>(null)
  const [recoveredCompilation, setRecoveredCompilation] = useState<CompiledAgent | null>(null)
  const [restoreAgentId, setRestoreAgentId] = useState('')
  const [restoreVersionId, setRestoreVersionId] = useState('')
  const restoring = useRef<AbortController | null>(null)
  const sourceRevision = useRef(0)
  useEffect(
    () => () => {
      restoring.current?.abort()
      sourceRevision.current++
    },
    [],
  )
  const [authoringScope, setAuthoringScope] = useState(0)
  const [profile, setProfile] = useState<AgentAuthoringProfile | null>(null)
  useEffect(() => {
    if (!client || !editor || !active) return
    const controller = new AbortController()
    void client
      .getAgentAuthoringProfile({ signal: controller.signal })
      .then(async (response) => {
        await verifyAgentAuthoringProfile(response.data)
        if (!controller.signal.aborted) setProfile(response.data)
      })
      .catch((error) => {
        if (!controller.signal.aborted) {
          setProfile(null)
          report(errorNotice(error))
        }
      })
    return () => controller.abort()
  }, [client, editor, active, report])

  const loadPage = async (next?: string) => {
    if (!client) return report({ tone: 'error', text: '请先连接工作空间。' })
    setBusy(true)
    report(null)
    try {
      const response = await client.listAgents(next)
      setAgents(response.data.items)
      setPageNumber(next ? pageNumber + 1 : 1)
      setLoaded(true)
      setListFailed(false)
      setCursor(response.data.next_cursor)
    } catch (error) {
      setListFailed(true)
      report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }

  useEffect(() => {
    if (!client) return
    let active = true
    void client
      .listAgents()
      .then((response) => {
        if (active) {
          setAgents(response.data.items)
          setCursor(response.data.next_cursor)
          setLoaded(true)
          setListFailed(false)
        }
      })
      .catch((error) => {
        if (active) {
          setListFailed(true)
          report(errorNotice(error))
        }
      })
    return () => {
      active = false
    }
  }, [client, report])

  const invalidate = () => {
    sourceRevision.current++
    setCompiled(null)
    setStage(null)
    report(null)
  }
  const formFields = (): AgentFormFields => ({
    name,
    displayName,
    executionKind,
    instructions,
    modelAlias,
    classification,
    deadline,
    environment,
    inputSchemaPath,
    outputSchemaPath,
    planPath,
  })
  const applyFields = (fields: AgentFormFields) => {
    setName(fields.name)
    setDisplayName(fields.displayName)
    setExecutionKind(fields.executionKind)
    setInstructions(fields.instructions)
    setModelAlias(fields.modelAlias)
    setClassification(fields.classification)
    setDeadline(fields.deadline)
    setEnvironment(fields.environment)
    setInputSchemaPath(fields.inputSchemaPath)
    setOutputSchemaPath(fields.outputSchemaPath)
    setPlanPath(fields.planPath)
  }
  const applySources = (sources: EditableAgentSources) => {
    setAuthoringScope((value) => value + 1)
    invalidate()
    applyFields(sources.fields)
    setYaml(sources.manifest)
    setInputSchema(sources.inputSchema)
    setOutputSchema(sources.outputSchema)
    setPlan(sources.plan)
    setSlotBindings(sources.slotBindings)
    setManifestPath(sources.manifestPath)
    setExactModel(sources.modelBinding)
    setSourceProfileDigest(sources.compilerProfileDigest)
    setRecoveredCompilation(null)
    setMode('yaml')
    setEditor(true)
  }
  const openNew = () => {
    setAuthoringScope((value) => value + 1)
    invalidate()
    setExisting(null)
    setManifestPath('agent.yaml')
    setExactModel(null)
    setSourceProfileDigest(null)
    setRecoveredCompilation(null)
    applyFields({
      name: `agent-${crypto.randomUUID()}`,
      displayName: '我的智能体',
      executionKind: 'model_chat',
      instructions: '请根据用户输入，用简洁的中文回答。将回答放在 answer 字段中。',
      modelAlias: '',
      classification: 'internal',
      deadline: '',
      environment: '',
      inputSchemaPath: 'input.schema.json',
      outputSchemaPath: 'output.schema.json',
      planPath: 'plan.json',
    })
    setStep(0)
    setInputSchema(DEFAULT_INPUT_SCHEMA)
    setOutputSchema(DEFAULT_OUTPUT_SCHEMA)
    setPlan('')
    setSlotBindings('[]')
    setYaml('')
    setMode('form')
    setEditor(true)
  }

  const openExisting = async (summary: AgentSummary) => {
    if (!client) return
    invalidate()
    const revision = sourceRevision.current
    setEditor(false)
    setExisting(null)
    setBusy(true)
    report(null)
    try {
      const response = await client.getResource('agents', summary.agent_id)
      const document = response.data.draft.document as {
        spec?: { authoring_package?: { artifact?: ArtifactRef } }
      }
      const artifact = document.spec?.authoring_package?.artifact
      if (!artifact)
        throw new Error(
          'recompile_required: This draft has no complete authoring source package. Import the original sources to edit it.',
        )
      const content = await readExactArtifact(client, artifact, {
        maximumBytes: MAX_EDITOR_BUNDLE_BYTES,
        purpose: 'authoring_document',
      })
      const source = await content.text()
      const recovered = await compileFrozenSourceBundle(source)
      const sources = await readEditableSourceBundle(source)
      if (sourceRevision.current !== revision) return
      applySources(sources)
      setRecoveredCompilation(recovered)
      setExisting(response.data)
      report({
        tone: 'info',
        text: '已加载完整源码，请使用当前工作空间配置重新校验后发布。',
        traceId: response.traceId,
      })
    } catch (error) {
      if (sourceRevision.current === revision) report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }

  const currentManifest = () => (mode === 'yaml' ? yaml : updateFormManifest(yaml, formFields()))
  const restorePublished = async () => {
    if (!client || busy) return
    restoring.current?.abort()
    const controller = new AbortController()
    restoring.current = controller
    setBusy(true)
    report(null)
    try {
      const restored = await restorePublishedSources(
        client,
        restoreAgentId.trim(),
        restoreVersionId.trim(),
        controller.signal,
      )
      if (controller.signal.aborted) return
      applySources(restored.sources)
      setExisting(restored.current)
      setRecoveredCompilation(restored.compiled)
      report({
        tone: 'success',
        text: '已恢复已发布版本的完整源码和精确绑定，修改仅在重新发布后生效。',
      })
    } catch (error) {
      if (!controller.signal.aborted) report(errorNotice(error))
    } finally {
      if (!controller.signal.aborted) setBusy(false)
    }
  }
  const switchMode = async (next: 'form' | 'yaml') => {
    if (next === mode) return
    if (next === 'yaml') {
      try {
        ensureValidFields()
        setYaml(currentManifest())
        setMode('yaml')
      } catch (error) {
        report(errorNotice(error))
      }
      return
    }
    setBusy(true)
    try {
      const revision = sourceRevision.current
      const fields = await manifestFormFields(yaml)
      if (revision !== sourceRevision.current) return
      applyFields(fields)
      setStep(0)
      setMode('form')
    } catch (error) {
      report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }

  const compile = async (): Promise<CompiledAgent> => {
    if (!client) throw new Error('请先连接工作空间。')
    ensureValidFields()
    const revision = sourceRevision.current
    const manifest = currentManifest()
    const sourceInput = inputSchema
    const sourceOutput = outputSchema
    const sourcePlan = plan
    const sourceSlots = slotBindings
    const manifestInspection = await inspectAgentManifest(manifest)
    const { sources, inspected } = await inspectAgentSources({
      manifest,
      manifestPath,
      inputSchema: sourceInput,
      outputSchema: sourceOutput,
      ...(manifestInspection.planPath ? { plan: sourcePlan } : {}),
    })
    const authoring = await client.getAgentAuthoringProfile()
    await verifyAgentAuthoringProfile(authoring.data)
    const savedModel = exactModel?.manifest_ref === inspected.modelRef ? exactModel : null
    const model =
      inspected.modelRef === null
        ? null
        : authoring.data.models.find((candidate) => candidate.alias === inspected.modelRef)
    if (inspected.modelRef !== null && !model && !savedModel) {
      throw new Error(
        `agent_binding_not_ready: Model ${inspected.modelRef} is not enabled by this tenant`,
      )
    }
    const slots = exactSlotBindings(sourceSlots)
    const featureQuery = exactFeatureSelections(slots)
    const deploymentFeatures = featureQuery
      ? exactResolvedFeatures((await client.resolveAgentBindings(featureQuery)).data)
      : []
    const result = await compileCapturedAgentSources(sources, authoring.data, {
      model:
        savedModel ??
        (model
          ? {
              manifest_ref: model.alias,
              deployment: model.deployment,
              selection_policy: model.selection_policy,
            }
          : null),
      slots,
      ...(deploymentFeatures.length ? { deployment_features: deploymentFeatures } : {}),
    })
    if (revision !== sourceRevision.current)
      throw new Error('校验过程中源码发生变化，请重新校验当前内容。')
    setProfile(authoring.data)
    setCompiled(result)
    return result
  }

  const validate = async () => {
    setBusy(true)
    report(null)
    try {
      const result = await compile()
      report({ tone: 'success', text: `${result.name} 校验通过，依赖已解析。` })
    } catch (error) {
      report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }

  const publish = async () => {
    if (!client) return
    setBusy(true)
    report(null)
    try {
      const result = await compile()
      const publication = await publishCompiledAgent(client, result, existing, setStage)
      setExisting(publication.resource)
      setEditor(false)
      await loadPage()
      report({ tone: 'success', text: `${result.name} 已发布，可以开始运行。` })
    } catch (error) {
      report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }

  const importYaml = async (file: File | undefined) => {
    if (!file) return
    if (file.size > MAX_EDITOR_SOURCE_BYTES)
      return report({ tone: 'error', text: 'agent.yaml 超过 1 MiB 编辑限制。' })
    invalidate()
    setExisting(null)
    setManifestPath('agent.yaml')
    setExactModel(null)
    setSourceProfileDigest(null)
    setRecoveredCompilation(null)
    setAuthoringScope((value) => value + 1)
    setYaml(await file.text())
    setInputSchema('')
    setOutputSchema('')
    setPlan('')
    setSlotBindings('[]')
    report({ tone: 'info', text: '已导入 agent.yaml，请补齐引用的 Schema 和 Plan 文件后校验。' })
    setMode('yaml')
    setEditor(true)
  }
  const importBundle = async (file: File | undefined) => {
    if (!file) return
    if (file.size > MAX_EDITOR_BUNDLE_BYTES)
      return report({ tone: 'error', text: '源码包超过 8 MiB 限制。' })
    setBusy(true)
    try {
      const sources = await readEditableSourceBundle(await file.text())
      setExisting(null)
      applySources(sources)
      report({ tone: 'info', text: '已导入源码与精确依赖，请使用当前工作空间配置校验后发布。' })
    } catch (error) {
      report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }
  const downloadSource = (filename: string, content: string, mediaType: string) => {
    const url = URL.createObjectURL(new Blob([content], { type: mediaType }))
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = filename
    anchor.click()
    URL.revokeObjectURL(url)
  }
  const exportBundle = async () => {
    setBusy(true)
    try {
      downloadSource('agent.sources.json', (await compile()).sourceBundle, 'application/json')
    } catch (error) {
      report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }
  const createWorkflow = async () => {
    if (!client || busy) return
    setBusy(true)
    try {
      const authoring = await client.getAgentAuthoringProfile()
      await verifyAgentAuthoringProfile(authoring.data)
      const seed = await compileAgentManifest({
        manifest: updateFormManifest('', { ...formFields(), executionKind: 'deterministic' }),
        inputSchema,
        outputSchema: inputSchema,
        profile: authoring.data,
        bindings: { model: null, slots: [] },
      })
      invalidate()
      setOutputSchema(inputSchema)
      setPlan(seed.typedPlan)
      setSlotBindings('[]')
      report(null)
    } catch (error) {
      report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }
  const useValidatedPlan = async () => {
    if (!compiled || compiled.executionKind !== 'deterministic') return
    setBusy(true)
    try {
      const fields = await manifestFormFields(compiled.canonicalManifest)
      invalidate()
      applyFields({ ...fields, executionKind: 'full_plan', planPath: 'plan.json' })
      setPlan(compiled.typedPlan)
      setSlotBindings('[]')
      setMode('form')
      report({ tone: 'info', text: '已载入可编辑的 Plan，修改后请重新校验。' })
    } catch (error) {
      report(errorNotice(error))
    } finally {
      setBusy(false)
    }
  }
  const visibleAgents = agents.filter((agent) =>
    `${agent.display_name} ${agent.name}`
      .toLocaleLowerCase()
      .includes(search.trim().toLocaleLowerCase()),
  )
  const outline = useMemo(() => planNodeOutline(plan), [plan])

  return (
    <section className={cx('stack')}>
      {!editor && (
        <div className={cx('page-toolbar')}>
          <label className={cx('agent-search')}>
            <Icon name="search" size={16} />
            <input
              aria-label="搜索当前页智能体"
              placeholder="搜索当前页智能体…"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
          </label>
          <div className={cx('actions')}>
            <button className={cx('button')} onClick={() => loadPage()} disabled={busy}>
              刷新
            </button>
            <details className={cx('import-menu')}>
              <summary className={cx('button')}>导入</summary>
              <div>
                {' '}
                <label className={cx('button file-button')}>
                  导入 YAML
                  <input
                    type="file"
                    accept=".yaml,.yml,text/yaml"
                    disabled={busy}
                    onChange={(event) => importYaml(event.target.files?.[0])}
                  />
                </label>
                <label className={cx('button file-button')}>
                  导入源码包
                  <input
                    type="file"
                    accept=".json,application/json"
                    disabled={busy}
                    onChange={(event) => importBundle(event.target.files?.[0])}
                  />
                </label>
              </div>
            </details>
            <button
              data-ui="agent-create"
              className={cx('button button--primary')}
              onClick={openNew}
              disabled={busy}
            >
              ＋ 创建智能体
            </button>
          </div>
        </div>
      )}
      {!editor && !loaded && !listFailed && (
        <article data-ui="panel" className={cx('panel')} role="status">
          正在加载智能体…
        </article>
      )}
      {!editor && listFailed && (
        <article data-ui="panel" className={cx('panel')} role="alert">
          列表加载失败，请检查提示后重试。
        </article>
      )}
      {!editor && loaded && !listFailed && agents.length === 0 && (
        <article data-ui="panel empty-state" className={cx('panel empty-content')} role="status">
          <Icon name="agents" size={32} />
          <h2>还没有智能体</h2>
          <p className={cx('body-copy')}>创建一个助手，选择模型并写下任务指令。</p>
          <button className={cx('button button--primary')} onClick={openNew}>
            创建第一个智能体
          </button>
        </article>
      )}
      {!editor && agents.length > 0 && (
        <div>
          <div className={cx('agent-list')} role="list">
            {visibleAgents.map((agent) => (
              <div
                data-ui="agent-row"
                className={cx('agent-row')}
                role="listitem"
                key={agent.agent_id}
                data-agent-id={agent.agent_id}
              >
                <div className={cx('agent-card-title')}>
                  <span className={cx('agent-avatar')}>
                    <Icon name="agents" size={21} />
                  </span>
                  <strong>{agent.display_name}</strong>
                  <span data-ui="agent-name">{agent.name}</span>
                </div>
                <Status value={agent.state} />
                <span className={cx('agent-meta')}>
                  {agent.environment ? `环境 · ${agent.environment}` : '尚未发布'}
                </span>
                <span className={cx('agent-meta')}>
                  {agent.published_at
                    ? `发布于 ${formatTime(agent.published_at)}`
                    : '发布后即可运行'}
                </span>
                <div className={cx('actions')}>
                  <button
                    className={cx('button')}
                    onClick={() => openExisting(agent)}
                    disabled={busy}
                  >
                    编辑
                  </button>
                  {onChat && (
                    <button
                      className={cx('button')}
                      disabled={busy || !agent.active_deployment}
                      onClick={() => onChat(agent)}
                    >
                      对话
                    </button>
                  )}
                  <button
                    className={cx('button button--primary')}
                    disabled={agent.state !== 'ready'}
                    onClick={() => onRun(agent)}
                  >
                    运行
                  </button>
                </div>
              </div>
            ))}
          </div>
          {visibleAgents.length === 0 && (
            <div className={cx('empty-content')}>
              <Icon name="search" />
              <h3>没有找到匹配的智能体</h3>
              <button className={cx('button')} onClick={() => setSearch('')}>
                清除搜索
              </button>
            </div>
          )}
          {(cursor || pageNumber > 1) && (
            <div className={cx('pagination')}>
              <span>第 {pageNumber} 页</span>
              <button
                className={cx('button')}
                disabled={busy || pageNumber === 1}
                onClick={() => loadPage()}
              >
                返回首页
              </button>
              <button
                className={cx('button')}
                disabled={busy || !cursor}
                onClick={() => cursor && loadPage(cursor)}
              >
                下一页
              </button>
            </div>
          )}
        </div>
      )}
      <details data-ui="panel" className={cx('list-tools')}>
        <summary>恢复已发布版本（高级）</summary>
        <form
          className={cx('form-grid')}
          onSubmit={(event) => {
            event.preventDefault()
            void restorePublished()
          }}
        >
          <label>
            <span>已发布智能体 ID</span>
            <input
              value={restoreAgentId}
              onChange={(event) => setRestoreAgentId(event.target.value)}
              maxLength={64}
              placeholder="agt_…"
              required
              disabled={busy}
            />
          </label>
          <label>
            <span>已发布版本 ID</span>
            <input
              value={restoreVersionId}
              onChange={(event) => setRestoreVersionId(event.target.value)}
              maxLength={64}
              placeholder="aif_… 或 arev_…"
              required
              disabled={busy}
            />
          </label>
          <button className={cx('button')} disabled={busy || !client}>
            恢复已发布源码
          </button>
        </form>
        <p className={cx('body-copy')}>
          读取所选版本及当前有权访问的源码。内容核验与编译全部通过后才替换编辑器。
        </p>
      </details>
      {editor && (
        <article
          ref={editorElement}
          data-ui="panel editor"
          className={cx('panel editor')}
          onChangeCapture={(event) => {
            if (!(event.target as HTMLElement).closest('[data-editor-view]')) invalidate()
          }}
        >
          <div data-ui="panel__heading" className={cx('panel__heading')}>
            <div>
              <p data-ui="kicker" className={cx('kicker')}>
                {existing ? '编辑智能体' : '新建智能体'}
              </p>
              <h2>{existing ? displayName : '创建你的智能体'}</h2>
            </div>
            <div className={cx('actions')}>
              <button className={cx('button')} disabled={busy} onClick={() => setEditor(false)}>
                返回列表
              </button>
              <div className={cx('segmented')}>
                <button
                  className={cx(mode === 'form' ? 'active' : '')}
                  disabled={busy}
                  onClick={() => switchMode('form')}
                >
                  分步表单
                </button>
                <button
                  className={cx(mode === 'yaml' ? 'active' : '')}
                  disabled={busy}
                  onClick={() => switchMode('yaml')}
                >
                  高级 YAML
                </button>
              </div>
            </div>
          </div>
          <fieldset disabled={busy} style={{ border: 0, padding: 0, margin: 0, minWidth: 0 }}>
            {sourceProfileDigest && (
              <p className={cx('body-copy')}>
                原始编译配置摘要： <code>{sourceProfileDigest}</code>
                。下次校验和发布使用当前工作空间配置，可能生成新的 Plan。
              </p>
            )}
            {mode === 'form' ? (
              <AgentWizard
                fields={formFields()}
                onChange={(patch) => {
                  applyFields({ ...formFields(), ...patch })
                  if (patch.modelAlias !== undefined) setExactModel(null)
                }}
                inputSchema={inputSchema}
                outputSchema={outputSchema}
                onInputSchemaChange={(next) => {
                  invalidate()
                  setInputSchema(next)
                }}
                onOutputSchemaChange={(next) => {
                  invalidate()
                  setOutputSchema(next)
                }}
                profile={profile}
                exactModel={exactModel}
                existing={Boolean(existing)}
                compiled={Boolean(compiled)}
                busy={busy}
                step={step}
              />
            ) : (
              <div className={cx('stack')}>
                <CodeEditor
                  label="agent.yaml"
                  value={yaml}
                  onChange={(next) => {
                    invalidate()
                    setYaml(next)
                  }}
                  disabled={busy}
                />
                <CodeEditor
                  language="json"
                  label="输入 Schema JSON"
                  value={inputSchema}
                  onChange={(next) => {
                    invalidate()
                    setInputSchema(next)
                  }}
                  disabled={busy}
                />
                <CodeEditor
                  language="json"
                  label="输出 Schema JSON"
                  value={outputSchema}
                  onChange={(next) => {
                    invalidate()
                    setOutputSchema(next)
                  }}
                  disabled={busy}
                />
              </div>
            )}
            {(executionKind === 'full_plan' ||
              executionKind === 'framework_graph' ||
              mode === 'yaml') &&
              (mode === 'yaml' || step === 1 || step === 3) && (
                <div className={cx('stack')}>
                  {!plan.trim() && executionKind === 'full_plan' ? (
                    <div className={cx('empty-state')}>
                      <h3>创建你的第一个工作流</h3>
                      <p>从开始和输出节点创建，然后添加步骤并配置连接。</p>
                      <button
                        className={cx('button button--primary')}
                        disabled={busy}
                        onClick={() => void createWorkflow()}
                      >
                        创建工作流
                      </button>
                    </div>
                  ) : (
                    <PlanEditor
                      source={plan}
                      onChange={(next) => {
                        invalidate()
                        setPlan(next)
                      }}
                      compiled={compiled}
                      disabled={busy}
                    />
                  )}
                  <details>
                    <summary>高级：编辑工作流源码</summary>
                    <CodeEditor
                      language="json"
                      label="Plan JSON"
                      value={plan}
                      onChange={(next) => {
                        invalidate()
                        setPlan(next)
                      }}
                      disabled={busy}
                    />
                  </details>
                  {outline && (
                    <details>
                      <summary>
                        节点概览 · {outline.nodes.length}
                        {outline.truncated ? '+' : ''} 个节点
                      </summary>
                      <table>
                        <thead>
                          <tr>
                            <th>节点</th>
                            <th>类型</th>
                            <th>入口</th>
                          </tr>
                        </thead>
                        <tbody>
                          {outline.nodes.map((node) => (
                            <tr key={node.id}>
                              <td>{node.id}</td>
                              <td>{node.kind}</td>
                              <td>{node.entry ? '入口' : ''}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      <p className={cx('body-copy')}>
                        {outline.truncated ? '仅显示前 128 个节点。' : ''}
                        此概览来自当前源码，请通过校验检查完整配置。
                      </p>
                    </details>
                  )}
                </div>
              )}
            {(executionKind === 'full_plan' ||
              executionKind === 'framework_graph' ||
              mode === 'yaml' ||
              slotBindings.trim() !== '[]') && (
              <details>
                <summary>高级：工作流依赖绑定</summary>
                <CodeEditor
                  language="json"
                  label="精确依赖绑定 JSON"
                  value={slotBindings}
                  onChange={(next) => {
                    invalidate()
                    setSlotBindings(next)
                  }}
                  disabled={busy}
                />
                <p className={cx('body-copy')}>没有依赖时使用 []；编译器会验证并冻结精确绑定。</p>
              </details>
            )}

            {(executionKind === 'full_plan' || executionKind === 'framework_graph') && (
              <AuthoringBindingsPanel
                key={authoringScope}
                client={client}
                disabled={busy}
                onResolved={(slots) => {
                  invalidate()
                  setSlotBindings(JSON.stringify(slots, null, 2))
                }}
              />
            )}
          </fieldset>
          {stage && <PublicationProgress stage={stage} />}
          {mode === 'form' && (
            <div className={cx('actions')}>
              <button
                className={cx('button')}
                disabled={busy || step === 0}
                onClick={() => setStep(step - 1)}
              >
                上一步
              </button>
              {step < 3 && (
                <button
                  className={cx('button button--primary')}
                  disabled={busy}
                  onClick={() => {
                    try {
                      if (step === 0 && !displayName.trim()) throw new Error('请填写智能体名称。')
                      if (
                        step === 1 &&
                        executionKind === 'model_chat' &&
                        (!modelAlias || !instructions.trim())
                      )
                        throw new Error('请选择模型并填写任务指令。')
                      if (step === 1 && executionKind === 'full_plan' && !plan.trim())
                        throw new Error('请先创建工作流。')
                      if (step === 2) {
                        ensureValidFields()
                        parseSchema(inputSchema)
                        parseSchema(outputSchema)
                      }
                      report(null)
                      setStep(step + 1)
                    } catch (error) {
                      report(errorNotice(error))
                    }
                  }}
                >
                  下一步
                </button>
              )}
            </div>
          )}
          {(mode === 'yaml' || step === 3) && (
            <div className={cx('actions')}>
              <button className={cx('button')} onClick={validate} disabled={busy}>
                校验配置
              </button>
              <button
                className={cx('button')}
                onClick={() => downloadSource('agent.yaml', currentManifest(), 'application/yaml')}
              >
                导出 YAML
              </button>
              <button className={cx('button')} onClick={exportBundle} disabled={busy}>
                导出源码包
              </button>
              {compiled?.executionKind === 'deterministic' && (
                <button className={cx('button')} onClick={useValidatedPlan} disabled={busy}>
                  编辑已校验 Plan
                </button>
              )}
              <button className={cx('button button--primary')} onClick={publish} disabled={busy}>
                {busy ? '处理中…' : '发布智能体'}
              </button>
            </div>
          )}
          {compiled && (
            <details className={cx('diagnostics')}>
              <summary>高级诊断</summary>
              <dl className={cx('metrics')}>
                <Metric label="清单摘要" value={compiled.manifestDigest} mono />
                <Metric label="Plan 摘要" value={compiled.typedPlanDigest} mono />
                <Metric label="源码映射摘要" value={compiled.sourceMapDigest} mono />
                <Metric label="资源 ID" value={existing?.resource_id} mono />
                <Metric label="ETag" value={existing?.etag} mono />
              </dl>
              <button
                className={cx('button')}
                onClick={() =>
                  downloadSource('source-map.json', compiled.sourceMap, 'application/json')
                }
              >
                导出编译源码映射
              </button>
            </details>
          )}
          {recoveredCompilation && (
            <details>
              <summary>已核验的恢复源码</summary>
              <p className={cx('body-copy')}>这些文件保留本地修改前的完整源码包及其编译映射。</p>
              <div className={cx('actions')}>
                <button
                  className={cx('button')}
                  onClick={() =>
                    downloadSource(
                      'recovered-agent.sources.json',
                      recoveredCompilation.sourceBundle,
                      'application/json',
                    )
                  }
                >
                  导出恢复源码包
                </button>
                <button
                  className={cx('button')}
                  onClick={() =>
                    downloadSource(
                      'recovered-source-map.json',
                      recoveredCompilation.sourceMap,
                      'application/json',
                    )
                  }
                >
                  导出恢复源码映射
                </button>
              </div>
            </details>
          )}
        </article>
      )}
    </section>
  )
}

function PublicationProgress({ stage }: { stage: PublicationStage }) {
  const stages: Array<{ id: PublicationStage; label: string }> = [
    { id: 'validating', label: '校验配置' },
    { id: 'publishing', label: '正在发布' },
    { id: 'activating', label: '正在激活' },
    { id: 'ready', label: '已就绪' },
  ]
  const active = stages.findIndex((item) => item.id === stage)
  return (
    <ol
      className={cx('publish-progress')}
      aria-live="polite"
      aria-label={`发布进度：${stages[active]?.label ?? stage}`}
    >
      {stages.map((item, index) => (
        <li className={cx(index <= active ? 'complete' : '')} key={item.id}>
          <span>{index + 1}</span>
          {item.label}
        </li>
      ))}
    </ol>
  )
}

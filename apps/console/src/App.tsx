import { exactFeatureSelections, exactResolvedFeatures } from './agent/authoring-query.ts'
import { RunValues } from './run/RunValues'
import { RunSignal } from './run/RunSignal'
import { RunSources } from './run/RunSources'
import { publishedRunDefaults } from './agent/published-run'
import { AuthorizedContent } from './run/AuthorizedContent'
import { restorePublishedSources } from './agent/restore'
import { PlanEditor } from './agent/PlanEditor'
import { readExactArtifact } from './api/artifact-content'
import type { RunEventHistory } from './api/sse'
import { AuthoringBindingsPanel } from './agent/AuthoringBindingsPanel'
import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { PlatformClient, PlatformProblem } from './api/client'
import { utcTimestamp } from './api/time'
import { discoverTaskIds, newReceipt, safeJson } from './api/security'
import {
  inspectAgentSources, compileCapturedAgentSources,
  compileFrozenSourceBundle,
  inspectAgentManifest,
  verifyAgentAuthoringProfile,
} from './agent/compiler'
import type { AgentExecutionKind, CompiledAgent, ResolvedAgentBindings } from './agent/compiler'
import { buildFormManifest, exactSlotBindings, manifestFormFields, MAX_EDITOR_BUNDLE_BYTES, MAX_EDITOR_SOURCE_BYTES, planNodeOutline, readEditableSourceBundle } from './agent/editor'
import type { AgentFormFields, EditableAgentSources } from './agent/editor'
import { TaskInbox } from './task/TaskInbox'
import {
  publishCompiledAgent,
} from './agent/publication'
import type { PublicationStage } from './agent/publication'
import type {
  AgentAuthoringProfile,
  AgentSummary,
  ArtifactView,
  ArtifactRef,
  JsonObject,
  OperationView,
  ResourceView,
  RunEvent,
  RunSummary,
  RunView,
} from './api/types'
import './App.css'

type ViewName = 'agents' | 'runs' | 'tasks' | 'settings'
type Notice = { tone: 'error' | 'success' | 'info'; text: string; traceId?: string | null }
const TERMINAL_RUNS = new Set(['succeeded', 'failed', 'cancelled', 'timed_out'])
const NAV: Array<{ id: ViewName; label: string; eyebrow: string }> = [
  { id: 'agents', label: 'Agents', eyebrow: '01' },
  { id: 'runs', label: 'Runs', eyebrow: '02' },
  { id: 'tasks', label: 'Tasks', eyebrow: '03' },
  { id: 'settings', label: 'Settings', eyebrow: '04' },
]
const DEFAULT_SCHEMA = JSON.stringify({
  $schema: 'https://json-schema.org/draft/2020-12/schema',
  type: 'object',
  properties: { message: { type: 'string', minLength: 1, maxLength: 128, 'x-platform-max-bytes': 512 } },
  required: ['message'],
  additionalProperties: false,
}, null, 2)

function errorNotice(error: unknown): Notice {
  if (error instanceof PlatformProblem) {
    const actions: Record<string, string> = {
      authentication_required: 'Sign in again.',
      permission_denied: 'Ask a tenant administrator for the required permission.',
      precondition_failed: 'Reload the Agent and compare the server version.',
      etag_mismatch: 'Reload the Agent and compare the server version.',
      idempotency_conflict: 'Keep the recovery handle and restore the original content.',
      capacity_exhausted: 'Wait for capacity and retry the same action.',
      cursor_expired: 'Refresh the list from its first page.',
      cursor_invalid: 'Refresh the list from its first page.',
    }
    const action = actions[error.code]
      ?? (error.retryable ? 'Retry the same action later.' : 'Open Advanced diagnostics or contact support.')
    return { tone: 'error', text: `${error.message} ${action}`, traceId: error.traceId }
  }
  return { tone: 'error', text: error instanceof Error ? error.message : 'Unknown console error' }
}

function Status({ value }: { value: string }) {
  const tone = ['ready', 'enabled', 'succeeded', 'approved', 'responded'].includes(value)
    ? 'positive'
    : ['failed', 'rejected', 'cancelled', 'timed_out', 'blocked', 'quarantined', 'corrupt'].includes(value)
      ? 'negative'
      : 'neutral'
  return <span className={`status status--${tone}`}>{value}</span>
}

function NoticeBox({ notice }: { notice: Notice | null }) {
  if (!notice) return <div role="status" aria-live="polite" aria-atomic="true" />
  return <div className={`notice notice--${notice.tone}`} role={notice.tone === 'error' ? 'alert' : 'status'} aria-live="polite" aria-atomic="true"><span>{notice.text}</span>{notice.traceId && <code>trace {notice.traceId}</code>}</div>
}

function Metric({ label, value, mono = false }: { label: string; value: string | number | null | undefined; mono?: boolean }) {
  return <div className="metric"><dt>{label}</dt><dd className={mono ? 'mono' : ''}>{value ?? '—'}</dd></div>
}

function SearchForm({ label, placeholder, value, onChange, onSubmit, busy }: { label: string; placeholder: string; value: string; onChange: (value: string) => void; onSubmit: () => void; busy: boolean }) {
  return <form className="search" onSubmit={(event) => { event.preventDefault(); onSubmit() }}><label><span>{label}</span><input value={value} onChange={(event) => onChange(event.target.value)} placeholder={placeholder} required autoComplete="off" /></label><button className="button button--primary" disabled={busy}>{busy ? 'Loading…' : 'Open'}</button></form>
}

function formatTime(value: string | null): string {
  return value ? new Date(value).toLocaleString() : '—'
}

function App() {
  const [view, setView] = useState<ViewName>('agents')
  const [endpoint, setEndpoint] = useState(window.location.origin)
  const [token, setToken] = useState('')
  const [tenant, setTenant] = useState('local development')
  const [ready, setReady] = useState<boolean | null>(null)
  const [notice, setNotice] = useState<{ scope: string; value: Notice | null } | null>(null)
  const [selectedTask, setSelectedTask] = useState<{ scope: string; id: string } | null>(null)
  const [launchAgent, setLaunchAgent] = useState<{ scope: string; agent: AgentSummary } | null>(null)

  const client = useMemo(() => { try { return new PlatformClient(endpoint, token) } catch { return null } }, [endpoint, token])
  // The key contains no credential. Remount every protected page when its session changes.
  const session = useMemo(() => ({ client, endpoint, tenant, key: crypto.randomUUID() }), [client, endpoint, tenant])
  const sessionScope = session.key
  const report = useCallback((value: Notice | null) => setNotice({ scope: sessionScope, value }), [sessionScope])
  const connect = async (event: FormEvent) => {
    event.preventDefault()
    report(null)
    try {
      const next = new PlatformClient(endpoint, token)
      const isReady = await next.readiness()
      setReady(isReady)
      report(isReady
        ? { tone: 'success', text: 'Gateway is ready. Credentials remain in browser memory only.' }
        : { tone: 'error', text: 'Gateway readiness endpoint is not ready.' })
    } catch (error) {
      setReady(false)
      report(errorNotice(error))
    }
  }

  const runAgent = (agent: AgentSummary) => {
    setLaunchAgent({ scope: sessionScope, agent })
    setView('runs')
  }

  return <div className="shell">
    <a className="skip-link" href="#console-main">Skip to content</a>
    <aside className="sidebar">
      <div className="brand" aria-label="Insight Agent Platform"><span className="brand__mark">IA</span><div><strong>Insight</strong><small>Agent Platform</small></div></div>
      <nav aria-label="Console sections">{NAV.map((item) => <button type="button" key={item.id} aria-current={view === item.id ? 'page' : undefined} className={view === item.id ? 'nav-item nav-item--active' : 'nav-item'} onClick={() => setView(item.id)}><span>{item.eyebrow}</span>{item.label}</button>)}</nav>
      <div className="session-summary"><span className={`pulse ${ready ? 'pulse--ready' : ''}`} aria-hidden="true" /><div><strong>{ready === null ? 'Not checked' : ready ? 'Gateway ready' : 'Unavailable'}</strong><small>{tenant || 'Tenant context unset'}</small></div></div>
    </aside>
    <main id="console-main" tabIndex={-1}>
      <header className="topbar"><div><p className="kicker">AGENT CONSOLE</p><h1>{NAV.find((item) => item.id === view)?.label}</h1></div><div className="contract"><span>CONTRACT</span><strong>insight.platform/v1</strong></div></header>
      <form className="connection-form session-connect" onSubmit={connect} aria-label="Gateway session">
        <label><span>Gateway origin</span><input type="url" value={endpoint} onChange={(event) => setEndpoint(event.target.value)} required /></label>
        <label><span>OIDC access token</span><input type="password" value={token} onChange={(event) => setToken(event.target.value)} autoComplete="off" spellCheck={false} placeholder="Memory only" /></label>
        <button className="button" type="submit">{ready ? 'Reconnect' : 'Connect'}</button>
      </form>
      <NoticeBox notice={notice?.scope === sessionScope ? notice.value : null} />
      <Fragment key={sessionScope}>
        {view === 'agents' && <Agents client={client} report={report} onRun={runAgent} />}
        {view === 'runs' && <Runs client={client} report={report} launchAgent={launchAgent?.scope === sessionScope ? launchAgent.agent : null} onTask={(id) => { setSelectedTask({ scope: sessionScope, id }); setView('tasks') }} />}
        {view === 'tasks' && <TaskInbox key={selectedTask?.scope === sessionScope ? selectedTask.id : 'direct'} client={client} report={report} selectedId={selectedTask?.scope === sessionScope ? selectedTask.id : ''} subjectKey={sessionScope} />}
        {view === 'settings' && <Settings client={client} report={report} tenant={tenant} setTenant={setTenant} ready={ready} endpoint={endpoint} />}
      </Fragment>
    </main>
  </div>
}

function Agents({ client, report, onRun }: { client: PlatformClient | null; report: (notice: Notice | null) => void; onRun: (agent: AgentSummary) => void }) {
  const [agents, setAgents] = useState<AgentSummary[]>([])
  const [cursor, setCursor] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [editor, setEditor] = useState(false)
  const [mode, setMode] = useState<'form' | 'yaml'>('form')
  const [existing, setExisting] = useState<ResourceView | null>(null)
  const [compiled, setCompiled] = useState<CompiledAgent | null>(null)
  const [stage, setStage] = useState<PublicationStage | null>(null)
  const [name, setName] = useState('hello-agent')
  const [displayName, setDisplayName] = useState('Hello Agent')
  const [executionKind, setExecutionKind] = useState<AgentExecutionKind>('deterministic')
  const [instructions, setInstructions] = useState('Respond with a concise typed answer.')
  const [modelAlias, setModelAlias] = useState('')
  const [classification, setClassification] = useState('internal')
  const [deadline, setDeadline] = useState('')
  const [environment, setEnvironment] = useState('')
  const [inputSchema, setInputSchema] = useState(DEFAULT_SCHEMA)
  const [outputSchema, setOutputSchema] = useState(DEFAULT_SCHEMA)
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
  useEffect(() => () => { restoring.current?.abort(); sourceRevision.current++ }, [])
  const [authoringScope, setAuthoringScope] = useState(0)
  const [profile, setProfile] = useState<AgentAuthoringProfile | null>(null)
  useEffect(() => {
    if (!client || !editor) return
    const controller = new AbortController()
    void client.getAgentAuthoringProfile({ signal: controller.signal }).then(async (response) => {
      await verifyAgentAuthoringProfile(response.data)
      if (!controller.signal.aborted) setProfile(response.data)
    }).catch((error) => { if (!controller.signal.aborted) { setProfile(null); report(errorNotice(error)) } })
    return () => controller.abort()
  }, [client, editor, report])

  const loadPage = async (next?: string) => {
    if (!client) return report({ tone: 'error', text: 'Connect to a valid Gateway first.' })
    setBusy(true)
    report(null)
    try {
      const response = await client.listAgents(next)
      setAgents(response.data.items)
      setCursor(response.data.next_cursor)
      report({ tone: 'success', text: response.data.items.length ? `Loaded ${response.data.items.length} Agents.` : 'This tenant has no Agents yet.', traceId: response.traceId })
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }

  const invalidate = () => { sourceRevision.current++; setCompiled(null); setStage(null); report(null) }
  const formFields = (): AgentFormFields => ({ name, displayName, executionKind, instructions, modelAlias, classification, deadline, environment, inputSchemaPath, outputSchemaPath, planPath })
  const applyFields = (fields: AgentFormFields) => {
    setName(fields.name); setDisplayName(fields.displayName); setExecutionKind(fields.executionKind)
    setInstructions(fields.instructions); setModelAlias(fields.modelAlias); setClassification(fields.classification)
    setDeadline(fields.deadline); setEnvironment(fields.environment)
    setInputSchemaPath(fields.inputSchemaPath); setOutputSchemaPath(fields.outputSchemaPath); setPlanPath(fields.planPath)
  }
  const applySources = (sources: EditableAgentSources) => {
    setAuthoringScope((value) => value + 1)
    invalidate()
    applyFields(sources.fields)
    setYaml(sources.manifest); setInputSchema(sources.inputSchema); setOutputSchema(sources.outputSchema)
    setPlan(sources.plan); setSlotBindings(sources.slotBindings)
    setManifestPath(sources.manifestPath); setExactModel(sources.modelBinding)
    setSourceProfileDigest(sources.compilerProfileDigest); setRecoveredCompilation(null)
    setMode('yaml'); setEditor(true)
  }
  const openNew = () => {
    setAuthoringScope((value) => value + 1)
    invalidate()
    setExisting(null)
    setManifestPath('agent.yaml'); setExactModel(null); setSourceProfileDigest(null); setRecoveredCompilation(null)
    applyFields({ name: 'hello-agent', displayName: 'Hello Agent', executionKind: 'deterministic', instructions: 'Respond with a concise typed answer.', modelAlias: '', classification: 'internal', deadline: '', environment: '', inputSchemaPath: 'input.schema.json', outputSchemaPath: 'output.schema.json', planPath: 'plan.json' })
    setInputSchema(DEFAULT_SCHEMA); setOutputSchema(DEFAULT_SCHEMA); setPlan(''); setSlotBindings('[]'); setYaml(''); setMode('form')
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
      const document = response.data.draft.document as { spec?: { authoring_package?: { artifact?: ArtifactRef } } }
      const artifact = document.spec?.authoring_package?.artifact
      if (!artifact) throw new Error('recompile_required: This draft has no complete authoring source package. Import the original sources to edit it.')
      const content = await readExactArtifact(client, artifact, { maximumBytes: MAX_EDITOR_BUNDLE_BYTES, purpose: 'authoring_document' })
      const source = await content.text()
      const recovered = await compileFrozenSourceBundle(source)
      const sources = await readEditableSourceBundle(source)
      if (sourceRevision.current !== revision) return
      applySources(sources)
      setRecoveredCompilation(recovered)
      setExisting(response.data)
      report({ tone: 'info', text: 'Loaded the complete Agent sources. Validate before publishing with the current tenant profile.', traceId: response.traceId })
    } catch (error) { if (sourceRevision.current === revision) report(errorNotice(error)) } finally { setBusy(false) }
  }

  const currentManifest = () => mode === 'yaml' ? yaml : buildFormManifest(formFields())
  const restorePublished = async () => {
    if (!client || busy) return
    restoring.current?.abort()
    const controller = new AbortController(); restoring.current = controller
    setBusy(true); report(null)
    try {
      const restored = await restorePublishedSources(client, restoreAgentId.trim(), restoreVersionId.trim(), controller.signal)
      if (controller.signal.aborted) return
      applySources(restored.sources); setExisting(restored.current); setRecoveredCompilation(restored.compiled)
      report({ tone: 'success', text: 'Recovered the exact published sources and bindings. Changes remain local until you publish a new draft generation.' })
    } catch (error) { if (!controller.signal.aborted) report(errorNotice(error)) }
    finally { if (!controller.signal.aborted) setBusy(false) }
  }
  const switchMode = async (next: 'form' | 'yaml') => {
    if (next === mode) return
    if (next === 'yaml') { setYaml(currentManifest()); setMode('yaml'); return }
    setBusy(true)
    try { applyFields(await manifestFormFields(yaml)); setMode('form') }
    catch (error) { report(errorNotice(error)) }
    finally { setBusy(false) }
  }

  const compile = async (): Promise<CompiledAgent> => {
    if (!client) throw new Error('Connect to a valid Gateway first.')
    const revision = sourceRevision.current
    const manifest = currentManifest()
    const sourceInput = inputSchema
    const sourceOutput = outputSchema
    const sourcePlan = plan
    const sourceSlots = slotBindings
    const manifestInspection = await inspectAgentManifest(manifest)
    const { sources, inspected } = await inspectAgentSources({ manifest, manifestPath, inputSchema:sourceInput, outputSchema:sourceOutput, ...(manifestInspection.planPath ? { plan:sourcePlan } : {}) })
    const authoring = await client.getAgentAuthoringProfile()
    await verifyAgentAuthoringProfile(authoring.data)
    const savedModel = exactModel?.manifest_ref === inspected.modelRef ? exactModel : null
    const model = inspected.modelRef === null ? null : authoring.data.models.find((candidate) => candidate.alias === inspected.modelRef)
    if (inspected.modelRef !== null && !model && !savedModel) {
      throw new Error(`agent_binding_not_ready: Model ${inspected.modelRef} is not enabled by this tenant`)
    }
    const slots = exactSlotBindings(sourceSlots)
    const featureQuery = exactFeatureSelections(slots)
    const deploymentFeatures = featureQuery ? exactResolvedFeatures((await client.resolveAgentBindings(featureQuery)).data) : []
    const result = await compileCapturedAgentSources(sources, authoring.data, { model: savedModel ?? (model ? { manifest_ref: model.alias, deployment: model.deployment, selection_policy: model.selection_policy } : null), slots, ...(deploymentFeatures.length ? { deployment_features: deploymentFeatures } : {}) })
    if (revision !== sourceRevision.current) throw new Error('The sources changed during validation. Validate the current editor contents again.')
    setProfile(authoring.data)
    setCompiled(result)
    return result
  }

  const validate = async () => {
    setBusy(true)
    report(null)
    try {
      const result = await compile()
      report({ tone: 'success', text: `${result.name} is valid and resolves exact tenant bindings.` })
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }

  const publish = async () => {
    if (!client) return
    setBusy(true)
    report(null)
    try {
      const result = await compile()
      const publication = await publishCompiledAgent(client, result, existing, setStage)
      setExisting(publication.resource)
      report({ tone: 'success', text: `${result.name} is ready to run.` })
      await loadPage()
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }

  const importYaml = async (file: File | undefined) => {
    if (!file) return
    if (file.size > MAX_EDITOR_SOURCE_BYTES) return report({ tone: 'error', text: 'agent.yaml exceeds the 1 MiB authoring limit.' })
    invalidate()
    setExisting(null)
    setManifestPath('agent.yaml'); setExactModel(null); setSourceProfileDigest(null); setRecoveredCompilation(null)
    setAuthoringScope((value) => value + 1)
    setYaml(await file.text())
    setInputSchema(''); setOutputSchema(''); setPlan(''); setSlotBindings('[]')
    report({ tone: 'info', text: 'Imported agent.yaml. Supply its referenced schema and Plan files below before validating.' })
    setMode('yaml')
    setEditor(true)
  }
  const importBundle = async (file: File | undefined) => {
    if (!file) return
    if (file.size > MAX_EDITOR_BUNDLE_BYTES) return report({ tone: 'error', text: 'The source bundle exceeds 8 MiB.' })
    setBusy(true)
    try {
      const sources = await readEditableSourceBundle(await file.text())
      setExisting(null)
      applySources(sources)
      report({ tone: 'info', text: 'Imported editable sources and exact slots. Validate with this tenant before publishing.' })
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }
  const downloadSource = (filename: string, content: string, mediaType: string) => {
    const url = URL.createObjectURL(new Blob([content], { type: mediaType }))
    const anchor = document.createElement('a')
    anchor.href = url; anchor.download = filename; anchor.click()
    URL.revokeObjectURL(url)
  }
  const exportBundle = async () => {
    setBusy(true)
    try { downloadSource('agent.sources.json', (await compile()).sourceBundle, 'application/json') }
    catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
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
      report({ tone: 'info', text: 'The validated Plan is now editable. Validate your changes before publishing.' })
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }
  const outline = useMemo(() => planNodeOutline(plan), [plan])

  return <section className="stack">
    <article className="panel toolbar"><div><p className="kicker">YOUR AGENTS</p><h2>Create, publish, and run</h2></div><div className="actions"><button className="button" onClick={() => loadPage()} disabled={busy}>Refresh</button><label className="button file-button">Import agent.yaml<input type="file" accept=".yaml,.yml,text/yaml" disabled={busy} onChange={(event) => importYaml(event.target.files?.[0])} /></label><label className="button file-button">Import source bundle<input type="file" accept=".json,application/json" disabled={busy} onChange={(event) => importBundle(event.target.files?.[0])} /></label><button className="button button--primary" onClick={openNew} disabled={busy}>New Agent</button></div></article>
    <details className="panel"><summary>Recover an exact published Agent version</summary><form className="form-grid" onSubmit={(event) => { event.preventDefault(); void restorePublished() }}>
      <label><span>Published Agent ID</span><input value={restoreAgentId} onChange={(event) => setRestoreAgentId(event.target.value)} maxLength={64} placeholder="agt_…" required disabled={busy} /></label>
      <label><span>Published version ID</span><input value={restoreVersionId} onChange={(event) => setRestoreVersionId(event.target.value)} maxLength={64} placeholder="aif_… or arev_…" required disabled={busy} /></label>
      <button className="button" disabled={busy || !client}>Recover published sources</button>
    </form><p className="body-copy">Reads the selected immutable version and its currently authorized source Artifact. Your editor is replaced only after exact content and Rust compilation checks pass.</p></details>
    {agents.length === 0 && <article className="panel empty-state" role="status"><p className="kicker">EMPTY TENANT</p><h2>No Agents loaded</h2><p className="body-copy">Refresh the list or create a deterministic Agent. Choose a template or supply a complete Plan.</p></article>}
    {agents.length > 0 && <article className="panel"><div className="agent-list" role="list">{agents.map((agent) => <div className="agent-row" role="listitem" key={agent.agent_id}><div><strong>{agent.display_name}</strong><span>{agent.name}</span></div><Status value={agent.state} /><span>{agent.environment ?? 'Not deployed'}</span><span>{formatTime(agent.published_at)}</span><div className="actions"><button className="button" onClick={() => openExisting(agent)} disabled={busy}>Edit</button><button className="button button--primary" disabled={agent.state !== 'ready'} onClick={() => onRun(agent)}>Run</button></div></div>)}</div>{cursor && <button className="button" onClick={() => loadPage(cursor)} disabled={busy}>Next page</button>}</article>}
    {editor && <article className="panel editor" onChangeCapture={(event) => { if (!(event.target as HTMLElement).closest('[data-editor-view]')) invalidate() }}><div className="panel__heading"><div><p className="kicker">{existing ? 'EDIT AGENT' : 'NEW AGENT'}</p><h2>{existing ? displayName : 'Define an Agent'}</h2></div><div className="segmented"><button className={mode === 'form' ? 'active' : ''} disabled={busy} onClick={() => switchMode('form')}>Form</button><button className={mode === 'yaml' ? 'active' : ''} disabled={busy} onClick={() => switchMode('yaml')}>YAML</button></div></div>
      <fieldset disabled={busy} style={{ border: 0, padding: 0, margin: 0, minWidth: 0 }}>
      {sourceProfileDigest && <p className="body-copy">Loaded source compiler profile digest: <code>{sourceProfileDigest}</code>. Your next Validate or Publish uses the current tenant profile and may produce a new Plan.</p>}
      {mode === 'form' ? <div className="form-grid">
        <label><span>Name</span><input value={name} disabled={Boolean(existing)} onChange={(event) => setName(event.target.value)} /></label>
        <label><span>Display name</span><input value={displayName} onChange={(event) => setDisplayName(event.target.value)} /></label>
        <label><span>Execution</span><select value={executionKind} onChange={(event) => setExecutionKind(event.target.value as AgentExecutionKind)}><option value="deterministic">Deterministic</option><option value="model_chat">Model chat</option><option value="full_plan">Full Plan</option><option value="framework_graph">Framework graph (Platform nodes)</option></select></label>
        <label><span>Classification</span><select value={classification} onChange={(event) => setClassification(event.target.value)}><option>public</option><option>internal</option><option>confidential</option><option>restricted</option></select></label>
        <label><span>Deadline seconds</span><input inputMode="numeric" value={deadline} placeholder={profile ? String(profile.default_deadline_seconds) : 'Tenant default'} onChange={(event) => setDeadline(event.target.value)} /></label>
        <label><span>Environment</span><input value={environment} placeholder={profile?.default_environment ?? 'Tenant default'} onChange={(event) => setEnvironment(event.target.value)} /></label>
        {executionKind === 'model_chat' && <label><span>Model</span><select value={modelAlias} onChange={(event) => { setModelAlias(event.target.value); setExactModel(null) }}><option value="">Select enabled model</option>{exactModel && !profile?.models.some((model) => model.alias === exactModel.manifest_ref) && <option value={exactModel.manifest_ref}>{exactModel.manifest_ref} (restored exact binding)</option>}{profile?.models.map((model) => <option key={model.alias}>{model.alias}</option>)}</select>{exactModel && <small>Restored exact deployment: {exactModel.deployment.deployment_id}. Choose a model explicitly to replace it.</small>}</label>}
        {executionKind !== 'deterministic' && <label className="field--wide"><span>Instructions{(executionKind === 'full_plan' || executionKind === 'framework_graph') ? ' (optional)' : ''}</span><textarea rows={5} value={instructions} onChange={(event) => setInstructions(event.target.value)} /></label>}
        <label><span>Input schema path</span><input value={inputSchemaPath} onChange={(event) => setInputSchemaPath(event.target.value)} /></label>
        <label><span>Output schema path</span><input value={outputSchemaPath} onChange={(event) => setOutputSchemaPath(event.target.value)} /></label>
        {(executionKind === 'full_plan' || executionKind === 'framework_graph') && <label><span>Plan path</span><input value={planPath} onChange={(event) => setPlanPath(event.target.value)} /></label>}
      </div> : <label className="json-field"><span>agent.yaml</span><textarea rows={20} value={yaml} onChange={(event) => setYaml(event.target.value)} spellCheck={false} /></label>}
      <div className="form-grid">
        <label className="field--wide"><span>Input schema JSON</span><textarea rows={8} value={inputSchema} onChange={(event) => setInputSchema(event.target.value)} spellCheck={false} /></label>
        <label className="field--wide"><span>Output schema JSON</span><textarea rows={8} value={outputSchema} onChange={(event) => setOutputSchema(event.target.value)} spellCheck={false} /></label>
      </div>
      {((executionKind === 'full_plan' || executionKind === 'framework_graph') || mode === 'yaml') && <div className="stack">
        <PlanEditor source={plan} onChange={(next) => { invalidate(); setPlan(next) }} compiled={compiled} disabled={busy} />
        <label className="json-field"><span>Plan JSON</span><textarea rows={18} value={plan} onChange={(event) => setPlan(event.target.value)} placeholder="Paste the complete Plan referenced by execution.plan" spellCheck={false} /></label>
        <p className="body-copy">Supply Full Plan JSON or a static framework graph export with explicit Platform nodes, schema documents, and dependency slots. Framework exports recover at Platform nodes; Python/JavaScript programs and dynamic graph code are unsupported. Validate checks the complete source before publication.</p>
        {outline && <details><summary>Node outline · {outline.nodes.length}{outline.truncated ? '+' : ''} nodes</summary><table><thead><tr><th>Node</th><th>Kind</th><th>Entry</th></tr></thead><tbody>{outline.nodes.map((node) => <tr key={node.id}><td>{node.id}</td><td>{node.kind}</td><td>{node.entry ? 'Entry' : ''}</td></tr>)}</tbody></table><p className="body-copy">{outline.truncated ? 'Showing the first 128 nodes. ' : ''}This outline shows the supplied JSON; use Validate to check it.</p></details>}
      </div>}
      {((executionKind === 'full_plan' || executionKind === 'framework_graph') || mode === 'yaml' || slotBindings.trim() !== '[]') && <label className="json-field"><span>Exact slot bindings JSON</span><textarea rows={9} value={slotBindings} onChange={(event) => setSlotBindings(event.target.value)} spellCheck={false} /><small>Supply the exact slot inputs returned by binding resolution. Context inputs carry logical deployment and policies; the compiler freezes their execution binding. Use [] when the source has no dependency slots.</small></label>}

      <AuthoringBindingsPanel key={authoringScope} client={client} disabled={busy} onResolved={(slots) => { invalidate(); setSlotBindings(JSON.stringify(slots, null, 2)) }} />
      </fieldset>
      {stage && <PublicationProgress stage={stage} />}
      <div className="actions"><button className="button" onClick={validate} disabled={busy}>Validate</button><button className="button" onClick={() => downloadSource('agent.yaml', currentManifest(), 'application/yaml')}>Export YAML</button><button className="button" onClick={exportBundle} disabled={busy}>Export source bundle</button>{compiled?.executionKind === 'deterministic' && <button className="button" onClick={useValidatedPlan} disabled={busy}>Edit validated Plan</button>}<button className="button button--primary" onClick={publish} disabled={busy}>{busy ? 'Working…' : 'Publish'}</button></div>
      {compiled && <details className="diagnostics"><summary>Advanced diagnostics</summary><dl className="metrics"><Metric label="Manifest digest" value={compiled.manifestDigest} mono /><Metric label="Plan digest" value={compiled.typedPlanDigest} mono /><Metric label="Source map digest" value={compiled.sourceMapDigest} mono /><Metric label="Resource ID" value={existing?.resource_id} mono /><Metric label="ETag" value={existing?.etag} mono /></dl><button className="button" onClick={() => downloadSource('source-map.json', compiled.sourceMap, 'application/json')}>Export compiled source map</button></details>}
      {recoveredCompilation && <details><summary>Verified recovered source artifacts</summary><p className="body-copy">These files preserve the exact source package and its Rust source map before your local edits.</p><div className="actions"><button className="button" onClick={() => downloadSource('recovered-agent.sources.json', recoveredCompilation.sourceBundle, 'application/json')}>Export recovered source bundle</button><button className="button" onClick={() => downloadSource('recovered-source-map.json', recoveredCompilation.sourceMap, 'application/json')}>Export recovered source map</button></div></details>}
    </article>}
  </section>
}

function PublicationProgress({ stage }: { stage: PublicationStage }) {
  const stages: Array<{ id: PublicationStage; label: string }> = [
    { id: 'validating', label: 'Validating' },
    { id: 'publishing', label: 'Publishing' },
    { id: 'activating', label: 'Activating' },
    { id: 'ready', label: 'Ready' },
  ]
  const active = stages.findIndex((item) => item.id === stage)
  return <ol className="publish-progress" aria-live="polite" aria-label={`Publication ${stage}`}>{stages.map((item, index) => <li className={index <= active ? 'complete' : ''} key={item.id}><span>{index + 1}</span>{item.label}</li>)}</ol>
}

function Runs({ client, report, launchAgent, onTask }: { client: PlatformClient | null; report: (notice: Notice | null) => void; launchAgent: AgentSummary | null; onTask: (id: string) => void }) {
  const [id, setId] = useState('')
  const [activeRunId, setActiveRunId] = useState('')
  const [run, setRun] = useState<RunView | null>(null)
  const [result, setResult] = useState<JsonObject | null>(null)
  const [events, setEvents] = useState<RunEvent[]>([])
  const [cursor, setCursor] = useState('')
  const [followError, setFollowError] = useState<Notice | null>(null)
  const [history, setHistory] = useState<RunEventHistory | null>(null)
  const followController = useRef<AbortController | null>(null)
  const refreshRun = useRef<() => void>(() => {})
  const selectionGeneration = useRef(0)
  const [busy, setBusy] = useState(false)
  const [summaries, setSummaries] = useState<RunSummary[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [stateFilter, setStateFilter] = useState('')
  const [agentFilter, setAgentFilter] = useState('')
  const [runAgent, setRunAgent] = useState<AgentSummary | null>(launchAgent)
  const [runDefaults, setRunDefaults] = useState<Awaited<ReturnType<typeof publishedRunDefaults>> | null>(null)
  const createIntent = useRef<{ intent: string; body: JsonObject; receipt: string } | null>(null)
  const creating = useRef(false)
  const [input, setInput] = useState('{\n  "message": "hello"\n}')

  useEffect(() => {
    if (!launchAgent || !client) return
    const controller = new AbortController()
    queueMicrotask(() => { if (!controller.signal.aborted) { setRunDefaults(null); createIntent.current=null } })
    publishedRunDefaults(client, launchAgent, controller.signal)
      .then((defaults) => { if (!controller.signal.aborted) setRunDefaults(defaults) })
      .catch((error: unknown) => { if (!controller.signal.aborted) report(errorNotice(error)) })
    return () => { controller.abort() }
  }, [client, launchAgent, report])

  useEffect(() => {
    const controller = new AbortController()
    const { signal } = controller
    followController.current = controller
    const clear = () => { setRun(null); setResult(null); setEvents([]); setCursor(''); setHistory(null); setBusy(false) }
    const invalidateSelection = () => { selectionGeneration.current++ }
    if (!client || !activeRunId) {
      refreshRun.current = () => {}
      return () => { controller.abort() }
    }

    let reading = false
    let refreshRequested = false
    let lastEventId: unknown
    // Coalesce event bursts and serialize reads. Events only request an authority refresh.
    const readCurrentRun = async () => {
      refreshRequested = true
      if (reading) return
      reading = true
      try {
        while (refreshRequested && !signal.aborted) {
          refreshRequested = false
          const current = await client.getRun(activeRunId, { signal })
          if (signal.aborted) return
          setRun((existing) => existing && existing.version > current.data.version ? existing : current.data)
          if (TERMINAL_RUNS.has(current.data.state)) {
            try {
              const output = await client.getRunResult(activeRunId, { signal })
              if (!signal.aborted) setResult(output.data)
            } catch (error) {
              if (!signal.aborted) setResult(null)
              if (!(error instanceof PlatformProblem) || error.status !== 409) throw error
            }
          }
        }
      } catch (error) {
        if (!signal.aborted) {
          setResult(null)
          if (error instanceof PlatformProblem && [401, 403].includes(error.status)) { controller.abort(); clear() }
          report(errorNotice(error))
        }
      } finally {
        reading = false
        if (!signal.aborted) setBusy(false)
      }
    }
    refreshRun.current = () => {
      if (signal.aborted) return
      setBusy(true)
      void readCurrentRun()
    }
    void client.followRunEvents(activeRunId, {
      signal,
      onClear: clear,
      onHistory(snapshot) { if (!signal.aborted) setHistory((previous) => ({ ...snapshot, truncated: Boolean(previous?.truncated || snapshot.truncated) })) },
      onUpdate(snapshot) {
        if (signal.aborted) return
        setEvents([...snapshot.events])
        setCursor(snapshot.cursor ?? '')
        const nextEventId = snapshot.events.at(-1)?.data.event_id
        if (nextEventId !== lastEventId) {
          lastEventId = nextEventId
          void readCurrentRun()
        }
      },
    }).catch((error: unknown) => {
      if (signal.aborted) return
      if (error instanceof PlatformProblem && [401, 403].includes(error.status)) controller.abort()
      const detail = error instanceof Error ? error.message : 'The event stream could not continue.'
      setFollowError({ tone: 'error', text: `Timeline following stopped. ${detail} Earlier history may be unavailable. Refresh reads the current Run; it does not restart or fill missing history.`, traceId: error instanceof PlatformProblem ? error.traceId : null })
      setBusy(false)
    })
    refreshRun.current()
    const onVisible = () => { if (document.visibilityState === 'visible') void readCurrentRun() }
    document.addEventListener('visibilitychange', onVisible)
    return () => {
      controller.abort()
      document.removeEventListener('visibilitychange', onVisible)
      refreshRun.current = () => {}
      invalidateSelection()
    }
  }, [activeRunId, client, report])

  const clearSelection = () => {
    selectionGeneration.current++
    followController.current?.abort()
    setActiveRunId('')
    setRun(null)
    setResult(null)
    setEvents([])
    setCursor('')
    setFollowError(null)
    setHistory(null)
    setBusy(false)
  }

  const loadList = async (pageCursor?: string) => {
    if (!client) return report({ tone: 'error', text: 'Connect to a valid Gateway first.' })
    setBusy(true)
    report(null)
    try {
      const response = await client.listRuns({ agentId: agentFilter || undefined, state: stateFilter || undefined, cursor: pageCursor })
      setSummaries(response.data.items)
      setNextCursor(response.data.next_cursor)
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }

  const load = (selectedId = id.trim()) => {
    if (!client) return report({ tone: 'error', text: 'Connect to a valid Gateway first.' })
    if (!selectedId) return
    if (selectedId === activeRunId) { refreshRun.current(); return }
    clearSelection()
    setId(selectedId)
    setActiveRunId(selectedId)
    setBusy(true)
    report(null)
  }

  const create = async () => {
    if (!client || !runAgent || !runDefaults || creating.current) return
    creating.current = true
    const generation = selectionGeneration.current
    setBusy(true)
    report(null)
    try {
      const value = JSON.parse(input) as JsonObject
      const intent=JSON.stringify({agent:runAgent.agent_id,defaults:runDefaults,input:value})
      if(createIntent.current?.intent !== intent) createIntent.current={intent,receipt:newReceipt(`run-create-${runAgent.agent_id}`),body:{
        agent_id:runAgent.agent_id,expected_agent_deployment:runDefaults.exactDeployment,
        input:{classification:runDefaults.classification,schema_digest:runDefaults.schemaDigest,value:{kind:'inline',value}},
        // oxlint-disable-next-line react/purity -- Freeze one deadline at the explicit submission intent.
        deadline:utcTimestamp(new Date(Date.now()+runDefaults.deadlineSeconds*1000)),
      }}
      const response=await client.createRun(createIntent.current.body,createIntent.current.receipt)
      if (generation !== selectionGeneration.current) return
      createIntent.current=null
      setRunAgent(null)
      setRunDefaults(null)
      setId(response.data.run_id)
      clearSelection()
      setActiveRunId(response.data.run_id)
      report({ tone: 'success', text: `${runAgent.name} started.` })
    } catch (error) { if (generation === selectionGeneration.current) report(errorNotice(error)) } finally { creating.current = false; if (generation === selectionGeneration.current) setBusy(false) }
  }

  const act = async (action: 'pause' | 'resume' | 'cancel') => {
    if (!client || !run) return
    const controller = followController.current
    setBusy(true)
    report(null)
    try {
      const response = await client.runAction(run.run_id, action, run.etag, newReceipt(`run-${action}-${run.run_id}-v${run.version}`))
      if (controller?.signal.aborted) return
      setRun((existing) => existing && existing.version > response.data.version ? existing : response.data)
      refreshRun.current()
      report({ tone: 'success', text: `${action} committed.`, traceId: response.traceId })
    } catch (error) { if (!controller?.signal.aborted) report(errorNotice(error)) } finally { if (!controller?.signal.aborted) setBusy(false) }
  }
  const taskIds = discoverTaskIds(events)

  return <section className="stack">
    {runAgent && <article className="panel"><p className="kicker">NEW RUN</p><h2>Run {runAgent.display_name}</h2><p className="body-copy">Defaults come from the selected published deployment. If activation changes, refresh Agents and choose again.</p><label className="json-field"><span>Input JSON</span><textarea rows={8} value={input} onChange={(event) => setInput(event.target.value)} spellCheck={false} /></label><button className="button button--primary" onClick={create} disabled={busy || !runDefaults}>Start Run</button></article>}
    <article className="panel"><div className="panel__heading"><div><p className="kicker">RECENT RUNS</p><h2>Bounded server history</h2></div><button className="button" onClick={() => loadList()} disabled={busy}>Refresh</button></div><div className="filters"><label><span>Agent ID</span><input value={agentFilter} onChange={(event) => setAgentFilter(event.target.value)} placeholder="Optional" /></label><label><span>State</span><select value={stateFilter} onChange={(event) => setStateFilter(event.target.value)}><option value="">All states</option>{['queued', 'running', 'waiting', 'cancelling', 'succeeded', 'failed', 'cancelled', 'timed_out'].map((state) => <option key={state}>{state}</option>)}</select></label></div>{summaries.map((summary) => <button className="run-row" key={summary.run_id} onClick={() => load(summary.run_id)}><span><strong>{summary.agent_name}</strong><small>{formatTime(summary.started_at)}</small></span><Status value={summary.state} /><span>{summary.waiting_task_count ? `${summary.waiting_task_count} waiting tasks` : summary.result_available ? 'Result ready' : 'In progress'}</span></button>)}{nextCursor && <button className="button" onClick={() => loadList(nextCursor)}>Next page</button>}</article>
    <article className="panel"><SearchForm label="Open Run by ID" placeholder="run_…" value={id} onChange={(value) => { clearSelection(); setId(value) }} onSubmit={() => load()} busy={busy} /></article>
    {run && <article className="panel"><div className="panel__heading"><div><p className="kicker">RUN</p><h2>{run.state === 'succeeded' ? 'Completed' : 'Current progress'}</h2></div><Status value={run.state} /></div><dl className="metrics"><Metric label="Started" value={formatTime(run.started_at)} /><Metric label="Updated" value={formatTime(run.updated_at)} /><Metric label="Deadline" value={formatTime(run.deadline)} /></dl><div className="actions"><button className="button" onClick={() => act('pause')} disabled={busy}>Pause</button><button className="button" onClick={() => act('resume')} disabled={busy}>Resume</button><button className="button button--danger" onClick={() => act('cancel')} disabled={busy}>Cancel</button><button className="button" onClick={() => load()} disabled={busy}>Refresh</button></div><details className="diagnostics"><summary>Advanced diagnostics</summary><dl className="metrics"><Metric label="Run ID" value={run.run_id} mono /><Metric label="Version" value={run.version} /><Metric label="Agent deployment" value={run.agent_deployment_id} mono /><Metric label="ETag" value={run.etag} mono /><Metric label="Cursor" value={cursor || 'origin'} mono /></dl></details></article>}
    {history?.truncated && <div className="notice notice--info" role="status">Earlier Run history is unavailable in this timeline. Retained replay floor: {history.replayFloor}; observed high water: {history.highWaterSequence}. Current Run state is read separately.</div>}
    <NoticeBox notice={followError} />
    {run && events.length === 0 && <article className="panel empty-state" role="status"><p className="kicker">DURABLE TIMELINE</p><h2>No public events in this bounded page</h2><p className="body-copy">{followError ? 'Following has stopped. Current Run reads remain separate from timeline history.' : 'Following committed progress automatically. The opaque resume cursor stays in this session’s memory.'}</p></article>}
    {events.length > 0 && <article className="panel"><div className="panel__heading"><div><p className="kicker">DURABLE TIMELINE</p><h2>{events.length} public events</h2></div></div><ol className="timeline">{events.map((event) => <li key={String(event.data.event_id)}><span className="timeline__dot" /><div><div className="timeline__header"><strong>{event.event}</strong></div><pre>{safeJson(event.data)}</pre><details><summary>Event diagnostics</summary><code>{event.id}</code></details></div></li>)}</ol>{taskIds.length > 0 && <div className="linked-tasks"><strong>Waiting tasks</strong>{taskIds.map((taskId) => <button className="button" key={taskId} onClick={() => onTask(taskId)}>Open task</button>)}</div>}</article>}
    {run && client && <RunSignal key={`signal:${run.run_id}`} client={client} runId={run.run_id} onAccepted={() => refreshRun.current()} onPermissionLost={clearSelection} />}
    {run && client && <RunValues key={run.run_id} client={client} runId={run.run_id} />}
    {run && client && <RunSources key={`sources:${run.run_id}`} client={client} run={run} />}
    {result && client && <article className="panel"><p className="kicker">TYPED RESULT</p><h2>Result</h2><AuthorizedContent client={client} content={result} onError={(error) => { setResult(null); report(errorNotice(error)) }} /></article>}
  </section>
}

function useSelectionGeneration() {
  const generation = useRef(0)
  useEffect(() => () => { generation.current++ }, [])
  return {
    next: () => ++generation.current,
    current: () => generation.current,
    accepts: (request: number) => request === generation.current,
  }
}

function Settings({ client, report, tenant, setTenant, ready, endpoint }: { client: PlatformClient | null; report: (notice: Notice | null) => void; tenant: string; setTenant: (value: string) => void; ready: boolean | null; endpoint: string }) {
  const [tenantLabel, setTenantLabel] = useState(tenant)
  const [diagnostic, setDiagnostic] = useState<'artifact' | 'operation'>('artifact')
  return <section className="stack"><article className="panel"><div className="panel__heading"><div><p className="kicker">SESSION</p><h2>Gateway and project readiness</h2></div><Status value={ready === null ? 'unchecked' : ready ? 'ready' : 'unavailable'} /></div><dl className="metrics"><Metric label="Gateway" value={endpoint} /><Metric label="Contract" value="insight.platform/v1" /><Metric label="Credential storage" value="Memory only" /></dl><label className="secondary-field"><span>Project / tenant label</span><input value={tenantLabel} onChange={(event) => setTenantLabel(event.target.value)} onBlur={() => setTenant(tenantLabel)} maxLength={128} /></label></article><article className="panel"><p className="kicker">FEATURE READINESS</p><h2>Resolved at publish time</h2><p className="body-copy">The Console reads exact tenant authoring bindings before compiling. Missing or disabled model and policy features fail closed; there are no bundle defaults.</p></article><article className="panel"><div className="panel__heading"><div><p className="kicker">ADVANCED DIAGNOSTICS</p><h2>Artifact and background operation lookup</h2></div><div className="segmented"><button className={diagnostic === 'artifact' ? 'active' : ''} onClick={() => setDiagnostic('artifact')}>Artifacts</button><button className={diagnostic === 'operation' ? 'active' : ''} onClick={() => setDiagnostic('operation')}>Operations</button></div></div>{diagnostic === 'artifact' ? <Artifacts client={client} report={report} /> : <Operations client={client} report={report} />}</article></section>
}

function Artifacts({ client, report }: { client: PlatformClient | null; report: (notice: Notice | null) => void }) {
  const [id, setId] = useState('')
  const generation = useSelectionGeneration()
  const [artifact, setArtifact] = useState<ArtifactView | null>(null)
  const [busy, setBusy] = useState(false)
  const load = async () => { if (!client) return; const request = generation.next(); setBusy(true); try { const current = await client.getArtifact(id.trim()); if (!generation.accepts(request)) return; setArtifact(current.data) } catch (error) { if (generation.accepts(request)) report(errorNotice(error)) } finally { if (generation.accepts(request)) setBusy(false) } }
  const download = async () => { if (!client || !artifact) return; const request = generation.current(); setBusy(true); try { const content = await client.downloadArtifact(artifact.artifact_id); if (!generation.accepts(request)) return; if (content.blob.size !== artifact.expected_size_bytes) throw new Error('artifact_size_mismatch'); const url = URL.createObjectURL(content.blob); const anchor = document.createElement('a'); anchor.href = url; anchor.download = artifact.artifact_id; anchor.click(); URL.revokeObjectURL(url) } catch (error) { if (generation.accepts(request)) report(errorNotice(error)) } finally { if (generation.accepts(request)) setBusy(false) } }
  return <div className="nested-panel"><SearchForm label="Artifact ID" placeholder="art_…" value={id} onChange={(value) => { generation.next(); setId(value); setArtifact(null); setBusy(false) }} onSubmit={load} busy={busy} />{artifact && <><dl className="metrics"><Metric label="Artifact ID" value={artifact.artifact_id} mono /><Metric label="State" value={artifact.state} /><Metric label="Purpose" value={artifact.purpose} /><Metric label="Size" value={`${artifact.expected_size_bytes} bytes`} /></dl><button className="button button--primary" disabled={artifact.state !== 'ready'} onClick={download}>Controlled download</button></>}</div>
}

function Operations({ client, report }: { client: PlatformClient | null; report: (notice: Notice | null) => void }) {
  const [id, setId] = useState('')
  const generation = useSelectionGeneration()
  const [operation, setOperation] = useState<OperationView | null>(null)
  const [busy, setBusy] = useState(false)
  const load = async () => { if (!client) return; const request = generation.next(); setBusy(true); try { const current = await client.getOperation(id.trim()); if (!generation.accepts(request)) return; setOperation(current.data) } catch (error) { if (generation.accepts(request)) report(errorNotice(error)) } finally { if (generation.accepts(request)) setBusy(false) } }
  return <div className="nested-panel"><SearchForm label="Operation ID" placeholder="job_…" value={id} onChange={(value) => { generation.next(); setId(value); setOperation(null); setBusy(false) }} onSubmit={load} busy={busy} />{operation && <><dl className="metrics"><Metric label="State" value={operation.state} /><Metric label="Kind" value={operation.kind} /><Metric label="Updated" value={formatTime(operation.updated_at)} /></dl>{operation.error && <div className="notice notice--error">{operation.error.message}</div>}</>}</div>
}

export default App

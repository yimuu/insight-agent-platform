import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { stringify as stringifyYaml } from 'yaml'
import { PlatformClient, PlatformProblem } from './api/client'
import { discoverTaskIds, newReceipt, safeJson } from './api/security'
import {
  compileAgentManifest,
  inspectAgentManifest,
  verifyAgentAuthoringProfile,
} from './agent/compiler'
import type { CompiledAgent } from './agent/compiler'
import {
  clearPublicationRecovery,
  publishCompiledAgent,
} from './agent/publication'
import type { PublicationStage } from './agent/publication'
import type {
  AgentAuthoringProfile,
  AgentSummary,
  ArtifactView,
  JsonObject,
  OperationView,
  ResourceView,
  RunEvent,
  RunSummary,
  RunView,
  TaskView,
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
  if (!notice) return null
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
  const [notice, setNotice] = useState<Notice | null>(null)
  const [selectedTask, setSelectedTask] = useState('')
  const [launchAgent, setLaunchAgent] = useState<AgentSummary | null>(null)

  const client = useMemo(() => { try { return new PlatformClient(endpoint, token) } catch { return null } }, [endpoint, token])
  const connect = async (event: FormEvent) => {
    event.preventDefault()
    setNotice(null)
    try {
      const next = new PlatformClient(endpoint, token)
      const isReady = await next.readiness()
      setReady(isReady)
      setNotice(isReady
        ? { tone: 'success', text: 'Gateway is ready. Credentials remain in browser memory only.' }
        : { tone: 'error', text: 'Gateway readiness endpoint is not ready.' })
    } catch (error) {
      setReady(false)
      setNotice(errorNotice(error))
    }
  }

  const runAgent = (agent: AgentSummary) => {
    setLaunchAgent(agent)
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
      <NoticeBox notice={notice} />
      {view === 'agents' && <Agents client={client} report={setNotice} onRun={runAgent} />}
      {view === 'runs' && <Runs client={client} report={setNotice} launchAgent={launchAgent} onTask={(id) => { setSelectedTask(id); setView('tasks') }} />}
      {view === 'tasks' && <Tasks key={selectedTask || 'direct'} client={client} report={setNotice} selectedId={selectedTask} />}
      {view === 'settings' && <Settings client={client} report={setNotice} tenant={tenant} setTenant={setTenant} ready={ready} endpoint={endpoint} />}
    </main>
  </div>
}

function buildFormManifest(values: { name: string; displayName: string; executionKind: 'deterministic' | 'model_chat'; instructions: string; modelAlias: string; classification: string; deadline: string; environment: string }): string {
  return stringifyYaml({
    apiVersion: 'insight.platform/v1',
    kind: 'Agent',
    metadata: { name: values.name, displayName: values.displayName || undefined },
    spec: {
      execution: { kind: values.executionKind },
      instructions: values.executionKind === 'model_chat' ? values.instructions || null : null,
      model: values.executionKind === 'model_chat' ? { ref: values.modelAlias } : null,
      input: { schema: 'input.schema.json', classification: values.classification },
      output: { schema: 'output.schema.json' },
      limits: values.deadline ? { deadlineSeconds: Number(values.deadline) } : null,
      publish: values.environment ? { environment: values.environment } : null,
    },
  }, { lineWidth: 0 })
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
  const [executionKind, setExecutionKind] = useState<'deterministic' | 'model_chat'>('deterministic')
  const [instructions, setInstructions] = useState('Respond with a concise typed answer.')
  const [modelAlias, setModelAlias] = useState('')
  const [classification, setClassification] = useState('internal')
  const [deadline, setDeadline] = useState('')
  const [environment, setEnvironment] = useState('')
  const [inputSchema, setInputSchema] = useState(DEFAULT_SCHEMA)
  const [outputSchema, setOutputSchema] = useState(DEFAULT_SCHEMA)
  const [yaml, setYaml] = useState('')
  const [profile, setProfile] = useState<AgentAuthoringProfile | null>(null)

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

  const openNew = () => {
    setExisting(null)
    setCompiled(null)
    setStage(null)
    setEditor(true)
  }

  const openExisting = async (summary: AgentSummary) => {
    if (!client) return
    setBusy(true)
    report(null)
    try {
      const response = await client.getResource('agents', summary.agent_id)
      const document = response.data.draft.document as { spec?: Record<string, unknown> }
      const spec = document.spec ?? {}
      setExisting(response.data)
      setName(String(spec.authoring_name ?? summary.name))
      setDisplayName(response.data.draft.display_name)
      setInstructions(String(spec.author_instructions ?? ''))
      setClassification(String(spec.input_classification ?? 'internal'))
      setDeadline(String(spec.default_deadline_seconds ?? ''))
      const input = spec.input_schema as { schema?: unknown } | undefined
      const output = spec.output_schema as { schema?: unknown } | undefined
      if (input?.schema) setInputSchema(JSON.stringify(input.schema, null, 2))
      if (output?.schema) setOutputSchema(JSON.stringify(output.schema, null, 2))
      setEditor(true)
      report({ tone: 'info', text: 'Loaded the current Agent draft. Publishing uses its exact ETag and stops on concurrent edits.', traceId: response.traceId })
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }

  const currentManifest = () => mode === 'yaml'
    ? yaml
    : buildFormManifest({ name, displayName, executionKind, instructions, modelAlias, classification, deadline, environment })

  const compile = async (): Promise<CompiledAgent> => {
    if (!client) throw new Error('Connect to a valid Gateway first.')
    const authoring = await client.getAgentAuthoringProfile()
    await verifyAgentAuthoringProfile(authoring.data)
    const manifest = currentManifest()
    const inspected = inspectAgentManifest(manifest)
    const model = inspected.modelRef === null
      ? null
      : authoring.data.models.find((candidate) => candidate.alias === inspected.modelRef)
    if (inspected.modelRef !== null && !model) {
      throw new Error(`agent_binding_not_ready: Model ${inspected.modelRef} is not enabled by this tenant`)
    }
    const result = await compileAgentManifest({
      manifest,
      inputSchema,
      outputSchema,
      profile: authoring.data,
      bindings: { model: model ? { manifest_ref: model.alias, deployment: model.deployment, selection_policy: model.selection_policy } : null },
    })
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
    if (file.size > 1_048_576) return report({ tone: 'error', text: 'agent.yaml exceeds the 1 MiB authoring limit.' })
    setYaml(await file.text())
    setMode('yaml')
    setEditor(true)
  }

  const exportYaml = () => {
    const blob = new Blob([currentManifest()], { type: 'application/yaml' })
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = 'agent.yaml'
    anchor.click()
    URL.revokeObjectURL(url)
  }

  return <section className="stack">
    <article className="panel toolbar"><div><p className="kicker">YOUR AGENTS</p><h2>Create, publish, and run</h2></div><div className="actions"><button className="button" onClick={() => loadPage()} disabled={busy}>Refresh</button><label className="button file-button">Import agent.yaml<input type="file" accept=".yaml,.yml,text/yaml" onChange={(event) => importYaml(event.target.files?.[0])} /></label><button className="button button--primary" onClick={openNew}>New Agent</button></div></article>
    {agents.length === 0 && <article className="panel empty-state" role="status"><p className="kicker">EMPTY TENANT</p><h2>No Agents loaded</h2><p className="body-copy">Refresh the list or create a deterministic Agent. No internal IDs are needed.</p></article>}
    {agents.length > 0 && <article className="panel"><div className="agent-list" role="list">{agents.map((agent) => <div className="agent-row" role="listitem" key={agent.agent_id}><div><strong>{agent.display_name}</strong><span>{agent.name}</span></div><Status value={agent.state} /><span>{agent.environment ?? 'Not deployed'}</span><span>{formatTime(agent.published_at)}</span><div className="actions"><button className="button" onClick={() => openExisting(agent)}>Edit</button><button className="button button--primary" disabled={agent.state !== 'ready'} onClick={() => onRun(agent)}>Run</button></div></div>)}</div>{cursor && <button className="button" onClick={() => loadPage(cursor)} disabled={busy}>Next page</button>}</article>}
    {editor && <article className="panel editor"><div className="panel__heading"><div><p className="kicker">{existing ? 'EDIT AGENT' : 'NEW AGENT'}</p><h2>{existing ? displayName : 'Define an Agent'}</h2></div><div className="segmented"><button className={mode === 'form' ? 'active' : ''} onClick={() => setMode('form')}>Form</button><button className={mode === 'yaml' ? 'active' : ''} onClick={() => { if (!yaml) setYaml(buildFormManifest({ name, displayName, executionKind, instructions, modelAlias, classification, deadline, environment })); setMode('yaml') }}>YAML</button></div></div>
      {mode === 'form' ? <div className="form-grid"><label><span>Name</span><input value={name} disabled={Boolean(existing)} onChange={(event) => setName(event.target.value)} /></label><label><span>Display name</span><input value={displayName} onChange={(event) => setDisplayName(event.target.value)} /></label><label><span>Execution</span><select value={executionKind} onChange={(event) => setExecutionKind(event.target.value as 'deterministic' | 'model_chat')}><option value="deterministic">Deterministic</option><option value="model_chat">Model chat</option></select></label><label><span>Classification</span><select value={classification} onChange={(event) => setClassification(event.target.value)}><option>public</option><option>internal</option><option>confidential</option><option>restricted</option></select></label><label><span>Deadline seconds</span><input inputMode="numeric" value={deadline} placeholder={profile ? String(profile.default_deadline_seconds) : 'Tenant default'} onChange={(event) => setDeadline(event.target.value)} /></label><label><span>Environment</span><input value={environment} placeholder={profile?.default_environment ?? 'Tenant default'} onChange={(event) => setEnvironment(event.target.value)} /></label>{executionKind === 'model_chat' && <><label><span>Model</span><select value={modelAlias} onChange={(event) => setModelAlias(event.target.value)}><option value="">Select enabled model</option>{profile?.models.map((model) => <option key={model.alias}>{model.alias}</option>)}</select></label><label className="field--wide"><span>Instructions</span><textarea rows={5} value={instructions} onChange={(event) => setInstructions(event.target.value)} /></label></>}<label className="field--wide"><span>Input schema</span><textarea rows={8} value={inputSchema} onChange={(event) => setInputSchema(event.target.value)} spellCheck={false} /></label><label className="field--wide"><span>Output schema</span><textarea rows={8} value={outputSchema} onChange={(event) => setOutputSchema(event.target.value)} spellCheck={false} /></label></div> : <label className="json-field"><span>agent.yaml</span><textarea rows={24} value={yaml} onChange={(event) => setYaml(event.target.value)} spellCheck={false} /></label>}
      {stage && <PublicationProgress stage={stage} />}
      <div className="actions"><button className="button" onClick={validate} disabled={busy}>Validate</button><button className="button" onClick={exportYaml}>Export YAML</button><button className="button button--primary" onClick={publish} disabled={busy}>{busy ? 'Working…' : 'Publish'}</button><button className="button" onClick={() => { clearPublicationRecovery(); setStage(null) }}>Discard recovery handle</button></div>
      {compiled && <details className="diagnostics"><summary>Advanced diagnostics</summary><dl className="metrics"><Metric label="Manifest digest" value={compiled.manifestDigest} mono /><Metric label="Plan digest" value={compiled.typedPlanDigest} mono /><Metric label="Resource ID" value={existing?.resource_id} mono /><Metric label="ETag" value={existing?.etag} mono /></dl></details>}
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
  const [run, setRun] = useState<RunView | null>(null)
  const [result, setResult] = useState<JsonObject | null>(null)
  const [events, setEvents] = useState<RunEvent[]>([])
  const [cursor, setCursor] = useState('')
  const [busy, setBusy] = useState(false)
  const [summaries, setSummaries] = useState<RunSummary[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [stateFilter, setStateFilter] = useState('')
  const [agentFilter, setAgentFilter] = useState('')
  const [runAgent, setRunAgent] = useState<AgentSummary | null>(launchAgent)
  const [agentResource, setAgentResource] = useState<ResourceView | null>(null)
  const [input, setInput] = useState('{\n  "message": "hello"\n}')

  useEffect(() => {
    if (!launchAgent || !client) return
    client.getResource('agents', launchAgent.agent_id)
      .then((response) => setAgentResource(response.data))
      .catch((error: unknown) => report(errorNotice(error)))
  }, [client, launchAgent, report])

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

  const load = async (selectedId = id.trim()) => {
    if (!client) return report({ tone: 'error', text: 'Connect to a valid Gateway first.' })
    setBusy(true)
    report(null)
    try {
      const savedCursor = sessionStorage.getItem(`insight.console.run-cursor.${selectedId}`) ?? ''
      const activeCursor = cursor || savedCursor
      const current = await client.getRun(selectedId)
      const page = await client.getRunEvents(selectedId, activeCursor || undefined)
      setId(selectedId)
      setRun(current.data)
      setEvents((existingEvents) => [...existingEvents, ...page].slice(-128))
      if (page.length) {
        const next = page.at(-1)!.id
        setCursor(next)
        sessionStorage.setItem(`insight.console.run-cursor.${selectedId}`, next)
      }
      if (TERMINAL_RUNS.has(current.data.state)) {
        try { setResult((await client.getRunResult(selectedId)).data) } catch (error) { if (!(error instanceof PlatformProblem) || error.status !== 409) throw error }
      }
      report({ tone: 'success', text: page.length ? `Loaded ${page.length} durable events.` : 'Run loaded; no newer durable events.', traceId: current.traceId })
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }

  const create = async () => {
    if (!client || !runAgent || !agentResource) return
    setBusy(true)
    report(null)
    try {
      const value = JSON.parse(input) as JsonObject
      const document = agentResource.draft.document as { spec?: Record<string, unknown> }
      const spec = document.spec ?? {}
      const inputSchema = spec.input_schema as { canonical_digest?: unknown } | undefined
      const seconds = Number(spec.default_deadline_seconds)
      if (!inputSchema?.canonical_digest || !Number.isInteger(seconds) || seconds < 1) {
        throw new Error('agent_authority_invalid: Run defaults are missing from the exact Agent draft')
      }
      const response = await client.createRun({
        agent_id: runAgent.agent_id,
        input: {
          classification: spec.input_classification,
          schema_digest: inputSchema.canonical_digest,
          value: { kind: 'inline', value },
        },
        deadline: new Date(Date.now() + seconds * 1000).toISOString(),
      }, newReceipt(`run-create-${runAgent.agent_id}`))
      setRunAgent(null)
      setAgentResource(null)
      setId(response.data.run_id)
      setRun(response.data)
      setEvents([])
      setCursor('')
      setResult(null)
      report({ tone: 'success', text: `${runAgent.name} started.` })
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }

  const act = async (action: 'pause' | 'resume' | 'cancel') => {
    if (!client || !run) return
    setBusy(true)
    report(null)
    try {
      const response = await client.runAction(run.run_id, action, run.etag, newReceipt(`run-${action}-${run.run_id}-v${run.version}`))
      setRun(response.data)
      report({ tone: 'success', text: `${action} committed.`, traceId: response.traceId })
    } catch (error) { report(errorNotice(error)) } finally { setBusy(false) }
  }
  const taskIds = discoverTaskIds(events)

  return <section className="stack">
    {runAgent && <article className="panel"><p className="kicker">NEW RUN</p><h2>Run {runAgent.display_name}</h2><label className="json-field"><span>Input JSON</span><textarea rows={8} value={input} onChange={(event) => setInput(event.target.value)} spellCheck={false} /></label><button className="button button--primary" onClick={create} disabled={busy || !agentResource}>Start Run</button></article>}
    <article className="panel"><div className="panel__heading"><div><p className="kicker">RECENT RUNS</p><h2>Bounded server history</h2></div><button className="button" onClick={() => loadList()} disabled={busy}>Refresh</button></div><div className="filters"><label><span>Agent ID</span><input value={agentFilter} onChange={(event) => setAgentFilter(event.target.value)} placeholder="Optional" /></label><label><span>State</span><select value={stateFilter} onChange={(event) => setStateFilter(event.target.value)}><option value="">All states</option>{['queued', 'running', 'waiting', 'cancelling', 'succeeded', 'failed', 'cancelled', 'timed_out'].map((state) => <option key={state}>{state}</option>)}</select></label></div>{summaries.map((summary) => <button className="run-row" key={summary.run_id} onClick={() => load(summary.run_id)}><span><strong>{summary.agent_name}</strong><small>{formatTime(summary.started_at)}</small></span><Status value={summary.state} /><span>{summary.waiting_task_count ? `${summary.waiting_task_count} waiting tasks` : summary.result_available ? 'Result ready' : 'In progress'}</span></button>)}{nextCursor && <button className="button" onClick={() => loadList(nextCursor)}>Next page</button>}</article>
    <article className="panel"><SearchForm label="Open Run by ID" placeholder="run_…" value={id} onChange={(value) => { setId(value); setRun(null); setEvents([]); setCursor(''); setResult(null) }} onSubmit={() => load()} busy={busy} /></article>
    {run && <article className="panel"><div className="panel__heading"><div><p className="kicker">RUN</p><h2>{run.state === 'succeeded' ? 'Completed' : 'Current progress'}</h2></div><Status value={run.state} /></div><dl className="metrics"><Metric label="Started" value={formatTime(run.started_at)} /><Metric label="Updated" value={formatTime(run.updated_at)} /><Metric label="Deadline" value={formatTime(run.deadline)} /></dl><div className="actions"><button className="button" onClick={() => act('pause')} disabled={busy}>Pause</button><button className="button" onClick={() => act('resume')} disabled={busy}>Resume</button><button className="button button--danger" onClick={() => act('cancel')} disabled={busy}>Cancel</button><button className="button" onClick={() => load()} disabled={busy}>Refresh</button></div><details className="diagnostics"><summary>Advanced diagnostics</summary><dl className="metrics"><Metric label="Run ID" value={run.run_id} mono /><Metric label="Version" value={run.version} /><Metric label="Agent deployment" value={run.agent_deployment_id} mono /><Metric label="ETag" value={run.etag} mono /><Metric label="Cursor" value={cursor || 'origin'} mono /></dl></details></article>}
    {run && events.length === 0 && <article className="panel empty-state" role="status"><p className="kicker">DURABLE TIMELINE</p><h2>No public events in this bounded page</h2><p className="body-copy">Refresh to check committed progress. The opaque resume cursor stays out of the default page.</p></article>}
    {events.length > 0 && <article className="panel"><div className="panel__heading"><div><p className="kicker">DURABLE TIMELINE</p><h2>{events.length} public events</h2></div></div><ol className="timeline">{events.map((event, index) => <li key={`${event.id}-${index}`}><span className="timeline__dot" /><div><div className="timeline__header"><strong>{event.event}</strong></div><pre>{safeJson(event.data)}</pre><details><summary>Event diagnostics</summary><code>{event.id}</code></details></div></li>)}</ol>{taskIds.length > 0 && <div className="linked-tasks"><strong>Waiting tasks</strong>{taskIds.map((taskId) => <button className="button" key={taskId} onClick={() => onTask(taskId)}>Open task</button>)}</div>}</article>}
    {result && <article className="panel"><p className="kicker">TYPED RESULT</p><h2>Result</h2><pre>{safeJson(result)}</pre></article>}
  </section>
}

function Tasks({ client, report, selectedId }: { client: PlatformClient | null; report: (notice: Notice | null) => void; selectedId: string }) {
  const [id, setId] = useState(selectedId)
  const [task, setTask] = useState<TaskView | null>(null)
  const [busy, setBusy] = useState(false)
  const [response, setResponse] = useState('{\n  "classification": "internal",\n  "schema_digest": "sha256:…",\n  "value": { "kind": "inline", "value": {} }\n}')
  const load = async () => { if (!client) return report({ tone: 'error', text: 'Connect to a valid Gateway first.' }); setBusy(true); report(null); try { const current = await client.getTask(id.trim()); setTask(current.data); report({ tone: 'success', text: 'Task loaded.', traceId: current.traceId }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  const act = async (action: 'submit-input' | 'approve' | 'reject' | 'cancel') => { if (!client || !task) return; setBusy(true); report(null); try { const body = action === 'submit-input' ? JSON.parse(response) as JsonObject : undefined; const result = await client.taskAction(task.task_id, action, task.etag, newReceipt(`task-${action}-${task.task_id}-v${task.version}`), body); setTask(result.data); report({ tone: 'success', text: `${action} committed.`, traceId: result.traceId }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  return <section className="stack"><article className="panel"><SearchForm label="Task ID" placeholder="int_… or apv_…" value={id} onChange={setId} onSubmit={load} busy={busy} /></article>{task && <article className="panel"><div className="panel__heading"><div><p className="kicker">TASK</p><h2>{task.safe_prompt_key}</h2></div><Status value={task.state} /></div><p className="body-copy">Respond to the pending request, then return to its Run timeline.</p>{task.state === 'pending' && <><label className="json-field"><span>Typed response</span><textarea rows={7} value={response} onChange={(event) => setResponse(event.target.value)} spellCheck={false} /></label><div className="actions"><button className="button button--primary" onClick={() => act('submit-input')} disabled={busy}>Submit input</button><button className="button" onClick={() => act('approve')} disabled={busy}>Approve</button><button className="button button--danger" onClick={() => act('reject')} disabled={busy}>Reject</button><button className="button" onClick={() => act('cancel')} disabled={busy}>Cancel</button></div></>}<details className="diagnostics"><summary>Advanced diagnostics</summary><dl className="metrics"><Metric label="Task ID" value={task.task_id} mono /><Metric label="Version" value={task.version} /><Metric label="Deadline" value={task.deadline} /><Metric label="ETag" value={task.etag} mono /></dl></details></article>}</section>
}

function Settings({ client, report, tenant, setTenant, ready, endpoint }: { client: PlatformClient | null; report: (notice: Notice | null) => void; tenant: string; setTenant: (value: string) => void; ready: boolean | null; endpoint: string }) {
  const [diagnostic, setDiagnostic] = useState<'artifact' | 'operation'>('artifact')
  return <section className="stack"><article className="panel"><div className="panel__heading"><div><p className="kicker">SESSION</p><h2>Gateway and project readiness</h2></div><Status value={ready === null ? 'unchecked' : ready ? 'ready' : 'unavailable'} /></div><dl className="metrics"><Metric label="Gateway" value={endpoint} /><Metric label="Contract" value="insight.platform/v1" /><Metric label="Credential storage" value="Memory only" /></dl><label className="secondary-field"><span>Project / tenant label</span><input value={tenant} onChange={(event) => setTenant(event.target.value)} maxLength={128} /></label></article><article className="panel"><p className="kicker">FEATURE READINESS</p><h2>Resolved at publish time</h2><p className="body-copy">The Console reads exact tenant authoring bindings before compiling. Missing or disabled model and policy features fail closed; there are no bundle defaults.</p></article><article className="panel"><div className="panel__heading"><div><p className="kicker">ADVANCED DIAGNOSTICS</p><h2>Artifact and background operation lookup</h2></div><div className="segmented"><button className={diagnostic === 'artifact' ? 'active' : ''} onClick={() => setDiagnostic('artifact')}>Artifacts</button><button className={diagnostic === 'operation' ? 'active' : ''} onClick={() => setDiagnostic('operation')}>Operations</button></div></div>{diagnostic === 'artifact' ? <Artifacts client={client} report={report} /> : <Operations client={client} report={report} />}</article></section>
}

function Artifacts({ client, report }: { client: PlatformClient | null; report: (notice: Notice | null) => void }) {
  const [id, setId] = useState('')
  const [artifact, setArtifact] = useState<ArtifactView | null>(null)
  const [busy, setBusy] = useState(false)
  const load = async () => { if (!client) return; setBusy(true); try { const current = await client.getArtifact(id.trim()); setArtifact(current.data) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  const download = async () => { if (!client || !artifact) return; setBusy(true); try { const content = await client.downloadArtifact(artifact.artifact_id); if (content.blob.size !== artifact.expected_size_bytes) throw new Error('artifact_size_mismatch'); const url = URL.createObjectURL(content.blob); const anchor = document.createElement('a'); anchor.href = url; anchor.download = artifact.artifact_id; anchor.click(); URL.revokeObjectURL(url) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  return <div className="nested-panel"><SearchForm label="Artifact ID" placeholder="art_…" value={id} onChange={setId} onSubmit={load} busy={busy} />{artifact && <><dl className="metrics"><Metric label="State" value={artifact.state} /><Metric label="Purpose" value={artifact.purpose} /><Metric label="Size" value={`${artifact.expected_size_bytes} bytes`} /></dl><button className="button button--primary" disabled={artifact.state !== 'ready'} onClick={download}>Controlled download</button></>}</div>
}

function Operations({ client, report }: { client: PlatformClient | null; report: (notice: Notice | null) => void }) {
  const [id, setId] = useState('')
  const [operation, setOperation] = useState<OperationView | null>(null)
  const [busy, setBusy] = useState(false)
  const load = async () => { if (!client) return; setBusy(true); try { setOperation((await client.getOperation(id.trim())).data) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  return <div className="nested-panel"><SearchForm label="Operation ID" placeholder="job_…" value={id} onChange={setId} onSubmit={load} busy={busy} />{operation && <><dl className="metrics"><Metric label="State" value={operation.state} /><Metric label="Kind" value={operation.kind} /><Metric label="Updated" value={formatTime(operation.updated_at)} /></dl>{operation.error && <div className="notice notice--error">{operation.error.message}</div>}</>}</div>
}

export default App

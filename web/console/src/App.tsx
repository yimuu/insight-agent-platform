import { useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { PlatformClient, PlatformProblem } from './api/client'
import { discoverTaskIds, newReceipt, safeJson } from './api/security'
import type { ArtifactView, DeploymentView, JsonObject, OperationView, ResourceView, RunEvent, RunView, TaskView } from './api/types'
import './App.css'

type ViewName = 'overview' | 'agents' | 'runs' | 'tasks' | 'artifacts' | 'operations'
type Notice = { tone: 'error' | 'success' | 'info'; text: string; traceId?: string | null }
const TERMINAL_RUNS = new Set(['succeeded', 'failed', 'cancelled', 'timed_out'])
const NAV: Array<{ id: ViewName; label: string; eyebrow: string }> = [
  { id: 'overview', label: 'Readiness', eyebrow: '01' }, { id: 'agents', label: 'Agents', eyebrow: '02' },
  { id: 'runs', label: 'Runs', eyebrow: '03' }, { id: 'tasks', label: 'Tasks', eyebrow: '04' },
  { id: 'artifacts', label: 'Artifacts', eyebrow: '05' }, { id: 'operations', label: 'Operations', eyebrow: '06' },
]

function errorNotice(error: unknown): Notice {
  if (error instanceof PlatformProblem) return { tone: 'error', text: `${error.code} · ${error.message}${error.retryable ? ' · retryable' : ''}`, traceId: error.traceId }
  return { tone: 'error', text: error instanceof Error ? error.message : 'Unknown console error' }
}

function Status({ value }: { value: string }) {
  const tone = ['ready', 'enabled', 'succeeded', 'approved', 'responded'].includes(value) ? 'positive' : ['failed', 'rejected', 'cancelled', 'timed_out', 'quarantined', 'corrupt'].includes(value) ? 'negative' : 'neutral'
  return <span className={`status status--${tone}`}>{value}</span>
}

function NoticeBox({ notice }: { notice: Notice | null }) {
  if (!notice) return null
  return <div className={`notice notice--${notice.tone}`} role={notice.tone === 'error' ? 'alert' : 'status'}><span>{notice.text}</span>{notice.traceId && <code>trace {notice.traceId}</code>}</div>
}

function Metric({ label, value, mono = false }: { label: string; value: string | number | null | undefined; mono?: boolean }) {
  return <div className="metric"><dt>{label}</dt><dd className={mono ? 'mono' : ''}>{value ?? '—'}</dd></div>
}

function SearchForm({ label, placeholder, value, onChange, onSubmit, busy }: { label: string; placeholder: string; value: string; onChange: (value: string) => void; onSubmit: () => void; busy: boolean }) {
  return <form className="search" onSubmit={(event) => { event.preventDefault(); onSubmit() }}><label><span>{label}</span><input value={value} onChange={(event) => onChange(event.target.value)} placeholder={placeholder} required autoComplete="off" /></label><button className="button button--primary" disabled={busy}>{busy ? 'Loading…' : 'Inspect'}</button></form>
}

function App() {
  const [view, setView] = useState<ViewName>('overview')
  const [endpoint, setEndpoint] = useState(window.location.origin)
  const [token, setToken] = useState('')
  const [tenant, setTenant] = useState('local development')
  const [ready, setReady] = useState<boolean | null>(null)
  const [notice, setNotice] = useState<Notice | null>(null)
  const [selectedTask, setSelectedTask] = useState('')

  const client = useMemo(() => { try { return new PlatformClient(endpoint, token) } catch { return null } }, [endpoint, token])
  const connect = async (event: FormEvent) => {
    event.preventDefault(); setNotice(null)
    try { const next = new PlatformClient(endpoint, token); const isReady = await next.readiness(); setReady(isReady); setNotice(isReady ? { tone: 'success', text: 'Gateway is ready. Credentials remain in browser memory only.' } : { tone: 'error', text: 'Gateway readiness endpoint is not ready.' }) }
    catch (error) { setReady(false); setNotice(errorNotice(error)) }
  }

  return <div className="shell">
    <aside className="sidebar"><div className="brand" aria-label="Insight Agent Platform"><span className="brand__mark">IA</span><div><strong>Insight</strong><small>Agent Platform</small></div></div><nav aria-label="Console sections">{NAV.map((item) => <button key={item.id} className={view === item.id ? 'nav-item nav-item--active' : 'nav-item'} onClick={() => setView(item.id)}><span>{item.eyebrow}</span>{item.label}</button>)}</nav><div className="session-summary"><span className={`pulse ${ready ? 'pulse--ready' : ''}`} aria-hidden="true" /><div><strong>{ready === null ? 'Not checked' : ready ? 'Gateway ready' : 'Unavailable'}</strong><small>{tenant || 'Tenant context unset'}</small></div></div></aside>
    <main><header className="topbar"><div><p className="kicker">OPERATIONS CONSOLE</p><h1>{NAV.find((item) => item.id === view)?.label}</h1></div><div className="contract"><span>CONTRACT</span><strong>insight.platform/v1</strong></div></header><NoticeBox notice={notice} />
      {view === 'overview' && <Overview endpoint={endpoint} token={token} tenant={tenant} ready={ready} setEndpoint={setEndpoint} setToken={setToken} setTenant={setTenant} onConnect={connect} />}
      {view === 'agents' && <Agents client={client} report={setNotice} />}
      {view === 'runs' && <Runs client={client} report={setNotice} onTask={(id) => { setSelectedTask(id); setView('tasks') }} />}
      {view === 'tasks' && <Tasks key={selectedTask || 'direct'} client={client} report={setNotice} selectedId={selectedTask} />}
      {view === 'artifacts' && <Artifacts client={client} report={setNotice} />}
      {view === 'operations' && <Operations client={client} report={setNotice} />}
    </main>
  </div>
}

function Overview({ endpoint, token, tenant, ready, setEndpoint, setToken, setTenant, onConnect }: { endpoint: string; token: string; tenant: string; ready: boolean | null; setEndpoint: (value: string) => void; setToken: (value: string) => void; setTenant: (value: string) => void; onConnect: (event: FormEvent) => void }) {
  return <section className="page-grid"><article className="panel panel--wide"><div className="panel__heading"><div><p className="kicker">SESSION</p><h2>Connect to a public Gateway</h2></div><Status value={ready === null ? 'unchecked' : ready ? 'ready' : 'unavailable'} /></div><form className="connection-form" onSubmit={onConnect}><label><span>Gateway origin</span><input type="url" value={endpoint} onChange={(e) => setEndpoint(e.target.value)} placeholder="http://127.0.0.1:8080" required /></label><label><span>Tenant label</span><input value={tenant} onChange={(e) => setTenant(e.target.value)} maxLength={128} /></label><label className="field--wide"><span>OIDC access token</span><input type="password" value={token} onChange={(e) => setToken(e.target.value)} autoComplete="off" spellCheck={false} placeholder="Held in memory; never persisted" /></label><button className="button button--primary">Check readiness</button></form></article><article className="panel"><p className="kicker">BOUNDARY</p><h2>Authority stays server-side</h2><p className="body-copy">This static console reads and mutates only the public <code>/v1</code> contract. It has no database, worker identity, internal RPC client, or business state.</p></article><article className="panel"><p className="kicker">CREDENTIAL POSTURE</p><h2>Memory-only session</h2><p className="body-copy">Access tokens are never written to localStorage, sessionStorage, URLs, logs, diagnostics, or rendered payloads. Reloading the page clears the session.</p></article></section>
}

function Agents({ client, report }: { client: PlatformClient | null; report: (notice: Notice | null) => void }) {
  const [resourceId, setResourceId] = useState(''), [deploymentId, setDeploymentId] = useState('')
  const [resource, setResource] = useState<ResourceView | null>(null), [deployment, setDeployment] = useState<DeploymentView | null>(null), [busy, setBusy] = useState(false)
  const load = async () => { if (!client) return report({ tone: 'error', text: 'Connect a valid Gateway endpoint first.' }); setBusy(true); report(null); try { const current = await client.getResource('agents', resourceId.trim()); setResource(current.data); setDeployment(deploymentId.trim() ? (await client.getDeployment('agents', resourceId.trim(), deploymentId.trim())).data : null); report({ tone: 'success', text: 'Agent authority projection loaded.', traceId: current.traceId }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  return <section className="stack"><article className="panel"><SearchForm label="Agent ID" placeholder="agt_…" value={resourceId} onChange={setResourceId} onSubmit={load} busy={busy} /><label className="secondary-field"><span>Deployment ID (optional)</span><input value={deploymentId} onChange={(e) => setDeploymentId(e.target.value)} placeholder="adep_…" /></label></article>{resource && <article className="panel"><div className="panel__heading"><div><p className="kicker">RESOURCE</p><h2>{resource.draft.display_name ?? resource.resource_id}</h2></div><Status value={resource.gate_state} /></div><dl className="metrics"><Metric label="Resource ID" value={resource.resource_id} mono /><Metric label="Kind" value={resource.resource_kind} /><Metric label="Lifecycle" value={resource.lifecycle_state} /><Metric label="Draft generation" value={resource.draft_generation} /><Metric label="Version" value={resource.version} /><Metric label="ETag" value={resource.etag} mono /></dl></article>}{deployment && <article className="panel"><div className="panel__heading"><div><p className="kicker">IMMUTABLE DEPLOYMENT</p><h2>{deployment.deployment_id}</h2></div><Status value={resource?.gate_state ?? 'unknown'} /></div><dl className="metrics"><Metric label="Version ID" value={deployment.resource_version_id} mono /><Metric label="Environment" value={deployment.environment} /><Metric label="Closure digest" value={deployment.closure_digest} mono /><Metric label="Created" value={deployment.created_at} /><Metric label="ETag" value={deployment.etag} mono /></dl></article>}</section>
}

function Runs({ client, report, onTask }: { client: PlatformClient | null; report: (notice: Notice | null) => void; onTask: (id: string) => void }) {
  const [id, setId] = useState(''), [run, setRun] = useState<RunView | null>(null), [result, setResult] = useState<JsonObject | null>(null)
  const [events, setEvents] = useState<RunEvent[]>([]), [cursor, setCursor] = useState(''), [busy, setBusy] = useState(false), [lastReceipt, setLastReceipt] = useState('')
  const load = async () => { if (!client) return report({ tone: 'error', text: 'Connect a valid Gateway endpoint first.' }); setBusy(true); report(null); try { const current = await client.getRun(id.trim()), page = await client.getRunEvents(id.trim(), cursor || undefined); setRun(current.data); setEvents((existing) => [...existing, ...page].slice(-128)); if (page.length) setCursor(page.at(-1)!.id); if (TERMINAL_RUNS.has(current.data.state)) { try { setResult((await client.getRunResult(id.trim())).data) } catch (error) { if (!(error instanceof PlatformProblem) || error.status !== 409) throw error } } report({ tone: 'success', text: page.length ? `Loaded ${page.length} durable events; cursor advanced.` : 'Run loaded; no newer durable events.', traceId: current.traceId }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  const act = async (action: 'pause' | 'resume' | 'cancel') => { if (!client || !run) return; const receipt = newReceipt(`run-${action}-${run.run_id}-v${run.version}`); setLastReceipt(receipt); setBusy(true); report(null); try { const response = await client.runAction(run.run_id, action, run.etag, receipt); setRun(response.data); report({ tone: 'success', text: `${action} committed at Run version ${response.data.version}.`, traceId: response.traceId }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  const taskIds = discoverTaskIds(events)
  return <section className="stack"><article className="panel"><SearchForm label="Run ID" placeholder="run_…" value={id} onChange={(value) => { setId(value); setEvents([]); setCursor(''); setResult(null) }} onSubmit={load} busy={busy} /></article>{run && <article className="panel"><div className="panel__heading"><div><p className="kicker">RUN AUTHORITY</p><h2>{run.run_id}</h2></div><Status value={run.state} /></div><dl className="metrics"><Metric label="Version" value={run.version} /><Metric label="Agent deployment" value={run.agent_deployment_id} mono /><Metric label="Started" value={run.started_at} /><Metric label="Updated" value={run.updated_at} /><Metric label="Deadline" value={run.deadline} /><Metric label="Cursor" value={cursor || 'origin'} mono /></dl><div className="actions"><button className="button" onClick={() => act('pause')} disabled={busy}>Pause</button><button className="button" onClick={() => act('resume')} disabled={busy}>Resume</button><button className="button button--danger" onClick={() => act('cancel')} disabled={busy}>Cancel</button><button className="button" onClick={load} disabled={busy}>Refresh events</button></div>{lastReceipt && <p className="receipt">Last Receipt <code>{lastReceipt}</code></p>}</article>}{events.length > 0 && <article className="panel"><div className="panel__heading"><div><p className="kicker">DURABLE TIMELINE</p><h2>{events.length} public events</h2></div><span className="muted">bounded page / replay cursor</span></div><ol className="timeline">{events.map((event, index) => <li key={`${event.id}-${index}`}><span className="timeline__dot" /><div><div className="timeline__header"><strong>{event.event}</strong><code>{event.id}</code></div><pre>{safeJson(event.data)}</pre></div></li>)}</ol>{taskIds.length > 0 && <div className="linked-tasks"><strong>Waiting tasks</strong>{taskIds.map((taskId) => <button className="button" key={taskId} onClick={() => onTask(taskId)}>{taskId}</button>)}</div>}</article>}{result && <article className="panel"><p className="kicker">TYPED RESULT</p><h2>Safe projection</h2><pre>{safeJson(result)}</pre></article>}</section>
}

function Tasks({ client, report, selectedId }: { client: PlatformClient | null; report: (notice: Notice | null) => void; selectedId: string }) {
  const [id, setId] = useState(selectedId), [task, setTask] = useState<TaskView | null>(null), [busy, setBusy] = useState(false)
  const [response, setResponse] = useState('{\n  "classification": "internal",\n  "schema_digest": "sha256:…",\n  "value": { "kind": "inline", "value": {} }\n}'), [lastReceipt, setLastReceipt] = useState('')
  const load = async () => { if (!client) return report({ tone: 'error', text: 'Connect a valid Gateway endpoint first.' }); setBusy(true); report(null); try { const current = await client.getTask(id.trim()); setTask(current.data); report({ tone: 'success', text: 'Task authority projection loaded.', traceId: current.traceId }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  const act = async (action: 'submit-input' | 'approve' | 'reject' | 'cancel') => { if (!client || !task) return; const receipt = newReceipt(`task-${action}-${task.task_id}-v${task.version}`); setLastReceipt(receipt); setBusy(true); report(null); try { const body = action === 'submit-input' ? JSON.parse(response) as JsonObject : undefined; const result = await client.taskAction(task.task_id, action, task.etag, receipt, body); setTask(result.data); report({ tone: 'success', text: `${action} committed at Task version ${result.data.version}.`, traceId: result.traceId }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  return <section className="stack"><article className="panel"><SearchForm label="Task ID" placeholder="int_… or apv_…" value={id} onChange={setId} onSubmit={load} busy={busy} /></article>{task && <article className="panel"><div className="panel__heading"><div><p className="kicker">TASK AUTHORITY</p><h2>{task.safe_prompt_key}</h2></div><Status value={task.state} /></div><dl className="metrics"><Metric label="Task ID" value={task.task_id} mono /><Metric label="Kind" value={task.task_kind} /><Metric label="Version" value={task.version} /><Metric label="Generation" value={task.generation} /><Metric label="Deadline" value={task.deadline} /><Metric label="Response schema" value={task.response_schema_digest} mono /></dl>{task.state === 'pending' && <><label className="json-field"><span>Typed input JSON</span><textarea rows={7} value={response} onChange={(e) => setResponse(e.target.value)} spellCheck={false} /></label><div className="actions"><button className="button button--primary" onClick={() => act('submit-input')} disabled={busy}>Submit input</button><button className="button" onClick={() => act('approve')} disabled={busy}>Approve</button><button className="button button--danger" onClick={() => act('reject')} disabled={busy}>Reject</button><button className="button" onClick={() => act('cancel')} disabled={busy}>Cancel</button></div></>}{lastReceipt && <p className="receipt">Receipt <code>{lastReceipt}</code></p>}</article>}</section>
}

function Artifacts({ client, report }: { client: PlatformClient | null; report: (notice: Notice | null) => void }) {
  const [id, setId] = useState(''), [artifact, setArtifact] = useState<ArtifactView | null>(null), [busy, setBusy] = useState(false)
  const load = async () => { if (!client) return report({ tone: 'error', text: 'Connect a valid Gateway endpoint first.' }); setBusy(true); report(null); try { const current = await client.getArtifact(id.trim()); setArtifact(current.data); report({ tone: 'success', text: 'Artifact metadata loaded.', traceId: current.traceId }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  const download = async () => { if (!client || !artifact) return; setBusy(true); report(null); try { const content = await client.downloadArtifact(artifact.artifact_id); if (content.blob.size !== artifact.expected_size_bytes) throw new Error('artifact_size_mismatch: Download does not match authority metadata'); const url = URL.createObjectURL(content.blob), anchor = document.createElement('a'); anchor.href = url; anchor.download = artifact.artifact_id; anchor.rel = 'noopener'; anchor.click(); URL.revokeObjectURL(url); report({ tone: 'success', text: `Authorized download started (${content.blob.size} bytes, ${content.mediaType}).` }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  return <section className="stack"><article className="panel"><SearchForm label="Artifact ID" placeholder="art_…" value={id} onChange={setId} onSubmit={load} busy={busy} /></article>{artifact && <article className="panel"><div className="panel__heading"><div><p className="kicker">SAFE METADATA</p><h2>{artifact.artifact_id}</h2></div><Status value={artifact.state} /></div><dl className="metrics"><Metric label="Purpose" value={artifact.purpose} /><Metric label="Classification" value={artifact.classification} /><Metric label="Size" value={`${artifact.expected_size_bytes.toLocaleString()} bytes`} /><Metric label="Media type" value={artifact.verified_media_type ?? artifact.declared_media_type} /><Metric label="Retain until" value={artifact.retain_until} /><Metric label="ETag" value={artifact.etag} mono /></dl><div className="actions"><button className="button button--primary" disabled={busy || artifact.state !== 'ready'} onClick={download}>Controlled download</button></div><p className="body-copy">Content is never rendered in the DOM. The browser requests the authorized public download route only after an explicit action.</p></article>}</section>
}

function Operations({ client, report }: { client: PlatformClient | null; report: (notice: Notice | null) => void }) {
  const [id, setId] = useState(''), [operation, setOperation] = useState<OperationView | null>(null), [busy, setBusy] = useState(false)
  const load = async () => { if (!client) return report({ tone: 'error', text: 'Connect a valid Gateway endpoint first.' }); setBusy(true); report(null); try { const current = await client.getOperation(id.trim()); setOperation(current.data); report({ tone: 'success', text: 'Operation authority projection loaded.', traceId: current.traceId }) } catch (error) { report(errorNotice(error)) } finally { setBusy(false) } }
  return <section className="stack"><article className="panel"><SearchForm label="Operation ID" placeholder="job_…" value={id} onChange={setId} onSubmit={load} busy={busy} /></article>{operation && <article className="panel"><div className="panel__heading"><div><p className="kicker">SHARED JOB PROJECTION</p><h2>{operation.operation_id}</h2></div><Status value={operation.state} /></div><dl className="metrics"><Metric label="Kind" value={operation.kind} /><Metric label="Updated" value={operation.updated_at} /><Metric label="Progress" value={operation.progress ? `${operation.progress.completed_units}/${operation.progress.total_units}` : null} /><Metric label="Result digest" value={operation.result?.result_digest} mono /><Metric label="Error code" value={operation.error?.code} /><Metric label="ETag" value={operation.etag} mono /></dl>{operation.error && <div className="notice notice--error">{operation.error.message}</div>}</article>}</section>
}

export default App

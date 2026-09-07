import { useCallback, useEffect, useRef, useState } from 'react'
import { PlatformClient, PlatformProblem } from '../api/client.ts'
import { newReceipt } from '../api/security.ts'
import type { Json, JsonObject, TaskForm, TaskView, TaskQueryPurpose } from '../api/types.ts'
import { TaskSchemaForm } from './TaskSchemaForm.tsx'

type Notice = { tone: 'error' | 'success' | 'info'; text: string; traceId?: string | null }
type Action = 'submit-input' | 'approve' | 'reject' | 'cancel'
type Filters = { purpose: TaskQueryPurpose; state: string; kind: string; runId: string }
const initialFilters: Filters = { purpose: 'respondable', state: 'pending', kind: '', runId: '' }
const permissionLost = (error: unknown) => error instanceof PlatformProblem && (error.status === 401 || error.status === 403)
const message = (error: unknown) => error instanceof Error ? error.message : 'Task request failed.'

function validActions(actions: unknown): actions is TaskView['allowed_actions'] {
  return Array.isArray(actions) && actions.length <= 4 && new Set(actions).size === actions.length && actions.every((action) => ['submit_input', 'approve', 'reject', 'cancel'].includes(action))
}

function taskFormMatches(task: TaskView, form: TaskForm): boolean {
  return form.schema_version === 2 && form.task_id === task.task_id && form.generation === task.generation && form.version === task.version
    && form.etag === task.etag && form.safe_prompt_key === task.safe_prompt_key && form.allowed_actions.every((action) => task.allowed_actions.includes(action))
    && typeof form.response_schema_digest === 'string' && form.response_schema_digest.length > 0
    && form.response_schema_digest === task.response_schema_digest && form.response_schema?.canonical_digest === form.response_schema_digest
}

export function TaskInbox({ client, report, selectedId, subjectKey }: {
  client: PlatformClient | null
  report(notice: Notice | null): void
  selectedId: string
  subjectKey: string
}) {
  const [id, setId] = useState(selectedId)
  const [filters, setFilters] = useState(initialFilters)
  const [filterDraft, setFilterDraft] = useState(initialFilters)
  const [items, setItems] = useState<TaskView[]>([])
  const [cursor, setCursor] = useState<string | null>(null)
  const [page, setPage] = useState(1)
  const [listBusy, setListBusy] = useState(false)
  const [listError, setListError] = useState('')
  const [task, setTask] = useState<TaskView | null>(null)
  const [form, setForm] = useState<TaskForm | null>(null)
  const [formError, setFormError] = useState('')
  const [detailBusy, setDetailBusy] = useState(false)
  const [mutating, setMutating] = useState(false)
  const [classification, setClassification] = useState('internal')
  const listRequest = useRef<{ generation: number; abort?: AbortController }>({ generation: 0 })
  const detailRequest = useRef<{ generation: number; abort?: AbortController }>({ generation: 0 })
  const mutationRequest = useRef<{ generation: number; abort?: AbortController }>({ generation: 0 })
  const receipt = useRef<{ intent: string; key: string } | null>(null)
  const mutationBusy = useRef(false)

  const clearDetail = useCallback(() => {
    detailRequest.current.abort?.abort(); detailRequest.current.generation++
    mutationRequest.current.abort?.abort(); mutationRequest.current.generation++
    receipt.current = null
    mutationBusy.current = false
    setTask(null); setForm(null); setFormError(''); setDetailBusy(false); setMutating(false); setClassification('internal')
  }, [])
  const clearProtected = useCallback(() => {
    listRequest.current.abort?.abort(); listRequest.current.generation++
    setItems([]); setCursor(null); setListBusy(false); setId('')
    clearDetail()
  }, [clearDetail])
  useEffect(() => () => {
    for (const request of [listRequest, detailRequest, mutationRequest]) { request.current.abort?.abort(); request.current.generation++ }
  }, [])

  const loadPage = useCallback(async (next?: string, pageNumber = 1) => {
    if (!client) { setListError('Connect to a valid Gateway first.'); return }
    listRequest.current.abort?.abort()
    const abort = new AbortController()
    const generation = ++listRequest.current.generation
    listRequest.current.abort = abort
    setListBusy(true); setListError(''); setItems([]); setCursor(null)
    try {
      const response = await client.listTasks({ ...filters, cursor: next }, { signal: abort.signal })
      if (generation !== listRequest.current.generation || abort.signal.aborted) return
      if (response.data.schema_version !== 1 || !Array.isArray(response.data.items) || response.data.items.length > 25 || response.data.items.some((item) => item.schema_version !== 2 || !validActions(item.allowed_actions))) throw new Error('Unsupported Task list response.')
      setItems(response.data.items); setCursor(response.data.next_cursor); setPage(pageNumber)
    } catch (error) {
      if (generation !== listRequest.current.generation || abort.signal.aborted) return
      if (permissionLost(error)) clearProtected()
      setListError(message(error))
    } finally { if (generation === listRequest.current.generation) setListBusy(false) }
  }, [client, filters, clearProtected])
  useEffect(() => {
    let active = true
    const request = listRequest.current
    queueMicrotask(() => { if (active) void loadPage() })
    return () => { active = false; request.abort?.abort(); request.generation++ }
  }, [loadPage])

  const loadTask = useCallback(async (taskId: string) => {
    clearDetail()
    setId(taskId)
    if (!client || !taskId) return
    const generation = detailRequest.current.generation
    const abort = new AbortController()
    detailRequest.current.abort = abort
    setDetailBusy(true)
    try {
      const response = await client.getTask(taskId, { signal: abort.signal, purpose: filters.purpose })
      if (generation !== detailRequest.current.generation || abort.signal.aborted) return
      if (response.data.task_id !== taskId || response.data.schema_version !== 2 || !validActions(response.data.allowed_actions)) throw new Error('Unsupported Task response.')
      setTask(response.data)
      if (response.data.allowed_actions.includes('submit_input')) {
        const frozen = await client.getTaskForm(taskId, { signal: abort.signal })
        if (generation !== detailRequest.current.generation || abort.signal.aborted) return
        if (!validActions(frozen.data.allowed_actions) || frozen.etag !== frozen.data.etag || !taskFormMatches(response.data, frozen.data)) throw new Error('Task changed while loading its form. Reload the Task to use its current version.')
        setForm(frozen.data)
      }
    } catch (error) {
      if (generation !== detailRequest.current.generation || abort.signal.aborted) return
      if (permissionLost(error)) { clearProtected(); setListError(message(error)) }
      else setFormError(error instanceof PlatformProblem && error.code === 'task_form_unavailable'
        ? 'This Task has no frozen response form. A response cannot be edited here.' : message(error))
    } finally { if (generation === detailRequest.current.generation) setDetailBusy(false) }
  }, [client, filters.purpose, clearDetail, clearProtected])
  useEffect(() => {
    let active = true
    queueMicrotask(() => { if (active && selectedId) void loadTask(selectedId) })
    return () => { active = false }
  }, [selectedId, loadTask])

  const act = async (action: Action, value?: Json) => {
    if (!client || !task || mutating || mutationBusy.current || detailBusy) return
    const allowed = action === 'submit-input' ? 'submit_input' : action
    if (!task.allowed_actions.includes(allowed) || (form && !form.allowed_actions.includes(allowed))) throw new Error('This action is not currently available. Reload the Task.')
    if (action === 'submit-input' && (!form || !taskFormMatches(task, form) || value === undefined)) throw new Error('Reload the Task’s frozen response form before submitting.')
    const body: JsonObject | undefined = action === 'submit-input' ? { classification, schema_digest: form!.response_schema_digest, value: { kind: 'inline', value } } : undefined
    if (body && new TextEncoder().encode(JSON.stringify(body)).length > 65_536) throw new Error('The complete Task response exceeds the 65536-byte submission limit.')
    const etag = action === 'submit-input' ? form!.etag : task.etag
    const intent = JSON.stringify({ taskId: task.task_id, etag, action, body })
    if (receipt.current?.intent !== intent) receipt.current = { intent, key: newReceipt(`task-${action}-${task.task_id}-v${task.version}`) }
    const abort = new AbortController()
    mutationRequest.current.abort = abort
    const generation = ++mutationRequest.current.generation
    mutationBusy.current = true
    setMutating(true); report(null)
    try {
      const result = await client.taskAction(task.task_id, action, etag, receipt.current.key, body, { signal: abort.signal })
      if (generation !== mutationRequest.current.generation || abort.signal.aborted) return
      if (result.data.task_id !== task.task_id || result.data.schema_version !== 2 || !validActions(result.data.allowed_actions)) throw new Error('Unsupported Task mutation response.')
      setTask(result.data); setForm(null); setFormError(''); receipt.current = null
      setItems((previous) => previous.map((item) => item.task_id === result.data.task_id ? result.data : item))
      report({ tone: 'success', text: `${action} committed.`, traceId: result.traceId })
    } catch (error) {
      if (generation !== mutationRequest.current.generation || abort.signal.aborted) return
      if (permissionLost(error)) { clearProtected(); setListError(message(error)) }
      else if (error instanceof PlatformProblem && (error.status === 409 || error.status === 412)) {
        await loadTask(task.task_id)
        report({ tone: 'error', text: `${message(error)} The Task was reloaded. Review its current state before taking another action.`, traceId: error.traceId })
      } else report({ tone: 'error', text: message(error), traceId: error instanceof PlatformProblem ? error.traceId : null })
      throw error
    } finally { if (generation === mutationRequest.current.generation) { mutationBusy.current = false; setMutating(false) } }
  }
  const invoke = (action: Action) => { void act(action).catch(() => {}) }
  const busy = detailBusy || mutating
  return <section className="stack">
    <article className="panel">
      <div className="panel__heading"><div><p className="kicker">TASK INBOX</p><h2>Current tasks</h2></div><button className="button" disabled={listBusy} onClick={() => void loadPage()}>Refresh inbox</button></div>
      <p className="body-copy">The server selects the tasks available to this session and checks each action when you submit.</p>
      <form className="form-grid" onSubmit={(event) => { event.preventDefault(); clearDetail(); setFilters({ ...filterDraft, runId: filterDraft.runId.trim() }) }}>
        <label><span>Task access</span><select value={filterDraft.purpose} onChange={(event) => setFilterDraft({ ...filterDraft, purpose: event.target.value as TaskQueryPurpose })}><option value="respondable">Available to respond</option><option value="viewable">Visible tasks</option></select></label>
        <label><span>Task state</span><select value={filterDraft.state} onChange={(event) => setFilterDraft({ ...filterDraft, state: event.target.value })}>{['', 'pending', 'responded', 'declined', 'approved', 'rejected', 'cancelled', 'expired'].map((state) => <option key={state} value={state}>{state || 'All states'}</option>)}</select></label>
        <label><span>Task kind</span><select value={filterDraft.kind} onChange={(event) => setFilterDraft({ ...filterDraft, kind: event.target.value })}>{['', 'approval', 'interaction_form', 'interaction_url_consent', 'interaction_business_input', 'external_authorization', 'human_work'].map((kind) => <option key={kind} value={kind}>{kind || 'All kinds'}</option>)}</select></label>
        <label><span>Task Run filter</span><input value={filterDraft.runId} onChange={(event) => setFilterDraft({ ...filterDraft, runId: event.target.value })} maxLength={128} placeholder="Any Run" /></label>
        <div className="actions"><button className="button" disabled={listBusy}>Apply Task filters</button></div>
      </form>
      {listError && <p className="notice notice--error" role="alert">{listError}</p>}
      {listBusy ? <p className="body-copy" role="status">Loading tasks…</p> : items.length ? <div className="task-inbox-list">{items.map((item) => <div className="panel__heading" key={item.task_id}>
        <div><strong>{item.safe_prompt_key}</strong><p className="body-copy muted">{item.task_kind} · {item.state}</p></div><button className="button" onClick={() => { report(null); void loadTask(item.task_id) }}>Open {item.safe_prompt_key}</button>
      </div>)}</div> : !listError && <p className="body-copy" role="status">{cursor ? 'No visible tasks in this page. Continue to the next page.' : 'No tasks in this page.'}</p>}
      <div className="actions"><span className="body-copy">Page {page}</span><button className="button" disabled={listBusy || page === 1} onClick={() => void loadPage()}>First Task page</button><button className="button" disabled={listBusy || !cursor} onClick={() => { if (cursor) void loadPage(cursor, page + 1) }}>Next Task page</button></div>
    </article>
    <article className="panel"><form className="search" onSubmit={(event) => { event.preventDefault(); report(null); void loadTask(id.trim()) }}>
      <label><span>Task ID</span><input value={id} onChange={(event) => { clearDetail(); setId(event.target.value); report(null) }} placeholder="int_… or apv_…" required autoComplete="off" maxLength={128} /></label>
      <button className="button button--primary" disabled={busy}>{detailBusy ? 'Loading…' : 'Open'}</button>
    </form></article>
    {formError && !task && <p className="notice notice--error" role="alert">{formError}</p>}
    {task && <article className="panel">
      <div className="panel__heading"><div><p className="kicker">TASK</p><h2>{task.safe_prompt_key}</h2></div><span className="status">{task.state}</span></div>
      <p className="body-copy">Deadline: {task.deadline}. Generation {task.generation}, version {task.version}.</p>
      {task.task_kind === 'approval' && task.state === 'pending' && <p className="body-copy">Review this approval request and choose an action.</p>}
      {task.task_kind === 'external_authorization' && task.state === 'pending' && <p className="body-copy">Complete this Task through its authorization flow.</p>}
      {formError && <p className="notice notice--error" role="alert">{formError}</p>}
      {detailBusy && <p className="body-copy" role="status">Loading the frozen response form…</p>}
      {form?.allowed_actions.includes('submit_input') && <div key={`${subjectKey}:${task.task_id}:${task.generation}:${task.version}`}>
        <label><span>Response classification</span><select value={classification} disabled={busy} onChange={(event) => setClassification(event.target.value)}>{['public', 'internal', 'confidential', 'restricted'].map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
        <TaskSchemaForm schema={form.response_schema} responseSchemaDigest={form.response_schema_digest} disabled={busy} onSubmit={(value) => act('submit-input', value)} />
      </div>}
      <div className="actions">
        {task.allowed_actions.includes('approve') && <button className="button button--primary" onClick={() => invoke('approve')} disabled={busy}>Approve</button>}
        {task.allowed_actions.includes('reject') && <button className="button button--danger" onClick={() => invoke('reject')} disabled={busy}>Reject</button>}
        {(form?.allowed_actions ?? task.allowed_actions).includes('cancel') && <button className="button" onClick={() => invoke('cancel')} disabled={busy}>Cancel</button>}
        <button className="button" onClick={() => { report(null); void loadTask(task.task_id) }} disabled={busy}>Reload Task</button>
      </div>
      <details className="diagnostics"><summary>Advanced diagnostics</summary><dl className="metrics"><div className="metric"><dt>Task ID</dt><dd className="mono">{task.task_id}</dd></div><div className="metric"><dt>ETag</dt><dd className="mono">{task.etag}</dd></div></dl></details>
    </article>}
  </section>
}

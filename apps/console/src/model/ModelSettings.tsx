import { INITIAL_QUOTA, hasPendingModelQuota, readModelQuota, resumeModelQuota, saveModelQuota } from './quota.ts'
import { useEffect, useRef, useState } from 'react'
import type { PlatformClient } from '../api/client.ts'
import type { JsonObject, ResourceView } from '../api/types.ts'
import type { ExactModelCredential, ModelConfigurationCatalog, ModelDefault, ModelResourcePage, ModelResourceSummary, ModelCredentialMetadata, ModelConnectionObservation, ModelQuotaLimits, ModelQuotaView } from './types.ts'
import { finishSourceCredential, hasPendingCredential, importSourceCredential, pendingSourceCredential } from './credentials.ts'
import { hasPendingModelPublication, pendingModelInput, publishModelConfiguration, resumeModelPublication } from './publication.ts'
import { hasPendingModelDefault, resumeModelDefault, selectModelDefault } from './default.ts'
import { CONNECTION_LABELS, pendingCredentialRevocation, probeConnection, readModelCredential, revokeCredential } from './management.ts'
import './models.css'

const EMPTY: ModelResourcePage = { schema_version: 1, items: [], next_after: null }
const ALIAS = /^[a-z][a-z0-9._-]{0,63}$/
function object(value: unknown): JsonObject { if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('model_configuration_invalid'); return value as JsonObject }
export function ModelSettings({ client, onError, onSaved }: { client: PlatformClient | null; onError: (error: unknown) => void; onSaved: (message: string) => void }) {
  const [catalog, setCatalog] = useState<ModelConfigurationCatalog | null>(null)
  const [defaultModel, setDefaultModel] = useState<ModelDefault | null>(null)
  const [sources, setSources] = useState<ModelResourcePage>(EMPTY)
  const [models, setModels] = useState<ModelResourcePage>(EMPTY)
  const [credentialView, setCredentialView] = useState<ModelCredentialMetadata | null>(null)
  const [observation, setObservation] = useState<ModelConnectionObservation | null>(null)
  const [quotaView, setQuotaView] = useState<ModelQuotaView | null>(null)
  const [quotaLimits, setQuotaLimits] = useState<ModelQuotaLimits>({ ...INITIAL_QUOTA })
  const [modelQuota, setModelQuota] = useState<ModelQuotaLimits>({ ...INITIAL_QUOTA })
  const [busy, setBusy] = useState(false)
  const [stage, setStage] = useState('')
  const [editing, setEditing] = useState<'source' | 'model' | null>(null)
  const [existing, setExisting] = useState<ResourceView | null>(null)
  const [alias, setAlias] = useState('')
  const [name, setName] = useState('')
  const [destination, setDestination] = useState('')
  const [key, setKey] = useState('')
  const [credential, setCredential] = useState<ExactModelCredential | null>(null)
  const [sourceId, setSourceId] = useState('')
  const [modelName, setModelName] = useState('')
  const [inputTokens, setInputTokens] = useState(8192)
  const [outputTokens, setOutputTokens] = useState(1024)
  const generation = useRef(0)
  const reload = async () => {
    if (!client) return
    const current = generation.current
    const [configuration, selected, providers, profiles] = await Promise.all([client.getModelConfiguration(), client.getModelDefault(), client.listModelResources('model_provider'), client.listModelResources('model_profile')])
    if (current !== generation.current) return
    setCatalog(configuration.data); setDefaultModel(selected.data); setSources(providers.data); setModels(profiles.data)
  }
  useEffect(() => {
    const current = ++generation.current
    if (!client) return
    const controller = new AbortController()
    void Promise.all([client.getModelConfiguration({ signal: controller.signal }), client.getModelDefault({ signal: controller.signal }), client.listModelResources('model_provider', undefined, { signal: controller.signal }), client.listModelResources('model_profile', undefined, { signal: controller.signal })])
      .then(([configuration, selected, providers, profiles]) => { if (!controller.signal.aborted) { setCatalog(configuration.data); setDefaultModel(selected.data); setSources(providers.data); setModels(profiles.data) } })
      .catch((error) => { if (!controller.signal.aborted) onError(error) })
    return () => { controller.abort(); generation.current = current + 1 }
  }, [client, onError])
  const perform = async (action: () => Promise<void>) => {
    const current = generation.current; setBusy(true)
    try { await action() } catch (error) { if (current === generation.current) onError(error) } finally { if (current === generation.current) setBusy(false) }
  }
  const start = (kind: 'source' | 'model') => { setEditing(kind); setExisting(null); setAlias(''); setName(''); setKey(''); setCredential(null); setModelName(''); setDestination(catalog?.destinations[0]?.destination_digest ?? ''); setSourceId(sources.items.find((item) => item.active_deployment)?.resource_id ?? ''); setInputTokens(8192); setOutputTokens(1024); setModelQuota({ ...INITIAL_QUOTA }) }
  const edit = async (item: ModelResourceSummary) => {
    if (!client) return
    const noun = item.resource_kind === 'model_provider' ? 'model-providers' : 'models'
    const response = await client.getResource(noun, item.resource_id)
    if (response.etag !== response.data.etag || response.data.resource_id !== item.resource_id) throw new Error('model_configuration_conflict')
    const current = response.data
    setModelQuota({ ...INITIAL_QUOTA })
    setExisting(current); setEditing(item.resource_kind === 'model_provider' ? 'source' : 'model'); setAlias(current.draft.alias ?? ''); setName(current.draft.display_name); setKey(''); setCredential(null)
    if (!current.active_deployment_id) return
    const deployment = (await client.getDeployment(noun, item.resource_id, current.active_deployment_id)).data
    const bindings = object(deployment.closure.bindings)
    if (item.resource_kind === 'model_provider') {
      setDestination(catalog?.destinations.find((choice) => choice.endpoint_identity_digest === bindings.endpoint_identity_digest)?.destination_digest ?? '')
      const credentials = bindings.secret_bindings
      if (!Array.isArray(credentials)) throw new Error('model_configuration_invalid')
      const keys = credentials.map(object).filter((binding) => binding.purpose === 'model_api_key')
      if (keys.length !== 1) throw new Error('model_configuration_invalid')
      setCredential(keys[0] as unknown as ExactModelCredential)
    } else {
      const source = object(bindings.provider_deployment)
      setSourceId(sources.items.find((candidate) => candidate.active_deployment?.deployment_id === source.deployment_id)?.resource_id ?? '')
      const spec = object(current.draft.document.spec)
      setModelName(String(object(spec.model_identity).value)); const limits = object(spec.limits)
      setInputTokens(Number(limits.maximum_input_tokens)); setOutputTokens(Number(limits.maximum_output_tokens))
    }
  }
  const save = async () => {
    if (!client || !catalog || !defaultModel || !editing || !ALIAS.test(alias) || !name.trim()) throw new Error('model_configuration_invalid: Enter a stable alias and display name.')
    if (editing === 'source') {
      const rawKey = key; setKey('')
      const exact = rawKey || !credential || hasPendingCredential()
        ? await importSourceCredential(client, { display_name: name.trim(), tenant_id: defaultModel.tenant_id, provider_id: catalog.secret_provider_id, alias, destination_digest: destination, resource_id: existing?.resource_id ?? null, resource_etag: existing?.etag ?? null }, rawKey)
        : credential
      const result = await publishModelConfiguration(client, { tenant_id: defaultModel.tenant_id, installation_digest: catalog.installation_digest, existing, quota: null,
        input: { kind: 'source', configuration: { schema_version: 1, alias, display_name: name.trim(), destination_digest: destination, credential: exact } } }, setStage)
      finishSourceCredential(); onSaved(`Source ${result.resource.draft.display_name} is ready.`)
    } else {
      const source = sources.items.find((item) => item.resource_id === sourceId)?.active_deployment
      if (!source || !modelName.trim()) throw new Error('model_configuration_invalid: Select a ready source and enter its model name.')
      const result = await publishModelConfiguration(client, { tenant_id: defaultModel.tenant_id, installation_digest: catalog.installation_digest, existing, quota: modelQuota,
        input: { kind: 'model', configuration: { schema_version: 1, alias, display_name: name.trim(), source, model: modelName.trim(), maximum_input_tokens: inputTokens, maximum_output_tokens: outputTokens, declared_at: new Date().toISOString() } } }, setStage)
      onSaved(`Model ${result.resource.draft.display_name} is ready.`)
    }
    setEditing(null); await reload()
  }
  const resume = async () => { if (!client || !catalog || !defaultModel) return; const source = pendingModelInput()?.kind === 'source'; await resumeModelPublication(client, defaultModel.tenant_id, catalog.installation_digest, setStage); if (source) finishSourceCredential(); setEditing(null); await reload(); onSaved('Model configuration recovered from current server state.') }
  const resumeCredential = async () => {
    if (!client || !defaultModel) return
    const pending = pendingSourceCredential(client.origin, defaultModel.tenant_id)
    if (!pending) return
    const current = pending.intent.resource_id ? await client.getResource('model-providers', pending.intent.resource_id) : null
    if (current && (current.etag !== pending.intent.resource_etag || current.data.etag !== current.etag)) throw new Error('model_configuration_conflict: The source changed while credential import was pending.')
    setEditing('source'); setExisting(current?.data ?? null); setAlias(pending.intent.alias); setName(pending.intent.display_name); setDestination(pending.intent.destination_digest); setCredential(pending.binding); setKey('')
    setStage(pending.binding ? 'Credential imported. Save to finish source publication.' : 'Re-enter the original key and save to resume credential import.')
  }
  const inspectCredential = async (item: ModelResourceSummary) => {
    if (!client || !defaultModel || !item.active_deployment) return
    const response = await client.getDeployment('model-providers', item.resource_id, item.active_deployment.deployment_id)
    if (response.data.resource_id !== item.resource_id || response.data.deployment_id !== item.active_deployment.deployment_id || response.data.closure_digest !== item.active_deployment.deployment_digest) throw new Error('model_credential_conflict')
    const bindings = object(response.data.closure.bindings).secret_bindings
    if (!Array.isArray(bindings)) throw new Error('model_credential_invalid')
    const keys = bindings.map(object).filter((binding) => binding.purpose === 'model_api_key')
    if (keys.length !== 1 || typeof keys[0].secret_binding_id !== 'string') throw new Error('model_credential_invalid')
    const value = await readModelCredential(client, defaultModel.tenant_id, keys[0].secret_binding_id)
    if (value.provider_id !== keys[0].provider_id) throw new Error('model_credential_conflict')
    setCredentialView(value)
  }
  const more = async (kind: 'model_provider' | 'model_profile') => { if (!client) return; const page = kind === 'model_provider' ? sources : models; if (!page.next_after) return; const result = await client.listModelResources(kind, page.next_after); const joined = { ...result.data, items: [...page.items, ...result.data.items] }; if (kind === 'model_provider') setSources(joined); else setModels(joined) }
  const ready = Boolean(client && catalog && defaultModel)
  return <section className="stack model-settings">
    <article className="panel"><div className="panel__heading"><div><p className="kicker">MODEL CONNECTIONS</p><h2>Sources and models</h2></div><button className="button" disabled={!client || busy} onClick={() => void perform(reload)}>Refresh</button></div>
      <p className="body-copy">Keep accounts and regions in separate sources. Each source can provide several models. New Agent authoring uses the selected default. Published Agents and existing runs keep their exact bindings.</p>
      {!ready && <p role="status">Sign in and install the model destination configuration to manage models.</p>}
      {stage && <p role="status">{stage}</p>}
      <button className="button" disabled={!ready || busy || !hasPendingModelPublication()} onClick={() => void perform(resume)}>Resume pending publication</button>
      {hasPendingCredential() && <button className="button" disabled={!ready || busy} onClick={() => void perform(resumeCredential)}>Resume credential import</button>}
    </article>
    <article className="panel"><div className="panel__heading"><h2>Sources</h2><button className="button button--primary" disabled={!ready || busy} onClick={() => start('source')}>Add source</button></div>
      {sources.items.length === 0 ? <p>No sources configured.</p> : <table><thead><tr><th>Source</th><th>Alias</th><th>State</th><th /></tr></thead><tbody>{sources.items.map((item) => <tr key={item.resource_id}><td>{item.display_name}</td><td><code>{item.alias}</code></td><td>{item.gate_state} · {item.active_deployment ? 'deployed' : 'draft'}</td><td><button className="button" disabled={busy} onClick={() => void perform(() => edit(item))}>Edit / rotate key</button><button className="button" disabled={busy || !item.active_deployment} onClick={() => void perform(() => inspectCredential(item))}>Credential</button></td></tr>)}</tbody></table>}
      {sources.next_after && <button className="button" disabled={busy} onClick={() => void perform(() => more('model_provider'))}>Load more sources</button>}
    </article>
    <article className="panel"><div className="panel__heading"><h2>Models</h2><button className="button button--primary" disabled={!ready || busy || !sources.items.some((item) => item.active_deployment)} onClick={() => start('model')}>Add model</button></div>
      {models.items.length === 0 ? <p>No models configured.</p> : <table><thead><tr><th>Model</th><th>Alias</th><th>Default</th><th /></tr></thead><tbody>{models.items.map((item) => <tr key={item.resource_id}><td>{item.display_name}</td><td><code>{item.alias}</code></td><td>{defaultModel?.default_model && item.active_deployment && defaultModel.default_model.deployment_id === item.active_deployment.deployment_id ? 'Selected' : <button className="button" disabled={busy || !item.active_deployment || item.gate_state !== 'enabled'} onClick={() => void perform(async () => { if (client && defaultModel && item.active_deployment) setDefaultModel(await selectModelDefault(client, defaultModel, item.active_deployment)) })}>Use as default</button>}</td><td><button className="button" disabled={busy} onClick={() => void perform(() => edit(item))}>Edit</button><button className="button" disabled={busy || !item.active_deployment} onClick={() => void perform(async () => { if (client && defaultModel && item.active_deployment) { const current = await readModelQuota(client, defaultModel.tenant_id, item.active_deployment); setQuotaView(current); setQuotaLimits({ ...(current.allocation?.limits ?? INITIAL_QUOTA) }) } })}>Quota</button><button className="button" disabled={busy || !item.active_deployment || item.gate_state !== 'enabled'} onClick={() => void perform(async () => { if (client && catalog && item.active_deployment) setObservation(await probeConnection(client, catalog.installation_digest, item.active_deployment)) })}>Test connection</button></td></tr>)}</tbody></table>}
      {models.next_after && <button className="button" disabled={busy} onClick={() => void perform(() => more('model_profile'))}>Load more models</button>}
      {hasPendingModelDefault() && <button className="button" disabled={!ready || busy} onClick={() => void perform(async () => { if (client && defaultModel) { setDefaultModel(await resumeModelDefault(client, defaultModel)); onSaved('Default selection recovered from current server state.') } })}>Resume default selection</button>}
      {defaultModel?.default_model && <button className="button" disabled={busy} onClick={() => void perform(async () => { if (client && defaultModel) setDefaultModel(await selectModelDefault(client, defaultModel, null)) })}>Clear default</button>}
    </article>
    <article className="panel"><h2>Execution quota</h2>
      <p className="body-copy">Limits apply to one published model deployment. Used and reserved amounts are retained when limits change. Zero blocks new reservations. Managing limits requires tenant administration permission.</p>
      {hasPendingModelQuota() && <button className="button" disabled={!ready || busy} onClick={() => void perform(async () => { if (client && defaultModel) { const current = await resumeModelQuota(client, defaultModel.tenant_id); setQuotaView(current); setQuotaLimits({ ...current.allocation!.limits }); onSaved('Quota allocation recovered.') } })}>Resume quota allocation</button>}
      {quotaView && <form className="stack" onSubmit={event => { event.preventDefault(); void perform(async () => { if (client) { const current = await saveModelQuota(client, quotaView, quotaLimits); setQuotaView(current); onSaved('Quota limits saved.') } }) }}>
        <p><code>{quotaView.model_deployment.deployment_id}</code></p>
        <p>Tenant concurrent model limit: {quotaView.tenant_concurrency.limit}; reserved: {quotaView.tenant_concurrency.reserved}; used: {quotaView.tenant_concurrency.used}.</p>
        {quotaView.allocation ? <table><thead><tr><th>Metric</th><th>Limit</th><th>Reserved</th><th>Used</th></tr></thead><tbody>{QUOTA_FIELDS.map(([field, label]) => <tr key={field}><th>{label}</th><td>{quotaView.allocation!.limits[field]}</td><td>{quotaView.allocation!.reserved[field]}</td><td>{quotaView.allocation!.used[field]}</td></tr>)}</tbody></table> : <p>No execution quota is allocated to this deployment.</p>}
        <QuotaFields value={quotaLimits} setValue={setQuotaLimits} disabled={busy} />
        <div className="actions"><button className="button" type="submit" disabled={busy}>Save quota limits</button><button className="button" type="button" disabled={busy} onClick={() => setQuotaView(null)}>Close quota</button></div>
      </form>}
    </article>
    <article className="panel"><h2>Connection and credentials</h2>
      <p className="body-copy">A connection test sends one short request and may incur a provider charge. It checks the selected deployment's protocol response. Agent execution and retrieval are tested separately.</p>
      {observation && <p role="status">{CONNECTION_LABELS[observation.outcome]} · {observation.model_identity.value} · <time dateTime={observation.observed_at}>{observation.observed_at}</time></p>}
      <button className="button" disabled={!ready || busy} onClick={() => void perform(async () => { if (client && defaultModel) { const pending = pendingCredentialRevocation(client, defaultModel.tenant_id); if (pending) setCredentialView(pending.credential); else setStage('No pending credential revocation.') } })}>Resume credential revocation</button>
      {credentialView && <div className="stack"><p><code>{credentialView.secret_binding_id}</code> · {credentialView.state} · generation {credentialView.generation}</p>
        <p className="body-copy">Revoking this credential permanently blocks further requests that use this binding, including other models using the same source. Import and publish a new key to reconnect.</p>
        <div className="actions"><button className="button" disabled={busy || credentialView.state === 'revoked'} onClick={() => void perform(async () => { if (client && defaultModel) { setCredentialView(await revokeCredential(client, defaultModel.tenant_id, credentialView)); onSaved('Credential revoked.'); await reload() } })}>Revoke this credential</button><button className="button" disabled={busy} onClick={() => setCredentialView(null)}>Close</button></div>
      </div>}
    </article>
    {editing && <article className="panel"><h2>{existing ? 'Edit' : 'Add'} {editing}</h2><form className="stack" onSubmit={(event) => { event.preventDefault(); void perform(save) }}>
      <label>Alias<input required pattern="[a-z][a-z0-9._-]{0,63}" maxLength={64} disabled={Boolean(existing) || busy} value={alias} onChange={(event) => setAlias(event.target.value)} /></label>
      <label>Display name<input required maxLength={255} disabled={busy} value={name} onChange={(event) => setName(event.target.value)} /></label>
      {editing === 'source' ? <><label>Installed destination<select required disabled={busy} value={destination} onChange={(event) => setDestination(event.target.value)}><option value="">Select destination</option>{catalog?.destinations.map((item) => <option key={item.destination_digest} value={item.destination_digest}>{item.base_url} · {item.region} · {item.protocol}</option>)}</select></label>
        <label>{credential ? 'New API key (leave empty to retain the current binding)' : 'API key'}<input type="password" autoComplete="new-password" required={!credential && !hasPendingCredential()} maxLength={4096} disabled={busy} value={key} onChange={(event) => setKey(event.target.value)} /></label><p className="body-copy">The key is sent to the credential service and cleared from the form. If import is interrupted, re-enter the same key to resume.</p></>
        : <><label>Source<select required disabled={busy} value={sourceId} onChange={(event) => setSourceId(event.target.value)}><option value="">Select source</option>{sources.items.filter((item) => item.active_deployment && item.gate_state === 'enabled').map((item) => <option key={item.resource_id} value={item.resource_id}>{item.display_name} · {item.alias}</option>)}</select></label>
          <label>Provider model name<input required maxLength={255} disabled={busy} value={modelName} onChange={(event) => setModelName(event.target.value)} placeholder="qwen3.8-flash" /></label>
          <label>Input token limit<input type="number" min={1} max={8192} required disabled={busy} value={inputTokens} onChange={(event) => setInputTokens(Number(event.target.value))} /></label>
          <label>Output token limit<input type="number" min={1} max={2048} required disabled={busy} value={outputTokens} onChange={(event) => setOutputTokens(Number(event.target.value))} /></label>
          <fieldset><legend>Execution quota for this publication</legend><QuotaFields value={modelQuota} setValue={setModelQuota} disabled={busy} /></fieldset>
          <p className="body-copy">Saving publishes a new deployment with the quota shown above. Existing runs retain their deployment and usage. Cost uses platform microunits and is not a provider billing estimate.</p>
          <p className="body-copy">Basic setup enables text with validated JSON fallback. The outbound classification ceiling is Internal. Provider training, retention, tokenizer and pricing remain unspecified. These are operator limits; saving does not certify model capabilities.</p></>}
      <div className="actions"><button className="button button--primary" type="submit" disabled={busy || !ready}>{busy ? stage || 'Working…' : 'Save and activate'}</button><button className="button" type="button" disabled={busy} onClick={() => { setEditing(null); setKey('') }}>Close editor</button></div>
    </form></article>}
  </section>
}

const QUOTA_FIELDS: [keyof ModelQuotaLimits, string][] = [['requests', 'Cumulative requests'], ['tokens', 'Cumulative tokens'], ['cost_microunits', 'Cumulative cost microunits']]
function QuotaFields({ value, setValue, disabled }: { value: ModelQuotaLimits; setValue: (value: ModelQuotaLimits) => void; disabled: boolean }) {
  return <>{QUOTA_FIELDS.map(([field, label]) => <label key={field}>{label}<input type="number" min={0} max={Number.MAX_SAFE_INTEGER} step={1} required disabled={disabled} value={Number.isFinite(value[field]) ? value[field] : ''} onChange={event => setValue({ ...value, [field]: event.target.value === '' ? Number.NaN : Number(event.target.value) })} /></label>)}</>
}

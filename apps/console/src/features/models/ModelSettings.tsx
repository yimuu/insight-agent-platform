import { MODEL_SERVICES, modelEndpoint, endpointUrl } from './endpoints.ts'
import type { ModelEndpoint, ModelProtocol } from '../../shared/api/model-types.ts'
import { configurationAlias } from './presentation.ts'
import { NoticeBox } from '../../shared/ui/console-ui'
import { errorNotice } from '../../shared/ui/feedback'
import type { Notice } from '../../shared/ui/feedback'
import { displayState } from '../../shared/i18n/display'
import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Models.module.css'
const cx = classNames(sharedStyles, localStyles)
import {
  INITIAL_QUOTA,
  hasPendingModelQuota,
  readModelQuota,
  resumeModelQuota,
  saveModelQuota,
} from './quota.ts'
import { useEffect, useRef, useState } from 'react'
import type { PlatformClient } from '../../shared/api/client.ts'
import type { JsonObject, ResourceView } from '../../shared/api/types.ts'
import type {
  ExactModelCredential,
  ModelConfigurationCatalog,
  ModelDefault,
  ModelResourcePage,
  ModelResourceSummary,
  ModelCredentialMetadata,
  ModelConnectionObservation,
  ModelQuotaLimits,
  ModelQuotaView,
} from '../../shared/api/model-types.ts'
import {
  finishSourceCredential,
  hasPendingCredential,
  importSourceCredential,
  pendingSourceCredential,
} from './credentials.ts'
import {
  hasPendingModelPublication,
  pendingModelInput,
  publishModelConfiguration,
  resumeModelPublication,
} from './publication.ts'
import { hasPendingModelDefault, resumeModelDefault, selectModelDefault } from './default.ts'
import {
  CONNECTION_LABELS,
  pendingCredentialRevocation,
  probeConnection,
  readModelCredential,
  revokeCredential,
} from './management.ts'

const EMPTY: ModelResourcePage = { schema_version: 1, items: [], next_after: null }
const ALIAS = /^[a-z][a-z0-9._-]{0,63}$/
function object(value: unknown): JsonObject {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('model_configuration_invalid')
  return value as JsonObject
}
export function ModelSettings({
  client,
  onSaved,
}: {
  client: PlatformClient | null
  onSaved: (message: string) => void
}) {
  const [catalog, setCatalog] = useState<ModelConfigurationCatalog | null>(null)
  const [defaultModel, setDefaultModel] = useState<ModelDefault | null>(null)
  const [sources, setSources] = useState<ModelResourcePage>(EMPTY)
  const [models, setModels] = useState<ModelResourcePage>(EMPTY)
  const [credentialView, setCredentialView] = useState<ModelCredentialMetadata | null>(null)
  const [observation, setObservation] = useState<ModelConnectionObservation | null>(null)
  const [quotaView, setQuotaView] = useState<ModelQuotaView | null>(null)
  const [quotaLimits, setQuotaLimits] = useState<ModelQuotaLimits>({ ...INITIAL_QUOTA })
  const [modelQuota, setModelQuota] = useState<ModelQuotaLimits>({ ...INITIAL_QUOTA })
  const [failure, setFailure] = useState<Notice | null>(null)
  const dialog = useRef<HTMLDialogElement>(null)
  const [addingModel, setAddingModel] = useState(false)
  const [busy, setBusy] = useState(false)
  const [stage, setStage] = useState('')
  const [editing, setEditing] = useState<'source' | 'model' | null>(null)
  const [existing, setExisting] = useState<ResourceView | null>(null)
  const [alias, setAlias] = useState('')
  const [name, setName] = useState('')
  const [service, setService] = useState('dashscope')
  const [baseUrl, setBaseUrl] = useState<string>(MODEL_SERVICES[0].url)
  const [protocol, setProtocol] = useState<ModelProtocol>('open_ai_responses')
  const [region, setRegion] = useState('cn-beijing')
  const [key, setKey] = useState('')
  const [credential, setCredential] = useState<ExactModelCredential | null>(null)
  const [sourceId, setSourceId] = useState('')
  const [modelName, setModelName] = useState('')
  const [inputTokens, setInputTokens] = useState(8192)
  const [outputTokens, setOutputTokens] = useState(2048)
  const generation = useRef(0)
  useEffect(() => {
    const element = dialog.current
    if (!editing || !element) return
    const trigger = document.activeElement as HTMLElement | null
    element.showModal()
    element.querySelector<HTMLInputElement>('input[autofocus]')?.focus()
    return () => {
      element.close()
      trigger?.focus()
    }
  }, [editing])
  const closeEditor = () => {
    setEditing(null)
    setKey('')
    setFailure(null)
    setStage('')
  }
  const reload = async () => {
    if (!client) return
    const current = generation.current
    const [configuration, selected, providers, profiles] = await Promise.all([
      client.getModelConfiguration(),
      client.getModelDefault(),
      client.listModelResources('model_provider'),
      client.listModelResources('model_profile'),
    ])
    if (current !== generation.current) return
    setCatalog(configuration.data)
    setDefaultModel(selected.data)
    setSources(providers.data)
    setModels(profiles.data)
  }
  useEffect(() => {
    const current = ++generation.current
    if (!client) return
    const controller = new AbortController()
    void Promise.all([
      client.getModelConfiguration({ signal: controller.signal }),
      client.getModelDefault({ signal: controller.signal }),
      client.listModelResources('model_provider', undefined, { signal: controller.signal }),
      client.listModelResources('model_profile', undefined, { signal: controller.signal }),
    ])
      .then(([configuration, selected, providers, profiles]) => {
        if (!controller.signal.aborted) {
          setCatalog(configuration.data)
          setDefaultModel(selected.data)
          setSources(providers.data)
          setModels(profiles.data)
        }
      })
      .catch((error) => {
        if (!controller.signal.aborted) setFailure(errorNotice(error))
      })
    return () => {
      controller.abort()
      generation.current = current + 1
    }
  }, [client])
  const perform = async (action: () => Promise<void>) => {
    const current = generation.current
    setBusy(true)
    setFailure(null)
    try {
      await action()
    } catch (error) {
      if (current === generation.current) setFailure(errorNotice(error))
    } finally {
      if (current === generation.current) {
        setBusy(false)
        setStage('')
      }
    }
  }
  const start = (kind: 'source' | 'model') => {
    setEditing(kind)
    setExisting(null)
    setAlias(configurationAlias(kind))
    setName(kind === 'source' ? '阿里百炼' : '')
    setFailure(null)
    setStage('')
    setKey('')
    setCredential(null)
    setModelName('')
    setService('dashscope')
    setBaseUrl(MODEL_SERVICES[0].url)
    setProtocol('open_ai_responses')
    setRegion('cn-beijing')
    setSourceId(
      sources.items.find((item) => item.active_deployment && item.gate_state === 'enabled')
        ?.resource_id ?? '',
    )
    setInputTokens(8192)
    setOutputTokens(2048)
    setModelQuota({ ...INITIAL_QUOTA })
  }
  const edit = async (item: ModelResourceSummary) => {
    if (!client) return
    setAddingModel(false)
    setFailure(null)
    setStage('')
    const noun = item.resource_kind === 'model_provider' ? 'model-providers' : 'models'
    const response = await client.getResource(noun, item.resource_id)
    if (response.etag !== response.data.etag || response.data.resource_id !== item.resource_id)
      throw new Error('model_configuration_conflict')
    const current = response.data
    setModelQuota({ ...INITIAL_QUOTA })
    setExisting(current)
    setEditing(item.resource_kind === 'model_provider' ? 'source' : 'model')
    setAlias(current.draft.alias ?? '')
    setName(current.draft.display_name)
    setKey('')
    setCredential(null)
    if (!current.active_deployment_id) return
    const deployment = (
      await client.getDeployment(noun, item.resource_id, current.active_deployment_id)
    ).data
    const bindings = object(deployment.closure.bindings)
    if (item.resource_kind === 'model_provider') {
      setService('custom')
      setBaseUrl(endpointUrl(bindings.endpoint as unknown as ModelEndpoint))
      setRegion(String(bindings.region))
      const spec = object(current.draft.document.spec)
      const adapter = object(spec.installed_adapter)
      setProtocol(
        String(adapter.qualified_name).includes('anthropic')
          ? 'anthropic_messages'
          : 'open_ai_responses',
      )
      const credentials = bindings.secret_bindings
      if (!Array.isArray(credentials)) throw new Error('model_configuration_invalid')
      const keys = credentials.map(object).filter((binding) => binding.purpose === 'model_api_key')
      if (keys.length !== 1) throw new Error('model_configuration_invalid')
      setCredential(keys[0] as unknown as ExactModelCredential)
    } else {
      const source = object(bindings.provider_deployment)
      setSourceId(
        sources.items.find(
          (candidate) => candidate.active_deployment?.deployment_id === source.deployment_id,
        )?.resource_id ?? '',
      )
      const spec = object(current.draft.document.spec)
      setModelName(String(object(spec.model_identity).value))
      const limits = object(spec.limits)
      setInputTokens(Number(limits.maximum_input_tokens))
      setOutputTokens(Number(limits.maximum_output_tokens))
    }
  }
  const save = async () => {
    if (!client || !catalog || !defaultModel || !editing || !ALIAS.test(alias) || !name.trim())
      throw new Error('model_configuration_invalid: Enter a stable alias and display name.')
    if (hasPendingModelPublication()) {
      await resume()
      return
    }
    if (editing === 'source') {
      const endpoint = modelEndpoint(baseUrl)
      if (!catalog.protocols.includes(protocol) || !/^[a-z][a-z0-9_-]{0,31}$/.test(region))
        throw new Error('请选择可用协议并填写有效区域。')
      setStage('正在保存 API Key…')
      const rawKey = key
      setKey('')
      const exact =
        rawKey || !credential || hasPendingCredential()
          ? await importSourceCredential(
              client,
              {
                display_name: name.trim(),
                tenant_id: defaultModel.tenant_id,
                provider_id: catalog.secret_provider_id,
                alias,
                endpoint,
                protocol,
                region,
                resource_id: existing?.resource_id ?? null,
                resource_etag: existing?.etag ?? null,
              },
              rawKey,
            )
          : credential
      setCredential(exact)
      const result = await publishModelConfiguration(
        client,
        {
          tenant_id: defaultModel.tenant_id,
          installation_digest: catalog.installation_digest,
          existing,
          quota: null,
          input: {
            kind: 'source',
            configuration: {
              schema_version: 2,
              alias,
              display_name: name.trim(),
              endpoint,
              protocol,
              region,
              credential: exact,
            },
          },
        },
        setStage,
      )
      finishSourceCredential()
      onSaved(`已连接 ${result.resource.draft.display_name}。`)
      if (addingModel) {
        await reload()
        setEditing('model')
        setExisting(null)
        setAlias(configurationAlias('model'))
        setName('')
        setModelName('')
        setSourceId(result.resource.resource_id)
        setCredential(null)
        return
      }
    } else {
      const source = sources.items.find((item) => item.resource_id === sourceId)?.active_deployment
      if (!source || !modelName.trim())
        throw new Error(
          'model_configuration_invalid: Select a ready source and enter its model name.',
        )
      const result = await publishModelConfiguration(
        client,
        {
          tenant_id: defaultModel.tenant_id,
          installation_digest: catalog.installation_digest,
          existing,
          quota: modelQuota,
          input: {
            kind: 'model',
            configuration: {
              schema_version: 1,
              alias,
              display_name: name.trim(),
              source,
              model: modelName.trim(),
              maximum_input_tokens: inputTokens,
              maximum_output_tokens: outputTokens,
              declared_at: new Date().toISOString(),
            },
          },
        },
        setStage,
      )
      onSaved(`模型 ${result.resource.draft.display_name} 已就绪。`)
    }
    setEditing(null)
    await reload()
  }
  const resume = async () => {
    if (!client || !catalog || !defaultModel) return
    const source = pendingModelInput()?.kind === 'source'
    await resumeModelPublication(
      client,
      defaultModel.tenant_id,
      catalog.installation_digest,
      setStage,
    )
    if (source) finishSourceCredential()
    setEditing(null)
    await reload()
    onSaved('已从服务端恢复当前模型配置。')
  }
  const resumeCredential = async () => {
    if (!client || !defaultModel) return
    const pending = pendingSourceCredential(client.origin, defaultModel.tenant_id)
    if (!pending) return
    const current = pending.intent.resource_id
      ? await client.getResource('model-providers', pending.intent.resource_id)
      : null
    if (
      current &&
      (current.etag !== pending.intent.resource_etag || current.data.etag !== current.etag)
    )
      throw new Error(
        'model_configuration_conflict: The source changed while credential import was pending.',
      )
    setEditing('source')
    setExisting(current?.data ?? null)
    setAlias(pending.intent.alias)
    setName(pending.intent.display_name)
    setService('custom')
    setBaseUrl(endpointUrl(pending.intent.endpoint))
    setProtocol(pending.intent.protocol)
    setRegion(pending.intent.region)
    setCredential(pending.binding)
    setKey('')
    setStage(
      pending.binding ? 'API Key 已保存，点击保存继续。' : '请重新输入刚才的 API Key，然后保存。',
    )
  }
  const inspectCredential = async (item: ModelResourceSummary) => {
    if (!client || !defaultModel || !item.active_deployment) return
    const response = await client.getDeployment(
      'model-providers',
      item.resource_id,
      item.active_deployment.deployment_id,
    )
    if (
      response.data.resource_id !== item.resource_id ||
      response.data.deployment_id !== item.active_deployment.deployment_id ||
      response.data.closure_digest !== item.active_deployment.deployment_digest
    )
      throw new Error('model_credential_conflict')
    const bindings = object(response.data.closure.bindings).secret_bindings
    if (!Array.isArray(bindings)) throw new Error('model_credential_invalid')
    const keys = bindings.map(object).filter((binding) => binding.purpose === 'model_api_key')
    if (keys.length !== 1 || typeof keys[0].secret_binding_id !== 'string')
      throw new Error('model_credential_invalid')
    const value = await readModelCredential(
      client,
      defaultModel.tenant_id,
      keys[0].secret_binding_id,
    )
    if (value.provider_id !== keys[0].provider_id) throw new Error('model_credential_conflict')
    setCredentialView(value)
  }
  const more = async (kind: 'model_provider' | 'model_profile') => {
    if (!client) return
    const page = kind === 'model_provider' ? sources : models
    if (!page.next_after) return
    const result = await client.listModelResources(kind, page.next_after)
    const joined = { ...result.data, items: [...page.items, ...result.data.items] }
    if (kind === 'model_provider') setSources(joined)
    else setModels(joined)
  }
  const ready = Boolean(client && catalog && defaultModel)
  return (
    <section data-ui="model-settings" className={cx('stack model-settings')}>
      <div className={cx('toolbar')}>
        <p className={cx('muted')}>连接模型服务，选择工作空间默认模型。</p>
        <div className={cx('actions')}>
          <button
            className={cx('button')}
            disabled={!client || busy}
            onClick={() => void perform(reload)}
          >
            刷新
          </button>
          <button
            className={cx('button button--primary')}
            disabled={!ready || busy || hasPendingModelPublication() || hasPendingCredential()}
            onClick={() => {
              setAddingModel(true)
              start(
                sources.items.some(
                  (item) => item.active_deployment && item.gate_state === 'enabled',
                )
                  ? 'model'
                  : 'source',
              )
            }}
          >
            ＋ 添加模型
          </button>
        </div>
      </div>
      {!editing && <NoticeBox notice={failure} />}
      {!ready && !failure && (
        <p role="status">{client ? '正在加载模型配置…' : '请先连接工作空间。'}</p>
      )}
      {!editing && (hasPendingModelPublication() || hasPendingCredential()) && (
        <div className={cx('recovery')}>
          <span>上次添加尚未完成，已保留进度。</span>
          <button
            className={cx('button')}
            disabled={!ready || busy}
            onClick={() => void perform(hasPendingModelPublication() ? resume : resumeCredential)}
          >
            继续添加
          </button>
        </div>
      )}
      {!editing && busy && <p role="status">{stage || '正在处理…'}</p>}
      <article data-ui="panel" className={cx('panel')}>
        <div data-ui="panel__heading" className={cx('panel__heading')}>
          <h2>
            模型服务 <span className={cx('count')}>{sources.items.length}</span>
          </h2>
          <button
            className={cx('button button--primary')}
            disabled={!ready || busy || hasPendingModelPublication() || hasPendingCredential()}
            onClick={() => {
              setAddingModel(false)
              start('source')
            }}
          >
            连接服务
          </button>
        </div>
        {sources.items.length === 0 ? (
          <div className={cx('empty-state')}>
            <div className={cx('provider-mark')} aria-hidden="true">
              ✳
            </div>
            <div>
              <h3>连接你的第一个模型</h3>
              <p>选择服务商并填写 API Key，即可开始添加模型。</p>
            </div>
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>来源</th>
                <th>状态</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {sources.items.map((item) => (
                <tr key={item.resource_id}>
                  <td>{item.display_name}</td>
                  <td>
                    {displayState(item.gate_state)} · {item.active_deployment ? '已部署' : '草稿'}
                  </td>
                  <td>
                    <button
                      className={cx('button')}
                      disabled={busy}
                      onClick={() => void perform(() => edit(item))}
                    >
                      设置
                    </button>
                    <button
                      className={cx('button')}
                      disabled={busy || !item.active_deployment}
                      onClick={() => void perform(() => inspectCredential(item))}
                    >
                      管理密钥
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {sources.next_after && (
          <button
            className={cx('button')}
            disabled={busy}
            onClick={() => void perform(() => more('model_provider'))}
          >
            加载更多来源
          </button>
        )}
      </article>
      {(sources.items.length > 0 || models.items.length > 0) && (
        <article data-ui="panel" className={cx('panel')}>
          <div data-ui="panel__heading" className={cx('panel__heading')}>
            <h2>
              可用模型 <span className={cx('count')}>{models.items.length}</span>
            </h2>
          </div>
          {models.items.length === 0 ? (
            <p>还没有模型。</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>模型</th>
                  <th>默认</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {models.items.map((item) => (
                  <tr key={item.resource_id}>
                    <td>{item.display_name}</td>
                    <td>
                      {defaultModel?.default_model &&
                      item.active_deployment &&
                      defaultModel.default_model.deployment_id ===
                        item.active_deployment.deployment_id ? (
                        '默认模型'
                      ) : (
                        <button
                          className={cx('button')}
                          disabled={
                            busy || !item.active_deployment || item.gate_state !== 'enabled'
                          }
                          onClick={() =>
                            void perform(async () => {
                              if (client && defaultModel && item.active_deployment)
                                setDefaultModel(
                                  await selectModelDefault(
                                    client,
                                    defaultModel,
                                    item.active_deployment,
                                  ),
                                )
                            })
                          }
                        >
                          设为默认
                        </button>
                      )}
                    </td>
                    <td>
                      <button
                        className={cx('button')}
                        disabled={busy}
                        onClick={() => void perform(() => edit(item))}
                      >
                        编辑
                      </button>
                      <button
                        className={cx('button')}
                        disabled={busy || !item.active_deployment}
                        onClick={() =>
                          void perform(async () => {
                            if (client && defaultModel && item.active_deployment) {
                              const current = await readModelQuota(
                                client,
                                defaultModel.tenant_id,
                                item.active_deployment,
                              )
                              setQuotaView(current)
                              setQuotaLimits({ ...(current.allocation?.limits ?? INITIAL_QUOTA) })
                            }
                          })
                        }
                      >
                        额度
                      </button>
                      <button
                        className={cx('button')}
                        disabled={busy || !item.active_deployment || item.gate_state !== 'enabled'}
                        onClick={() =>
                          void perform(async () => {
                            if (client && catalog && item.active_deployment)
                              setObservation(
                                await probeConnection(
                                  client,
                                  catalog.installation_digest,
                                  item.active_deployment,
                                ),
                              )
                          })
                        }
                      >
                        检测连接
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {models.next_after && (
            <button
              className={cx('button')}
              disabled={busy}
              onClick={() => void perform(() => more('model_profile'))}
            >
              加载更多模型
            </button>
          )}
          {hasPendingModelDefault() && (
            <button
              className={cx('button')}
              disabled={!ready || busy}
              onClick={() =>
                void perform(async () => {
                  if (client && defaultModel) {
                    setDefaultModel(await resumeModelDefault(client, defaultModel))
                    onSaved('已从服务端恢复当前默认模型。')
                  }
                })
              }
            >
              继续设置默认模型
            </button>
          )}
          {defaultModel?.default_model && (
            <button
              className={cx('button')}
              disabled={busy}
              onClick={() =>
                void perform(async () => {
                  if (client && defaultModel)
                    setDefaultModel(await selectModelDefault(client, defaultModel, null))
                })
              }
            >
              清除默认模型
            </button>
          )}
        </article>
      )}
      {(quotaView || hasPendingModelQuota()) && (
        <article data-ui="panel" className={cx('panel')}>
          <h2>执行额度</h2>
          <p className={cx('body-copy')}>
            额度应用于所选模型部署。修改限额保留已用及预留用量，设为零将阻止新请求。管理额度需要管理员权限。
          </p>
          {hasPendingModelQuota() && (
            <button
              className={cx('button')}
              disabled={!ready || busy}
              onClick={() =>
                void perform(async () => {
                  if (client && defaultModel) {
                    const current = await resumeModelQuota(client, defaultModel.tenant_id)
                    setQuotaView(current)
                    setQuotaLimits({ ...current.allocation!.limits })
                    onSaved('已恢复额度分配。')
                  }
                })
              }
            >
              继续分配额度
            </button>
          )}
          {quotaView && (
            <form
              className={cx('stack')}
              onSubmit={(event) => {
                event.preventDefault()
                void perform(async () => {
                  if (client) {
                    const current = await saveModelQuota(client, quotaView, quotaLimits)
                    setQuotaView(current)
                    onSaved('已保存额度限制。')
                  }
                })
              }}
            >
              <p>
                <code>{quotaView.model_deployment.deployment_id}</code>
              </p>
              <p>
                工作空间模型并发上限： {quotaView.tenant_concurrency.limit}；预留：{' '}
                {quotaView.tenant_concurrency.reserved}；已用： {quotaView.tenant_concurrency.used}.
              </p>
              {quotaView.allocation ? (
                <table>
                  <thead>
                    <tr>
                      <th>指标</th>
                      <th>限额</th>
                      <th>预留</th>
                      <th>已用</th>
                    </tr>
                  </thead>
                  <tbody>
                    {QUOTA_FIELDS.map(([field, label]) => (
                      <tr key={field}>
                        <th>{label}</th>
                        <td>{quotaView.allocation!.limits[field]}</td>
                        <td>{quotaView.allocation!.reserved[field]}</td>
                        <td>{quotaView.allocation!.used[field]}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <p>此部署尚未分配执行额度。</p>
              )}
              <QuotaFields value={quotaLimits} setValue={setQuotaLimits} disabled={busy} />
              <div className={cx('actions')}>
                <button className={cx('button')} type="submit" disabled={busy}>
                  保存额度
                </button>
                <button
                  className={cx('button')}
                  type="button"
                  disabled={busy}
                  onClick={() => setQuotaView(null)}
                >
                  关闭额度面板
                </button>
              </div>
            </form>
          )}
        </article>
      )}
      {(observation ||
        credentialView ||
        (client &&
          defaultModel &&
          pendingCredentialRevocation(client, defaultModel.tenant_id))) && (
        <article data-ui="panel" className={cx('panel')}>
          <h2>连接与密钥</h2>
          <p className={cx('body-copy')}>
            连接检测会发送一次简短请求，可能产生厂商费用。它验证所选模型的协议响应；智能体执行与检索需分别验证。
          </p>
          {observation && (
            <p role="status">
              {CONNECTION_LABELS[observation.outcome]} · {observation.model_identity.value} ·{' '}
              <time dateTime={observation.observed_at}>{observation.observed_at}</time>
            </p>
          )}
          <button
            className={cx('button')}
            disabled={!ready || busy}
            onClick={() =>
              void perform(async () => {
                if (client && defaultModel) {
                  const pending = pendingCredentialRevocation(client, defaultModel.tenant_id)
                  if (pending) setCredentialView(pending.credential)
                  else setStage('没有待恢复的凭据撤销操作。')
                }
              })
            }
          >
            继续撤销凭据
          </button>
          {credentialView && (
            <div className={cx('stack')}>
              <p>
                <code>{credentialView.secret_binding_id}</code> · {credentialView.state} · 代次{' '}
                {credentialView.generation}
              </p>
              <p className={cx('body-copy')}>
                撤销将永久阻止使用此绑定的新请求，包括同一来源下的其他模型。重新连接需要导入并发布新密钥。
              </p>
              <div className={cx('actions')}>
                <button
                  className={cx('button')}
                  disabled={busy || credentialView.state === 'revoked'}
                  onClick={() =>
                    void perform(async () => {
                      if (client && defaultModel) {
                        setCredentialView(
                          await revokeCredential(client, defaultModel.tenant_id, credentialView),
                        )
                        onSaved('已撤销凭据。')
                        await reload()
                      }
                    })
                  }
                >
                  撤销此凭据
                </button>
                <button
                  className={cx('button')}
                  disabled={busy}
                  onClick={() => setCredentialView(null)}
                >
                  关闭
                </button>
              </div>
            </div>
          )}
        </article>
      )}
      {editing && (
        <dialog
          ref={dialog}
          className={cx('editor')}
          aria-labelledby="model-editor-title"
          onCancel={(event) => {
            event.preventDefault()
            if (!busy) closeEditor()
          }}
        >
          <header className={cx('editor-heading')}>
            <div>
              <p className={cx('muted')}>
                {addingModel
                  ? editing === 'source'
                    ? '步骤 1 / 2 · 连接服务'
                    : '步骤 2 / 2 · 添加模型'
                  : '模型配置'}
              </p>
              <h2 id="model-editor-title">
                {editing === 'source'
                  ? existing
                    ? '服务设置'
                    : '连接模型服务'
                  : existing
                    ? '编辑模型'
                    : '添加模型'}
              </h2>
            </div>
            <button
              type="button"
              className={cx('close-button')}
              aria-label="关闭"
              disabled={busy}
              onClick={closeEditor}
            >
              ×
            </button>
          </header>
          <form
            className={cx('stack')}
            onSubmit={(event) => {
              event.preventDefault()
              void perform(save)
            }}
          >
            <fieldset disabled={busy || hasPendingModelPublication()} className={cx('form-fields')}>
              {editing === 'source' ? (
                <>
                  <label>
                    模型服务
                    <select
                      value={service}
                      onChange={(event) => {
                        const id = event.target.value
                        setService(id)
                        const preset = MODEL_SERVICES.find((item) => item.id === id)
                        if (preset) {
                          setBaseUrl(preset.url)
                          setProtocol(preset.protocol)
                          setRegion(preset.region)
                          if (!existing) setName(preset.name.split(' · ')[0])
                        } else if (!existing) setName('自定义服务')
                      }}
                    >
                      {MODEL_SERVICES.filter((item) =>
                        catalog?.protocols.includes(item.protocol),
                      ).map((item) => (
                        <option key={item.id} value={item.id}>
                          {item.name}
                        </option>
                      ))}
                      <option value="custom">自定义兼容服务</option>
                    </select>
                  </label>
                  {service === 'custom' && (
                    <>
                      <label>
                        API 地址
                        <input
                          required
                          type="url"
                          value={baseUrl}
                          onChange={(event) => setBaseUrl(event.target.value)}
                          placeholder="https://api.example.com/v1"
                        />
                      </label>
                      <label>
                        接口协议
                        <select
                          value={protocol}
                          onChange={(event) => setProtocol(event.target.value as ModelProtocol)}
                        >
                          {catalog?.protocols.includes('open_ai_responses') && (
                            <option value="open_ai_responses">OpenAI Responses</option>
                          )}
                          {catalog?.protocols.includes('anthropic_messages') && (
                            <option value="anthropic_messages">Anthropic Messages</option>
                          )}
                        </select>
                      </label>
                    </>
                  )}
                  <label>
                    {credential ? 'API Key（留空保留已保存的密钥）' : 'API Key'}
                    <input
                      type="password"
                      autoFocus
                      autoComplete="new-password"
                      required={!credential && !hasPendingModelPublication()}
                      maxLength={4096}
                      disabled={busy}
                      value={key}
                      onChange={(event) => setKey(event.target.value)}
                    />
                  </label>
                  <details className={cx('advanced')}>
                    <summary>更多设置</summary>{' '}
                    <label>
                      {editing === 'source' ? '服务名称' : '显示名称'}
                      <input
                        maxLength={255}
                        disabled={busy}
                        value={name}
                        onChange={(event) => setName(event.target.value)}
                      />
                    </label>
                    {service !== 'custom' && (
                      <label>
                        API 地址
                        <input
                          required
                          type="url"
                          value={baseUrl}
                          onChange={(event) => setBaseUrl(event.target.value)}
                        />
                      </label>
                    )}
                    <label>
                      服务区域
                      <input
                        required
                        maxLength={32}
                        value={region}
                        onChange={(event) => setRegion(event.target.value)}
                        placeholder="例如 cn-beijing、global"
                      />
                    </label>
                    <p className={cx('muted')}>API Key 加密保存，不会保存在浏览器中。</p>
                  </details>
                </>
              ) : (
                <>
                  <label>
                    来源
                    <select
                      required
                      disabled={busy}
                      value={sourceId}
                      onChange={(event) => setSourceId(event.target.value)}
                    >
                      <option value="">选择来源</option>
                      {sources.items
                        .filter((item) => item.active_deployment && item.gate_state === 'enabled')
                        .map((item) => (
                          <option key={item.resource_id} value={item.resource_id}>
                            {item.display_name}
                          </option>
                        ))}
                    </select>
                  </label>
                  <label>
                    模型 ID
                    <input
                      required
                      maxLength={255}
                      disabled={busy}
                      value={modelName}
                      onChange={(event) => {
                        setModelName(event.target.value)
                        if (!existing && (!name || name === modelName)) setName(event.target.value)
                      }}
                      autoFocus
                      placeholder="输入百炼控制台中的模型 ID"
                    />
                  </label>
                  <details className={cx('advanced')}>
                    <summary>高级设置 · 名称与限额</summary>
                    <div className={cx('form-fields')}>
                      {' '}
                      <label>
                        显示名称
                        <input
                          maxLength={255}
                          disabled={busy}
                          value={name}
                          onChange={(event) => setName(event.target.value)}
                        />
                      </label>
                      <label>
                        输入 Token 上限
                        <input
                          type="number"
                          min={1}
                          max={8192}
                          required
                          disabled={busy}
                          value={inputTokens}
                          onChange={(event) => setInputTokens(Number(event.target.value))}
                        />
                      </label>
                      <label>
                        输出 Token 上限
                        <input
                          type="number"
                          min={1}
                          max={2048}
                          required
                          disabled={busy}
                          value={outputTokens}
                          onChange={(event) => setOutputTokens(Number(event.target.value))}
                        />
                      </label>
                      <fieldset>
                        <legend>累计执行额度</legend>
                        <QuotaFields value={modelQuota} setValue={setModelQuota} disabled={busy} />
                      </fieldset>
                      <p className={cx('muted')}>
                        额度为平台累计限制，零表示禁止调用。费用单位不代表厂商账单。
                      </p>
                    </div>
                  </details>
                  <p className={cx('muted')}>
                    每次回答最多 {outputTokens.toLocaleString()} Token，可在高级设置调整。
                    长文同时受智能体输出字段的长度限制。
                  </p>
                  <p className={cx('muted')}>
                    累计限额：{modelQuota.requests} 次请求 / {modelQuota.tokens.toLocaleString()}{' '}
                    Token。可在高级设置调整。
                  </p>
                </>
              )}
            </fieldset>
            <NoticeBox notice={failure} />
            {stage && (
              <p role="status" className={cx('muted')}>
                {stage}
              </p>
            )}
            <footer className={cx('editor-actions')}>
              <button
                className={cx('button button--primary')}
                type="submit"
                disabled={busy || !ready}
              >
                {busy
                  ? '正在保存…'
                  : hasPendingModelPublication()
                    ? '重试保存'
                    : editing === 'source' && addingModel
                      ? '下一步'
                      : '保存'}
              </button>
              <button className={cx('button')} type="button" disabled={busy} onClick={closeEditor}>
                取消
              </button>
            </footer>
          </form>
        </dialog>
      )}
    </section>
  )
}

const QUOTA_FIELDS: [keyof ModelQuotaLimits, string][] = [
  ['requests', '累计请求数'],
  ['tokens', '累计 Token 数'],
  ['cost_microunits', '累计费用（微单位）'],
]
function QuotaFields({
  value,
  setValue,
  disabled,
}: {
  value: ModelQuotaLimits
  setValue: (value: ModelQuotaLimits) => void
  disabled: boolean
}) {
  return (
    <>
      {QUOTA_FIELDS.map(([field, label]) => (
        <label key={field}>
          {label}
          <input
            type="number"
            min={0}
            max={Number.MAX_SAFE_INTEGER}
            step={1}
            required
            disabled={disabled}
            value={Number.isFinite(value[field]) ? value[field] : ''}
            onChange={(event) =>
              setValue({
                ...value,
                [field]: event.target.value === '' ? Number.NaN : Number(event.target.value),
              })
            }
          />
        </label>
      ))}
    </>
  )
}

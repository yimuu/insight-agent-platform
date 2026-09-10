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
  onError,
  onSaved,
}: {
  client: PlatformClient | null
  onError: (error: unknown) => void
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
        if (!controller.signal.aborted) onError(error)
      })
    return () => {
      controller.abort()
      generation.current = current + 1
    }
  }, [client, onError])
  const perform = async (action: () => Promise<void>) => {
    const current = generation.current
    setBusy(true)
    try {
      await action()
    } catch (error) {
      if (current === generation.current) onError(error)
    } finally {
      if (current === generation.current) setBusy(false)
    }
  }
  const start = (kind: 'source' | 'model') => {
    setEditing(kind)
    setExisting(null)
    setAlias('')
    setName('')
    setKey('')
    setCredential(null)
    setModelName('')
    setDestination(catalog?.destinations[0]?.destination_digest ?? '')
    setSourceId(sources.items.find((item) => item.active_deployment)?.resource_id ?? '')
    setInputTokens(8192)
    setOutputTokens(1024)
    setModelQuota({ ...INITIAL_QUOTA })
  }
  const edit = async (item: ModelResourceSummary) => {
    if (!client) return
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
      setDestination(
        catalog?.destinations.find(
          (choice) => choice.endpoint_identity_digest === bindings.endpoint_identity_digest,
        )?.destination_digest ?? '',
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
    if (editing === 'source') {
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
                destination_digest: destination,
                resource_id: existing?.resource_id ?? null,
                resource_etag: existing?.etag ?? null,
              },
              rawKey,
            )
          : credential
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
              schema_version: 1,
              alias,
              display_name: name.trim(),
              destination_digest: destination,
              credential: exact,
            },
          },
        },
        setStage,
      )
      finishSourceCredential()
      onSaved(`模型来源 ${result.resource.draft.display_name} 已就绪。`)
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
    setDestination(pending.intent.destination_digest)
    setCredential(pending.binding)
    setKey('')
    setStage(
      pending.binding ? '凭据已导入，保存后完成来源发布。' : '请重新输入原密钥并保存，以继续导入。',
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
      <article data-ui="panel" className={cx('panel')}>
        <div data-ui="panel__heading" className={cx('panel__heading')}>
          <div>
            <p data-ui="kicker" className={cx('kicker')}>
              模型来源
            </p>
            <h2>连接你的模型</h2>
          </div>
          <button
            className={cx('button')}
            disabled={!client || busy}
            onClick={() => void perform(reload)}
          >
            刷新
          </button>
        </div>
        <p className={cx('body-copy')}>
          按账户和区域管理来源，每个来源可配置多个模型。默认模型用于新建智能体，已发布智能体和已有运行保留其原始绑定。
        </p>
        {!ready && <p role="status">连接工作空间并完成模型目标配置后，可在这里管理模型。</p>}
        {stage && <p role="status">{stage}</p>}
        <button
          className={cx('button')}
          disabled={!ready || busy || !hasPendingModelPublication()}
          onClick={() => void perform(resume)}
        >
          继续未完成的发布
        </button>
        {hasPendingCredential() && (
          <button
            className={cx('button')}
            disabled={!ready || busy}
            onClick={() => void perform(resumeCredential)}
          >
            继续导入凭据
          </button>
        )}
      </article>
      <article data-ui="panel" className={cx('panel')}>
        <div data-ui="panel__heading" className={cx('panel__heading')}>
          <h2>模型来源</h2>
          <button
            className={cx('button button--primary')}
            disabled={!ready || busy}
            onClick={() => start('source')}
          >
            添加来源
          </button>
        </div>
        {sources.items.length === 0 ? (
          <p>还没有模型来源。</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>来源</th>
                <th>别名</th>
                <th>状态</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {sources.items.map((item) => (
                <tr key={item.resource_id}>
                  <td>{item.display_name}</td>
                  <td>
                    <code>{item.alias}</code>
                  </td>
                  <td>
                    {displayState(item.gate_state)} · {item.active_deployment ? '已部署' : '草稿'}
                  </td>
                  <td>
                    <button
                      className={cx('button')}
                      disabled={busy}
                      onClick={() => void perform(() => edit(item))}
                    >
                      编辑 / 轮换密钥
                    </button>
                    <button
                      className={cx('button')}
                      disabled={busy || !item.active_deployment}
                      onClick={() => void perform(() => inspectCredential(item))}
                    >
                      凭据
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
      <article data-ui="panel" className={cx('panel')}>
        <div data-ui="panel__heading" className={cx('panel__heading')}>
          <h2>模型</h2>
          <button
            className={cx('button button--primary')}
            disabled={!ready || busy || !sources.items.some((item) => item.active_deployment)}
            onClick={() => start('model')}
          >
            添加模型
          </button>
        </div>
        {models.items.length === 0 ? (
          <p>还没有模型。</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>模型</th>
                <th>别名</th>
                <th>默认</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {models.items.map((item) => (
                <tr key={item.resource_id}>
                  <td>{item.display_name}</td>
                  <td>
                    <code>{item.alias}</code>
                  </td>
                  <td>
                    {defaultModel?.default_model &&
                    item.active_deployment &&
                    defaultModel.default_model.deployment_id ===
                      item.active_deployment.deployment_id ? (
                      'Selected'
                    ) : (
                      <button
                        className={cx('button')}
                        disabled={busy || !item.active_deployment || item.gate_state !== 'enabled'}
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
      <article data-ui="panel" className={cx('panel')}>
        <h2>连接与凭据</h2>
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
      {editing && (
        <article data-ui="panel" className={cx('panel')}>
          <h2>
            {existing ? '编辑' : '添加'} {editing === 'source' ? '来源' : '模型'}
          </h2>
          <form
            className={cx('stack')}
            onSubmit={(event) => {
              event.preventDefault()
              void perform(save)
            }}
          >
            <label>
              别名
              <input
                required
                pattern="[a-z][a-z0-9._-]{0,63}"
                maxLength={64}
                disabled={Boolean(existing) || busy}
                value={alias}
                onChange={(event) => setAlias(event.target.value)}
              />
            </label>
            <label>
              显示名称
              <input
                required
                maxLength={255}
                disabled={busy}
                value={name}
                onChange={(event) => setName(event.target.value)}
              />
            </label>
            {editing === 'source' ? (
              <>
                <label>
                  已安装的访问目标
                  <select
                    required
                    disabled={busy}
                    value={destination}
                    onChange={(event) => setDestination(event.target.value)}
                  >
                    <option value="">选择访问目标</option>
                    {catalog?.destinations.map((item) => (
                      <option key={item.destination_digest} value={item.destination_digest}>
                        {item.base_url} · {item.region} · {item.protocol}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  {credential ? '新 API 密钥（留空保留当前绑定）' : 'API 密钥'}
                  <input
                    type="password"
                    autoComplete="new-password"
                    required={!credential && !hasPendingCredential()}
                    maxLength={4096}
                    disabled={busy}
                    value={key}
                    onChange={(event) => setKey(event.target.value)}
                  />
                </label>
                <p className={cx('body-copy')}>
                  密钥发送至凭据服务后会从表单清除。导入中断时，请重新输入同一密钥继续。
                </p>
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
                          {item.display_name} · {item.alias}
                        </option>
                      ))}
                  </select>
                </label>
                <label>
                  厂商模型名称
                  <input
                    required
                    maxLength={255}
                    disabled={busy}
                    value={modelName}
                    onChange={(event) => setModelName(event.target.value)}
                    placeholder="qwen3.8-flash"
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
                  <legend>本次发布的执行额度</legend>
                  <QuotaFields value={modelQuota} setValue={setModelQuota} disabled={busy} />
                </fieldset>
                <p className={cx('body-copy')}>
                  保存会发布新部署并设置上述额度，已有运行保留原部署及用量。费用额度使用平台微单位，不代表厂商账单估算。
                </p>
                <p className={cx('body-copy')}>
                  基础配置启用文本与经校验的 JSON
                  输出，出站数据最高为“内部”。这些设置是操作限额，不构成对厂商训练、数据保留、分词或定价能力的认证。
                </p>
              </>
            )}
            <div className={cx('actions')}>
              <button
                className={cx('button button--primary')}
                type="submit"
                disabled={busy || !ready}
              >
                {busy ? stage || '处理中…' : '保存并激活'}
              </button>
              <button
                className={cx('button')}
                type="button"
                disabled={busy}
                onClick={() => {
                  setEditing(null)
                  setKey('')
                }}
              >
                关闭编辑器
              </button>
            </div>
          </form>
        </article>
      )}
    </section>
  )
}

const QUOTA_FIELDS: [keyof ModelQuotaLimits, string][] = [
  ['requests', 'Cumulative requests'],
  ['tokens', 'Cumulative tokens'],
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

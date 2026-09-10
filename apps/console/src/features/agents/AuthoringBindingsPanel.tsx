import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Agents.module.css'
const cx = classNames(sharedStyles, localStyles)
import { useEffect, useRef, useState } from 'react'
import { PlatformClient, PlatformProblem } from '../../shared/api/client'
import type { Json } from '../../shared/api/types'
import { parseBindingSelections } from '../../shared/api/authoring-query.ts'
import type {
  AuthoringDependency,
  BindingResolution,
  DependencyKind,
} from '../../shared/api/authoring-query.ts'

const EMPTY = '{\n  "schema_version": 1,\n  "slots": []\n}'
export function AuthoringBindingsPanel({
  client,
  disabled,
  onResolved,
}: {
  client: PlatformClient | null
  disabled: boolean
  onResolved: (slots: Json[]) => void
}) {
  const [kind, setKind] = useState<DependencyKind>('model')
  const [environment, setEnvironment] = useState('')
  const [contract, setContract] = useState('')
  const [items, setItems] = useState<AuthoringDependency[]>([])
  const [cursor, setCursor] = useState<string | null>(null)
  const [selections, setSelections] = useState(EMPTY)
  const [slotId, setSlotId] = useState('')
  const [resolution, setResolution] = useState<BindingResolution | null>(null)
  const [message, setMessage] = useState('')
  const [busy, setBusy] = useState(false)
  const controller = useRef<AbortController | null>(null)
  useEffect(() => () => controller.current?.abort(), [client])
  const begin = () => {
    controller.current?.abort()
    const next = new AbortController()
    controller.current = next
    setBusy(true)
    setMessage('')
    return next.signal
  }
  const clearQuery = () => {
    controller.current?.abort()
    setItems([])
    setCursor(null)
    setResolution(null)
    setBusy(false)
    setMessage('')
  }
  const fail = (error: unknown, signal: AbortSignal) => {
    if (signal.aborted) return
    if (error instanceof PlatformProblem && [401, 403].includes(error.status)) {
      setItems([])
      setCursor(null)
      setResolution(null)
      setSelections(EMPTY)
      setSlotId('')
    }
    setMessage(error instanceof Error ? error.message : 'The authoring query failed.')
  }
  const discover = async (next?: string) => {
    if (!client) return
    const signal = begin()
    try {
      const page = await client.listAuthoringDependencies(
        {
          kind,
          environment: environment.trim() || undefined,
          interfaceContractDigest: contract.trim() || undefined,
          cursor: next,
        },
        { signal },
      )
      if (signal.aborted) return
      setItems(page.data.items)
      setCursor(page.data.next_cursor)
      setMessage(
        page.data.items.length
          ? '已读取当前工作空间的部署。'
          : page.data.next_cursor
            ? '本页没有可见部署，可继续查看下一页。'
            : '本页没有可见部署。',
      )
    } catch (error) {
      fail(error, signal)
    } finally {
      if (!signal.aborted) setBusy(false)
    }
  }
  const resolve = async () => {
    if (!client) return
    const signal = begin()
    setResolution(null)
    try {
      const request = parseBindingSelections(selections)
      const response = await client.resolveAgentBindings(request, { signal })
      if (signal.aborted) return
      setResolution(response.data)
      setMessage('请检查契约匹配与调用授权后再应用精确绑定。')
    } catch (error) {
      fail(error, signal)
    } finally {
      if (!signal.aborted) setBusy(false)
    }
  }
  const select = (item: AuthoringDependency, active: boolean) => {
    try {
      const request = parseBindingSelections(selections)
      const slot = request.slots.find((entry) => entry.slot_id === slotId.trim())
      if (!slot || slot.target.kind !== item.kind)
        throw new Error('请填写选择 JSON 中具有相同依赖类型的槽 ID。')
      const selector: Json = active
        ? { kind: 'active', resource_id: item.resource_id, environment: item.environment }
        : { kind: 'exact', deployment: { ...item.deployment } }
      if (slot.target.kind === 'context') slot.target.deployment = selector
      else slot.target.candidates = [selector]
      setSelections(JSON.stringify(request, null, 2))
      setResolution(null)
      setMessage(`已替换 ${slot.slot_id} 的目标，请解析后再应用。`)
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'The target could not be selected.')
    }
  }
  const apply = () => {
    if (!resolution || resolution.slots.some((slot) => slot.resolution.kind !== 'resolved')) return
    const slots = resolution.slots.flatMap((slot) =>
      slot.resolution.kind === 'resolved' ? [slot.resolution.binding as Json] : [],
    )
    onResolved(slots)
    setMessage('已应用精确绑定，请在发布前校验完整源码。')
  }
  return (
    <details
      data-ui="nested-panel authoring-bindings"
      className={cx('nested-panel authoring-bindings')}
    >
      <summary>发现与解析依赖绑定</summary>
      <fieldset
        disabled={disabled || busy || !client}
        style={{ border: 0, margin: 0, padding: 0, minWidth: 0 }}
      >
        <div className={cx('form-grid')}>
          <label>
            <span>依赖类型</span>
            <select
              value={kind}
              onChange={(event) => {
                clearQuery()
                setKind(event.target.value as DependencyKind)
              }}
            >
              {['model', 'capability', 'context', 'child_agent', 'skill'].map((value) => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </label>
          <label>
            <span>依赖环境</span>
            <input
              maxLength={64}
              value={environment}
              onChange={(event) => {
                clearQuery()
                setEnvironment(event.target.value)
              }}
              placeholder="所有环境"
            />
          </label>
          <label className={cx('field--wide')}>
            <span>期望接口契约摘要</span>
            <input
              maxLength={71}
              value={contract}
              onChange={(event) => {
                clearQuery()
                setContract(event.target.value)
              }}
              placeholder="可选；留空表示不检查契约兼容性"
            />
          </label>
        </div>
        <div className={cx('actions')}>
          <button className={cx('button')} onClick={() => discover()}>
            查找部署
          </button>
          {cursor && (
            <button className={cx('button')} onClick={() => discover(cursor)}>
              下一页依赖
            </button>
          )}
        </div>
        {items.length > 0 && (
          <>
            <label>
              <span>目标依赖槽 ID</span>
              <input
                value={slotId}
                maxLength={128}
                onChange={(event) => setSlotId(event.target.value)}
                placeholder="要在选择 JSON 中替换的依赖槽"
              />
            </label>
            <div className={cx('stack')}>
              {items.map((item) => (
                <div
                  data-ui="nested-panel"
                  className={cx('nested-panel')}
                  key={item.deployment.deployment_id}
                >
                  <strong>
                    {item.resource_id} · {item.environment}
                  </strong>
                  <p>
                    契约匹配：{' '}
                    {item.contract_match === null ? '未检查' : item.contract_match ? '是' : '否'} ·
                    调用授权： {item.call_authorized ? '是' : '否'}
                  </p>
                  <code>{item.deployment.deployment_id}</code>
                  <div className={cx('actions')}>
                    <button className={cx('button')} onClick={() => select(item, false)}>
                      使用精确部署
                    </button>
                    <button className={cx('button')} onClick={() => select(item, true)}>
                      使用当前激活部署
                    </button>
                  </div>
                  <details>
                    <summary>精确部署与接口契约</summary>
                    <pre>{JSON.stringify(item, null, 2)}</pre>
                  </details>
                </div>
              ))}
            </div>
          </>
        )}
        <label className={cx('json-field')}>
          <span>依赖选择 JSON</span>
          <textarea
            rows={12}
            value={selections}
            maxLength={262_144}
            spellCheck={false}
            onChange={(event) => {
              setSelections(event.target.value)
              setResolution(null)
            }}
          />
          <small>
            使用 schema_version 1，提供槽 ID、需求摘要、可选接口摘要及 Active/Exact
            目标。策略须为精确引用；解析只读取当前状态。
          </small>
        </label>
        <button className={cx('button')} onClick={resolve}>
          解析依赖选择
        </button>
        {resolution && (
          <div>
            {resolution.slots.map((slot) => (
              <p key={slot.slot_id}>
                <strong>{slot.slot_id}</strong>:{' '}
                {slot.resolution.kind === 'rejected'
                  ? `Rejected: ${slot.resolution.code}`
                  : `契约匹配：${slot.resolution.contract_match === null ? '未检查' : slot.resolution.contract_match ? '是' : '否'} · 调用授权：${slot.resolution.call_authorized ? '是' : '否'}`}
              </p>
            ))}
            <button
              className={cx('button')}
              disabled={resolution.slots.some((slot) => slot.resolution.kind !== 'resolved')}
              onClick={apply}
            >
              应用已解析绑定
            </button>
          </div>
        )}
      </fieldset>
      <p role="status" aria-live="polite">
        {busy ? '正在读取依赖…' : message}
      </p>
    </details>
  )
}

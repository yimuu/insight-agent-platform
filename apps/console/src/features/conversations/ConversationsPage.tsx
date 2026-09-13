import { useEffect, useRef, useState } from 'react'
import { PlatformClient, PlatformProblem } from '../../shared/api/client'
import type { Conversation, ConversationTurn } from '../../shared/api/conversation-types'
import type {
  AgentSummary,
  ArtifactRef,
  JsonObject,
  RunEvent,
  RunView,
} from '../../shared/api/types'
import { readExactArtifact } from '../../shared/api/artifact-content'
import { newReceipt } from '../../shared/api/security'
import { utcTimestamp } from '../../shared/api/time'
import { displayState } from '../../shared/i18n/display'
import { ExecutionCanvas } from '../runs/ExecutionCanvas'
import { NoticeBox, Status } from '../../shared/ui/console-ui'
import { errorNotice } from '../../shared/ui/feedback'
import type { Notice } from '../../shared/ui/feedback'
import styles from './Conversations.module.css'
import { liveAnswer } from './live-answer'
import { failureSummary } from './failure-summary'
const terminal = (state: string) =>
  ['succeeded', 'failed', 'cancelled', 'timed_out'].includes(state)
interface Message {
  turn: ConversationTurn
  run: RunView
  input: string
  answer: string | null
  answerError?: string
}
interface Live {
  attempt: number
  text: string
  sequence: number
}
class PreviewLimitError extends Error {}
async function textValue(
  client: PlatformClient,
  content: JsonObject,
  field: string,
  signal: AbortSignal,
): Promise<string> {
  const value = content.value as JsonObject
  let body: unknown
  if (value?.kind === 'inline') body = value.value
  else if (value?.kind === 'artifact') {
    if (Number((value.artifact as JsonObject)?.byte_length) > 262144) throw new PreviewLimitError()
    const blob = await readExactArtifact(client, value.artifact as unknown as ArtifactRef, {
      signal,
      maximumBytes: 262144,
    })
    body = JSON.parse(await blob.text())
  } else throw new Error('会话正文格式不完整。')
  signal.throwIfAborted()
  if (
    !body ||
    typeof body !== 'object' ||
    Array.isArray(body) ||
    typeof (body as Record<string, unknown>)[field] !== 'string'
  )
    throw new Error('会话正文不符合已发布的文本接口。')
  const text = (body as Record<string, string>)[field]!
  if (new TextEncoder().encode(text).length > 262144) throw new PreviewLimitError()
  return text
}
export function Conversations({
  client,
  launchAgent,
  active: visible,
}: {
  client: PlatformClient | null
  launchAgent: AgentSummary | null
  active: boolean
}) {
  const [agents, setAgents] = useState<AgentSummary[]>([])
  const [agentCursor, setAgentCursor] = useState<string | null>(null)
  const [agent, setAgent] = useState(launchAgent?.agent_id ?? '')
  const [items, setItems] = useState<Conversation[]>([])
  const [next, setNext] = useState<string | null>(null)
  const [id, setId] = useState(
    () => new URL(window.location.href).searchParams.get('conversation') ?? '',
  )
  const [conversation, setConversation] = useState<Conversation | null>(null)
  const [messages, setMessages] = useState<Message[]>([])
  const [selectedRun, setSelectedRun] = useState('')
  const [events, setEvents] = useState<RunEvent[]>([])
  const [live, setLive] = useState<Record<string, Live>>({})
  const [partial, setPartial] = useState(false)
  const [streamState, setStreamState] = useState('')
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [loading, setLoading] = useState(false)
  const [notice, setNotice] = useState<Notice | null>(null)
  const [reload, setReload] = useState(0)
  const [retryLive, setRetryLive] = useState(0)
  const generation = useRef(0)
  const loadAbort = useRef<AbortController | null>(null)
  const intent = useRef<{
    id: string
    message: string
    version: number
    deadline: string
    receipt: string
  } | null>(null)
  const createIntent = useRef<{ agent: string; receipt: string } | null>(null)
  const sending = useRef(false)
  const stopping = useRef(false)
  const cancelIntent = useRef<{ run: string; etag: string; receipt: string } | null>(null)
  const bottom = useRef<HTMLDivElement>(null)
  const followBottom = useRef(true)
  const select = (value: string) => {
    if (value === id) return
    followBottom.current = true
    generation.current++
    loadAbort.current?.abort()
    setId(value)
    setConversation(null)
    setMessages([])
    setSelectedRun('')
    setEvents([])
    setLive({})
    setInput('')
    setNotice(null)
    intent.current = null
    const url = new URL(window.location.href)
    if (value) url.searchParams.set('conversation', value)
    else url.searchParams.delete('conversation')
    window.history.replaceState(null, '', url)
  }
  useEffect(() => {
    if (launchAgent) {
      setAgent(launchAgent.agent_id)
      select('')
    }
  }, [launchAgent])
  useEffect(() => {
    if (!client || !visible) return
    const c = new AbortController()
    void Promise.all([
      client.listAgents(),
      client.listConversations(undefined, undefined, c.signal),
    ])
      .then(([a, l]) => {
        if (!c.signal.aborted) {
          setAgents(a.data.items)
          setAgentCursor(a.data.next_cursor)
          setItems(l.data.items)
          setNext(l.data.next_cursor)
        }
      })
      .catch((e) => {
        if (!c.signal.aborted) setNotice(errorNotice(e))
      })
    return () => c.abort()
  }, [client, visible])
  useEffect(() => {
    if (!client || !id) return
    const c = new AbortController()
    const g = generation.current
    loadAbort.current = c
    setLoading(true)
    setNotice(null)
    const load = async () => {
      const current = (await client.getConversation(id, c.signal)).data
      if (c.signal.aborted || g !== generation.current) return
      setConversation(current)
      const turns: ConversationTurn[] = []
      let cursor: string | undefined
      do {
        const page = (await client.listConversationTurns(id, cursor, c.signal)).data
        turns.push(...page.items)
        cursor = page.next_cursor ?? undefined
        if (turns.length > 128) throw new Error('会话轮数超出限制。')
      } while (cursor)
      const loaded: Message[] = []
      for (let at = 0; at < turns.length; at += 4) {
        const batch = await Promise.all(
          turns.slice(at, at + 4).map(async (turn) => {
            const run = (await client.getRun(turn.run_id, { signal: c.signal })).data
            const content = (
              await client.getRunValueContent(run.run_id, run.input_value_id, { signal: c.signal })
            ).data
            const input = await textValue(client, content, current.input_field, c.signal)
            let answer: string | null = null
            let answerError: string | undefined
            if (run.state === 'succeeded' && run.output_value_id) {
              try {
                answer = await textValue(
                  client,
                  (await client.getRunResult(run.run_id, { signal: c.signal })).data,
                  'answer',
                  c.signal,
                )
              } catch (e) {
                if (
                  c.signal.aborted ||
                  (e instanceof PlatformProblem && [401, 403].includes(e.status))
                )
                  throw e
                answerError =
                  e instanceof PreviewLimitError
                    ? '回答超出预览上限。可在运行记录中读取完整结果。'
                    : '暂时无法读取回答，请刷新重试。'
              }
            }
            return { turn, run, input, answer, answerError }
          }),
        )
        loaded.push(...batch)
      }
      if (c.signal.aborted || g !== generation.current) return
      setMessages(loaded)
      setSelectedRun((previous) =>
        loaded.some((m) => m.run.run_id === previous)
          ? previous
          : (loaded.at(-1)?.run.run_id ?? ''),
      )
    }
    void load()
      .catch((e) => {
        if (!c.signal.aborted && g === generation.current) {
          setMessages([])
          setLive({})
          setNotice(errorNotice(e))
        }
      })
      .finally(() => {
        if (!c.signal.aborted && g === generation.current) setLoading(false)
      })
    return () => c.abort()
  }, [client, id, reload])
  const active = messages.find((m) => !terminal(m.run.state))
  const currentDeployment = agents.find(
    (a) => a.agent_id === conversation?.agent_id,
  )?.active_deployment
  const newerDeployment = Boolean(
    conversation &&
    currentDeployment &&
    currentDeployment.deployment_id !== conversation.agent_deployment.deployment_id,
  )
  useEffect(() => {
    if (!client || !selectedRun) return
    const c = new AbortController()
    const g = generation.current
    setEvents([])
    let refreshing = false
    let ended = false
    void client
      .followRunEvents(selectedRun, {
        signal: c.signal,
        onClear: () => {
          if (c.signal.aborted || g !== generation.current) return
          setEvents([])
        },
        onUpdate: (s) => {
          if (c.signal.aborted || g !== generation.current) return
          setEvents([...s.events])
          if (refreshing || ended) return
          refreshing = true
          void client
            .getRun(selectedRun, { signal: c.signal })
            .then(({ data }) => {
              if (c.signal.aborted || g !== generation.current) return
              setMessages((ms) =>
                ms.map((m) => (m.run.run_id === data.run_id ? { ...m, run: data } : m)),
              )
              if (terminal(data.state)) {
                ended = true
                setReload((n) => n + 1)
              }
            })
            .catch((e) => {
              if (!c.signal.aborted && g === generation.current) {
                setNotice(errorNotice(e))
                if (e instanceof PlatformProblem && [401, 403].includes(e.status)) {
                  setMessages([])
                  setLive({})
                  c.abort()
                }
              }
            })
            .finally(() => {
              refreshing = false
            })
        },
      })
      .catch((e) => {
        if (!c.signal.aborted && g === generation.current) {
          setNotice(errorNotice(e))
          if (e instanceof PlatformProblem && [401, 403].includes(e.status)) {
            setMessages([])
            setLive({})
            setEvents([])
          }
        }
      })
    return () => c.abort()
  }, [client, selectedRun])
  useEffect(() => {
    if (!client || !active) return
    const c = new AbortController()
    const g = generation.current
    let timer: ReturnType<typeof setTimeout>
    let done = false
    const poll = async () => {
      try {
        const { data } = await client.getRun(active.run.run_id, { signal: c.signal })
        if (c.signal.aborted || g !== generation.current) return
        setMessages((ms) => ms.map((m) => (m.run.run_id === data.run_id ? { ...m, run: data } : m)))
        if (terminal(data.state)) {
          done = true
          setReload((n) => n + 1)
        }
      } catch (e) {
        if (
          !c.signal.aborted &&
          g === generation.current &&
          e instanceof PlatformProblem &&
          [401, 403].includes(e.status)
        ) {
          done = true
          setMessages([])
          setLive({})
          setNotice(errorNotice(e))
        }
      } finally {
        if (!c.signal.aborted && g === generation.current && !done)
          timer = setTimeout(() => void poll(), 2000)
      }
    }
    timer = setTimeout(() => void poll(), 2000)
    return () => {
      c.abort()
      clearTimeout(timer)
    }
  }, [client, active?.run.run_id])
  useEffect(() => {
    if (!client || !active) return
    const c = new AbortController()
    const g = generation.current
    setLive({})
    setPartial(false)
    setStreamState('正在连接实时输出…')
    let observed: Record<string, Live> = {}
    void client
      .followLiveText(active.run.run_id, {
        signal: c.signal,
        onFrame: (f) => {
          if (c.signal.aborted || g !== generation.current) return
          if (f.kind === 'opened') {
            setPartial(true)
            setStreamState('正在接收实时输出')
          }
          if (f.kind === 'reset') {
            observed = {
              ...observed,
              [f.model_turn_id]: { attempt: f.attempt_no, text: '', sequence: 0 },
            }
            setLive(observed)
          }
          if (f.kind === 'text') {
            const old = observed[f.model_turn_id]
            if (old && old.attempt > f.attempt_no) return
            if (old?.attempt === f.attempt_no && f.text_sequence <= old.sequence) return
            const text = (old?.attempt === f.attempt_no ? old.text : '') + f.text
            const total = Object.entries(observed).reduce(
              (n, [key, value]) =>
                n + (key === f.model_turn_id ? 0 : new TextEncoder().encode(value.text).length),
              new TextEncoder().encode(text).length,
            )
            if (total > 1048576) {
              c.abort()
              setStreamState('实时文本达到显示上限，等待完整结果。')
              return
            }
            observed = {
              ...observed,
              [f.model_turn_id]: { attempt: f.attempt_no, text, sequence: f.text_sequence },
            }
            setLive(observed)
          }
          if (f.kind === 'gap') {
            setPartial(true)
            setStreamState('实时输出可能不完整，完成后显示完整回答。')
          }
          if (f.kind === 'closed') {
            setStreamState(f.reason === 'terminal' ? '正在读取完整回答…' : '实时连接已结束')
            if (['authorization_changed', 'expired'].includes(f.reason)) {
              setLive({})
              setMessages([])
            }
            setReload((n) => n + 1)
          }
        },
      })
      .catch((e) => {
        if (!c.signal.aborted && g === generation.current) {
          setStreamState('实时连接中断，可重新连接；最终结果仍会更新。')
          setPartial(true)
          if (e instanceof PlatformProblem && [401, 403].includes(e.status)) {
            setMessages([])
            setLive({})
            setNotice(errorNotice(e))
          }
        }
      })
    return () => c.abort()
  }, [client, active?.run.run_id, retryLive])
  useEffect(() => {
    if (followBottom.current) bottom.current?.scrollIntoView({ block: 'nearest' })
  }, [messages.length, live])
  const create = async () => {
    if (!client || !agent || sending.current) return
    sending.current = true
    setBusy(true)
    const g = generation.current
    if (createIntent.current?.agent !== agent)
      createIntent.current = { agent, receipt: newReceipt('conversation-create') }
    try {
      const title = agents.find((a) => a.agent_id === agent)?.display_name ?? '新对话'
      const result = await client.createConversation(
        agent,
        title.slice(0, 40),
        createIntent.current.receipt,
      )
      if (g !== generation.current) return
      createIntent.current = null
      setItems((old) => [
        result.data,
        ...old.filter((x) => x.conversation_id !== result.data.conversation_id),
      ])
      select(result.data.conversation_id)
    } catch (e) {
      if (g === generation.current)
        setNotice(
          e instanceof PlatformProblem && e.status === 400
            ? {
                tone: 'error',
                text: '该智能体尚不支持对话。请发布一个必填文本输入字段，并使用 answer 文本输出。',
              }
            : errorNotice(e),
        )
    } finally {
      sending.current = false
      setBusy(false)
    }
  }
  const send = async () => {
    if (
      !client ||
      !conversation ||
      !input.trim() ||
      loading ||
      active ||
      newerDeployment ||
      sending.current
    )
      return
    if (new TextEncoder().encode(input).length > 16384) {
      setNotice({ tone: 'error', text: '消息不能超过 16 KiB。' })
      return
    }
    sending.current = true
    setBusy(true)
    setNotice(null)
    const g = generation.current
    if (intent.current?.id !== id || intent.current?.message !== input)
      intent.current = {
        id,
        message: input,
        version: conversation.version,
        deadline: utcTimestamp(new Date(Date.now() + 300000)),
        receipt: newReceipt('conversation-send'),
      }
    const command = intent.current
    try {
      const { data } = await client.sendConversationMessage(
        id,
        command.version,
        command.message,
        command.deadline,
        command.receipt,
      )
      if (g !== generation.current) return
      intent.current = null
      setInput('')
      setSelectedRun(data.run_id)
      setReload((n) => n + 1)
    } catch (e) {
      if (g === generation.current) {
        setNotice(errorNotice(e))
        if (
          e instanceof PlatformProblem &&
          e.status >= 400 &&
          e.status < 500 &&
          ![408, 429].includes(e.status)
        )
          intent.current = null
        if (e instanceof PlatformProblem && [401, 403].includes(e.status)) {
          setMessages([])
          setLive({})
          setEvents([])
        }
        if (e instanceof PlatformProblem && e.status === 409) {
          setNotice({
            tone: 'error',
            text: '会话状态已变化，请刷新后重试。如果智能体已重新发布，请新建对话。',
          })
          intent.current = null
          setReload((n) => n + 1)
        }
      }
    } finally {
      sending.current = false
      setBusy(false)
    }
  }
  const stop = async () => {
    if (!client || !active || busy || stopping.current) return
    stopping.current = true
    if (cancelIntent.current?.run !== active.run.run_id)
      cancelIntent.current = {
        run: active.run.run_id,
        etag: active.run.etag,
        receipt: newReceipt(`cancel-${active.run.run_id}`),
      }
    const command = cancelIntent.current
    setBusy(true)
    const g = generation.current
    try {
      await client.runAction(command.run, 'cancel', command.etag, command.receipt)
      cancelIntent.current = null
      if (g === generation.current) setReload((n) => n + 1)
    } catch (e) {
      if (g === generation.current) {
        setNotice(errorNotice(e))
        if (e instanceof PlatformProblem && e.status === 409) {
          cancelIntent.current = null
          setReload((n) => n + 1)
        }
        if (e instanceof PlatformProblem && [401, 403].includes(e.status)) {
          setMessages([])
          setLive({})
          setEvents([])
        }
      }
    } finally {
      stopping.current = false
      setBusy(false)
    }
  }
  return (
    <div className={styles.layout}>
      <aside className={styles.sidebar}>
        <header>
          <strong>工作空间对话</strong>
          <button onClick={() => select('')}>＋ 新对话</button>
        </header>
        {items.map((item) => (
          <button
            className={styles.item}
            aria-current={item.conversation_id === id ? 'page' : undefined}
            key={item.conversation_id}
            onClick={() => select(item.conversation_id)}
          >
            <strong>{item.title}</strong>
            <small>{new Date(item.created_at).toLocaleString()}</small>
          </button>
        ))}
        {next && (
          <button
            onClick={() => {
              if (client)
                void client
                  .listConversations(undefined, next)
                  .then(({ data }) => {
                    setItems((old) => [...old, ...data.items])
                    setNext(data.next_cursor)
                  })
                  .catch((e) => setNotice(errorNotice(e)))
            }}
          >
            更多对话
          </button>
        )}
      </aside>
      <main className={styles.chat}>
        <header>
          <h2>{conversation?.title ?? '开始新的对话'}</h2>
          {conversation && (
            <button disabled={loading || busy} onClick={() => setReload((n) => n + 1)}>
              刷新
            </button>
          )}
        </header>
        <NoticeBox notice={notice} />
        {newerDeployment && (
          <div role="status" className={styles.hint}>
            智能体已更新。本会话保留原版本和历史记录，请新建对话使用更新后的配置。
            <button
              disabled={busy}
              onClick={() => {
                setAgent(conversation!.agent_id)
                select('')
              }}
            >
              使用新版本对话
            </button>
          </div>
        )}
        {!id ? (
          <div className={styles.welcome}>
            <h3>和你的 Agent 对话</h3>
            <p>消息保存在工作空间，刷新后可以继续。</p>
            <label>
              选择智能体
              <select value={agent} onChange={(e) => setAgent(e.target.value)}>
                <option value="">请选择已发布的对话智能体</option>
                {(launchAgent && !agents.some((a) => a.agent_id === launchAgent.agent_id)
                  ? [launchAgent, ...agents]
                  : agents
                )
                  .filter((a) => a.active_deployment)
                  .map((a) => (
                    <option key={a.agent_id} value={a.agent_id}>
                      {a.display_name}
                    </option>
                  ))}
              </select>
            </label>
            {agentCursor && (
              <button
                onClick={() => {
                  if (client)
                    void client
                      .listAgents(agentCursor)
                      .then(({ data }) => {
                        setAgents((old) => [
                          ...old,
                          ...data.items.filter(
                            (item) => !old.some((a) => a.agent_id === item.agent_id),
                          ),
                        ])
                        setAgentCursor(data.next_cursor)
                      })
                      .catch((e) => setNotice(errorNotice(e)))
                }}
              >
                加载更多智能体
              </button>
            )}
            <button disabled={!agent || busy} onClick={() => void create()}>
              {busy ? '正在创建…' : '开始对话'}
            </button>
          </div>
        ) : (
          <>
            <div
              className={styles.messages}
              onScroll={(e) => {
                const panel = e.currentTarget
                followBottom.current =
                  panel.scrollHeight - panel.scrollTop - panel.clientHeight < 64
              }}
            >
              {loading && messages.length === 0 && <p role="status">正在读取会话…</p>}
              {!loading && messages.length === 0 && (
                <p className={styles.hint}>发送第一条消息开始调试。</p>
              )}
              {messages.map((m) => (
                <article key={m.turn.run_id}>
                  <div className={styles.user}>
                    <small>你</small>
                    <p>{m.input}</p>
                  </div>
                  <div className={styles.answer}>
                    <div className={styles.answerHead}>
                      <strong>Agent</strong>
                      <Status value={m.run.state} />
                      <button
                        aria-pressed={selectedRun === m.run.run_id}
                        onClick={() => setSelectedRun(m.run.run_id)}
                      >
                        {m.run.state === 'failed' ? '查看失败原因' : '查看执行'}
                      </button>
                    </div>
                    {m.answer !== null ? (
                      <p>{m.answer}</p>
                    ) : m.answerError ? (
                      <p>{m.answerError}</p>
                    ) : active?.run.run_id === m.run.run_id ? (
                      <>
                        {Object.entries(live).map(([key, value], index) => (
                          <div key={key}>
                            <small>模型调用 {index + 1}</small>
                            <p>{liveAnswer(value.text)}</p>
                          </div>
                        ))}
                        <small role="status">{streamState || '正在执行…'}</small>
                        {partial && (
                          <small className={styles.hint}>
                            实时预览可能缺少开头，以完成后的回答为准。
                          </small>
                        )}
                        <button onClick={() => setRetryLive((n) => n + 1)}>重新连接实时输出</button>
                      </>
                    ) : (
                      <p>
                        {(['failed', 'timed_out'].includes(m.run.state)
                          ? failureSummary(events, m.run.run_id)
                          : null) ??
                          (m.run.state === 'failed'
                            ? '本轮未完成。点击“查看失败原因”读取执行记录；如记录未提供原因，请保留运行编号以便排查。'
                            : m.run.state === 'timed_out'
                              ? '本轮已超过执行时限。可以缩短请求后重新发送。'
                              : displayState(m.run.state))}
                      </p>
                    )}
                  </div>
                </article>
              ))}
              <div ref={bottom} />
            </div>
            <form
              className={styles.composer}
              onSubmit={(e) => {
                e.preventDefault()
                void send()
              }}
            >
              <textarea
                aria-label="消息"
                placeholder="输入消息…"
                rows={3}
                value={input}
                disabled={busy || loading || !conversation || newerDeployment}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
                    e.preventDefault()
                    void send()
                  }
                }}
              />
              <div>
                <small>Ctrl / ⌘ + Enter 发送</small>
                {active ? (
                  <button
                    type="button"
                    disabled={busy || active.run.state === 'cancelling'}
                    onClick={() => void stop()}
                  >
                    停止生成
                  </button>
                ) : (
                  <button
                    disabled={busy || loading || !conversation || newerDeployment || !input.trim()}
                  >
                    {busy ? '发送中…' : '发送'}
                  </button>
                )}
              </div>
            </form>
          </>
        )}
      </main>
      <aside className={styles.execution}>
        <header>
          <strong>执行详情</strong>
          <small>选择消息查看对应轮次</small>
        </header>
        <ExecutionCanvas key={selectedRun} events={events} client={client} runId={selectedRun} />
      </aside>
    </div>
  )
}

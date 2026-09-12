import sharedStyles from '../../shared/ui/Primitives.module.css'
import { classNames } from '../../shared/ui/class-names.ts'
import localStyles from './Connection.module.css'
const cx = classNames(sharedStyles, localStyles)
import { useEffect, useRef, useState } from 'react'
import { PlatformClient } from '../../shared/api/client.ts'
import { NoticeBox } from '../../shared/ui/console-ui'
import { errorNotice } from '../../shared/ui/feedback'
import type { Notice } from '../../shared/ui/feedback'
import { readTokenFile, tokenExpiry } from './session'
import type { ConsoleSession } from './session'
import { browserAuth, checkedBrowserSession } from './browser-session'
import type { BrowserSessionState } from './browser-session'

export function ConnectionPage(props: {
  reason?: string
  onConnect: (session: ConsoleSession) => void
  initialOrigin?: string
  onCancel?: () => void
}) {
  const [state, setState] = useState<BrowserSessionState | null>(null)
  const [notice, setNotice] = useState<Notice | null>(null)
  const [busy, setBusy] = useState(false)
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [name, setName] = useState('')
  const [showPassword, setShowPassword] = useState(false)
  const [retry, setRetry] = useState(0)
  const connected = useRef(props.onConnect)
  connected.current = props.onConnect
  const pending = useRef<AbortController | null>(null)
  const connect = async (next: BrowserSessionState, signal: AbortSignal) => {
    const client = new PlatformClient(window.location.origin, '', 'cookie')
    try {
      await client.listAgents()
      if (signal.aborted) {
        client.dispose()
        return
      }
      connected.current({
        key: crypto.randomUUID(),
        client,
        expiresAt: next.expires_at ? Date.parse(next.expires_at) : null,
      })
    } catch (error) {
      client.dispose()
      throw error
    }
  }
  useEffect(() => {
    const controller = new AbortController()
    pending.current = controller
    setNotice(null)
    void browserAuth('session', undefined, controller.signal)
      .then(async (value) => {
        const next = checkedBrowserSession(value)
        if (controller.signal.aborted) return
        setState(next)
        if (next.authenticated) await connect(next, controller.signal)
      })
      .catch((error) => {
        if (!controller.signal.aborted) setNotice(errorNotice(error))
      })
    return () => controller.abort()
  }, [retry])
  const submit = async () => {
    if (busy || !state) return
    setBusy(true)
    setNotice(null)
    try {
      const input = {
        schema_version: 1,
        email,
        password,
        ...(state.setup_required ? { display_name: name } : {}),
      }
      await browserAuth(state.setup_required ? 'setup' : 'login', input, pending.current?.signal)
      setPassword('')
      const next = checkedBrowserSession(
        await browserAuth('session', undefined, pending.current?.signal),
      )
      if (!next.authenticated)
        throw new Error('登录未完成，请检查浏览器是否允许本站 Cookie 后重试。')
      await connect(next, pending.current!.signal)
    } catch (error) {
      if (!pending.current?.signal.aborted) {
        setNotice(errorNotice(error))
        // A competing setup may have completed; read current authority instead of retrying setup.
        try {
          setState(
            checkedBrowserSession(await browserAuth('session', undefined, pending.current?.signal)),
          )
        } catch {
          /* Retain the original actionable error. */
        }
      }
    } finally {
      if (!pending.current?.signal.aborted) setBusy(false)
    }
  }
  if (state?.authentication === 'bearer') return <TokenConnectionPage {...props} />
  return (
    <section className={cx('connection-page')}>
      <div className={cx('connection-intro')}>
        <span className={cx('brand__mark')}>IA</span>
        <h1>{state?.setup_required ? '创建你的工作空间' : '欢迎回来'}</h1>
        <p className={cx('body-copy')}>
          {state?.setup_required
            ? '设置管理员账号，开始构建你的第一个智能体。'
            : '登录 Insight，继续你的工作。'}
        </p>
      </div>
      <form
        className={cx('panel stack')}
        aria-label={state?.setup_required ? '创建管理员' : '登录工作空间'}
        onSubmit={(event) => {
          event.preventDefault()
          void submit()
        }}
      >
        <NoticeBox
          notice={notice ?? (props.reason ? { tone: 'info', text: props.reason } : null)}
        />
        {!state ? (
          <>
            <p role="status">{notice ? '登录服务暂不可用' : '正在连接工作空间…'}</p>
            {notice && (
              <button
                type="button"
                className={cx('button')}
                onClick={() => setRetry((value) => value + 1)}
              >
                重新连接
              </button>
            )}
          </>
        ) : (
          <>
            {state.setup_required && (
              <label>
                <span>你的名字</span>
                <input
                  autoComplete="name"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                  maxLength={128}
                  required
                  disabled={busy}
                  autoFocus
                />
              </label>
            )}
            <label>
              <span>邮箱</span>
              <input
                type="email"
                autoComplete="username"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                maxLength={254}
                required
                disabled={busy}
                autoFocus={!state.setup_required}
                placeholder="you@example.com"
              />
            </label>
            <label>
              <span>密码</span>
              <input
                type={showPassword ? 'text' : 'password'}
                autoComplete={state.setup_required ? 'new-password' : 'current-password'}
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                minLength={12}
                maxLength={256}
                required
                disabled={busy}
              />
              {state.setup_required && <small>至少 12 个字符，可使用密码管理器生成。</small>}
            </label>
            <button
              type="button"
              className={cx('button button--link')}
              onClick={() => setShowPassword((value) => !value)}
            >
              {showPassword ? '隐藏密码' : '显示密码'}
            </button>
            <button className={cx('button button--primary')} disabled={busy}>
              {busy ? '正在登录…' : state.setup_required ? '创建账号并开始' : '登录'}
            </button>
          </>
        )}
      </form>
    </section>
  )
}

export function TokenConnectionPage({
  onConnect,
  initialOrigin = window.location.origin,
  onCancel,
  reason = '',
}: {
  reason?: string
  onConnect: (session: ConsoleSession) => void
  initialOrigin?: string
  onCancel?: () => void
}) {
  const [origin, setOrigin] = useState(initialOrigin)
  const [token, setToken] = useState('')
  const [busy, setBusy] = useState(false)
  const [notice, setNotice] = useState<Notice | null>(null)
  const pending = useRef<PlatformClient | null>(null)
  const revision = useRef(0)
  useEffect(
    () => () => {
      revision.current++
      pending.current?.dispose()
    },
    [],
  )
  const connect = async () => {
    if (busy) return
    const request = ++revision.current
    setBusy(true)
    setNotice(null)
    let client: PlatformClient | null = null
    try {
      const value = readTokenFile(token)
      const expiresAt = tokenExpiry(value)
      if (expiresAt !== null && expiresAt <= Date.now())
        throw new Error('会话已过期，请取得新的令牌后重新连接。')
      client = new PlatformClient(origin, value)
      pending.current = client
      if (!(await client.readiness())) throw new Error('服务尚未就绪，请稍后重试。')
      await client.listAgents()
      if (revision.current !== request) {
        client.dispose()
        return
      }
      pending.current = null
      setToken('')
      onConnect({ key: crypto.randomUUID(), client, expiresAt })
    } catch (error) {
      client?.dispose()
      if (revision.current === request) setNotice(errorNotice(error))
    } finally {
      if (revision.current === request) setBusy(false)
    }
  }
  return (
    <section className={cx('connection-page')}>
      <div className={cx('connection-intro')}>
        <span className={cx('brand__mark')}>IA</span>
        <p data-ui="kicker" className={cx('kicker')}>
          INSIGHT 智能体平台
        </p>
        <h1>连接你的工作空间</h1>
        <p className={cx('body-copy')}>配置模型、创建智能体，让任务从这里开始。</p>
      </div>
      <form
        className={cx('panel stack')}
        data-ui="connection-form"
        aria-label="连接工作空间"
        onSubmit={(event) => {
          event.preventDefault()
          void connect()
        }}
      >
        <div>
          <h2>工作空间会话</h2>
          <p className={cx('body-copy')}>
            令牌仅保存在本次页面的内存中。刷新或退出后需要重新连接。
          </p>
        </div>
        <NoticeBox notice={notice ?? (reason ? { tone: 'info', text: reason } : null)} />
        <label>
          <span>访问令牌</span>
          <input
            autoFocus
            type="password"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            maxLength={32_768}
            autoComplete="off"
            spellCheck={false}
            required
            disabled={busy}
            placeholder="粘贴访问令牌"
          />
        </label>
        <label className={cx('button file-button')}>
          导入令牌文件
          <input
            type="file"
            disabled={busy}
            onChange={async (event) => {
              const file = event.target.files?.[0]
              event.target.value = ''
              if (!file) return
              const current = ++revision.current
              try {
                if (file.size > 32_768) throw new Error('令牌文件超过 32 KiB 限制。')
                const value = readTokenFile(await file.text())
                if (revision.current === current) {
                  setToken(value)
                  setNotice(null)
                }
              } catch (error) {
                if (revision.current === current) setNotice(errorNotice(error))
              }
            }}
          />
        </label>
        <details>
          <summary>高级连接设置</summary>
          <label className={cx('secondary-field')}>
            <span>服务地址</span>
            <input
              type="url"
              value={origin}
              onChange={(event) => setOrigin(event.target.value)}
              required
              disabled={busy}
            />
          </label>
        </details>
        <details>
          <summary>如何取得访问令牌</summary>
          <p className={cx('body-copy')}>
            本地安装完成后，使用安装工具生成的私有令牌文件。令牌过期时运行同一安装的 session
            命令重新签发。其他环境请向工作空间管理员获取访问会话。
          </p>
        </details>
        <div className={cx('actions')}>
          <button className={cx('button button--primary')} disabled={busy}>
            {busy ? '正在验证连接…' : '进入工作空间'}
          </button>
          {onCancel && (
            <button type="button" className={cx('button')} onClick={onCancel}>
              返回工作空间
            </button>
          )}
        </div>
      </form>
    </section>
  )
}

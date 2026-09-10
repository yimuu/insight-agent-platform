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

export function ConnectionPage({
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

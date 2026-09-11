import sharedStyles from '../shared/ui/Primitives.module.css'
import { classNames } from '../shared/ui/class-names.ts'
import localStyles from './Layout.module.css'
const cx = classNames(sharedStyles, localStyles)
import { Fragment, Suspense, lazy, useCallback, useEffect, useState } from 'react'
const TaskInbox = lazy(() =>
  import('../features/tasks/TaskInbox').then((module) => ({ default: module.TaskInbox })),
)
const ModelSettings = lazy(() =>
  import('../features/models/ModelSettings').then((module) => ({ default: module.ModelSettings })),
)
const Agents = lazy(() =>
  import('../features/agents/AgentsPage').then((module) => ({ default: module.Agents })),
)
const Runs = lazy(() =>
  import('../features/runs/RunsPage').then((module) => ({ default: module.Runs })),
)
const Settings = lazy(() =>
  import('../features/settings/SettingsPage').then((module) => ({ default: module.Settings })),
)
import { ConnectionPage } from '../features/connection/ConnectionPage'
import type { ConsoleSession } from '../features/connection/session'
import type { AgentSummary } from '../shared/api/types'
import { NoticeBox } from '../shared/ui/console-ui'
import { errorNotice } from '../shared/ui/feedback'
import type { Notice } from '../shared/ui/feedback'

type ViewName = 'agents' | 'runs' | 'tasks' | 'models' | 'settings'
const NAV: Array<{ id: ViewName; label: string; eyebrow: string; description: string }> = [
  { id: 'agents', label: '智能体', eyebrow: '01', description: '构建、发布和运行你的智能体' },
  { id: 'runs', label: '运行记录', eyebrow: '02', description: '查看任务进度与执行结果' },
  { id: 'tasks', label: '待办任务', eyebrow: '03', description: '处理需要你参与的审批与输入' },
  { id: 'models', label: '模型配置', eyebrow: '04', description: '管理模型来源、凭据和可用额度' },
  { id: 'settings', label: '设置', eyebrow: '05', description: '工作空间连接与高级诊断' },
]

export default function App() {
  const [session, setSession] = useState<ConsoleSession | null>(null)
  const [reconnecting, setReconnecting] = useState(false)
  const [connectionReason, setConnectionReason] = useState('')
  const [view, setView] = useState<ViewName>('agents')
  const [tenant, setTenant] = useState('我的工作空间')
  const [notice, setNotice] = useState<{ scope: string; value: Notice | null } | null>(null)
  const [selectedTask, setSelectedTask] = useState<{ scope: string; id: string } | null>(null)
  const [launchAgent, setLaunchAgent] = useState<{ scope: string; agent: AgentSummary } | null>(
    null,
  )
  const [expiring, setExpiring] = useState(false)
  const sessionScope = session?.key ?? ''
  const report = useCallback(
    (value: Notice | null) => setNotice({ scope: sessionScope, value }),
    [sessionScope],
  )
  const reportError = useCallback((error: unknown) => report(errorNotice(error)), [report])
  const reportSaved = useCallback((text: string) => report({ tone: 'success', text }), [report])
  useEffect(() => {
    if (!session) return
    const revoke = () => {
      setConnectionReason('会话已失效或到期，请更新访问令牌后重新连接。')
      session.client.dispose()
      setSession((current) => (current?.key === session.key ? null : current))
    }
    session.client.onAuthenticationRequired(revoke)
    const check = () => {
      setExpiring(session.expiresAt !== null && session.expiresAt - Date.now() < 120_000)
      if (session.expiresAt !== null && session.expiresAt <= Date.now()) revoke()
    }
    check()
    const timer = setInterval(check, 1000)
    return () => {
      clearInterval(timer)
      session.client.onAuthenticationRequired()
      session.client.dispose()
    }
  }, [session])
  if (!session || reconnecting)
    return (
      <ConnectionPage
        initialOrigin={session?.client.origin}
        reason={connectionReason}
        onCancel={session ? () => setReconnecting(false) : undefined}
        onConnect={(next) => {
          setConnectionReason('')
          setSession(next)
          setReconnecting(false)
          setView('agents')
        }}
      />
    )
  const current = NAV.find((item) => item.id === view)!
  const client = session.client
  return (
    <div className={cx('shell')}>
      <a className={cx('skip-link')} href="#console-main">
        跳转到内容
      </a>
      <aside className={cx('sidebar')}>
        <div className={cx('brand')}>
          <span className={cx('brand__mark')}>IA</span>
          <div>
            <strong>Insight</strong>
            <small>智能体工作空间</small>
          </div>
        </div>
        <nav aria-label="控制台导航">
          {NAV.map((item) => (
            <button
              type="button"
              key={item.id}
              aria-current={view === item.id ? 'page' : undefined}
              className={cx(view === item.id ? 'nav-item nav-item--active' : 'nav-item')}
              onClick={() => setView(item.id)}
            >
              <span>{item.eyebrow}</span>
              {item.label}
            </button>
          ))}
        </nav>
        <div className={cx('session-summary')}>
          <span className={cx('pulse pulse--ready')} />
          <div>
            <strong>已连接</strong>
            <small>{tenant}</small>
          </div>
        </div>
      </aside>
      <main id="console-main" tabIndex={-1}>
        <header className={cx('topbar')}>
          <div>
            <p data-ui="kicker" className={cx('kicker')}>
              工作空间 / {current.label}
            </p>
            <h1>{current.label}</h1>
            <p className={cx('body-copy')}>{current.description}</p>
          </div>
          <details className={cx('session-menu')}>
            <summary>工作空间会话</summary>
            <div className={cx('session-menu__content')}>
              <p>{tenant}</p>
              <button className={cx('button')} onClick={() => setReconnecting(true)}>
                更换连接
              </button>
              <button
                className={cx('button')}
                onClick={() => {
                  client.dispose()
                  setSession(null)
                }}
              >
                退出会话
              </button>
            </div>
          </details>
        </header>
        {expiring && (
          <div className={cx('notice notice--info')} role="status">
            会话将在两分钟内到期，请准备新令牌并通过“更换连接”更新。
          </div>
        )}
        <NoticeBox notice={notice?.scope === sessionScope ? notice.value : null} />
        <Suspense fallback={<p role="status">正在加载页面…</p>}>
          <Fragment key={sessionScope}>
            {view === 'agents' && (
              <Agents
                client={client}
                report={report}
                onRun={(agent) => {
                  setLaunchAgent({ scope: sessionScope, agent })
                  setView('runs')
                }}
              />
            )}
            {view === 'models' && (
              <ModelSettings client={client} onError={reportError} onSaved={reportSaved} />
            )}
            {view === 'runs' && (
              <Runs
                client={client}
                report={report}
                launchAgent={launchAgent?.scope === sessionScope ? launchAgent.agent : null}
                onTask={(id) => {
                  setSelectedTask({ scope: sessionScope, id })
                  setView('tasks')
                }}
              />
            )}
            {view === 'tasks' && (
              <TaskInbox
                key={selectedTask?.scope === sessionScope ? selectedTask.id : 'direct'}
                client={client}
                report={report}
                selectedId={selectedTask?.scope === sessionScope ? selectedTask.id : ''}
                subjectKey={sessionScope}
              />
            )}
            {view === 'settings' && (
              <Settings
                client={client}
                report={report}
                tenant={tenant}
                setTenant={setTenant}
                ready
                endpoint={client.origin}
              />
            )}
          </Fragment>
        </Suspense>
      </main>
    </div>
  )
}

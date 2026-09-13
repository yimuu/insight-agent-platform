import sharedStyles from '../shared/ui/Primitives.module.css'
import { classNames } from '../shared/ui/class-names.ts'
import localStyles from './Layout.module.css'
const cx = classNames(sharedStyles, localStyles)
import { Fragment, Suspense, lazy, useCallback, useEffect, useState } from 'react'
import { Icon } from '../shared/ui/Icon'
const TaskInbox = lazy(() =>
  import('../features/tasks/TaskInbox').then((module) => ({ default: module.TaskInbox })),
)
const ModelSettings = lazy(() =>
  import('../features/models/ModelSettings').then((module) => ({ default: module.ModelSettings })),
)
const Agents = lazy(() =>
  import('../features/agents/AgentsPage').then((module) => ({ default: module.Agents })),
)
const Conversations = lazy(() =>
  import('../features/conversations/ConversationsPage').then((module) => ({
    default: module.Conversations,
  })),
)
const Runs = lazy(() =>
  import('../features/runs/RunsPage').then((module) => ({ default: module.Runs })),
)
const Settings = lazy(() =>
  import('../features/settings/SettingsPage').then((module) => ({ default: module.Settings })),
)
import { ConnectionPage } from '../features/connection/ConnectionPage'
import { browserAuth } from '../features/connection/browser-session'
import type { ConsoleSession } from '../features/connection/session'
import type { AgentSummary } from '../shared/api/types'
import { NoticeBox } from '../shared/ui/console-ui'
import type { Notice } from '../shared/ui/feedback'

type ViewName = 'conversations' | 'agents' | 'runs' | 'tasks' | 'models' | 'settings'
const NAV: Array<{ id: ViewName; label: string; description: string }> = [
  { id: 'agents', label: '智能体', description: '构建、发布和运行你的智能体' },
  { id: 'conversations', label: '对话调试', description: '持续对话，查看实时回答与执行过程' },
  { id: 'runs', label: '运行记录', description: '查看任务进度与执行结果' },
  { id: 'tasks', label: '待办任务', description: '处理需要你参与的审批与输入' },
  { id: 'models', label: '模型配置', description: '连接服务商，管理可用模型' },
  { id: 'settings', label: '设置', description: '管理工作空间与连接设置' },
]

function currentView(): ViewName {
  const value = window.location.hash.slice(1)
  return NAV.some((item) => item.id === value) ? (value as ViewName) : 'agents'
}

export default function App() {
  const [session, setSession] = useState<ConsoleSession | null>(null)
  const [chatAgent, setChatAgent] = useState<{ scope: string; agent: AgentSummary } | null>(null)
  const [reconnecting, setReconnecting] = useState(false)
  const [connectionReason, setConnectionReason] = useState('')
  const [view, setView] = useState<ViewName>(currentView)
  const [visited, setVisited] = useState<Set<ViewName>>(() => new Set([currentView()]))
  const navigate = useCallback((next: ViewName) => {
    setView(next)
    setVisited((previous) => new Set([...previous, next]))
    window.history.pushState(null, '', `#${next}`)
  }, [])
  useEffect(() => {
    const follow = () => {
      const next = currentView()
      setView(next)
      setVisited((previous) => new Set([...previous, next]))
    }
    window.addEventListener('popstate', follow)
    window.addEventListener('hashchange', follow)
    return () => {
      window.removeEventListener('popstate', follow)
      window.removeEventListener('hashchange', follow)
    }
  }, [])
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
  const reportSaved = useCallback((text: string) => report({ tone: 'success', text }), [report])
  useEffect(() => {
    if (!session) return
    const revoke = () => {
      setConnectionReason(
        session.client.authentication === 'cookie'
          ? '登录已过期，请重新登录。'
          : '会话已失效或到期，请更新访问令牌后重新连接。',
      )
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
          setVisited(new Set([view]))
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
          {NAV.map((item, index) => (
            <Fragment key={item.id}>
              {(index === 0 || item.id === 'models') && (
                <span className={cx('nav-group')}>{index === 0 ? '工作空间' : '管理'}</span>
              )}
              <a
                href={`#${item.id}`}
                aria-current={view === item.id ? 'page' : undefined}
                className={cx(view === item.id ? 'nav-item nav-item--active' : 'nav-item')}
                onClick={(event) => {
                  event.preventDefault()
                  navigate(item.id)
                  report(null)
                }}
              >
                <Icon name={item.id} />
                {item.label}
              </a>
            </Fragment>
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
            <h1>{current.label}</h1>
            <p className={cx('body-copy')}>{current.description}</p>
          </div>
          <details className={cx('session-menu')}>
            <summary>{tenant}</summary>
            <div className={cx('session-menu__content')}>
              <p>{tenant}</p>
              {client.authentication === 'bearer' && (
                <button className={cx('button')} onClick={() => setReconnecting(true)}>
                  更换连接
                </button>
              )}
              <button
                className={cx('button')}
                onClick={async () => {
                  try {
                    if (client.authentication === 'cookie') await browserAuth('logout')
                    client.dispose()
                    setSession(null)
                  } catch {
                    report({ tone: 'error', text: '暂时无法退出登录，请稍后重试。' })
                  }
                }}
              >
                退出登录
              </button>
            </div>
          </details>
        </header>
        {expiring && (
          <div className={cx('notice notice--info')} role="status">
            {client.authentication === 'cookie'
              ? '登录将在两分钟后过期。请完成当前操作后重新登录。'
              : '会话将在两分钟内到期，请通过“更换连接”更新令牌。'}
          </div>
        )}
        {notice?.scope === sessionScope && notice.value && (
          <div className={cx('global-notice')}>
            <NoticeBox notice={notice.value} onDismiss={() => report(null)} />
          </div>
        )}
        <div className={cx('page-content')}>
          <Suspense fallback={<p role="status">正在加载页面…</p>}>
            <Fragment key={sessionScope}>
              {visited.has('agents') && (
                <div hidden={view !== 'agents'}>
                  <Agents
                    client={client}
                    active={view === 'agents'}
                    report={report}
                    onChat={(agent) => {
                      setChatAgent({ scope: sessionScope, agent: { ...agent } })
                      navigate('conversations')
                    }}
                    onRun={(agent) => {
                      setLaunchAgent({ scope: sessionScope, agent: { ...agent } })
                      navigate('runs')
                    }}
                  />
                </div>
              )}
              {visited.has('models') && (
                <div hidden={view !== 'models'}>
                  <ModelSettings client={client} onSaved={reportSaved} />
                </div>
              )}
              {visited.has('conversations') && (
                <div hidden={view !== 'conversations'}>
                  <Conversations
                    active={view === 'conversations'}
                    client={client}
                    launchAgent={chatAgent?.scope === sessionScope ? chatAgent.agent : null}
                  />
                </div>
              )}
              {visited.has('runs') && (
                <div hidden={view !== 'runs'}>
                  <Runs
                    client={client}
                    report={report}
                    launchAgent={launchAgent?.scope === sessionScope ? launchAgent.agent : null}
                    onTask={(id) => {
                      setSelectedTask({ scope: sessionScope, id })
                      navigate('tasks')
                    }}
                  />
                </div>
              )}
              {visited.has('tasks') && (
                <div hidden={view !== 'tasks'}>
                  <TaskInbox
                    key={selectedTask?.scope === sessionScope ? selectedTask.id : 'direct'}
                    client={client}
                    report={report}
                    selectedId={selectedTask?.scope === sessionScope ? selectedTask.id : ''}
                    subjectKey={sessionScope}
                  />
                </div>
              )}
              {visited.has('settings') && (
                <div hidden={view !== 'settings'}>
                  <Settings
                    client={client}
                    report={report}
                    tenant={tenant}
                    setTenant={setTenant}
                    ready
                    endpoint={client.origin}
                  />
                </div>
              )}
            </Fragment>
          </Suspense>
        </div>
      </main>
    </div>
  )
}

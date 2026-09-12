import { displayState } from '../i18n/display'
import sharedStyles from './Primitives.module.css'
import { classNames } from './class-names.ts'
const cx = classNames(sharedStyles)

import type { Notice } from './feedback'
import { Icon } from './Icon'

export function Status({ value }: { value: string }) {
  const tone = ['ready', 'enabled', 'succeeded', 'approved', 'responded'].includes(value)
    ? 'positive'
    : [
          'failed',
          'rejected',
          'cancelled',
          'timed_out',
          'blocked',
          'quarantined',
          'corrupt',
        ].includes(value)
      ? 'negative'
      : 'neutral'
  return (
    <span data-ui="status" data-status={value} className={cx(`status status--${tone}`)}>
      {displayState(value)}
    </span>
  )
}

export function NoticeBox({
  notice,
  onDismiss,
}: {
  notice: Notice | null
  onDismiss?: () => void
}) {
  if (!notice) return null
  return (
    <div
      data-ui={`notice notice--${notice.tone}`}
      className={cx(`notice notice--${notice.tone}`)}
      role={notice.tone === 'error' ? 'alert' : 'status'}
      aria-live="polite"
      aria-atomic="true"
    >
      <Icon
        name={notice.tone === 'success' ? 'check' : 'info'}
        style={{ flexShrink: 0, marginTop: 1 }}
      />
      <div className={cx('notice__body')}>
        <span>{notice.text}</span>
        {(notice.detail || notice.traceId) && (
          <details>
            <summary>诊断详情</summary>
            {notice.detail && <pre>{notice.detail}</pre>}
            {notice.traceId && <code>追踪 ID {notice.traceId}</code>}
          </details>
        )}
      </div>
      {onDismiss && (
        <button
          type="button"
          className={cx('icon-button')}
          aria-label="关闭提示"
          onClick={onDismiss}
        >
          <Icon name="close" size={16} />
        </button>
      )}
    </div>
  )
}

export function Metric({
  label,
  value,
  mono = false,
}: {
  label: string
  value: string | number | null | undefined
  mono?: boolean
}) {
  return (
    <div data-ui="metric" className={cx('metric')}>
      <dt>{label}</dt>
      <dd className={cx(mono ? 'mono' : '')}>{value ?? '—'}</dd>
    </div>
  )
}

export function SearchForm({
  label,
  placeholder,
  value,
  onChange,
  onSubmit,
  busy,
}: {
  label: string
  placeholder: string
  value: string
  onChange: (value: string) => void
  onSubmit: () => void
  busy: boolean
}) {
  return (
    <form
      data-ui="search"
      className={cx('search')}
      onSubmit={(event) => {
        event.preventDefault()
        onSubmit()
      }}
    >
      <label>
        <span>{label}</span>
        <input
          value={value}
          onChange={(event) => onChange(event.target.value)}
          placeholder={placeholder}
          required
          autoComplete="off"
        />
      </label>
      <button className={cx('button button--primary')} disabled={busy}>
        {busy ? '加载中…' : '打开'}
      </button>
    </form>
  )
}

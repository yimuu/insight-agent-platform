import { displayState } from '../i18n/display'
import sharedStyles from './Primitives.module.css'
import { classNames } from './class-names.ts'
const cx = classNames(sharedStyles)

import type { Notice } from './feedback'

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

export function NoticeBox({ notice }: { notice: Notice | null }) {
  if (!notice) return <div role="status" aria-live="polite" aria-atomic="true" />
  return (
    <div
      data-ui={`notice notice--${notice.tone}`}
      className={cx(`notice notice--${notice.tone}`)}
      role={notice.tone === 'error' ? 'alert' : 'status'}
      aria-live="polite"
      aria-atomic="true"
    >
      <span>{notice.text}</span>
      {notice.detail && (
        <details>
          <summary>诊断详情</summary>
          <pre>{notice.detail}</pre>
        </details>
      )}
      {notice.traceId && <code>追踪 ID {notice.traceId}</code>}
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

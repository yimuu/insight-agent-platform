import type { CSSProperties } from 'react'

const paths = {
  conversations: 'M4 4h16v12H9l-5 4V4Z M8 8h8M8 12h5',
  agents:
    'M9 3h6M12 3v3M5 8h14a1 1 0 0 1 1 1v10H4V9a1 1 0 0 1 1-1ZM8 12v2m8-2v2m-8 3h8M1 11v5m22-5v5',
  runs: 'M4 4v6h6M4 10a8 8 0 1 1 0 5m8-8v5l3 2',
  tasks: 'M9 5h11M9 12h11M9 19h11M2 5l1 1 3-3M2 12l1 1 3-3M2 19l1 1 3-3',
  models: 'M9 3h6l3 3v12l-3 3H9l-3-3V6l3-3ZM9 8h6v8H9V8M1 9h5m-5 6h5m12-6h5m-5 6h5',
  settings: 'M4 7h16M4 17h16M8 4v6m8 4v6',
  plus: 'M12 5v14M5 12h14',
  search: 'M21 21l-5-5M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0',
  close: 'M6 6l12 12M6 18 18 6',
  check: 'M5 12l4 4L19 6',
  info: 'M12 16v-4m0-4v.01M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0',
  arrow: 'M19 12H5m6-6-6 6 6 6',
  chevron: 'm9 5 7 7-7 7',
} as const

export function Icon({
  name,
  size = 18,
  style,
}: {
  name: keyof typeof paths
  size?: number
  style?: CSSProperties
}) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.65"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      style={style}
    >
      <path d={paths[name]} />
    </svg>
  )
}

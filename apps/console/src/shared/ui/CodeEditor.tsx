import { useEffect, useRef, useState } from 'react'
import type { EditorView } from 'codemirror'
import type { Compartment } from '@codemirror/state'
import { parseDocument } from 'yaml'
import styles from './CodeEditor.module.css'

export function CodeEditor({
  value,
  onChange,
  language = 'yaml',
  label,
  disabled = false,
  maximumBytes = 1_048_576,
}: {
  value: string
  onChange(value: string): void
  language?: 'yaml' | 'json'
  label: string
  disabled?: boolean
  maximumBytes?: number
}) {
  const host = useRef<HTMLDivElement>(null)
  const view = useRef<EditorView | null>(null)
  const change = useRef(onChange)
  const source = useRef(value)
  const synchronizing = useRef(false)
  const editable = useRef<Compartment | null>(null)
  const locked = useRef(disabled)
  const [failure, setFailure] = useState('')
  const [diagnostic, setDiagnostic] = useState('')
  useEffect(() => {
    change.current = onChange
    source.current = value
  }, [onChange, value])
  useEffect(() => {
    let cancelled = false
    void (async () => {
      try {
        const [{ EditorView, basicSetup }, { Compartment, EditorState }, extension] =
          await Promise.all([
            import('codemirror'),
            import('@codemirror/state'),
            language === 'yaml'
              ? import('@codemirror/lang-yaml').then((module) => module.yaml())
              : import('@codemirror/lang-json').then((module) => module.json()),
          ])
        if (cancelled || !host.current) return
        const compartment = new Compartment()
        editable.current = compartment
        const editor = new EditorView({
          parent: host.current,
          doc: source.current,
          extensions: [
            basicSetup,
            extension,
            EditorView.lineWrapping,
            compartment.of([
              EditorView.editable.of(!locked.current),
              EditorView.contentAttributes.of({ 'aria-readonly': String(locked.current) }),
            ]),
            EditorState.phrases.of({
              Find: '查找',
              Replace: '替换',
              next: '下一个',
              previous: '上一个',
              all: '全部',
              'match case': '区分大小写',
              'by word': '全字匹配',
              regexp: '正则表达式',
              replace: '替换',
              'replace all': '全部替换',
              close: '关闭',
              'Go to line': '跳转到行',
            }),
            EditorView.contentAttributes.of({
              'aria-label': label,
              'aria-multiline': 'true',
            }),
            EditorView.updateListener.of((update) => {
              if (update.docChanged && !synchronizing.current)
                change.current(update.state.doc.toString())
            }),
          ],
        })
        view.current = editor
      } catch {
        if (!cancelled) setFailure('代码编辑器加载失败，请刷新页面重试。')
      }
    })()
    return () => {
      cancelled = true
      view.current?.destroy()
      view.current = null
    }
  }, [language, label])
  useEffect(() => {
    locked.current = disabled
    if (view.current && editable.current) {
      void import('codemirror').then(({ EditorView }) => {
        if (view.current && editable.current)
          view.current.dispatch({
            effects: editable.current.reconfigure([
              EditorView.editable.of(!locked.current),
              EditorView.contentAttributes.of({ 'aria-readonly': String(locked.current) }),
            ]),
          })
      })
    }
  }, [disabled])
  useEffect(() => {
    const editor = view.current
    if (editor && editor.state.doc.toString() !== value) {
      synchronizing.current = true
      try {
        editor.dispatch({ changes: { from: 0, to: editor.state.doc.length, insert: value } })
      } finally {
        synchronizing.current = false
      }
    }
    const timer = setTimeout(() => {
      if (new TextEncoder().encode(value).byteLength > maximumBytes) {
        setDiagnostic('源码超过编辑器字节限制，请减少内容后再校验。')
        return
      }
      try {
        if (language === 'json') JSON.parse(value)
        const document = parseDocument(value, { uniqueKeys: true })
        const error = document.errors[0]
        setDiagnostic(
          error
            ? `语法错误：第 ${error.linePos?.[0].line ?? '?'} 行，第 ${error.linePos?.[0].col ?? '?'} 列。${error.code}`
            : '',
        )
      } catch {
        setDiagnostic('JSON 语法不完整，请检查引号、逗号和括号。')
      }
    }, 200)
    return () => clearTimeout(timer)
  }, [value, language, maximumBytes])
  return (
    <div className={styles.editor} data-code-editor={label}>
      <div className={styles.heading}>
        <span>{label}</span>
        <small>{language.toUpperCase()} · Ctrl / ⌘ F 搜索</small>
      </div>
      <div ref={host} />
      {(failure || diagnostic) && (
        <p role="alert" className={styles.error}>
          {failure || diagnostic}
        </p>
      )}
    </div>
  )
}

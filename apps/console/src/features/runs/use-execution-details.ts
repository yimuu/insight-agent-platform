import { useEffect, useRef, useState } from 'react'
import { PlatformProblem } from '../../shared/api/client'
import type { PlatformClient } from '../../shared/api/client'
import type { ExecutionDetail } from '../../shared/api/types'
import type { ExecutionNode } from './execution-graph'
import { nextDetailBatch } from './execution-detail-queue'

/** Four requests at a time. New events update the queue instead of cancelling it. */
export function useExecutionDetails(
  client: PlatformClient | null | undefined,
  runId: string | undefined,
  nodes: ExecutionNode[],
  selected: string,
  terminalEpoch: string,
) {
  const [details, setDetails] = useState<Record<string, ExecutionDetail>>({})
  const [notice, setNotice] = useState('')
  const [retry, setRetry] = useState(0)
  const targets = useRef<
    {
      id: string
      kind: 'node_execution' | 'model_turn'
      sourceId: string
      stamp: string
      priority: boolean
    }[]
  >([])
  const supported = nodes.filter(
    (node) => node.kind === 'node_execution' || node.kind === 'model_turn',
  )
  const chosen = supported.find((node) => node.id === selected)
  targets.current = [
    ...(chosen ? [chosen] : []),
    ...supported.filter((node) => node.id !== selected).slice(0, 128),
  ].map((node) => ({
    id: node.id,
    kind: node.kind as 'node_execution' | 'model_turn',
    sourceId: node.sourceId,
    priority: node.id === selected,
    stamp: `${String((node.latest.data.data as Record<string, unknown>).source_projection_version)}:${terminalEpoch}:${retry}`,
  }))
  useEffect(() => {
    setDetails({})
    setNotice('')
    if (!client || !runId) return
    const c = new AbortController()
    const seen = new Map<string, string>()
    let cursor = 0
    let timer: ReturnType<typeof setTimeout> | undefined
    const pump = async () => {
      const permitted = new Set(targets.current.map((target) => target.id))
      for (const key of seen.keys()) if (!permitted.has(key)) seen.delete(key)
      const next = nextDetailBatch(targets.current, seen, cursor)
      const batch = next.batch
      cursor = next.cursor
      if (batch.length) {
        const results = await Promise.allSettled(
          batch.map(async (target) => {
            let data: ExecutionDetail
            try {
              data = (
                await client.getExecutionDetail(runId, target.kind, target.sourceId, c.signal)
              ).data
            } catch (error) {
              if (
                !c.signal.aborted &&
                error instanceof PlatformProblem &&
                [401, 403].includes(error.status)
              ) {
                setDetails({})
                setNotice('无权继续读取节点详情，请重新连接工作空间。')
                c.abort()
              }
              throw error
            }
            if (
              data.run_id !== runId ||
              data.source_id !== target.sourceId ||
              data.source_kind !== target.kind
            )
              throw new Error('节点详情身份不一致。')
            return data
          }),
        )
        if (c.signal.aborted) return
        const changes: Record<string, ExecutionDetail> = {}
        results.forEach((result, index) => {
          const target = batch[index]!
          seen.set(target.id, target.stamp)
          if (result.status === 'fulfilled') changes[target.id] = result.value
          else setNotice('部分节点详情暂时无法读取，可重试；其它节点仍可查看。')
        })
        setDetails((previous) => ({
          ...Object.fromEntries(Object.entries(previous).filter(([id]) => permitted.has(id))),
          ...changes,
        }))
      }
      if (!c.signal.aborted) timer = setTimeout(() => void pump(), batch.length ? 0 : 1000)
    }
    void pump()
    return () => {
      c.abort()
      clearTimeout(timer)
    }
  }, [client, runId, retry])
  return {
    details,
    notice,
    retry: () => {
      setNotice('')
      setRetry((n) => n + 1)
    },
  }
}

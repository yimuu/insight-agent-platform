interface Target {
  id: string
  stamp: string
  priority: boolean
}
export function nextDetailBatch<T extends Target>(
  targets: T[],
  seen: ReadonlyMap<string, string>,
  cursor: number,
): { batch: T[]; cursor: number } {
  if (!targets.length) return { batch: [], cursor: 0 }
  const dirty = (target: T) => seen.get(target.id) !== target.stamp
  const priority = targets.find((target) => target.priority && dirty(target))
  const start = cursor % targets.length
  const ordered = [...targets.slice(start), ...targets.slice(0, start)]
  const batch = [
    ...(priority ? [priority] : []),
    ...ordered.filter((target) => target !== priority && dirty(target)),
  ].slice(0, 4)
  return {
    batch,
    cursor: batch.length ? (targets.indexOf(batch.at(-1)!) + 1) % targets.length : start,
  }
}

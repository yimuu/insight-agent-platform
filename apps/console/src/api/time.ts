/** Serialize a browser clock instant using the public UtcTimestamp wire contract. */
export function utcTimestamp(value: Date): string {
  const milliseconds = value.toISOString()
  if (!/^\d{4}-/.test(milliseconds)) throw new RangeError('Timestamp is outside the public calendar range.')
  return `${milliseconds.slice(0, -1)}000Z`
}

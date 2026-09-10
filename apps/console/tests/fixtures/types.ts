import assert from 'node:assert/strict'
import type { AddressInfo } from 'node:net'

export function tcpPort(server: { address(): AddressInfo | string | null }): number {
  const address = server.address()
  assert.ok(address && typeof address !== 'string', 'Fixture must bind a TCP port')
  return address.port
}
export function errorValue(value: unknown): Error & { reason?: string; code?: string } {
  assert.ok(value instanceof Error)
  return value
}
export interface CdpResult {
  data?: string
  exceptionDetails?: { exception?: { description: string }; text: string }
  result?: { value: unknown }
  root?: { nodeId: number }
  nodeId?: number
}

export function objectValue(value: unknown): Record<string, unknown> {
  assert.ok(value !== null && typeof value === 'object' && !Array.isArray(value))
  return value as Record<string, unknown>
}

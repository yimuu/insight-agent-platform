import type { ExactDeploymentRef } from './types.ts'
export interface Conversation {
  schema_version: 1
  conversation_id: string
  agent_id: string
  agent_deployment: ExactDeploymentRef
  input_field: string
  input_schema_digest: string
  title: string
  created_by: string
  version: number
  turn_count: number
  created_at: string
  updated_at: string
}
export interface ConversationTurn {
  schema_version: 1
  conversation_id: string
  ordinal: number
  created_at: string
  run_id: string
  history_through: number
  conversation_version: number
}
export interface ConversationPage<T> {
  schema_version: 1
  items: T[]
  next_cursor: string | null
}

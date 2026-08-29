export type JsonObject = Record<string, unknown>

export interface AuthorityResponse<T> {
  data: T
  etag: string | null
  traceId: string | null
}

export interface ApiProblemShape {
  code?: string
  detail?: string
  message?: string
  request_id?: string
  retryable?: boolean
  status?: number
  trace_id?: string
  title?: string
}

export interface RunView {
  schema_version: 1
  run_id: string
  agent_deployment_id: string
  state: string
  version: number
  input_value_id: string
  output_value_id: string | null
  pause_generation: number
  cancel_generation: number
  deadline: string
  started_at: string | null
  terminal_at: string | null
  created_at: string
  updated_at: string
  etag: string
}

export interface TaskView {
  schema_version: 1
  task_id: string
  task_kind: string
  state: string
  generation: number
  version: number
  safe_prompt_key: string
  response_schema_digest: string | null
  owner: JsonObject
  deadline: string
  responded_at: string | null
  created_at: string
  updated_at: string
  etag: string
}

export interface ArtifactView {
  schema_version: 1
  artifact_id: string
  purpose: string
  classification: string
  state: string
  version: number
  expected_size_bytes: number
  declared_media_type: string | null
  verified_media_type: string | null
  content: JsonObject | null
  retain_until: string
  created_at: string
  updated_at: string
  etag: string
}

export interface OperationView {
  operation_id: string
  tenant_id: string
  kind: string
  target: JsonObject
  state: string
  progress: { completed_units: number; total_units: number } | null
  result: { result_digest: string } | null
  error: { code: string; message: string } | null
  created_at: string
  updated_at: string
  etag: string
}

export interface ResourceView {
  schema_version: 1
  resource_id: string
  resource_kind: string
  lifecycle_state: string
  gate_state: string
  draft_generation: number
  version: number
  draft: { display_name?: string; validation?: JsonObject | null }
  etag: string
}

export interface DeploymentView {
  schema_version: 1
  deployment_id: string
  resource_id: string
  resource_kind: string
  resource_version_id: string
  environment: string
  closure_digest: string
  created_at: string
  etag: string
}

export interface RunEvent {
  id: string
  event: string
  data: JsonObject
}

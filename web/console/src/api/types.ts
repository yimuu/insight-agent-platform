export type JsonObject = Record<string, unknown>

export interface AuthorityResponse<T> {
  data: T
  etag: string | null
  traceId: string | null
}

export interface ListPage<T> {
  schema_version: 1
  items: T[]
  next_cursor: string | null
}

export interface AgentSummary {
  schema_version: 1
  name: string
  display_name: string
  agent_id: string
  state: 'draft' | 'validating' | 'publishing' | 'ready' | 'blocked'
  environment: string | null
  updated_at: string
  published_at: string | null
  required_features: Array<'model'>
  latest_run_state: string | null
}

export interface RunSummary {
  schema_version: 1
  run_id: string
  agent_name: string
  agent_id: string
  state: string
  started_at: string | null
  terminal_at: string | null
  waiting_task_count: number
  result_available: boolean
}

export interface ExactVersionRef {
  revision_id: string
  resource_kind: 'policy_revision' | 'agent_interface_revision' | 'agent_plan_revision'
  semantic_digest: string
}

export interface ExactDeploymentRef {
  deployment_id: string
  resource_kind: 'policy_deployment' | 'model_deployment'
  deployment_digest: string
}

export interface ExactPolicyBinding {
  deployment: ExactDeploymentRef & { resource_kind: 'policy_deployment' }
  revision: ExactVersionRef & { resource_kind: 'policy_revision' }
}

export interface AgentAuthoringProfile {
  schema_version: 1
  default_deadline_seconds: number
  default_environment: string
  policy_versions: Array<ExactVersionRef & { resource_kind: 'policy_revision' }>
  deployment_policies: ExactPolicyBinding[]
  execution_profile: ExactPolicyBinding
  model_loop: {
    maximum_rounds: number
    maximum_capability_calls: number
    maximum_parallel_calls_per_round: number
    token_budget: number
  }
  models: Array<{
    alias: string
    deployment: ExactDeploymentRef & { resource_kind: 'model_deployment' }
    selection_policy: ExactPolicyBinding
  }>
  profile_digest: string
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
  content: ArtifactRef | null
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
  draft: { display_name: string; document: JsonObject; validation: JsonObject | null }
  etag: string
}

export interface PublishedVersionSummary {
  resource_version_id: string
  revision_no: number
  content_digest: string
  artifact_id: string | null
  etag: string
}

export interface PublishResourceResponse {
  schema_version: 1
  resource_id: string
  resource_kind: 'agent'
  draft_generation: number
  version: number
  published_versions: PublishedVersionSummary[]
  etag: string
}

export interface ArtifactRef {
  artifact_id: string
  content_digest: string
  byte_length: number
  media_type: string
  classification: string
  display_name: string | null
}

export interface PrepareArtifactUploadResponse {
  schema_version: 1
  artifact_id: string
  operation_id: string
  upload_grant_id: string
  artifact_etag: string
  upload_target: { url: string; completion_proof: string }
  upload_expires_at: string
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

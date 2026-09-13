import type { ArtifactRef, ExactDeploymentRef, Json, JsonObject, ResourceView } from './types.ts'

export type ModelResourceKind = 'model_provider' | 'model_profile'
export type ModelResourceNoun = 'model-providers' | 'models'
export type ModelProtocol = 'open_ai_responses' | 'anthropic_messages'
export interface ModelEndpoint {
  scheme: 'https'
  host: string
  port: number
  base_path: string
}
export interface ModelDestinationChoice {
  base_url: string
  protocol: ModelProtocol
  region: string
}
export interface ModelConfigurationCatalog {
  schema_version: 2
  installation_digest: string
  environment: string
  secret_provider_id: string
  protocols: ModelProtocol[]
  maximum_classification: 'internal'
}
export interface ModelResourceSummary {
  resource_id: string
  resource_kind: ModelResourceKind
  alias: string | null
  display_name: string
  version: number
  etag: string
  lifecycle_state: string
  gate_state: string
  active_deployment: ExactDeploymentRef | null
}
export interface ModelResourcePage {
  schema_version: 1
  items: ModelResourceSummary[]
  next_after: string | null
}
export interface ModelDefault {
  schema_version: 1
  tenant_id: string
  default_model: ExactDeploymentRef | null
  version: number
  etag: string
}
export interface ExactModelCredential {
  secret_binding_id: string
  binding_generation: number
  provider_id: string
  purpose: string
  resolution_policy: JsonObject
  resolution_policy_digest: string
}
export type ModelConfigurationInput =
  | {
      kind: 'source'
      configuration: {
        schema_version: 2
        alias: string
        display_name: string
        endpoint: ModelEndpoint
        protocol: ModelProtocol
        region: string
        credential: ExactModelCredential
      }
    }
  | {
      kind: 'model'
      configuration: {
        schema_version: 1
        alias: string
        display_name: string
        source: ExactDeploymentRef
        model: string
        maximum_input_tokens: number
        maximum_output_tokens: number
        declared_at: string
      }
    }
export interface ModelDeclaration {
  schema_version: 1
  content: Json
  content_digest: string
  size_bytes: number
}
export interface CompiledModelConfiguration {
  schema_version: 1
  draft: ResourceView['draft'] & { alias: string }
  environment: string
  deployment: { resource_kind: ModelResourceKind; bindings: JsonObject }
  declaration: ModelDeclaration
}
export interface ModelPublicationRequest {
  tenant_id: string
  input: ModelConfigurationInput
  quota: ModelQuotaLimits | null
  installation_digest: string
  existing: Pick<ResourceView, 'resource_id' | 'etag' | 'version' | 'draft_generation'> | null
}
export interface ModelPublicationResult {
  resource: ResourceView
  deployment: ExactDeploymentRef
  artifact: ArtifactRef
}
export interface ModelCredentialMetadata {
  schema_version: 1
  tenant_id: string
  secret_binding_id: string
  provider_id: string
  purpose: 'model_api_key'
  state: 'active' | 'revoked'
  generation: number
  version: number
  etag: string
}
export type ModelConnectionOutcome =
  | 'response_received'
  | 'credentials_rejected'
  | 'model_unavailable'
  | 'rate_limited'
  | 'provider_unavailable'
  | 'invalid_response'
  | 'timed_out'
  | 'transport_unavailable'
export interface ModelConnectionObservation {
  schema_version: 1
  model_deployment: ExactDeploymentRef
  provider_deployment: ExactDeploymentRef
  model_identity: { stability: 'pinned' | 'externally_mutable'; value: string }
  protocol: ModelProtocol
  observed_at: string
  outcome: ModelConnectionOutcome
}

export interface ModelQuotaLimits {
  requests: number
  tokens: number
  cost_microunits: number
}
export interface ModelQuotaView {
  schema_version: 1
  tenant_id: string
  model_deployment: ExactDeploymentRef
  allocation: {
    limits: ModelQuotaLimits
    reserved: ModelQuotaLimits
    used: ModelQuotaLimits
  } | null
  tenant_concurrency: { limit: number; reserved: number; used: number }
  etag: string
}

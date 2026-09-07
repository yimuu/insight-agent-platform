import type { PlatformClient } from '../api/client.ts'
import type { AgentSummary, JsonObject, RunView } from '../api/types.ts'
import { sameJson } from '../api/artifact-content.ts'
import { restorePublishedSources } from './restore.ts'

const object = (value: unknown): value is JsonObject => value !== null && typeof value === 'object' && !Array.isArray(value)
const digest = (value: unknown): value is string => typeof value === 'string' && /^sha256:[0-9a-f]{64}$/.test(value)

/** Defaults come from the selected immutable deployment, never from its current editable draft. */
export async function publishedRunDefaults(client: PlatformClient, agent: AgentSummary, signal?: AbortSignal) {
  const exact = agent.active_deployment
  if (!exact || exact.resource_kind !== 'agent_deployment' || !digest(exact.deployment_digest)) throw new Error('Refresh Agents and select an Agent with a published active deployment.')
  const response = await client.getDeployment('agents', agent.agent_id, exact.deployment_id, { signal })
  const deployment = response.data
  if (deployment.schema_version !== 1 || deployment.resource_id !== agent.agent_id || deployment.resource_kind !== 'agent'
    || deployment.deployment_id !== exact.deployment_id || deployment.closure_digest !== exact.deployment_digest
    || deployment.etag !== response.etag || deployment.etag !== `"${exact.deployment_id}-${exact.deployment_digest.slice(7)}"`
    || !object(deployment.closure) || deployment.closure.resource_kind !== 'agent' || !object(deployment.closure.bindings)) throw new Error('The exact Agent deployment differs from the selected server summary. Refresh Agents.')
  const plan = deployment.closure.bindings.plan
  if (!object(plan) || plan.resource_kind !== 'agent_plan_revision' || typeof plan.revision_id !== 'string' || !digest(plan.semantic_digest)) throw new Error('The selected deployment has no exact Plan reference.')
  const published = await client.getResourceVersion('agents', agent.agent_id, plan.revision_id, { signal })
  const version = published.data
  if (version.schema_version !== 1 || version.resource_id !== agent.agent_id || version.resource_version_id !== plan.revision_id
    || version.content_digest !== plan.semantic_digest || version.etag !== published.etag || version.etag !== `"${plan.revision_id}-${plan.semantic_digest.slice(7)}"`
    || !object(version.payload.document) || version.payload.document.resource_kind !== 'agent' || !object(version.payload.document.spec)) throw new Error('Published Run defaults differ from the selected Plan.')
  const spec = version.payload.document.spec
  if (!object(spec.input_schema) || !digest(spec.input_schema.canonical_digest) || !Number.isInteger(spec.default_deadline_seconds)
    || Number(spec.default_deadline_seconds) < 1 || Number(spec.default_deadline_seconds) > 3600
    || !['public', 'internal', 'confidential', 'restricted'].includes(String(spec.input_classification))
    || spec.typed_plan_digest !== plan.semantic_digest) throw new Error('Published Run input defaults are invalid.')
  signal?.throwIfAborted()
  return { schemaDigest: spec.input_schema.canonical_digest, classification: String(spec.input_classification), deadlineSeconds: Number(spec.default_deadline_seconds), deploymentId: exact.deployment_id, exactDeployment: exact }
}

/** Runtime metadata grants no source access: the exact source path performs its own current reads. */
export async function frozenRunSources(client: PlatformClient, run: RunView, signal?: AbortSignal) {
  const definition = (await client.getRunDefinition(run.run_id, { signal })).data
  if (definition.schema_version !== 1 || definition.run_id !== run.run_id || definition.agent_deployment?.deployment_id !== run.agent_deployment_id
    || definition.agent_deployment.resource_kind !== 'agent_deployment' || definition.plan?.resource_kind !== 'agent_plan_revision'
    || definition.agent_interface?.resource_kind !== 'agent_interface_revision') throw new Error('Run definition differs from the selected Run.')
  const restored = await restorePublishedSources(client, definition.agent_id, definition.plan.revision_id, signal)
  const deploymentResponse = await client.getDeployment('agents', definition.agent_id, run.agent_deployment_id, { signal })
  const deployment = deploymentResponse.data
  if (deployment.schema_version !== 1 || deployment.resource_kind !== 'agent' || deployment.deployment_id !== run.agent_deployment_id
    || deployment.closure_digest !== definition.agent_deployment.deployment_digest || deployment.resource_id !== definition.agent_id
    || deployment.etag !== deploymentResponse.etag || deployment.etag !== `"${run.agent_deployment_id}-${definition.agent_deployment.deployment_digest.slice(7)}"`
    || !object(deployment.closure) || deployment.closure.resource_kind !== 'agent' || !object(deployment.closure.bindings)
    || !sameJson(deployment.closure.bindings.plan, definition.plan) || !sameJson(deployment.closure.bindings.interface, definition.agent_interface)
    || restored.compiled.typedPlanDigest !== definition.plan.semantic_digest || restored.compiled.contractDigest !== definition.agent_interface.semantic_digest) throw new Error('Frozen source does not describe this Run’s exact deployment.')
  signal?.throwIfAborted()
  return restored.compiled
}

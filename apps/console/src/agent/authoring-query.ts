import { parseDocument } from 'yaml'
import type { Json, ListPage, ExactDeploymentRef } from '../api/types.ts'

export type DependencyKind = 'model' | 'capability' | 'context' | 'child_agent' | 'skill'
export interface DependencyFilters { kind: DependencyKind; environment?: string; interfaceContractDigest?: string; cursor?: string }
export interface AuthoringDependency {
  schema_version: 1; kind: DependencyKind; resource_id: string; environment: string
  deployment: { deployment_id: string; resource_kind: string; deployment_digest: string }
  interface_contract_digest: string; contract_match: boolean | null; call_authorized: boolean
}
export interface SlotSelection { slot_id: string; requirement_digest: string; interface_contract_digest: string | null; target: { kind: DependencyKind; [key: string]: Json } }
export interface BindingSelections { schema_version: 1; slots: SlotSelection[] }
export interface DeploymentFeatures { schema_version: 1; deployment: ExactDeploymentRef; interface_contract_digest: string; required_features: string[] }
export interface BindingResolution {
  schema_version: 1
  slots: Array<{ slot_id: string; resolution:
    | { kind: 'rejected'; code: string }
    | { kind: 'resolved'; deployment_features: DeploymentFeatures[]; binding: { slot_id: string; requirement_digest: string; target: { kind: DependencyKind; [key: string]: Json } }; observed_contract_digests: string[]; contract_match: boolean | null; call_authorized: boolean }
  }>
}
function invalid(): never { throw new Error('authoring_query_invalid: The authoring query contains an unsupported shape or mismatched response.') }
function object(value: unknown): Record<string, unknown> { if (!value || typeof value !== 'object' || Array.isArray(value)) return invalid(); return value as Record<string, unknown> }
function closed(value: unknown, keys: string[]): Record<string, unknown> { const row = object(value); if (Object.keys(row).some((key) => !keys.includes(key)) || keys.some((key) => !(key in row))) invalid(); return row }
function string(value: unknown, max = 4096): asserts value is string { if (typeof value !== 'string' || value.length === 0 || new TextEncoder().encode(value).length > max) invalid() }
function digest(value: unknown): asserts value is string { if (typeof value !== 'string' || !/^sha256:[a-f0-9]{64}$/.test(value)) invalid() }
function array(value: unknown, max: number): unknown[] { if (!Array.isArray(value) || value.length > max) return invalid(); return value }
function kind(value: unknown): asserts value is DependencyKind { if (!['model', 'capability', 'context', 'child_agent', 'skill'].includes(String(value))) invalid() }
function match(value: unknown): void { if (value !== null && typeof value !== 'boolean') invalid() }
function exactDeployment(value: unknown): void { const row = closed(value, ['deployment_id', 'resource_kind', 'deployment_digest']); string(row.deployment_id, 64); string(row.resource_kind, 64); digest(row.deployment_digest) }
function exactVersion(value: unknown): void { const row = closed(value, ['revision_id', 'resource_kind', 'semantic_digest']); string(row.revision_id, 64); if (row.resource_kind !== 'policy_revision') invalid(); digest(row.semantic_digest) }
function policy(value: unknown): void { const row = closed(value, ['deployment', 'revision']); exactDeployment(row.deployment); exactVersion(row.revision) }
function selector(value: unknown): void { const row = object(value); if (row.kind === 'exact') { closed(row, ['kind', 'deployment']); exactDeployment(row.deployment) } else if (row.kind === 'active') { closed(row, ['kind', 'resource_id', 'environment']); string(row.resource_id, 64); string(row.environment, 64) } else invalid() }
function consistency(value: unknown): void {
  const row = object(value)
  if (row.mode === 'external_observation') { closed(row, ['mode']); return }
  if (row.mode === 'pin_at_run_admission' || row.mode === 'latest_at_query_start') { closed(row, ['mode', 'dataset_id']); string(row.dataset_id, 64); return }
  if (row.mode !== 'pinned_generation') invalid()
  const generation = closed(closed(row, ['mode', 'generation']).generation, ['dataset_id', 'generation_id', 'generation_digest'])
  string(generation.dataset_id, 64); string(generation.generation_id, 64); digest(generation.generation_digest)
}
function target(value: unknown, selected: boolean): void {
  const row = object(value); kind(row.kind)
  if (row.kind === 'context') {
    const fields = ['deployment', 'consistency', 'allowed_projection', 'authorization_policy', 'ranking_policy']
    const context = selected ? closed(row, ['kind', ...fields]) : closed(closed(row, ['kind', 'binding']).binding, ['context_deployment', ...fields.slice(1)])
    if (selected) selector(context.deployment); else exactDeployment(context.context_deployment)
    // The owning Rust validator checks consistency semantics and policy relationships.
    consistency(context.consistency); array(context.allowed_projection, 128).forEach((field) => string(field, 128)); exactVersion(context.authorization_policy); exactVersion(context.ranking_policy)
  } else {
    closed(row, ['kind', 'candidates', 'selection_policy', ...(row.kind === 'capability' ? ['tool_alias'] : [])])
    const candidates = array(row.candidates, 16); if (!candidates.length) invalid()
    candidates.forEach(selected ? selector : exactDeployment); policy(row.selection_policy)
    if (row.kind === 'capability' && row.tool_alias !== null) string(row.tool_alias, 128)
  }
}
export function strictAuthoringJson(text: string, max = 262_144): unknown {
  if (new TextEncoder().encode(text).length > max) throw new Error('authoring_query_too_large: Authoring query exceeded its byte limit.')
  let value: unknown
  try { value = JSON.parse(text) } catch { throw new Error('Authoring selections must contain complete JSON.') }
  const parsed = parseDocument(text, { uniqueKeys: true, strict: true })
  if (parsed.errors.length) throw new Error('Authoring JSON contains duplicate keys or invalid structure.')
  return value
}
export function parseBindingSelections(text: string): BindingSelections {
  const row = closed(strictAuthoringJson(text), ['schema_version', 'slots'])
  if (row.schema_version !== 1) invalid()
  const slots = array(row.slots, 64); if (!slots.length) invalid()
  const seen = new Set<string>()
  for (const slot of slots) {
    const entry = closed(slot, ['slot_id', 'requirement_digest', 'interface_contract_digest', 'target'])
    string(entry.slot_id, 128); if (seen.has(entry.slot_id)) invalid(); seen.add(entry.slot_id)
    digest(entry.requirement_digest); if (entry.interface_contract_digest !== null) digest(entry.interface_contract_digest); target(entry.target, true)
  }
  return row as unknown as BindingSelections
}
export function parseDependencyPage(text: string, filters: DependencyFilters): ListPage<AuthoringDependency> {
  const page = closed(strictAuthoringJson(text, 1_048_576), ['schema_version', 'items', 'next_cursor'])
  if (page.schema_version !== 1) invalid(); if (page.next_cursor !== null) string(page.next_cursor, 4096)
  for (const item of array(page.items, 25)) {
    const row = closed(item, ['schema_version', 'kind', 'resource_id', 'environment', 'deployment', 'interface_contract_digest', 'contract_match', 'call_authorized'])
    if (row.schema_version !== 1 || row.kind !== filters.kind || typeof row.call_authorized !== 'boolean') invalid()
    string(row.resource_id, 64); string(row.environment, 64); exactDeployment(row.deployment); digest(row.interface_contract_digest); match(row.contract_match)
    if (filters.environment && row.environment !== filters.environment) invalid()
    if (row.contract_match !== (filters.interfaceContractDigest ? filters.interfaceContractDigest === row.interface_contract_digest : null)) invalid()
  }
  return page as unknown as ListPage<AuthoringDependency>
}
export function parseBindingResolution(text: string, request: BindingSelections): BindingResolution {
  const response = closed(strictAuthoringJson(text, 1_048_576), ['schema_version', 'slots'])
  if (response.schema_version !== 1) invalid()
  const slots = array(response.slots, 64); if (slots.length !== request.slots.length) invalid()
  slots.forEach((item, index) => {
    const row = closed(item, ['slot_id', 'resolution']); const expected = request.slots[index]
    if (row.slot_id !== expected.slot_id) invalid()
    const result = object(row.resolution)
    if (result.kind === 'rejected') { closed(result, ['kind', 'code']); if (!['invalid', 'denied', 'not_found', 'disabled', 'contract_mismatch', 'unavailable'].includes(String(result.code))) invalid(); return }
    closed(result, ['kind', 'binding', 'observed_contract_digests', 'contract_match', 'call_authorized', 'deployment_features'])
    if (result.kind !== 'resolved' || typeof result.call_authorized !== 'boolean') invalid()
    const binding = closed(result.binding, ['slot_id', 'requirement_digest', 'target'])
    if (binding.slot_id !== expected.slot_id || binding.requirement_digest !== expected.requirement_digest || object(binding.target).kind !== expected.target.kind) invalid()
    target(binding.target, false)
    const observed = array(result.observed_contract_digests, 16); observed.forEach(digest)
    const count = expected.target.kind === 'context' ? 1 : array(expected.target.candidates, 16).length
    const resolvedCount = expected.target.kind === 'context' ? 1 : array(object(binding.target).candidates, 16).length
    if (resolvedCount !== count || observed.length !== count || result.contract_match !== (expected.interface_contract_digest ? observed.every((value) => value === expected.interface_contract_digest) : null)) invalid()
    const features = array(result.deployment_features, 16)
    const ambiguous = ['capability','context','child_agent'].includes(expected.target.kind)
    if (features.length !== (ambiguous ? count : 0)) invalid()
    const targets = expected.target.kind === 'context' ? [object(object(binding.target).binding).context_deployment] : array(object(binding.target).candidates,16)
    features.forEach((value, at) => {
      const evidence = closed(value, ['schema_version','deployment','interface_contract_digest','required_features'])
      if (evidence.schema_version !== 1 || evidence.interface_contract_digest !== observed[at]) invalid()
      exactDeployment(evidence.deployment)
      const exact = object(evidence.deployment), actual = object(targets[at])
      if (Object.keys(exact).some((key) => exact[key] !== actual[key])) invalid()
      const values = array(evidence.required_features,16)
      const order = ['model','context','remote-capability','mcp','sandbox']
      if (values.some((feature, index) => !order.includes(String(feature)) || (index > 0 && order.indexOf(String(values[index-1])) >= order.indexOf(String(feature))))) invalid()
    })
    match(result.contract_match)
  })
  return response as unknown as BindingResolution
}

/** Transport projection of already chosen exact slots. Rust remains the semantic validator. */
export function exactFeatureSelections(values: Json[]): BindingSelections | null {
  const slots: unknown[] = []
  if (values.length > 64) invalid()
  for (const value of values) {
    const binding=closed(value,['slot_id','requirement_digest','target']); const resolved=object(binding.target)
    target(resolved,false)
    if (resolved.kind==='model' || resolved.kind==='skill') continue
    const targetInput = resolved.kind==='context'
      ? {kind:'context',deployment:{kind:'exact',deployment:object(resolved.binding).context_deployment},...Object.fromEntries(Object.entries(object(resolved.binding)).filter(([key])=>key!=='context_deployment'))}
      : {...resolved,candidates:array(resolved.candidates,16).map(deployment=>({kind:'exact',deployment}))}
    slots.push({slot_id:binding.slot_id,requirement_digest:binding.requirement_digest,interface_contract_digest:null,target:targetInput})
  }
  return slots.length ? parseBindingSelections(JSON.stringify({schema_version:1,slots})) : null
}
export function exactResolvedFeatures(response: BindingResolution): DeploymentFeatures[] {
  const unique=new Map<string,DeploymentFeatures>()
  for (const slot of response.slots) {
    if(slot.resolution.kind!=='resolved') throw new Error(`Exact deployment feature resolution rejected: ${slot.resolution.code}`)
    for(const evidence of slot.resolution.deployment_features) {
      const previous=unique.get(evidence.deployment.deployment_id)
      if(previous && JSON.stringify(previous)!==JSON.stringify(evidence)) invalid()
      unique.set(evidence.deployment.deployment_id,evidence)
    }
  }
  return [...unique.values()].sort((a,b)=>a.deployment.deployment_id < b.deployment.deployment_id ? -1 : a.deployment.deployment_id > b.deployment.deployment_id ? 1 : 0)
}

# Installed authoring profile review

Joint contract decision accepted by root and agent_product before implementation. Implementation
qualification remains pending the real installation and model execution evidence.

The existing authoring profile response remains a bounded, digest-protected query. Its environment
comes from the installed catalog. Execution and Selection must be real Policy kinds with matching
exact revision/deployment identities, active current heads and the same environment. Scheduling is
not an Execution policy. The query requires current AgentWrite, PolicyRead and ModelRead, an active
tenant and a single read-only repeatable snapshot. Missing installation configuration is unavailable;
an invalid installed default fails rather than appearing unconfigured.

TenantConfig remains the only default authority. An optional default resolves to one exact Model
deployment under the authoring reference `project/default`, never a Registry alias or persistent projection.
Resolution validates the current Model/Provider/Secret closure and matching installation environment
and secret provider. No default returns an empty model list. No first-candidate fallback is permitted.
Publication and runtime authorization retain their current checks; this query issues no permit.

The CLI consumes the same profile. Offline compilation accepts explicit frozen compiler inputs from
an exported profile or source restoration, without synthesizing policy identities. Recovery reuses
the captured exact dependency closure; a new default only changes new authoring attempts.

Evidence required: actual installed Policy kind/gate/tenant checks, absent/present/invalid defaults,
current permission denial, environment mismatch, profile digest validation and use by the browser
and CLI compilers. Existing model default tests separately prove Receipt/CAS and frozen Run behavior.

The installed browser journey exposed a mismatch between the initial bare selector and the owning
compiler grammar. On 2026-09-10 root and integration_operations jointly accepted the correction:
the existing model-binding `alias` is a directly usable `project/` reference with the compiler's
bounded stable-name suffix. Bare names, other namespaces and exact deployment IDs are not aliases;
the separate deployment field retains the exact identity. The API type and generated schemas must
validate this same subset. Gateway emits the qualified reference, both clients consume it unchanged,
and the profile digest covers it. The compiler grammar, default-state authority and frozen bindings
do not change. A Gateway profile-to-compiler regression must exercise the actual emitted reference;
API tests must reject unsupported suffixes and the previous bare spelling.

The correction passed the API and real-PostgreSQL Gateway suites, the generated JSON Schema and
compiler conformance tests, and strict affected-crate Clippy. The Gateway regression builds the
production response and compiles its selected reference unchanged. Browser harness checks passed
with that same reference. The previous installed browser failure remains failed evidence; the
corrected image still requires a fresh installation and actual model journey.

# ADR-0016: Provider-owned endpoints

Status: Accepted design; implementation and qualification pending.

A ModelProvider Deployment owns its canonical HTTPS endpoint together with the existing endpoint
digest, credentials, region and exact policies. The endpoint is part of the frozen deployment
closure. Model configuration accepts the endpoint and supported protocol in the browser; it no
longer selects a route that must first be written to an installation file. Provider presets are
input suggestions only. The shared installer always provisions the model configuration policies
and actual adapter identities, even before the user has connected a model.

Security's dispatch permit includes the endpoint read from the already-authorized frozen provider
closure. Egress verifies its digest against the exact request and resolves the route only after
Security approval. ModelWorker still cannot supply an arbitrary transport URL. A deployment-owned
public-HTTPS policy constrains supported adapters, credential purpose and exact network/TLS/trust/
data policy identities. DNS checks, public-address filtering, TLS and hostname verification,
redirect rejection and bounded time/body/concurrency remain enforced before credential release.
Explicit fixed-route test transports remain a separately selected mode for local TLS qualification;
there is no fallback from a rejected public route to another grant.

## Contract and architecture review

The implementing Codex reviewed Registry's provider deployment closure and configuration compiler,
the public configuration contract, current PostgreSQL schema, Model dispatch authorization and
permit, Egress route resolver, installation catalog and current model documentation together.
This is a cross-boundary implementation review, not independent human review. The review accepts
the following obligations before implementation: one endpoint authority in the frozen Registry
deployment; no route table, deployment-file writes or duplicate business projection; a versioned
configuration input/catalog and dispatch permit; strict canonical endpoint/digest equality; existing
Registry Receipt/CAS and PostgreSQL transactions; no new business event or external-write retry.

Existing snapshots are not converted or supplied with invented endpoints. Fresh provisioning is
required. Uncertain publication resumes the same declared endpoint and original Receipt/CAS.
Credential import and declaration recovery may not silently switch addresses. The provider's
region remains an administrator declaration rather than a claim about geographic DNS routing.
Capacity bounds remain with their current owners. Tests must cover destination edits, frozen-run
identity, denied policy/credential/endpoint substitution, private DNS answers, publication response
loss, browser configuration without a deployment address and actual model execution. Earlier
fixed-route qualification alone is not evidence of the replacement.

This supersedes ADR-0010's requirement for an installed model destination before API configuration.
Remote Context destination policy and other execution/provider boundaries are unchanged.

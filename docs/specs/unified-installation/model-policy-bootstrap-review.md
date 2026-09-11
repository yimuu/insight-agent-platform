# Model installation Policy bootstrap

Status: jointly accepted upstream design; implementation and physical evidence are in progress.

The installer freezes the installation, Tenant, creator, request, Artifact/Blob, retention and ten
Policy identities before physical IO and synchronizes its private recovery file and directory.
Foundation owns this bounded versioned input. Registry owns one pure builder for the complete
policy declaration and its ordinary Resource, PublishedVersion and Deployment payloads. PostgreSQL
uses that same builder; no independent JSON rules supplied by a client are accepted. Default-rule
changes cannot silently rewrite an installed identity.

The declaration contains the complete current protocol, destination-network, TLS, trust, data,
safety, budget, public-projection, OnlyCandidate and execution rules. Rules with existing typed
documents use those owners. The remaining declarations identify the existing deployment grant and
durable execution boundaries; they do not introduce an unimplemented limit or a new authorization
engine. The safety instruction is the configuration compiler's exact accounted input. Basic input
and output budgets are 8192 and 2048 tokens. The existing generic Policy qualification Artifact
contains an explicit installation declaration and typed structural validation evidence; it says
nothing about provider conformance or real model output.

The installer stages the canonical declaration through the existing AWS Artifact provider. A
successful stage is followed by HEAD and bounded GET of the exact generation, including sealed
locator context and plaintext SHA verification. Unknown PUT outcomes permit only exact owned
readback; the operation and object identities are not rotated. The installer persists the original
encrypted locator, generation and evidence as bounded typed material. PostgreSQL validates their
binding to the seed and declaration, without pretending to perform provider IO inside a transaction.

A separate one-shot command follows base bootstrap and atomically creates the ten ordinary
Resources, immutable Versions and Deployments plus the real Artifact/Blob. It does not add a table,
grant, public management command, runtime DDL or new aggregate. Any preexisting partial or different
facts cause rejection. Replaying a completed command verifies immutable exact facts and never
repairs, rotates or writes ciphertext again. The declaration is readable through the ordinary
Artifact owner and its original retention contract.

Read-only verification is a separate PostgreSQL snapshot operation. It verifies stable installation,
Tenant and principal identities and the immutable seeded material, preserving subsequent permission
revocation, Tenant settings/default model, scheduler accounting and legitimate current Resource
head/gate changes. A passing installation verification is not a statement that a later current
business admission will succeed. Normal Registry/ModelLoop authorization remains required.

Evidence must cover strict seed/material decoding, missing/duplicate/swapped identities, altered
rules or source digest, ciphertext/generation/storage/context mismatch, atomic rollback, exact
replay without events or mutation, read-only verification after user configuration and revocation,
partial-state rejection, actual generation readback and normal Artifact read. Provider-backed tests
remain separate from PostgreSQL concurrency tests and from actual model/provider qualification.

The jointly accepted readable-Artifact closure uses current metadata version 2 with an explicit
origin: Upload carries its real verification Job; Installation carries the frozen ServerRequest.
Upload includes existing workload staging as well as public uploads. Upload/verification commands
require the Upload origin, while ordinary reads accept either validated origin. No compatibility
reader, fake Job, scanner evidence or newly readable builtin backend is introduced.

Each Policy identity also freezes its ArtifactLink ID before physical IO. The same transaction
creates ten ordinary Definition references owned by the actual immutable Policy revisions, using
the existing ArtifactReferenceSnapshot and link_key_digest algorithm. Normal reference release,
current read authorization and GC remain authoritative; verification never reactivates a released
link. These references express published declaration reachability, not provider qualification.

Implementation evidence: the current seed and pure builder unit tests pass; all 35 Artifact domain
unit tests pass, including Installation-origin rejection at upload replay and completion. The three
affected owning crates pass strict all-target Clippy. A fresh current-schema PostgreSQL database
passes the complete development bootstrap target, including late transaction rollback, concurrent
create/replay, ten published-reference links, exact ordinary Artifact reads, current authoring
policy resolution and preservation after permission withdrawal and reference release. A separate
fresh database passes the complete existing Artifact lifecycle target (27.16 seconds), including
upload, scan, deduplication, replay and cleanup. These are PostgreSQL/domain proofs; the synthetic
locator in the bootstrap fixture is explicitly not physical S3 evidence. The installer/provider
owner separately verifies staged bytes and installation deployment before claiming that closure.

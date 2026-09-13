# ADR-0014: Repository Compose entry point

Status: Accepted design; implementation and deployment evidence are tracked separately.

The repository supplies a Compose document that can build and start the local installation.
Users no longer run a container to generate an installation declaration or a second Compose file.
The existing finite preparation service derives its public input from bounded deployment
environment fields and publishes it to a dedicated volume before dependent services start.
Compose continues to own lifecycle; preparation gains no Docker API access or serving loop.

For this source-build entry point, `package_digest` identifies a canonical, versioned inventory
of actual installed executable bytes, including provisioning executables. It does not pretend to
be an OCI image digest: a container cannot discover its own final image digest without an external
authority. Released image selection remains the deployment system's responsibility. The existing
explicit-input image renderer continues to bind its selected immutable image. Both entry points
use the same typed installation factory and process renderer, and refuse drift in prepared state.

The checked-in Compose document is produced by the owning deployment builder. Its conformance
test ensures source changes cannot silently leave a second topology definition behind. User
environment fields change only the installation name and loopback Console port. Role JSON,
certificates, database passwords and provider configuration stay private installation outputs.

## Contract and architecture review

Before implementation, the implementing Codex compared `InstallationInputV1`, the current schema,
ADR-0012, the current installation documentation, the Compose builder, executable inventory and
private-state recovery owner together. This is a cross-boundary implementation review, not an
independent human review. Ownership, identities and schemas stay with their existing authorities;
there is no database, event, business transaction or public API change. The new process command
has closed arguments, bounded environment values and an immutable input publication. Only finite
installation tasks read the input volume. Existing lock, drift and unknown-outcome errors remain.
Restart re-derives the same input and verifies its bytes instead of replacing identities.

Verification must cover default and invalid environment values, actual executable identity changes,
repeated publication and drift, Compose conformance and a real source-built installation. Unit
tests or earlier image qualifications alone must not be described as the new startup succeeding.

This changes input delivery in ADR-0012. Authentication and model-destination policy are separate
boundaries and are not relaxed by this decision.

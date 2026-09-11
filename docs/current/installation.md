# Local installation

The complete local entry point runs the platform and Console as separate Compose containers.
The host needs Docker with Compose and Python 3; consuming built images requires neither Cargo nor
Node. Native host processes use the shared installation launcher below. Sandbox execution uses its
separate Kubernetes/OpenSandbox installation.

The current O artifacts passed image and offline consumer checks, and its separately frozen Native
package passed a real public CLI ModelLoop with typed result, complete durable events and controlled
business restart on the same input/package. Current-image physical Compose/Kind, actual retrieval
and browser signed-object transport remain separate acceptance work. The
[evidence index](../specs/unified-installation/deployment-review.md#o-current-delivery-evidence)
retains the earlier physical recovery results and their original failures without transferring them.

The configuration owner is
[`InstallationInputV1`](../../crates/deployment/platform-deployment-contracts/src/installation.rs).
The shared Rust renderer supplies Native, Compose and Helm process configuration. It binds
service addresses, TLS names, selected roles, actual worker executable digests and private file
references. Compose and Helm do not patch the generated process JSON.
The installation Helm chart keeps cluster DNS and sets Pod `ndots:1`, so generated
cluster-qualified provider names are resolved before inherited host search suffixes.
This applies to chart Jobs, serving processes, Console and dependencies; it does not
change TLS identities or add retries to installation operations.

## Start with Compose

Select the runtime and Console images by immutable digest. Compose also accepts an exact image ID
from `docker image inspect` for a locally built image. The runtime's package digest is the digest
in that selected image reference. The source build commands are:

```sh
docker build --target runtime -f deploy/images/platform.Dockerfile -t insight-runtime:local .
pnpm --dir apps/console build
docker build -f deploy/images/console.Dockerfile -t insight-console:local apps/console
RUNTIME_IMAGE=$(docker image inspect --format '{{.Id}}' insight-runtime:local)
CONSOLE_IMAGE=$(docker image inspect --format '{{.Id}}' insight-console:local)
RUNTIME_DIGEST=$RUNTIME_IMAGE
```

The source Console build needs the pinned Rust/WASM and `wasm-bindgen` tooling described in
[Console engineering](../../apps/console/README.md). These tools are not needed by an installed
Console container.

Generate the public installation declaration with the selected runtime image:

```sh
docker run --rm --network none --read-only --cap-drop ALL \
  --entrypoint /usr/local/bin/platform-installation "$RUNTIME_IMAGE" \
  compose-input my-platform "$RUNTIME_DIGEST" > input.json
```

The declaration contains no passwords or private keys. Add any intended model destinations to its
`model_destinations` input before installation; this is a deployment allowlist, not a configured
model account. Each destination supplies the owning protocol, canonical HTTPS endpoint prefix and
region. An empty list starts the base platform but leaves model configuration and its Agent
authoring profile unavailable. See [model configuration](model-configuration.md) for the distinction
between deployment destinations, sources, credentials and model profiles.

Remote Context uses the same separation. `remote_context_destinations` selects physical HTTPS
addresses, public trust roots, regions, byte ceilings and optional credential-purpose/header
mappings before the first installation. Its default empty list denies remote search. The shared
renderer enables the ContextRemote role for an installed destination; it does not create Context
resources, Policy revisions or conformance Artifacts. Those are published through the ordinary
Registry lifecycle. A destination contains no tenant, Context deployment or SecretBinding IDs.
See the [document-review example](../../examples/productization/document-review) for source and
publication steps; a local protocol test is not evidence of a deployed public provider.
The `compose-input`, `kubernetes-input` and `native-input` factories accept an optional trailing
`--remote-context-destinations FILE` containing the same owning array. They derive the worker,
network, role paths and credentials together; do not edit only the array after generating an input.
The file contains public trust material and DNS endpoints, not tenant or SecretBinding identities.
For a containerized factory, mount that public file read-only at the supplied absolute path.

Every remote dispatch still needs a current Security decision over the actual Context Query,
Run, Job lease, principal and frozen dependencies. The installed destination only constrains the
physical route. Credential mappings must match the currently authorized exact Secret purposes;
an empty mapping permits an anonymous provider without weakening authenticated providers.
Changing a destination requires a new reviewed deployment input, not a Registry request or
editing an already prepared installation.

For the Beijing DashScope Responses endpoint, replace the generated empty array before the first
`up` with this input fragment. Keep the other generated fields unchanged:

```json
"model_destinations": [{
  "protocol": "open_ai_responses",
  "endpoint": {
    "scheme": "https",
    "host": "dashscope.aliyuncs.com",
    "port": 443,
    "base_path": "/compatible-mode"
  },
  "region": "cn-beijing"
}]
```

This declares the permitted destination; it does not contain a key or create a model account.

Run the host wrapper with absolute paths and a new private directory:

```sh
python3 tools/install/platform_compose.py up \
  --input /absolute/path/input.json \
  --directory /absolute/path/private-installation \
  --runtime-image "$RUNTIME_IMAGE" --console-image "$CONSOLE_IMAGE"
```

The wrapper prepares identity and dependency files, starts the local dependencies, runs the
one-shot initializer, starts each selected role directly, and checks every role plus Console's
Gateway transport. It delivers `public-ca.pem` and prints safe certificate-file and session metadata. Open the reported Console endpoint
and use the token from the reported private file to establish the administrator session. The same
file is accepted by the public [model CLI](model-configuration.md).

The default Console binds `127.0.0.1:8088`. The durable local S3 endpoint binds `127.0.0.1:8333`
for browser-issued Artifact uploads; signed URLs retain the `s3.localhost` HTTPS hostname. Other process and database endpoints
remain inside the Compose network. Local S3 CORS permits PUT from the exact declared Console
origin; short-lived upload grants still require current server authorization. The generated S3
certificate uses the installation's private CA. Browser uploads require explicit trust in that
public CA; the installer never modifies OS trust or disables certificate/SAN checks. The CLI can
use the explicit public `--ca-file` option. Ordinary same-origin Console reads do not prove that
browser S3 uploads have been qualified.

Controlled container reconstruction stops serving processes before NATS, S3, OpenBao and PostgreSQL,
then verifies the original containers exited cleanly before removing them while retaining volumes.
The shared local dependency profile gives SeaweedFS 45 seconds to stop and uses the fixed NATS
image's normal SIGINT shutdown protocol for Compose and Native. Helm consumes the same S3 grace
period; it does not depend on optional Kubernetes container stop-signal support.

## Start native processes

The native entry point is `tools/install/platform_native.py`. It uses the same installation owner,
identity, provider lifecycle and renderer as Compose; Docker runs only the four dependencies.
The CLI's `qualification-aws` namespace is an explicit AWS fixture and is not the ordinary native
startup path.

Build the host workspace binaries and Console assets first. Select an absolute binaries directory,
Console source directory containing its built `dist`, and Node executable. Preflight requires
Node 24.11.1 or a later 24.x release, validates the actual host OS/architecture, and freezes hashes
of the executable and Console/WASM files before creating dependency resources. A changed or missing
artifact is rejected; restarting with another build requires a separate explicit installation.

```bash
mkdir -m 700 /absolute/native-installation
"$BIN_DIRECTORY/platform-installation" native-input my-native "$PACKAGE_DIGEST" \
  --output /absolute/native-installation/output --port-base 28000 \
  > /absolute/native-installation/input.json
native=(python3 tools/install/platform_native.py \
  --input /absolute/native-installation/input.json \
  --directory /absolute/native-installation \
  --binaries "$BIN_DIRECTORY" --console-directory "$CONSOLE_DIRECTORY" --node "$NODE_FILE")
"${native[@]}" render
"${native[@]}" up
```

Set any model destinations in the public input before `render`. `up` remains in the foreground;
Ctrl-C stops and reaps the host processes it started while preserving dependency containers, data,
private identity and recovery journals. Native roles share the host user's trust boundary; separate
processes do not provide the distinct OS identities used by the container deployment. A second
terminal can use the same arguments with `status`, `verify`, or explicit `session`; `session` writes
the renewed token to the reported private file without printing it. Repeating `up` keeps the frozen
identity and checks the existing provider and business authority instead of initializing another one.

## Identity and recovery

One-shot initialization owns a separate private volume. It freezes installation identity before
external writes and uses the existing schema, database-role, bootstrap and JetStream owners.
The model Policy declaration is written to S3 and read back at its exact object generation before
the owning PostgreSQL transaction installs its references. An uncertain external effect is resolved
only by the same persisted identity and exact observation.

The local physical provider is persistent S3 plus OpenBao Transit/KV v2. Initial self-initialization
has a one-time durable start permission and no automatic container/Pod restart. If that step fails,
keep the original volumes and files: the tools report incomplete or unknown outcome and do not
repeat initialization. ProviderReady requires actual certificate authentication and exact cluster,
mount, key and canary checks. Later starts use a separate serving configuration without self-init.
Read-only verification may acquire short authentication tokens and cause the provider's own audit
or authentication-lease records; it does not alter provider configuration, secrets or objects.
This is a local private-volume trust model, not production HSM isolation or a power-loss/HA claim.

An installation with model destinations also freezes the tenant Model concurrency account identity.
Its initial finite limit is a local deployment setting, not a provider rate guarantee. Each exact
Model deployment needs an explicit public quota allocation before execution. Verification and
restarts preserve existing limits, usage and reservations; they do not replenish budget.

Serving containers receive only their own read-only configuration and credential volume. The
Artifact, Security Authority, Outbox and History roles keep their existing narrow database grants.
Other basic local roles use an explicitly development-only DML login with no database ownership,
role membership, DDL or temporary-table permission. This development login is not a claim of
production least-privilege qualification. The initializer's database credential and CA/JWT issuer
private keys are not mounted into serving containers. No container mounts a Docker socket.

Repeated `up` verifies a completed installation before starting services; it does not reset schema,
regrant permissions, refresh frozen policy deadlines or regenerate credentials. `verify` performs
installation checks and readiness observations, and issues no new session. A successful owner
result may create the host's immutable `ready-owner-proof.json` binding; it does not change provider
configuration or business state:

```sh
python3 tools/install/platform_compose.py verify \
  --input /absolute/path/input.json --directory /absolute/path/private-installation \
  --runtime-image "$RUNTIME_IMAGE" --console-image "$CONSOLE_IMAGE"
```

Use the same arguments with `session` for explicit renewal. Sessions use the existing OIDC verifier,
last 900 seconds and are delivered as a raw token plus one newline in a caller-owned `0600` file.
Token bytes are never printed in command metadata. New installations explicitly bind a tenant
administrator; this does not increase the permissions of existing native developer identities.

## Obtain the public CA

Initial `up` delivers the exact installation CA to `public-ca.pem` in the host `--directory`, with
mode `0600`. The explicit `public-trust` operation reads the installed Ready identity and exports
only this public certificate, without contacting providers or renewing a session:

```sh
python3 tools/install/platform_compose.py public-trust \
  --input /absolute/path/input.json --directory /absolute/path/private-installation \
  --runtime-image "$RUNTIME_IMAGE" --console-image "$CONSOLE_IMAGE"
PUBLIC_CA_FILE=/absolute/path/private-installation/public-ca.pem
shasum -a 256 "$PUBLIC_CA_FILE"
```

Use the same operation and installation arguments with the Native or Helm wrapper. An installation
created before host Ready evidence was retained must first run explicit `verify`; export does not
infer its identity from a certificate or rerun provisioning. A different existing destination file
is rejected and retained. Keep the original private installation files and the Ready binding.

The reported `certificate_sha256` is the SHA-256 of the original PEM **file bytes**; compare its
hexadecimal part with `shasum`. It is different from the DER certificate fingerprint displayed by
browsers or `openssl x509 -fingerprint`. Export proves the frozen installation binding, not the
reachability or TLS name of a service. Pass this file to the CLI's `--ca-file`. Browser trust must
be installed explicitly by the operator in their chosen browser trust store; the tools never
modify OS trust, bypass TLS validation or route Kubernetes service names automatically.

Input, file, role, provider or object drift fails closed. The wrapper retains resources and evidence
on failure; it does not delete volumes or repair foreign state. Keep the original private directory
and declaration to resume the same installation. Replacing a package is a separate deployment
operation, not an edit that an installation verification command silently accepts.

## Local Kubernetes

The [installation Helm chart and wrapper](../../deploy/helm/insight-platform-installation/README.md)
consume the same owner. The host operator controls the cluster; Pods have no Kubernetes API token.
Prepare and provision results are checked against actual Job/Pod identities before serving is
allowed. Every role has a separate volume; installation private state is confined to one-shot
operations. Existing per-role production charts and their qualification gates remain separate.

After the original OpenBao initializer stops, `up/resume` checks the ordinary provider through
actual authentication and provider reads before platform provision. TCP readiness alone does not
satisfy this check. An interrupted observation keeps its original Job; resolving that old result
does not replace the fresh observation required for this invocation. The original initialization
proof and any pending platform provision request remain unchanged.

Helm `up/resume` reuses a previously delivered session instead of renewing it implicitly. If only
that session has expired, the installation can finish its Ready checks and return `SessionExpired`
with an explicit renewal instruction. Use the same arguments with `session`; do not repeat
provisioning or replace the installation identity. Changed or unknown session material remains a
separate failure and is not treated as ordinary expiry.

Local Kubernetes object access also requires routing the exact signed S3 service hostname
and trusting the installation's public CA in the browser. A Console port-forward alone does not
provide object transport. The tools neither install trust into the user's OS nor weaken TLS.

For offline restarts, keep the exact selected images locally. The Compose wrapper validates and
creates an installation-specific retention tag for each selected image so rebuilding an unrelated
local build tag does not discard its last cache reference. Execution still uses the original
immutable digest. A pre-existing retention tag pointing to different bytes is rejected, and the
wrapper does not remove images or retarget another installation's reference.

# Local installation

The complete local entry point runs the platform and Console as separate Compose containers.
The host needs Docker with Compose; consuming built images requires no host Python, Cargo or Node. Native host processes use the shared installation launcher below. Sandbox execution uses its
separate Kubernetes/OpenSandbox installation.

The declarative Compose and local Kind/Helm paths have passed physical installation and recovery
checks for the images in the [local qualification reports](../qualifications/README.md).
Earlier Native ModelLoop results remain separate package evidence. External model and retrieval
dispatch, browser signed-object transport and release qualification are not implied by startup.

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

From the repository root:

```sh
docker compose up -d --build
```

Open **http://127.0.0.1:8088**. Create the first administrator with a name, email and password.
Subsequent visits use the same login. A browser session lasts eight hours and survives process
restarts. Log out to revoke that session. No token file or browser CA installation is needed.

The root `compose.yaml` is checked against the Rust topology producer. Docker builds the runtime
and Console, including the pinned WASM compiler. The host needs Docker with Compose; it does not
need Python, Cargo, Node or a separate configuration-generation command. First compilation may
take several minutes. Subsequent unchanged starts reuse the built images and installed volumes.

The optional [.env.example](../../.env.example) documents installation name, public loopback port
and image overrides. Defaults are `my-platform` and `8088`. A prepared installation freezes these
inputs and actual executable identities; changing those is a deployment change, not an ordinary
restart. Existing installations are not silently migrated or reset.

For the explicitly supported schema 16 installation, the conversation release has a separate
preserving-data upgrade under [ADR-0017](../adr/0017-conversation-live-debugging-and-release-upgrade.md).
Build the new images first. Stop serving processes, including Console, local identity, gateways and
workers; keep PostgreSQL, NATS, S3 and OpenBao running. Then run:

```sh
docker compose run --rm --no-deps installation-prepare compose-upgrade --serving-stopped
docker compose restart nats
docker compose up -d
```

The upgrade validates the exact installed source schema, package and role outputs. PostgreSQL
commits the additive schema and deployment receipt atomically; a retry of the same upgrade resumes
file publication. It preserves the bootstrap identity, accounts, model configuration and Run data.
Unknown schemas or edited role outputs are rejected. Serving processes still perform no DDL, and
ordinary startup neither upgrades nor erases data. Fresh installations continue to use the one-command
startup above. This release-specific upgrade is not a general historical migration chain.

Models are configured in the browser. Select a provider or enter a custom compatible HTTPS URL,
then save a key and model ID. Installation always provisions the configuration policies and
actual adapters; no deployment address allowlist must be populated before the page is usable.
The Registry freezes each source endpoint and credentials. Egress still enforces authorization,
public DNS addresses, TLS hostname verification and redirect/body/time limits.

Common commands:

```sh
docker compose ps
docker compose logs --tail 100 console local-identity installation-provision
docker compose run --rm --no-deps installation-ready
docker compose stop
docker compose up -d
```

`stop` preserves containers and data. `docker compose down` removes containers and the network,
while preserving named volumes. `docker compose down --volumes` deliberately removes this
installation's data; it is not part of normal startup.

The browser uploads through the Console origin. The Console verifies the internal object-store
CA and forwards only the exact signed upload request. Browser certificate exceptions and direct
cross-origin access to S3 are unnecessary. External model service certificates are still verified.

Advanced immutable-image deployments may use the typed `compose-input` and `compose` producers.
Remote Context destinations retain their separately reviewed installation policy; the default
empty remote-context list denies remote search. They are not model account configuration.

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

`up` remains in the foreground;
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

The local physical provider is persistent S3 plus OpenBao Transit/KV v2. OpenBao uses one ordinary
server configuration for both first installation and later starts. The finite Rust bootstrap
records its one-time intent, initializes through the API, retains recovery material privately,
waits for the active server and installs the fixed authentication policies and keys. It revokes
the temporary root token before provider readiness can be committed.

If initialization is interrupted, retain the original volumes and input. A later bootstrap may
observe the existing provider and finish root-token revocation. It never repeats uncertain
initialization or configuration writes. Lost initialization responses without persisted recovery
material remain `ExternalOutcomeUnknown`; missing state does not authorize a replacement identity.
Provider readiness requires actual certificate authentication and exact cluster, mount, key and
canary checks. Read-only verification may create short authentication leases and provider audit
records; it does not change provider configuration, secrets or objects. The static seal and local
private volumes are a development trust model, not production HSM isolation or an HA claim.

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

Repeated `docker compose up -d` runs the installation dependencies against the existing identity.
An already completed provision verifies the installed schema, grants, policies, objects and role
files; it does not reset, regrant or regenerate them. Compose may leave already running workloads
running during this check. To run explicit checks without changing workload lifecycle:

```sh
docker compose -f compose.json run --rm --no-deps installation-verify
docker compose -f compose.json run --rm --no-deps installation-ready
```

Explicit CLI bearer sessions use the existing OIDC verifier and last 900 seconds. Compose and Helm browser login instead use the local identity service and an eight-hour HttpOnly session cookie. Explicit issuance writes a raw token
plus one newline in a private file; token bytes never appear in logs. New installations bind the
tenant administrator without changing existing identities' permissions.

## Obtain the public CA

The `public-trust` operation reads the installed Ready identity and exports only its public
certificate, without contacting providers or renewing a session. Copy from its read-only container:

```sh
umask 077
docker compose -f compose.json run --no-deps --name my-platform-trust \
  installation-public-trust > public-trust.json
docker cp my-platform-trust:/installation/private/ca.pem ./public-ca.pem
docker rm my-platform-trust
PUBLIC_CA_FILE="$(pwd)/public-ca.pem"
shasum -a 256 "$PUBLIC_CA_FILE"
```

Native retains its `public-trust` operation. The Helm chart provides an explicit suspended
`installation-public-trust` Job template, using the file delivery procedure in its README.

The reported `certificate_sha256` is the SHA-256 of the original PEM **file bytes**; compare its
hexadecimal part with `shasum`. It is different from the DER certificate fingerprint displayed by
browsers or `openssl x509 -fingerprint`. Export proves the frozen installation binding, not the
reachability or TLS name of a service. Pass this file to the CLI's `--ca-file`. Console object uploads
use the CA in the server's deployment configuration; the tools never modify OS/browser trust or
bypass TLS validation.

Input, file, role, provider or object drift fails closed. Failed installation jobs and data volumes
are retained; no normal command repairs foreign state or deletes volumes. Keep the original
input and private volumes. Replacing the package is a separate deployment operation.

## Local Kubernetes

Prepare a Kubernetes input with the selected runtime's `kubernetes-input NAME PACKAGE_DIGEST`
command, then generate Helm values using `helm-values --input FILE --runtime-image IMAGE
--console-image IMAGE`. Both product image references must use repository digests accessible to
the cluster. See the [chart guide](../../deploy/helm/insight-platform-installation/README.md) for
complete commands. With values prepared, startup is one native Helm command:

```sh
helm upgrade --install installation deploy/helm/insight-platform-installation \
  --namespace my-platform --create-namespace -f values.json \
  --set node="$NODE_NAME" --wait --wait-for-jobs --timeout 15m
```

Helm submits the complete resource set. One installation Job publishes prepared configuration,
initializes dependencies, provisions the platform and publishes completion. Dependency init
containers wait for preparation; serving init containers wait for completion. These bounded,
input-bound gate files live on a separate read-only publication volume and confer no platform
permissions. Serving processes still check their own configuration and installed schema.
No Pod receives a Kubernetes API token or a Docker socket. Installation private state is visible
only to administrative Jobs; each role receives only its own configuration and credentials.

Repeating the Helm command creates a new finite verification-capable installation Job against the
same PVCs. Existing deployments retain their normal Kubernetes lifecycle. Interrupted or failed
initialization remains subject to the same one-time write guard. The chart keeps PVCs and Job
evidence across uninstall; data removal is an explicit operator action. This chart remains a
single-node development topology with node-bound ReadWriteOnce volumes, not a multi-node HA chart.

Console can be port-forwarded to its configured loopback origin. The chart deploys the same password login service and owner/session tables, and model addresses are configured in the browser. No model destination declaration is needed in Helm values. With the V3 Console transport, object uploads
use that same port-forward; the Console pod resolves the installed S3 service hostname and validates
its installation CA. Direct CLI object access still needs an explicit route and `--ca-file`.
This transport change does not by itself claim a new Kubernetes qualification run.

Keep the selected digest-pinned images available in your registry or local cache for restarts.
The deployment does not create host-side image retention tags or change another image reference.

For a package-only patch after the explicit conversation schema upgrade, stop serving roles and
keep dependencies running. Using the same installation/output mounts, obtain the canonical
release digest with `platform-installation compose-release-digest`, then invoke the new package's
`platform-installation compose-rollout --serving-stopped --expected-release sha256:…`.
This verifies the unchanged current schema and advances deployment evidence without changing
business data or bootstrap identities. Restart serving only after it succeeds. For interrupted
rollout, repeat the same package and original expected digest; unknown output edits are rejected.
The expected digest is the command's canonical digest, not a hash of formatted `release.json`.

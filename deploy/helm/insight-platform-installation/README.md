# Local Kubernetes installation

This chart consumes the same Rust installation owner as Compose. It provisions a new, exclusive
namespace and its own PVCs, then directly starts the generated role binaries and Console. It is a
development topology. Its disposable Kind qualification exercises a fresh installation, verification,
controller recovery and session delivery on the selected images. Render and API fixture tests remain
separate evidence; they do not qualify a cluster or a browser journey.

Build the runtime and Console images and make their repository digests available to the selected
cluster. Generate `input.json` with the runtime's `platform-installation kubernetes-input NAME
PACKAGE_DIGEST` command. The installation name is also its namespace. Model destinations, if used,
must pass the owning installation input decoder; this chart does not patch role configuration.

Run `tools/install/platform_helm.py up` with an absolute `--input`, private `--directory`, immutable
`--runtime-image` and `--console-image`, and explicit `--kubeconfig`, `--context` and `--node`.
`--storage-class` selects a provisioner; otherwise Kubernetes uses its configured default. The
selected node keeps the installation's ReadWriteOnce volumes together. The provisioner must supply
empty private volume directories; the initializer refuses unknown pre-existing files.

The host first records namespace ownership and runs prepare. A one-time provider-start Job
authorizes one bare OpenBao Pod with restart disabled. The wrapper observes that exact Pod until
the installation owner confirms ProviderReady, then requests its clean stop with an API UID
precondition. A finalizer retains the original exit evidence until its removal is atomically checked.
Only then can the separate serving configuration start, followed by platform provision and roles. Helm reads the actual completed Job and its Pod's termination message before
each later phase. Changing a values flag cannot synthesize a Ready result. These Job/Pod objects are
retained as deployment evidence. Pods have no Kubernetes API token, Kubernetes client or role grant.

Each serving process mounts only its own role PVC. Console mounts only its configuration file.
The installation private volume is visible only to one-shot owner operations. S3 and OpenBao each
receive their own dependency files and persistent data volume. Runtime roles receive only their own
S3 identity or OpenBao certificate credentials and the public installation CA. HTTPS hostname and
CA verification remain enabled. The local static seal and private volumes are a development trust
boundary, not production HSM isolation.

Repeated `up` runs verification before starting the installed closure. `verify` requires an already
completed installation. An interrupted controller can read back the same completed Job; `resume`
can explicitly start a new owner invocation after a terminal failed Job, using the same installation
journal and PVC identities. Provider initialization is the exception: an unknown start never creates
a second Job or initializer Pod; it can only observe the original recorded identity. It does not
delete failed evidence or repair foreign state. A missing or
changed PVC, namespace, input, credential or installed configuration fails instead of being adopted.
Verification retains existing serving objects without applying them; only the subsequent successful
serving phase can start missing roles. Helm keep policies retain those objects, PVCs and completed
Jobs across phase changes and uninstall. Removing an installation is an explicit namespace cleanup
by its operator, outside this wrapper.

After `up`, the host delivers the initial administrator session to a local `session-token` file with
mode 0600. Run the same wrapper with `session` to explicitly renew it. A short-lived delivery Pod
generates the token in a private file; the operator reads that fixed file through Kubernetes exec,
never Pod logs. Only the delivery Pod is removed, using an API UID precondition. If a response is
lost, the same recorded nonce and Pod identity are reused. Ordinary `verify` never signs a token.
Printed output contains the tenant, endpoint, expiry and local file path, without the token itself.
If a completed delivery has expired, `up/resume` reports `SessionExpired` and retains the Ready
installation. Use explicit `session`; neither retrying provisioning nor a new identity renews it.

Initial `up` also delivers `public-ca.pem` in the host directory. Explicit `public-trust` uses a
separate bare Pod with read-only installation mounts and the same Ready identity, then removes
only that exact Pod. Interrupted delivery retains its nonce and UID; it does not change provider,
provisioning or session state. Existing installations without a host Ready binding must run
explicit `verify` first. See [public CA delivery](../../../docs/current/installation.md#obtain-the-public-ca)
for file-digest checking and explicit browser trust. Export does not prove browser routing or TLS.

For browser access, Console can be port-forwarded to the exact configured origin. Object URLs retain
the signed `s3.NAME.svc.cluster.local:8333` host: the browser needs a matching DNS route/tunnel
and must trust the public installation CA. A Console tunnel alone does not provide object access,
and neither the chart nor Console rewrites signed URLs or bypasses TLS verification.

Run the installation Helm, provider lifecycle and Native tests under `tools/tests` after building
the installation binary. They exercise real Helm rendering against disposable API fixtures and
host recovery boundaries; they do not substitute for the actual container and cluster tests.
The S3 primitive qualification separately exercises versioning, exact object operations, TLS
rejection and controlled container recreation. The explicit AWS adapter qualification remains a
separate provider path and is not the default installation dependency.

For actual Helm qualification, run `tools/qualification/qualify-platform-installation-kind.py` with
`--runtime-image REPOSITORY@DIGEST`, `--console-image REPOSITORY@DIGEST` and an unused absolute
`--report` file. Both images and the pinned node and dependency images must already be available in
the local Docker store. The harness verifies image index, platform manifest, configuration and layer
bytes before importing them. It supports matching Linux amd64 or arm64 images. Docker's containerd
image store is required; a classic-store configuration ID does not establish a repository digest.
The repository's productization workflow already demonstrates the pinned Docker setup action with
`containerd-snapshotter` enabled.

Install Kind v0.33.0 and Helm before invoking the harness. Kubernetes commands use the pinned node's
matching client through a private shim, so no system kubectl replacement is needed. The harness
creates a unique cluster, private kubeconfig and random loopback API port, with no application host
port mapping. It never reads an existing kubeconfig or uses an existing cluster. It verifies initial
private session delivery, read-only verification, a new runtime Pod becoming Ready after an explicit
UID-conditional deletion, unchanged installation and role files, and explicit session renewal.
It also verifies the installed public CA and exact service DNS with Linux OpenSSL 3, including
rejection of an unknown CA and incorrect service name. The required installation CI lane runs the
physical provider checks, current Compose installation with internal Policy Artifact readback,
Compose cleanup, and then this Kind qualification in sequence. A passed result requires confirmed
cleanup of its own cluster. A failed full installation preserves its original cluster, volumes and
private recovery material plus a safe report; deletion requires explicit ownership-scoped cleanup.
Credential bytes and Kubernetes credentials are never included in the report.

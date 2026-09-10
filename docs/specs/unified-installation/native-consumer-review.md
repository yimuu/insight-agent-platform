# Native installation consumer

Status: architecture direction, owning launch contract and foreground consumer jointly accepted by
Root, Operations and Product. A fresh native installation has now passed all selected process and
Console readiness, session renewal during supervision, duplicate-supervisor rejection, three-source
model configuration, the real Qwen connection probe, the actual Console Models browser view, and
second-source binding rotation/revocation. A later installation also passed explicit recovery after
cleanly stopping and replacing all four dependency containers: private installation records and
public sources, models, default, quota and existing Run remained unchanged, and the recovered
credential passed a real Qwen connection probe. Its first recovery attempt failed before S3 began
listening and remains recorded separately; the new S3 readiness correction still needs a fresh
runtime qualification. Full ModelLoop remains open: the published text-only Agent reached its
ModelLoop but was rejected by the inconsistent tool-budget contract before any ModelTurn was
created. Earlier model staging credential and eager RPC startup failures were repaired and
regression-tested; the original failed installation state remains available.

The startup correction is jointly accepted by Root and Product: the owning native launch contract supplies the
selected server-before-consumer order. Each spawned role must pass the installer's existing bounded,
read-only readiness observer before the next role starts. The observer accepts only an exact process
selected in the validated input, and the final complete readiness check still includes Console.
All process launches and observations share one 120-second serving deadline; individual probes do
not extend it. Physical dependencies retain their separate bounded initialization lifecycle.
There is no process restart or business retry; a failed child interrupts the probe and closes all
owned children. This changes deployment startup sequencing, not application identities, schemas,
transactions, events or authorization. Existing shutdown/deadline evidence must still pass, with
new tests for selected-role rejection, readiness ordering and a fresh full installation.

Native development uses the same `InstallationInputV1`, `PreparedInstallation`, physical provider
observations, role renderer and one-shot provision/verify commands as Compose. It does not translate
the earlier CLI project identity into an installation identity. The explicit AWS qualification
fixture remains isolated from the ordinary startup entry point.

The deployment tool emits a bounded native launch plan from the owning process registry and actual
host artifacts. The Python consumer does not invent role arguments, credentials, provider policies
or service configuration. Before preparation or dependency startup it verifies the host executable
format and architecture, executable permissions, the supported Node runtime, and the built Console
assets including the authoring compiler WASM. Source and executable drift fail closed.

The [owning launch type](../../../crates/deployment/platform-deployment-contracts/src/native_installation.rs)
also closes the Console server/bundle relationship and preparation directory tree. The
[producer](../../../tools/rust/platform-deployment-tooling/src/native.rs) reads bounded regular
artifacts without following links and checks file identity before and after hashing. The consumer
gives the owner an exclusive staged input copy before freezing the same declaration and launch plan;
it cannot accidentally combine one caller input with a different plan during an input replacement.

`up` is a foreground supervisor. Its separate private lifecycle lock excludes a second supervisor;
each installer command holds the existing installation lock only for that command. A running
installation can therefore issue another short session. `status` checks the frozen installation's
readiness endpoints; it does not claim ownership of processes started by another supervisor.

Every required child is represented by its current child-process handle. A spawn failure, readiness
failure, or any required child's exit, including exit zero, stops and reaps all children within a
bounded shutdown interval and fails startup. SIGINT and SIGTERM use the same cleanup. No persisted PID
list grants permission to signal a process. After an uncatchable supervisor failure, occupied ports
prevent automatic restart; external service managers or the operator own orphan recovery.

Children receive only the renderer's explicit environment and the minimum ordinary host settings.
Ambient database credentials, AWS credentials, proxies and Node options do not pass through. Logs
are bounded, private and drained without blocking process supervision; command failures expose safe
classifications. Issuer, provider administrator and seal materials are never given to serving roles.

The shared dependency plan retains its persistent volumes when foreground serving stops. Native
dependency containers may use the actual non-root host UID/GID to read their own private bind mounts;
this is a trusted single-user development host, not operating-system isolation between roles.
Compose and Kubernetes retain their separately specified serving identities. No supervisor exit
resets providers, deletes volumes, repeats self-initialization or repairs durable business state.

The same Console transport provides the Agent/Run flow. CLI public connections must be established
explicitly from the same installation's endpoint, tenant and private session file; a new connection
reference cannot synthesize a `LocalProjectState` or act as an authentication authority. Connection
configuration must be jointly reviewed before general Agent CLI support is claimed.

Qualification must cover preflight rejection before effects, concurrent session issuance and second
supervisor rejection, partial spawn and readiness failures, required-child exit, bounded signal
cleanup, retained dependency data, and a real native Console/Agent journey. Unit tests or provider
primitive tests alone do not establish the complete native installation claim.

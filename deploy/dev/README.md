# Platform local dependency profile

`compose.yaml` is consumed only by the `insight dev --profile base` supervisor. It provides the
durable/external dependencies that existing Platform processes require: PostgreSQL 16, NATS and
a pinned LocalStack Community 3.8.1 S3/KMS implementation. The platform Gateway, Artifact
Gateway, Artifact Data Worker, Orchestration Worker and Native Capability Worker are **not**
reimplemented here; the CLI starts their checked Cargo products as independent processes.

## Current base profile

From a checked-out repository, create a project-local non-production identity and start the
profile:

```sh
cargo run --locked -p insight-cli -- doctor
cargo run --locked -p insight-cli -- init --path /path/to/my-platform-project --name my-platform-project
cargo run --locked -p insight-cli -- dev --path /path/to/my-platform-project --profile base
```

`status`, `logs --role <role>`, and `stop` use the same `--path`. The supervisor provisions the
fresh local PostgreSQL baseline and developer identity, creates the versioned S3 bucket and KMS
key, then starts the seven independent roles. It writes only generated state below
`/path/to/my-platform-project/.insight/`; private keys do not enter the role configuration files.

PostgreSQL, NATS, and LocalStack keep their fixed loopback ports `5432`, `4222`, and `4566`.
NATS JetStream requires mutual TLS even in the development profile. `insight init` generates a
project-local CA, `localhost` server certificate and client certificate; the supervisor passes only
the three exact server-side paths to Compose, while NATS-consuming Platform roles receive the CA
and client identity directly. Compose has no plaintext NATS fallback and no checked-in private key.
Platform role ports are allocated from free loopback ports and recorded in the local profile, so
they do not collide with ordinary desktop applications. `stop` stops every Platform role but
deliberately leaves PostgreSQL, NATS and LocalStack running. LocalStack Community 3.8.1 does not
persist this profile's S3/KMS authority across container recreation; keeping the pinned dependency
container alive is therefore part of the base profile's durable local restart contract. A later
`dev` reuses the same immutable profile closure and, when the source fingerprint is unchanged, the
existing release binaries without invoking a release build. Removing the Compose dependency
containers is an explicit local-environment teardown, not a supported durability/recovery operation.

The `full` profile is intentionally not implemented yet. Nor does the base profile claim a
first-run authoring workflow, external-model operation, gVisor qualification, multi-node
Kubernetes validation, or production deployment readiness.

The LocalStack endpoint is reached by its HTTPS `localhost.localstack.cloud` endpoint. The
profile creates a versioned bucket and KMS key explicitly before emitting any Artifact process
config. The `test` credentials are isolated development fixture credentials, not a production
AWS identity or a user-supplied secret.

These pinned images establish only the M1 local dependency boundary. They do not qualify
production S3/KMS, GitOps promotion, multi-node Kubernetes or gVisor.

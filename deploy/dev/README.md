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
key, then starts the six independent roles. It writes only generated state below
`/path/to/my-platform-project/.insight/`; private keys do not enter the role configuration files.

PostgreSQL, NATS, and LocalStack keep their fixed loopback ports `5432`, `4222`, and `4566`.
Platform role ports are allocated from free loopback ports and recorded in the local profile, so
they do not collide with ordinary desktop applications. `stop` stops containers but retains the
PostgreSQL and LocalStack volumes. A restart with unchanged source and profile state reuses the
existing release binaries instead of invoking a release build.

The `full` profile is intentionally not implemented yet. Nor does the base profile claim a
first-run authoring workflow, external-model operation, gVisor qualification, multi-node
Kubernetes validation, or production deployment readiness.

The LocalStack endpoint is reached by its HTTPS `localhost.localstack.cloud` endpoint. The
profile creates a versioned bucket and KMS key explicitly before emitting any Artifact process
config. The `test` credentials are isolated development fixture credentials, not a production
AWS identity or a user-supplied secret.

These pinned images establish only the M1 local dependency boundary. They do not qualify
production S3/KMS, GitOps promotion, multi-node Kubernetes or gVisor.

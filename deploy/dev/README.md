# Platform local dependency profile

`compose.yaml` is consumed only by the forthcoming `insight dev` supervisor. It provides the
durable/external dependencies that existing Platform processes require: PostgreSQL 16, NATS and
a pinned LocalStack Community 3.8.1 S3/KMS implementation. The platform Gateway, Artifact
Gateway, Artifact Data Worker, Orchestration Worker and Native Capability Worker are **not**
reimplemented here; the CLI starts their checked Cargo products as independent processes.

The LocalStack endpoint is reached by its HTTPS `localhost.localstack.cloud` endpoint. The
profile creates a versioned bucket and KMS key explicitly before emitting any Artifact process
config. The `test` credentials are isolated development fixture credentials, not a production
AWS identity or a user-supplied secret.

These pinned images establish only the M1 local dependency boundary. They do not qualify
production S3/KMS, GitOps promotion, multi-node Kubernetes or gVisor.

# Platform local dependency closure

`compose.yaml` is embedded in the `insight` binary and copied into project-local generated state by
`insight dev`. It supplies the exact external dependencies shared by every development closure:
PostgreSQL 16, NATS, and pinned LocalStack Community S3/KMS/Secrets Manager. Platform roles remain
independent processes with separate identities, configuration, pools, and permits.

The supported user path installs the prebuilt CLI and runs:

```sh
insight doctor
insight init --path /path/to/my-platform-project --name my-platform-project
insight dev --path /path/to/my-platform-project
```

Repository contributors may explicitly replace the signed prebuilt runtime with a source build:

```sh
cargo run --locked -p insight-cli -- dev \
  --path /path/to/my-platform-project --from-source
```

The default `starter` closure provides deterministic Agent publication and Run execution. Optional
roles are additive and user-selected with `--features model,remote-capability,context,mcp,sandbox` or
`--features all`; there are no legacy profile aliases. Feature enablement appends only its exact
configuration, identities, certificates, binaries, and readiness checks. It never rebuilds existing
authority or silently enables external access.

`status`, `logs --role <role>`, `stop`, and `start` use the same `--path`. `reset` first prints the
exact project-local data and Compose resources it would remove, then requires the project name via
`--confirm`. Generated state remains below `/path/to/my-platform-project/.insight/`; private keys do
not enter role configuration files.

PostgreSQL, NATS, and LocalStack use fixed loopback ports `5432`, `4222`, and `4566`. NATS JetStream
requires mutual TLS. Platform role ports are allocated once and retained in the profile authority.
`stop` stops Platform roles but preserves dependency containers so existing PostgreSQL, object, KMS,
and Secret authority can be restarted. Removing those project-scoped containers or volumes is an
explicit reset, not an automatic failure-recovery action.

This is a single-node, non-production development environment. It does not qualify multi-node
Kubernetes, strong tenant isolation, capacity, chaos, restore, soak, or GitOps promotion; L4 through
L6 remain Not run.

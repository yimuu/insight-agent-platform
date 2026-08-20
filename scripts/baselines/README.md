# Crate boundary dependency baselines

`crate-boundary-third-party-features.tsv` is the normative, sorted snapshot of
every non-workspace package `(name, version, enabled features)` in the complete
`cargo metadata --locked --all-features` resolve graph.  The crate-boundary CI
gate compares it byte-for-byte, including the SQLx, Tokio, Axum, and Reqwest
rows.

`cargo-tree-workspace-all-features.txt` is the corresponding human-readable
`cargo tree --locked --workspace --all-features -e features` record.  Workspace
paths are normalized to `<workspace>`; its header records the pinned toolchain.

The current files include the reviewed MCP protocol/JWT/PKCE cryptography surface and the
Kubernetes client used only by the admission-locked gVisor launcher. The latter is pinned to the
AWS-LC Rustls provider; enabling Ring is a hard boundary failure. Regenerate the files with
`scripts/record-crate-boundary-baselines.sh` only for a separately reviewed dependency/feature
change; workspace crate moves must leave the normative TSV unchanged unless they also introduce a
reviewed third-party runtime closure.

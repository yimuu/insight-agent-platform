# Crate boundary dependency baselines

`crate-boundary-third-party-features.tsv` is the normative, sorted snapshot of
every non-workspace package `(name, version, enabled features)` in the complete
`cargo metadata --locked --all-features` resolve graph.  The crate-boundary CI
gate compares it byte-for-byte, including the SQLx, Tokio, Axum, and Reqwest
rows.

`cargo-tree-workspace-all-features.txt` is the corresponding human-readable
`cargo tree --locked --workspace --all-features -e features` record.  Workspace
paths are normalized to `<workspace>`; its header records the pinned toolchain.

The current files include the reviewed MCP protocol/JWT/PKCE cryptography
surface. Regenerate them with `scripts/record-crate-boundary-baselines.sh` only
for a separately reviewed dependency/feature change; workspace crate moves
must leave the normative TSV unchanged.

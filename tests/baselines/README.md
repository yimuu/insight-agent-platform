# Phase 0 compatibility baselines

`root-public-api.txt` is generated and verified by
`scripts/check-public-api-baseline.sh`. The generator is pinned to the
repository's exact Rust compiler and records canonical declarations, including
function and associated-item signatures, generics, bounds, where predicates,
public fields, enum variants, trait supertraits, and workspace-authored impls.
Compiler-provided impl noise and rustdoc's unstable numeric IDs are excluded.

The root facade and every workspace member are rendered together, so a facade
alias keeps its root public path even when the defining item moves into a
member crate. Update the baseline only for an independently approved breaking
API change:

```bash
UPDATE_PUBLIC_API_BASELINE=1 bash scripts/check-public-api-baseline.sh
```

Protocol and migration byte baselines in this directory are verified by the
Phase 0 compatibility integration tests.

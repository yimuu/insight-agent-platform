# `insight.platform/v1` machine contracts

This tree is the checked-in target contract for the Platform v2 clean replacement. It is under
implementation and is not the current public behavior. Current behavior remains documented in
`docs/current` until qualification and cutover are complete.

The Rust producer is `crates/platform-contracts`. Generated registries must match that crate
byte-for-byte. Limits and fixtures are validated by both Rust tests and the independent contract
checker. `/v1/mcp/oauth/callback` is the first active machine-contract slice because its domain,
PostgreSQL first-winner authority and hardened HTTP adapter are implemented. It remains
`implementing_not_current`: checked-in paths are target contracts, not deployed/current behavior,
until qualification and clean replacement complete.

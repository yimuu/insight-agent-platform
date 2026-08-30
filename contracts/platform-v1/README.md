# `insight.platform/v1` machine contracts

This tree is the checked-in current public contract for the Platform v2 clean replacement.
Current behavior is documented in `docs/current` and exposed through `/v1`.

The Rust producer is `crates/platform-contracts`. Generated registries must match that crate
byte-for-byte. Limits and fixtures are validated by both Rust tests and the independent contract
checker. `/v1/mcp/oauth/callback` includes its domain, PostgreSQL first-winner authority and
hardened HTTP adapter. Repository contract status is `current`; release-only L4-L6 production
qualification remains separately reported and does not redefine the public contract.

`schemas/capacity-profile.schema.json` is the generated closed shape for environment replica,
pool, permit, queue, lease/heartbeat, scan, HPA, SLO and recovery inputs. A valid profile is only a
candidate input: production status additionally requires its exact digest in the L5 capacity gate
and passing L4-L6 evidence for the same deployment configuration.

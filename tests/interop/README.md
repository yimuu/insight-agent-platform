# MCP external SDK interoperability fixtures

The qualification fixtures pin two independent upstream SDK releases:

- TypeScript `@modelcontextprotocol/sdk@1.30.0`
- Go `modelcontextprotocol/go-sdk@91e4e1a0b8ca01cfa680f142815b1152a0513326`
  (stateless MCP branch)

They are intentionally outside the Rust dependency graph. The qualification
runner starts real child processes and covers stdio plus Streamable HTTP in
both platform-client and platform-server directions. Dependency lockfiles are
checked in; floating `latest` versions are forbidden.

The upstream SDK high-level clients still default to the session-era
`2025-11-25` handshake at these exact revisions. The fixtures therefore use
the SDK-owned JSON-RPC, MCP content/task types and validation primitives while
providing the stateless `2026-07-28` envelope adapter in fixture code. This is
reported separately from high-level SDK API coverage so a green test cannot
misrepresent an upstream capability.

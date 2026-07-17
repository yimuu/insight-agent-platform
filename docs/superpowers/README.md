# Design-document authority

The canonical authored DSL, compiler IR, scope runtime, and Operation boundary are defined only by [DSL vNext Region/SSA Design](./specs/2026-07-16-dsl-vnext-region-ssa-design.md), the top-level README, and executable tests.

Earlier dated specifications and plans are retained as historical decision records. Their original status records what was true when they were written; it does not make graph fields, `core.*` nodes, node events, or flat-scheduler behavior part of the current contract. Cross-cutting decisions such as PostgreSQL ownership, TLS, HTTP lifecycle, history privacy, and resource limits remain applicable only where they do not conflict with the canonical vNext documents and current code.

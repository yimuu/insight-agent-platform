# Compiled static framework source

This small echo graph uses the platform's explicit `langgraph_static_typed_ports_v1` export. Open this directory as the authoring project and import `agent.json`, the input/output schema files, and `framework.json`. The CLI and Console compile the preserved graph source with the same Rust core; Registry independently repeats that compilation when publishing.

The format is a deliberately limited, data-only export. It does not load arbitrary LangGraph Python, reducers, dynamic graph construction or a framework checkpointer. After lowering, the resulting platform nodes use ordinary Run durability. The neighboring `langgraph-reference` example wraps an entire external graph in a Capability, whose platform recovery boundary is the invocation.

Regenerate/check this source using `cargo run -p insight-platform-contract-tooling --bin product_authoring_examples -- --write` / the same command without `--write`. Publishing still requires the current tenant's authorized compiler profile and exact dependencies.

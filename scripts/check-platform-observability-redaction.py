#!/usr/bin/env python3
"""Reject high-cardinality or secret-bearing production tracing fields."""

import pathlib
import re

root = pathlib.Path(__file__).resolve().parents[1]
forbidden_fields = (
    "tenant_id",
    "principal_id",
    "resource_id",
    "run_id",
    "job_id",
    "task_id",
    "conversation_id",
    "server_id",
    "request_id",
    "invocation_id",
    "artifact_id",
    "worker_process_generation_id",
    "token",
    "secret",
    "prompt",
    "response",
    "arguments",
    "object_key",
    "url",
)
forbidden_format_fragments = (
    "generation={",
    "manifest={",
    "server_name={",
    "run_id={",
    "job_id={",
    "tenant_id={",
    "principal_id={",
    "token={",
    "secret={",
)
macro = re.compile(r"tracing::(?:trace|debug|info|warn|error)!\((.*?)\);", re.DOTALL)
failures = []

for path in sorted((root / "crates").rglob("*.rs")):
    if "tests" in path.parts or "examples" in path.parts or path.name in {"tests.rs", "build.rs"}:
        continue
    source = path.read_text(encoding="utf-8")
    production = source.split("#[cfg(test)]", 1)[0]
    for match in macro.finditer(production):
        body = match.group(1)
        fields = [
            field
            for field in forbidden_fields
            if re.search(rf"\b{re.escape(field)}\b\s*(?:=|,)", body)
        ]
        if fields:
            line = production.count("\n", 0, match.start()) + 1
            failures.append(f"{path.relative_to(root)}:{line} forbidden tracing fields {fields}")
    for fragment in forbidden_format_fragments:
        if fragment in production:
            line = production.count("\n", 0, production.index(fragment)) + 1
            failures.append(f"{path.relative_to(root)}:{line} forbidden diagnostic fragment {fragment}")

if failures:
    raise SystemExit("\n".join(f"observability redaction: {failure}" for failure in failures))
print("Platform production tracing redaction contract passed.")

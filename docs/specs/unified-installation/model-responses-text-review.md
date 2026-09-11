# Responses text fragments and optional observation

Status: jointly accepted by root and product, implemented, and the owning regressions pass.
No additional provider request is authorized by this change. Native K's one failed Run, frozen
package and publication remain intact.

## Evidence and owning boundary

K successfully connected, compiled and published. Its one ModelTurn dispatched, then failed with the
fixed adapter classification `openai_responses_invalid_field`. The exact rejected field and value
were not retained. This evidence does not establish an empty delta, empty terminal text, missing
identity, fingerprint, final-output schema mismatch or provider defect as the historical cause.
The public failed Run and quota readbacks agree with PostgreSQL: one request, 1,523 tokens and zero
cost units consumed, with all model reservations and tenant concurrency released. This is the
existing `Reconciled` conservative failure measurement: canonical input-token estimate plus maximum
output tokens, not observed provider token usage. Zero cost units is not proof of a free request.
Safe evidence is in the private K journey's `workspace-k/evidence` directory.

The [Responses adapter](../../../crates/adapters/platform-model-adapters/src/openai_responses.rs)
previously used the same nonempty-string decoder for identities, text fragments, terminal content
and complete function arguments. Its optional-string decoder was used only for the fingerprint.
The [normalized stream owner](../../../crates/domains/platform-models/src/stream.rs) requires
nonempty live deltas. The [response owner](../../../crates/domains/platform-models/src/types.rs)
validates the final response, schema, usage and optional observation. These owners should retain
their current semantics; wire fragments must not force an empty normalized delta or a fake answer.

The [OpenAI streaming reference](https://developers.openai.com/api/reference/resources/responses/streaming-events)
describes text and function-argument deltas as strings, without a documented minimum length. The
[Alibaba Responses reference](https://www.alibabacloud.com/help/en/model-studio/qwen-api-via-openai-responses)
likewise describes string fragments and explicitly documents empty text in a newly added content
part. Those initial parts are already ignored by this adapter. Accepting an empty incremental
fragment as contributing no content is an interpretation of the documented string contract, not
evidence that K received one. The reviewed Responses pages do not establish a guarantee that
`system_fingerprint` is present or that an empty value is a valid identified fingerprint.

## Accepted minimum mapping

- Separate identity strings from content strings. Existing required identity fields, including
  actual model identity and tool identifiers/names, remain nonempty and retain their current
  validation. Do not substitute a configured model identity for a missing observed identity.
- A consumed text/argument fragment must be a JSON string without NUL, but may be empty. Missing,
  null, numeric, boolean, object and array values still fail. Empty live fragments produce no
  normalized frame and do not advance normalized sequence/count/byte observations. The existing
  raw HTTP/SSE byte budget counts the entire input including such events.
- Apply this to visible text deltas and function-argument deltas. Validate the existing closed
  event keys, event type and tool association before discarding an empty fragment. An empty
  fragment cannot register a tool, bypass a projection, manufacture terminal output or turn an
  unknown item into a valid one. Keep the final function arguments subject to complete JSON and
  projected input-schema validation; an empty final argument string is not valid JSON.
- Permit empty terminal `output_text.text` parts to contribute zero bytes to the concatenation.
  A nonempty sibling can still provide the final response. Empty-only final content without a
  valid structured result or tool intent remains rejected by the existing adapter/response owner.
  Keep JSON parsing, schema validation, refusal handling and complete observed usage checks. No
  repair, truncation, fallback answer or success synthesized from deltas is permitted.
- Normalize absent, null or empty optional `system_fingerprint` to `None`, as an explicit adapter
  rule meaning no fingerprint was observed. Preserve a valid nonempty fingerprint unchanged and
  retain its owning length/control-character checks. Wrong JSON types and control-bearing strings
  fail. This does not loosen required actual-model identity or pinned-model matching. The existing
  domain already allows `None` and rejects `Some("")`; no domain change is needed. This rule does
  not claim that the official documentation promises empty fingerprints, or that K received one.
- Continue to discard hidden reasoning. Do not change unknown JSON/event handling, accepted
  output kinds, tool support, provider capability declarations, diagnostic payload visibility,
  response limits, usage quality or request/response ABI. Error classification may distinguish
  fixed identity/text/fingerprint categories, but may not expose arbitrary field names or values.

## Identity, safety and recovery

Advance only the OpenAI Responses mapping semantic identity from 2 to 3 in the existing
[protocol identity owner](../../../crates/foundation/platform-contracts/src/model_destination.rs).
Anthropic remains at 2 because its mapping is unchanged. This uses the existing per-protocol
digest authority, not a new compatibility path. A new executable/installed descriptor closure is
required; do not change K's exact records or replay its failed paid attempt.

PostgreSQL remains the sole ModelTurn, lease, quota, Receipt and event authority. There is no
schema, public DTO, transaction, role grant, provider retry, endpoint or credential change.
Fragments remain bounded by the existing raw response/deadline and normalized stream limits.
Discarding a zero-length fragment creates no new buffer, queue or unbounded allocation. Terminal
digest evidence continues to reflect the actual full received terminal object, including an empty
fingerprint or text part; normalization must not rewrite the evidence bytes to hide that difference.

## Required evidence before acceptance as implemented

1. RED the production codec with an empty text delta, empty known-tool argument delta and an empty
   terminal text part followed by valid content. GREEN must preserve only the real nonempty
   normalized output and complete terminal usage; empty fragments must not add live frames.
2. Exercise absent/null/wrong-type/NUL content, missing/empty required identities, unknown JSON
   keys, unknown tool association, empty final arguments and all-empty final output. They remain
   failures. Include structured output and tool projection checks so an empty fragment cannot
   bypass the real final owner.
3. Verify fingerprint absent/null/empty becomes `None`; a nonempty value is preserved, invalid
   type/control/over-limit values fail, and missing or mismatched pinned model identity still fails.
   Keep the full terminal evidence digest distinct when actual metadata differs.
4. Use the real brokered SSE adapter with synthetic events, including empty fragments across
   framing chunks, interleaved discarded reasoning and valid terminal usage. Verify one dispatch,
   no reconnect, retained raw-byte limits, unchanged nonempty frame sequencing and no hidden
   reasoning in normalized output. Empty events alone cannot produce success.
5. Verify the exact OpenAI identity advances while Anthropic stays unchanged and old OpenAI
   descriptors are rejected. Run the relevant adapter/domain tests, strict Clippy and boundary
   checks. These are mapping regressions, not proof that K's unknown historical field is fixed,
   nor a substitute for a separately authorized future actual ModelLoop.

## Implemented regression evidence

The five new production-adapter test groups failed on the old implementation in
`/private/tmp/insight-responses-text-red.log`. They cover empty text/tool fragments, empty optional
fingerprint, the closed error categories and actual brokered SSE normalization. After the mapping
change, the initial run passed four groups; the all-empty final-response case correctly returned
the existing host's `InvalidNormalizedResponse`, while the old success-only test helper unwrapped
it. The test helper now preserves that Result for negative cases; production response validation
was not weakened. That intermediate test failure is preserved in
`/private/tmp/insight-responses-text-green-first.log`.

All 49 adapter tests passed with no ignored tests in
`/private/tmp/insight-responses-text-all-green.log`. The actual brokered path receives synthetic
SSE byte by byte, discards empty and hidden-reasoning fragments, preserves the nonempty normalized
sequence and terminal usage, counts empty events against the raw cap, and rejects empty-only final
output or a missing terminal. The exact terminal digest remains sensitive to actual empty metadata
and content parts. Separate tests keep missing/wrong-type content and identities, unknown tools,
invalid final arguments, schema mismatch, invalid fingerprints and pinned-model drift rejected.
The existing structured-output test helper now binds its real schema digest consistently with the
separately reviewed response-contract check, rather than relying on an inconsistent fixture.

Foundation's 119 and Model domain's 16 library tests also passed with no ignored tests in
`/private/tmp/insight-responses-text-owners-green.log`. Model-worker deployment and crate-boundary
checks passed. Strict all-target Clippy for those three crates passed in
`/private/tmp/insight-responses-text-clippy.log`; formatting and diff whitespace checks passed.
These tests use synthetic provider content and do not identify the actual field
which caused K's failure. K remains a failed actual ModelLoop, with its conservative quota accounting
and public/PG agreement preserved independently from this mapping regression evidence.

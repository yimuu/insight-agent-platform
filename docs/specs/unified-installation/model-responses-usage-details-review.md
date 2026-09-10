# Responses billing-detail metadata

Status: jointly accepted by root and product, implemented, and owning regressions pass. No further
provider request is authorized by this change. Native M and its failed Run remain frozen.

## Evidence and scope

M's one public ModelLoop attempt failed after dispatch with the fixed classification
`openai_responses_unknown_field`. Its public and durable quota records agree: one request and
1,549 reconciled tokens, no remaining model reservation. These tokens are the existing conservative
input estimate plus output allowance, not observed provider usage. The historical rejected field
was not retained and cannot be inferred from that classification.

The separately authorized, synthetic structure diagnostic issued exactly one request. It observed
HTTP 200, one `response.completed` event, 8,959 raw bytes and 1,082 ms elapsed. It saved field names
only through a fixed whitelist or SHA-256, JSON types and emptiness; it saved no response text,
reasoning, IDs, token values, headers or credential. It is not a Platform qualification or replay
of M. Evidence is the private `/private/tmp/insight-qwen-responses-structure-m-20260910.json`; its
one-attempt script refuses to overwrite that report. No further provider call is needed for this
mapping review or its synthetic regressions.

Public candidate-name hashes identify the three extension names in that structure as `x_details`,
`x_billing_type` and `prompt_tokens_details`. The array contained an object with input/output/total
token integers, a nonempty billing-type string, cache-count metadata and reasoning-count metadata.
The diagnostic intentionally does not establish the actual billing-type value or numeric counts.
It also observed empty delta strings, supporting the previous fragment mapping independently of
K's or M's exact historical cause.

The [Alibaba Create response reference](https://www.alibabacloud.com/help/en/model-studio/qwen-api-via-openai-responses)
describes `x_details` as finer billing metadata and fixes its billing type to `response_api`.
It documents reasoning-count details and explains that the completed stream contains the same
Response object. Its [Retrieve response reference](https://www.alibabacloud.com/help/en/model-studio/retrieve-a-response)
also documents cache-count details in this array. These documented structures justify a narrow
recognized metadata extension; they do not justify accepting arbitrary extension keys.

The current [production codec](../../../crates/adapters/platform-model-adapters/src/openai_responses.rs)
rejects `x_details` at the closed top-level usage object. The
[ModelUsage owner](../../../crates/domains/platform-models/src/types.rs) and the
[declared usage contract](../../../crates/foundation/platform-contracts/src/model.rs) already
distinguish actual complete usage from provider guarantees. They need no change.

## Accepted minimum boundary

- Add one optional `x_details` member to this codec's accepted terminal usage keys. Absence retains
  the current mapping. Presence must be an array (including an empty array), never null or an
  object. Use a codec-owned finite maximum of 16 entries for this recognized metadata profile;
  this is a local parsing limit, not a claim about the provider's maximum.
- Each entry is a closed object with required unsigned integer `input_tokens`, `output_tokens`
  and `total_tokens`, and required `x_billing_type` exactly `response_api`. Two optional closed
  objects are supported: `prompt_tokens_details` containing only optional unsigned integer
  `cached_tokens`, and `output_tokens_details` containing only optional unsigned integer
  `reasoning_tokens`. Empty detail objects are accepted; explicit null, malformed numeric values,
  missing required fields, other billing types and unknown nested fields fail closed.
- This is the current text billing subset. Do not add provider built-in tool statistics, multimodal
  breakdowns, cache-creation fields or new capability claims as part of this correction. Those
  unselected extensions remain rejected, even where other provider modes document them.
- Validate these metadata objects and discard them. Do not sum entries, substitute their counts
  for the aggregate counts, infer monetary cost, or change cache/reasoning profile gates. The
  existing required aggregate input/output counts remain the sole normalized token accounting
  source. Missing aggregate usage still fails even if a complete detail entry is present.
- Preserve the original full terminal object when calculating response evidence. Adding recognized
  metadata changes that evidence digest, but cannot change normalized output, stream accounting,
  actual model identity, usage quality, quota charging or the request bytes.
- Classify invalid extension shapes with one fixed safe code, such as
  `openai_responses_invalid_usage_details`. Do not include an arbitrary field name or value in the
  message or diagnostic. Existing unknown fields outside this extension remain rejected.

The codec remains the single wire-mapping owner. There is no new provider-specific execution path,
backend registry, public DTO, persistent metadata, role grant, transaction or event. PostgreSQL
retains ModelTurn, lease, quota and failure settlement authority. Post-dispatch parse failures keep
their existing uncertainty/conservative accounting and cannot trigger a paid automatic retry.
The whole body/event still consumes its existing raw-response/deadline limits; the new validator
iterates at most 16 already parsed entries and builds no secondary payload buffer.

Advance only OpenAI Responses mapping semantics from 3 to 4 in the
[existing identity owner](../../../crates/foundation/platform-contracts/src/model_destination.rs).
Anthropic stays at 2. Rebuild the exact executable/descriptor closure for any later qualification;
do not mutate M, change an old installed profile or replay its failed Run. This does not change ABI,
endpoint, protocol version, model capabilities, normalized schema or the database schema.

## Required regression evidence

1. RED through the actual production codec with a synthetic documented detail entry, then GREEN
   with the same canonical output and aggregate observed usage as an otherwise identical response
   without the extension. Prove different full-terminal evidence digests and no double counting,
   including multiple entries whose counts deliberately differ from the aggregate.
2. Exercise absence, empty array, one entry, exact 16 entries and a rejected 17th entry; test each
   required field absent and null, each counter as negative/fraction/string/bool/array/object,
   incorrect billing type and unknown fields at every supported object depth.
3. Prove valid optional cache/reasoning objects do not override their aggregate/profile behavior;
   missing or malformed aggregate input/output still rejects rather than using details as fallback.
4. Run the same documented structure through the real broker SSE parser and host with arbitrary
   bounded chunks, one actual terminal and complete aggregate usage. No provider, credential or
   network call is required. Keep hidden reasoning absent from normalized output/diagnostics.
5. Assert OpenAI's new exact mapping identity independently and Anthropic's unchanged identity;
   run the adapter and affected owning suites, strict Clippy and existing boundary checks. These
   establish implementation evidence only; later real Platform success remains a separate claim.

## Implementation evidence

The production codec now applies the closed validator and its local entry bound before consuming
aggregate usage. Five new test groups exercise the actual codec and broker SSE path, including
one-byte chunks, hidden-reasoning suppression, no fabricated terminal, raw-response limits, both
usage declaration settings, profile-gated aggregate detail counters and complete terminal digests.
The independent mapping assertion requires OpenAI semantics 4 and rejects semantics 3; Anthropic
remains at 2 and continues rejecting its old identity.

The first test invocation caught a fixture return-type mistake before execution; that log is
retained separately. After fixing only that assertion, the original decoder rejected both an empty
detail array and a one-entry array in actual codec RED runs. The implementation then passed all
five new groups. The complete affected library suites passed 189 tests (119 Foundation, 54 Model
Adapters and 16 Models), with no ignored tests. Strict all-target Clippy for these three crates and
the Model Worker deployment and crate-boundary checks passed. The private evidence index and logs
use the `/private/tmp/insight-responses-usage-details-` prefix.

The separately authorized [Native N execution](deployment-review.md#native-n-model-and-business-restart-evidence)
then succeeded with one request, 1,159 ProviderReported tokens and a typed public result. Its
same-input/package business restart preserved that result and accounting. Provider cost remains
unknown. The original event-based harness failure is retained. The later
[O delivery](deployment-review.md#o-current-delivery-evidence) passed the actual Native ModelLoop,
complete public events and business restart after the repaired image build. It is a separately
authorized attempt, not a repeat of N's request or a rewrite of Native M's failure.

Root independently reviewed the implemented validator, retained terminal evidence, mapping identity
and behavioral tests and accepted the slice without blocking findings. These are synthetic parser,
accounting and host evidence, not a second vendor invocation or proof of a successful M ModelTurn.

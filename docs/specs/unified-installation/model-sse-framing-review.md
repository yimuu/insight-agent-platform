# Model provider SSE framing

Status: jointly accepted and implemented; the owning regression suite passed. Wider runtime
integration is pending the separate Context/Egress work. Native G's original Run and publication
are preserved. An independent provider diagnostic is owned by root and is not a G retry or Platform
qualification.

## Evidence and owner

G passed connection, compilation, HTTPS upload and publication. Its one ModelTurn reached dispatch
with the corrected response cap, then failed with `model_sse_unknown_field`. The exact persisted
attempt says `request_sent=true` and reconciled accounting. The actual field was not retained, so
this evidence does not identify the upstream field or prove a provider-model defect. Safe evidence
is beside the original report under the private `workspace-g/evidence` directory.

The previous common [Model SSE decoder](../../../crates/adapters/platform-model-adapters/src/provider_sse.rs)
accepted comments, `event` and `data`, but explicitly rejected even `id` in a test. It split
only on LF, stripped a preceding CR, and had no initial BOM handling. Its `finish` method could dispatch
data without the required blank-line delimiter. The real decoder regressions independently reproduced
these behaviors before the implementation changed; they are not conclusions inferred from G's unknown field.

Root's separate, authorized provider diagnostic returned HTTP 200 with identity-encoded SSE. Its
bounded 1,079-byte sample contained two each of `id`, `event`, `data`, comment and blank lines, using
LF. It dispatched once, did not retry, stopped early, and saved neither field values nor response
body; usage is unknown. The private report is
`/private/tmp/insight-qwen-sse-diagnostic-20260910.json`. It confirms standard `id` usage in that
sample, not G's historical field, a completed model answer or Platform qualification.

The [WHATWG SSE framing and interpretation rules](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream)
define UTF-8, one optional initial BOM, CRLF/CR/LF line endings, first-colon separation and one
optional leading ASCII space in a field value. `id` and `retry` control EventSource transport state;
an incomplete event at EOF is not dispatched. The platform is not a browser EventSource and must
not adopt its reconnect or Last-Event-ID behavior merely to parse these fields.

## Accepted transport-only rule

Keep one decoder shared by both installed model protocols. No provider-specific branch or new
business DTO is needed. Root and product jointly accepted the following precise profile before implementation:

- Recognize `id` and `retry` as discarded transport metadata, including empty fields. Do not
  parse an ID as a Platform resource, preserve it as evidence, log it, send it in a header or use
  it for replay. Do not convert `retry` to a duration or change any timer. Invalid/nonnumeric or
  arbitrarily large retry values have no scheduling effect. UTF-8 validation and the existing raw
  response/line limits still apply; a NUL-bearing ID is discarded without becoming an identity.
- Accept exactly one UTF-8 BOM at byte offset zero, including across arbitrary chunks. It still
  counts toward the raw response budget. Never remove a later BOM from a field or JSON body, and
  do not replace invalid UTF-8 with lossy characters.
- Parse CRLF, standalone CR and LF consistently across chunk boundaries. A CR ends a line; only
  its immediately following LF is consumed as part of the same delimiter. Keep no unbounded look
  ahead or new queue. Existing byte budgets count all input bytes, including discarded metadata,
  comments, BOM and both bytes of CRLF.
- Only a complete blank-line-delimited data event reaches the existing business decoder. EOF
  must not manufacture a delimiter or a terminal event. Pending event data or an incomplete field
  line fails closed; complete metadata/comment-only lines create no event. A partial BOM at EOF
  is invalid. The normal adapter still requires its valid protocol terminal response.
- Keep `[DONE]` distinct from a successful Model response. After its completed delimiter, only
  the pending LF of that delimiter's CRLF may be consumed; other subsequent bytes are rejected.
  This avoids different results when the CR and LF are delivered in separate chunks. No marker
  creates output, accounting or durable success by itself.
- Retain the existing stricter `event` behavior: duplicate event fields, invalid names, mismatch
  against data's `type`, duplicate JSON keys, malformed JSON and unsupported business fields are
  rejected. The accepted minimum retains rejection of other unknown SSE field names as well. This is
  an explicit platform SSE profile, not a claim to implement all EventSource behavior; broadening
  arbitrary transport-field handling would require an explicit joint decision.

The last two points close actual parser ambiguities rather than weakening semantic validation.
Discarded metadata must not appear in `ModelProviderWireEvent`, RPC envelopes, canonical output,
provider-response evidence digests or safe failures. The same retained event data must produce the
same semantic output regardless of metadata and newline layout.

## Identity, security and recovery

[The installed adapter identity](../../../crates/foundation/platform-contracts/src/model_destination.rs)
explicitly requires a new semantic version when the closed mapping changes. The accepted change advances
`wire_mapping_semantics` from 1 to 2 for both protocols because they share this parser. Canonical request,
response and RPC shapes remain unchanged. Rebuild the exact worker/installed-adapter closure;
do not rewrite G's old exact declarations or replay its failed paid attempt. No compatibility
path selects the old parser under the new identity.

PostgreSQL remains the ModelTurn/Job/lease/Receipt/accounting authority. The decoder has no database
access or authorization role. This change creates no schema, transaction, event, quota, retry,
provider connection, endpoint, TLS, credential or cancellation behavior. All existing deadline,
in-flight, raw-byte, RPC-frame and normalized-output limits remain. Error output uses fixed safe
classifications and never includes field names or values supplied by the provider.

## Required regression evidence

1. RED the real decoder with a complete synthetic valid event preceded/interleaved by standard
   transport metadata. After joint acceptance, the same bytes must pass while emitting only the
   original typed event; canary metadata never appears in the event or diagnostic.
2. Feed the same stream at every two-chunk split and byte-by-byte, for LF/CR/CRLF and initial BOM,
   including splits inside the BOM and between CR/LF. Compare emitted events and raw byte counts.
3. Verify repeated/empty/NUL IDs and valid, malformed and huge retry strings remain inert and
   bounded. Exceed the byte cap using metadata alone; it must still fail. Unknown field handling
   follows the jointly chosen profile, never an accidental fallthrough.
4. Reject duplicate JSON/type conflicts, invalid UTF-8, a second/midstream BOM used as a field prefix, incomplete fields,
   missing final event delimiters, and payload after DONE. DONE alone cannot normalize into a
   successful response; its legal CRLF split must be accepted without an invented extra event.
   A literal BOM inside a valid JSON string remains data and is never stripped.
5. Use the real brokered SSE connector and both production protocol adapters with synthetic valid
   terminal responses. Added framing metadata must preserve canonical output/usage/request digest,
   and malformed business content must remain rejected after the request. Retain one dispatch,
   no automatic reconnect and the existing failure-accounting guards.
6. Check the new installed mapping identity and reject the prior digest under the new adapter
   descriptor. Run adapter/owner/RPC regression suites, strict Clippy and boundary checks. Only a
   separately authorized, new immutable installation and one new Run can establish a successful
   actual ModelLoop; fixtures and root's direct SSE diagnostic cannot establish that qualification.

Root accepted the EOF, unknown-field, DONE/chunk and semantic-identity rules before implementation.
The independently observed vendor field remains separate evidence and cannot retroactively identify
the bytes of G's failed response. A new platform invocation still requires separate authorization.

## Implemented evidence

The old real decoder passed its four existing tests and failed all four new behavior groups in
`/private/tmp/insight-model-sse-red.log`: standard metadata, BOM/newlines, invalid UTF-8 in discarded
comments, and EOF event invention. The updated decoder passed the same cases. Its additional
tests cover mixed newlines, empty data lines, all two-chunk splits, byte-by-byte input, byte limits,
fixed safe errors, strict business JSON and DONE framing.

The production brokered connector and both protocol adapters pass with synthetic terminal
responses. Adding metadata/BOM and each newline form preserves the complete canonical response,
usage, provider-response evidence, stream evidence and canonical request digest. Each fixture
records exactly one broker open. Truncated and malformed events still fail; DONE-only preserves
the existing missing-terminal failure classification and does not reconnect inside the adapter.
Both constructors reject the independently reconstructed prior semantic digest before I/O.

`/private/tmp/insight-model-sse-owning-tests.log` records 180 passing tests across Foundation,
Model Adapters, Models and Registry, with none ignored. Their all-target strict Clippy passed in
`/private/tmp/insight-model-sse-owning-clippy.log`; the dependency boundary scan and local document
links also passed. The broader run passed the three real SSE/RPC capacity tests but its local
mTLS fixture hit the sandbox's listener restriction. The authorized retry and broader Clippy then
met the separately changing Context/Egress source before tests could run. Those broader logs are
not evidence of a complete current-runtime pass and must be rerun once that work is stable.

No test here calls a real model provider or changes G, its frozen declarations, its failed Run or
its accounting. These results establish the parser and mapping behavior, not a successful actual
ModelLoop or complete installation qualification.

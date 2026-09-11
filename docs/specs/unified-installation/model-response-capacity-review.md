# Basic model response capacity

Status: jointly accepted and implemented; local contract regressions and the
[O Native ModelLoop/business restart](deployment-review.md#o-current-delivery-evidence) passed.
O also verified the repaired public-event drain; earlier failures remain unchanged. Native F and its single failed
Run remain immutable evidence. The later [node-response-schema repair](model-node-response-schema-review.md)
uses each ModelLoop's own object schema; it does not replace the byte limits reviewed here.

## Actual failure and owning path

F passed public connection, online compilation, HTTPS Artifact upload and Agent publication. Its
ModelTurn was admitted and claimed, but the production ModelWorker rejected it before dispatch
with `model_output_too_large`. The persisted attempt records `request_sent=false` and zero usage.
This failure is not a provider response or evidence about Qwen's capabilities. The private safe
diagnosis is retained beside the original `workspace-f/evidence/report.json` under
`/private/tmp/insight-native-cli-model-review-20260910`.

[Registry's basic source producer](../../../crates/definitions/platform-registry/src/model_configuration.rs)
previously declared a response-byte cap equal to the current Inline hard limit. The production
[Inline materializer](../../../apps/services/platform-model-worker/src/lib.rs) additionally reserves
space before dispatch, so that default can never pass its preflight. Runtime takes the Inline
limit from the checked-in [HardLimitProfile](../../../crates/foundation/platform-contracts/src/limits.rs)
through [ModelTurnLimits](../../../crates/domains/platform-models/src/lib.rs), not its smaller Q1
default. The request's actual response size is unknown at this point.

The complete path has distinct, retained bounds:

- [Egress HTTP](../../../crates/adapters/platform-egress/src/lib.rs) rejects oversized declared
  content length and counts actual body bytes with checked arithmetic. Cancellation releases the
  existing in-flight permit. The [SSE decoder](../../../crates/adapters/platform-model-adapters/src/provider_sse.rs)
  also limits accumulated bytes and strict JSON event data.
- [Egress RPC](../../../crates/protocols/platform-egress-rpc/src/lib.rs) transports each typed
  `ModelStreamFrame::Event` as canonical JSON metadata, with digest, shape, byte and message checks.
  Its payload allowance does not increase the metadata allowance. The shared installation renderer
  previously gave both Egress and ModelWorker a metadata limit below the supported basic structured
  output size, so a valid large terminal SSE event can fail here independently of the F failure.
- [Adapter normalization](../../../crates/adapters/platform-model-adapters/src/provider_wire.rs)
  counts encoded event bytes. [CanonicalModelResponse](../../../crates/domains/platform-models/src/types.rs)
  separately validates actual canonical response size against the provider and ModelTurn limits,
  and checks message, structured output, tools and actual usage.
- The production Inline materializer checks the complete resulting `ValueRef` with actual Inline
  JSON limits before allocating output identities. Neither raw-wire size nor preflight acceptance
  replaces that final check. Normalized content may contain both message text and parsed structured
  output, so a fixed reserve is not proof that every accepted wire response will fit.

## Accepted single-owner change

Move the existing Inline response reserve and its checked capacity calculation into the
[foundation Model contract](../../../crates/foundation/platform-contracts/src/model.rs). This is a
pure shared budget rule, not an output store, capability declaration or new durable state. Accepted
interface: `inline_model_provider_response_capacity(inline_hard_max: u64) -> Option<u32>`. It returns
the positive capacity left after the existing reserve, capped by the existing maximum provider
response size; insufficient space returns `None`. No unchecked subtraction, addition or narrowing
is permitted. The reserve has one definition; neither Registry nor the worker repeats its number.

Registry computes its default from the checked-in hard profile through that helper, retaining the
existing desired response cap as an upper bound. The worker applies the same helper to its actual
Inline limit before comparing the immutable provider declaration. At today's bounds the source
default becomes 983,040 bytes. The existing preflight failure classification and actual Inline
validation remain. Other provider limits, including the stream-delta limit, remain valid under the
new default and are independently validated. This does not modify tools, usage guarantees, token
limits, model identity, parser tolerance or provider wire behavior.

Jointly accepted physical transport change: move the existing
`MAX_EGRESS_METADATA_BYTES_HARD` definition into foundation contracts, with the RPC module using
that same constant. The shared renderer can then directly use this owner for only its Egress server
and ModelWorker client configurations. Set those two metadata allowances to the existing hard
bound. Keep all other clients, payload allowances, hard limits, RPC version, canonical encoding,
digests and message validation unchanged. There is no second metadata-limit registry and no new
dependency from deployment tooling to a transport implementation.

The runtime Model stream uses demand-driven `unfold`; this change adds no application queue or
prefetch. Increasing the metadata allowance raises the maximum encoded metadata per frame by
960 KiB. Egress's current model in-flight limit and worker concurrency remain unchanged, as do the
bounded live-delta buffers. Sixteen simultaneous frames can therefore allow an additional 15 MiB
of encoded metadata per representation; decoding, copies and transport buffers make this neither
an RSS estimate nor a total memory bound. The Egress server setting applies to its shared endpoint,
not just Model messages; other authorized clients retain their smaller limits. Existing role,
current authorization, per-operation capacity, timeout and cancellation checks remain mandatory.

## Identity, persistence and recovery

The generated source declaration and subsequent immutable Provider revision naturally receive new
content digests. ModelWorker's rebuilt executable is bound by the existing worker identity. This
capacity slice changes no Plan IR, compiler/model protocol semantic version, database table, schema
version, public DTO, quota algorithm, Receipt, event or transaction boundary. Foundation relocation
does not introduce alternate decoders or legacy behavior.

An old exact Provider declaration is not rewritten, and the worker must still reject declarations
that cannot fit its actual preflight budget. F's original attempt and terminal Run are not retried.
After accepted source changes and tests, any actual model verification uses a new immutable
installation, newly published dependencies and one separately authorized Run. Pre-dispatch failure
retains zero sent/usage accounting; post-response materialization failure keeps the original
conservative accounting and must not automatically replay paid provider I/O. There is no truncation
or implicit Artifact promotion in this Inline-only materializer; implementing another output store
would require its own owner review.

## Required evidence before implementation is accepted

1. Add a RED regression that obtains the real `basic_provider_request_limits()`, inserts it into a
   valid typed execution fixture and calls the production `InlineModelOutputMaterializer`
   `validate_execution`. Do not use a fake materializer or restate the desired number in place of
   the producer. After the owner change, the same test is GREEN for both installed protocols.
   A Registry dev dependency in the worker test target is sufficient; the boundary checker must
   allow only that dev edge, never a new shipped runtime dependency.
2. Check the shared arithmetic at insufficient/zero space, exact reserve, first positive capacity,
   current profile and maximum integer input. At the worker boundary the exact derived cap passes,
   one byte more fails before dispatch, and malformed excessive declarations remain rejected.
3. Run real production materialization for a valid small normalized response and independently
   oversized canonical output. Preserve `model_output_too_large`, `request_sent=true` and no output
   identity allocation on the latter. Keep domain/schema/token/usage rejection tests unchanged.
4. Exercise the actual SSE decoder and production RPC metadata encoder/decoder with a terminal
   event larger than the old allowance, including the basic profile's supported structured-output
   size. The old renderer-sized limit must reject it; the reviewed setting must round-trip the
   exact canonical event and identity. At-limit and over-limit frames, digest tampering, wrong
   operation/schema and strict JSON failures must still fail closed. Synthetic frames prove the
   transport contract, not provider or full ModelLoop qualification.
5. Renderer tests independently assert the two selected metadata limits match the shared owner,
   while other role settings and all concurrency/buffer bounds remain unchanged. Run focused
   owner/Registry/worker/adapter/RPC tests, strict Clippy and the dependency boundary checker.
6. Keep concise accepted ADR/current explanations of bounded basic output and preflight versus
   actual-byte checks aligned with the implementation. Actual installation/ModelTurn acceptance remains a
   separate gate, including final typed output and durable model evidence; no success is inferred
   from these local tests.

Root owns the renderer and its tests. Product owns the shared capacity contract, Registry default,
worker/RPC regressions and this review. Root accepted the shared constant placement, complete
capacity path and per-frame memory change before implementation. No new provider invocation is
authorized by this document alone.

## Local evidence

The real Registry default, a valid typed execution and the production materializer reproduced F's
exact pre-dispatch failure before the change; the unchanged regression passed afterward. Worker
tests also verify the exact cap, one-byte excess, invalid limits, actual normalized content/digest,
oversized Inline rejection and no value-identity allocation on that failure. The real SSE decoder
and RPC encoder/decoder round-trip a terminal response with the basic structured-output size;
the old metadata allowance rejects it. The exact hard-bound frame passes, while one-byte excess,
altered identity/digest, duplicate keys and unknown fields remain rejected.

The six affected owner/consumer libraries passed 195 tests. The existing real-mTLS-NATS test remains
explicitly ignored in that command because it requires its separate physical qualification harness;
these tests do not replace it. Six-crate all-targets strict Clippy, the Platform contract checker,
the complete resolved dependency scan and `git diff --check` passed. Mutating the new Registry test
dependency into normal, build or mixed dev/normal kinds made the actual boundary checker reject it.
Root's shared-renderer fixture passed and its two production changes were independently reviewed.

Logs are retained under `/private/tmp/insight-model-response-`: `capacity-red.log`,
`capacity-green.log`, `capacity-tests.log`, `rpc-tests.log`, `all-tests.log`,
`owner-clippy.log`, `boundaries.log`, `dev-edge-negative.log` and `contract-check.log`.
The renderer test is recorded in `renderer-test.log`. A combined Clippy command including deployment
tooling encountered the independently edited public-trust owner's SHA formatting error; it is not
reported as passing by this slice. Root/Ops retain responsibility for that separate source seal.
No provider invocation, successful G ModelTurn or production capacity qualification is claimed.

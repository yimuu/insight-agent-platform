# Remote Context response capacity and uncertain RPC outcomes

Status: jointly accepted; owning outcome semantics, RPC/Worker consumers and fenced PostgreSQL
settlement are implemented and have passed their targeted regressions. Full public provider and
document-review business qualification remain separate.

The Remote Context RPC carries the complete normalized result in metadata. Previously the local client
used 64 KiB although the physical HTTP grant could permit 1 MiB. Normalization adds digests and the
Worker adds citation and observation fields, so raising the client alone cannot guarantee delivery.
An RPC failure after submission also incorrectly reported a before-dispatch failure.

Use the existing 1 MiB metadata owner for the installed Context client. Keep the physical HTTP,
complete RPC frame and final Inline value bounds independent; raw bytes do not reserve or promise
space for subsequent representations. Check the real serialized representations instead of an
estimated per-item expansion. Never truncate, silently promote to Artifact, or replay HTTP to fit.

The server encodes the complete existing outcome. If a successful connector result cannot fit,
return a bounded permanent after-dispatch failure binding the actual request and normalized response
digests with the capacity-rejection stage. It must not disappear into an RPC status that implies
the provider was never contacted. The Worker validates the full observation Inline value before
attempting success settlement. Invalid or oversized output goes through the existing permanent
failure and fenced quota settlement, rather than abandoning a Running Job for lease recovery.

Local request validation and encoding failures remain before dispatch. Once the client submits the
RPC, transport errors, timeout and invalid result envelopes have uncertain provider dispatch status.
Add one closed `UncertainDispatch` transport failure class with evidence of the actual RPC submission,
explicitly not proof of HTTP dispatch. The Worker must not automatically retry that class; it uses
the existing permanent-failure outcome. A closed diagnostic formatter records only the enumerated
failure stage and actual evidence SHA in the existing safe failure message. Untrusted upstream
messages, query text and URLs never enter that diagnostic. It is explanatory text, not a value to
parse back into business state. Existing Query failure storage and Job failure digest preserve it;
the failure digest still hashes the complete owning Failure, not the raw response. No fake Artifact
reference is created to carry evidence.
The unary RPC deadline is bounded by the admitted query deadline. No transport retry is introduced.

This changes internal outcome semantics: the Remote outcome operation advances to v2 and its worker
capability binds outcome semantics v2 alongside the already reviewed execution/authentication v2.
External HTTP request/response and mapping remain v1. No public API, persistence field, table,
physical schema, new quota account or business state is added. Existing Query/Job/CAS and PostgreSQL
quota settlement remain authoritative. Cancellation and failures after the outcome transaction
continue to use the existing first-winner and lease recovery rules.
If the admitted deadline has already elapsed, the existing timeout owner may win instead; an RPC
timeout does not grant permission to override that deadline or promise a permanent-failure commit.

Required evidence: a normalized result larger than 64 KiB through the real RPC encoder/decoder;
exact complete-frame bound and one-byte overflow; overflow becomes a bounded known after-dispatch
failure; post-submission invalid/status outcomes remain uncertain and cannot select automatic retry;
an observation whose content alone fits but full Inline value exceeds its limit is explicitly
rejected and settled without output. Preserve actual TLS, current authorization and old capability
rejection tests. These tests do not qualify a deployed document provider or a human response.

The complete-frame codec tests and Worker tests passed, including after-dispatch capacity failure,
uncertain submitted RPC outcome and complete Inline observation limits. The real PostgreSQL fixture
now exercises ordinary orchestration defer, actual Context claim and permanent failure settlement:
the original Query is consumed, the safe stage/evidence digest is retained, the Job hashes the
complete Failure, reservations are released with query usage accounted and no result bytes, and
replay creates no duplicate accounting. The deadline owner still governs an already expired Query.
Installation producer/consumer tests use the existing metadata limit without promising that a
maximum-size HTTP response can fit every later representation.

# Durable Model events and terminal watch

Status: jointly accepted by Root, Operations and Noether after independent registry, schema,
transaction, public projection, CLI and Model lock/terminal-path review. The PostgreSQL and CLI
corrections are implemented, source-sealed and locally verified, including real PostgreSQL/HTTP
regressions and strict Clippy. O's final image build, separate Native public CLI Run and same-package
business restart also passed their bounded checks. Current-image Compose/Kind and the remaining
external/browser scenarios are not qualified by that result. Product owns PostgreSQL, Root owns
CLI, and Operations owns this review/documentation.
Historical-event repair is excluded.

## Observed defect and existing authority

Native N's actual Qwen Run and its unique ModelTurn succeeded with one provider request and
1,159 ProviderReported tokens. The acceptance harness failed because it did not receive public
Model start/completion events. That observation is not a failed model invocation. N's events,
receipts, installation and provider attempt remain unchanged. The repair now has fresh local
regression evidence; a later explicitly authorized physical acceptance attempt remains separate.

The later [N supplemental and restart evidence](deployment-review.md#native-n-model-and-business-restart-evidence)
also confirms the typed public result and unchanged business state after controlled same-package
restart, without another execution request. The original failed report remains intact; none of
these readbacks qualifies the repaired event path in a new physical installation.

The [existing public event registry](../../../contracts/platform-v1/events/public-run-events.json)
and its [owning enum](../../../crates/foundation/platform-contracts/src/registry.rs) already declare
the durable ModelTurn projections. Actual claim and settlement append Model events in the same
PostgreSQL transaction as their owning state. The
[storage mapper](../../../crates/adapters/platform-postgres/src/repository.rs) previously omitted
ModelTurn, so those rows received internal visibility and no Run public sequence. The
[public reader](../../../crates/adapters/platform-postgres/src/repository/run_queries.rs) reads
the committed `events` table directly under a read-only repeatable snapshot. History delivery
latency is not the cause.

The current [schema](../../../crates/adapters/platform-postgres/schema.sql),
[ADR-0009](../../adr/0009-durable-kernel-and-agent-domain-boundaries.md),
[ADR-0010](../../adr/0010-unified-installation-and-model-configuration.md) and
[current API](../../current/api.md) remain the upstream boundaries. This repair adds no public
event type, DTO, table, migration, stream, sequence authority or asynchronous projection.

## Implemented repair

The slice contains four changes: the existing durable Model projection, its explicit command
Run association, the actual Run/Node terminal payload field, and the two CLI watch read orders.

Only for the existing `model_turn` aggregate, parse the existing `PublicRunEventType` and require
its owning durable source kind to be ModelTurn. Preserve the explicit mappings for other
aggregates. Live-only deltas, internal retry/control progress, unknown names and a wrong aggregate
must remain unprojected. Do not activate a generic fallback for all event sources.

The Model control command is a second necessary call-site correction: it previously used
`append_command_event`, whose implicit Run binding is only available for a Run aggregate. It now uses
the existing explicit-Run append path with the verified next ModelTurn's Run ID and version.
The Model owner supplies this relationship inside its original locked transaction; no generic
aggregate lookup, payload inference or new business authority is introduced.

Independent inspection also found that both direct orchestration terminal events and
[convergence events](../../../crates/adapters/platform-postgres/src/repository/convergence_commands.rs)
store `terminal_state`, while their previous mapper read `state`. The direct helper and the convergence
`emit` helper are the only current producers of the four affected Run/Node terminal event types;
both were read. Map these types using only their owning `terminal_state` field, with no
alternate-field fallback. Root includes this correction in the bounded slice. It is not asserted
as an observed N cause without a separate read-only examination of N's stored metadata.

## Transactions, security, capacity and recovery

Model claim, settlement, cancellation and control acquire the existing quota and parent locks
before ModelTurn/Job mutation. The Run sequence increment must reenter that already held Run
lock, never introduce a Job-to-Run lock acquisition. The cross-review includes recovery and Run
convergence call sites, not only the success path. Sequence assignment, Event, outbox, owning
state, quotas and Receipt remain atomic in the original transaction/savepoint. Replaying the
original completed command must not allocate another sequence or duplicate the event.

The public API constructs `DurablePublicRunEventData` from the checked source kind, nominal ID
and projection version; its summary remains absent. Event/Run IDs, sequence, trace and time
retain their existing envelope. Stored Model payloads, provider text, RunValue bodies, quota IDs,
usage details, credentials and signed object URLs are not copied to the public stream. Current
principal and RuntimeRead checks remain mandatory on every page.

This projects existing event rows rather than generating additional work. Run-local serialization,
the existing bounded page, safe integer validation, retention floor and signed cursor rules
remain unchanged. A history gap or revoked permission remains an explicit error. No backfill,
renumbering, visibility rewrite or provider replay repairs N or another retained installation.

## CLI terminal observation

Both [watch functions](../../../apps/insight-cli/src/run.rs) previously fetched an event page before
reading Run state. A terminal commit between those reads could make a non-full old page appear
complete. This was a separate code defect, not evidence for N's mapper failure.

Read the current Run before fetching that iteration's page. Finish only when that Run was
already terminal and the later page is not full; otherwise preserve the opaque cursor and
continue under the existing timeout. A full page always requires another iteration. The proof
depends on owning terminal convergence: execution members and external obligations must have
settled before the Run becomes terminal, and their events commit atomically with those facts.
The inspected Model claim gate permits only Running/Waiting parents; result, cancellation and
control paths reject a Run with `terminal_at`. Convergence holds the Run lock and waits for its
nonterminal members and active work to clear. Receipt replay does not append another event.
Consequently these Model events cannot newly commit after an already committed terminal Run.
The Run's terminal event is not necessarily the last sequence in its own transaction: the
subsequent page must drain all later events too, rather than stop on that event type.
This is a bounded durable-prefix drain, not a promise to wait for future unrelated audit work.
No new watermark, DTO, automatic cursor reset or retry of a failed provider request is needed.

## Acceptance checks

- Mapper tests cover every existing durable Model event and reject wrong-source, live-only,
  internal-progress and unknown events. Direct and converged terminal payloads independently
  prove their exact mapping and reject the obsolete `state`-only field shape.
- A real PostgreSQL claim and settlement expose ID-only public Model events in increasing Run
  sequence; actual command cancellation/timeout also carries the correct Run. Readback uses the
  ordinary principal-aware public reader. Wrong principal/current permission remains denied.
- Existing same-command Receipt replay leaves sequence, Event/outbox counts and quotas unchanged.
  A rejected/rolled-back operation leaves no sequence increment or partial event. Concurrent
  same-Run Model work and control exercise the real lock order with bounded completion.
- Real HTTP CLI fixtures cover both watch functions when terminality changes after the first
  page, prove the following page is drained with its original cursor, and retain full-page,
  cursor-error and journal recovery behavior. No fixture needs a real model request.
- Owning targeted tests and strict Clippy pass; later physical Run acceptance is recorded
  separately. The existing successful provider result in N is preserved as its own evidence.

## Recorded local evidence

The PostgreSQL index is `/private/tmp/insight-model-public-events-evidence-index.json`; the CLI
index is `/private/tmp/insight-cli-terminal-drain-evidence.json`. All indexed current source hashes
and the CLI's indexed log byte digests were independently checked. Neither suite made a provider
request or changed a retained installation's events.

PostgreSQL mapper tests demonstrated the original omissions, then both passed. The real isolated
PostgreSQL target passed in 9.80 seconds with no skipped test in that target. It exercised all
existing durable Model types through actual claim, settlement and control, ordinary current-principal
two-item pages, source/Run/version association, and direct/converged Run/Node terminal events.
Completion/control contention completed within its bound. Cancellation, real in-flight timeout,
early timeout, stale fence and deliberate rollback retained their owning semantics. Receipt replay
and rejected transactions preserved exact sequence/event/outbox/receipt/ledger/quota snapshots;
revoked and unrelated principals were denied. The successful fixture was exactly cleaned up.

The focused local PostgreSQL library run passed 26 tests, with one pre-existing explicit corpus
fixture ignored and nine unrelated dedicated-database tests filtered; this is not a claim that
every PostgreSQL target ran. Strict all-target Clippy and formatting passed. Genuine mapper,
public-reader and missing-Run-association RED evidence is separated in the index from test import,
mutation-ID, lease/deadline and finite test-quota preparation failures. Six failed fixtures were
cleanly stopped with their original data and logs retained.

The CLI's actual old-code race produced two records instead of three. After the read-order repair,
the complete CLI library passed 180 tests with none ignored in 15.58 seconds; strict all-target
Clippy and formatting also passed. HTTP tests cover both ordinary and journaled watches, a short
page followed by terminal commit, an already-terminal first read followed by a full page and its
129th event, and a second watch using the preserved cursor. Independent review corrected an initial
full-page fixture that did not actually start terminal. An earlier parallel-suite test-only
three-second timeout is retained separately; only that fixture's observation window was increased,
not the production timeout or cursor/error behavior.

These local results do not turn N's original harness report green. The separate
[O delivery evidence](deployment-review.md#o-current-delivery-evidence) now records the final image
build and a real successful Native CLI ModelLoop with nine consecutive public events. The Node
event after Run completion was drained before watch terminated, then read unchanged after the
same-package business restart. This closes the actual Native event/watch acceptance without
rewriting N or transferring that qualification to a current-image Compose/Kind or browser journey.

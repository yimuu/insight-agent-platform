# Remediation Status Synchronization Design

Date: 2026-07-12

Status: Design approved in conversation; awaiting written spec review.

## Context

The repository has two historical review documents:

- `docs/reviews/2026-07-11-stable-baseline-review.md`
- `docs/reviews/2026-07-11-dependency-governance-review.md`

Those files were written as dated audit snapshots. Since then, the codebase has implemented the stable-baseline remediation milestones A0-A8 and dependency-governance groups R0-R6. The original review text still describes many findings and roadmap entries as open, so reading those files directly can now produce the wrong operational conclusion.

The status synchronization work creates a current-state entrypoint without rewriting the historical review evidence.

## Goals

1. Add one current remediation status document at `docs/reviews/2026-07-12-remediation-status.md`.
2. Preserve the two 2026-07-11 review files as historical audit snapshots.
3. Add a small cross-reference at the top of each 2026-07-11 review pointing readers to the current status document.
4. Record which roadmap items are implemented, intentionally deferred, or still open for verification.
5. Separate confirmed fixes from still-open Needs verification items.
6. Record that current local `main` is ahead of `origin/main` until the pending commits are pushed.

## Non-goals

- Do not change production code.
- Do not change dependencies or the lockfile.
- Do not reclassify a Needs verification item as a confirmed defect without new evidence.
- Do not rewrite the original 2026-07-11 review conclusions, evidence, severity counts, or roadmap tables in place.
- Do not mark R7 as completed; it is a future SQLx upgrade gate, not a current remediation requirement.
- Do not suppress cargo-deny duplicate warnings or cargo-audit output.

## Chosen approach

Create a new status index document and link to it from the older reviews.

This approach keeps the audit trail intact while giving maintainers one accurate starting point for the current repository state. It avoids editing old findings into a hybrid document that is neither a clean snapshot nor a current status report.

## Status document structure

`docs/reviews/2026-07-12-remediation-status.md` should contain:

1. **Scope and date**
   - State that the document reflects the repository state on 2026-07-12.
   - State that it supersedes the operational status of the 2026-07-11 reviews but does not replace their historical evidence.

2. **Repository state**
   - Record the branch state at the time of writing.
   - Record that R6 commits are local if the branch is still ahead of `origin/main`.

3. **Stable baseline status**
   - A table for A0-A8.
   - Each row includes milestone, original finding IDs, current status, evidence commits or docs, and remaining notes.
   - A0-A8 should be marked implemented when the corresponding work exists in commits/specs/tests/docs.

4. **Dependency governance status**
   - A table for R0-R7.
   - R0-R6 should be marked implemented when current code/dependency evidence supports closure.
   - R7 should be marked future gate / not executed.
   - Rows should include evidence such as manifest state, `cargo tree` checks, or specific commits.

5. **Residual duplicate dependencies**
   - Record that duplicate dependency warnings still exist.
   - Explicitly identify accepted residual paths for `sha2 0.10/0.11` and `thiserror 1/2`.
   - State that duplicate warnings are not used as a suppression policy and require separate value/risk analysis before further work.

6. **Open Needs verification**
   - Keep Needs verification separate from confirmed findings.
   - Preserve the stable-baseline Needs verification list as open unless already covered by a dedicated implemented milestone.
   - Keep dependency Needs verification items that remain meaningful after current code state; mark stale ones as superseded by completed implementation evidence instead of deleting them silently.

7. **Recommended next actions**
   - Push pending local commits.
   - Optionally run a focused status-refresh review after push.
   - Select the next engineering milestone from Needs verification or R7 rather than revisiting already closed A0-A8/R0-R6 items.

## Cross-reference design

Add a short note near the top of each historical review:

```markdown
> Current remediation status: see `docs/reviews/2026-07-12-remediation-status.md`.
> This document remains a dated audit snapshot and should not be read as the current open-work list.
```

Do not edit the original finding text beneath that note.

## Evidence rules

The status document may reference:

- Commit hashes from `git log --oneline`.
- Existing design and plan documents under `docs/superpowers`.
- Current manifest/dependency state in `Cargo.toml` and `Cargo.lock`.
- Current dependency graph checks such as `cargo tree -i paste`, `cargo tree -i serde_yaml`, `cargo tree -i cel@0.14.0`, `cargo tree -i yaml_serde@0.10.4`, `cargo tree -i sha2@0.10.9`, `cargo tree -i sha2@0.11.0`, `cargo tree -i thiserror@1.0.69`, and `cargo tree -i thiserror@2.0.18`.
- Recent full verification gates when already run in the current remediation session.

The document should not claim a verification command passed unless the command was actually run and observed in this session or is clearly cited as historical evidence from a committed review.

## Verification design

Because this is documentation-only work, required verification is:

- `rg -n "2026-07-12-remediation-status" docs/reviews/2026-07-11-stable-baseline-review.md docs/reviews/2026-07-11-dependency-governance-review.md`
- `rg -n "paste|serde_yaml|cel-interpreter|cel =|yaml_serde|sha2|thiserror|R7|Needs verification" docs/reviews/2026-07-12-remediation-status.md`
- `git diff --check`
- `git status --short --branch`

If the implementation edits Markdown tables heavily, also visually inspect the changed sections with `sed` to catch malformed rows.

## Acceptance criteria

The work is complete when:

1. `docs/reviews/2026-07-12-remediation-status.md` exists and is the current status entrypoint.
2. Both 2026-07-11 review documents link to the new status document near the top.
3. A0-A8 and R0-R6 have explicit implemented/current-state rows with evidence.
4. R7 is clearly marked as a future SQLx upgrade gate.
5. Remaining Needs verification work is listed separately from completed remediation.
6. Historical review findings are preserved rather than rewritten in place.
7. Documentation-only verification commands pass.

# Prototype A progress

- [x] Plan written (`implementation-plan.md`).
- [x] RED: `ex01_three_checkpoints` fails to compile (`red-ex01.log`: no `eligibility` module).
- [x] GREEN: Clock mask (`contains` vs `covers`), `ClockRange::predates` → structural, `change_graph::{op_range,in_ancestry,iter_hashes}`, `eligibility::{evidence,session}`, `PatchLog::scope`, `inner.rs::exid_to_obj` structural check. `green-ex01.log`.
  - Fixture bug found: `set_author` resets the actor; fixtures must set author before actor.
  - Design fix: `scope_for` only requires classification of changes within the view's heads ancestry (before-scope compiles on post-import graph, ruling 5).
- [x] EX-01 1,920 schedules (120 orders × 16 partitions), duplicates, E2-before-E1, status-only variant, evidence-only status stream: green (`green-ex01-schedules.log`).
  - Test bug found: comparing `Debug` of `HashMap`-backed hydrate maps is order-unstable; canonical `flat()` used instead.
- [x] EX-02 (+ boundary-edits-restored-object variant), EX-03 (late A2, non-prefix capture reread, invalidation contrast): green on first run (`red-ex02-ex03.log` shows they passed without further production change — the evaluator was already general enough; recorded honestly, not as a red).
- [x] EX-05/06/07: green on first run (`red-ex05-07.log`). A-C01 falsifier executed: reverting `ClockRange::predates` to `covers` makes EX-07 replay produce an empty restored section (`red-ex07-predates-covers.log`); restored to `contains`.
- [x] Failure atomicity and actor-table recompilation tests: green (`green-all-fixtures.log`).
- [x] Full `cargo test --locked --offline -p automerge --lib --tests`: all green, 1 ignored (`full-suite.log`). rustfmt clean.

## Fix wave 1 (review round 1)

- [x] RED (compile): new tests for findings 1–3 + EX-10 against missing API (`fix-1-red-compile.log`).
- [x] RED (behavioral): EX-10 self-diff probe on pre-fix predicate emits `Increment -100` (`fix-1-red-ex10-counter.log`) → `visible_before` now `before.covers`; `predates` stays `before.contains`, docs corrected → GREEN (`fix-1-green-ex10-counter.log`; probe file removed after recording).
- [x] Finding 1: bindings immutable (`SessionError::ConflictingBinding`, atomic rejection both orders, rebinding A2→CTXG rejected).
- [x] Finding 2: `ViewId{session, index}`; all accessors return `Result`; `ForeignView` for foreign/out-of-range.
- [x] Finding 3: `StatusDelta` now covers reasons, waiting inventory, bindings; `is_empty()` iff snapshots identical.
- [x] Findings 1–4 green (`fix-1-green-findings-1-4.log`, 20 tests). Commit `f4066e60`.
- [x] Item 5: `Session::view_at(policy, heads)` — EX-02 fixed-heads view with patch transition.
- [x] Item 6: `Envelope` (`export`/`restore`), per-checkpoint frozen inputs; restore re-evaluates each checkpoint against only its own frozen inputs and rejects mismatches (`InconsistentEnvelope`, tamper test).
- [x] 23 fixtures green (`fix-1-green-fixtures.log`); full suite 498 passed / 0 failed / 1 ignored (`fix-1-full-suite.log`), same head as final tests. rustfmt clean.

## Fix wave 2 (re-review round 1)

- [x] RED (compile): `fix-2-red-compile.log` (`ContextBinding`, `ContextKind`, `Reason::UnresolvedGrant` missing).
- [x] RED (behavior, probe on pre-fix code, then deleted): `fix-2-red-behavior.log` — A3 in fresh context before G is `Excluded` (expected `Pending`); restored encoding is `UnicodeCodePoint` (expected `Utf16CodeUnit`).
- [x] Finding A: `ContextBinding { context, kind: Established | FreshGrant }`; `FreshGrant` without grant ⇒ `Pending` + `Reason::UnresolvedGrant`; with grant ⇒ `Eligible` + `AdmittedByContext`; `Established` never admits/blocks. EX-03 envelope test oracle corrected to `Pending` and restored pending decision asserted with reason.
- [x] Finding B: `Envelope.text_encoding`; `restore` uses `load_with_options(...text_encoding(..))`; Utf16 round-trip test.
- [x] Tautology replaced with before/after `current()`, `checkpoint_count()`, view and frozen `InspectionSnapshot` equality; `out_of_range_checkpoint_is_rejected` added.
- [x] Reason-only-change test now uses a second authorized revocation (a CTX0 grant no longer changes reasons for an `Established` binding).
- [x] 27 fixtures green (`fix-2-green-fixtures.log`); full suite 502 passed / 0 failed / 1 ignored (`fix-2-full-suite.log`). rustfmt clean.

## A3 — structural extension and adoption

- [x] Plan update in `implementation-plan.md` (A3 table).
- [x] RED (compile): EX-04 tests against missing `Session::{author, inspect_all}` (`extend-red-ex04.log`).
- [x] GREEN EX-04: `Automerge::transaction_view`, `Clock::mask()`, `Session::{author, inspect_all}` (`extend-green-ex04.log`). Commit 1.
- [x] EX-08/09/11: view-scoped `list_view/text_view/spans_view/marks_view` readers; tests passed first run after two test-literal fixes (op counter `5@bob`, counter Display `Counter: 10`) — no production semantic change was needed (`extend-green-ex08-09-11.log`). Commit 2.
- [x] RED (behavior): `ex07_restored_container_with_formatted_text_replays_once` — exposure of a restored text emitted `SpliceText{marks: None}` for `hello` although `el` is bold (`extend-red-ex12-14.log`, the pre-existing `flush_obj` TODO / A-C02).
- [x] GREEN: `PatchLog::ExposeQueue::flush_obj` exposes text span-by-span via `spans_for` with marks (`extend-green-ex07-formatted.log` shows the corrected patch stream; then oracle fixed to count total spliced width instead of one splice). EX-12/13/14 passed with the test-side `FormattedText` observer (`extend-green-ex12-14.log`).
- [x] 40 fixtures green (`extend-green-fixtures.log`); full suite 515 passed / 0 failed / 1 ignored (`extend-full-suite.log`). rustfmt clean.

## A3 repair wave 1 (extension review)

- [x] RED (compile): `extend-fix-1-red-compile.log` (`author_with_time`, `list_elements_view`, `QueuedActorBranch` absent).
- [x] RED (behavior, probe on pre-fix code, deleted after recording): `extend-fix-1-red-behavior.log` — (1) actor0 authoring sees excluded `Camping`; (2) ordinary reverse diff exposes `ab` not `a\u{fffc}b`; (3) no-op `author` prunes queued C. All three reviewer counterexamples reproduced.
- [x] Fix 1: `transaction_view` validates view, allocates/isolates actor, then compiles mask against resulting actor table.
- [x] Fix 2: `flush_obj` emits U+FFFC placeholder splices for `Span::Block` with correct width.
- [x] Fix 3: no-op authoring publishes nothing (stage discarded); real authoring that would prune queued changes → `SessionError::QueuedActorBranch{actor, lost}` (bounded policy: reject, never discard).
- [x] Fix 4: `author` applies immutable-binding validation (identical idempotent, conflicting rejected atomically). `author_with_time` added for deterministic hashes.
- [x] Fix 5: `list_view` doc corrected (value-op id); `list_elements_view` exposes `(value_op_id, element_id)`; decoded keys/preds asserted (`5@bob` keyed/pred on `4@alice`; EX-08 Y insertion keyed after `4@alice`).
- [x] EX-14 leakage: X imported after mark excluded — single unmarked splice.
- [x] Log naming corrected in report: `extend-green-ex07-formatted.log` records the 3-vs-1 oracle failure, not a green run.
- [x] 48 fixtures green (`extend-fix-1-green-fixtures.log`); full suite 523 passed / 0 failed / 1 ignored (`extend-fix-1-full-suite.log`). rustfmt clean.

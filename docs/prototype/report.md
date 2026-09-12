# Prototype A (external interpretation) — Task A1 checkpoint report

**Status: DONE_WITH_CONCERNS** (first evidence checkpoint; structural catalogue EX-08–14, EX-04 authoring, import-path matrix and envelope persistence are *not* implemented — see "Unsupported / unproven").

Baseline `7f96e1b27981` (`main@up`). Workspace `automerge-revocation-a`. All commands run from `rust/` with Rust 1.90, `--locked --offline`, external `CARGO_TARGET_DIR`.

## Commits (jj, this workspace only)

| Commit | Content |
|---|---|
| `b8b6c887048e` | Clock participation mask, `change_graph` helpers, `eligibility::{evidence,session}`, `PatchLog::scope`, `inner.rs` structural target check; EX-01 three checkpoints green. Plan + progress docs. |
| `09bcdcfa40e1` | EX-01 1,920 schedules/duplicates/E2-before-E1/status-only, EX-02 (+variant), EX-03, EX-05, EX-06 (×2), EX-07, failed-group atomicity, actor-recompilation tests. |
| `@` (report) | This report. |

## Reproduction

```sh
cd rust
cargo test --locked --offline -p automerge --test revocation_prototype   # 16 fixtures, ~2.7 s
cargo test --locked --offline -p automerge --lib eligibility             # 3 pure evaluator tests
cargo test --locked --offline -p automerge --lib --tests                 # full suite
cargo fmt -p automerge -- --check
```

Logs in run dir `../automerge-revocation-design/implementation-1/a/`: `red-ex01.log`, `green-ex01.log`, `green-ex01-schedules.log`, `red-ex02-ex03.log`, `red-ex05-07.log`, `red-ex07-predates-covers.log`, `green-all-fixtures.log`, `full-suite.log`.

## Executed results

Full suite (`full-suite.log`): every `test result: ok`; 489 passed, 1 ignored (baseline 472 + 19 new tests minus overlap in counting: 16 fixtures + 3 evaluator unit tests). No pre-existing test changed behaviour.

`revocation_prototype` (16 passed):

| Test | Fixture | What is asserted (against independent expectations, never the production evaluator) |
|---|---|---|
| `ex01_three_checkpoints` | EX-01 | Views `{title: Weekend plan, suggestion: Camping}` → `{title: Weekend plan}` → restored; R `Pending([])`/`Authorized`/`Invalidated`; A `Eligible`/`Excluded(OutsideFrontier{R})`/`Eligible`; C eligible throughout; hydrate(before)+patches == hydrate(after) for both transitions; restored value is A's original op `2@0101…`; A still in doc; cp1/cp2 reread unchanged after E2. |
| `ex01_all_1920_schedules` | EX-01 | 120 orders × 16 contiguous partitions = 1,920 schedules (asserted count). After **each** published group: view, integration state, C waiting-on-A, A eligibility, R authority against an order-free set model; patch replay. All schedules converge to identical endpoint. |
| `ex01_duplicates_are_idempotent` | EX-01 | Redelivering each event and the whole set: no patches, empty status delta, unchanged view. |
| `ex01_e2_before_e1_exposes_unresolved_then_resolves` | EX-01 | E2 first ⇒ R and E2 `Pending([E1])`, A eligible; late E1 ⇒ `Invalidated`, status delta `Pending→Invalidated`, no patches. |
| `ex01_status_only_variant_…` | EX-01 var. | Independent eligible delete hides suggestion; E1/E2 produce **empty content patches but non-empty eligibility+authority deltas**. |
| `ex01_status_stream_without_alice_content` | falsifier (3) | R→E1→E2 with no Alice content: status stream non-empty. |
| `ex02_missing_boundary_then_resolution` | EX-02 | `{x:1,y:2}` → `{}` with A1/A2 `Pending(MissingBoundary{R, missing:[H]})`, integrated and *not* waiting; heads unchanged between cp0/cp1 (view ≠ heads); H (a real empty change, `len()==0`) ⇒ `{x:1}`, A1 `Pending→Eligible`, A2 `Pending→Excluded`, exactly 1 patch; cp0/cp1 frozen. |
| `ex02_variant_boundary_also_edits_restored_object` | EX-02 var. | H also puts `z`; exactly 2 patches (no duplicate exposure). |
| `ex03_fresh_grant_non_prefix_selection` | EX-03 | One actor; bindings A1,A2→ctx0, A3→ctxG; G grant. Final `{before, after}`, A2 excluded, A3 `AdmittedByContext(ctxG)`. Late A2 after G and A3: A3 waits structurally (`waiting == {A2}`, not integrated), then integrates with exactly 1 patch; selection captured as `[Eligible, Excluded, Eligible]` and reread via `hydrate_view`. |
| `ex03_removing_restrictions_is_not_the_grant` | EX-03 contrast | Invalidating R restores `during` too — distinct from G. |
| `ex05_eligible_overwrite_through_excluded_predecessor` | EX-05 | With B excluded, `get_all` candidates are exactly `[1@alice, 3@carol]` (A and C) and hydrated `title` has `conflict: true`; all-eligible gives only C. |
| `ex06_eligible_delete_of_excluded_replacement` | EX-06 | C deletes B; B excluded ⇒ `{title: Original}` with candidate `1@alice`. |
| `ex06_simple_excluded_delete_targeting_eligible_value` | EX-06 | Excluded delete ⇒ A reappears; restoring delete hides again; replay both ways. |
| `ex07_excluded_container_eligible_child_and_restore_replay` | EX-07 | Root has no `section`; Bob's root edit visible (no causal taint); child inspectable by ObjId (`Plans`); restoration replay yields `section.title == Plans` with **exactly one** attach patch. |
| `failed_group_publishes_nothing` | ruling 4 | Group with valid C + conflicting evidence id ⇒ `SessionError::Evidence`; current view id, hydrate, inspection unchanged; C not in doc. |
| `captured_scope_recompiles_after_earlier_sorting_actor_arrives` | falsifier (2) | Actor `0x00…` arrives after an EX-03 capture; old capture rereads identically (mask recompiled against new actor table). |

Pure evaluator unit tests: `late_e1_cannot_overwrite_e2` (same set, two insertion orders ⇒ equal logs and `Invalidated`), `e2_without_e1_is_unresolved`, `pending_revocation_does_not_exclude`.

## TDD evidence (honest account)

- **Real red**: `red-ex01.log` — compile failure (`could not find eligibility`). After production code: `UnclassifiedChange` failure (before-view compiled against post-import graph must only require classification within its heads' ancestry — fixed in `scope_for`), then an op-identity mismatch caused by a *fixture* bug (`set_author` resets the actor; author must be set before actor). Then green (`green-ex01.log`).
- **Real red**: `ex01_all_1920_schedules` first failed on my convergence assertion — comparing `Debug` output of a `HashMap`-backed hydrate map is order-unstable. Fixed test to compare a canonical `BTreeMap`. Not a production defect.
- **Passed on first run (not red)**: EX-02, EX-03, EX-05, EX-06, EX-07, atomicity, recompilation. The evaluator/mask from EX-01 was already general. Logs are named `red-*` for the workflow step but record passes.
- **Executed falsifier (A-C01)**: reverting `ClockRange::predates` to `before.covers` makes EX-07 restoration replay produce `section: {}` instead of `section: {title: Plans}` (`red-ex07-predates-covers.log`). This confirms the two-predicate exposure rule is load-bearing, not speculative.

## Implementation (code paths changed)

| File | Change |
|---|---|
| `src/clock.rs` | `Clock` is now `{counters, mask: Option<Arc<OpMask>>}`. `contains` = structural; `covers` = `contains && !mask.excludes`. `OpMask` actor-indexed `RangeInclusive<u64>` lists. `ClockRange::predates` → `before.contains` (exposure), `visible_before/after` keep `covers` (participation). |
| `src/change_graph.rs` | `op_range(hash) -> Option<(actor, range)>` (None for empty changes), `in_ancestry(frontier, hash) -> Option<bool>` via `seq_clock_for_heads`, `iter_hashes`. |
| `src/eligibility/evidence.rs` | Pure: `EventId`, `AuthorizationContextId`, `Evidence::{Revocation,Authorizes,Invalidates{after},Grant}`, `Authority`, `Eligibility`, `Reason`, `Decision`, `EvidenceLog` (set; id reuse with different content is an error), `GraphFacts`, `authorities`, `evaluate`. |
| `src/eligibility/mod.rs` | `Selection`, `ViewSpec`, `ViewError`; `Automerge::{scope_for (crate), hydrate_view, get_all_view, diff_view}`. |
| `src/eligibility/session.rs` | `Session`, `Input::{Change,Evidence,Binding}`, `Capture{spec, inspection}`, `InspectionSnapshot{decisions, authorities, waiting, bindings}`, `Transition{before, after, patches, status}`, `StatusDelta`. Staged clone; failure discards. |
| `src/patches/patch_log.rs` | `scope: Option<Clock>` — when set, `make_current_patches` does not re-resolve a clock from heads. |
| `src/transaction/inner.rs` | `exid_to_obj` uses `contains` (structural availability only; ruling 2). |
| `src/lib.rs` | `pub mod eligibility`. |

**Bounded deviation from the design packet:** instead of a new `Scope` type threaded through ~60 `Option<Clock>` call sites, the mask lives inside `Clock`. Every existing `covers` call site automatically becomes "participates"; only `predates` and `exid_to_obj` were switched to `contains`. Since `Clock` is `pub(crate)` and its only public constructor path is `from_iter`, unmasked behaviour is byte-identical (allow-all compatibility: full suite unchanged).

## Concrete design costs observed (architecture A)

1. **Heads do not identify a view.** EX-02 cp0 and cp1 have identical heads and different content. Every read/patch API keyed by heads (`*_at`, `diff`, `AutoCommit` caches) is unusable for eligibility views; the prototype adds parallel `*_view` entry points.
2. **Every transition is a full `DiffIter` recomputation** between two compiled scopes over the whole document. No incremental import patch logging is used (`apply_changes` runs with an inactive log). Cost scales with document size, not with the delta. No performance claim.
3. **Two independently evolving inputs ⇒ schedule-dependent intermediate views.** The 1,920-schedule test confirms convergence at the endpoint and correctness of each intermediate view against a set-based expectation; intermediate screens legitimately differ between schedules (e.g. C waiting for A in some orders).
4. **Mask staleness risk.** `OpMask` is bound to the actor table; it is recompiled at every use (`scope_for`) rather than cached. Caching would require a generation check (packet's `StaleGeneration`); not implemented.
5. **Capture requires an external sidecar.** `Capture` holds `ViewSpec` + `InspectionSnapshot` in memory only. Envelope serialization/restore and content-only-vs-complete-capture distinction are **not implemented**.
6. **Whole-group staging clones the entire `Automerge` plus evidence** per delivery group (ruling 4).
7. **Pending target hides content without heads change** (EX-02): the "pending" mask is the same mechanism as exclusion at the read layer; only the inspection snapshot distinguishes them — status-only transitions must therefore always be published alongside patches.

## Unsupported / unproven (explicitly)

- EX-04 (authoring against a view, `transaction_args_scoped`, context binding on commit) — not implemented.
- EX-08/09 lists, EX-10/11 counters, EX-12/13/14 marks and the formatting-aware patch observer — not implemented or tested. The mask is applied inside `covers`, so list/counter/mark paths *should* honour it, but this is source reasoning, not evidence.
- Import paths: only `apply_changes` (batched, inactive patch log). Merge, incremental load (including the empty-doc `*self = doc` branch), sync, and `AutoCommit` — not exercised.
- Envelope persistence (`save` + sidecar), `ViewId` canonical digest / foreign-capture rejection — not implemented. `ViewError::ForeignHeads` exists but is only reachable by handcrafted specs.
- Property-based generation of larger histories — not added (fixed enumeration only).
- Collapsed-gap insertion characterisation (A-C10) — not started.
- Permutation bounds used: EX-01 exhaustive 1,920; EX-02/03 only the named orders (not enumerated).

## Minimal reproductions

- A-C01 exposure bug: change `src/clock.rs` `predates` body to `before.covers(id)`; run `cargo test --locked --offline -p automerge --test revocation_prototype ex07` → replay mismatch `section: Map({})`.
- Fixture pitfall: `Automerge::new().with_actor(a).with_author(Some(x))` yields a random actor; use `with_author` first.

## Next steps (for review/continuation)

1. EX-04 scoped authoring (`Automerge::transaction_args` variant taking `ViewSpec` + context).
2. EX-08–11 with real list/counter reads through `get_all_view`/`hydrate_view`; add `spans_view`/`marks_view` for EX-12–14 plus a formatting-aware observer.
3. Envelope round-trip and content-only-vs-complete-capture test.
4. Merge/incremental-load/sync path matrix with re-evaluation after each import.

---

# Fix wave 1 (review round 1) — addendum

**Status: DONE_WITH_CONCERNS** (all six required items implemented and tested; concerns listed at the end).

## Commits

| Commit | Content |
|---|---|
| `f4066e605c4a` | Findings 1–4: immutable bindings, namespaced checked `ViewId` + `Result` accessors, complete `StatusDelta`, `visible_before` participation fix + `predates` doc correction; EX-10 test. |
| `@` (this addendum's commit) | Items 5–6: `Session::view_at`, `Envelope` export/restore with per-checkpoint frozen inputs; report/progress. |

## Test totals (exact, same head)

- `cargo test --locked --offline -p automerge --test revocation_prototype`: **23 passed** (`fix-1-green-fixtures.log`).
- `cargo test --locked --offline -p automerge --lib --tests`: **498 passed, 0 failed, 1 ignored** (`fix-1-full-suite.log`; its `revocation_prototype` section shows 23). Arithmetic: 472 baseline + 23 fixtures + 3 evaluator unit tests = 498. The earlier report's "489 / overlap" statement was wrong: `full-suite.log` was run before the final two regressions were added (14 fixtures), and the coordinator's 491 corresponds to 16 fixtures.

## TDD evidence

| Step | Log | Content |
|---|---|---|
| Red (compile) | `fix-1-red-compile.log` | 20 errors: missing `ConflictingBinding`, `ForeignView`, `waiting_changes`, `Result` accessors. |
| Red (behavior) | `fix-1-red-ex10-counter.log` | Probe test on pre-fix `visible_before`: same-capture `diff_view` emits `[Increment { prop: "n", value: -100 }]` exactly as the review predicted. |
| Green | `fix-1-green-ex10-counter.log` | Same probe after `visible_before → before.covers`. Probe file then deleted; the durable test is `ex10_excluded_increment_self_diff_duplicate_and_restore_replay`. |
| Green | `fix-1-green-findings-1-4.log` | 20 tests incl. the 1,920 schedules. One iteration: my initial reason-change assertion (R+E1 changes eligibility, not only reasons) was wrong; replaced with a Grant that adds `AdmittedByContext` while eligibility stays `Eligible`. |
| Green | `fix-1-green-fixtures.log` | 23 tests incl. items 5–6. Items 5/6 tests passed on first compile (no separate red beyond the compile failure). |

## Repairs by finding

1. **Bindings** (`session.rs` `deliver`): identical → no-op; conflicting → `SessionError::ConflictingBinding{hash, existing, attempted}` before `apply_changes`, whole group discarded. Test `conflicting_context_binding_is_rejected_atomically_in_both_orders` covers both orders, a group with otherwise-valid A3 (not integrated, not in doc), unchanged view/bindings, and rejection of rebinding A2→CTXG after the fact (`ex03_check_final` still holds).
2. **ViewId** = `{session: SessionId, index}` with a process-unique `SessionId`. `capture/hydrate/get_all/decision/authority/waiting/is_integrated` return `Result<_, SessionError>`; foreign/out-of-range → `ForeignView`. Test `foreign_view_id_is_rejected_not_misread` (two sessions, same index, different content). Not a persistent digest: a restored session gets a fresh namespace and old ids are rejected (tested); `ViewId::index()` + `Session::checkpoint(i)` re-address checkpoints after restore.
3. **StatusDelta** adds `reason_changes`, `waiting_changes` (`Option` before/after), `binding_changes`; `is_empty()` iff the two `InspectionSnapshot`s are equal. Test `queued_only_receipt_is_an_inspection_transition`: C-before-A signals `waiting: None→{A}`, duplicate is a no-op, A's arrival signals `{A}→None` + `newly_integrated`, binding receipt and reason-only changes are signalled. EX-01 duplicate/1,920 tests still assert empty deltas for duplicates.
4. **Counter predicate**: `ClockRange::visible_before` now `before.covers` (participation); `predates` stays `before.contains` (exposure) with corrected doc comment. Test `ex10_…`: 115 → 15 with replay, self-diff empty, empty group and duplicate `+100` produce no patches, restoring `+100` replays to 115.
5. **EX-02 fixed heads**: `Session::view_at(policy, heads)` filters the policy capture's selection to the ancestry of `heads` and compiles it. Test: pending capture at heads `[A2]` shows `{}`; after H integrates (session heads move), `view_at(resolved, [A2])` shows `{x:1}` with A1 eligible / A2 excluded / H absent; old capture still pending `{}`; `diff_view(old, fixed)` is exactly one patch and replays.
6. **EX-03 envelope**: `Envelope { base bytes, checkpoints: [{received raw change bytes, evidence, bindings, frozen capture}] }`. `restore` replays checkpoints in order, re-evaluates each against **its own** frozen evidence/bindings, and errors (`InconsistentEnvelope`) if heads, selection or inspection differ. Test round-trips a waiting checkpoint (A2 waiting for A1), a pre-grant checkpoint (A3 excluded) and the final gap `[Eligible, Excluded, Eligible]`; also shows content-only `save/load` yields `{before, during, after}` (no interpretation). Tamper test rejects a corrupted frozen selection.

## Remaining concerns (not blockers for this wave)

- `Envelope` is an in-memory Rust struct, not serialized bytes; "disposable and private" per the prompt. Serialization would be a straightforward addition but is untested.
- `SessionId` is a process-local counter: foreign-session detection is sound within a process, not across processes (would need a digest).
- `restore` recomputes `authorities` from the frozen log rather than storing them separately — acceptable since the log is the frozen input, but it means the evaluator itself is trusted on restore (the mismatch check would catch evaluator drift as `InconsistentEnvelope`).
- Atomicity test still triggers rejection before `apply_changes`; a structural failure *during* content application is not exercised (reviewer's note stands).
- EX-04 authoring, EX-08/09/11–14, merge/incremental-load/sync paths remain unimplemented/unproven as before. EX-10 is now covered for the map-counter case only.

---

# Fix wave 2 (re-review round 1) — addendum

**Status: DONE** for the three requested items; prior concerns (in-memory envelope, process-local `SessionId`, `apply_changes`-time structural failure untested, EX-04/08/09/11–14 and import paths unproven) still stand.

## Commit

Single commit for this wave, parent `ece98c313ac2`; the commit id is the head of `jj log` in this workspace (the id changes when this line is edited, so it is not inlined).

## Test totals (same head)

- `cargo test --locked --offline -p automerge --test revocation_prototype`: **27 passed** (`fix-2-green-fixtures.log`).
- `cargo test --locked --offline -p automerge --lib --tests`: **502 passed, 0 failed, 1 ignored** (`fix-2-full-suite.log`; prototype section 27). 472 + 27 + 3 = 502.

## TDD evidence

| Step | Log | Content |
|---|---|---|
| Red (compile) | `fix-2-red-compile.log` | `ContextBinding`/`ContextKind`/`Reason::UnresolvedGrant` absent. |
| Red (behavior) | `fix-2-red-behavior.log` | Probe on pre-fix code: A3 (CTXG, no G) `Excluded` not `Pending`; restored doc encoding `UnicodeCodePoint` not `Utf16CodeUnit`. Probe deleted after recording; durable tests below. |
| Green | `fix-2-green-fixtures.log` | 27 tests. Two test adjustments during the wave: (a) `ex03_removing_restrictions_is_not_the_grant` now delivers G up front (without it, A3 is correctly pending and the `{before}` oracle no longer holds; the contrast — invalidating R restores `during` — is preserved); (b) the reason-only-change check in `queued_only_receipt_…` uses a second authorized revocation, since a grant for an `Established` context no longer alters reasons. |

## Repairs

**A. Fresh-grant context (`evidence.rs`).** `Input::Binding(hash, ContextBinding { context, kind })` with `ContextKind::{Established, FreshGrant}`. Evaluation: `FreshGrant` + matching `Grant` ⇒ `Eligible`/`AdmittedByContext` (revocations bypassed); `FreshGrant` without grant ⇒ `Pending`/`UnresolvedGrant` and the change is masked out of the default view (not allow-all, not excluded); `Established` contexts are evaluated only through revocation frontiers. Tests: `ex03_fresh_context_is_pending_until_grant_arrives` (A3 `Pending`, A2 `Excluded`, A1 `Eligible`, default `{before}`; G ⇒ `Pending→Eligible` status for A3 only, exactly one patch, replay, frozen pending checkpoint unchanged); `fresh_context_without_grant_is_not_allow_all` (no revocation at all: `Established` eligible, `FreshGrant` pending); `ex03_envelope_round_trip_…` oracle corrected — the pre-grant checkpoint is `Pending` with `UnresolvedGrant(CTXG)` and restores as such even though G is in later checkpoints' frozen inputs. Bindings remain immutable (`ConflictingBinding` compares the whole `ContextBinding`).

**B. Envelope text encoding (`session.rs`).** `Envelope.text_encoding` captured from the published doc; `restore` loads with `LoadOptions::new().text_encoding(..)`. Test `envelope_preserves_non_default_text_encoding` (Utf16 base with a simple map; restored `text_encoding()` equal, heads equal, view equal).

**C. Assertions.** Tautology replaced: `before = s.current()`, `count_before`, view, and `Arc<InspectionSnapshot>` captured *before* the rejected delivery; asserted equal afterwards. `out_of_range_checkpoint_is_rejected` added (`checkpoint(1)` on a one-checkpoint session ⇒ `ForeignView`).

**Report correction.** The fix-1 addendum said authorities are "not stored separately" on restore. That was inaccurate: every frozen `InspectionSnapshot` includes its `authorities` table, and `restore` compares the recomputed snapshot (including authorities) against it, rejecting mismatch with `InconsistentEnvelope`.

---

# A3 — structural extension and adoption — addendum

**Status: DONE_WITH_CONCERNS.** Items 1–4 of the A3 brief implemented and tested; item 5 (import routes) not implemented, listed as unsupported.

## Commits (jj, workspace A; parent `5394572bbbd3`)

| Commit | Content |
|---|---|
| `7d2543d9` | Selected-view authoring: `Automerge::transaction_view`, `Clock::mask()`, `Session::{author, inspect_all}`; EX-04 tests. |
| `fd64c84a` | View-scoped `list_view/text_view/spans_view/marks_view`; EX-08/09/11 tests. |
| `@` (head) | `flush_obj` formatted exposure fix; EX-12/13/14 + formatted EX-07 tests; docs. |

## Test totals (head)

- `cargo test --locked --offline -p automerge --test revocation_prototype`: **40 passed** (`extend-green-fixtures.log`).
- `cargo test --locked --offline -p automerge --lib --tests`: **515 passed, 0 failed, 1 ignored** (`extend-full-suite.log`) = 472 + 40 + 3.

## Implemented fixture IDs this wave

| ID | Test | What is asserted |
|---|---|---|
| EX-04 | `ex04_inspect_then_adopt_scalar_without_reinstating_alice` | Bob's view `{title}`; `inspect_all` view exposes A's `Camping` at op `2@alice` while A is `Excluded`; `Session::author(view, bob, actor, ctx, f)`: inside `f`, `tx.get(suggestion)` is `None`; D commits with deps == captured heads, author `bob`, actor `bob`, **`pred` empty** (no targeting of hidden A), one eligible new hash bound to `CTX_BOB`; A stays `Excluded`; view `{title, suggestion}` with candidate `4@bob`; replay ok; after invalidating R, `get_all` = `{2@alice, 4@bob}` (ordinary concurrent conflict). |
| EX-04 var. | `ex04_variant_object_id_edit_into_hidden_container_allowed` | Object-ID edit into a structurally present but excluded map is accepted (ruling 2); no root patches; inspectable by ObjId. |
| EX-08 | `ex08_insertion_after_excluded_anchor_survives_with_stable_order` | `[L,Z?,X,Y,R]` (with concurrent Z near X); excluding X ⇒ 4 elements without X, Y present, surviving element ids in the same relative order as the full view; hydrate replay equals `list_view`; restoring X returns the exact full value+id order. |
| EX-08 char. | `ex08_capture_reread_after_earlier_sorting_actor_arrives` | Actor `0x00` inserts after capture; old capture rereads `[L,Y,R]`. |
| EX-09 | `ex09_eligible_replacement_at_excluded_insertion_identity` | Bob's `put(list,1,Y)` has `pred.len()==1` (targets X's value); excluding X ⇒ `[L,Y,R]` with element id `5@bob`; restoring X keeps Y (direct predecessor). |
| EX-09 var. | `ex09_variant_replace_excluded_map_element_with_eligible_map` | New map visible with `k=new`; old excluded map's child not visible via `get_all`. |
| EX-11 | `ex11_excluded_counter_base_supplies_nothing_to_eligible_increment` | `105` ⇒ no counter at all (`get_all` empty), C still `Eligible`, self-diff empty. |
| EX-11 | `ex11_eligible_increment_on_excluded_replacement_base` | `105` ⇒ `10` with candidate `1@alice` (not 15); empty/duplicate group no-ops; restoring B ⇒ `105`, candidate `2@bob`. |
| EX-11 compat | `ex11_compat_increment_suppresses_targeted_scalar_candidate` | Increment with `pred.len()==2` over `{Counter 10, "text"}`; allow-all view equals ordinary Automerge (characterized); excluding the increment re-exposes both candidates. |
| EX-12 | `ex12_eligible_mark_over_partly_excluded_text` | `a**bXYc**d` ⇒ `a**bc**d` (`marks_view` = `[1,3)`), formatted replay; restore returns the exact original formatting. |
| EX-13 | `ex13_dormant_mark_and_surviving_interior_insertion` | `ab**XY**cd` ⇒ `abcd` with no non-empty visible mark; variant `ab**XQY**cd` ⇒ `ab**Q**cd`; formatted replay. |
| EX-14 | `ex14_excluded_mark_and_excluded_unmark` | A: `**aXb**` ⇒ `aXb`, no `SpliceText` carries bold; B: `**a**bc**d**` ⇒ `**abcd**` when unmark excluded, back on restore; formatted replay each step. |
| EX-07 fmt | `ex07_restored_container_with_formatted_text_replays_once` | Restoring an excluded map containing bold text emits splices whose widths sum to the text length, with marks, no standalone `Mark`; formatted observer from empty reproduces `h**el**lo`. |

## TDD evidence

- `extend-red-ex04.log`: compile red (`Session::author/inspect_all` absent). EX-04 then passed on first run of the implementation (no intermediate semantic failure).
- EX-08/09/11 passed first run; two *test literal* corrections (`extend-green-ex08-09-11.log`). No production semantics changed — honest note: these fixtures did not force a production fix.
- **Real behavioral red**: `extend-red-ex12-14.log` — formatted EX-07 failed: exposure emitted `SpliceText{marks: None}` for the whole restored text. Cause: `patch_log.rs::ExposeQueue::flush_obj` used `text_for` + `None` marks (the baseline TODO the design packet flagged as A-C02). Fix: expose via `spans_for`, one splice per span with its `MarkSet`. `extend-green-ex07-formatted.log` shows the corrected 3-splice stream; my oracle had asserted "exactly one splice", corrected to "splice widths sum to text length, no standalone Mark".
- EX-12/13/14 themselves passed once the observer existed (`extend-green-ex12-14.log`); the mark filter paths (`marks_at`, `RichTextQueryState`) honoured the mask through `covers` without further change — this is executed evidence for these fixtures only.

## Production code changed this wave

| File | Change |
|---|---|
| `src/eligibility/mod.rs` | `transaction_view`, `list_view`, `text_view`, `spans_view`, `marks_view`. |
| `src/eligibility/session.rs` | `inspect_all`, `author` (staged clone, `set_author`/`set_actor`, view transaction, commit, bind, publish, record). |
| `src/clock.rs` | `Clock::mask()` accessor. |
| `src/patches/patch_log.rs` | `flush_obj` text exposure with marks (affects ordinary exposure paths too; full suite unchanged: 515/0/1). |

Source LOC (eligibility module): evidence 402, mod 194, session 676 = 1,272. Test file: 3,092 lines, 40 tests. Files changed since baseline: 12 (incl. docs).

## Debug fixture cost (NOT a benchmark)

Debug build, unoptimized: all 40 fixtures ≈ 3.1 s wall, of which the 1,920-schedule EX-01 test is ≈ 2.6 s (~1.4 ms per schedule incl. clone-staging and full diff). Labelled as debug fixture cost only.

## Unsupported / unproven (explicit)

- Import routes: only `apply_changes` batches and `Session::author`. **Merge, incremental load, sync, `AutoCommit`, `load` of eligibility state via ordinary bytes** — not exercised; heads-keyed public APIs (`*_at`, `diff`) remain allow-all and unaware of views.
- `Session::author` produces an inactive-log transaction; patches come from the endpoint `diff_view`, not from transaction logging.
- `inspect_all` is an allow-all view of the same heads (inspection), not a general per-change reveal; pending vs excluded is distinguished by the decision table, not by separate inspection views.
- Text fixtures use `ExpandMark::None`/`Both` only in the configurations above; collapsed-gap insertion (A-C10) across expansion modes is not characterized. Block markers are not exposed by `flush_obj` (fixtures have none).
- Envelope still in-memory; `SessionId` process-local; structural failure during `apply_changes` untested (as before).
- Formatting observer is test-side and text-object-local; it handles `SpliceText`, `DeleteSeq`, `Mark` only.

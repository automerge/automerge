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

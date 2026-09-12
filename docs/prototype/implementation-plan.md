# Prototype A — local implementation plan (code-grounded)

Baseline `7f96e1b2`. Workspace `automerge-revocation-a`. Disposable experiment.

## Bounded engineering choice (deviation from packet type sketch)

The packet proposed a new `Scope { history, mask }` type replacing `Option<Clock>` at ~60
call sites. To get a working vertical slice inside the budget, the **participation mask is
carried inside `Clock` itself** (`clock.rs`):

```rust
pub(crate) struct Clock { counters: Vec<u32>, mask: Option<Arc<OpMask>> }
impl Clock {
    fn contains(&self, id) -> bool;      // structural: counters[actor] >= id.counter
    fn covers(&self, id) -> bool;        // participates: contains && !mask.excludes(id)
}
pub(crate) struct OpMask { excluded: Vec<Vec<RangeInclusive<u64>>> } // actor-indexed
```

Every existing `covers` call site therefore already applies the two-predicate rule as
"participates". The only exposure predicate is changed explicitly: `ClockRange::predates`
uses `before.contains` (A-C01) while `visible_before` keeps `covers`. Transaction target
check `inner.rs::exid_to_obj` uses `contains` (ruling 2: structural availability only).
`OpMask` is recompiled from the `Selection` at every use (never persisted).

Cost if wrong: identical to the packet's cost — the predicate split lives in one place.

## Source files

| File | Change |
|---|---|
| `src/clock.rs` | `Clock` gains `mask`; `contains`, `with_mask`; `OpMask`; `ClockRange::predates` → `contains`. |
| `src/change_graph.rs` | `op_range(hash) -> Option<(actor_idx, RangeInclusive<u64>)>`, `in_ancestry(frontier, hash) -> Option<bool>`. |
| `src/eligibility/mod.rs` | `Eligibility`, `Selection`, `ViewSpec`, `ViewError`; `Automerge::{scope_for, hydrate_view, get_all_view, diff_view}`. |
| `src/eligibility/evidence.rs` | pure evaluator: `EventId`, `Evidence`, `Authority`, `Decision`, `Reason`, `EvidenceLog`, `GraphFacts`, `evaluate`. |
| `src/eligibility/session.rs` | `Session`, `Input`, `Capture`, `ViewId`, `Transition`, `StatusDelta`, staged group delivery. |
| `src/patches/patch_log.rs` | `scope: Option<Clock>` override so `make_current_patches` does not re-resolve from heads. |
| `src/transaction/inner.rs` | `exid_to_obj` uses `contains`. |
| `src/lib.rs` | `pub mod eligibility` (experimental, doc-hidden). |
| `tests/revocation_prototype.rs` | EX-01 (1,920 schedules, duplicates, E2-before-E1, status-only variant, capture reread), EX-02, EX-03, EX-05, EX-06, EX-07. |

## Test-first steps

1. RED: `tests/revocation_prototype.rs::ex01_three_checkpoints` fails to compile (no `eligibility` module). Save `red-ex01.log`.
2. GREEN: clock mask + change_graph helpers + eligibility module + evaluator + session. Save `green-ex01.log`.
3. RED/GREEN: `ex01_all_schedules` (120 × 16), `ex01_duplicates_idempotent`, `ex01_e2_before_e1`, `ex01_status_only_variant`.
4. RED/GREEN: `ex02_missing_boundary`, `ex03_fresh_grant_non_prefix`.
5. RED/GREEN: `ex05_overwrite_through_excluded`, `ex06_delete_of_excluded_replacement`, `ex07_restored_container_patch_replay`.
6. Full `cargo test --locked --offline -p automerge --lib --tests`; rustfmt check; commit; report.

Command (from `rust/`): `cargo test --locked --offline -p automerge --test revocation_prototype`

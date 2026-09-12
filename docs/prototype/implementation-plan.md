# B1 local implementation plan

Baseline: `7f96e1b2798102cc4aa271172be131299d6fbe95`; clean working revision verified 2026-09-12 16:41 UTC. Baseline suite was supplied by coordinator (472 passed, 1 ignored); not rerun before changes.

## Bounded interfaces and files

- Feature `experimental-revocation` in `rust/automerge/Cargo.toml`; private `prototype` module in `src/lib.rs`, with private fixture tests. The delivery entry point stages a cloned Session; no global load semantics changed.
- `prototype::{Session, Capture, Input, Eligibility, Authority, Revoke}`: retained original Change archive, immutable mock evidence, per-change context, native control evaluator, frozen stable hash selections, status deltas and real endpoint patches. One control per control-only change. Native action 8 carries a versioned bytes payload (target author plus retained frontier); root/empty-key sentinel, no predecessors. Validate frontier ancestry when dependencies are ready; queued credible controls participate in provisional evaluation.
- Native encoders/decoders in `types.rs`, `op_set2/types.rs`, `legacy/`, plus action-role filtering in `op_set2/{op.rs,op_set/{index,visible,top_op,found_op}.rs,change/batch.rs}`. Expanded/native persistence must retain action and identity.
- `clock.rs`: retain structural upper bounds and add optional non-prefix operation participation selection. Hash selections compile against the live actor table. Historical structural existence must remain separate from participation.
- `automerge.rs`: narrow private selected-clock access to existing hydration/conflict/diff machinery, not history replay. Captures retain heads, hash statuses and received manifests; both diff endpoints compile against post-import graph.

## Executable test-first checkpoints

Run from `rust/`, always `--locked --offline`; logs under assigned implementation-1/b directory.
1. Write `prototype::tests::ex01_authority_checkpoints` before APIs (initial compilation red); native control roundtrip/noninterference behavior red, implement action 8 and gated staged receipt.
2. Make EX-01 three expected screens/statuses/original candidate identity green using real ops; replay native patches. Enumerate 120 receipt orders x 16 partitions, causal E2-before-E1 and duplicates; status-only case.
3. Add behavioral red/green for EX-02 missing boundary/queued R and frozen live reread; EX-03 one actor non-prefix/context grant, delayed structural dependencies.
4. Add behavioral red/green direct-target EX-05/06 and restored-map EX-07 patch replay. These are minimum evidence before any claim of generalized selection support.
5. Test atomic reject, empty-key independence, unknown target, ancestry, compact native roundtrip, feature gate. Selected authoring will be added only if time permits with genuine tests; omitted paths are explicit.
6. Focused: `cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype`. Checkpoint full: `cargo test --locked --offline -p automerge --features experimental-revocation --lib --tests`, plus default suite and targeted rustfmt. Self-review with `jj diff`, commit coherent checkpoint using `jj`.

Scope priority is the running map/native-control slice. EX-04 and EX-08–14, general sync/import paths, formatting replay and optimization remain unproven unless actually executed. No dependency additions, capability system, filtered replay, or ordinary-map control substitute.

## Fix wave 1 (review F1, 20-minute bound)

1. Add behavior tests in `prototype/tests.rs` for received R retaining known H but depending on missing X. Cover eventual valid X (H in ancestry) and invalid X (H outside ancestry); assert separate authority/integration/target status, eligible-only materialization, frozen rereads and atomic invalid-group rejection. Run before production edits; logs `fix-1-red-f1.log`.
2. In `prototype/policy.rs::resolve_controls`, require integrated R before final frontier exclusion. Add a precise unresolved-validation reason when H is already known; preserve missing-H EX-02 reasoning and pending-authority EX-01 behavior. No quarantine/removal semantics. Record F2 dissent without changing control eligibility.
3. Add named EX-04 scalar adoption using existing selected editing and retained original inspection; assert Bob's new identity, mock authorship, empty predecessors, A still excluded and ordinary conflict after restoration. Correct feature-aware action-range diagnostic in `op_set2/types.rs` with a covering assertion.
4. Run focused prototype tests plus all 4,032 schedules, full feature/default suites, targeted formatting. Update progress/report with exact evidence and jj commits; no structural extension.

## Actual bounded implementation decisions

- Existing `Automerge` private helpers sufficed: no `automerge.rs` edits were needed. `prototype/core.rs` orchestrates the real graph and `prototype/policy.rs::resolve_controls(HistoryFacts, &PolicySnapshot) -> Interpretation` is the pure evaluator. `prototype/tests.rs` plus the original `first_test` exercise private APIs. `prototype/gate_tests.rs` runs with the feature disabled.
- `Session::{deliver, receive, capture, scope, edit, export, restore, observe}`; `Transition::apply` checks full captured endpoint identity and stages hydrated patch application. `receive(version, group)` requires experimental version 1 before parsing the entire group. Ordinary load APIs are not redefined.
- Capture includes hash-keyed eligibility/reasons, authority, integrated/received manifests, missing dependencies/frontiers/evidence, mock actor-author bindings and complete policy snapshot. Export is an in-memory `Package` with native original change bytes plus that capture, not a persistent production envelope serializer. Default text encoding is fixed by `Session::new` and non-map content is rejected.
- `Clock` now names structural membership, optional selected operation IDs and transaction-local actor/start. This deliberately uses a `BTreeSet<OpId>` rather than interval optimization. It supports whole-change gaps, with original history retained.
- B1 explicitly rejects lists, text, counters, marks, table creation and competing controls at the Session boundary. Removed speculative list/text exposure edits after review. Only map exposure and structural object authoring gates changed. Ordinary allow-all documents retain all their existing paths.
- Trusted mock `Content(Change, author_bytes)` supplies affirmative base admission; fresh `Context(hash, grant_id)` has explicit independent pending/eligible admission. The experiment does not infer permission from actor IDs, sequence numbers or arrival order. Constructor uses fresh actor only for control authoring and respects ancestry-covered frontiers.


# Prototype B — B1, approved repair and B3 structural evidence

**Status: DONE_WITH_CONCERNS** — runnable EX-01–14 catalogue checkpoints, including B3 structural/formatting replay. Scope variants and integration limitations remain; not production integration. Latest source **`c33dcb62`**; final suites **531 feature / 475 default passed, 1 ignored each**. B3 extension review pending.

Date: 2026-09-12. Workspace: `automerge-revocation-b`.

## Commits and provenance

- Baseline: `7f96e1b2798102cc4aa271172be131299d6fbe95`.
- Implementation: **`174274e71989a4369a967e61e996eae34ff08aee`**, jj change `vqxmktrsqwzpszyoppvvqnnzxszmmwpl`, description `prototype B: native controls, frozen map interpretations and staged patch transitions`.
- Initial report: `4e28b301a9fa5de42bc95e5e99865493759d58a3`, `prototype B: document executed B1 evidence and limitations`.
- Fix wave 1: **`a797e199ac519f4c95203633f1d3d420217ba80f`**, jj change `osvtqqzkwoopwkwkrqtxpoxnrvrvyymz`, `prototype B: keep unvalidated queued controls pending; cover scalar adoption`. Source tested by all `fix-1-*` final runs below. This report update is a subsequent documentation-only commit.
- B3 structural implementation: **`c33dcb6238a8dc412757fb29f7c66f401a7e79c9`**, jj change `xkmvsvrwukummsxtqrrlpwnqqktnmksq`, `prototype B: selected sequences, counters and rich-text replay evidence`. This latest report update follows as documentation only.
- Used only the assigned source workspace/run directory and supplied read-only brief/spec/plan/B design/TDD skill. No sibling implementation, historical revocation branch, subagents, model invocations, pushes or shared bookmark changes.
- Clean pinned parent and Rust 1.90 were verified. Coordinator supplied the clean-baseline result (472 passed, 1 ignored); I did not independently rerun that baseline. Final default-source regression suite was run below.

## What runs

Private APIs, compiled with `experimental-revocation`, retain **actual native action-8 operations**. One control-only change consumes an operation identity; payload v1 contains target author bytes and canonical retained hashes. Canonical root/empty-key fields are storage sentinels, not map values. Controls are excluded from value visibility/index/topness. Identity survives change parsing, batched integration, compact save/load and incremental native change bytes.

`prototype/policy.rs::resolve_controls(HistoryFacts, &PolicySnapshot)` is pure. `prototype/core.rs::Session` handles staged receipt, validation, frozen capture, selected authoring, reconstruction and publication. It retains every received original `Change`, including queued and excluded content. Native retained-frontier ancestry is evaluated independently of selected content heads. No filtered-history replay or synthetic materializer is used.

The mock adapter supplies explicit trusted actor-to-author bindings and affirmative base admission with `Input::Content`; it is not a signature or capability evaluator. `Authorize(R)` and `Invalidate(R)` are immutable evidence identities with the fixed dependency `Invalidate(R) -> Authorize(R)`. `Context(change, grant)` is an explicit trusted fresh-context association; `Grant(grant)` resolves its admission. It is not inferred from actor rotation, sequence numbers or receipt time.

### Executed fixtures

| Fixture | Actual evidence |
|---|---|
| EX-01 | All three policy checkpoints; original suggestion identity reinstated; C depends on R and remains eligible; R pending/authorized/invalidated inspection. **120 orders × 16 partitions = 1,920 schedules** of A/R/C/E1/E2, H preloaded. Every published group has independent intermediate expectations and native patch replay; every event is redelivered; frozen endpoints are reread on the later live graph. E2-before-E1 and a deletion/status-only variant are covered. |
| EX-02 | Credible queued R with missing H makes integrated A1/A2 pending; empty H restores only A1. Frozen pending capture and unchanged isolated content heads with later known boundary pass. A map-restoration variant with an eligible boundary edit emits its two child puts once. **24 × 8 = 192 schedules** of A1/A2/H/R, authority bundled with R and H0 preloaded. |
| EX-03 | One continuing Alice actor; A1/A3 eligible, A2 excluded; fresh-context A3 pending until G; A3 can wait structurally on delayed A2. Complete-package reconstruction preserves the hole. **120 × 16 = 1,920 schedules** of A1/A2/A3/R/G, context bundled with A3 and authority with R. Each group checks independent state/status expectations and patch replay. |
| EX-04 | Added in fix wave 1: inspect retained excluded Camping; Bob authors new eligible D with a new hash/op identity, captured mock authorship and empty predecessors against the excluded view. A remains excluded. Later invalidation restores A alongside D as two equal-valued conflict candidates. Native patch replay and frozen captures pass. No general recovery API or durable adoption provenance format. |
| EX-05 | Eligible A, excluded replacement B, eligible C: conflict candidates are original A and C identities, not transitive suppression. Invalidation leaves only C. |
| EX-06 | Eligible deletion targeting excluded B does not delete A. An excluded deletion restores A; invalidation hides it again. A new selected-view deletion records A as its actual predecessor. |
| EX-07 | Excluded map attachment with eligible child and independent root edit; child remains object-locally inspectable and root-unreachable. Restoring attachment exposes existing child; patch replay passes. |
| EX-08/09 (B3) | Hidden insertion anchor retains Y; concurrent Z/Y ordering; exact insertion element IDs distinct from replacement value IDs; selected-index authoring and earlier-sorting actor reread. Eligible scalar/map replacement at excluded element preserves original element, without adopting old map children. |
| EX-10/11 (B3) | Map/list counters exclude +100 but retain +5; self-diff/empty/duplicate/restoration; excluded base contributes no counter; increments remain attached to original base and do not migrate to old/new bases. Both eligible increment suppressing a scalar conflict and excluded increment failing to suppress that scalar are covered. |
| EX-12/13 (B3) | Eligible marks over hidden XY preserve bc; dormant XY mark has no visible range; pre-exclusion interior Q remains marked. Collapsed-gap post-exclusion Q is unmarked for all four expansion modes in the fixed characterization. Code-point/UTF-8/UTF-16 selected read/edit/reconstruction fixtures include emoji. |
| EX-14 (B3) | Excluded mark/unmark, cold insertion mark leakage, partial unmark plus independent eligible unmark, and selected authoring not copying excluded formatting. Real text/spans/marks reads and separate formatting-state patch replay. |
| Additional model test | **48 proptest cases**, valid chains of 1–17 Alice register replacements, arbitrary granted-context holes. Independent direct-successor candidate oracle, real transition replay, reconstruction and reinstatement. No random invalid operation fragments. |

EX-02/03 enumeration freezes and rereads every published capture, but does not separately redeliver each event in every schedule as EX-01 does. The finite bounds above are not exhaustive Automerge or authority-graph verification.

Additional checks: native control-only actor persistence; native versus ordinary empty-key predecessors; concurrent empty-key write; unknown target reevaluated on author arrival; ancestry/mixed-change/group rejection; full policy-only identity changes before control receipt; earlier-sorting actor insertion; foreign/stale capture and patch rejection; missing package bytes and altered provenance rejection; transaction-local selected reads; structural actor isolation against unobserved continuing-actor work; low-level edits into excluded but structurally observed maps.

## Captures, reads and transitions

A capture contains document identity, selected content heads, hash-keyed classification/reasons, received hashes (classification keys), integrated manifest, missing dependencies/frontiers/evidence, actor-author bindings and complete immutable policy inputs. It does **not** persist actor-table indices. Both endpoints compile against the same post-import graph into structural clocks plus selected operation membership. The selected set is a `BTreeSet<OpId>`, deliberately unoptimized.

Selected root hydration and conflict candidates call real Automerge helpers. Publication uses `DiffIter` and `PatchLog::make_scoped_patches`; full endpoint diffs are intentional. A separate inspection delta includes changed hashes and policy-only changes. `Transition::apply` verifies the entire before-capture and stages native hydrated patch replay before updating its observer.

`Session::edit` allocates the ordinary isolated transaction actor using captured **structural heads**, then compiles selected targets in the resulting actor table. Dependencies retain observed excluded history; predecessors come from the selected view. The transaction-local range starts at its allocated start operation, not at all actor history. Structurally available hidden objects remain editable. Authoring runs on a staged clone and imports the new original change through session validation.

`Package` is an **in-memory complete experiment export**: original native change bytes plus capture/policy/provenance, version 1. Restore decodes and integrates a fresh operation graph and checks the full capture. This is not a serialized durable envelope format. B3 captures the explicit session text encoding and preserves it on reconstruction; mismatched scopes reject. Code-point/UTF-8/UTF-16 are exercised; grapheme-rich-text replay remains unproven. Native document-only save/load retains R but **does not restore its external authority interpretation**; a test explicitly demonstrates that distinction.

## Commands and actual outcomes

All cargo commands were synchronous, run from `rust/`, with configured separate target directory and `--locked --offline`. Logs are under:

`/home/fintohaps/Developer/Ink+Switch/automerge-revocation-design/implementation-1/b/`

Initial B1 commands on `174274e7` (fix-wave final results follow):

```sh
cargo test --locked --offline -p automerge --features experimental-revocation --lib --tests
# full-feature-final.log: 499 passed, 0 failed, 1 ignored

cargo test --locked --offline -p automerge --lib --tests
# full-default-final.log: 473 passed, 0 failed, 1 ignored
```

The feature run has 211 passing library tests (27 prototype tests) plus 288 integration tests. Default has 185 passing library tests (one new gate-disabled test) plus 288 integration tests. Only the known non-root profile and unused `bench.0.debug` manifest warnings remain. No dependencies added. A first full feature run before the final few regression additions also passed 494 tests (`full-feature-pre-refactor.log`).

Formatting, run at repository root before committing:

```sh
jj diff --name-only | grep '\.rs$' | xargs rustfmt --edition 2021 --config skip_children=true
jj diff --name-only | grep '\.rs$' | xargs rustfmt --edition 2021 --config skip_children=true --check
# rustfmt-final.log: exit 0, no output
```

Self-review used `jj diff --git` / `jj diff --stat`; implementation diff saved as `checkpoint-final.diff`. An optional Python log-totalling command failed because `python3` is absent; this did not affect any test command. Counts above are sums of the recorded suite summaries.

### TDD evidence (including real failures)

For the table, `T` is the exact prefix:

`cargo test --locked --offline -p automerge --features experimental-revocation --lib`

| Command suffix after T | Red evidence | Green evidence |
|---|---|---|
| `prototype` | `red-01.log`: absent EX-01 API, compilation failure. `build-02.log` / `build-03.log`: missing trait import/exhaustive native-action arms. | `green-01.log`: EX-01 passes. |
| `prototype` then `prototype::tests::ex07` | `behavior-02.log`: **behavior failure**, restored attachment patch omitted existing child; 8 other tests including 1,920 schedules passed. | `green-07.log`: structural-existence exposure fix, replay passes. |
| `prototype::tests::evidence_only` | `red-status.log`: **behavior failure**, E2-before-R had no status notification. | `green-status.log`: full policy capture/delta. |
| `prototype::tests::ex03_fresh_context_is_pending` then `prototype::tests::ex03` | `red-context.log`: **behavior failure**, fresh A3 was excluded rather than pending. | `green-context.log`: both context tests pass. |
| `prototype::tests::selected_authoring` | `red-authoring-api.log`: missing API. `red-authoring-behavior.log`: **behavior failure**, hidden map rejected as invalid object. | `green-authoring.log`: selected targets, transaction-local reads, hidden object edit and unobserved-history isolation pass. |
| `prototype::tests::native_gate` then `prototype::tests::native` | `red-gate.log`: absent gated receive API. | `green-native.log`: 3 native/gate tests pass. |
| `prototype::tests::capture_export` then `prototype -- --skip ex01_all_1920` | `red-export.log`: missing export/restore API. | `green-map-checkpoint.log`: 18 tests pass. |
| `prototype::tests::inspection` | `red-inspection.log`: missing dependency manifest API. | `green-inspection.log`: queued inspection/provenance checks pass. |
| `prototype::tests::unsupported` | `red-unsupported.log`: **behavior failure**, unproven list accepted at session boundary. | `green-unsupported.log`: unsupported content/competing control rejected. |
| `prototype::tests::patch_observer` | `red-observer.log`: absent checked observer API. | `green-observer.log`: stale/foreign endpoints rejected atomically. |
| `prototype::tests::capture_scope` | `red-capture-heads.log`: **behavior failure**, unknown heads silently accepted. | `green-capture-heads.log`: explicit rejection before authoring. |

Some new tests passed immediately on the existing slice: direct-target EX-05/06, EX-02 baseline, native noninterference/roundtrip, schedules and generated register model. These are executed regressions, **not claimed independent behavioral red/green cycles**. All final behavior tests run in the full suites above.

## Protocol observations and costs

- Feature-disabled parsing rejects standalone action-8 changes and compact control-bearing documents. `prototype_gate_tests::prototype_gate_rejects_native_action_8_without_feature` also proves that ordinary incremental load **accepts an earlier valid prefix** and returns success when the following action-8 chunk is unsupported. This is a characterization of this feature-disabled code, not a universal claim about old readers. Logs: `gate-disabled-characterized.log`, final default suite.
- The first gate-disabled fixture setup tried to construct unsupported action 8 through the expanded builder and panicked during validation (`gate-disabled.log`). The corrected test uses actual feature-enabled native bytes (`gate-fixture-bytes.log`). The expanded builder is not a supported fail-closed boundary.
- Session receipt requires version/capability 1 **before** parsing all changes; wrong version or any failed validation publishes nothing. Native change bytes alone are not a safe delivery contract for unaware consumers.
- Control constructor currently requires a fresh actor, exactly one control per control-only change, canonical sentinel row, no predecessors, no expand/mark metadata. It retains frontier hashes only if covered by dependency ancestry; EX-01 uses R.deps=[A], retain=[H].
- Binary payload parsing, action conversion, reconstructed indexes, batch value state and scanning readers all needed native handling. Anonymization and expanded JSON serialization explicitly reject controls; they cannot safely rewrite or silently drop frontier meaning.
- EX-02 moves rather than removes provisional complexity: before H integrates, credible R is retained in a separate session archive and affects pending inspection. After integration its frontier is known. C's causal dependency on R is real, but external evidence still gates R and heads remain insufficient.
- EX-03 needs external authorization-context discrimination. This native design does not make grants self-contained.

## Concrete engineering costs and limitations

1. **Scan/clone cost:** each delivery stages a whole Session clone, scans the archive to find causally ready changes, repeatedly reconstructs change metadata, evaluates captures, compiles per-operation BTreeSets, and diffs complete endpoints. No production benchmark claim. Final feature library suite, including all schedules/property tests, took 73.52 seconds in the debug test profile; this is test runtime, not a comparative performance measurement.
2. **Storage/capture cost:** original Change archive duplicates graph storage; captures duplicate hash classifications, inspection and evidence/provenance. Export includes all received original bytes. No interval compression, canonical capture hashing or durable package serializer.
3. **Structural support is bounded evidence, not universal coverage:** B3 lifts list/text/counter/mark rejection with the regressions detailed below. Table creation and competing controls remain rejected. Fixed EX-08–14 and 32 generated formatting cases pass; general structural model generation, grapheme-rich-text patch replay, arbitrary nested rich-text block payloads and all rich-text conflict combinations remain unproven.
4. **Recovery remains limited:** EX-04 named scalar adoption now passes after fix wave 1 using the existing editing API. The test records A as the reviewed source locally; no durable adoption provenance format, general recovery, policy forks, signatures, Keyhive graph, unauthorized-versus-invalidated variants, multiple control composition or grant revocation.
5. **Private prototype boundary only:** native encoding is accepted by the feature build; session-specific ancestry, scope restrictions and group atomicity are not globally imposed on ordinary Automerge load/merge/sync. Ordinary raw control-bearing imports are transport/noninterference characterizations, not authorization-aware APIs. The experimental session is the supported interpretation path. No sync negotiation adapter, bundle route or strict compact-session import implementation.
6. **No optimized selected import-log proof:** transitions discard import logs and use endpoint diffs. The native batch patch test proves content noninterference under allow-all only. Map/list/text/block restored-subtree exposure and a simultaneous nested text edit are covered by B3; arbitrary nested conflict combinations are not.
7. **Trusted mock facts are essential:** actor-author bytes are external bindings checked for consistency and captured. Existing native author metadata is not used as a capability proof or reconciled by this mock adapter. Context associations are trusted immutable facts; delivering content before its required fresh-context association is outside the bundled fixture input contract. A `Context(h, g)` association overrides restriction applicability for h in this single-control model; it is not general context-versus-control evaluation. Extending beyond one control requires revisiting that boundary.
8. **Private API rough edges:** string errors, internal unwraps behind validated captured contexts, fresh-control actor restriction and full-capture comparisons are intentional disposable costs. No public production API is proposed.

## Fix wave 1 — reciprocal review disposition and executed evidence

**Status: DONE_WITH_CONCERNS. F1 corrected; scoped re-review APPROVED.** The initial passing 499-test suite did not cover pre-validation effectiveness. The supplied reciprocal review and coordinator ruling were read completely. No other model/reviewer was invoked.

### F1: validated control readiness precedes final exclusion

The behavioral red covered both eventual valid and invalid X:

1. H and Alice's A are integrated. Receive R with `deps=[X]`, `retain=[H]`; X is missing and H is known.
2. Without evidence, R authority is pending and A stays eligible with visible Camping.
3. `Authorize(R)` makes R's authority authorized while R remains structurally queued. **A must be Pending and hidden**, not Excluded. Both tests failed here on the reviewed code (`left: Excluded; right: Pending`).
4. Valid X includes H in its ancestry: receipt integrates X and validates/integrates R, then A becomes Excluded. This is a status-only Pending→Excluded transition with no content patches.
5. Invalid X does not include H: ancestry validation rejects the whole group. Neither X nor an independent change submitted in that group is published. Retrying rejects again. The before-view and full inspection capture remain unchanged.

`prototype/policy.rs` now returns unresolved frontier evaluation while R is not in the integrated manifest, even if all retained hashes are known. New `Reason::AwaitingControlValidation(R)` identifies the unresolved validation; the existing capture's `missing_dependencies[R]={X}` supplies the dependency detail and `unresolved_frontiers` correctly does **not** claim H is missing. Actual missing H retains `MissingFrontier(R)` for EX-02. Authority evaluation, R's own eligibility, native ready-loop validation and whole-group staging are unchanged.

**Remaining availability limitation:** after the rejected invalid-X group, the previously accepted R is still archived, authorized and queued; A remains Pending/hidden indefinitely absent other policy evidence. Rejection also prevents X from entering the live graph through that group. This is not final exclusion, and no statement removal, quarantine or automatic recovery semantics were invented. A production policy for invalid queued controls remains required. The tests explicitly preserve and reconstruct the pending capture and reread the earlier visible capture after subsequent input.

### Other findings

- **F2 retained as documented dissent:** reviewer suggested making invalidated R eligible as a retained statement. Per coordinator/spec, R eligibility continues to describe whether its control effect participates: pending authority→Pending, authorized→Eligible, invalidated→Excluded. R's native statement/history remains structurally retained in all cases; authority and integration stay separately inspectable. No requested semantics change was made.
- **F3:** added named EX-04 scalar adoption regression on the existing API; it passed immediately, so it is not claimed as a new behavioral-red cycle. It asserts Bob's new actor/op/hash, explicit mock author binding in exported capture inputs, observed dependencies, empty predecessors, original A bytes unchanged, A still excluded, and two ordinary candidates after restoration. EX-02/03 per-event duplicate enumeration and historical isolation via a dedicated API remain omitted; single-control context override cost is explicit above.
- **F4:** feature-enabled action errors now say 0–8; feature-disabled errors still say 0–7. Added assertion failed before correction. Other reported string-error/unwrap/scan/clone/legacy-validation-order costs remain unchanged. No structural work was added.

### Exact commands/results

All commands run synchronously from `rust/` except rustfmt, using the same configured separate target directory. Logs in the assigned `implementation-1/b/` directory:

| Command | Log and outcome |
|---|---|
| `cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::tests::native_queued_control` | `fix-1-red-f1.log`: **2 failed behavior tests**, exit 101, Excluded versus Pending. Same command after fix: `fix-1-green-f1.log`: **2 passed**, exit 0. |
| `cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::tests::ex04` | `fix-1-ex04.log`: **1 passed**, exit 0 on existing authoring implementation. |
| `cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::tests::native_action_diagnostic` | `fix-1-red-diagnostic.log`: **1 failed**, exit 101 (advertised 0–7 under enabled feature). Corrected diagnostic passes in focused/final runs. |
| `cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype -- --skip all_192` | `fix-1-focused.log`: **28 passed**, exit 0, including native/EX-01–07/model/diagnostic regressions. |
| `cargo test --locked --offline -p automerge --features experimental-revocation --lib all_192` | `fix-1-schedules.log`: **3 passed**, **4,032 schedules**, exit 0, 74.35s. Existing independent fixture oracles unchanged. |
| `cargo test --locked --offline -p automerge --features experimental-revocation --lib --tests` | `fix-1-full-feature.log`: **503 passed, 0 failed, 1 ignored**, exit 0 (215 lib, including 31 prototype tests, plus 288 integration). Library runtime 79.01s, not a performance benchmark. |
| `cargo test --locked --offline -p automerge --lib --tests` | `fix-1-full-default.log`: **473 passed, 0 failed, 1 ignored**, exit 0 (185 lib plus 288 integration), including feature-disabled diagnostic/gate. |

Only the two known manifest warnings remain. Targeted formatting was applied and then checked at repository root:

```sh
rustfmt --edition 2021 --config skip_children=true --check \
  rust/automerge/src/prototype/policy.rs \
  rust/automerge/src/prototype/tests.rs \
  rust/automerge/src/prototype/gate_tests.rs \
  rust/automerge/src/op_set2/types.rs
# fix-1-rustfmt.log: exit 0, no output
```

Complete fix diff self-reviewed with `jj diff --git` and saved as `fix-1-diff.patch`. No source changes follow the final fix-wave full-suite/formatting runs.

## B3 structural extension — implementation and evidence

Started 2026-09-12 17:47 UTC from approved repair/report `53836b40`. Source checkpoint committed at approximately 18:12 UTC, within the 40-minute bound. The supplied Fable re-review approves F1 and task quality; that review executed no tests. Coordinator-supplied repair reruns were 503/473 passed, 1 ignored; B3 ran its own full suites below. Native action 8, ancestry-validation gate, policy evaluator and context semantics were not changed.

### New interfaces and source changes

- `prototype/core.rs`: lift rejection of List/Text/Counter/Increment/Mark families; keep Table and competing-control rejection. `Capture` now includes `TextEncoding`; `Session::with_encoding` constructs explicitly, restore preserves encoding, and scope rejects mismatches. Plain `Transition::apply` skips standalone `Mark` because hydrated text has no formatting state and panics on those patches. This is explicitly a **plain projection observer**, not rich-text certification.
- `prototype/format_observer.rs` (**test only**): independent per-character mark maps plus block payloads. It replays `SpliceText` with insertion marks, `Mark` (including null removals), `DeleteSeq`, block `Insert`, nested block field patches, and before-path attachment deletion/replacement/index shifts. It compares this replay to the complete real selected `spans_for` result. Expected fixture formatting and the generated range model do not consult production eligibility/mark code. It never fills formatting from the after-view. Unhandled text patch kinds/grapheme indexing panic in this test observer rather than silently pretending support.
- `iter/list_range.rs`, `iter/spans.rs::push_block`: full exposure uses prior structural existence rather than selected participation, extending the reviewed map exposure fix to restored list elements and text blocks.
- `patches/patch_log.rs::ExposeQueue::flush_obj`: expose complete selected text through internal spans, retaining splice marks and block IDs/children instead of collapsing every block to a plain U+FFFC. Block entries remain native `Insert(Object(Map), id)` plus child patches; no `Span::Block` is discarded.
- `iter/spans.rs::SpanState::push_str`: added-text coalescing compares **complete after-formatting**, not the mark delta between endpoints. Unchanged mark boundaries still split restored text into marked/unmarked runs.
- New `prototype/structural_tests.rs`: 26 structural tests including 32 generated formatting cases and 24 receipt/batch schedules. `tests/prototype_exposure_compat.rs`: two control-free regressions run under both feature configurations. Saved minimized proptest seed retained in `proptest-regressions/prototype/structural_tests.txt`.

### Behaviors beyond catalogue headlines

EX-07 now covers: excluded text attachment with eligible text and marks; nested map/text restoration with simultaneous eligible Q delta and exact nonduplicated character count; excluded list-map element exposing eligible child; excluded text block exposing eligible child payload; and attachment exclusion/restoration replay in both directions. Control-free restoration verifies bold `bc`, a real paragraph block at index 2 and its `kind` property. Ordinary reverse-deletion diff also checks restored text spanning an unchanged mark boundary.

Sequence inspection resolves object IDs against the live actor table, reads visible values and their actual insertion element keys separately, and checks recorded insertion/predecessor identities. EX-08's selected replacement changes the value operation ID while all surviving element IDs retain order. EX-09 proves Y is a replacement of X's element, not a new insertion. An early actor import is followed by frozen reads.

Counter tests use real selected candidates and patch replay, not a numerical reducer. EX-11 with a retained old base yields 10 rather than 15; restoring B yields 105; a new selected-authored counter base 50 has its own candidate and does not inherit +5. Mixed scalar/counter conflicts are exercised in both exclusion directions.

Formatting fixtures compare actual spans, selected plain text and marks (not only JSON), replay transitions, freeze/reread captures, and reconstruct selected sessions. EX-14 includes standalone Mark patches and cold already-excluded mark import where insertion patches must not leak formatting. EX-13 distinguishes pre-exclusion interior Q (bold) from Q typed into the collapsed visible gap (not bold for any of None/Before/After/Both in this history); this is a characterization, not a new expansion policy.

Native route characterization constructs a mixed list/counter/text/mark history with R and passes it through direct changes, one batched apply, merge, compact load, and incremental change bytes. Every original change's bytes and heads match. Each route then feeds a session with explicit policy and yields the same selected hydrated state, formatting replay and reconstruction. **This does not prove selected optimized native import logs, session sync negotiation or authority persistence in native document bytes.**

The extra structural receipt test enumerates **6 permutations × 4 contiguous partitions = 24 schedules** of A/eligible text+mark B/R, with H and authoritative R evidence preloaded. Every published group checks independently expected plain/bold strings and replays rich patches; each event is redelivered, and all captures are reread. Existing EX-01/02/03 **4,032 schedules** remain unchanged and pass in final full runs. No broad structural permutation claim beyond those 24 schedules.

### TDD failures and fixes

Here `T` is `cargo test --locked --offline -p automerge --features experimental-revocation --lib`, run from `rust/`; all logs are in assigned `implementation-1/b/`.

| Command suffix / fixture | Actual red | Green / disposition |
|---|---|---|
| `prototype::structural_tests` (initial EX-08/09) | `extend-red-lists.log`: 3 behavior failures at map-only admission gate. | `extend-green-lists.log`: 3 pass after allowing lists. Core list ordering/replacement needed no further fix for those tests. |
| `prototype::structural_tests::ex1` (EX-10/11) | `extend-red-counters.log`: 3 rejected by counter gate. | `extend-counters-behavior.log`: 3 pass after permitting counters/increments. No counter folding fix. |
| `prototype::structural_tests::ex1` (EX-12–14) | `extend-red-text.log`: observer API setup compile failure (`ConcreteTextValue` needs `make_string`). `extend-red-text-behavior.log`: 2 rejected by text gate. `extend-text-behavior.log`: real plain-observer panic on standalone Mark. | `extend-green-marks.log`: plain observer explicitly excludes marks, separate rich observer replays them. |
| `prototype::structural_tests` (restoration) | `extend-red-exposure.log`: 3 behavior failures: lost formatting, collapsed/lost block payload and empty restored list-map child. | `extend-green-exposure.log`: all 11 then-current tests pass with spans-aware and structural exposure fixes. |
| `prototype::structural_tests` (text block) | `extend-red-block.log`: restored block child missing, 14 other tests pass. | `extend-green-block.log`: 15 pass with structural block exposure. |
| `prototype::structural_tests::ex12_unicode_selected` | `extend-red-encoding-api.log`: absent constructor. `extend-red-encoding-behavior.log`: UTF-8 reconstructs as code-point. | `extend-green-encoding.log`: explicit captured encoding preserved in all 3 tested encodings. |
| `prototype::structural_tests::model_formatted` then `prototype::structural_tests::restored_text_splice` | `extend-model-formatting.log`: generated case lost bold on restored X. Minimized to gap=1, extra=3, start_seed=9, len_seed=0; seed saved. `extend-red-mark-boundary.log`: fixed minimal regression also fails. | `extend-green-mark-boundary.log`: complete after-format comparison fixes restored splice coalescing; 21 then-current tests pass including generated cases. |
| `prototype::structural_tests::ex07_formatted_attachment_exclusion` | `extend-red-observer-parent.log`: **test observer bug**, parent DeleteMap did not clear text. | `extend-green-observer-parent.log`: captured-before-path replay added to observer; no production fix needed. |
| `prototype::structural_tests::ex0` then `…::ex08` | `extend-element-identities.log`: **test helper bug**, stale embedded actor index after early import yielded no elements. | `extend-element-identities-corrected.log`: resolve object through live table; value and element identities checked separately. |

Other extension variants passed first run: map-valued replacement, counter self-diffs/mixed scalar suppression, partial unmarks, most fixed mark range materialization, selected authoring, mixed native routes, 24 structural schedules and default-feature compatibility. They are executed evidence, not individually claimed red/green cycles. The generated formatting test demonstrates why fixed EX-12/13 alone was not sufficient.

### Final commands, totals and timings

```sh
# From rust/
cargo test --locked --offline -p automerge --features experimental-revocation --lib --tests
# extend-checkpoint-feature.log: 531 passed, 0 failed, 1 ignored, exit 0
# 241 lib (57 prototype tests, including 26 structural) + 290 integration

cargo test --locked --offline -p automerge --lib --tests
# extend-checkpoint-default.log: 475 passed, 0 failed, 1 ignored, exit 0
# 185 lib + 290 integration

cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::structural_tests
# extend-checkpoint-focused.log: 26 passed, exit 0; 1.08s test execution
```

Final feature library execution took **57.22s**, default library **7.86s**; the focused structural test run took **1.08s**, and the separate 24-schedule run **0.69s**. All are **debug test-run timings, not production or comparative benchmarks**. Full logs before the final extra schedule test (`extend-full-feature-first.log`, `extend-full-feature-final.log`, `extend-full-default-final.log`) are retained but superseded by `extend-checkpoint-*`.

Changed Rust files formatted and checked at source root with:

```sh
jj diff --name-only | grep '\.rs$' | xargs rustfmt --edition 2021 --config skip_children=true
jj diff --name-only | grep '\.rs$' | xargs rustfmt --edition 2021 --config skip_children=true --check
# extend-rustfmt-final.log: exit 0, no output
```

Only the known two manifest warnings remain. No dependency additions or environment changes. Source unchanged after these final commands; extension diff self-reviewed via `jj diff --git` and saved as `extend-checkpoint.diff`.

### Concrete size/cost and remaining limitations

At the tested source checkpoint, `wc -l` reports `core.rs` **596**, `policy.rs` **145**, `format_observer.rs` **195**, `structural_tests.rs` **1,223**, existing `tests.rs` **1,225**, `gate_tests.rs` **32**, `prototype.rs` **86**, and default compatibility tests **88**: **3,590 physical Rust lines** across those eight experiment/test files. This includes comments/tests, not a complexity score. Extension touches **9 Rust files**, one saved seed and local documentation. Cumulative experiment touches **28 Rust files**, plus Cargo manifest, one seed and three documentation files (**33 files**). Compared with the approved repair source, shared production mechanics changed in only list diff, spans diff and patch exposure; the pure authority evaluator/native action implementation is unchanged.

New runtime costs: rich restored-subtree exposure scans spans instead of a plain string; full-after-mark comparison may split more splices correctly. Captures add encoding. Session cloning, archive scans, per-op BTreeSets, full endpoint diffs and duplicate capture data remain. The rich replay observer is test-side only; applications would still need their own checked rich-text consumer.

Unsupported/unproven: table creation still rejected; competing controls still rejected; grapheme indexing is accepted by the existing Automerge constructor but **not proven by this extension's observer** (which explicitly panics); arbitrary PutSeq/scalar conflict transformations in text and arbitrarily nested rich-text inside block payloads are not covered by the test observer. In-memory Package is not a durable serialized envelope. No sync negotiation or selected optimized incremental-import-log proof, general recovery/provenance, full Keyhive, signatures, general multi-control contexts or invalid-control quarantine policy. The F1 invalid queued-control availability limitation is unchanged. EX-02/03 per-event duplicate enumeration and dedicated historical-isolation API remain prior omissions. No claim that every rich-text operation combination is correct follows from this finite catalogue.

## Minimal reproductions

From `rust/`:

```sh
# All B3 structural examples, rich patch replay, generated model and 24 schedules
cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::structural_tests

# Minimized restored-formatting failure found by property testing
cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::structural_tests::restored_text_splice

# Control-free compatibility for shared exposure/coalescing fixes
cargo test --locked --offline -p automerge --test prototype_exposure_compat

# Review F1: known H, missing X, then valid/invalid X; native validation gates exclusion
cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::tests::native_queued_control

# Named scalar adoption, without admitting the excluded original
cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::tests::ex04

# Three resolved authority checkpoints, original identity and C survival
cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::first_test::ex01_authority_checkpoints

# Full finite receipt/batch enumeration (all three fixtures)
cargo test --locked --offline -p automerge --features experimental-revocation --lib all_192

# Previously failing missing-child patch restoration
cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::tests::ex07

# Direct-target chains, then selected authoring/hidden object capability
cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::tests::ex0
cargo test --locked --offline -p automerge --features experimental-revocation --lib prototype::tests::selected_authoring

# Legacy-reader prefix acceptance and experimental gate necessity
cargo test --locked --offline -p automerge --lib prototype_gate
```

The pre-fix EX-07 minimal trace is: excluded `root.section = map M`; eligible `M.title = Plans`; invalidate R. Old diff emitted only `PutMap(section, M)` and replay produced an empty M. The fix makes map exposure test prior **structural existence**, not prior participation; current reproduction passes.

## Assessment

The running checkpoint demonstrates B's concrete causal benefit and native statement persistence with the approved pre-validation exclusion repair intact. B3 now supplies bounded real sequence/counter/rich-text evidence and exposed a formatting coalescing bug that fixed examples missed. Native controls still require external evidence snapshots, pre-integration provisional handling, non-prefix selection and nonlocal subtree patch treatment. Invalid queued controls retain a concrete pending-availability limitation under strict rejection. Ready for coordinator-managed extension review and comparative evidence collection; no production selection or universal structural correctness claim.

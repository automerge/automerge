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

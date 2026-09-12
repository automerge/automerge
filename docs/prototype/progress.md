# Prototype A progress

- [x] Plan written (`implementation-plan.md`).
- [x] RED: `ex01_three_checkpoints` fails to compile (`red-ex01.log`: no `eligibility` module).
- [x] GREEN: Clock mask (`contains` vs `covers`), `ClockRange::predates` → structural, `change_graph::{op_range,in_ancestry,iter_hashes}`, `eligibility::{evidence,session}`, `PatchLog::scope`, `inner.rs::exid_to_obj` structural check. `green-ex01.log`.
  - Fixture bug found: `set_author` resets the actor; fixtures must set author before actor.
  - Design fix: `scope_for` only requires classification of changes within the view's heads ancestry (before-scope compiles on post-import graph, ruling 5).

#![feature(proc_macro_hygiene)]

#[path = "../../automerge/src/automerge/signing/incoming/boundary_cutoff.rs"]
mod boundary_cutoff;
#[path = "../../automerge/src/automerge/signing/incoming/boundary_memo.rs"]
mod boundary_memo;
mod cutoff_spec;
#[path = "../actor_clock/proof.rs"]
mod history_proof;
#[path = "../actor_clock/spec.rs"]
mod history_spec;
#[path = "../incoming_authors/spec.rs"]
mod lookup_spec;
mod memo_proof;
mod memo_spec;
mod overlay_bridge;
mod proof;
mod spec;

#[path = "../../automerge/src/automerge/signing/incoming/walk.rs"]
mod walk;
#[path = "../incoming_walk/proof.rs"]
mod walk_proof;
#[path = "../incoming_walk/spec.rs"]
mod walk_spec;

#[path = "../../automerge/src/automerge/signing/incoming/boundary_summary.rs"]
mod boundary_summary;

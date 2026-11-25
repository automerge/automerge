#![feature(proc_macro_hygiene)]

mod exactness;
mod interpretation;
mod proof;
mod spec;

#[path = "../incoming_walk/memo_cut.rs"]
mod memo_cut;
#[path = "../../automerge/src/automerge/signing/incoming/walk.rs"]
mod walk;
#[path = "../incoming_walk/proof.rs"]
mod walk_proof;
#[path = "../incoming_walk/spec.rs"]
mod walk_spec;

#[path = "../../automerge/src/automerge/signing/incoming/boundary_summary.rs"]
mod boundary_summary;

#![feature(proc_macro_hygiene)]

mod memo_cut;
#[path = "proof.rs"]
mod walk_proof;
#[path = "spec.rs"]
mod walk_spec;

#[path = "../../automerge/src/automerge/signing/incoming/walk.rs"]
mod walk;

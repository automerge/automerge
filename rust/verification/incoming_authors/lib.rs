#![feature(proc_macro_hygiene)]

mod slot_spec;
mod spec;

mod chain_proof;
mod chain_spec;

#[path = "../../automerge/src/automerge/signing/incoming/actor_chains.rs"]
mod actor_chains;
#[path = "../../automerge/src/automerge/signing/incoming/actor_slots.rs"]
mod actor_slots;

#[path = "../../automerge/src/automerge/signing/incoming/author_checks.rs"]
mod author_checks;

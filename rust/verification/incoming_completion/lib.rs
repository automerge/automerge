#![feature(proc_macro_hygiene)]

#[path = "../actor_clock/admission_proof.rs"]
mod admission_proof;
#[path = "../actor_clock/admission_spec.rs"]
mod admission_spec;
mod clock_spec;
mod exact_publication;
#[path = "../incoming_authors/spec.rs"]
mod lookup_spec;
#[path = "../actor_clock/proof.rs"]
mod proof;
#[path = "../../automerge/src/clock/seq_slots.rs"]
mod seq_slots;
#[path = "../actor_clock/spec.rs"]
mod spec;

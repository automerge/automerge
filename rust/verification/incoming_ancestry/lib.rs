#![feature(proc_macro_hygiene)]

#[path = "../../automerge/src/automerge/signing/incoming/actor_input.rs"]
mod actor_input;
mod actor_input_spec;
#[path = "../incoming_authors/chain_proof.rs"]
mod chain_proof;
#[path = "../incoming_authors/chain_spec.rs"]
mod chain_spec;
mod cone_spec;
#[path = "../incoming_intrinsic/evaluation_spec.rs"]
mod evaluation_spec;
mod gate_spec;
#[path = "../actor_clock/spec.rs"]
mod history_spec;
mod live_spec;
#[path = "../incoming_authors/projection_spec.rs"]
mod projection_spec;
#[path = "../incoming_authors/source_spec.rs"]
mod source_spec;

#[path = "../../automerge/src/automerge/signing/incoming/actor_chains.rs"]
mod actor_chains;
#[path = "../../automerge/src/automerge/signing/incoming/author_checks.rs"]
mod author_checks;
#[path = "../incoming_authors/spec.rs"]
mod lookup_spec;
#[path = "../incoming_authors/slot_spec.rs"]
mod slot_spec;
#[path = "../incoming_walk/proof.rs"]
mod walk_proof;
#[path = "../incoming_walk/spec.rs"]
mod walk_spec;

#[path = "../../automerge/src/automerge/signing/incoming/actor_slots.rs"]
mod actor_slots;
#[path = "../../automerge/src/automerge/signing/incoming/ancestry_gate.rs"]
mod ancestry_gate;
#[path = "../../automerge/src/automerge/signing/incoming/walk.rs"]
mod walk;

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

#[path = "../../automerge/src/automerge/signing/incoming/actor_chains.rs"]
mod actor_chains;
#[path = "../../automerge/src/automerge/signing/incoming/actor_slots.rs"]
mod actor_slots;
#[path = "../actor_clock/admission_spec.rs"]
mod admission_spec;
#[path = "../../automerge/src/automerge/signing/incoming/ancestry_gate.rs"]
mod ancestry_gate;
#[path = "../../automerge/src/automerge/signing/incoming/author_checks.rs"]
mod author_checks;
mod canonical_composition;
#[path = "../incoming_authors/chain_proof.rs"]
mod chain_proof;
#[path = "../incoming_authors/chain_spec.rs"]
mod chain_spec;
#[path = "../incoming_ancestry/cone_spec.rs"]
mod cone_spec;
#[path = "../incoming_intrinsic/evaluation_spec.rs"]
mod evaluation_spec;
#[path = "../incoming_ancestry/gate_spec.rs"]
mod gate_spec;
#[path = "../incoming_ancestry/live_spec.rs"]
mod live_spec;
#[path = "../incoming_authors/projection_spec.rs"]
mod projection_spec;
mod ready_cutoff;
#[path = "../incoming_authors/slot_spec.rs"]
mod slot_spec;
#[path = "../incoming_authors/source_spec.rs"]
mod source_spec;

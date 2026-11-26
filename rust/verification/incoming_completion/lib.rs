#![feature(proc_macro_hygiene)]

#[path = "../actor_clock/admission_proof.rs"]
mod admission_proof;
#[path = "../actor_clock/admission_spec.rs"]
mod admission_spec;
#[path = "../../automerge/src/automerge/signing/incoming/candidate_origin.rs"]
mod candidate_origin;
mod clock_spec;
#[path = "../../automerge/src/automerge/signing/incoming/delivery.rs"]
mod delivery;
mod delivery_spec;
mod exact_publication;
#[path = "../actor_clock/proof.rs"]
mod history_proof;
#[path = "../actor_clock/spec.rs"]
mod history_spec;
#[path = "../incoming_authors/spec.rs"]
mod lookup_spec;
#[path = "../incoming_groups/membership_proof.rs"]
mod membership_proof;
mod origin_spec;
#[path = "../../automerge/src/automerge/signing/incoming/pending.rs"]
mod pending;
mod pending_spec;
#[path = "../../automerge/src/automerge/signing/incoming/publication.rs"]
mod publication;
mod publication_spec;
#[path = "../../automerge/src/clock/seq_slots.rs"]
mod seq_slots;

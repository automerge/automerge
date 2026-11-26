#![feature(proc_macro_hygiene)]

#[path = "../../automerge/src/automerge/signing/incoming/actor_slots.rs"]
mod actor_slots;
#[path = "../../automerge/src/automerge/signing/incoming/apply_order.rs"]
mod apply_order;
#[path = "../incoming_authors/spec.rs"]
mod lookup_spec;
#[path = "../incoming_groups/order_spec.rs"]
mod order_spec;
#[path = "../../automerge/src/automerge/signing/incoming/prepare_plan.rs"]
mod prepare_plan;
#[path = "../incoming_plan/prepare_spec.rs"]
mod prepare_spec;
#[path = "../incoming_plan/schedule_spec.rs"]
mod schedule_spec;
#[path = "../incoming_authors/slot_spec.rs"]
mod slot_spec;

mod admission {
    pub(crate) use crate::projection::{select as select_mask, FragmentView};
}
#[path = "../../automerge/src/admission/projection.rs"]
mod projection;
#[path = "../fragment_selection/projection_spec.rs"]
mod projection_spec;
#[path = "../incoming_topology/proof.rs"]
mod proof;
#[path = "../../automerge/src/admission/selection.rs"]
mod selection;
#[path = "../fragment_selection/selection_proof.rs"]
mod selection_proof;
#[path = "../fragment_selection/selection_spec.rs"]
mod selection_spec;
#[path = "../incoming_topology/spec.rs"]
mod spec;

#[path = "../actor_clock/admission_proof.rs"]
mod admission_proof;
#[path = "../actor_clock/admission_spec.rs"]
mod admission_spec;
#[path = "../incoming_plan/composition.rs"]
mod composition;
#[path = "../actor_clock/proof.rs"]
mod history_proof;
#[path = "../actor_clock/spec.rs"]
mod history_spec;

#[path = "../../automerge/src/automerge/signing/incoming/candidate_origin.rs"]
mod candidate_origin;
#[path = "../incoming_plan/entry_spec.rs"]
mod entry_spec;
#[path = "../incoming_completion/origin_spec.rs"]
mod origin_spec;
#[path = "../../automerge/src/automerge/signing/incoming/pending.rs"]
mod pending;
#[path = "../incoming_completion/pending_spec.rs"]
mod pending_spec;
#[path = "../../automerge/src/automerge/signing/incoming/prepare_entry.rs"]
mod prepare_entry;

#[path = "../../automerge/src/automerge/signing/incoming/admission_txn.rs"]
mod admission_txn;
#[path = "../../automerge/src/automerge/signing/incoming/apply_payloads.rs"]
mod apply_payloads;
mod payload_spec;
#[path = "../../automerge/src/automerge/signing/incoming/publication.rs"]
mod publication;
#[path = "../incoming_completion/publication_spec.rs"]
mod publication_spec;
mod transaction_spec;

mod clock_model;
#[path = "../incoming_completion/clock_spec.rs"]
mod clock_spec;
mod document_spec;
mod effect_model;
#[path = "../../automerge/src/clock/seq_slots.rs"]
mod seq_slots;

mod plan_bridge;

#[path = "spec.rs"]
mod abstract_spec;
mod linked;
mod refinement;

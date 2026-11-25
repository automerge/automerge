#![feature(proc_macro_hygiene)]

mod batch_spec;
#[path = "../../automerge/src/automerge/signing/incoming/canonical_shape.rs"]
mod canonical_shape;
mod canonical_spec;
mod chain_spec;
#[path = "../../automerge/src/automerge/signing/incoming/evaluation.rs"]
mod evaluation;
#[path = "../../automerge/src/automerge/signing/incoming/evaluation_batch.rs"]
mod evaluation_batch;
mod evaluation_spec;
mod index_spec;
#[path = "../../automerge/src/automerge/signing/incoming/intrinsic_chain.rs"]
mod intrinsic_chain;
#[path = "../../automerge/src/automerge/signing/incoming/intrinsic_index.rs"]
mod intrinsic_index;
#[path = "../../automerge/src/automerge/signing/incoming/intrinsic_shape.rs"]
mod intrinsic_shape;
#[path = "../incoming_authors/spec.rs"]
mod lookup_spec;
#[path = "../incoming_groups/membership_proof.rs"]
mod membership_proof;
#[path = "../../automerge/src/automerge/signing/incoming/plan_contents.rs"]
mod plan_contents;
mod plan_spec;
mod shape_spec;
#[path = "../../automerge/src/automerge/signing/incoming/snapshot_index.rs"]
mod snapshot_index;
mod snapshot_spec;
#[path = "../../automerge/src/automerge/signing/incoming/topology_input.rs"]
mod topology_input;
mod topology_input_spec;

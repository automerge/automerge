#![feature(proc_macro_hygiene)]

mod chain_spec;
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
mod shape_spec;

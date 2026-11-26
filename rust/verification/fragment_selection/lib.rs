#![feature(proc_macro_hygiene)]

#[path = "../incoming_authors/spec.rs"]
mod lookup_spec;
#[path = "../../automerge/src/admission/projection.rs"]
mod projection;
mod projection_spec;

// The two kernels use the same per-occurrence edge-counting lemmas.
#[path = "../incoming_topology/proof.rs"]
mod proof;
#[path = "../incoming_topology/spec.rs"]
mod spec;

mod selection_proof;
mod selection_spec;

#[path = "../../automerge/src/admission/selection.rs"]
mod selection;

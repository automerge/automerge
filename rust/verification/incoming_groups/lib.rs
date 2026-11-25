#![feature(proc_macro_hygiene)]

mod cache_spec;
mod evidence_spec;
mod frontier_spec;
mod membership_proof;
mod order_spec;
#[path = "../../automerge/src/signature/fragment/provisional.rs"]
mod provisional;
mod provisional_soundness;
mod provisional_spec;
#[path = "../../automerge/src/signature/fragment/scoped.rs"]
mod scoped;
#[path = "../../automerge/src/signature/fragment/scoped_cache.rs"]
mod scoped_cache;
mod scoped_soundness;
mod scoped_spec;

#[path = "../../automerge/src/automerge/signing/incoming/apply_order.rs"]
mod apply_order;
#[path = "../../automerge/src/signature/fragment/frontiers.rs"]
mod frontiers;
#[path = "../../automerge/src/automerge/signing/incoming/group_membership.rs"]
mod group_membership;
#[path = "../incoming_authors/spec.rs"]
mod lookup_spec;

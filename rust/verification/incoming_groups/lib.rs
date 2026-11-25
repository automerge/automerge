#![feature(proc_macro_hygiene)]

mod evidence_spec;
mod frontier_spec;
mod membership_proof;

#[path = "../../automerge/src/signature/fragment/evidence.rs"]
mod evidence;
#[path = "../../automerge/src/signature/fragment/frontiers.rs"]
mod frontiers;
#[path = "../../automerge/src/automerge/signing/incoming/group_membership.rs"]
mod group_membership;
#[path = "../incoming_authors/spec.rs"]
mod lookup_spec;

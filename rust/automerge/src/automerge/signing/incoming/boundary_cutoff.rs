//! Interpret the snapshot's applied and candidate boundary maxima.

#![allow(unexpected_cfgs)]

use super::boundary_summary::Summary;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    cutoff_spec::*,
    lookup_spec::{model, modeled},
    spec::{laws, summary_inv, view},
};
#[cfg(automerge_verus)]
use vstd::prelude::*;

/// Never test an applied node against a candidate maximum or vice versa.
#[cfg_attr(automerge_verus, verus_spec(inside =>
    requires laws::<A,K>(), summary_inv(summary), usable_cutoff(hash, applied_position, applied_max, candidate_position),
    ensures inside == cutoff(view(summary), hash, applied_position, applied_max, candidate_position),
))]
pub(super) fn covers<'a, A: Clone + Eq + Hash + 'a, K: Copy + Ord>(
    summary: &Summary<A, K>,
    hash: K,
    applied_position: impl Fn(K) -> Option<(usize, u32)>,
    applied_max: impl Fn(usize) -> Option<u32>,
    candidate_position: impl Fn(K) -> (&'a A, u64),
) -> bool {
    let position = applied_position(hash);
    #[cfg(automerge_verus)]
    proof! { assert(position==model(applied_position,hash)); }
    if let Some((slot, seq)) = position {
        let maximum = applied_max(slot);
        #[cfg(automerge_verus)]
        proof! { assert(maximum==model(applied_max,slot)); }
        match maximum {
            Some(maximum) => seq <= maximum,
            None => false,
        }
    } else {
        let (actor, seq) = candidate_position(hash);
        #[cfg(automerge_verus)]
        proof! { assert((actor,seq)==model(candidate_position,hash)); }
        match summary.maxima.get(actor) {
            Some(maximum) => seq <= *maximum,
            None => false,
        }
    }
}

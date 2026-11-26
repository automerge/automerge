//! Materialize the exact apply schedule from its owned delivery cohort.

#![allow(unexpected_cfgs)]

use super::candidate_origin::Candidate;
use std::collections::HashMap;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    payload_spec::*,
};
#[cfg(automerge_verus)]
use vstd::{prelude::*, std_specs::hash::hash_map_inv};

/// Decode every scheduled hash from this prepared cohort. Equal-hash
/// overlapping deliveries retain the same last-occurrence policy as before.
#[cfg_attr(automerge_verus, verus_spec(out=>
    requires hash_laws::<H>(), sources(candidates@,members,key), covered(candidates@,schedule@,members,key),
    ensures exact_payloads(candidates@,schedule@,members,key,out@),
))]
pub(super) fn materialize<'a, G: 'a, T: 'a, H: Copy + Eq + Hash>(
    candidates: &'a [Candidate<G>],
    schedule: &[H],
    members: impl Fn(&'a Candidate<G>) -> &'a [T],
    key: impl Fn(&'a T) -> H,
) -> Vec<&'a T> {
    let mut by_hash = HashMap::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=candidates@.len(),hash_laws::<H>(),hash_map_inv(&by_hash),sources(candidates@,members,key),
            indexed(candidates@,members,key,i as int,0,by_hash@),
        decreases candidates@.len()-i,
    ))]
    while i < candidates.len() {
        let group = &candidates[i];
        #[cfg(automerge_verus)]
        proof! {assert(modeled(members,group));}
        let changes = members(group);
        #[cfg(automerge_verus)]
        proof! {assert(changes==model(members,group));}
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant i<candidates@.len(),j<=changes@.len(),hash_laws::<H>(),hash_map_inv(&by_hash),sources(candidates@,members,key),
                changes==model(members,&candidates@[i as int]),indexed(candidates@,members,key,i as int,j as int,by_hash@),
            decreases changes@.len()-j,
        ))]
        while j < changes.len() {
            let change = &changes[j];
            #[cfg(automerge_verus)]
            proof_decl! {let ghost before=by_hash@;assert(modeled(key,change));}
            let hash = key(change);
            #[cfg(automerge_verus)]
            proof! {assert(hash==model(key,change));}
            by_hash.insert(hash, change);
            #[cfg(automerge_verus)]
            proof! {add_payload(candidates@,members,key,i as int,j as int,before,by_hash@);}
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! {finish_group(candidates@,members,key,i as int,by_hash@);}
        i += 1;
    }
    let mut data = Vec::with_capacity(schedule.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=schedule@.len(),hash_laws::<H>(),hash_map_inv(&by_hash),covered(candidates@,schedule@,members,key),
            indexed(candidates@,members,key,candidates@.len() as int,0,by_hash@),
            data@.len()==i,forall|j:int| 0<=j<i ==> #[trigger] data@[j]==by_hash@[schedule@[j]],
        decreases schedule@.len()-i,
    ))]
    while i < schedule.len() {
        let hash = schedule[i];
        #[cfg(automerge_verus)]
        proof! {assert(by_hash@.contains_key(hash));}
        let change = *by_hash
            .get(&hash)
            .expect("scheduled change belongs to the prepared cohort");
        data.push(change);
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! {finish_payloads(candidates@,schedule@,members,key,by_hash@,data@);}
    data
}

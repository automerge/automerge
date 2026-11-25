//! Compare two deliveries' member sets without ordering their changes.

#![allow(unexpected_cfgs)]

use std::collections::HashSet;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    delivery_spec::*,
    lookup_spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::{prelude::*, std_specs::hash::hash_set_inv};

/// The right-hand delivery has already passed intrinsic duplicate checking.
/// Retain the original defensive check of uniqueness on the left-hand side.
#[cfg_attr(automerge_verus, verus_spec(equal =>
    requires hash_laws::<K>(), hash_projection(left@, hash), hash_projection(right@, hash),
        hashes(right@,hash).no_duplicates(),
    ensures equal <==> same_hashes(left@,right@,hash),
))]
pub(super) fn same_members<T, K: Copy + Eq + Hash>(
    left: &[T],
    right: &[T],
    hash: impl Fn(&T) -> K,
) -> bool {
    if left.len() != right.len() {
        #[cfg(automerge_verus)]
        proof! { lengths_differ(left@,right@,hash); }
        return false;
    }
    let mut set = HashSet::with_capacity(left.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=left@.len(), hash_laws::<K>(), hash_set_inv(&set), hash_projection(left@,hash),
            set@ =~= hashes(left@.take(i as int),hash).to_set(),
        decreases left@.len()-i,
    ))]
    while i < left.len() {
        let member = &left[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(hash,member)); }
        let key = hash(member);
        #[cfg(automerge_verus)]
        proof! { assert(key==model(hash,member)); set_step(left@,hash,i as int); }
        set.insert(key);
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { assert(left@.take(i as int) =~= left@); }
    if set.len() != left.len() {
        #[cfg(automerge_verus)]
        proof! { if hashes(left@,hash).no_duplicates() { hashes(left@,hash).unique_seq_to_set(); } }
        return false;
    }
    #[cfg(automerge_verus)]
    proof! { hashes(left@,hash).lemma_no_dup_set_cardinality(); }
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=right@.len(), left@.len()==right@.len(), hash_laws::<K>(), hash_set_inv(&set),
            hash_projection(right@,hash), hashes(left@,hash).no_duplicates(), hashes(right@,hash).no_duplicates(),
            set@ =~= hashes(left@,hash).to_set(),
            forall|j:int| 0<=j<i ==> set@.contains(#[trigger] model(hash,&right@[j])),
        decreases right@.len()-i,
    ))]
    while i < right.len() {
        let member = &right[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(hash,member)); }
        let key = hash(member);
        #[cfg(automerge_verus)]
        proof! { assert(key==model(hash,member)); }
        if !set.contains(&key) {
            #[cfg(automerge_verus)]
            proof! {
                assert(hashes(right@,hash)[i as int]==key);
                assert(hashes(right@,hash).contains(key));
            }
            return false;
        }
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { equal_sets(left@,right@,hash,set@); }
    true
}

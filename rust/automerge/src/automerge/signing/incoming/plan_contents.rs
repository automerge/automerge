//! Project verified occurrence indices into the exact plan contents.

#![allow(unexpected_cfgs)]

use std::collections::HashMap;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    plan_spec::*,
};
#[cfg(automerge_verus)]
use vstd::prelude::*;

/// The inverse of the exact candidate or resolved-order sequence.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires crate::shape_spec::hash_laws::<K>(), hashes@.no_duplicates(),
    ensures inverse(hashes@, hashes@.len() as int, out@),
))]
pub(super) fn positions<K: Copy + Eq + Hash>(hashes: &[K]) -> HashMap<K, usize> {
    let mut out = HashMap::with_capacity(hashes.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= hashes@.len(), hashes@.no_duplicates(), crate::shape_spec::hash_laws::<K>(),
            vstd::std_specs::hash::hash_map_inv(&out), inverse(hashes@, i as int, out@),
        decreases hashes@.len() - i,
    ))]
    while i < hashes.len() {
        #[cfg(automerge_verus)]
        proof_decl! { let ghost prior = out@; }
        out.insert(hashes[i], i);
        #[cfg(automerge_verus)]
        proof! { position_added(hashes@, i as int, prior, out@); }
        i += 1;
    }
    out
}

/// Materialize the plan in the same order without another sort or filter.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires forall|i:int| 0<=i<order@.len() ==> modeled(resolve,#[trigger] order@[i]),
    ensures out@.len()==order@.len(),
        forall|i:int| 0<=i<order@.len() ==> #[trigger] out@[i]==model(resolve,order@[i]),
))]
pub(super) fn ordered_refs<'a, K: Copy, V>(
    order: &[K],
    resolve: impl Fn(K) -> &'a V,
) -> Vec<&'a V> {
    let mut out = Vec::with_capacity(order.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= order@.len(), out@.len()==i,
            forall|j:int| 0<=j<order@.len() ==> modeled(resolve,#[trigger] order@[j]),
            forall|j:int| 0<=j<i ==> #[trigger] out@[j]==model(resolve,order@[j]),
        decreases order@.len()-i,
    ))]
    while i < order.len() {
        let key = order[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(resolve,key)); }
        let value = resolve(key);
        #[cfg(automerge_verus)]
        proof! { assert(value==model(resolve,key)); }
        out.push(value);
        i += 1;
    }
    out
}

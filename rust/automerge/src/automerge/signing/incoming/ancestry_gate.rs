//! Check equivocation in the exact unapplied dependency cone of one head.

#![allow(unexpected_cfgs)]

use super::{
    actor_slots,
    walk::{walk, Visit},
};
use std::hash::Hash;
use std::marker::PhantomData;

#[cfg(automerge_verus)]
use crate::{gate_spec::*, walk_spec};
#[cfg(automerge_verus)]
use vstd::prelude::*;

/// A batch precheck and the exact same slot lookup used for every cone.
/// A collision elsewhere in the batch merely requires a narrower check.
#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Guard<'s, K, S, F> {
    #[cfg(automerge_verus)]
    pub(super) hashes: &'s [K],
    pub(super) slot: F,
    pub(super) conflict: Option<K>,
    pub(super) kind: PhantomData<(&'s [K], S)>,
}

#[cfg_attr(automerge_verus, verus_verify)]
impl<'s, K: Copy + Eq + Hash, S: Eq + Hash, F: Fn(K) -> (S, bool)> Guard<'s, K, S, F> {
    #[cfg_attr(automerge_verus, verus_spec(out =>
        requires slot_laws::<S>(), slot_inputs(hashes@,slot),
        ensures out.hashes==hashes, out.slot==slot, screened(&out),
    ))]
    pub(super) fn new(hashes: &'s [K], slot: F) -> Self {
        let conflict = actor_slots::find_conflict(hashes, &slot);
        Self {
            #[cfg(automerge_verus)]
            hashes,
            slot,
            conflict,
            kind: PhantomData,
        }
    }

    /// The view stops at applied history and keeps only unapplied ancestors.
    /// The no-conflict path need not walk at all.
    #[cfg_attr(automerge_verus, verus_spec(out =>
        requires hash_laws::<K>(), vstd::pervasive::obeys_clone_eq::<K>(), slot_laws::<S>(), screened(self),
            exists|nodes:Set<K>| #[trigger] walk_spec::supports(seq![head],parents,classify,nodes),
            cone_is_in_batch(walk_spec::model(seq![head],parents,classify),seq![head],self.hashes@),
        ensures cone_result(walk_spec::model(seq![head],parents,classify),seq![head],self.slot,out),
    ))]
    pub(super) fn check(
        &self,
        head: K,
        parents: impl Fn(K) -> Vec<K>,
        classify: impl Fn(K) -> Visit,
    ) -> Option<K> {
        #[cfg(automerge_verus)]
        proof_decl! { let ghost roots=seq![head];let ghost graph=walk_spec::model(roots,parents,classify); }
        #[cfg(automerge_verus)]
        proof! {
            if self.conflict.is_none() { clean_batch_implies_clean_cone(graph,roots,self.hashes@,self.slot); }
        }
        self.conflict?;
        let start = vec![head; 1];
        #[cfg(automerge_verus)]
        proof! {
            assert(vstd::pervasive::cloned::<K>(head,start@[0]));
            assert(start@ =~= roots);
        }
        let cone = walk(start, parents, classify);
        #[cfg(automerge_verus)]
        proof! { prepare_scan(graph,roots,self.hashes@,self.slot,cone@); }
        let result = actor_slots::find_conflict(&cone, &self.slot);
        #[cfg(automerge_verus)]
        proof! { finish_scan(graph,roots,self.slot,cone@,result); }
        result
    }
}

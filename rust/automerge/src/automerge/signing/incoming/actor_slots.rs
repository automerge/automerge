//! Detect conflicting actor sequences in a set of distinct candidate hashes.

#![allow(unexpected_cfgs)]

use std::collections::HashSet;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    slot_spec::*,
    spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::laws_eq::obeys_concrete_eq;
#[cfg(automerge_verus)]
use vstd::prelude::*;
#[cfg(automerge_verus)]
use vstd::std_specs::hash::{hash_set_inv, obeys_key_model};

/// A hash conflicts if its slot is already applied or was used by an earlier
/// distinct candidate. Applied hashes themselves are not part of `candidates`.
#[cfg_attr(automerge_verus, verus_spec(conflict =>
    requires obeys_key_model::<S>(), obeys_concrete_eq::<S>(), candidates@.no_duplicates(),
        forall|i: int| 0 <= i < candidates@.len() ==> modeled(slot, #[trigger] candidates@[i]),
    ensures conflict_result(candidates@, slot, conflict),
))]
pub(super) fn find_conflict<K: Copy, S: Eq + Hash>(
    candidates: &[K],
    slot: impl Fn(K) -> (S, bool),
) -> Option<K> {
    let mut seen = HashSet::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant obeys_key_model::<S>(), obeys_concrete_eq::<S>(), hash_set_inv(&seen),
            i <= candidates@.len(), candidates@.no_duplicates(),
            forall|j: int| 0 <= j < candidates@.len() ==> modeled(slot, #[trigger] candidates@[j]),
            consistent(candidates@, slot, i as int),
            remembers(candidates@, slot, i as int, seen@),
        decreases candidates@.len() - i,
    ))]
    while i < candidates.len() {
        let hash = candidates[i];
        #[cfg(automerge_verus)]
        proof_decl! { let ghost before = seen@; assert(modeled(slot, hash)); }
        let (key, in_applied) = slot(hash);
        #[cfg(automerge_verus)]
        proof! { assert(model(slot, hash) == (key, in_applied)); }
        if in_applied || !seen.insert(key) {
            #[cfg(automerge_verus)]
            proof! { report_conflict(candidates@, slot, i as int, before, in_applied); }
            return Some(hash);
        }
        #[cfg(automerge_verus)]
        proof! { no_conflict(candidates@, slot, i as int, before, seen@); }
        i += 1;
    }
    None
}

#[cfg(test)]
#[path = "actor_slots/tests.rs"]
mod tests;

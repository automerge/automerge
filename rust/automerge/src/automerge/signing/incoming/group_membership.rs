//! Exact delivered-set comparison after canonical graph traversal.

#![allow(unexpected_cfgs)]

use std::collections::HashSet;
use std::hash::Hash;

#[cfg(automerge_verus)]
use vstd::laws_eq::obeys_concrete_eq;
#[cfg(automerge_verus)]
use vstd::prelude::*;
#[cfg(automerge_verus)]
use vstd::std_specs::hash::{hash_set_inv, obeys_key_model};

/// The canonical walk has no duplicates. Length plus inclusion therefore
/// implies equality, not merely that the delivery contains the canonical set.
#[cfg_attr(automerge_verus, verus_spec(matches =>
    requires obeys_key_model::<K>(), obeys_concrete_eq::<K>(), hash_set_inv(delivered),
        canonical@.no_duplicates(),
    ensures matches <==> canonical@.to_set() =~= delivered@,
))]
pub(super) fn matches<K: Eq + Hash>(canonical: &[K], delivered: &HashSet<K>) -> bool {
    #[cfg(automerge_verus)]
    proof! { canonical@.unique_seq_to_set(); }
    if canonical.len() != delivered.len() {
        return false;
    }
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant obeys_key_model::<K>(), obeys_concrete_eq::<K>(), hash_set_inv(delivered),
            canonical@.no_duplicates(), i <= canonical@.len(), canonical@.to_set().len() == delivered@.len(),
            forall|j: int| 0 <= j < i ==> delivered@.contains(#[trigger] canonical@[j]),
        decreases canonical@.len() - i,
    ))]
    while i < canonical.len() {
        if !delivered.contains(&canonical[i]) {
            #[cfg(automerge_verus)]
            proof! { assert(canonical@.contains(canonical@[i as int])); }
            return false;
        }
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { crate::membership_proof::equal_cardinality(canonical@, delivered@); }
    true
}

#[cfg(test)]
mod tests {
    use super::matches;
    use std::collections::HashSet;

    #[test]
    fn rejects_missing_extra_and_substituted_members_regardless_of_order() {
        let delivered = HashSet::from([1, 2, 3]);
        assert!(matches(&[3, 1, 2], &delivered));
        assert!(!matches(&[3, 1], &delivered));
        assert!(!matches(&[3, 1, 2, 4], &delivered));
        assert!(!matches(&[3, 1, 4], &delivered));
    }
}

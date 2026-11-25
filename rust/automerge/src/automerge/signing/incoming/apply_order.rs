//! Build a duplicate-free schedule from selected, already-resolved members.

#![allow(unexpected_cfgs)]

use std::collections::HashSet;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    order_spec::*,
};
#[cfg(automerge_verus)]
use vstd::prelude::*;
#[cfg(automerge_verus)]
use vstd::std_specs::hash::hash_set_inv;

/// Applied members project to `None`. All other members project to their
/// positions in the resolved topological order. Repeated members across
/// fragments contribute just one rank.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires forall|i: int| 0 <= i < members@.len() ==> modeled(rank, #[trigger] members@[i]),
    ensures out@.no_duplicates(), exactly_selected(members@, rank, members@.len() as int, out@),
))]
pub(super) fn collect_ranks<K: Copy>(
    members: &[K],
    rank: impl Fn(K) -> Option<usize>,
) -> Vec<usize> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= members@.len(), hash_set_inv(&seen), seen@ == out@.to_set(), out@.no_duplicates(),
            forall|j: int| 0 <= j < members@.len() ==> modeled(rank, #[trigger] members@[j]),
            exactly_selected(members@, rank, i as int, out@),
        decreases members@.len() - i,
    ))]
    while i < members.len() {
        let member = members[i];
        #[cfg(automerge_verus)]
        proof_decl! { let ghost before = out@; let ghost prior_seen = seen@; assert(modeled(rank, member)); }
        let position = rank(member);
        #[cfg(automerge_verus)]
        proof! { assert(position == model(rank, member)); }
        if let Some(position) = position {
            if seen.insert(position) {
                out.push(position);
            }
        }
        #[cfg(automerge_verus)]
        proof! { include_member(members@, rank, i as int, before, prior_seen, out@, seen@); }
        i += 1;
    }
    out
}

/// Sort the chosen ranks, preserving their exact membership and uniqueness.
#[cfg_attr(automerge_verus, verus_spec(
    requires old(ranks)@.no_duplicates(),
    ensures final(ranks)@.no_duplicates(), final(ranks)@.to_set() == old(ranks)@.to_set(),
        increasing(final(ranks)@),
))]
pub(super) fn sort_ranks(ranks: &mut [usize]) {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost before = ranks@; }
    ranks.sort_unstable();
    #[cfg(automerge_verus)]
    proof! { sorted_permutation(before, ranks@); }
}

#[cfg(test)]
mod tests {
    use super::{collect_ranks, sort_ranks};

    #[test]
    fn drops_applied_members_and_repeats_then_orders_the_exact_unapplied_set() {
        // The first selected fragment supplies ranks 9 and 1; the second
        // overlaps rank 9 and adds rank 5. Both mention the applied hash 0.
        let mut ranks = collect_ranks(&[9, 0, 1, 9, 5, 0], |hash| (hash != 0).then_some(hash));
        assert_eq!(ranks, [9, 1, 5]);
        sort_ranks(&mut ranks);
        assert_eq!(ranks, [1, 5, 9]);
    }
}

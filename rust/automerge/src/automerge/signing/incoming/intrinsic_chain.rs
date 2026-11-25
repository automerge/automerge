//! Check each actor's entire run of delivered members.

#![allow(unexpected_cfgs)]

use super::intrinsic_index::same;

#[cfg(automerge_verus)]
use crate::chain_spec::*;
#[cfg(automerge_verus)]
use vstd::prelude::*;

/// Rows are (actor, sequence, original member index). The dependency query
/// is made with (predecessor index, child index), never across actors.
#[cfg_attr(automerge_verus, verus_spec(result =>
    requires order_laws::<A>(), bounded(old(rows)@), stable_edges(old(rows)@, edge),
    ensures final(rows)@.to_multiset() == old(rows)@.to_multiset(),
        final(rows)@.len() == old(rows)@.len(), ordered(final(rows)@),
        chain_result(final(rows)@, edge, result),
))]
pub(super) fn check<A: Copy + Ord>(
    rows: &mut [(A, u64, usize)],
    edge: impl Fn(usize, usize) -> bool,
) -> Result<(), usize> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost before = rows@; }
    rows.sort_unstable();
    #[cfg(automerge_verus)]
    proof! { permutation_facts(before, rows@, edge); }
    let mut i = 1usize;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant
            1 <= i <= rows@.len() + 1, order_laws::<A>(), bounded(rows@), stable_edges(rows@, edge),
            valid_prefix(rows@, edge, i as int), ordered(rows@),
            rows@.to_multiset() == old(rows)@.to_multiset(), rows@.len() == old(rows)@.len(),
        decreases rows@.len() + 1 - i,
    ))]
    while i < rows.len() {
        let (left_actor, left_seq, left) = rows[i - 1];
        let (right_actor, right_seq, right) = rows[i];
        #[cfg(automerge_verus)]
        proof! {
            assert(rows@.contains(rows@[i as int - 1]));
            assert(rows@.contains(rows@[i as int]));
        }
        if same(&left_actor, &right_actor) {
            if right_seq != left_seq + 1 {
                #[cfg(automerge_verus)]
                proof! { broken_at(rows@, edge, i as int); }
                return Err(right);
            }
            #[cfg(automerge_verus)]
            proof! { assert(can_query(edge, left, right)); }
            let linked = edge(left, right);
            #[cfg(automerge_verus)]
            proof! { assert(linked == edge_value(edge, left, right)); }
            if !linked {
                #[cfg(automerge_verus)]
                proof! { broken_at(rows@, edge, i as int); }
                return Err(right);
            }
        }
        i += 1;
    }
    Ok(())
}

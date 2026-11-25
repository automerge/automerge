use crate::lookup_spec::model;
use vstd::prelude::*;
use vstd::std_specs::slice::{cmp_le, sorted_by_le};

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::seq_lib::group_to_multiset_ensures};

// The output is exactly the set of unapplied ranks in the member list, even
// if the same hash occurs in multiple independently verified fragments.
pub(crate) open spec fn exactly_selected<K, F: Fn(K) -> Option<usize>>(
    members: Seq<K>, rank: F, end: int, out: Seq<usize>,
) -> bool {
    forall|r: usize| #[trigger] out.contains(r) <==> exists|j: int| 0 <= j < end
        && #[trigger] model(rank, members[j]) == Some(r)
}

pub(crate) open spec fn increasing(out: Seq<usize>) -> bool {
    forall|i: int, j: int| 0 <= i < j < out.len() ==> #[trigger] out[i] < #[trigger] out[j]
}

pub(crate) proof fn include_member<K, F: Fn(K) -> Option<usize>>(
    members: Seq<K>, rank: F, i: int, before: Seq<usize>, seen: Set<usize>, next: Seq<usize>, after: Set<usize>,
)
    requires 0 <= i < members.len(), exactly_selected(members, rank, i, before),
        seen == before.to_set(), before.no_duplicates(),
        match model(rank, members[i]) {
            None => next == before && after == seen,
            Some(r) => after == seen.insert(r) && next == if seen.contains(r) { before } else { before.push(r) },
        },
    ensures exactly_selected(members, rank, i + 1, next), after == next.to_set(), next.no_duplicates(),
{
    if let Some(r) = model(rank, members[i]) {
        if !seen.contains(r) {
            assert forall|p: int, q: int| 0 <= p < q < next.len() implies next[p] != next[q] by {
                if q == before.len() {
                    assert(before.contains(before[p]));
                }
            }
        }
    }
}

pub(crate) proof fn sorted_permutation(before: Seq<usize>, after: Seq<usize>)
    requires before.no_duplicates(), after.to_multiset() == before.to_multiset(),
        sorted_by_le(after, |a: usize, b: usize| cmp_le(a, b)),
    ensures after.no_duplicates(), after.to_set() == before.to_set(), increasing(after),
{
    before.lemma_multiset_has_no_duplicates();
    after.lemma_multiset_has_no_duplicates_conv();
    assert(after.to_set() =~= before.to_set()) by {
        assert forall|r: usize| #[trigger] after.contains(r) <==> #[trigger] before.contains(r) by {
            vstd::seq_lib::to_multiset_contains(after, r);
            vstd::seq_lib::to_multiset_contains(before, r);
            assert(after.to_multiset().count(r) == before.to_multiset().count(r));
        }
    }
    assert forall|i: int, j: int| 0 <= i < j < after.len() implies #[trigger] after[i] < #[trigger] after[j] by {
        assert(cmp_le(after[i], after[j]));
    }
}

// Ranks name positions in the original dependency order. If the selected
// set includes every unapplied dependency of its members, sorting that set
// by rank yields an apply schedule that has each such dependency earlier.
pub(crate) proof fn dependencies_come_first(out: Seq<usize>, unapplied_parents: Seq<Vec<usize>>)
    requires increasing(out),
        forall|i: int| 0 <= i < out.len() ==> #[trigger] out[i] < unapplied_parents.len(),
        forall|i: int, p: usize| 0 <= i < out.len()
            && #[trigger] unapplied_parents[out[i] as int]@.contains(p) ==> out.contains(p) && p < out[i],
    ensures forall|i: int, p: usize| 0 <= i < out.len()
        && #[trigger] unapplied_parents[out[i] as int]@.contains(p) ==> out.take(i).contains(p),
{
    assert forall|i: int, p: usize| 0 <= i < out.len()
        && #[trigger] unapplied_parents[out[i] as int]@.contains(p) implies out.take(i).contains(p) by {
        let j = choose|j: int| 0 <= j < out.len() && out[j] == p;
        assert(j < i);
        assert(out.take(i)[j] == p);
    }
}

} // verus!

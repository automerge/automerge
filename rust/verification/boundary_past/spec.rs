use crate::boundary_summary::Summary;
use std::hash::Hash;
use vstd::laws_cmp::{obeys_cmp, obeys_cmp_concrete_eq};
use vstd::laws_eq::obeys_concrete_eq;
use vstd::pervasive::obeys_clone_eq;
use vstd::prelude::*;
use vstd::std_specs::{btree::*, hash::*};

verus! {

pub(crate) ghost struct SummaryView<A, K> {
    pub maxima: Map<A, u64>,
    pub seeds: Set<K>,
}

pub(crate) open spec fn view<A, K>(s: &Summary<A, K>) -> SummaryView<A, K> {
    SummaryView { maxima: s.maxima@, seeds: s.seeds@ }
}

pub(crate) open spec fn summary_inv<A: Eq + Hash, K: Ord>(s: &Summary<A, K>) -> bool {
    hash_map_inv(&s.maxima) && btree_set_inv(&s.seeds)
}

pub(crate) open spec fn laws<A: Clone + Eq + Hash, K: Copy + Ord>() -> bool {
    &&& obeys_key_model::<A>()
    &&& obeys_concrete_eq::<A>()
    &&& obeys_clone_eq::<A>()
    &&& obeys_cmp::<K>()
    &&& obeys_cmp_concrete_eq::<K>()
    &&& key_obeys_cmp_spec::<K>()
}

pub(crate) open spec fn empty_view<A, K>() -> SummaryView<A, K> {
    SummaryView { maxima: Map::empty(), seeds: Set::empty() }
}

pub(crate) open spec fn seed_view<A, K>(k: K) -> SummaryView<A, K> {
    SummaryView { maxima: Map::empty(), seeds: Set::empty().insert(k) }
}

pub(crate) open spec fn actor_view<A, K>(a: A, seq: u64) -> SummaryView<A, K> {
    SummaryView { maxima: Map::empty().insert(a, seq), seeds: Set::empty() }
}

pub(crate) open spec fn value<A>(clock: Map<A, u64>, a: A) -> Option<u64> {
    if clock.contains_key(a) { Some(clock[a]) } else { None }
}

pub(crate) open spec fn maximum(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (None, _) => right,
        (_, None) => left,
        (Some(l), Some(r)) => Some(if l >= r { l } else { r }),
    }
}

pub(crate) open spec fn max_join<A>(left: Map<A, u64>, right: Map<A, u64>, out: Map<A, u64>) -> bool {
    forall|a: A| #[trigger] value(out, a) == maximum(value(left, a), value(right, a))
}

// Seeds combine by set union and actor entries by attained maximum (absence
// does not create a zero entry).
pub(crate) open spec fn join<A, K>(left: SummaryView<A, K>, right: SummaryView<A, K>, out: SummaryView<A, K>) -> bool {
    &&& max_join(left.maxima, right.maxima, out.maxima)
    &&& out.seeds =~= left.seeds.union(right.seeds)
}

} // verus!

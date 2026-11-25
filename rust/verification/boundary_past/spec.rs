use crate::boundary_summary::{Contribution, Summary};
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

pub(crate) open spec fn projected<'a, A: 'a, K: 'a, F: Fn(K) -> Contribution<'a, A, K>>(
    project: F, k: K,
) -> Contribution<'a, A, K> {
    choose|c: Contribution<'a, A, K>| project.ensures((k,), c)
}

pub(crate) open spec fn models<'a, A: Eq + Hash + 'a, K: Ord + 'a, F: Fn(K) -> Contribution<'a, A, K>>(
    project: F, k: K,
) -> bool {
    let part = projected(project, k);
    &&& project.requires((k,))
    &&& project.ensures((k,), part)
    &&& forall|c: Contribution<'a, A, K>| #[trigger] project.ensures((k,), c) ==> c == part
    &&& match part { Contribution::Memoized(cached) => summary_inv(cached), _ => true }
}

pub(crate) open spec fn term<'a, A: 'a, K: 'a, F: Fn(K) -> Contribution<'a, A, K>>(
    project: F, node: K,
) -> SummaryView<A, K> {
    match projected(project, node) {
        Contribution::Applied(seed) => seed_view(seed),
        Contribution::Candidate(actor, seq) => actor_view(actor, seq),
        Contribution::Memoized(cached) => view(cached),
    }
}

// Each output maximum bounds the corresponding entries in the inputs and
// must be attained by one of them. The seed set is their exact union.
pub(crate) open spec fn aggregate<'a, A: 'a, K: 'a, F: Fn(K) -> Contribution<'a, A, K>>(
    nodes: Seq<K>, project: F, end: int, out: SummaryView<A, K>,
) -> bool {
    &&& forall|j: int, a: A| 0 <= j < end && #[trigger] term(project, nodes[j]).maxima.contains_key(a)
        ==> out.maxima.contains_key(a) && out.maxima[a] >= term(project, nodes[j]).maxima[a]
    &&& forall|a: A| #[trigger] out.maxima.contains_key(a) ==> exists|j: int| 0 <= j < end
        && #[trigger] term(project, nodes[j]).maxima.contains_key(a) && term(project, nodes[j]).maxima[a] == out.maxima[a]
    &&& forall|k: K| #[trigger] out.seeds.contains(k) <==> exists|j: int| 0 <= j < end
        && #[trigger] term(project, nodes[j]).seeds.contains(k)
}

} // verus!

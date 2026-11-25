use vstd::prelude::*;

verus! {

// The graph is indexed by candidate, with edges pointing to dependents.
// The initial counter includes each such edge occurrence and each missing
// dependency occurrence. Dependencies already in the applied graph are omitted.
pub open spec fn well_formed(edges: Seq<Vec<usize>>, initial: Seq<usize>) -> bool {
    &&& edges.len() == initial.len() <= usize::MAX
    &&& forall|p: int, j: int| 0 <= p < edges.len() && 0 <= j < edges[p]@.len()
        ==> #[trigger] edges[p]@[j] < edges.len()
    &&& forall|v: usize| v < initial.len() ==> #[trigger] initial[v as int]
        >= candidate_edges(edges, Seq::empty(), v)
}

// Count edges into v whose source is not in done. Multiplicity matters for
// the counters, but not for the graph property ready() below.
#[verifier::opaque]
pub open spec fn candidate_edges(edges: Seq<Vec<usize>>, done: Seq<usize>, v: usize) -> nat
    decreases edges.len(),
{
    if edges.len() == 0 {
        0
    } else {
        candidate_edges(edges.drop_last(), done, v)
            + if done.contains((edges.len() - 1) as usize) { 0 }
              else { edges.last()@.to_multiset().count(v) }
    }
}

pub open spec fn ready(
    edges: Seq<Vec<usize>>, initial: Seq<usize>, done: Seq<usize>, v: usize,
) -> bool {
    &&& v < edges.len()
    // No missing dependency outside the candidates and the applied graph.
    &&& initial[v as int] == candidate_edges(edges, Seq::empty(), v)
    &&& forall|p: usize| p < edges.len() && #[trigger] edges[p as int]@.contains(v)
        ==> done.contains(p)
}

pub open spec fn valid_order(edges: Seq<Vec<usize>>, initial: Seq<usize>, order: Seq<usize>) -> bool {
    &&& order.no_duplicates()
    &&& forall|i: int| 0 <= i < order.len()
        ==> ready(edges, initial, order.take(i), #[trigger] order[i])
}

pub open spec fn resolution(edges: Seq<Vec<usize>>, initial: Seq<usize>, order: Seq<usize>) -> bool {
    &&& valid_order(edges, initial, order)
    // Nothing else can be appended. Any valid order is contained in this
    // fixed point, by induction along that order (proved in proof.rs).
    &&& forall|v: usize| #[trigger] ready(edges, initial, order, v) ==> order.contains(v)
}

} // verus!

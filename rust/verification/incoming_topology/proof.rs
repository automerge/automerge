use crate::spec::*;
use vstd::multiset::group_multiset_axioms;
use vstd::prelude::*;
use vstd::seq_lib::group_to_multiset_ensures;
use vstd::set_lib::{lemma_len_subset, range_set_properties};

verus! {

broadcast use {group_multiset_axioms, group_to_multiset_ensures, vstd::seq_lib::group_seq_properties};

pub open spec fn worklist(
    edges: Seq<Vec<usize>>, initial: Seq<usize>, waiting: Seq<usize>, order: Seq<usize>,
    done: Seq<usize>, tail: Seq<usize>,
) -> bool {
    &&& waiting.len() == initial.len()
    &&& valid_order(edges, initial, order)
    &&& forall|v: usize| v < waiting.len() ==> #[trigger] waiting[v as int]
        + candidate_edges(edges, Seq::empty(), v)
        == initial[v as int] + candidate_edges(edges, done, v) + tail.to_multiset().count(v)
    &&& forall|v: usize| v < waiting.len() ==> (#[trigger] order.contains(v) <==> waiting[v as int] == 0)
}

// The count is zero exactly when every candidate predecessor has fired.
pub proof fn zero_edges(edges: Seq<Vec<usize>>, done: Seq<usize>, v: usize)
    requires edges.len() <= usize::MAX,
    ensures
        candidate_edges(edges, done, v) == 0 <==>
            forall|p: usize| p < edges.len() && #[trigger] edges[p as int]@.contains(v)
                ==> done.contains(p),
    decreases edges.len(),
{
    reveal(candidate_edges);
    if edges.len() > 0 {
        let last = (edges.len() - 1) as usize;
        let prefix = edges.drop_last();
        zero_edges(prefix, done, v);
        assert(edges[last as int] == edges.last());
        assert(edges.last()@.contains(v) <==> edges.last()@.to_multiset().count(v) > 0);
        if candidate_edges(edges, done, v) == 0 {
            assert forall|p: usize| p < edges.len() && #[trigger] edges[p as int]@.contains(v)
                implies done.contains(p) by {
                if p != last {
                    assert(prefix[p as int] == edges[p as int]);
                }
            }
        } else if forall|p: usize| p < edges.len() && #[trigger] edges[p as int]@.contains(v)
            ==> done.contains(p) {
            assert forall|p: usize| p < prefix.len() && #[trigger] prefix[p as int]@.contains(v)
                implies done.contains(p) by {
                assert(prefix[p as int] == edges[p as int]);
            }
            assert(edges[last as int] == edges.last());
            assert(false);
        }
    }
}

// Moving one source into the processed prefix transfers its outgoing edges
// from the graph-wide count to the current row's unprocessed suffix.
pub proof fn partition_edges(edges: Seq<Vec<usize>>, done: Seq<usize>, parent: usize, v: usize)
    requires edges.len() <= usize::MAX, !done.contains(parent),
    ensures candidate_edges(edges, done, v) == candidate_edges(edges, done.push(parent), v)
        + if parent < edges.len() { edges[parent as int]@.to_multiset().count(v) } else { 0 },
    decreases edges.len(),
{
    reveal(candidate_edges);
    if edges.len() > 0 {
        let last = (edges.len() - 1) as usize;
        partition_edges(edges.drop_last(), done, parent, v);
        assert(edges.drop_last().len() == last);
        if parent < last {
            assert(edges.drop_last()[parent as int] == edges[parent as int]);
        }
        assert(done.push(parent).contains(last) <==> done.contains(last) || last == parent);
    }
}

proof fn append_ready(edges: Seq<Vec<usize>>, initial: Seq<usize>, order: Seq<usize>, v: usize)
    requires valid_order(edges, initial, order), !order.contains(v), ready(edges, initial, order, v),
    ensures valid_order(edges, initial, order.push(v)),
{
    let next = order.push(v);
    assert forall|i: int, j: int| 0 <= i < j < next.len() implies next[i] != next[j] by {
        if j == order.len() {
            assert(order.contains(order[i]));
        }
    }
    assert forall|i: int| 0 <= i < next.len()
        implies ready(edges, initial, next.take(i), #[trigger] next[i]) by {
        assert(next.take(i) =~= order.take(i));
        if i == order.len() {
            assert(order.take(i) =~= order);
        }
    }
}

pub proof fn seed(edges: Seq<Vec<usize>>, initial: Seq<usize>, order: Seq<usize>, v: usize)
    requires well_formed(edges, initial), v < initial.len(), initial[v as int] == 0,
        valid_order(edges, initial, order), !order.contains(v),
    ensures valid_order(edges, initial, order.push(v)),
{
    zero_edges(edges, Seq::empty(), v);
    assert forall|p: usize| p < edges.len() && #[trigger] edges[p as int]@.contains(v)
        implies order.contains(p) by {};
    append_ready(edges, initial, order, v);
}

pub proof fn begin_parent(
    edges: Seq<Vec<usize>>, initial: Seq<usize>, waiting: Seq<usize>, order: Seq<usize>, cursor: int,
)
    requires edges.len() <= usize::MAX, 0 < cursor <= order.len(),
        worklist(edges, initial, waiting, order, order.take(cursor - 1), Seq::empty()),
    ensures worklist(edges, initial, waiting, order, order.take(cursor), edges[order[cursor - 1] as int]@),
{
    let done = order.take(cursor - 1);
    let parent = order[cursor - 1];
    assert(!done.contains(parent)) by {
        if done.contains(parent) {
            let j = choose|j: int| 0 <= j < done.len() && done[j] == parent;
            assert(order[j] == parent);
        }
    }
    assert(order.take(cursor) =~= done.push(parent));
    let row = edges[parent as int]@;
    assert forall|v: usize| v < waiting.len() implies #[trigger] waiting[v as int]
        + candidate_edges(edges, Seq::empty(), v)
        == initial[v as int] + candidate_edges(edges, order.take(cursor), v) + row.to_multiset().count(v) by {
        partition_edges(edges, done, parent, v);
        assert(Seq::<usize>::empty().to_multiset().count(v) == 0);
    }
}

pub proof fn split_tail(tail: Seq<usize>, v: usize)
    requires tail.len() > 0,
    ensures tail.to_multiset().count(v) == tail.drop_first().to_multiset().count(v)
        + if v == tail.first() { 1nat } else { 0nat },
{
    assert(tail.remove(0) =~= tail.drop_first());
    assert(tail.contains(tail.first()));
}

pub proof fn child_is_waiting(
    edges: Seq<Vec<usize>>, initial: Seq<usize>, waiting: Seq<usize>, order: Seq<usize>,
    done: Seq<usize>, tail: Seq<usize>, child: usize,
)
    requires well_formed(edges, initial), worklist(edges, initial, waiting, order, done, tail),
        child < edges.len(), tail.len() > 0, tail.first() == child,
    ensures 0 < waiting[child as int],
{
    split_tail(tail, child);
}

pub proof fn enqueue(
    edges: Seq<Vec<usize>>, initial: Seq<usize>, before: Seq<usize>, order: Seq<usize>,
    done: Seq<usize>, tail: Seq<usize>, child: usize,
)
    requires well_formed(edges, initial), worklist(edges, initial, before, order, done, tail),
        child < edges.len(), tail.len() > 0, tail.first() == child,
        before[child as int] == 1, done.is_prefix_of(order),
    ensures valid_order(edges, initial, order.push(child)),
{
    split_tail(tail, child);
    zero_edges(edges, done, child);
    assert(ready(edges, initial, done, child));
    assert forall|p: usize| p < edges.len() && #[trigger] edges[p as int]@.contains(child)
        implies order.contains(p) by {
        let k = choose|k: int| 0 <= k < done.len() && done[k] == p;
        assert(order[k] == p);
    }
    append_ready(edges, initial, order, child);
}

pub proof fn consume_child(
    edges: Seq<Vec<usize>>, initial: Seq<usize>, before: Seq<usize>, waiting: Seq<usize>,
    old_order: Seq<usize>, order: Seq<usize>, done: Seq<usize>, tail: Seq<usize>, child: usize,
)
    requires well_formed(edges, initial), worklist(edges, initial, before, old_order, done, tail),
        child < edges.len(), tail.len() > 0, tail.first() == child, before[child as int] > 0,
        waiting == before.update(child as int, (before[child as int] - 1) as usize),
        order == if waiting[child as int] == 0 { old_order.push(child) } else { old_order },
        valid_order(edges, initial, order),
    ensures worklist(edges, initial, waiting, order, done, tail.drop_first()),
{
    assert forall|v: usize| v < waiting.len() implies #[trigger] waiting[v as int]
        + candidate_edges(edges, Seq::empty(), v)
        == initial[v as int] + candidate_edges(edges, done, v) + tail.drop_first().to_multiset().count(v) by {
        split_tail(tail, v);
        assert(before[v as int] == waiting[v as int] + if v == child { 1int } else { 0int });
    }
    assert forall|v: usize| v < waiting.len() implies (#[trigger] order.contains(v) <==> waiting[v as int] == 0) by {
        assert(old_order.contains(v) <==> before[v as int] == 0);
    }
}

pub proof fn bound_order(edges: Seq<Vec<usize>>, initial: Seq<usize>, order: Seq<usize>)
    requires edges.len() <= usize::MAX, valid_order(edges, initial, order),
    ensures order.len() <= edges.len(),
{
    let range = Set::<usize>::range(0, edges.len() as usize);
    assert(order.to_set().subset_of(range)) by {
        assert forall|v: usize| #[trigger] order.to_set().contains(v) implies range.contains(v) by {
            let i = choose|i: int| 0 <= i < order.len() && order[i] == v;
            assert(ready(edges, initial, order.take(i), order[i]));
        }
    }
    order.unique_seq_to_set();
    range_set_properties(0usize, edges.len() as usize);
    lemma_len_subset(order.to_set(), range);
}

pub proof fn finish(edges: Seq<Vec<usize>>, initial: Seq<usize>, waiting: Seq<usize>, order: Seq<usize>)
    requires well_formed(edges, initial), worklist(edges, initial, waiting, order, order, Seq::empty()),
    ensures resolution(edges, initial, order),
{
    assert forall|v: usize| #[trigger] ready(edges, initial, order, v) implies order.contains(v) by {
        zero_edges(edges, order, v);
        assert(Seq::<usize>::empty().to_multiset().count(v) == 0);
        assert(waiting[v as int] == 0);
    }
}

// A fixed point with a valid order is not merely maximal: it contains
// every vertex of any other valid order, even on inputs with cycles.
pub proof fn greatest(
    edges: Seq<Vec<usize>>, initial: Seq<usize>, resolved: Seq<usize>, other: Seq<usize>,
)
    requires resolution(edges, initial, resolved), valid_order(edges, initial, other),
    ensures forall|v: usize| #[trigger] other.contains(v) ==> resolved.contains(v),
    decreases other.len(),
{
    if other.len() > 0 {
        let prefix = other.drop_last();
        assert(valid_order(edges, initial, prefix)) by {
            assert forall|i: int| 0 <= i < prefix.len()
                implies ready(edges, initial, prefix.take(i), #[trigger] prefix[i]) by {
                assert(prefix.take(i) =~= other.take(i));
            }
        }
        greatest(edges, initial, resolved, prefix);
        let v = other.last();
        assert(ready(edges, initial, prefix, v));
        assert forall|p: usize| p < edges.len() && #[trigger] edges[p as int]@.contains(v)
            implies resolved.contains(p) by {};
        assert(ready(edges, initial, resolved, v));
        assert(other =~= prefix.push(v));
    }
}

} // verus!

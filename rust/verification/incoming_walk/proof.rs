use crate::walk_spec::*;
use vstd::prelude::*;
use vstd::set_lib::lemma_len_subset;

verus! {

broadcast use {vstd::set::group_set_lemmas, vstd::seq_lib::group_seq_properties};

pub(crate) open spec fn accounted<K>(queue: Seq<K>, done: Set<K>) -> bool {
    &&& queue.no_duplicates()
    &&& queue.to_set().disjoint(done)
}

pub(crate) open spec fn settled<K>(graph: Graph<K>, roots: Seq<K>, seen: Set<K>, done: Set<K>) -> bool {
    &&& seen.subset_of(graph.nodes)
    &&& forall|node: K| #[trigger] seen.contains(node) ==> reached(graph, roots, node)
    &&& forall|node: K| #[trigger] done.contains(node) && expands((graph.visit)(node))
        ==> (graph.parents)(node).to_set().subset_of(seen)
}

pub(crate) open spec fn output<K>(graph: Graph<K>, done: Set<K>, kept: Seq<K>) -> bool {
    &&& kept.no_duplicates()
    &&& forall|node: K| #[trigger] kept.contains(node) <==> done.contains(node) && keeps((graph.visit)(node))
}

pub(crate) open spec fn state<K>(
    graph: Graph<K>, roots: Seq<K>, seen: Set<K>, queue: Seq<K>, done: Set<K>, kept: Seq<K>,
) -> bool {
    &&& accounted(queue, done)
    &&& seen =~= done.union(queue.to_set())
    &&& roots.to_set().subset_of(seen)
    &&& settled(graph, roots, seen, done)
    &&& output(graph, done, kept)
    &&& done.len() <= graph.nodes.len()
}

pub(crate) open spec fn active<K>(
    graph: Graph<K>, roots: Seq<K>, seen: Set<K>, queue: Seq<K>, done: Set<K>, node: K,
) -> bool {
    &&& accounted(queue, done.insert(node))
    &&& !done.contains(node)
    &&& seen =~= done.insert(node).union(queue.to_set())
    &&& roots.to_set().subset_of(seen)
    &&& settled(graph, roots, seen, done)
}

pub(crate) proof fn enqueue_model<K>(node: K, seen: Set<K>, queue: Seq<K>, next_seen: Set<K>, next: Seq<K>)
    requires queue.no_duplicates(), queue.to_set().subset_of(seen),
        next_seen == seen.insert(node), next == if seen.contains(node) { queue } else { queue.push(node) },
    ensures next.no_duplicates(),
        next.to_set() =~= if seen.contains(node) { queue.to_set() } else { queue.to_set().insert(node) },
{
    if !seen.contains(node) {
        assert(!queue.contains(node));
        assert forall|i: int, j: int| 0 <= i < j < next.len() implies next[i] != next[j] by {
            if j == queue.len() {
                assert(queue.contains(queue[i]));
            }
        }
    }
}

pub(crate) proof fn prefix_step<K>(xs: Seq<K>, i: int)
    requires 0 <= i < xs.len(),
    ensures xs.take(i + 1).to_set() =~= xs.take(i).to_set().insert(xs[i]),
{
    xs.lemma_take_succ_push(i);
}

pub(crate) proof fn init_walk<K>(graph: Graph<K>, roots: Seq<K>, seen: Set<K>, queue: Seq<K>, kept: Seq<K>)
    requires roots.to_set().subset_of(graph.nodes),
        seen == roots.to_set(), seen == queue.to_set(), queue.no_duplicates(), kept.len() == 0,
    ensures state(graph, roots, seen, queue, Set::empty(), kept),
{
    assert forall|node: K| #[trigger] seen.contains(node) implies reached(graph, roots, node) by {
        assert forall|s: Set<K>| #[trigger] contains_walk(graph, roots, s) implies s.contains(node) by {};
    }
}

pub(crate) proof fn take_next<K>(
    graph: Graph<K>, roots: Seq<K>, seen: Set<K>, before: Seq<K>, queue: Seq<K>, done: Set<K>,
    kept: Seq<K>, node: K,
)
    requires state(graph, roots, seen, before, done, kept), before.len() > 0,
        node == before.first(), queue =~= before.drop_first(),
    ensures active(graph, roots, seen, queue, done, node),
{
    assert(before.contains(node));
    assert(queue.no_duplicates());
    assert(!queue.contains(node)) by {
        if queue.contains(node) {
            let i = choose|i: int| 0 <= i < queue.len() && queue[i] == node;
            assert(before[i + 1] == node);
        }
    }
    assert(before.to_set() =~= queue.to_set().insert(node)) by {
        assert forall|k: K| #[trigger] before.contains(k) <==> k == node || #[trigger] queue.contains(k) by {
            if before.contains(k) && k != node {
                let i = choose|i: int| 0 <= i < before.len() && before[i] == k;
                assert(queue[i - 1] == k);
            }
            if queue.contains(k) {
                let i = choose|i: int| 0 <= i < queue.len() && queue[i] == k;
                assert(before[i + 1] == k);
            }
        }
    }
}

pub(crate) proof fn keep_node<K>(graph: Graph<K>, done: Set<K>, before: Seq<K>, kept: Seq<K>, node: K)
    requires output(graph, done, before), !done.contains(node),
        kept == if keeps((graph.visit)(node)) { before.push(node) } else { before },
    ensures output(graph, done.insert(node), kept),
{
    if keeps((graph.visit)(node)) {
        enqueue_model(node, done, before, done.insert(node), kept);
    }
}

pub(crate) proof fn follow_edge<K>(graph: Graph<K>, roots: Seq<K>, seen: Set<K>, node: K, adj: Seq<K>, i: int)
    requires 0 <= i < adj.len(), seen.contains(node), reached(graph, roots, node),
        expands((graph.visit)(node)), adj == (graph.parents)(node), adj.to_set().subset_of(graph.nodes),
    ensures graph.nodes.contains(adj[i]), reached(graph, roots, adj[i]),
{
    assert(adj.contains(adj[i]));
    assert forall|s: Set<K>| #[trigger] contains_walk(graph, roots, s) implies s.contains(adj[i]) by {
        assert(s.contains(node));
    }
}

pub(crate) proof fn extend_walk<K>(
    graph: Graph<K>, roots: Seq<K>, seen: Set<K>, queue: Seq<K>, seen2: Set<K>, queue2: Seq<K>,
    done: Set<K>, node: K, parent: K,
)
    requires active(graph, roots, seen, queue, done, node), graph.nodes.contains(parent),
        reached(graph, roots, parent), seen2 == seen.insert(parent),
        queue2 == if seen.contains(parent) { queue } else { queue.push(parent) },
    ensures active(graph, roots, seen2, queue2, done, node),
{
    enqueue_model(parent, seen, queue, seen2, queue2);
    assert forall|k: K| #[trigger] seen2.contains(k) implies reached(graph, roots, k) by {};
}

pub(crate) proof fn finish_node<K>(
    graph: Graph<K>, roots: Seq<K>, seen: Set<K>, queue: Seq<K>, done: Set<K>, kept: Seq<K>, node: K,
)
    requires active(graph, roots, seen, queue, done, node),
        output(graph, done.insert(node), kept),
        expands((graph.visit)(node)) ==> (graph.parents)(node).to_set().subset_of(seen),
    ensures state(graph, roots, seen, queue, done.insert(node), kept),
        done.insert(node).len() == done.len() + 1,
{
    let next = done.insert(node);
    assert(next.subset_of(graph.nodes));
    lemma_len_subset(next, graph.nodes);
    assert forall|k: K| #[trigger] next.contains(k) && expands((graph.visit)(k))
        implies (graph.parents)(k).to_set().subset_of(seen) by {};
}

pub(crate) proof fn finish_walk<K>(
    graph: Graph<K>, roots: Seq<K>, seen: Set<K>, queue: Seq<K>, done: Set<K>, kept: Seq<K>,
)
    requires state(graph, roots, seen, queue, done, kept), queue.len() == 0,
    ensures walk_result(graph, roots, kept),
{
    assert(seen =~= done);
    assert(contains_walk(graph, roots, seen));
    assert forall|node: K| #[trigger] kept.contains(node) <==> reached(graph, roots, node)
        && keeps((graph.visit)(node)) by {
        if reached(graph, roots, node) {
            assert(seen.contains(node));
        }
    }
}

// If the excluded set is closed under parents, stopping at the cut is
// equivalent to taking the full ancestry and subtracting that set. This
// connects the cut traversal to the graph definition of canonical members.
pub(crate) proof fn cut_membership<K>(
    full: Graph<K>, cut: Graph<K>, head: K, excluded: Set<K>, node: K,
)
    requires
        full.parents == cut.parents,
        forall|k: K| expands(#[trigger] (full.visit)(k)),
        forall|k: K| expands(#[trigger] (cut.visit)(k)) <==> !excluded.contains(k),
        forall|k: K| keeps(#[trigger] (cut.visit)(k)) <==> !excluded.contains(k),
        forall|k: K| #[trigger] excluded.contains(k) ==> (full.parents)(k).to_set().subset_of(excluded),
    ensures reached(cut, seq![head], node) && keeps((cut.visit)(node))
        <==> reached(full, seq![head], node) && !excluded.contains(node),
{
    let roots = seq![head];
    if reached(cut, roots, node) {
        assert forall|s: Set<K>| #[trigger] contains_walk(full, roots, s) implies s.contains(node) by {
            assert(contains_walk(cut, roots, s)) by {
                assert forall|k: K| #[trigger] s.contains(k) && expands((cut.visit)(k))
                    implies (cut.parents)(k).to_set().subset_of(s) by {};
            }
        }
    }
    if reached(full, roots, node) && !excluded.contains(node) {
        assert forall|s: Set<K>| #[trigger] contains_walk(cut, roots, s) implies s.contains(node) by {
            let unrestricted = s.union(excluded);
            assert(contains_walk(full, roots, unrestricted)) by {
                assert forall|k: K| #[trigger] unrestricted.contains(k) && expands((full.visit)(k))
                    implies (full.parents)(k).to_set().subset_of(unrestricted) by {
                    if excluded.contains(k) {
                        assert((full.parents)(k).to_set().subset_of(excluded));
                    } else {
                        assert(expands((cut.visit)(k)));
                        assert((cut.parents)(k).to_set().subset_of(s));
                    }
                }
            }
            assert(unrestricted.contains(node));
        }
    }
}

} // verus!

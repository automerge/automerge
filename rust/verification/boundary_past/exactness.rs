use crate::boundary_summary::Contribution;
use crate::interpretation::*;
use crate::memo_cut::{cache_cut_is_exact, contributes, decomposition, root_reached};
use crate::spec::*;
use crate::walk_spec::{expands, keeps, walk_result, Graph};
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

proof fn part_exact<'a, A: 'a, K: 'a, F: Fn(K) -> Contribution<'a, A, K>>(
    history: History<K, A>, cut: Graph<K>, project: F, node: K,
)
    requires applied_closure(history), accurate(history, project, node, expands((cut.visit)(node))),
    ensures summary_of(history, |k: K| contributes(history.graph, cut, node, k), term(project, node)),
{
    reveal(summary_of);
    root_reached(history.graph, node);
    let region = |k: K| contributes(history.graph, cut, node, k);
    let summary = term(project, node);
    match projected(project, node) {
        Contribution::Applied(seed) => {
            assert forall|k: K| #[trigger] region(k) implies history.applied.contains(k) by {
                assert(ancestor(history, node, k));
            }
            assert forall|k: K| #[trigger] recovered(history, summary, k)
                <==> region(k) && history.applied.contains(k) by {
                assert(summary.seeds.contains(node));
            }
        },
        Contribution::Candidate(actor, seq) => {
            assert forall|a: A| #[trigger] summary.maxima.contains_key(a) implies exists|k: K| {
                &&& #[trigger] region(k)
                &&& !history.applied.contains(k)
                &&& (history.actor)(k) == a
                &&& (history.seq)(k) == summary.maxima[a]
            } by {
                assert(a == actor);
                assert(region(node));
            }
        },
        Contribution::Memoized(cached) => {
            assert forall|k: K| #[trigger] region(k) <==> ancestor(history, node, k) by {};
            assert(region =~= (|k: K| ancestor(history, node, k)));
        },
    }
}

proof fn union_is_exact<'a, A: 'a, K: 'a, F: Fn(K) -> Contribution<'a, A, K>>(
    history: History<K, A>, cut: Graph<K>, nodes: Seq<K>, project: F, combined: Set<K>, out: SummaryView<A, K>,
)
    requires aggregate(nodes, project, nodes.len() as int, out), decomposition(history.graph, cut, nodes, combined),
        forall|i: int| 0 <= i < nodes.len() ==> summary_of(history,
            |k: K| contributes(history.graph, cut, #[trigger] nodes[i], k), term(project, nodes[i])),
    ensures summary_of(history, |k: K| combined.contains(k), out),
{
    reveal(summary_of);
    reveal(decomposition);
    let region = |k: K| combined.contains(k);
    assert forall|k: K| #[trigger] region(k) && !history.applied.contains(k) implies {
        let a = (history.actor)(k);
        out.maxima.contains_key(a) && (history.seq)(k) <= out.maxima[a]
    } by {
        let j = choose|j: int| 0 <= j < nodes.len() && contributes(history.graph, cut, #[trigger] nodes[j], k);
        let a = (history.actor)(k);
        let hit = nodes[j];
        let input = term(project, hit);
        let region = |p: K| contributes(history.graph, cut, hit, p);
        assert(summary_of(history, region, input));
        assert(region(k));
        assert(input.maxima.contains_key(a));
        assert((history.seq)(k) <= input.maxima[a]);
    }
    assert forall|a: A| #[trigger] out.maxima.contains_key(a) implies exists|k: K| {
        &&& #[trigger] region(k)
        &&& !history.applied.contains(k)
        &&& (history.actor)(k) == a
        &&& (history.seq)(k) == out.maxima[a]
    } by {
        let j = choose|j: int| 0 <= j < nodes.len() && #[trigger] term(project, nodes[j]).maxima.contains_key(a)
            && term(project, nodes[j]).maxima[a] == out.maxima[a];
        let hit = nodes[j];
        let input = term(project, hit);
        let input_region = |p: K| contributes(history.graph, cut, hit, p);
        assert(summary_of(history, input_region, input));
        assert(input.maxima.contains_key(a));
        let k = choose|k: K| #[trigger] input_region(k) && !history.applied.contains(k)
            && (history.actor)(k) == a && (history.seq)(k) == input.maxima[a];
        assert(contributes(history.graph, cut, hit, k));
        assert(region(k));
        assert((history.seq)(k) == out.maxima[a]);
    }
    assert forall|seed: K| #[trigger] out.seeds.contains(seed) implies history.applied.contains(seed) by {
        let j = choose|j: int| 0 <= j < nodes.len() && #[trigger] term(project, nodes[j]).seeds.contains(seed);
        assert(term(project, nodes[j]).seeds.subset_of(history.applied));
    }
    assert(out.seeds.subset_of(history.applied));
    assert forall|k: K| #[trigger] recovered(history, out, k) <==> region(k) && history.applied.contains(k) by {
        if recovered(history, out, k) {
            let seed = choose|seed: K| #[trigger] out.seeds.contains(seed) && ancestor(history, seed, k);
            let j = choose|j: int| 0 <= j < nodes.len() && #[trigger] term(project, nodes[j]).seeds.contains(seed);
            assert(recovered(history, term(project, nodes[j]), k));
            assert(contributes(history.graph, cut, nodes[j], k));
            assert(combined.contains(k));
        }
        if combined.contains(k) && history.applied.contains(k) {
            let j = choose|j: int| 0 <= j < nodes.len() && contributes(history.graph, cut, #[trigger] nodes[j], k);
            let input = term(project, nodes[j]);
            assert(recovered(history, input, k));
            let seed = choose|seed: K| #[trigger] input.seeds.contains(seed) && ancestor(history, seed, k);
            assert(out.seeds.contains(seed));
        }
    }
    assert(summary_of(history, region, out));
}

// Combining the verified worklist with the verified fold gives an exact fresh
// entry if any reused memo entries are exact for the same graph snapshot.
pub(crate) proof fn summarizes_boundary_past<'a, A: 'a, K: 'a, F: Fn(K) -> Contribution<'a, A, K>>(
    history: History<K, A>, cut: Graph<K>, root: K, visited: Seq<K>, project: F, combined: Set<K>, out: SummaryView<A, K>,
)
    requires applied_closure(history),
        forall|k: K| #[trigger] expands((history.graph.visit)(k)),
        forall|k: K| #[trigger] keeps((cut.visit)(k)),
        forall|k: K| #[trigger] expands((cut.visit)(k)) ==> (history.graph.parents)(k) == (cut.parents)(k),
        walk_result(cut, seq![root], visited), aggregate(visited, project, visited.len() as int, out),
        decomposition(history.graph, cut, visited, combined),
        forall|i: int| 0 <= i < visited.len() ==> accurate(history, project, #[trigger] visited[i], expands((cut.visit)(visited[i]))),
    ensures summary_of(history, |k: K| ancestor(history, root, k), out),
{
    cache_cut_is_exact(history.graph, cut, root, visited, combined);
    assert forall|i: int| 0 <= i < visited.len() implies summary_of(history,
        |k: K| contributes(history.graph, cut, #[trigger] visited[i], k), term(project, visited[i])) by {
        part_exact(history, cut, project, visited[i]);
    }
    union_is_exact(history, cut, visited, project, combined, out);
    assert((|k: K| combined.contains(k)) =~= (|k: K| ancestor(history, root, k)));
}

} // verus!

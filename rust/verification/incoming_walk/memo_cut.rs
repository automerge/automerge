use crate::walk_spec::*;
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

// At a stop we substitute its entire past; at an expanded node we contribute
// just that node (its parents will themselves be visited). This is a statement
// about sets, independent of traversal order and overlapping cache entries.
pub(crate) open spec fn contributes<K>(full: Graph<K>, cut: Graph<K>, from: K, node: K) -> bool {
    node == from || !expands((cut.visit)(from)) && reached(full, seq![from], node)
}

#[verifier::opaque]
pub(crate) open spec fn decomposition<K>(full: Graph<K>, cut: Graph<K>, visited: Seq<K>, combined: Set<K>) -> bool {
    forall|k: K| #[trigger] combined.contains(k) <==> exists|j: int| 0 <= j < visited.len()
        && contributes(full, cut, #[trigger] visited[j], k)
}

pub(crate) proof fn root_reached<K>(graph: Graph<K>, root: K)
    ensures reached(graph, seq![root], root),
{
    let roots = seq![root];
    assert(roots.contains(root));
    assert forall|s: Set<K>| #[trigger] contains_walk(graph, roots, s) implies s.contains(root) by {};
}

proof fn follow<K>(graph: Graph<K>, root: K, node: K, parent: K)
    requires reached(graph, seq![root], node), expands((graph.visit)(node)), (graph.parents)(node).contains(parent),
    ensures reached(graph, seq![root], parent),
{
    assert forall|s: Set<K>| #[trigger] contains_walk(graph, seq![root], s) implies s.contains(parent) by {
        assert(s.contains(node));
    }
}

pub(crate) proof fn cache_cut_is_exact<K>(full: Graph<K>, cut: Graph<K>, root: K, visited: Seq<K>, combined: Set<K>)
    requires
        forall|k: K| #[trigger] expands((full.visit)(k)),
        forall|k: K| #[trigger] keeps((cut.visit)(k)),
        forall|k: K| #[trigger] expands((cut.visit)(k)) ==> (full.parents)(k) == (cut.parents)(k),
        walk_result(cut, seq![root], visited), decomposition(full, cut, visited, combined),
    ensures forall|k: K| #[trigger] combined.contains(k) <==> reached(full, seq![root], k),
{
    reveal(decomposition);
    let roots = seq![root];
    root_reached(cut, root);
    assert(visited.contains(root));
    let r = choose|j: int| 0 <= j < visited.len() && visited[j] == root;
    assert(contributes(full, cut, visited[r], root));
    assert(combined.contains(root));

    assert forall|k: K| #[trigger] combined.contains(k) implies reached(full, roots, k) by {
        let j = choose|j: int| 0 <= j < visited.len() && contributes(full, cut, #[trigger] visited[j], k);
        let hit = visited[j];
        assert(visited.contains(hit));
        assert(reached(cut, roots, hit));
        assert forall|s: Set<K>| #[trigger] contains_walk(full, roots, s) implies s.contains(k) by {
            assert(contains_walk(cut, roots, s)) by {
                assert forall|v: K| #[trigger] s.contains(v) && expands((cut.visit)(v))
                    implies (cut.parents)(v).to_set().subset_of(s) by {};
            }
            assert(s.contains(hit));
            if hit != k {
                assert(contains_walk(full, seq![hit], s));
                assert(reached(full, seq![hit], k));
            }
        }
    }

    assert(contains_walk(full, roots, combined)) by {
        assert forall|k: K| #[trigger] combined.contains(k) && expands((full.visit)(k))
            implies (full.parents)(k).to_set().subset_of(combined) by {
            let j = choose|j: int| 0 <= j < visited.len() && contributes(full, cut, #[trigger] visited[j], k);
            let hit = visited[j];
            assert forall|p: K| #[trigger] (full.parents)(k).contains(p) implies combined.contains(p) by {
                if expands((cut.visit)(hit)) {
                    assert(k == hit);
                    assert(visited.contains(hit));
                    assert(reached(cut, roots, hit));
                    follow(cut, root, hit, p);
                    assert(visited.contains(p));
                    let i = choose|i: int| 0 <= i < visited.len() && visited[i] == p;
                    assert(contributes(full, cut, visited[i], p));
                } else {
                    root_reached(full, hit);
                    assert(reached(full, seq![hit], k));
                    follow(full, hit, k, p);
                    assert(contributes(full, cut, visited[j], p));
                }
            }
        }
    }
    assert forall|k: K| #[trigger] combined.contains(k) <==> reached(full, roots, k) by {};
}

} // verus!

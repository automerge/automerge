use crate::walk::Visit;
use vstd::prelude::*;

verus! {

#[verifier::reject_recursive_types(K)]
pub(crate) ghost struct Graph<K> {
    pub nodes: Set<K>,
    pub parents: spec_fn(K) -> Seq<K>,
    pub visit: spec_fn(K) -> Visit,
}

pub(crate) open spec fn keeps(visit: Visit) -> bool {
    matches!(visit, Visit::Keep | Visit::KeepAndExpand)
}

pub(crate) open spec fn expands(visit: Visit) -> bool {
    matches!(visit, Visit::Expand | Visit::KeepAndExpand)
}

// Only expanded nodes need a parent lookup. Ignored and kept-only nodes
// can be external references and do not need an entry in the graph.
pub(crate) open spec fn models<K, P: Fn(K) -> Vec<K>, C: Fn(K) -> Visit>(
    graph: Graph<K>, roots: Seq<K>, parents: P, classify: C,
) -> bool {
    &&& roots.to_set().subset_of(graph.nodes)
    &&& forall|node: K| #[trigger] graph.nodes.contains(node) ==> classify.requires((node,))
    &&& forall|node: K, step: Visit| graph.nodes.contains(node)
        && #[trigger] classify.ensures((node,), step) ==> step == (graph.visit)(node)
    &&& forall|node: K| graph.nodes.contains(node) && expands((graph.visit)(node))
        ==> #[trigger] parents.requires((node,)) && (graph.parents)(node).to_set().subset_of(graph.nodes)
    &&& forall|node: K, list: Vec<K>| graph.nodes.contains(node) && expands((graph.visit)(node))
        && #[trigger] parents.ensures((node,), list) ==> list@ == (graph.parents)(node)
}

// Obtain the graph from the callbacks' contracts without passing a ghost
// argument to executable code. A choice of the finite node universe cannot
// affect reachability; the callbacks determine the edges and classifications.
pub(crate) open spec fn with_nodes<K, P: Fn(K) -> Vec<K>, C: Fn(K) -> Visit>(
    nodes: Set<K>, parents: P, classify: C,
) -> Graph<K> {
    Graph {
        nodes,
        parents: |k: K| (choose|r: Vec<K>| parents.ensures((k,), r))@,
        visit: |k: K| choose|step: Visit| classify.ensures((k,), step),
    }
}

pub(crate) open spec fn supports<K, P: Fn(K) -> Vec<K>, C: Fn(K) -> Visit>(
    roots: Seq<K>, parents: P, classify: C, nodes: Set<K>,
) -> bool {
    models(with_nodes(nodes, parents, classify), roots, parents, classify)
}

pub(crate) open spec fn model<K, P: Fn(K) -> Vec<K>, C: Fn(K) -> Visit>(
    roots: Seq<K>, parents: P, classify: C,
) -> Graph<K> {
    with_nodes(choose|nodes: Set<K>| #[trigger] supports(roots, parents, classify, nodes), parents, classify)
}

// To contain the traversal, a set must contain every root and must also
// contain every parent of each expanded node it contains.
pub(crate) open spec fn contains_walk<K>(graph: Graph<K>, roots: Seq<K>, s: Set<K>) -> bool {
    &&& roots.to_set().subset_of(s)
    &&& forall|node: K| #[trigger] s.contains(node) && expands((graph.visit)(node))
        ==> (graph.parents)(node).to_set().subset_of(s)
}

// The reached nodes form the least set that contains the traversal.
pub(crate) open spec fn reached<K>(graph: Graph<K>, roots: Seq<K>, node: K) -> bool {
    forall|s: Set<K>| #[trigger] contains_walk(graph, roots, s) ==> s.contains(node)
}

pub(crate) open spec fn walk_result<K>(graph: Graph<K>, roots: Seq<K>, kept: Seq<K>) -> bool {
    &&& kept.no_duplicates()
    &&& forall|node: K| #[trigger] kept.contains(node)
        <==> reached(graph, roots, node) && keeps((graph.visit)(node))
}

} // verus!

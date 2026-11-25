use crate::boundary_summary::Contribution;
use crate::spec::*;
use crate::walk_spec::{reached, Graph};
use vstd::prelude::*;

verus! {

#[verifier::reject_recursive_types(K)]
pub(crate) ghost struct History<K, A> {
    pub graph: Graph<K>,
    pub applied: Set<K>,
    pub actor: spec_fn(K) -> A,
    pub seq: spec_fn(K) -> u64,
}

pub(crate) open spec fn ancestor<K, A>(history: History<K, A>, root: K, node: K) -> bool {
    reached(history.graph, seq![root], node)
}

pub(crate) open spec fn applied_closure<K, A>(history: History<K, A>) -> bool {
    forall|root: K, node: K| #[trigger] ancestor(history, root, node) && history.applied.contains(root)
        ==> history.applied.contains(node)
}

pub(crate) open spec fn recovered<K, A>(history: History<K, A>, summary: SummaryView<A, K>, node: K) -> bool {
    exists|seed: K| #[trigger] summary.seeds.contains(seed) && ancestor(history, seed, node)
}

// A summary has two parts. Candidate entries are the attained maxima in the
// unapplied part of the region. The inclusive pasts of its applied seeds
// cover exactly the applied part. The seeds need not be a minimal set.
#[verifier::opaque]
pub(crate) open spec fn summary_of<K, A>(
    history: History<K, A>, region: spec_fn(K) -> bool, summary: SummaryView<A, K>,
) -> bool {
    &&& forall|k: K| #[trigger] region(k) && !history.applied.contains(k) ==> {
        let actor = (history.actor)(k);
        summary.maxima.contains_key(actor) && (history.seq)(k) <= summary.maxima[actor]
    }
    &&& forall|a: A| #[trigger] summary.maxima.contains_key(a) ==> exists|k: K| {
        &&& #[trigger] region(k)
        &&& !history.applied.contains(k)
        &&& (history.actor)(k) == a
        &&& (history.seq)(k) == summary.maxima[a]
    }
    &&& summary.seeds.subset_of(history.applied)
    &&& forall|k: K| #[trigger] recovered(history, summary, k) <==> region(k) && history.applied.contains(k)
}

// An applied stop summarizes its applied causal past; an unapplied stop takes
// a memo entry already proved exact for that node on the same graph snapshot.
// An expanded, unapplied node contributes its own (actor, seq) only.
pub(crate) open spec fn accurate<'a, A: 'a, K: 'a, F: Fn(K) -> Contribution<'a, A, K>>(
    history: History<K, A>, project: F, node: K, expand: bool,
) -> bool {
    match projected(project, node) {
        Contribution::Applied(seed) => !expand && seed == node && history.applied.contains(node),
        Contribution::Candidate(actor, seq) => expand && !history.applied.contains(node)
            && actor == (history.actor)(node) && seq == (history.seq)(node),
        Contribution::Memoized(cached) => !expand && !history.applied.contains(node)
            && summary_of(history, |k: K| ancestor(history, node, k), view(cached)),
    }
}

} // verus!

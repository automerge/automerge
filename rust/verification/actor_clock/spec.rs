use vstd::prelude::*;

verus! {

#[verifier::reject_recursive_types(K)]
pub ghost struct History<K, A> {
    pub nodes: Set<K>,
    pub parents: spec_fn(K) -> Set<K>,
    pub actor: spec_fn(K) -> A,
    pub sequence: spec_fn(K) -> nat,
}

pub open spec fn causal_set<K, A>(history: History<K, A>, past: Set<K>) -> bool {
    &&& past.subset_of(history.nodes)
    &&& forall|k: K| #[trigger] past.contains(k) ==> (history.parents)(k).subset_of(past)
}

pub open spec fn actor_chains<K, A>(history: History<K, A>) -> bool {
    &&& forall|k: K| #[trigger] history.nodes.contains(k) ==> (history.sequence)(k) >= 1
    &&& forall|l: K, r: K| #[trigger] history.nodes.contains(l) && #[trigger] history.nodes.contains(r)
        && (history.actor)(l) == (history.actor)(r) && (history.sequence)(l) == (history.sequence)(r) ==> l == r
    &&& forall|k: K| #[trigger] history.nodes.contains(k) && (history.sequence)(k) > 1 ==> exists|p: K| {
        &&& #[trigger] (history.parents)(k).contains(p)
        &&& (history.actor)(p) == (history.actor)(k)
        &&& (history.sequence)(p) + 1 == (history.sequence)(k)
    }
}

pub open spec fn covers<K, A>(history: History<K, A>, clock: Map<A, nat>, node: K) -> bool {
    let actor = (history.actor)(node);
    clock.dom().contains(actor) && (history.sequence)(node) <= clock[actor]
}

// The clock is the attained maximum for each actor in past ∩ side. An
// empty actor bucket has no entry. Side need not itself be parent-closed.
pub open spec fn maxima<K, A>(history: History<K, A>, past: Set<K>, side: Set<K>, clock: Map<A, nat>) -> bool {
    &&& forall|node: K| #[trigger] past.contains(node) && #[trigger] side.contains(node) ==> covers(history, clock, node)
    &&& forall|actor: A| #[trigger] clock.dom().contains(actor) ==> exists|witness: K| {
        &&& #[trigger] past.contains(witness)
        &&& side.contains(witness)
        &&& (history.actor)(witness) == actor
        &&& (history.sequence)(witness) == clock[actor]
    }
}

} // verus!

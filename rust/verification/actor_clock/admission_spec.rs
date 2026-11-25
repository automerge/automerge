use crate::spec::History;
use vstd::prelude::*;

verus! {

#[verifier::reject_recursive_types(K)]
pub ghost struct Fragments<K> {
    // Both the head and each member belong to the head's inclusive past.
    pub past: spec_fn(K) -> Set<K>,
    pub level: spec_fn(K) -> nat,
    pub rank: spec_fn(K) -> nat,
    pub members: spec_fn(K) -> Set<K>,
    pub boundary: spec_fn(K) -> Set<K>,
}

pub open spec fn ancestry<K, A>(history: History<K, A>, fragments: Fragments<K>) -> bool {
    forall|h: K| #[trigger] history.nodes.contains(h) ==> {
        let past = (fragments.past)(h);
        &&& past.contains(h)
        &&& past.subset_of(history.nodes)
        &&& (history.parents)(h).subset_of(past)
        &&& forall|k: K| #[trigger] past.contains(k) ==> (fragments.past)(k).subset_of(past)
    }
}

pub open spec fn canonical<K>(fragments: Fragments<K>, h: K) -> bool {
    let past = (fragments.past)(h);
    let members = (fragments.members)(h);
    let boundary = (fragments.boundary)(h);
    &&& forall|b: K| #[trigger] boundary.contains(b) ==> past.contains(b)
        && (fragments.level)(b) >= (fragments.level)(h) && (fragments.rank)(b) < (fragments.rank)(h)
    &&& forall|k: K| #[trigger] members.contains(k) ==> k == h || (fragments.level)(k) < (fragments.level)(h)
    &&& forall|k: K| #[trigger] members.contains(k) <==> past.contains(k)
        && !(exists|b: K| #[trigger] boundary.contains(b) && (fragments.past)(b).contains(k))
}

pub open spec fn selected_member<K>(fragments: Fragments<K>, heads: Set<K>, k: K) -> bool {
    exists|h: K| #[trigger] heads.contains(h) && (fragments.members)(h).contains(k)
}

pub open spec fn selected_boundaries<K>(fragments: Fragments<K>, heads: Set<K>, before: Set<K>) -> bool {
    forall|h: K, b: K| #[trigger] heads.contains(h) && #[trigger] (fragments.boundary)(h).contains(b)
        ==> before.contains(b) || selected_member(fragments, heads, b)
}

// The old history and selected canonical fragments supply the precise set
// intended for publication.
pub open spec fn assembly<K, A>(
    history: History<K, A>, fragments: Fragments<K>, heads: Set<K>, before: Set<K>, after: Set<K>,
    max_level: nat,
) -> bool {
    &&& ancestry(history, fragments)
    &&& before.subset_of(history.nodes)
    &&& forall|k: K| #[trigger] before.contains(k) ==> (fragments.past)(k).subset_of(before)
    &&& forall|h: K| #[trigger] heads.contains(h) ==> history.nodes.contains(h)
        && (fragments.level)(h) <= max_level && canonical(fragments, h)
    &&& selected_boundaries(fragments, heads, before)
    &&& forall|k: K| #[trigger] after.contains(k) <==> before.contains(k) || selected_member(fragments, heads, k)
}

// Semantics of extending the compact actor clock with the selected maxima.
pub open spec fn joins<A>(before: Map<A, nat>, selected: Map<A, nat>, after: Map<A, nat>) -> bool {
    &&& after.dom() =~= before.dom().union(selected.dom())
    &&& forall|a: A| #[trigger] after.dom().contains(a) ==> after[a] ==
        if !selected.dom().contains(a) || (before.dom().contains(a) && before[a] >= selected[a]) { before[a] }
        else { selected[a] }
}

} // verus!

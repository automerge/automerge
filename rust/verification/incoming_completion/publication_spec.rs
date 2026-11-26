use crate::publication::Publish;
use vstd::prelude::*;

verus! {

pub(crate) open spec fn merged<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(
    initial: S, selected: Seq<P>, members: &'a [H], step: F, states: Seq<S>, end: int,
) -> bool {
    &&& 0 <= end <= selected.len()
    &&& states.len() == end + 1
    &&& states[0] == initial
    &&& forall|i: int| 0 <= i < end ==> #[trigger] step.ensures((states[i], Publish::MergeProof(&selected[i])), states[i + 1])
}

pub(crate) open spec fn included<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(
    initial: S, selected: Seq<P>, members: &'a [H], step: F, states: Seq<S>, accepted: S,
) -> bool {
    &&& merged(initial, selected, members, step, states, selected.len() as int)
    &&& step.ensures((states.last(), Publish::IncludeMembers(members)), accepted)
}

pub(crate) open spec fn reclaimed<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(
    initial: S, selected: Seq<P>, members: &'a [H], step: F, states: Seq<S>, accepted: S, after: S,
) -> bool {
    included(initial, selected, members, step, states, accepted) && step.ensures((accepted, Publish::Reclaim), after)
}

pub(crate) open spec fn merge_ready<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(
    initial: S, selected: Seq<P>, members: &'a [H], step: F, states: Seq<S>, i: int,
) -> bool {
    step.requires((states[i], Publish::MergeProof(&selected[i])))
}

pub(crate) open spec fn include_ready<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(
    initial: S, selected: Seq<P>, members: &'a [H], step: F, states: Seq<S>,
) -> bool {
    step.requires((states.last(), Publish::IncludeMembers(members)))
}

pub(crate) open spec fn reclaim_ready<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(
    initial: S, selected: Seq<P>, members: &'a [H], step: F, states: Seq<S>, accepted: S,
) -> bool {
    step.requires((accepted, Publish::Reclaim))
}

pub(crate) open spec fn prune_ready<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(
    initial: S, selected: Seq<P>, members: &'a [H], step: F, states: Seq<S>, accepted: S, after: S,
) -> bool {
    step.requires((after, Publish::RemoveDrafts))
}

pub(crate) open spec fn can_publish<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(initial: S, selected: Seq<P>, members: &'a [H], step: F) -> bool {
    &&& forall|i: int, states: Seq<S>| 0 <= i < selected.len() && #[trigger] merged(initial, selected, members, step, states, i)
        ==> merge_ready(initial, selected, members, step, states, i)
    &&& forall|states: Seq<S>| #[trigger] merged(initial, selected, members, step, states, selected.len() as int)
        ==> include_ready(initial, selected, members, step, states)
    &&& forall|states: Seq<S>, accepted: S| #[trigger] included(initial, selected, members, step, states, accepted)
        ==> reclaim_ready(initial, selected, members, step, states, accepted)
    &&& forall|states: Seq<S>, accepted: S, after: S| #[trigger] reclaimed(initial, selected, members, step, states, accepted, after)
        ==> prune_ready(initial, selected, members, step, states, accepted, after)
}

// The merge trace has one event for each selected occurrence (and none for
// non-selected proofs). The last three events happen in this fixed order.
pub(crate) open spec fn publication_trace<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(initial: S, selected: Seq<P>, members: &'a [H], step: F, out: S) -> bool {
    exists|states: Seq<S>, accepted: S, after: S| #[trigger] reclaimed(initial, selected, members, step, states, accepted, after)
        && step.ensures((after, Publish::RemoveDrafts), out)
}

pub(crate) proof fn record_merge<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(
    initial: S, selected: Seq<P>, members: &'a [H], step: F, states: Seq<S>, i: int, next: S,
)
    requires 0 <= i < selected.len(), merged(initial, selected, members, step, states, i),
        step.ensures((states[i], Publish::MergeProof(&selected[i])), next),
    ensures merged(initial, selected, members, step, states.push(next), i + 1),
{
    let after = states.push(next);
    assert forall|j: int| 0 <= j < i + 1
        implies #[trigger] step.ensures((after[j], Publish::MergeProof(&selected[j])), after[j + 1]) by {
        if j < i { assert(step.ensures((states[j], Publish::MergeProof(&selected[j])), states[j + 1])); }
    }
}

pub(crate) proof fn record_publish<'a, S, P: 'a, H: 'a, F: Fn(S, Publish<&'a P, &'a [H]>) -> S>(
    initial: S, selected: Seq<P>, members: &'a [H], step: F, states: Seq<S>, accepted: S, after: S, out: S,
)
    requires reclaimed(initial, selected, members, step, states, accepted, after), step.ensures((after, Publish::RemoveDrafts), out),
    ensures publication_trace(initial, selected, members, step, out),
{}

} // verus!

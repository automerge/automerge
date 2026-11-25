use crate::publication::Publish;
use vstd::prelude::*;

verus! {

pub(crate) open spec fn merged<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(
    initial: S, selected: Seq<P>, step: F, states: Seq<S>, end: int,
) -> bool {
    &&& 0 <= end <= selected.len()
    &&& states.len() == end + 1
    &&& states[0] == initial
    &&& forall|i: int| 0 <= i < end ==> #[trigger] step.ensures((states[i], Publish::MergeProof(&selected[i])), states[i + 1])
}

pub(crate) open spec fn included<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(
    initial: S, selected: Seq<P>, step: F, states: Seq<S>, accepted: S,
) -> bool {
    &&& merged(initial, selected, step, states, selected.len() as int)
    &&& step.ensures((states.last(), Publish::IncludeMembers), accepted)
}

pub(crate) open spec fn reclaimed<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(
    initial: S, selected: Seq<P>, step: F, states: Seq<S>, accepted: S, after: S,
) -> bool {
    included(initial, selected, step, states, accepted) && step.ensures((accepted, Publish::Reclaim), after)
}

pub(crate) open spec fn merge_ready<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(
    initial: S, selected: Seq<P>, step: F, states: Seq<S>, i: int,
) -> bool {
    step.requires((states[i], Publish::MergeProof(&selected[i])))
}

pub(crate) open spec fn include_ready<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(
    initial: S, selected: Seq<P>, step: F, states: Seq<S>,
) -> bool {
    step.requires((states.last(), Publish::IncludeMembers))
}

pub(crate) open spec fn reclaim_ready<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(
    initial: S, selected: Seq<P>, step: F, states: Seq<S>, accepted: S,
) -> bool {
    step.requires((accepted, Publish::Reclaim))
}

pub(crate) open spec fn prune_ready<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(
    initial: S, selected: Seq<P>, step: F, states: Seq<S>, accepted: S, after: S,
) -> bool {
    step.requires((after, Publish::RemoveDrafts))
}

pub(crate) open spec fn can_publish<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(initial: S, selected: Seq<P>, step: F) -> bool {
    &&& forall|i: int, states: Seq<S>| 0 <= i < selected.len() && #[trigger] merged(initial, selected, step, states, i)
        ==> merge_ready(initial, selected, step, states, i)
    &&& forall|states: Seq<S>| #[trigger] merged(initial, selected, step, states, selected.len() as int)
        ==> include_ready(initial, selected, step, states)
    &&& forall|states: Seq<S>, accepted: S| #[trigger] included(initial, selected, step, states, accepted)
        ==> reclaim_ready(initial, selected, step, states, accepted)
    &&& forall|states: Seq<S>, accepted: S, after: S| #[trigger] reclaimed(initial, selected, step, states, accepted, after)
        ==> prune_ready(initial, selected, step, states, accepted, after)
}

// The merge trace has one event for each selected occurrence (and none for
// non-selected proofs). The last three events happen in this fixed order.
pub(crate) open spec fn publication_trace<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(initial: S, selected: Seq<P>, step: F, out: S) -> bool {
    exists|states: Seq<S>, accepted: S, after: S| #[trigger] reclaimed(initial, selected, step, states, accepted, after)
        && step.ensures((after, Publish::RemoveDrafts), out)
}

pub(crate) proof fn record_merge<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(
    initial: S, selected: Seq<P>, step: F, states: Seq<S>, i: int, next: S,
)
    requires 0 <= i < selected.len(), merged(initial, selected, step, states, i),
        step.ensures((states[i], Publish::MergeProof(&selected[i])), next),
    ensures merged(initial, selected, step, states.push(next), i + 1),
{
    let after = states.push(next);
    assert forall|j: int| 0 <= j < i + 1
        implies #[trigger] step.ensures((after[j], Publish::MergeProof(&selected[j])), after[j + 1]) by {
        if j < i { assert(step.ensures((states[j], Publish::MergeProof(&selected[j])), states[j + 1])); }
    }
}

pub(crate) proof fn record_publish<'a, S, P: 'a, F: Fn(S, Publish<&'a P>) -> S>(
    initial: S, selected: Seq<P>, step: F, states: Seq<S>, accepted: S, after: S, out: S,
)
    requires reclaimed(initial, selected, step, states, accepted, after), step.ensures((after, Publish::RemoveDrafts), out),
    ensures publication_trace(initial, selected, step, out),
{}

pub(crate) open spec fn was_prepared<S, P, E, F: FnOnce(S) -> (S, Result<P, E>)>(initial: S, prepare: F, prepared: S, plan: P) -> bool {
    prepare.ensures((initial,), (prepared, Ok(plan)))
}

pub(crate) open spec fn was_applied<S, P, E, F: FnOnce(S) -> (S, Result<P, E>), A: FnOnce(S, &P) -> (S, Result<(), E>)>(
    initial: S, prepare: F, apply: A, prepared: S, applied: S, plan: P, result: Result<(), E>,
) -> bool {
    &&& was_prepared(initial, prepare, prepared, plan)
    &&& result is Ok
    &&& apply.ensures((prepared, &plan), (applied, result))
}

pub(crate) open spec fn can_run<S, P, E, F: FnOnce(S) -> (S, Result<P, E>), A: FnOnce(S, &P) -> (S, Result<(), E>), C: FnOnce(S, P) -> S>(
    initial: S, prepare: F, apply: A, complete: C,
) -> bool {
    &&& prepare.requires((initial,))
    &&& forall|prepared: S, plan: P| #[trigger] was_prepared(initial, prepare, prepared, plan) ==> apply.requires((prepared, &plan))
    &&& forall|prepared: S, applied: S, plan: P, r: Result<(), E>|
        #[trigger] was_applied(initial, prepare, apply, prepared, applied, plan, r) ==> complete.requires((applied, plan))
}

pub(crate) open spec fn admission_trace<S, P, E, F: FnOnce(S) -> (S, Result<P, E>), A: FnOnce(S, &P) -> (S, Result<(), E>), C: FnOnce(S, P) -> S>(
    initial: S, prepare: F, apply: A, complete: C, result: (S, Result<(), E>),
) -> bool {
    let (state, result) = result;
    match result {
        Err(error) => prepare.ensures((initial,), (state, Err(error)))
            || (exists|prepared: S, plan: P| #[trigger] was_prepared(initial, prepare, prepared, plan)
                && apply.ensures((prepared, &plan), (state, Err(error)))),
        Ok(_) => exists|prepared: S, applied: S, plan: P, r: Result<(), E>|
            #[trigger] was_applied(initial, prepare, apply, prepared, applied, plan, r)
            && complete.ensures((applied, plan), state),
    }
}

} // verus!

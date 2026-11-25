//! Execute incoming admission phases in their required order.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::publication_spec::*;
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) enum Publish<P> {
    MergeProof(P),
    IncludeMembers,
    Reclaim,
    RemoveDrafts,
}

/// Visit exactly the selected proof payloads before publishing any selected
/// members. Only then reclaim redundant evidence and prune published drafts.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires can_publish(initial, selected@, step),
    ensures publication_trace(initial, selected@, step, out),
))]
pub(super) fn publish<'a, S, P: 'a>(
    initial: S,
    selected: &'a [P],
    step: impl Fn(S, Publish<&'a P>) -> S,
) -> S {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost mut states = seq![initial]; }
    let mut state = initial;
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= selected@.len(), can_publish(initial, selected@, step),
            merged(initial, selected@, step, states, i as int), state == states[i as int],
        decreases selected@.len() - i,
    ))]
    while i < selected.len() {
        let proof = &selected[i];
        #[cfg(automerge_verus)]
        proof! { assert(merge_ready(initial, selected@, step, states, i as int)); }
        let next = step(state, Publish::MergeProof(proof));
        #[cfg(automerge_verus)]
        proof! {
            record_merge(initial, selected@, step, states, i as int, next);
            states = states.push(next);
        }
        state = next;
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { assert(include_ready(initial, selected@, step, states)); }
    let accepted = step(state, Publish::IncludeMembers);
    #[cfg(automerge_verus)]
    proof! {
        assert(included(initial, selected@, step, states, accepted));
        assert(reclaim_ready(initial, selected@, step, states, accepted));
    }
    let after_reclaim = step(accepted, Publish::Reclaim);
    #[cfg(automerge_verus)]
    proof! {
        assert(reclaimed(initial, selected@, step, states, accepted, after_reclaim));
        assert(prune_ready(initial, selected@, step, states, accepted, after_reclaim));
    }
    let done = step(after_reclaim, Publish::RemoveDrafts);
    #[cfg(automerge_verus)]
    proof! { record_publish(initial, selected@, step, states, accepted, after_reclaim, done); }
    done
}

/// Do not complete a plan if preparation or application returned an error.
/// The adapters specify which document state (if any) those steps can mutate.
#[cfg_attr(automerge_verus, verus_spec(result =>
    requires can_run(initial, prepare, apply, complete),
    ensures admission_trace(initial, prepare, apply, complete, result),
))]
pub(crate) fn run<S, P, E>(
    initial: S,
    prepare: impl FnOnce(S) -> (S, Result<P, E>),
    apply: impl FnOnce(S, &P) -> (S, Result<(), E>),
    complete: impl FnOnce(S, P) -> S,
) -> (S, Result<(), E>) {
    let (prepared, result) = prepare(initial);
    let plan = match result {
        Ok(plan) => plan,
        Err(error) => return (prepared, Err(error)),
    };
    #[cfg(automerge_verus)]
    proof! { assert(was_prepared(initial, prepare, prepared, plan)); }
    let (applied, result) = apply(prepared, &plan);
    match result {
        Ok(()) => {}
        Err(error) => return (applied, Err(error)),
    }
    #[cfg(automerge_verus)]
    proof! {
        assert(was_applied(initial, prepare, apply, prepared, applied, plan, result));
    }
    (complete(applied, plan), Ok(()))
}

#[cfg(test)]
mod tests {
    use super::{publish, run, Publish};

    #[test]
    fn merges_only_selected_proofs_before_accepting_and_reclaiming() {
        let log = publish(Vec::new(), &[4, 1], |mut log, step| {
            log.push(match step {
                Publish::MergeProof(&4) => "proof 4",
                Publish::MergeProof(&1) => "proof 1",
                Publish::MergeProof(_) => panic!("unselected proof"),
                Publish::IncludeMembers => "accept",
                Publish::Reclaim => "reclaim",
                Publish::RemoveDrafts => "remove drafts",
            });
            log
        });
        assert_eq!(
            log,
            ["proof 4", "proof 1", "accept", "reclaim", "remove drafts"]
        );
    }

    #[test]
    fn preparation_and_apply_errors_never_reach_completion() {
        assert_eq!(
            run(
                0,
                |s| (s, Err::<(), _>("prepare")),
                |_, _| panic!("apply"),
                |_, _| panic!("complete")
            ),
            (0, Err("prepare")),
        );
        assert_eq!(
            run(
                0,
                |s| (s, Ok(1)),
                |s, _| (s, Err("apply")),
                |_, _| panic!("complete")
            ),
            (0, Err("apply")),
        );
    }
}

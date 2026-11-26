//! Execute incoming admission phases in their required order.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::publication_spec::*;
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) enum Publish<P, M> {
    MergeProof(P),
    IncludeMembers(M),
    Reclaim,
    RemoveDrafts,
}

/// Visit exactly the selected proof payloads before publishing any selected
/// members. Only then reclaim redundant evidence and prune published drafts.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires can_publish(initial, selected@, members, step),
    ensures publication_trace(initial, selected@, members, step, out),
))]
pub(super) fn publish<'a, S, P: 'a, H: 'a>(
    initial: S,
    selected: &'a [P],
    members: &'a [H],
    step: impl Fn(S, Publish<&'a P, &'a [H]>) -> S,
) -> S {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost mut states = seq![initial]; }
    let mut state = initial;
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= selected@.len(), can_publish(initial, selected@, members, step),
            merged(initial, selected@, members, step, states, i as int), state == states[i as int],
        decreases selected@.len() - i,
    ))]
    while i < selected.len() {
        let proof = &selected[i];
        #[cfg(automerge_verus)]
        proof! { assert(merge_ready(initial, selected@, members, step, states, i as int)); }
        let next = step(state, Publish::MergeProof(proof));
        #[cfg(automerge_verus)]
        proof! {
            record_merge(initial, selected@, members, step, states, i as int, next);
            states = states.push(next);
        }
        state = next;
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { assert(include_ready(initial, selected@, members, step, states)); }
    let accepted = step(state, Publish::IncludeMembers(members));
    #[cfg(automerge_verus)]
    proof! {
        assert(included(initial, selected@, members, step, states, accepted));
        assert(reclaim_ready(initial, selected@, members, step, states, accepted));
    }
    let after_reclaim = step(accepted, Publish::Reclaim);
    #[cfg(automerge_verus)]
    proof! {
        assert(reclaimed(initial, selected@, members, step, states, accepted, after_reclaim));
        assert(prune_ready(initial, selected@, members, step, states, accepted, after_reclaim));
    }
    let done = step(after_reclaim, Publish::RemoveDrafts);
    #[cfg(automerge_verus)]
    proof! { record_publish(initial, selected@, members, step, states, accepted, after_reclaim, done); }
    done
}

#[cfg(test)]
mod tests {
    use super::{publish, Publish};

    #[test]
    fn merges_only_selected_proofs_before_accepting_and_reclaiming() {
        let log = publish(Vec::new(), &[4, 1], &[9], |mut log, step| {
            log.push(match step {
                Publish::MergeProof(&4) => "proof 4",
                Publish::MergeProof(&1) => "proof 1",
                Publish::MergeProof(_) => panic!("unselected proof"),
                Publish::IncludeMembers(members) => {
                    assert_eq!(members, &[9]);
                    "accept"
                }
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
}

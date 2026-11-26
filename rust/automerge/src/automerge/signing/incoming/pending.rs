//! Rebuild the pending queue from viable, still-blocked deliveries.

#![allow(unexpected_cfgs)]

use std::collections::HashSet;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    pending_spec::*,
};
#[cfg(automerge_verus)]
use vstd::{prelude::*, std_specs::hash::hash_set_inv};

/// Disproved old orphans must not block a new batch; invalid new deliveries
/// still abort the batch without reaching application or completion.
#[cfg_attr(automerge_verus, verus_verify)]
#[derive(Clone, Copy)]
pub(crate) enum Origin {
    ExistingPending,
    IncomingBatch,
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) enum Route<V, E> {
    Evaluated(V),
    Disproved(E),
}

#[cfg_attr(automerge_verus, verus_spec(result =>
    ensures result == match (origin, evaluation) {
        (_, Ok(v)) => Ok(Route::Evaluated(v)),
        (Origin::ExistingPending, Err(e)) => Ok(Route::Disproved(e)),
        (Origin::IncomingBatch, Err(e)) => Err(e),
    },
))]
pub(super) fn route<V, E>(origin: Origin, evaluation: Result<V, E>) -> Result<Route<V, E>, E> {
    match evaluation {
        Ok(value) => Ok(Route::Evaluated(value)),
        Err(error) => match origin {
            Origin::ExistingPending => Ok(Route::Disproved(error)),
            Origin::IncomingBatch => Err(error),
        },
    }
}

/// An eligible delivery is still viable if something remains unpublished,
/// no unapplied member's slot conflicts, and its established authors agree
/// with the proof entries attached to this delivery.
#[cfg_attr(automerge_verus, verus_spec(keep =>
    requires for_members(members@, accepted), for_members(members@, unconflicted),
        !every(members@, accepted, members@.len() as int)
            && every(members@, unconflicted, members@.len() as int) ==> stable_proof_check(proof_ok),
    ensures keep == viable_result(members@, accepted, unconflicted, proof_ok),
))]
pub(super) fn recheck<T>(
    members: &[T],
    accepted: impl Fn(&T) -> bool,
    unconflicted: impl Fn(&T) -> bool,
    proof_ok: impl Fn() -> bool,
) -> bool {
    !all(members, &accepted) && all(members, &unconflicted) && proof_ok()
}

#[cfg_attr(automerge_verus, verus_spec(passed =>
    requires for_members(items@, *test),
    ensures passed == every(items@, *test, items@.len() as int),
))]
pub(super) fn all<T, F: Fn(&T) -> bool>(items: &[T], test: &F) -> bool {
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= items@.len(), for_members(items@, *test), every(items@, *test, i as int),
        decreases items@.len() - i,
    ))]
    while i < items.len() {
        let member = &items[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(*test, member)); }
        let passed = (*test)(member);
        #[cfg(automerge_verus)]
        proof! { assert(passed == model(*test, member)); }
        if !passed {
            #[cfg(automerge_verus)]
            proof! { assert(!every(items@, *test, items@.len() as int)); }
            return false;
        }
        i += 1;
    }
    true
}

/// Keep the first viable representative of each still-blocked delivery.
/// Recheck viability against the post-apply view, never the planning view.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires hash_set_inv(remaining), callbacks(candidates@, remaining@, state, viable, same),
    ensures retention(candidates@, remaining@, state, viable, same, out@),
))]
pub(super) fn rebuild<T, S>(
    candidates: Vec<T>,
    remaining: &HashSet<usize>,
    state: &S,
    viable: impl Fn(&S, &T) -> bool,
    same: impl Fn(&T, &T) -> bool,
) -> Vec<T> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost input = candidates@; let ghost mut positions = Seq::<int>::empty(); }
    let mut kept = Vec::new();
    #[cfg(automerge_verus)]
    proof! { assert(retained(input, remaining@, state, viable, same, 0, kept@, positions)); }
    #[cfg_attr(automerge_verus, verus_spec(iter =>
        invariant hash_set_inv(remaining), iter.seq().len() == input.len(), callbacks(input, remaining@, state, viable, same),
            forall|j: int| 0 <= j < input.len() ==> #[trigger] iter.seq()[j] == (j as usize, input[j]),
            retained(input, remaining@, state, viable, same, iter.index(), kept@, positions),
        ensures retained(input, remaining@, state, viable, same, input.len() as int, kept@, positions),
    ))]
    for (i, candidate) in candidates.into_iter().enumerate() {
        let eligible = remaining.contains(&i);
        if eligible {
            #[cfg(automerge_verus)]
            proof! {
                assert(input.contains(input[i as int]));
                assert(viability_ready(viable,state,&candidate));
            }
            let still_viable = viable(state, &candidate);
            #[cfg(automerge_verus)]
            proof! { assert(still_viable == observed_viability(viable,state,&candidate)); }
            if still_viable {
                #[cfg(automerge_verus)]
                proof! { compare_kept(input, remaining@, state, viable, same, i as int, kept@, positions); }
                let duplicate = already_kept(&kept, &candidate, &same);
                if !duplicate {
                    #[cfg(automerge_verus)]
                    proof! {
                        keep_one(input, remaining@, state, viable, same, i as int, kept@, positions);
                        positions = positions.push(i as int);
                    }
                    kept.push(candidate);
                } else {
                    #[cfg(automerge_verus)]
                    proof! { skip_one(input, remaining@, state, viable, same, i as int, kept@, positions); }
                }
            } else {
                #[cfg(automerge_verus)]
                proof! { skip_one(input, remaining@, state, viable, same, i as int, kept@, positions); }
            }
        } else {
            #[cfg(automerge_verus)]
            proof! { skip_one(input, remaining@, state, viable, same, i as int, kept@, positions); }
        }
    }
    #[cfg(automerge_verus)]
    proof! { finish(input, remaining@, state, viable, same, kept@, positions); }
    kept
}

#[cfg_attr(automerge_verus, verus_spec(duplicate =>
    requires forall|j: int| 0 <= j < kept@.len() ==> comparable(*same, #[trigger] kept@[j], *new),
    ensures duplicate == represented(kept@, *same, *new),
))]
fn already_kept<T, F: Fn(&T, &T) -> bool>(kept: &[T], new: &T, same: &F) -> bool {
    let mut j = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant j <= kept@.len(),
            forall|k: int| 0 <= k < kept@.len() ==> comparable(*same, #[trigger] kept@[k], *new),
            forall|k: int| 0 <= k < j ==> !equivalent(*same, #[trigger] kept@[k], *new),
        decreases kept@.len() - j,
    ))]
    while j < kept.len() {
        let earlier = &kept[j];
        #[cfg(automerge_verus)]
        proof! {
            assert(comparable(*same, *earlier, *new));
            assert((*same).requires((earlier, new)));
        }
        let matched = (*same)(earlier, new);
        #[cfg(automerge_verus)]
        proof! { assert(matched == equivalent(*same, *earlier, *new)); }
        if matched {
            #[cfg(automerge_verus)]
            proof! { assert(represented(kept@, *same, *new)); }
            return true;
        }
        j += 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{rebuild, recheck, route, Origin, Route};
    use std::collections::HashSet;

    #[test]
    fn retains_first_viable_copy_but_notices_a_different_attachment() {
        // (content, attachment, viable, occurrence)
        let queued = vec![
            (1, 2, true, 'a'),  // already selected, not eligible for rechecking
            (1, 2, false, 'b'), // no longer viable, must not shadow 'c'
            (1, 2, true, 'c'),
            (1, 2, true, 'd'), // exact repeated delivery
            (1, 3, true, 'e'), // same content, independent attached proof
        ];
        let retained = rebuild(
            queued,
            &HashSet::from([1, 2, 3, 4]),
            &(),
            |_, entry| {
                assert_ne!(entry.3, 'a');
                entry.2
            },
            |a, b| (a.0, a.1) == (b.0, b.1),
        );
        assert_eq!(retained, [(1, 2, true, 'c'), (1, 3, true, 'e')]);
    }

    #[test]
    fn rechecks_proof_only_after_publication_and_slot_tests() {
        assert!(!recheck(
            &[1],
            |_| true,
            |_| panic!("already accepted"),
            || panic!("already accepted")
        ));
        assert!(!recheck(
            &[1],
            |_| false,
            |_| false,
            || panic!("slot conflict")
        ));
        assert!(!recheck(&[1], |_| false, |_| true, || false));
        assert!(recheck(&[1], |_| false, |_| true, || true));
    }

    #[test]
    fn only_old_orphans_can_be_dropped_on_evaluation_error() {
        let error = Err::<(), _>("bad proof");
        assert!(matches!(
            route(Origin::ExistingPending, error),
            Ok(Route::Disproved("bad proof"))
        ));
        assert!(matches!(
            route(Origin::IncomingBatch, error),
            Err("bad proof")
        ));
    }
}

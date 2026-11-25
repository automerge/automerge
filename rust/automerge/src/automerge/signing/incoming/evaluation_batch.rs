//! Partition candidate occurrences without losing a result or its origin index.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::batch_spec::*;
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) enum Outcome<V> {
    Waiting,
    Verified(V),
    Dropped,
}

/// Validate owned deliveries in order. Temporary slot/key ledgers are
/// threaded through the batch; no partially validated group list escapes.
#[cfg_attr(automerge_verus, verus_spec(result =>
    requires validation_ready(groups@, initial, validate),
    ensures validation_result(groups@, initial, validate, result),
))]
pub(super) fn validate_batch<G, S, V, E>(
    groups: Vec<G>,
    initial: S,
    validate: impl Fn(S, G) -> (S, Result<V, E>),
) -> Result<Vec<V>, E> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost inputs=groups@; let ghost mut states=seq![initial]; }
    let mut state = initial;
    let mut validated = Vec::new();
    #[cfg_attr(automerge_verus, verus_spec(iter =>
        invariant iter.seq()==inputs, validated@.len()==iter.index(),
            validation_ready(inputs,initial,validate),
            validation_prefix(inputs,initial,validate,states,validated@), state==states[validated@.len() as int],
        ensures validated@.len()==inputs.len(), validation_prefix(inputs,initial,validate,states,validated@),
    ))]
    for group in groups {
        #[cfg(automerge_verus)]
        proof! { assert(validate.requires((state,group))); }
        let (next, result) = validate(state, group);
        match result {
            Ok(value) => {
                #[cfg(automerge_verus)]
                proof! {
                    extend_validated(inputs,initial,validate,states,validated@,next,value);
                    states=states.push(next);
                }
                validated.push(value);
            }
            Err(error) => {
                #[cfg(automerge_verus)]
                proof! { validation_stopped(inputs,initial,validate,states,validated@,next,error); }
                return Err(error);
            }
        }
        state = next;
    }
    #[cfg(automerge_verus)]
    proof! { validation_finished(inputs,initial,validate,states,validated); }
    Ok(validated)
}

pub(super) type Partition<V> = (Vec<usize>, Vec<(usize, V)>);

/// Stop at the first fatal error. Otherwise return exactly the waiting and
/// verified occurrences, keeping original candidate indices and order.
#[cfg_attr(automerge_verus, verus_spec(result =>
    requires can_assess(candidates@, initial, inspect),
    ensures assessed(candidates@, initial, inspect, result),
))]
pub(super) fn assess<G, S, V, E>(
    candidates: &[G],
    initial: S,
    inspect: impl Fn(S, &G) -> (S, Result<Outcome<V>, E>),
) -> Result<Partition<V>, E> {
    #[cfg(automerge_verus)]
    proof_decl! {
        let ghost mut states = seq![initial];
        let ghost mut outcomes = Seq::<Outcome<V>>::empty();
    }
    let mut state = initial;
    let mut waiting = Vec::new();
    let mut verified = Vec::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= candidates@.len(), can_assess(candidates@, initial, inspect),
            evaluated_prefix(candidates@, initial, inspect, states, outcomes, i as int),
            state == states[i as int], partition(outcomes, waiting@, verified@),
        decreases candidates@.len() - i,
    ))]
    while i < candidates.len() {
        #[cfg(automerge_verus)]
        proof! { assert(can_inspect(candidates@, inspect, states, i as int)); }
        let (next, result) = inspect(state, &candidates[i]);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => {
                #[cfg(automerge_verus)]
                proof! { record_error(candidates@, initial, inspect, states, outcomes, i as int, next, error); }
                return Err(error);
            }
        };
        #[cfg(automerge_verus)]
        proof_decl! {
            record_success(candidates@, initial, inspect, states, outcomes, i as int, next, outcome);
            let ghost prior = outcomes;
            let ghost old_waiting = waiting@;
            let ghost old_verified = verified@;
            states = states.push(next);
            outcomes = outcomes.push(outcome);
        }
        match outcome {
            Outcome::Waiting => waiting.push(i),
            Outcome::Verified(value) => verified.push((i, value)),
            Outcome::Dropped => {}
        }
        #[cfg(automerge_verus)]
        proof! { record_partition(prior, old_waiting, old_verified, outcome, waiting@, verified@); }
        state = next;
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { end_batch(candidates@, initial, inspect, states, outcomes, waiting, verified); }
    Ok((waiting, verified))
}

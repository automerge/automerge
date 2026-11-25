//! Accumulate evidence privately, in frontier order, failing at the first error.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::{
    evidence_spec::*,
    lookup_spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::prelude::*;

/// Try only entries of this attachment for the exact frontier being checked.
#[cfg_attr(automerge_verus, verus_spec(witness =>
    requires forall|i: int| 0 <= i < entries@.len() ==> modeled(check, #[trigger] entries@[i]),
    ensures search_result(entries@, check, witness),
))]
pub(super) fn first_witness<S: Copy, W>(
    entries: &[S],
    check: impl Fn(S) -> Option<W>,
) -> Option<W> {
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= entries@.len(),
            forall|j: int| 0 <= j < entries@.len() ==> modeled(check, #[trigger] entries@[j]),
            no_witness(entries@, check, i as int),
        decreases entries@.len() - i,
    ))]
    while i < entries.len() {
        let signature = entries[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(check, signature)); }
        let found = check(signature);
        #[cfg(automerge_verus)]
        proof! { assert(found == model(check, signature)); }
        if let Some(witness) = found {
            #[cfg(automerge_verus)]
            proof! { report_witness(entries@, check, i as int, witness); }
            return Some(witness);
        }
        i += 1;
    }
    None
}

/// Run the per-frontier check over exactly these obligations. No partial
/// accumulator is returned on failure. The accumulator can also thread a
/// per-call prepared-key cache through the early verification pass.
#[cfg_attr(automerge_verus, verus_spec(result =>
    requires forall|i: int, states: Seq<S>| 0 <= i < hashes@.len()
        && #[trigger] trace(hashes@, step, initial, states, i)
        ==> step.requires((hashes@[i], states[i])),
    ensures outcome(hashes@, step, initial, result),
))]
pub(crate) fn assemble_frontiers<H: Copy, S, E>(
    hashes: &[H],
    initial: S,
    step: impl Fn(H, S) -> Result<S, E>,
) -> Result<S, E> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost mut states = seq![initial]; }
    let mut state = initial;
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= hashes@.len(), trace(hashes@, step, initial, states, i as int), state == states[i as int],
            forall|j: int, prefix: Seq<S>| 0 <= j < hashes@.len()
                && #[trigger] trace(hashes@, step, initial, prefix, j) ==> step.requires((hashes@[j], prefix[j])),
        decreases hashes@.len() - i,
    ))]
    while i < hashes.len() {
        let hash = hashes[i];
        #[cfg(automerge_verus)]
        proof! { assert(step.requires((hash, state))); }
        match step(hash, state) {
            Ok(next) => {
                #[cfg(automerge_verus)]
                proof! {
                    extend(hashes@, step, initial, states, i as int, next);
                    states = states.push(next);
                }
                state = next;
            }
            Err(error) => {
                #[cfg(automerge_verus)]
                proof! { record_failure(hashes@, step, initial, states, i as int, error); }
                return Err(error);
            }
        }
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { record_success(hashes@, step, initial, states, state); }
    Ok(state)
}

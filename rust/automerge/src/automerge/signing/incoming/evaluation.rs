//! Order the ancestry, canonicality, and proof checks for an incoming group.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::{
    evaluation_spec::*,
    lookup_spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::prelude::*;

/// A known broken dependency takes priority over waiting for any unresolved
/// member. Neither an unresolved member nor a missing author is itself an
/// error at this step.
#[cfg_attr(automerge_verus, verus_spec(result =>
    requires forall|i:int| 0 <= i < members@.len() ==> #[trigger] modeled(observe, &members@[i]),
    ensures status_result(members@, observe, result),
))]
pub(super) fn ancestry<T, E>(
    members: &[T],
    observe: impl Fn(&T) -> Result<bool, E>,
) -> Result<bool, E> {
    let mut ready = true;
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= members@.len(),
            forall|j:int| 0 <= j < members@.len() ==> #[trigger] modeled(observe, &members@[j]),
            unbroken(members@, observe, i as int), ready == all_ready(members@, observe, i as int),
        decreases members@.len() - i,
    ))]
    while i < members.len() {
        let member = &members[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(observe, member)); }
        let status = observe(member);
        #[cfg(automerge_verus)]
        proof! { assert(status == model(observe, member)); }
        match status {
            Err(error) => {
                #[cfg(automerge_verus)]
                proof! { report_broken(members@, observe, i as int, error); }
                return Err(error);
            }
            Ok(resolved) => ready = ready && resolved,
        }
        i += 1;
    }
    Ok(ready)
}

/// Only a resolved, non-equivocating group reaches canonicality and proof
/// checks. The memo is private planning state, carried through the shape check.
#[cfg_attr(automerge_verus, verus_spec(result =>
    requires can_decide(status, memo, equivocation, canonical, proof),
    ensures decision(status, memo, equivocation, canonical, proof, result),
))]
pub(super) fn decide<S, F, P, E>(
    status: Result<bool, E>,
    memo: S,
    equivocation: impl FnOnce() -> Option<E>,
    canonical: impl FnOnce(S) -> (S, Result<F, E>),
    proof: impl FnOnce() -> Result<P, E>,
) -> (S, Result<Option<(F, P)>, E>) {
    match status {
        Err(error) => return (memo, Err(error)),
        Ok(false) => return (memo, Ok(None)),
        Ok(true) => {}
    }
    if let Some(error) = equivocation() {
        return (memo, Err(error));
    }
    let (next, shape) = canonical(memo);
    let fragment = match shape {
        Ok(fragment) => fragment,
        Err(error) => return (next, Err(error)),
    };
    #[cfg(automerge_verus)]
    proof! { assert(shape_ok(memo, canonical, next, fragment)); }
    match proof() {
        Ok(proof) => (next, Ok(Some((fragment, proof)))),
        Err(error) => (next, Err(error)),
    }
}

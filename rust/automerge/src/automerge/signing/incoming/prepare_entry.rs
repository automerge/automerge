//! Keep the original candidates and the single ancestry view through prepare.

#![allow(unexpected_cfgs)]

use super::candidate_origin::{self, Candidate};

#[cfg(automerge_verus)]
use crate::entry_spec::*;
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Prepared<G, P> {
    pub candidates: Vec<Candidate<G>>,
    pub plan: P,
}

/// Validate new input before opening pending. The plan is made from the
/// exact owned candidate list returned for later apply and completion.
#[cfg_attr(automerge_verus, verus_spec(out=>
    requires can_prepare(raw,pending,validate,plan),
    ensures preparation(raw,pending,validate,plan,out),
))]
pub(super) fn prepare<L, G, P, E>(
    raw: Vec<L>,
    pending: impl FnOnce() -> Vec<G>,
    validate: impl FnOnce(Vec<L>) -> Result<Vec<G>, E>,
    plan: impl FnOnce(&Vec<Candidate<G>>) -> Result<P, E>,
) -> Result<Prepared<G, P>, E> {
    let fresh = match validate(raw) {
        Ok(fresh) => fresh,
        Err(error) => {
            #[cfg(automerge_verus)]
            proof! { reveal(preparation); }
            return Err(error);
        }
    };
    let old = pending();
    #[cfg(automerge_verus)]
    proof_decl! {
        let ghost previous=old;
        let ghost incoming=fresh;
        assert(collected(raw,pending,validate,previous,incoming));
    }
    let candidates = candidate_origin::candidates(old, fresh);
    #[cfg(automerge_verus)]
    proof! { assert(exact_candidates(raw,pending,validate,previous,incoming,candidates)); }
    match plan(&candidates) {
        Ok(plan_value) => {
            #[cfg(automerge_verus)]
            proof! { ready_preparation(raw,pending,validate,plan,previous,incoming,candidates,plan_value); }
            Ok(Prepared {
                candidates,
                plan: plan_value,
            })
        }
        Err(error) => {
            #[cfg(automerge_verus)]
            proof! { failed_plan(raw,pending,validate,plan,previous,incoming,candidates,error); }
            Err(error)
        }
    }
}

/// Resolve once. Pass the same candidate slice and the same resulting view
/// to evaluation and to the plan compiler; an evaluation error never plans.
#[cfg_attr(automerge_verus, verus_spec(out=>
    requires can_analyze(candidates,resolve,evaluate,plan),
    ensures analysis(candidates,resolve,evaluate,plan,out),
))]
pub(super) fn analyze<'a, G: 'a, A, V, P, E>(
    candidates: &'a [G],
    resolve: impl FnOnce(&'a [G]) -> A,
    evaluate: impl FnOnce(&'a [G], &A) -> Result<V, E>,
    plan: impl FnOnce(&'a [G], &A, V) -> Result<P, E>,
) -> Result<P, E> {
    let ancestry = resolve(candidates);
    let result = evaluate(candidates, &ancestry);
    match result {
        Ok(evaluated) => {
            #[cfg(automerge_verus)]
            proof! { assert(assessed(candidates,resolve,evaluate,ancestry,evaluated)); }
            plan(candidates, &ancestry, evaluated)
        }
        Err(error) => Err(error),
    }
}

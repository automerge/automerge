//! Author propagation over the topologically resolved changes.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::{chain_proof::*, chain_spec::*};
#[cfg(automerge_verus)]
use vstd::laws_eq::obeys_concrete_eq;
#[cfg(automerge_verus)]
use vstd::prelude::*;

use super::author_checks::same;

#[cfg_attr(automerge_verus, verus_verify)]
#[derive(Clone, Copy)]
pub(crate) enum Dependency<A> {
    Candidate(usize),
    Applied { actor: A, seq: u64, slot: usize },
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Candidate<A> {
    pub actor: A,
    pub seq: u64,
    pub actor_already_applied: bool,
    pub deps: Vec<Dependency<A>>,
}

#[cfg_attr(automerge_verus, verus_verify)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthorSource {
    Initial(usize),
    Applied(usize),
}

#[cfg_attr(automerge_verus, verus_verify)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Resolution {
    Linked(AuthorSource),
    Broken(usize),
}

/// Classify all resolved candidates in dependency order. Author sources are
/// origins, not keys: a valid chain can have an origin with no author.
#[cfg_attr(automerge_verus, verus_spec(outcomes =>
    requires obeys_concrete_eq::<A>(), well_formed(rows@),
    ensures outcomes@.len() == rows@.len(), classified(rows@, outcomes@),
))]
pub(super) fn propagate<A: Copy + Eq>(rows: &[Candidate<A>]) -> Vec<Resolution> {
    let mut outcomes = Vec::with_capacity(rows.len());
    let mut index = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant obeys_concrete_eq::<A>(), well_formed(rows@),
            index == outcomes@.len() <= rows@.len(), classified(rows@, outcomes@),
        decreases rows@.len() - index,
    ))]
    while index < rows.len() {
        let next = classify(rows, &outcomes, index);
        #[cfg(automerge_verus)]
        proof! { extend(rows@, outcomes@, next); }
        outcomes.push(next);
        index += 1;
    }
    outcomes
}

#[cfg_attr(automerge_verus, verus_spec(next =>
    requires obeys_concrete_eq::<A>(), well_formed(rows@), index == earlier@.len() < rows@.len(),
    ensures classification(rows@, earlier@, index as int, next),
))]
fn classify<A: Copy + Eq>(
    rows: &[Candidate<A>],
    earlier: &[Resolution],
    index: usize,
) -> Resolution {
    let row = &rows[index];
    let target = row.seq - 1;
    let mut predecessor = None;
    let mut j = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant obeys_concrete_eq::<A>(), well_formed(rows@), index == earlier@.len() < rows@.len(),
            *row == rows@[index as int], target + 1 == row.seq,
            j <= row.deps@.len(), no_broken(row.deps@, earlier@, j as int),
            first_binding(rows@, earlier@, index as int, j as int, predecessor),
        decreases row.deps@.len() - j,
    ))]
    while j < row.deps.len() {
        let dep = row.deps[j];
        let (actor, seq, source) = match dep {
            Dependency::Candidate(parent) => match earlier[parent] {
                Resolution::Broken(cause) => {
                    #[cfg(automerge_verus)]
                    proof! { via_broken_dep(rows@, earlier@, index as int, j as int, cause); }
                    return Resolution::Broken(cause);
                }
                Resolution::Linked(source) => (rows[parent].actor, rows[parent].seq, source),
            },
            Dependency::Applied { actor, seq, slot } => (actor, seq, AuthorSource::Applied(slot)),
        };
        #[cfg(automerge_verus)]
        proof_decl! { let ghost prev = predecessor; }
        if predecessor.is_none() && seq == target && same(&actor, &row.actor) {
            predecessor = Some(source);
        }
        #[cfg(automerge_verus)]
        proof! {
            assert(dep_cause(dep, earlier@).is_none());
            assert(binding(rows@, earlier@, index as int, dep) ==
                if seq == target && actor == row.actor { Some(source) } else { None });
            next_dep(rows@, earlier@, index as int, j as int, prev, predecessor);
        }
        j += 1;
    }
    if target == 0 {
        if row.actor_already_applied {
            Resolution::Broken(index)
        } else {
            Resolution::Linked(AuthorSource::Initial(index))
        }
    } else if let Some(source) = predecessor {
        Resolution::Linked(source)
    } else {
        Resolution::Broken(index)
    }
}

#[cfg(test)]
#[path = "actor_chains/tests.rs"]
mod tests;

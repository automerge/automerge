//! Author-sensitive checks shared with the Verus harness.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::spec::*;
#[cfg(automerge_verus)]
use vstd::laws_eq::obeys_concrete_eq;
#[cfg(automerge_verus)]
use vstd::prelude::*;
#[cfg(automerge_verus)]
use vstd::std_specs::cmp::PartialEqSpec;

#[cfg_attr(automerge_verus, verus_spec(equal =>
    requires obeys_concrete_eq::<A>(),
    ensures equal == (*left == *right),
))]
pub(super) fn same<A: Eq>(left: &A, right: &A) -> bool {
    let equal = <A as PartialEq>::eq(left, right);
    #[cfg(automerge_verus)]
    proof! {
        reveal(obeys_concrete_eq);
        assert(<A as PartialEqSpec>::eq_spec(left, right) == (*left == *right));
    }
    equal
}

/// Reuse a group's preverification only when every required frontier has a
/// cached witness under the exact author established by ancestry.
#[cfg_attr(automerge_verus, verus_spec(matches =>
    requires obeys_concrete_eq::<A>(),
        forall|i: int| 0 <= i < frontiers@.len() ==> modeled(expected, #[trigger] frontiers@[i]),
        forall|i: int| 0 <= i < frontiers@.len() ==> modeled(cached, #[trigger] frontiers@[i]),
    ensures matches <==> forall|i: int| 0 <= i < frontiers@.len()
        ==> matches_author(expected, cached, #[trigger] frontiers@[i]),
))]
pub(super) fn cache_matches<'a, H: Copy, A: Eq + 'a>(
    frontiers: &[H],
    expected: impl Fn(H) -> Option<A>,
    cached: impl Fn(H) -> Option<&'a A>,
) -> bool {
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= frontiers@.len(), obeys_concrete_eq::<A>(),
            forall|j: int| 0 <= j < frontiers@.len() ==> modeled(expected, #[trigger] frontiers@[j]),
            forall|j: int| 0 <= j < frontiers@.len() ==> modeled(cached, #[trigger] frontiers@[j]),
            forall|j: int| 0 <= j < i ==> matches_author(expected, cached, #[trigger] frontiers@[j]),
        decreases frontiers@.len() - i,
    ))]
    while i < frontiers.len() {
        let hash = frontiers[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(expected, hash)); assert(modeled(cached, hash)); }
        let established = expected(hash);
        #[cfg(automerge_verus)]
        proof! { assert(established == model(expected, hash)); }
        let Some(author) = established else {
            return false;
        };
        let recorded = cached(hash);
        #[cfg(automerge_verus)]
        proof! { assert(recorded == model(cached, hash)); }
        let Some(saved) = recorded else { return false };
        if !same(&author, saved) {
            return false;
        }
        #[cfg(automerge_verus)]
        proof! { assert(matches_author(expected, cached, hash)); }
        i += 1;
    }
    true
}

#[cfg(test)]
#[path = "author_checks/tests.rs"]
mod tests;

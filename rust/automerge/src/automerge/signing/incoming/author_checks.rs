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
fn same<A: Eq>(left: &A, right: &A) -> bool {
    let equal = <A as PartialEq>::eq(left, right);
    #[cfg(automerge_verus)]
    proof! {
        reveal(obeys_concrete_eq);
        assert(<A as PartialEqSpec>::eq_spec(left, right) == (*left == *right));
    }
    equal
}

/// Return the author source of the first direct dependency at the exact actor
/// and sequence requested. The source can represent a predecessor with no author.
#[cfg_attr(automerge_verus, verus_spec(found =>
    requires obeys_concrete_eq::<A>(),
        forall|i: int| 0 <= i < deps@.len() ==> modeled(describe, #[trigger] deps@[i]),
    ensures predecessor_result(deps@, describe, actor, seq, found),
))]
pub(super) fn find_predecessor<K: Copy, A: Eq, T>(
    deps: &[K],
    actor: A,
    seq: u64,
    describe: impl Fn(K) -> Option<(A, u64, T)>,
) -> Option<T> {
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= deps@.len(), obeys_concrete_eq::<A>(),
            forall|j: int| 0 <= j < deps@.len() ==> modeled(describe, #[trigger] deps@[j]),
            forall|j: int| 0 <= j < i ==> link_author(describe, #[trigger] deps@[j], actor, seq).is_none(),
        decreases deps@.len() - i,
    ))]
    while i < deps.len() {
        let dep = deps[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(describe, dep)); }
        let description = describe(dep);
        #[cfg(automerge_verus)]
        proof! { assert(description == model(describe, dep)); }
        if let Some((dep_actor, dep_seq, source)) = description {
            if dep_seq == seq && same(&dep_actor, &actor) {
                #[cfg(automerge_verus)]
                proof! {
                    assert(link_author(describe, deps@[i as int], actor, seq) == Some(source));
                    assert(predecessor_result(deps@, describe, actor, seq, Some(source)));
                }
                return Some(source);
            }
        }
        #[cfg(automerge_verus)]
        proof! { assert(link_author(describe, dep, actor, seq).is_none()); }
        i += 1;
    }
    None
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

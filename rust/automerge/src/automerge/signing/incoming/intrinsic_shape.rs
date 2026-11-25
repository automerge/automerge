//! Single-head, reachability, and fragment-level checks for a delivery.

#![allow(unexpected_cfgs)]

use super::intrinsic_index::same;
use std::collections::HashSet;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    shape_spec::*,
};
#[cfg(automerge_verus)]
use vstd::{laws_eq::obeys_concrete_eq, prelude::*, std_specs::hash::hash_set_inv};

#[cfg_attr(automerge_verus, verus_spec(head =>
    requires hash_laws::<K>(), members@.no_duplicates(), edge_view(members@, parents, in_group),
    ensures head_result(members@, parents, head),
))]
pub(super) fn unique_head<'a, K: Copy + Eq + Hash + 'a>(
    members: &[K],
    parents: impl Fn(K) -> &'a [K],
    in_group: impl Fn(K) -> bool,
) -> Option<K> {
    let mut has_dependent = HashSet::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= members@.len(), hash_laws::<K>(), hash_set_inv(&has_dependent),
            edge_view(members@, parents, in_group), marked(members@, parents, i as int, has_dependent@),
        decreases members@.len() - i,
    ))]
    while i < members.len() {
        let node = members[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(parents, node)); }
        let deps = parents(node);
        #[cfg(automerge_verus)]
        proof_decl! { let ghost prior = has_dependent@; assert(deps == model(parents, node)); }
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant j <= deps@.len(), i < members@.len(), deps == model(parents, members@[i as int]),
                hash_laws::<K>(), hash_set_inv(&has_dependent), edge_view(members@, parents, in_group),
                marked(members@, parents, i as int, prior),
                row_prefix(members@, deps@, j as int, prior, has_dependent@),
            decreases deps@.len() - j,
        ))]
        while j < deps.len() {
            let dep = deps[j];
            #[cfg(automerge_verus)]
            proof_decl! { let ghost before = has_dependent@; assert(modeled(in_group, dep)); }
            let internal = in_group(dep);
            #[cfg(automerge_verus)]
            proof! { assert(internal == members@.contains(dep)); }
            if internal {
                has_dependent.insert(dep);
            }
            #[cfg(automerge_verus)]
            proof! { mark_one(members@, deps@, j as int, prior, before, has_dependent@); }
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! { end_row(members@, parents, i as int, prior, has_dependent@); }
        i += 1;
    }

    let mut head = None;
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= members@.len(), hash_laws::<K>(), hash_set_inv(&has_dependent), members@.no_duplicates(),
            marked(members@, parents, members@.len() as int, has_dependent@),
            sole_unmarked(members@, has_dependent@, i as int, head),
        decreases members@.len() - i,
    ))]
    while i < members.len() {
        let node = members[i];
        if !has_dependent.contains(&node) {
            if head.is_some() {
                #[cfg(automerge_verus)]
                proof! { two_heads(members@, parents, has_dependent@, i as int, head); }
                return None;
            }
            head = Some(node);
        }
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { end_heads(members@, parents, has_dependent@, head); }
    head
}

#[cfg_attr(automerge_verus, verus_spec(ok =>
    requires members@.no_duplicates(), visited@.no_duplicates(), visited@.to_set().subset_of(members@.to_set()),
    ensures ok <==> members@.to_set() =~= visited@.to_set(),
))]
pub(super) fn spans<K>(members: &[K], visited: &[K]) -> bool {
    #[cfg(automerge_verus)]
    proof! {
        members@.unique_seq_to_set();
        visited@.unique_seq_to_set();
        if members@.len() == visited@.len() {
            vstd::set_lib::lemma_len_subset(visited@.to_set(), members@.to_set());
            crate::membership_proof::equal_cardinality(visited@, members@.to_set());
        }
    }
    members.len() == visited.len()
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) enum LevelFault<K> {
    NonSingletonLoose,
    Member(K),
}

#[cfg_attr(automerge_verus, verus_spec(result =>
    requires obeys_concrete_eq::<K>(), modeled(level, head),
        forall|i: int| 0 <= i < members@.len() ==> modeled(level, #[trigger] members@[i]),
    ensures level_result(members@, head, level, result),
))]
pub(super) fn levels<K: Copy + Eq>(
    members: &[K],
    head: K,
    level: impl Fn(K) -> usize,
) -> Result<(), LevelFault<K>> {
    let head_level = level(head);
    #[cfg(automerge_verus)]
    proof! { assert(head_level == model(level, head)); }
    if head_level == 0 {
        return if members.len() == 1 {
            Ok(())
        } else {
            Err(LevelFault::NonSingletonLoose)
        };
    }
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= members@.len(), head_level == model(level, head), head_level > 0, obeys_concrete_eq::<K>(),
            forall|j: int| 0 <= j < members@.len() ==> modeled(level, #[trigger] members@[j]),
            level_prefix(members@, head, level, i as int),
        decreases members@.len() - i,
    ))]
    while i < members.len() {
        let member = members[i];
        if !same(&member, &head) {
            #[cfg(automerge_verus)]
            proof! { assert(modeled(level, member)); }
            let height = level(member);
            #[cfg(automerge_verus)]
            proof! { assert(height == model(level, member)); }
            if height >= head_level {
                #[cfg(automerge_verus)]
                proof! { assert(members@.contains(member)); }
                return Err(LevelFault::Member(member));
            }
        }
        i += 1;
    }
    Ok(())
}

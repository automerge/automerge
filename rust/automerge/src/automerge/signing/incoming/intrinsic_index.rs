//! Intrinsic member validation and the per-batch actor-slot ledger.

#![allow(unexpected_cfgs)]

use std::collections::HashMap;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    index_spec::*,
    lookup_spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::{prelude::*, std_specs::hash::hash_map_inv};

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Member<A, H> {
    pub actor: A,
    pub seq: u64,
    pub hash: H,
    pub has_author: bool,
    pub conflicts_with_applied: bool,
}

#[cfg_attr(automerge_verus, verus_verify)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    Sequence,
    MisplacedAuthor,
    DuplicateHash,
    ConflictingSlot,
}

/// Index every validated member, and remember each exact actor-slot binding
/// for the current batch. Repeated delivery of a hash in a different group
/// is not equivocation; repeated membership within one group is invalid.
#[cfg_attr(automerge_verus, verus_spec(result =>
    requires laws::<A, H>(), hash_map_inv(old(batch)), observations(changes@, project),
    ensures hash_map_inv(final(batch)), index_result(changes@, project, old(batch)@, final(batch)@, result),
))]
pub(super) fn index<'a, M, A: Eq + Hash, H: Copy + Eq + Hash>(
    changes: &'a [M],
    project: impl Fn(&'a M) -> Member<A, H>,
    batch: &mut HashMap<(A, u64), H>,
) -> Result<HashMap<H, &'a M>, (usize, Fault)> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost initial = batch@; }
    let mut members = HashMap::with_capacity(changes.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= changes@.len(), laws::<A, H>(), observations(changes@, project),
            hash_map_inv(&members), hash_map_inv(batch), initial == old(batch)@,
            indexed(changes@, project, i as int, members@, initial, batch@),
        decreases changes@.len() - i,
    ))]
    while i < changes.len() {
        let change = &changes[i];
        #[cfg(automerge_verus)]
        proof_decl! {
            assert(modeled(project, change));
            let ghost prior_members = members@;
            let ghost prior_batch = batch@;
        }
        let member = project(change);
        #[cfg(automerge_verus)]
        proof! { assert(member == model(project, change)); }
        if let Err(fault) = insert(&mut members, batch, change, member) {
            #[cfg(automerge_verus)]
            proof! { stop(changes@, project, i as int, prior_members, initial, batch@, fault); }
            return Err((i, fault));
        }
        #[cfg(automerge_verus)]
        proof! { extend(changes@, project, i as int, prior_members, members@, initial, prior_batch, batch@); }
        i += 1;
    }
    Ok(members)
}

#[cfg_attr(automerge_verus, verus_spec(result =>
    requires laws::<A, H>(), hash_map_inv(old(members)), hash_map_inv(old(batch)),
    ensures hash_map_inv(final(members)), hash_map_inv(final(batch)),
        match result {
            Ok(()) => issue(member, old(members)@, old(batch)@).is_none()
                && final(members)@ == old(members)@.insert(member.hash, change)
                && final(batch)@ == old(batch)@.insert((member.actor, member.seq), member.hash),
            Err(fault) => issue(member, old(members)@, old(batch)@) == Some(fault)
                && final(members)@ == old(members)@ && final(batch)@ == old(batch)@,
        },
))]
fn insert<'a, M, A: Eq + Hash, H: Copy + Eq + Hash>(
    members: &mut HashMap<H, &'a M>,
    batch: &mut HashMap<(A, u64), H>,
    change: &'a M,
    member: Member<A, H>,
) -> Result<(), Fault> {
    if member.seq == 0 || member.seq > u64::from(u32::MAX) {
        return Err(Fault::Sequence);
    }
    if member.has_author && member.seq != 1 {
        return Err(Fault::MisplacedAuthor);
    }
    if members.contains_key(&member.hash) {
        return Err(Fault::DuplicateHash);
    }
    if member.conflicts_with_applied {
        return Err(Fault::ConflictingSlot);
    }
    let key = (member.actor, member.seq);
    if let Some(&existing) = batch.get(&key) {
        if !same(&existing, &member.hash) {
            return Err(Fault::ConflictingSlot);
        }
        #[cfg(automerge_verus)]
        proof! { assert(batch@.insert(key, member.hash) =~= batch@); }
    } else {
        batch.insert(key, member.hash);
    }
    members.insert(member.hash, change);
    Ok(())
}

#[cfg_attr(automerge_verus, verus_spec(equals =>
    requires vstd::laws_eq::obeys_concrete_eq::<H>(),
    ensures equals == (*left == *right),
))]
pub(super) fn same<H: Eq>(left: &H, right: &H) -> bool {
    let equals = <H as PartialEq>::eq(left, right);
    #[cfg(automerge_verus)]
    proof! {
        use vstd::std_specs::cmp::PartialEqSpec;
        reveal(vstd::laws_eq::obeys_concrete_eq);
        assert(<H as PartialEqSpec>::eq_spec(left, right) == (*left == *right));
    }
    equals
}

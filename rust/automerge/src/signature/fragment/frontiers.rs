//! Compute one maximum member per actor.

#![allow(unexpected_cfgs)]

use std::collections::BTreeMap;

#[cfg(automerge_verus)]
use crate::{
    frontier_spec::*,
    lookup_spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::prelude::*;
#[cfg(automerge_verus)]
use vstd::std_specs::btree::btree_map_inv;

#[cfg_attr(automerge_verus, verus_spec(
    requires actor_order::<A>(), btree_map_inv(old(frontiers)),
    ensures btree_map_inv(final(frontiers)), final(frontiers)@ == with_member(old(frontiers)@, actor, seq, hash),
))]
pub(super) fn offer<A: Ord, H>(frontiers: &mut BTreeMap<A, (u64, H)>, actor: A, seq: u64, hash: H) {
    if let Some((current, _)) = frontiers.get(&actor) {
        if *current >= seq {
            return;
        }
    }
    frontiers.insert(actor, (seq, hash));
}

/// Return exactly one maximum member per actor in the given input. `describe`
/// projects a member's actor, sequence, and hash without consulting evidence.
#[cfg_attr(automerge_verus, verus_spec(frontiers =>
    requires actor_order::<A>(),
        forall|i: int| 0 <= i < members@.len() ==> #[trigger] modeled(describe, &members@[i]),
    ensures btree_map_inv(&frontiers), exact_frontiers(members@, describe, members@.len() as int, frontiers@),
))]
pub(super) fn pick<'m, M, A: Ord, H>(
    members: &'m [M],
    describe: impl Fn(&'m M) -> (A, u64, H),
) -> BTreeMap<A, (u64, H)> {
    let mut frontiers = BTreeMap::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant actor_order::<A>(), btree_map_inv(&frontiers), i <= members@.len(),
            forall|j: int| 0 <= j < members@.len() ==> #[trigger] modeled(describe, &members@[j]),
            exact_frontiers(members@, describe, i as int, frontiers@),
        decreases members@.len() - i,
    ))]
    while i < members.len() {
        let member = &members[i];
        #[cfg(automerge_verus)]
        proof_decl! { let ghost before = frontiers@; assert(modeled(describe, member)); }
        let (actor, seq, hash) = describe(member);
        #[cfg(automerge_verus)]
        proof! { assert(model(describe, member) == (actor, seq, hash)); }
        offer(&mut frontiers, actor, seq, hash);
        #[cfg(automerge_verus)]
        proof! { added_member(members@, describe, i as int, before, frontiers@); }
        i += 1;
    }
    frontiers
}

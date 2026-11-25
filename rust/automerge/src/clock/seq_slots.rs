//! Maximum updates on the actor-indexed sequence slots of a SeqClock.

#![allow(unexpected_cfgs)]

use std::num::NonZeroU32;

#[cfg(automerge_verus)]
use crate::{
    clock_spec::*,
    lookup_spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_spec(changed =>
    requires data.is_some() ==> actor < old(slots)@.len(),
    ensures final(slots)@.len() == old(slots)@.len(),
        updated(old(slots)@, actor, data, final(slots)@, changed),
))]
pub(super) fn include(slots: &mut [Option<NonZeroU32>], actor: usize, data: Option<u32>) -> bool {
    if let Some(data) = data {
        let prior = slots[actor];
        match prior {
            None => {
                slots[actor] = NonZeroU32::new(data);
                true
            }
            Some(old_data) if old_data.get() < data => {
                slots[actor] = NonZeroU32::new(data);
                true
            }
            _ => false,
        }
    } else {
        false
    }
}

/// Include the selected member sequences using the post-apply actor table.
#[cfg_attr(automerge_verus, verus_spec(
    requires forall|j: int| 0 <= j < hashes@.len() ==> usable(lookup, #[trigger] hashes@[j], old(slots)@.len()),
    ensures maxima(old(slots)@, hashes@, lookup, hashes@.len() as int, final(slots)@),
))]
pub(super) fn include_members<K: Copy>(
    slots: &mut [Option<NonZeroU32>],
    hashes: &[K],
    lookup: impl Fn(K) -> (usize, u32),
) {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost base = slots@; }
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= hashes@.len(), maxima(base, hashes@, lookup, i as int, slots@),
            forall|j: int| 0 <= j < hashes@.len() ==> usable(lookup, #[trigger] hashes@[j], base.len()),
        decreases hashes@.len() - i,
    ))]
    while i < hashes.len() {
        let hash = hashes[i];
        #[cfg(automerge_verus)]
        proof_decl! { let ghost prior = slots@; assert(usable(lookup, hash, base.len())); }
        let (actor, seq) = lookup(hash);
        #[cfg(automerge_verus)]
        proof! { assert(model(lookup, hash) == (actor, seq)); }
        let _changed = include(slots, actor, Some(seq));
        #[cfg(automerge_verus)]
        proof! { include_next(base, hashes@, lookup, i as int, prior, slots@, _changed); }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{include_members, NonZeroU32};

    #[test]
    fn includes_only_the_exact_actor_maxima_and_preserves_unmentioned_slots() {
        let mut slots = [NonZeroU32::new(5), None, NonZeroU32::new(4), None];
        let members = [(0, 3), (1, 2), (0, 8), (1, 1), (0, 8)];
        include_members(&mut slots, &members, |slot| slot);
        assert_eq!(
            slots,
            [
                NonZeroU32::new(8),
                NonZeroU32::new(2),
                NonZeroU32::new(4),
                None
            ]
        );
    }
}

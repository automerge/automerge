use crate::lookup_spec::{model, modeled};
use std::num::NonZeroU32;
use vstd::prelude::*;

verus! {

pub(crate) open spec fn number(slot: Option<NonZeroU32>) -> u32 {
    match slot { None => 0, Some(n) => n@ }
}

pub(crate) open spec fn updated(
    old: Seq<Option<NonZeroU32>>, actor: usize, data: Option<u32>, new: Seq<Option<NonZeroU32>>, changed: bool,
) -> bool {
    match data {
        None => !changed && new == old,
        Some(seq) => {
            let a = actor as int;
            &&& changed == (old[a].is_none() || number(old[a]) < seq)
            &&& number(new[a]) == if number(old[a]) > seq { number(old[a]) } else { seq }
            &&& forall|i: int| 0 <= i < old.len() && i != a ==> #[trigger] new[i] == old[i]
        },
    }
}

pub(crate) open spec fn usable<K, F: Fn(K) -> (usize, u32)>(lookup: F, hash: K, len: nat) -> bool {
    &&& modeled(lookup, hash)
    &&& model(lookup, hash).0 < len
    &&& model(lookup, hash).1 > 0
}

// Each counter is the attained maximum of the old counter and exactly the
// selected member sequences for this actor. Absence has numeric value zero.
pub(crate) open spec fn maxima<K, F: Fn(K) -> (usize, u32)>(
    old: Seq<Option<NonZeroU32>>, hashes: Seq<K>, lookup: F, end: int, new: Seq<Option<NonZeroU32>>,
) -> bool {
    &&& new.len() == old.len()
    &&& forall|a: usize| a < new.len() ==> #[trigger] number(new[a as int]) >= number(old[a as int])
    &&& forall|i: int| 0 <= i < end ==>
        number(new[model(lookup, #[trigger] hashes[i]).0 as int]) >= model(lookup, hashes[i]).1
    &&& forall|a: usize| a < new.len() && #[trigger] number(new[a as int]) > number(old[a as int])
        ==> exists|i: int| 0 <= i < end && model(lookup, #[trigger] hashes[i]) == (a, number(new[a as int]))
}

pub(crate) proof fn include_next<K, F: Fn(K) -> (usize, u32)>(
    base: Seq<Option<NonZeroU32>>, hashes: Seq<K>, lookup: F, i: int,
    prior: Seq<Option<NonZeroU32>>, next: Seq<Option<NonZeroU32>>, changed: bool,
)
    requires 0 <= i < hashes.len(), maxima(base, hashes, lookup, i, prior),
        forall|j: int| 0 <= j <= i ==> model(lookup, #[trigger] hashes[j]).0 < prior.len(),
        model(lookup, hashes[i]).0 < prior.len(), next.len() == prior.len(),
        updated(prior, model(lookup, hashes[i]).0, Some(model(lookup, hashes[i]).1), next, changed),
    ensures maxima(base, hashes, lookup, i + 1, next),
{
    let (actor, seq) = model(lookup, hashes[i]);
    assert forall|a: usize| a < next.len() implies #[trigger] number(next[a as int]) >= number(prior[a as int]) by {};
    assert forall|j: int| 0 <= j < i + 1 implies
        number(next[model(lookup, #[trigger] hashes[j]).0 as int]) >= model(lookup, hashes[j]).1 by {
        if j < i {
            assert(number(prior[model(lookup, hashes[j]).0 as int]) >= model(lookup, hashes[j]).1);
        }
    }
    assert forall|a: usize| a < next.len() && #[trigger] number(next[a as int]) > number(base[a as int])
        implies exists|j: int| 0 <= j < i + 1 && model(lookup, #[trigger] hashes[j]) == (a, number(next[a as int])) by {
        if a == actor && number(next[a as int]) == seq {
            assert(model(lookup, hashes[i]) == (a, number(next[a as int])));
        } else {
            assert(number(next[a as int]) == number(prior[a as int]));
        }
    }
}

} // verus!

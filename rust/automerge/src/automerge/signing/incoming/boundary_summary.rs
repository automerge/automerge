//! Sparse summary of candidate maxima and applied seeds in a causal past.

#![allow(unexpected_cfgs)]

use std::collections::{BTreeSet, HashMap};
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{proof::*, spec::*};
#[cfg(automerge_verus)]
use vstd::prelude::*;
#[cfg(automerge_verus)]
use vstd::std_specs::{btree::*, hash::*, iter::IteratorSpec};
#[cfg(automerge_verus)]
verus! { broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas}; }

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Summary<A, K> {
    pub maxima: HashMap<A, u64>,
    pub seeds: BTreeSet<K>,
}

#[cfg_attr(automerge_verus, verus_verify)]
impl<A: Clone + Eq + Hash, K: Copy + Ord> Summary<A, K> {
    #[cfg_attr(automerge_verus, verus_spec(empty =>
        ensures summary_inv(&empty), view(&empty) == empty_view(),
    ))]
    pub(super) fn new() -> Self {
        Self {
            maxima: HashMap::new(),
            seeds: BTreeSet::new(),
        }
    }

    #[cfg_attr(automerge_verus, verus_spec(out =>
        requires laws::<A, K>(), vstd::pervasive::obeys_clone_eq::<K>(), summary_inv(self),
        ensures summary_inv(&out), view(&out) == view(self),
    ))]
    pub(super) fn copy_exact(&self) -> Self {
        let maxima = self.maxima.clone();
        #[cfg(automerge_verus)]
        proof! {
            assert(vstd::pervasive::obeys_clone_eq::<u64>());
            assert(maxima@ =~= self.maxima@) by {
                assert forall|a: A| #[trigger] maxima@.contains_key(a) implies maxima@[a] == self.maxima@[a] by {
                    assert(vstd::pervasive::cloned::<u64>(self.maxima@[a], maxima@[a]));
                }
            }
        }
        Self {
            maxima,
            seeds: self.seeds.clone(),
        }
    }

    #[cfg_attr(automerge_verus, verus_spec(
        requires laws::<A, K>(), summary_inv(old(self)),
        ensures summary_inv(final(self)), join(view(old(self)), seed_view(hash), view(final(self))),
            final(self).maxima@ == old(self).maxima@, final(self).seeds@ == old(self).seeds@.insert(hash),
    ))]
    pub(super) fn seed(&mut self, hash: K) {
        self.seeds.insert(hash);
    }

    #[cfg_attr(automerge_verus, verus_spec(
        requires laws::<A, K>(), summary_inv(old(self)),
        ensures summary_inv(final(self)), join(view(old(self)), actor_view(actor, seq), view(final(self))),
    ))]
    pub(super) fn candidate(&mut self, actor: A, seq: u64) {
        if let Some(&current) = self.maxima.get(&actor) {
            if current >= seq {
                return;
            }
        }
        self.maxima.insert(actor, seq);
    }

    #[cfg_attr(automerge_verus, verus_spec(
        requires laws::<A, K>(), summary_inv(old(self)), summary_inv(other),
        ensures summary_inv(final(self)), join(view(old(self)), view(other), view(final(self))),
    ))]
    pub(super) fn merge(&mut self, other: &Self) {
        #[cfg(automerge_verus)]
        proof_decl! { let ghost base = view(self); }
        let entries = other.maxima.iter();
        #[cfg(automerge_verus)]
        proof_decl! { let ghost all = entries.remaining(); }
        #[cfg_attr(automerge_verus, verus_spec(iter =>
            invariant laws::<A, K>(), summary_inv(self), iter.seq() == all,
                max_prefix(other.maxima@, all, iter.index(), base.maxima, self.maxima@),
                forall|i: int| 0 <= i < all.len() ==> other.maxima@.contains_key(#[trigger] *all[i].0)
                    && other.maxima@[*all[i].0] == *all[i].1,
                forall|a: A| #[trigger] other.maxima@.contains_key(a) ==> all.contains((&a, &other.maxima@[a])),
                self.seeds@ == base.seeds,
            ensures max_join(base.maxima, other.maxima@, self.maxima@),
        ))]
        for (actor, &seq) in entries {
            #[cfg(automerge_verus)]
            proof_decl! { let ghost prior = self.maxima@; }
            let owned = <A as Clone>::clone(actor);
            #[cfg(automerge_verus)]
            proof! {
                assert(vstd::pervasive::cloned::<A>(*actor, owned));
                assert(owned == *actor);
                assert(all[iter.index()] == (actor, &seq));
            }
            self.candidate(owned, seq);
            #[cfg(automerge_verus)]
            proof! { max_step(other.maxima@, all, iter.index(), base.maxima, prior, self.maxima@); }
        }

        #[cfg(automerge_verus)]
        proof! { assert(max_join(base.maxima, other.maxima@, self.maxima@)); }
        let seeds = other.seeds.iter();
        #[cfg(automerge_verus)]
        proof_decl! {
            let ghost seed_list = seeds.remaining();
            let ghost merged_maxima = self.maxima@;
        }
        #[cfg_attr(automerge_verus, verus_spec(iter =>
            invariant laws::<A, K>(), summary_inv(self), self.maxima@ == merged_maxima,
                max_join(base.maxima, other.maxima@, merged_maxima), seed_list.unref().to_set() == other.seeds@,
                iter.seq() == seed_list, self.seeds@ =~= base.seeds.union(seed_list.take(iter.index()).unref().to_set()),
            ensures self.seeds@ =~= base.seeds.union(other.seeds@),
        ))]
        for &seed in seeds {
            #[cfg(automerge_verus)]
            proof_decl! { let ghost prior = self.seeds@; }
            self.seed(seed);
            #[cfg(automerge_verus)]
            proof! { seed_step(base.seeds, seed_list, iter.index(), prior, self.seeds@); }
        }
    }
}

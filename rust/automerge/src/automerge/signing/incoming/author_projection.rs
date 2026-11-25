//! Interpret actor-chain results at their exact resolved change hashes.

#![allow(unexpected_cfgs)]

use super::actor_chains::{AuthorSource, Resolution};
use std::collections::HashMap;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    projection_spec::*,
    spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::{prelude::*, std_specs::hash::hash_map_inv};

type Materialized<K, T> = (HashMap<K, Option<T>>, HashMap<K, K>);

/// Resolve only the designated origin of each linked chain. A linked but
/// authorless result has a present `None` entry; broken changes never do.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires hash_laws::<K>(), hashes@.no_duplicates(), hashes@.len()==outcomes@.len(),
        source_views(hashes@,outcomes@,initial,applied),
    ensures materialized(hashes@,outcomes@,initial,applied,hashes@.len() as int,out.0@,out.1@),
))]
pub(super) fn materialize<K: Copy + Eq + Hash, T>(
    hashes: &[K],
    outcomes: &[Resolution],
    initial: impl Fn(usize) -> Option<T>,
    applied: impl Fn(usize) -> Option<T>,
) -> Materialized<K, T> {
    let mut authors = HashMap::new();
    let mut broken = HashMap::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=hashes@.len(), hashes@.len()==outcomes@.len(), hashes@.no_duplicates(), hash_laws::<K>(),
            source_views(hashes@,outcomes@,initial,applied), hash_map_inv(&authors), hash_map_inv(&broken),
            materialized(hashes@,outcomes@,initial,applied,i as int,authors@,broken@),
        decreases hashes@.len()-i,
    ))]
    while i < hashes.len() {
        let hash = hashes[i];
        #[cfg(automerge_verus)]
        proof_decl! { let ghost old_authors=authors@; let ghost old_broken=broken@; }
        match outcomes[i] {
            Resolution::Linked(AuthorSource::Initial(root)) => {
                #[cfg(automerge_verus)]
                proof! { assert(modeled(initial,root)); }
                let author = initial(root);
                #[cfg(automerge_verus)]
                proof! { assert(author==model(initial,root)); }
                authors.insert(hash, author);
            }
            Resolution::Linked(AuthorSource::Applied(slot)) => {
                #[cfg(automerge_verus)]
                proof! { assert(modeled(applied,slot)); }
                let author = applied(slot);
                #[cfg(automerge_verus)]
                proof! { assert(author==model(applied,slot)); }
                authors.insert(hash, author);
            }
            Resolution::Broken(cause) => {
                broken.insert(hash, hashes[cause]);
            }
        }
        #[cfg(automerge_verus)]
        proof! { record(hashes@,outcomes@,initial,applied,i as int,old_authors,old_broken,authors@,broken@); }
        i += 1;
    }
    (authors, broken)
}

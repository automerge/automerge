//! Project verified occurrence indices into the exact plan contents.

#![allow(unexpected_cfgs)]

use std::collections::HashMap;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::plan_spec::*;
#[cfg(automerge_verus)]
use vstd::prelude::*;

/// The inverse of the exact candidate or resolved-order sequence.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires crate::shape_spec::hash_laws::<K>(), hashes@.no_duplicates(),
    ensures inverse(hashes@, hashes@.len() as int, out@),
))]
pub(super) fn positions<K: Copy + Eq + Hash>(hashes: &[K]) -> HashMap<K, usize> {
    let mut out = HashMap::with_capacity(hashes.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= hashes@.len(), hashes@.no_duplicates(), crate::shape_spec::hash_laws::<K>(),
            vstd::std_specs::hash::hash_map_inv(&out), inverse(hashes@, i as int, out@),
        decreases hashes@.len() - i,
    ))]
    while i < hashes.len() {
        #[cfg(automerge_verus)]
        proof_decl! { let ghost prior = out@; }
        out.insert(hashes[i], i);
        #[cfg(automerge_verus)]
        proof! { position_added(hashes@, i as int, prior, out@); }
        i += 1;
    }
    out
}

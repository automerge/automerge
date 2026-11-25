//! Assemble the canonical descriptor from its dependency walks.

#![allow(unexpected_cfgs)]

use super::intrinsic_index::same;

#[cfg(automerge_verus)]
use crate::{
    canonical_spec::*,
    lookup_spec::{model, modeled},
};
#[cfg(automerge_verus)]
use vstd::{laws_eq::obeys_concrete_eq, prelude::*};

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Shape<K> {
    pub head: K,
    pub level: usize,
    pub boundary: Vec<K>,
    pub members: Vec<K>,
    pub checkpoints: Vec<K>,
}

/// Loose fragments are singleton changes with their direct parents as the
/// boundary. For hard fragments, use the exact boundary returned by the
/// first walk to compute members, then project only non-head checkpoints.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires obeys_concrete_eq::<K>(), vstd::pervasive::obeys_clone_eq::<K>(), can_assemble(head, level, direct, boundary, members),
    ensures descriptor(head, level, direct, boundary, members, out),
))]
pub(super) fn assemble<K: Copy + Eq>(
    head: K,
    level: impl Fn(K) -> usize,
    direct: impl FnOnce(K) -> Vec<K>,
    boundary: impl FnOnce(Vec<K>, usize) -> Vec<K>,
    members: impl FnOnce(&Vec<K>) -> Vec<K>,
) -> Shape<K> {
    let height = level(head);
    #[cfg(automerge_verus)]
    proof! { assert(height==model(level,head)); }
    let parents = direct(head);
    if height == 0 {
        let only = vec![head; 1];
        #[cfg(automerge_verus)]
        proof! {
            assert(vstd::pervasive::cloned::<K>(head, only@[0]));
            assert(only@ =~= seq![head]);
        }
        return Shape {
            head,
            level: 0,
            boundary: parents,
            members: only,
            checkpoints: Vec::new(),
        };
    }
    let cut = boundary(parents, height);
    #[cfg(automerge_verus)]
    proof! { assert(produced_boundary(head,level,direct,boundary,cut)); }
    let content = members(&cut);
    let checkpoints = checkpoints(head, &content, &level);
    Shape {
        head,
        level: height,
        boundary: cut,
        members: content,
        checkpoints,
    }
}

#[cfg_attr(automerge_verus, verus_spec(out =>
    requires obeys_concrete_eq::<K>(), every_level(content@,*level),
    ensures checkpoint_set(content@,head,*level,content@.len() as int,out@),
))]
fn checkpoints<K: Copy + Eq, F: Fn(K) -> usize>(head: K, content: &[K], level: &F) -> Vec<K> {
    let mut out = Vec::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=content@.len(), obeys_concrete_eq::<K>(), every_level(content@,*level),
            checkpoint_set(content@,head,*level,i as int,out@),
        decreases content@.len()-i,
    ))]
    while i < content.len() {
        let hash = content[i];
        #[cfg(automerge_verus)]
        proof_decl! { let ghost prior=out@; }
        if !same(&hash, &head) {
            #[cfg(automerge_verus)]
            proof! { assert(modeled(*level,hash)); }
            let height = (*level)(hash);
            #[cfg(automerge_verus)]
            proof! { assert(height==model(*level,hash)); }
            if height > 0 {
                out.push(hash);
            }
        }
        #[cfg(automerge_verus)]
        proof! { checkpoint_next(content@,head,*level,i as int,prior,out@); }
        i += 1;
    }
    out
}

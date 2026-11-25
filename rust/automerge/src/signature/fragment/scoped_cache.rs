//! Choose cached or fresh evidence without changing delivery scope.

#![allow(unexpected_cfgs)]

use super::scoped::{self, Bucket, Failure, Operations};

#[cfg(automerge_verus)]
use crate::{
    cache_spec::*,
    lookup_spec::{model, modeled},
    scoped_spec::batch_ready,
};
#[cfg(automerge_verus)]
use vstd::{laws_eq::obeys_concrete_eq, prelude::*};

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Scope<'s, H, Att, S> {
    pub hashes: &'s [H],
    pub attachment: &'s Att,
    pub cached: &'s S,
}

/// Own the exact inputs alongside the store produced from those inputs.
#[cfg_attr(automerge_verus, verus_verify)]
#[derive(Clone, Debug)]
pub(crate) struct Owned<H, Att, S> {
    pub(super) hashes: Vec<H>,
    pub(super) attachment: Att,
    pub(super) cached: S,
}

#[cfg_attr(automerge_verus, verus_verify)]
impl<H, Att, S> Owned<H, Att, S> {
    #[cfg_attr(automerge_verus, verus_spec(out =>
        requires preverify.requires((&hashes,&attachment)),
        ensures prepared_scope(hashes,attachment,preverify,out),
    ))]
    pub(super) fn prepare<E>(
        hashes: Vec<H>,
        attachment: Att,
        preverify: impl FnOnce(&Vec<H>, &Att) -> Result<S, E>,
    ) -> Result<Self, E> {
        match preverify(&hashes, &attachment) {
            Ok(cached) => Ok(Self {
                hashes,
                attachment,
                cached,
            }),
            Err(error) => Err(error),
        }
    }

    #[cfg_attr(automerge_verus, verus_spec(out =>
        ensures out.hashes@==self.hashes@, out.attachment==&self.attachment, out.cached==&self.cached,
    ))]
    pub(super) fn scope(&self) -> Scope<'_, H, Att, S> {
        Scope {
            hashes: self.hashes.as_slice(),
            attachment: &self.attachment,
            cached: &self.cached,
        }
    }
}

/// The cache belongs to this delivery. Reuse it only for the exact now-known
/// authors, or verify the same attachment and obligations with that resolver.
#[cfg_attr(automerge_verus, verus_spec(result =>
    requires obeys_concrete_eq::<A>(), cache_ready(&scope,ops.author,owner),
        cache_ok(&scope,ops.author,owner) ==> copy.requires((scope.cached,)),
        !cache_ok(&scope,ops.author,owner) ==> batch_ready(scope.hashes@,scope.attachment,empty,&ops),
    ensures settled(&scope,empty,&ops,owner,copy,result),
))]
pub(super) fn settle<'s, H: Copy, Att: 's, S, A: Eq + 's, E, K, V: Copy, W, const N: usize>(
    scope: Scope<'s, H, Att, S>,
    empty: S,
    ops: Operations<
        impl Fn(&'s Att, H) -> Bucket<V, N>,
        impl Fn(H) -> Option<A>,
        impl Fn(A) -> Result<K, E>,
        impl Fn(&K, H, V) -> Option<W>,
        impl Fn(S, W) -> S,
    >,
    owner: impl Fn(&'s S, H) -> Option<&'s A>,
    copy: impl FnOnce(&'s S) -> S,
) -> Result<S, Failure<H, E>> {
    if matches(&scope, &ops.author, &owner) {
        Ok(copy(scope.cached))
    } else {
        scoped::fresh(scope.hashes, scope.attachment, empty, ops)
    }
}

#[cfg_attr(automerge_verus, verus_spec(result =>
    requires obeys_concrete_eq::<A>(), cache_ready(scope,*author,*owner),
    ensures result == cache_ok(scope,*author,*owner),
))]
fn matches<
    's,
    H: Copy,
    Att,
    S,
    A: Eq + 's,
    R: Fn(H) -> Option<A>,
    O: Fn(&'s S, H) -> Option<&'s A>,
>(
    scope: &Scope<'s, H, Att, S>,
    author: &R,
    owner: &O,
) -> bool {
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=scope.hashes@.len(), cache_ready(scope,*author,*owner), obeys_concrete_eq::<A>(),
            prefix_matches(scope,*author,*owner,i as int),
        decreases scope.hashes@.len()-i,
    ))]
    while i < scope.hashes.len() {
        let hash = scope.hashes[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(*author,hash)); }
        let expected = (*author)(hash);
        #[cfg(automerge_verus)]
        proof! { assert(expected==model(*author,hash)); }
        let Some(expected) = expected else {
            return false;
        };
        #[cfg(automerge_verus)]
        proof! { assert(owner_ready(*owner,scope.cached,hash)); }
        let known = (*owner)(scope.cached, hash);
        #[cfg(automerge_verus)]
        proof! { assert(known==known_owner(*owner,scope.cached,hash)); }
        let Some(known) = known else {
            return false;
        };
        if !equal(&expected, known) {
            return false;
        }
        i += 1;
    }
    true
}

#[cfg_attr(automerge_verus, verus_spec(out =>
    requires obeys_concrete_eq::<A>(),
    ensures out==(*left==*right),
))]
fn equal<A: Eq>(left: &A, right: &A) -> bool {
    let out = <A as PartialEq>::eq(left, right);
    #[cfg(automerge_verus)]
    proof! {
        use vstd::std_specs::cmp::PartialEqSpec;
        reveal(obeys_concrete_eq);
        assert(<A as PartialEqSpec>::eq_spec(left,right)==(*left==*right));
    }
    out
}

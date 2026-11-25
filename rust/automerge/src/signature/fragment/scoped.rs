//! Follow one delivery's attachments and author resolver through verification.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    scoped_spec::*,
};
#[cfg(automerge_verus)]
use vstd::prelude::*;

/// Exact-hash projection of the attachment, in its original order. `present`
/// counts unsupported raw entries too, for the existing error precedence.
#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Bucket<V, const N: usize> {
    pub present: bool,
    pub entries: [Option<V>; N],
    pub len: usize,
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Operations<R, A, D, C, B> {
    pub entries: R,
    pub author: A,
    pub decode: D,
    pub check: C,
    pub retain: B,
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) enum Failure<H, E> {
    MissingSignature(H),
    MissingAuthor(H),
    InvalidKey(H, E),
}

/// Verify all the frontiers of this single attachment. The callbacks are
/// narrow operations: open an exact hash bucket, read its author, decode a
/// key, check a particular signature, and retain the resulting witness.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires batch_ready(hashes@, attachment, initial, &ops),
    ensures verification_result(hashes@, attachment, initial, &ops, out),
))]
pub(super) fn fresh<'s, H: Copy, Att: 's, A, K, E, S, V: Copy, W, const N: usize>(
    hashes: &[H],
    attachment: &'s Att,
    initial: S,
    ops: Operations<
        impl Fn(&'s Att, H) -> Bucket<V, N>,
        impl Fn(H) -> Option<A>,
        impl Fn(A) -> Result<K, E>,
        impl Fn(&K, H, V) -> Option<W>,
        impl Fn(S, W) -> S,
    >,
) -> Result<S, Failure<H, E>> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost mut states = seq![initial]; }
    let mut store = initial;
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= hashes@.len(), batch_ready(hashes@, attachment, initial, &ops),
            verified_prefix(hashes@, attachment, initial, &ops, states, i as int),
            store == states[i as int],
        decreases hashes@.len() - i,
    ))]
    while i < hashes.len() {
        let hash = hashes[i];
        #[cfg(automerge_verus)]
        proof! { assert(frontier_ready(attachment, hash, &ops)); }
        match one(attachment, hash, store, &ops) {
            Ok(next) => {
                #[cfg(automerge_verus)]
                proof! {
                    extend_verified(hashes@, attachment, initial, &ops, states, i as int, next);
                    states = states.push(next);
                }
                store = next;
            }
            Err(failure) => {
                #[cfg(automerge_verus)]
                proof! { stop(hashes@, attachment, initial, &ops, states, i as int, failure); }
                return Err(failure);
            }
        }
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { finish(hashes@, attachment, initial, &ops, states, store); }
    Ok(store)
}

#[cfg_attr(automerge_verus, verus_spec(out =>
    requires frontier_ready(attachment, hash, ops),
        forall|w: W| #[trigger] checked(attachment, hash, ops, w) ==> ops.retain.requires((store, w)),
    ensures match out {
        Ok(next) => retained_step(attachment, hash, ops, store, next),
        Err(error) => frontier_error(attachment, hash, ops, error),
    },
))]
fn one<'s, H: Copy, Att: 's, A, K, E, S, V: Copy, W, const N: usize>(
    attachment: &'s Att,
    hash: H,
    store: S,
    ops: &Operations<
        impl Fn(&'s Att, H) -> Bucket<V, N>,
        impl Fn(H) -> Option<A>,
        impl Fn(A) -> Result<K, E>,
        impl Fn(&K, H, V) -> Option<W>,
        impl Fn(S, W) -> S,
    >,
) -> Result<S, Failure<H, E>> {
    let entries = (ops.entries)(attachment, hash);
    #[cfg(automerge_verus)]
    proof! {
        assert(ops.entries.ensures((attachment, hash), entries));
        assert(entries == bucket(attachment, hash, ops.entries));
    }
    if !entries.present {
        return Err(Failure::MissingSignature(hash));
    }
    let author = (ops.author)(hash);
    #[cfg(automerge_verus)]
    proof! { assert(author == model(ops.author, hash)); }
    let Some(author) = author else {
        return Err(Failure::MissingAuthor(hash));
    };
    let key = match (ops.decode)(author) {
        Ok(key) => key,
        Err(error) => return Err(Failure::InvalidKey(hash, error)),
    };
    #[cfg(automerge_verus)]
    proof! { assert(key_for(hash, ops.author, ops.decode, key)); }
    let Some(witness) = search(&entries, &key, hash, &ops.check) else {
        #[cfg(automerge_verus)]
        proof! { missing_with_key(attachment, hash, ops, key, entries); }
        return Err(Failure::MissingSignature(hash));
    };
    #[cfg(automerge_verus)]
    proof! { checked_with_key(attachment, hash, ops, key, entries, witness); }
    let next = (ops.retain)(store, witness);
    #[cfg(automerge_verus)]
    proof! { assert(retained_step(attachment, hash, ops, store, next)); }
    Ok(next)
}

#[cfg_attr(automerge_verus, verus_spec(out =>
    requires ready_to_search(*entries, key, hash, *check),
    ensures found_in(*entries, key, hash, *check, out),
))]
pub(super) fn search<H: Copy, V: Copy, W, K, C: Fn(&K, H, V) -> Option<W>, const N: usize>(
    entries: &Bucket<V, N>,
    key: &K,
    hash: H,
    check: &C,
) -> Option<W> {
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=entries.len, ready_to_search(*entries,key,hash,*check),
            no_hit(*entries,key,hash,*check,i as int),
        decreases entries.len-i,
    ))]
    while i < entries.len {
        if let Some(signature) = entries.entries[i] {
            #[cfg(automerge_verus)]
            proof! { assert(can_try(*check,key,hash,signature)); }
            let trial = (*check)(key, hash, signature);
            #[cfg(automerge_verus)]
            proof! { assert(trial==attempt(*check,key,hash,signature)); }
            if let Some(witness) = trial {
                #[cfg(automerge_verus)]
                proof! { report_hit(*entries,key,hash,*check,i as int,witness); }
                return Some(witness);
            }
        }
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { reveal(found_in); }
    None
}

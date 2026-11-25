//! Check attached evidence before every actor's ancestry is known.

#![allow(unexpected_cfgs)]

use super::scoped::{search, Bucket, Failure, Operations};
use std::collections::HashMap;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::model,
    provisional_spec::*,
    scoped_spec::{bucket, found_in},
};
#[cfg(automerge_verus)]
use vstd::{prelude::*, std_specs::hash::hash_map_inv};

/// Require a typed attached signature at every frontier. Only a frontier
/// whose author is genuinely unresolved may defer crypto verification.
/// Prepared keys may be shared between deliveries, never proof witnesses.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires cache_laws::<A>(), decoded_cache(old(prepared),ops.decode),
        early_ready(hashes@,attachment,empty,&ops),
    ensures decoded_cache(final(prepared),ops.decode), preserved(old(prepared)@,final(prepared)@),
        provisional_result(hashes@,attachment,empty,&ops,out),
))]
pub(super) fn verify<
    's,
    H: Copy,
    Att: 's,
    A: Clone + Eq + Hash,
    K,
    E,
    S,
    V: Copy,
    W,
    const N: usize,
>(
    hashes: &[H],
    attachment: &'s Att,
    empty: S,
    prepared: &mut HashMap<A, K>,
    ops: Operations<
        impl Fn(&'s Att, H) -> Bucket<V, N>,
        impl Fn(H) -> Option<Option<A>>,
        impl Fn(A) -> Result<K, E>,
        impl Fn(&K, H, V) -> Option<W>,
        impl Fn(S, W) -> S,
    >,
) -> Result<S, Failure<H, E>> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost base=prepared@; let ghost mut states=seq![empty]; }
    let mut store = empty;
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=hashes@.len(), cache_laws::<A>(), decoded_cache(prepared,ops.decode),
            base==old(prepared)@, preserved(base,prepared@), early_ready(hashes@,attachment,empty,&ops),
            early_prefix(hashes@,attachment,empty,&ops,states,i as int), store==states[i as int],
        decreases hashes@.len()-i,
    ))]
    while i < hashes.len() {
        let hash = hashes[i];
        #[cfg(automerge_verus)]
        proof_decl! {
            let ghost before=prepared@;
            assert(ready_at(attachment,hash,&ops));
        }
        let out = one(attachment, hash, store, prepared, &ops);
        #[cfg(automerge_verus)]
        proof! { keep_original(base,before,prepared@); }
        match out {
            Ok(next) => {
                #[cfg(automerge_verus)]
                proof! { record(hashes@,attachment,empty,&ops,states,i as int,next); states=states.push(next); }
                store = next;
            }
            Err(failure) => {
                #[cfg(automerge_verus)]
                proof! { stop(hashes@,attachment,empty,&ops,states,i as int,failure); }
                return Err(failure);
            }
        }
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { finish(hashes@,attachment,empty,&ops,states,store); }
    Ok(store)
}

#[cfg_attr(automerge_verus, verus_spec(out =>
    requires cache_laws::<A>(), decoded_cache(old(prepared),ops.decode), ready_at(attachment,hash,ops),
        forall|w:W| #[trigger] checked_tentative(attachment,hash,ops,w) ==> ops.retain.requires((store,w)),
    ensures decoded_cache(final(prepared),ops.decode), preserved(old(prepared)@,final(prepared)@),
        match out {Ok(next)=>early_step(attachment,hash,ops,store,next), Err(failure)=>error_at(attachment,hash,ops,failure)},
))]
fn one<'s, H: Copy, Att: 's, A: Clone + Eq + Hash, K, E, S, V: Copy, W, const N: usize>(
    attachment: &'s Att,
    hash: H,
    store: S,
    prepared: &mut HashMap<A, K>,
    ops: &Operations<
        impl Fn(&'s Att, H) -> Bucket<V, N>,
        impl Fn(H) -> Option<Option<A>>,
        impl Fn(A) -> Result<K, E>,
        impl Fn(&K, H, V) -> Option<W>,
        impl Fn(S, W) -> S,
    >,
) -> Result<S, Failure<H, E>> {
    let entries = (ops.entries)(attachment, hash);
    #[cfg(automerge_verus)]
    proof! { assert(entries==bucket(attachment,hash,ops.entries)); }
    if entries.len == 0 {
        return Err(Failure::MissingSignature(hash));
    }
    let tentative = (ops.author)(hash);
    #[cfg(automerge_verus)]
    proof! { assert(tentative==model(ops.author,hash)); }
    let Some(tentative) = tentative else {
        return Ok(store);
    };
    let Some(author) = tentative else {
        return Err(Failure::MissingAuthor(hash));
    };
    #[cfg(automerge_verus)]
    proof_decl! { let ghost before=prepared@; }
    let witness = if let Some(key) = prepared.get(&author) {
        #[cfg(automerge_verus)]
        proof! { assert(tentative_key(hash,ops.author,ops.decode,*key)); }
        let result = search(&entries, key, hash, &ops.check);
        #[cfg(automerge_verus)]
        proof! { record_search(attachment,hash,ops,entries,*key,result); }
        result
    } else {
        let original = author.clone();
        #[cfg(automerge_verus)]
        proof! { assert(vstd::pervasive::cloned::<A>(author,original)); assert(original==author); }
        let key = match (ops.decode)(original) {
            Ok(key) => key,
            Err(error) => return Err(Failure::InvalidKey(hash, error)),
        };
        #[cfg(automerge_verus)]
        proof! { assert(tentative_key(hash,ops.author,ops.decode,key)); }
        let result = search(&entries, &key, hash, &ops.check);
        #[cfg(automerge_verus)]
        proof! { record_search(attachment,hash,ops,entries,key,result); }
        prepared.insert(author, key);
        #[cfg(automerge_verus)]
        proof! { append_key(before,prepared@,ops.decode,author,key); }
        result
    };
    let Some(witness) = witness else {
        return Err(Failure::MissingSignature(hash));
    };
    let next = (ops.retain)(store, witness);
    #[cfg(automerge_verus)]
    proof! { accepted(attachment,hash,ops,store,next,witness); }
    Ok(next)
}

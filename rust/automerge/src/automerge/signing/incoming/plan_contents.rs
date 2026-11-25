//! Project verified occurrence indices into the exact plan contents.

#![allow(unexpected_cfgs)]

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    plan_spec::*,
};
#[cfg(automerge_verus)]
use vstd::{prelude::*, std_specs::hash::hash_set_inv};

/// The selected member list in occurrence and delivery order, including
/// repeats across groups. Application later projects its unique ranks.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires member_views(selected@, contents, hash),
    ensures out@ =~= flatten(selected@, contents, hash),
))]
pub(super) fn selected_members<'a, T: 'a, H>(
    selected: &[usize],
    contents: impl Fn(usize) -> &'a [T],
    hash: impl Fn(&'a T) -> H,
) -> Vec<H> {
    let mut out = Vec::new();
    let mut i = 0;
    #[cfg(automerge_verus)]
    proof! { reveal(flatten); assert(flatten(selected@.take(0), contents, hash) == Seq::<H>::empty()); }
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= selected@.len(), member_views(selected@, contents, hash),
            out@ =~= flatten(selected@.take(i as int), contents, hash),
        decreases selected@.len() - i,
    ))]
    while i < selected.len() {
        let selected_index = selected[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(contents, selected_index)); }
        let members = contents(selected_index);
        #[cfg(automerge_verus)]
        proof_decl! { let ghost base = out@; assert(members == model(contents, selected_index)); }
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant j <= members@.len(), i < selected@.len(), selected_index == selected@[i as int],
                members == model(contents, selected_index), member_views(selected@, contents, hash),
                base =~= flatten(selected@.take(i as int), contents, hash),
                out@ =~= base + hashes(members@.take(j as int), hash),
            decreases members@.len() - j,
        ))]
        while j < members.len() {
            let member = &members[j];
            #[cfg(automerge_verus)]
            proof! { assert(modeled(hash, member)); }
            let h = hash(member);
            #[cfg(automerge_verus)]
            proof! { assert(h == model(hash, member)); extend_hashes(members@,j as int,hash); }
            out.push(h);
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! { assert(members@.take(j as int) =~= members@); extend_groups(selected@,i as int,contents,hash); }
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { assert(selected@.take(i as int) =~= selected@); }
    out
}

/// Candidate indices still blocked: directly waiting candidates and the
/// candidate indices of verified occurrences not selected by the selector.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires forall|i:int| 0<=i<excluded@.len() ==> modeled(source,#[trigger] excluded@[i]),
    ensures remaining(waiting@, excluded@, source, excluded@.len() as int, out@),
))]
pub(super) fn blocked(
    waiting: &[usize],
    excluded: &[usize],
    source: impl Fn(usize) -> usize,
) -> HashSet<usize> {
    let mut out = HashSet::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= waiting@.len(), hash_set_inv(&out), out@ =~= waiting@.take(i as int).to_set(),
        decreases waiting@.len() - i,
    ))]
    while i < waiting.len() {
        let index = waiting[i];
        #[cfg(automerge_verus)]
        proof! {
            waiting@.lemma_take_succ_push(i as int);
            waiting@.take(i as int).lemma_push_to_set_commute(index);
        }
        out.insert(index);
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { assert(waiting@.take(i as int) =~= waiting@); }
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= excluded@.len(), hash_set_inv(&out),
            forall|j:int| 0<=j<excluded@.len() ==> modeled(source,#[trigger] excluded@[j]),
            remaining(waiting@,excluded@,source,i as int,out@),
        decreases excluded@.len() - i,
    ))]
    while i < excluded.len() {
        let index = excluded[i];
        #[cfg(automerge_verus)]
        proof_decl! { let ghost prior=out@; assert(modeled(source,index)); }
        let candidate = source(index);
        #[cfg(automerge_verus)]
        proof! { assert(candidate==model(source,index)); }
        out.insert(candidate);
        #[cfg(automerge_verus)]
        proof! { extend_remaining(waiting@,excluded@,source,i as int,prior,out@); }
        i += 1;
    }
    out
}

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

/// Move exactly the selected payloads out of the verified occurrence list.
/// The verified-list indices do not have to survive document apply.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires ordered_selection(selected@,items@.len()),
        forall|j:int| 0<=j<items@.len() ==> #[trigger] modeled(payload,items@[j]),
    ensures out@.len()==selected@.len(),
        forall|i:int| 0<=i<selected@.len() ==> #[trigger] out@[i]==model(payload,items@[selected@[i] as int]),
))]
pub(super) fn take_selected<T, V>(
    items: Vec<T>,
    selected: &[usize],
    payload: impl Fn(T) -> V,
) -> Vec<V> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost input=items@; }
    let mut out = Vec::with_capacity(selected.len());
    let mut next = 0;
    #[cfg_attr(automerge_verus, verus_spec(iter =>
        invariant iter.seq().len()==input.len(), ordered_selection(selected@,input.len()),
            forall|j:int| 0<=j<input.len() ==> #[trigger] iter.seq()[j]==(j as usize,input[j]),
            forall|j:int| 0<=j<input.len() ==> modeled(payload,#[trigger] input[j]),
            emitted(input,selected@,payload,iter.index(),next,out@),
        ensures emitted(input,selected@,payload,input.len() as int,next,out@),
    ))]
    for (i, item) in items.into_iter().enumerate() {
        if next < selected.len() && selected[next] == i {
            #[cfg(automerge_verus)]
            proof! { assert(modeled(payload,item)); }
            let value = payload(item);
            #[cfg(automerge_verus)]
            proof! { assert(value==model(payload,item)); }
            out.push(value);
            next += 1;
        }
    }
    #[cfg(automerge_verus)]
    proof! { finish_selection(input,selected@,payload,next,out@); }
    out
}

/// Materialize the plan in the same order without another sort or filter.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires forall|i:int| 0<=i<order@.len() ==> modeled(resolve,#[trigger] order@[i]),
    ensures out@.len()==order@.len(),
        forall|i:int| 0<=i<order@.len() ==> #[trigger] out@[i]==model(resolve,order@[i]),
))]
pub(super) fn ordered_refs<'a, K: Copy, V>(
    order: &[K],
    resolve: impl Fn(K) -> &'a V,
) -> Vec<&'a V> {
    let mut out = Vec::with_capacity(order.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= order@.len(), out@.len()==i,
            forall|j:int| 0<=j<order@.len() ==> modeled(resolve,#[trigger] order@[j]),
            forall|j:int| 0<=j<i ==> #[trigger] out@[j]==model(resolve,order@[j]),
        decreases order@.len()-i,
    ))]
    while i < order.len() {
        let key = order[i];
        #[cfg(automerge_verus)]
        proof! { assert(modeled(resolve,key)); }
        let value = resolve(key);
        #[cfg(automerge_verus)]
        proof! { assert(value==model(resolve,key)); }
        out.push(value);
        i += 1;
    }
    out
}

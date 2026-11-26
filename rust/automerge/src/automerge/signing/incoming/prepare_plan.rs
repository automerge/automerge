//! Project a selection of verified occurrences into one coherent plan.

#![allow(unexpected_cfgs)]

use super::{actor_slots, apply_order};
use crate::admission::{select_mask, FragmentView};
use std::collections::HashSet;
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    prepare_spec::*,
};
#[cfg(automerge_verus)]
use vstd::{prelude::*, std_specs::hash::hash_set_inv};

pub(crate) type Verified<F, P> = (usize, (F, P));

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Projection<H, P> {
    pub proofs: Vec<P>,
    pub members: Vec<H>,
    pub remaining: HashSet<usize>,
}

/// Use one mask and one verified occurrence at a time. The proof and the
/// delivery whose members are published cannot come from different indices.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires views(candidates@,verified@,excluded@,contents,hash),
    ensures exact(candidates@,verified@,excluded@,waiting@,contents,hash,verified@.len() as int,&out),
))]
pub(super) fn project<'a, 'm, C: 'a, M: 'm, F, P, H>(
    candidates: &'a [C],
    verified: Vec<Verified<F, P>>,
    excluded: &[bool],
    waiting: &[usize],
    contents: impl Fn(&'a C) -> &'m [M],
    hash: impl Fn(&'m M) -> H,
) -> Projection<H, P> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost occurrences=verified@; }
    let mut out = Projection {
        proofs: Vec::new(),
        members: Vec::new(),
        remaining: HashSet::new(),
    };
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=waiting@.len(), hash_set_inv(&out.remaining), out.remaining@ =~= waiting@.take(i as int).to_set(),
        decreases waiting@.len()-i,
    ))]
    while i < waiting.len() {
        let index = waiting[i];
        #[cfg(automerge_verus)]
        proof! {
            waiting@.lemma_take_succ_push(i as int);
            waiting@.take(i as int).lemma_push_to_set_commute(index);
        }
        out.remaining.insert(index);
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! {
        assert(waiting@.take(i as int) =~= waiting@);
        initialize(candidates@,occurrences,excluded@,waiting@,contents,hash,&out);
    }
    #[cfg_attr(automerge_verus, verus_spec(iter =>
        invariant iter.seq().len()==occurrences.len(), views(candidates@,occurrences,excluded@,contents,hash),
            forall|j:int| 0<=j<occurrences.len() ==> #[trigger] iter.seq()[j]==(j as usize,occurrences[j]),
            hash_set_inv(&out.remaining),exact(candidates@,occurrences,excluded@,waiting@,contents,hash,iter.index(),&out),
        ensures exact(candidates@,occurrences,excluded@,waiting@,contents,hash,occurrences.len() as int,&out),
    ))]
    for (i, (candidate, (_, proof))) in verified.into_iter().enumerate() {
        #[cfg(automerge_verus)]
        proof_decl! {
            let ghost before_proofs=out.proofs@;
            let ghost before_members=out.members@;
            let ghost before_remaining=out.remaining@;
        }
        if excluded[i] {
            out.remaining.insert(candidate);
        } else {
            out.proofs.push(proof);
            let delivery = &candidates[candidate];
            #[cfg(automerge_verus)]
            proof! { assert(modeled(contents,delivery)); }
            let members = contents(delivery);
            #[cfg(automerge_verus)]
            proof! { assert(members==model(contents,delivery)); }
            let mut j = 0;
            #[cfg_attr(automerge_verus, verus_spec(
                invariant j<=members@.len(), views(candidates@,occurrences,excluded@,contents,hash),
                    i<occurrences.len(), !excluded@[i as int], candidate==occurrences[i as int].0,
                    members==model(contents,&candidates@[candidate as int]),
                    out.members@ =~= before_members+row(members@.take(j as int),hash),
                decreases members@.len()-j,
            ))]
            while j < members.len() {
                #[cfg(automerge_verus)]
                proof! { assert(modeled(hash,&members@[j as int])); }
                let value = hash(&members[j]);
                #[cfg(automerge_verus)]
                proof! { assert(value==model(hash,&members@[j as int])); extend_row(members@,j as int,hash); }
                out.members.push(value);
                j += 1;
            }
            #[cfg(automerge_verus)]
            proof! { assert(members@.take(j as int) =~= members@); }
        }
        #[cfg(automerge_verus)]
        proof! {
            extend_projection(candidates@,occurrences,excluded@,waiting@,contents,hash,i as int,
                before_proofs,before_members,before_remaining,&out);
        }
    }
    out
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Sources<'a, H, R, F> {
    pub ranked: &'a [H],
    pub rank: R,
    pub slot: F,
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Plan<H, P> {
    pub projection: Projection<H, P>,
    pub ordered: Vec<H>,
}

/// Select directly from the live verified occurrences, then project and
/// schedule the exact same cohort. No standalone selector list intervenes.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires crate::schedule_spec::slot_laws::<S>(),
        crate::projection_spec::hash_laws::<H>(), crate::projection_spec::observations(verified@,selector.view,selector.accepted),
        crate::projection_spec::raw_member_count(verified@,selector.view,verified@.len() as int)<=usize::MAX,
        ready(candidates@,verified@,contents,hash,sources.ranked@,sources.rank,sources.slot),
    ensures from_selection(candidates@,verified@,waiting@,selector.view,selector.accepted,contents,hash,sources.ranked@,sources.rank,sources.slot,out),
))]
pub(super) fn build<'a, 'm, C: 'a, M: 'm, F, P, H: Copy + Eq + Hash + 'static, S: Eq + Hash>(
    candidates: &'a [C],
    verified: Vec<Verified<F, P>>,
    waiting: &[usize],
    selector: Selector<
        impl for<'v> Fn(&'v Verified<F, P>) -> FragmentView<'v, H>,
        impl Fn(H) -> bool,
    >,
    contents: impl Fn(&'a C) -> &'m [M],
    hash: impl Fn(&'m M) -> H,
    sources: Sources<'_, H, impl Fn(H) -> Option<usize>, impl Fn(usize) -> (S, bool)>,
) -> Result<Plan<H, P>, H> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost occurrences=verified@; }
    let excluded = select_mask(&verified, selector.view, selector.accepted);
    #[cfg(automerge_verus)]
    proof! { selected_ready(candidates@,occurrences,excluded@,contents,hash,sources.ranked@,sources.rank,sources.slot); }
    let projection = project(candidates, verified, &excluded, waiting, contents, hash);
    #[cfg(automerge_verus)]
    proof! { assert(crate::schedule_spec::rank_inputs(projection.members@,sources.ranked@,sources.rank,sources.slot)); }
    match schedule(
        &projection.members,
        sources.ranked,
        sources.rank,
        sources.slot,
    ) {
        Ok(ordered) => {
            #[cfg(automerge_verus)]
            proof! { record_plan(candidates@,occurrences,waiting@,selector.view,selector.accepted,contents,hash,sources.ranked@,sources.rank,sources.slot,excluded@,projection,ordered); }
            Ok(Plan {
                projection,
                ordered,
            })
        }
        Err(conflict) => {
            #[cfg(automerge_verus)]
            proof! { record_conflict(candidates@,occurrences,waiting@,selector.view,selector.accepted,contents,hash,sources.ranked@,sources.rank,sources.slot,excluded@,projection,conflict); }
            Err(conflict)
        }
    }
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Selector<V, A> {
    pub view: V,
    pub accepted: A,
}

#[cfg_attr(automerge_verus, verus_spec(out =>
    requires crate::schedule_spec::slot_laws::<S>(),crate::schedule_spec::rank_inputs(members@,ranked@,rank,slot),
    ensures crate::schedule_spec::schedule_result(members@,ranked@,rank,slot,out),
))]
fn schedule<H: Copy, S: Eq + Hash>(
    members: &[H],
    ranked: &[H],
    rank: impl Fn(H) -> Option<usize>,
    slot: impl Fn(usize) -> (S, bool),
) -> Result<Vec<H>, H> {
    let mut ranks = apply_order::collect_ranks(members, rank);
    #[cfg(automerge_verus)]
    proof_decl! {
        let ghost first=ranks@;
        crate::schedule_spec::scan_ready(members@,ranked@,rank,slot,ranks@);
    }
    if let Some(conflict) = actor_slots::find_conflict(&ranks, &slot) {
        #[cfg(automerge_verus)]
        proof! { crate::schedule_spec::conflict_at(members@,ranked@,rank,slot,ranks@,conflict); }
        return Err(ranked[conflict]);
    }
    apply_order::sort_ranks(&mut ranks);
    #[cfg(automerge_verus)]
    proof! { crate::schedule_spec::sorted_ready(members@,ranked@,rank,slot,first,ranks@); }
    let mut ordered = Vec::with_capacity(ranks.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=ranks@.len(),ordered@.len()==i,
            crate::schedule_spec::ranked_order(members@,ranked@,rank,slot,ranks@),
            forall|j:int| 0<=j<i ==> #[trigger] ordered@[j]==ranked@[ranks@[j] as int],
        decreases ranks@.len()-i,
    ))]
    while i < ranks.len() {
        ordered.push(ranked[ranks[i]]);
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! { crate::schedule_spec::finish_order(members@,ranked@,rank,slot,ranks@,ordered); }
    Ok(ordered)
}

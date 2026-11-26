//! Translate exact fragment occurrences into the dense exclusion graph.

#![allow(unexpected_cfgs)]

use super::selection;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    lookup_spec::{model, modeled},
    projection_spec::*,
};
#[cfg(automerge_verus)]
use vstd::{
    prelude::*,
    std_specs::hash::{hash_map_inv, hash_set_inv},
};

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct FragmentView<'a, H> {
    pub members: &'a [H],
    pub boundary: &'a [H],
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Symbols<H> {
    pub(super) ids: HashMap<H, usize>,
    pub(super) hashes: Vec<H>,
}

#[cfg_attr(automerge_verus, verus_verify)]
impl<H: Copy + Eq + Hash> Symbols<H> {
    #[cfg_attr(automerge_verus, verus_spec(out=>ensures valid(&out),out.hashes@.len()==0))]
    fn new() -> Self {
        let out = Self {
            ids: HashMap::new(),
            hashes: Vec::new(),
        };
        #[cfg(automerge_verus)]
        proof! { assert(out.hashes@.to_set() =~= Set::<H>::empty()); }
        out
    }

    #[cfg_attr(automerge_verus, verus_spec(out=>
        requires hash_laws::<H>(), valid(old(self)),
        ensures valid(final(self)), interned(old(self).hashes@,final(self).hashes@,hash,out),
    ))]
    fn intern(&mut self, hash: H) -> usize {
        if let Some(&index) = self.ids.get(&hash) {
            return index;
        }
        let index = self.hashes.len();
        #[cfg(automerge_verus)]
        proof_decl! { let ghost before=self.hashes@;let ghost old_ids=self.ids@; }
        self.ids.insert(hash, index);
        self.hashes.push(hash);
        #[cfg(automerge_verus)]
        proof! { added_symbol(before,old_ids,self.hashes@,self.ids@,hash); }
        index
    }
}

/// Use raw fragments of one borrowed occurrence list and exact accepted
/// membership. A boundary is a requirement only if neither accepted
/// history nor this fragment itself supplies it.
#[cfg_attr(automerge_verus, verus_spec(out=>
    requires hash_laws::<H>(), observations(items@,view,accepted),
        raw_member_count(items@,view,items@.len() as int)<=usize::MAX,
    ensures greatest(items@,view,accepted,out@),
))]
pub(crate) fn select<'i, 'm, T: 'i, H: Copy + Eq + Hash + 'm>(
    items: &'i [T],
    view: impl Fn(&'i T) -> FragmentView<'m, H>,
    accepted: impl Fn(H) -> bool,
) -> Vec<bool> {
    let mut symbols = Symbols::new();
    let members = member_rows(items, &view, &mut symbols);
    #[cfg(automerge_verus)]
    proof_decl! { let ghost previous=symbols.hashes@; }
    let requirements = requirement_rows(items, &view, &accepted, &mut symbols);
    #[cfg(automerge_verus)]
    proof! {
        members_follow_extension(items@,view,previous,symbols.hashes@,members@);
        count_is_preserved(items@,view,symbols.hashes@,members@);
    }
    let counts = count_suppliers(&members, symbols.hashes.len());
    #[cfg(automerge_verus)]
    proof! { ready_to_exclude(items@,view,accepted,&symbols,members@,requirements@,counts@); }
    let excluded = selection::exclude(&members, &requirements, counts);
    #[cfg(automerge_verus)]
    proof! { translate_selection(items@,view,accepted,&symbols,members@,requirements@,excluded@); }
    excluded
}

#[cfg_attr(automerge_verus, verus_spec(out=>
    requires hash_laws::<H>(), valid(old(symbols)), member_observations(items@,*view),
    ensures valid(final(symbols)), prefix(old(symbols).hashes@,final(symbols).hashes@),
        out@.len()==items@.len(),members_aligned(items@,*view,final(symbols).hashes@,out@),
))]
fn member_rows<'i, 'm, T: 'i, H: Copy + Eq + Hash + 'm, F: Fn(&'i T) -> FragmentView<'m, H>>(
    items: &'i [T],
    view: &F,
    symbols: &mut Symbols<H>,
) -> Vec<Vec<usize>> {
    let mut rows = Vec::with_capacity(items.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=items@.len(), rows@.len()==i, hash_laws::<H>(), valid(symbols), member_observations(items@,*view),
            prefix(old(symbols).hashes@,symbols.hashes@),members_aligned(items@,*view,symbols.hashes@,rows@),
        decreases items@.len()-i,
    ))]
    while i < items.len() {
        let item = &items[i];
        #[cfg(automerge_verus)]
        proof! {assert(modeled(*view,item));}
        let fragment = (*view)(item);
        #[cfg(automerge_verus)]
        proof! {assert(fragment==model(*view,item));}
        let mut row = Vec::with_capacity(fragment.members.len());
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant j<=fragment.members@.len(),i<items@.len(),fragment==model(*view,&items@[i as int]),
                hash_laws::<H>(), valid(symbols),member_observations(items@,*view),rows@.len()==i,
                prefix(old(symbols).hashes@,symbols.hashes@),members_aligned(items@,*view,symbols.hashes@,rows@),
                aligned_row(fragment.members@.take(j as int),row@,symbols.hashes@),
            decreases fragment.members@.len()-j,
        ))]
        while j < fragment.members.len() {
            let hash = fragment.members[j];
            #[cfg(automerge_verus)]
            proof_decl! {let ghost before=symbols.hashes@;let ghost prior=row@;}
            let id = symbols.intern(hash);
            row.push(id);
            #[cfg(automerge_verus)]
            proof! {
                members_follow_extension(items@,*view,before,symbols.hashes@,rows@);
                extend_member_row(fragment.members@,j as int,prior,before,symbols.hashes@,id,row@);
                prove_prefix(old(symbols).hashes@,before,symbols.hashes@);
            }
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! { assert(fragment.members@.take(j as int) =~= fragment.members@); }
        rows.push(row);
        i += 1;
    }
    rows
}

#[cfg_attr(automerge_verus, verus_spec(out=>
    requires hash_laws::<H>(), valid(old(symbols)), observations(items@,*view,*accepted),
    ensures valid(final(symbols)),prefix(old(symbols).hashes@,final(symbols).hashes@),
        exact_requirements(items@,*view,*accepted,items@.len() as int,0,final(symbols),out@),
))]
fn requirement_rows<
    'i,
    'm,
    T: 'i,
    H: Copy + Eq + Hash + 'm,
    F: Fn(&'i T) -> FragmentView<'m, H>,
    A: Fn(H) -> bool,
>(
    items: &'i [T],
    view: &F,
    accepted: &A,
    symbols: &mut Symbols<H>,
) -> Vec<Vec<usize>> {
    let mut required = Vec::<Vec<usize>>::with_capacity(symbols.hashes.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=symbols.hashes@.len(),required@.len()==i,
            forall|h:int| 0<=h<i ==> (#[trigger] required@[h])@.len()==0,
        decreases symbols.hashes@.len()-i,
    ))]
    while i < symbols.hashes.len() {
        required.push(Vec::new());
        i += 1;
    }
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=items@.len(),hash_laws::<H>(),valid(symbols),observations(items@,*view,*accepted),
            prefix(old(symbols).hashes@,symbols.hashes@),exact_requirements(items@,*view,*accepted,i as int,0,symbols,required@),
        decreases items@.len()-i,
    ))]
    while i < items.len() {
        let item = &items[i];
        #[cfg(automerge_verus)]
        proof! {assert(modeled(*view,item));}
        let fragment = (*view)(item);
        #[cfg(automerge_verus)]
        proof! {assert(fragment==model(*view,item));}
        let own = member_set(fragment.members);
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant i<items@.len(),j<=fragment.boundary@.len(),hash_laws::<H>(),valid(symbols),observations(items@,*view,*accepted),
                fragment==model(*view,&items@[i as int]),hash_set_inv(&own),own@==fragment.members@.to_set(),
                prefix(old(symbols).hashes@,symbols.hashes@),exact_requirements(items@,*view,*accepted,i as int,j as int,symbols,required@),
            decreases fragment.boundary@.len()-j,
        ))]
        while j < fragment.boundary.len() {
            let boundary = fragment.boundary[j];
            #[cfg(automerge_verus)]
            proof_decl! {let ghost before=*symbols;let ghost prior=required@;}
            if !own.contains(&boundary) {
                #[cfg(automerge_verus)]
                proof! {assert(modeled(*accepted,boundary));}
                let covered = (*accepted)(boundary);
                #[cfg(automerge_verus)]
                proof! {assert(covered==model(*accepted,boundary));}
                if !covered {
                    let id = symbols.intern(boundary);
                    if id == required.len() {
                        required.push(Vec::new());
                    }
                    required[id].push(i);
                }
            }
            #[cfg(automerge_verus)]
            proof! {extend_requirements(items@,*view,*accepted,i as int,j as int,&before,prior,symbols,required@);prove_prefix(old(symbols).hashes@,before.hashes@,symbols.hashes@);}
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! {advance_requirements(items@,*view,*accepted,i as int,symbols,required@);}
        i += 1;
    }
    required
}

#[cfg_attr(automerge_verus, verus_spec(out=>
    requires hash_laws::<H>(), ensures hash_set_inv(&out),out@==members@.to_set(),
))]
fn member_set<H: Copy + Eq + Hash>(members: &[H]) -> HashSet<H> {
    let mut set = HashSet::with_capacity(members.len());
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=members@.len(),hash_laws::<H>(),hash_set_inv(&set),set@==members@.take(i as int).to_set(),
        decreases members@.len()-i,
    ))]
    while i < members.len() {
        #[cfg(automerge_verus)]
        proof! {members@.lemma_take_succ_push(i as int);members@.take(i as int).lemma_push_to_set_commute(members@[i as int]);}
        set.insert(members[i]);
        i += 1;
    }
    #[cfg(automerge_verus)]
    proof! {assert(members@.take(i as int) =~= members@);}
    set
}

#[cfg_attr(automerge_verus, verus_spec(out=>
    requires total_members(members@,members@.len() as int)<=usize::MAX,
        forall|i:int,j:int| 0<=i<members@.len() && 0<=j<members@[i]@.len() ==> #[trigger] members@[i]@[j]<size,
    ensures counted(members@,members@.len() as int,out@,size as nat),
))]
fn count_suppliers(members: &[Vec<usize>], size: usize) -> Vec<usize> {
    let mut counts = vec![0usize; size];
    let mut i = 0;
    #[cfg(automerge_verus)]
    proof! {reveal(crate::spec::candidate_edges);}
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i<=members@.len(),counted(members@,i as int,counts@,size as nat),
            total_members(members@,members@.len() as int)<=usize::MAX,
            forall|f:int,j:int| 0<=f<members@.len() && 0<=j<members@[f]@.len() ==> #[trigger] members@[f]@[j]<size,
        decreases members@.len()-i,
    ))]
    while i < members.len() {
        let row = &members[i];
        #[cfg(automerge_verus)]
        proof_decl! {let ghost before=counts@;count_limit(members@,i as int);start_counts(before,row@);}
        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant i<members@.len(),row@==members@[i as int]@,j<=row@.len(),
                counted(members@,i as int,before,size as nat),
                partial_counts(before,row@,j as int,counts@), total_members(members@,i as int)+row@.len()<=usize::MAX,
                forall|p:int| 0<=p<row@.len() ==> #[trigger] row@[p]<size,
            decreases row@.len()-j,
        ))]
        while j < row.len() {
            let id = row[j];
            #[cfg(automerge_verus)]
            proof_decl! {let ghost prior=counts@;count_room(members@,i as int,counts@,before,row@,j as int,id);}
            counts[id] += 1;
            #[cfg(automerge_verus)]
            proof! {count_edge(before,row@,j as int,prior,counts@);}
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! {finish_row_count(members@,i as int,before,counts@,size as nat);}
        i += 1;
    }
    counts
}

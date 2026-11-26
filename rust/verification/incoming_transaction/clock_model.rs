use crate::clock_spec::{maxima, number};
use crate::history_proof::chain_prefix;
use crate::history_spec::{actor_chains, causal_set, History};
use crate::lookup_spec::model;
use std::num::NonZeroU32;
use vstd::prelude::*;

verus! {

broadcast use {vstd::set::group_set_lemmas, vstd::seq_lib::group_seq_properties};

pub(crate) open spec fn aligned_slots<H>(graph:History<H,usize>,slots:Seq<Option<NonZeroU32>>)->bool {
    forall|h:H| #[trigger] graph.nodes.contains(h) ==> (graph.actor)(h)<slots.len()
}

pub(crate) open spec fn accepted_clock<H>(graph:History<H,usize>,slots:Seq<Option<NonZeroU32>>)->Set<H> {
    graph.nodes.filter(|h:H| (graph.sequence)(h)<=number(slots[(graph.actor)(h) as int]))
}

// Semantic view of the checked sequence-slot fold, with the graph lookup
// normalized to the actual post-apply history's actor and sequence.
pub(crate) open spec fn normalized_maxima<H>(graph:History<H,usize>,members:Seq<H>,old:Seq<Option<NonZeroU32>>,new:Seq<Option<NonZeroU32>>)->bool {
    &&& old.len()==new.len()
    &&& forall|a:usize| a<old.len() ==> #[trigger] number(new[a as int])>=number(old[a as int])
    &&& forall|i:int| 0<=i<members.len() ==>
        number(new[(graph.actor)(#[trigger] members[i]) as int])>=(graph.sequence)(members[i])
    &&& forall|a:usize| a<old.len() && #[trigger] number(new[a as int])>number(old[a as int])
        ==> exists|i:int| 0<=i<members.len() && (graph.actor)(#[trigger] members[i])==a
            && (graph.sequence)(members[i])==number(new[a as int])
}

pub(crate) proof fn fold_observes_maxima<H,F:Fn(H)->(usize,u32)>(graph:History<H,usize>,members:Seq<H>,lookup:F,old:Seq<Option<NonZeroU32>>,new:Seq<Option<NonZeroU32>>)
    requires maxima(old,members,lookup,members.len() as int,new),
        forall|i:int| 0<=i<members.len() ==> {
            let h=#[trigger] members[i]; (graph.actor)(h)==model(lookup,h).0 && (graph.sequence)(h)==model(lookup,h).1
        },
    ensures normalized_maxima(graph,members,old,new),
{
    assert forall|a:usize| a<old.len() && #[trigger] number(new[a as int])>number(old[a as int])
        implies exists|i:int| 0<=i<members.len() && (graph.actor)(#[trigger] members[i])==a
            && (graph.sequence)(members[i])==number(new[a as int]) by {
        let i=choose|i:int| 0<=i<members.len() && #[trigger] model(lookup,members[i])==(a,number(new[a as int]));
        assert((graph.actor)(members[i])==a && (graph.sequence)(members[i])==number(new[a as int]));
    }
}

pub(crate) proof fn closed_maxima_are_exact<H>(graph:History<H,usize>,members:Seq<H>,old:Seq<Option<NonZeroU32>>,new:Seq<Option<NonZeroU32>>)
    requires actor_chains(graph),aligned_slots(graph,old),normalized_maxima(graph,members,old,new),
        causal_set(graph,accepted_clock(graph,old).union(members.to_set())),
    ensures accepted_clock(graph,new) =~= accepted_clock(graph,old).union(members.to_set()),
{
    let before=accepted_clock(graph,old);
    let after=before.union(members.to_set());
    let updated=accepted_clock(graph,new);
    assert forall|h:H| #[trigger] updated.contains(h) <==> after.contains(h) by {
        if updated.contains(h) || after.contains(h) {
            assert(graph.nodes.contains(h));
            let a=(graph.actor)(h);
            let seq=(graph.sequence)(h);
            assert(number(new[a as int])>=number(old[a as int]));
            if after.contains(h) && !before.contains(h) {
                assert(members.contains(h));
                let j=choose|j:int| 0<=j<members.len() && #[trigger] members[j]==h;
                assert(number(new[(graph.actor)(members[j]) as int])>=seq);
            }
            if updated.contains(h) && !before.contains(h) {
                assert(number(new[a as int])>number(old[a as int]));
                let j=choose|j:int| 0<=j<members.len() && (graph.actor)(#[trigger] members[j])==a
                    && (graph.sequence)(members[j])==number(new[a as int]);
                let witness=members[j];
                assert(members.contains(witness));
                assert(after.contains(witness));
                chain_prefix(graph,after,h,witness);
            }
        }
    }
}

} // verus!

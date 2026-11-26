use crate::lookup_spec::{model, modeled};
use crate::order_spec::{exactly_selected, increasing};
use crate::slot_spec::{conflict_result, consistency_is_inherited, consistent};
use std::hash::Hash;
use vstd::{laws_eq::obeys_concrete_eq, prelude::*, std_specs::hash::obeys_key_model};

verus! {

broadcast use {vstd::seq_lib::group_seq_properties,vstd::set::group_set_lemmas};

pub(crate) open spec fn slot_laws<S:Eq+Hash>()->bool { obeys_key_model::<S>() && obeys_concrete_eq::<S>() }

pub(crate) open spec fn rank_inputs<H,S,R:Fn(H)->Option<usize>,F:Fn(usize)->(S,bool)>(members:Seq<H>,ranked:Seq<H>,rank:R,slot:F)->bool {
    &&& ranked.no_duplicates()
    &&& forall|i:int| 0<=i<members.len() ==> modeled(rank,#[trigger] members[i]) && match model(rank,members[i]) {
        None=> true,
        Some(r)=>r<ranked.len() && ranked[r as int]==members[i] && modeled(slot,r),
    }
}

pub(crate) open spec fn chosen_ranks<H,R:Fn(H)->Option<usize>>(members:Seq<H>,ranked:Seq<H>,rank:R,ranks:Seq<usize>)->bool {
    &&& ranks.no_duplicates() && exactly_selected(members,rank,members.len() as int,ranks)
    &&& forall|i:int| 0<=i<ranks.len() ==> (#[trigger] ranks[i]) < ranked.len()
}

pub(crate) open spec fn ranked_order<H,S,R:Fn(H)->Option<usize>,F:Fn(usize)->(S,bool)>(members:Seq<H>,ranked:Seq<H>,rank:R,slot:F,ranks:Seq<usize>)->bool {
    &&& chosen_ranks(members,ranked,rank,ranks)
    &&& increasing(ranks)
    &&& consistent(ranks,slot,ranks.len() as int)
}

pub(crate) open spec fn reports<H,S,R:Fn(H)->Option<usize>,F:Fn(usize)->(S,bool)>(members:Seq<H>,ranked:Seq<H>,rank:R,slot:F,ranks:Seq<usize>,index:usize,hash:H)->bool {
    &&& chosen_ranks(members,ranked,rank,ranks)
    &&& ranks.contains(index) && index<ranked.len()
    &&& conflict_result(ranks,slot,Some(index)) && ranked[index as int]==hash
}

#[verifier::opaque]
pub(crate) open spec fn schedule_result<H,S,R:Fn(H)->Option<usize>,F:Fn(usize)->(S,bool)>(members:Seq<H>,ranked:Seq<H>,rank:R,slot:F,result:Result<Vec<H>,H>)->bool {
    match result {
        Ok(order)=>exists|ranks:Seq<usize>| #[trigger] ranked_order(members,ranked,rank,slot,ranks)
            && order@.len()==ranks.len()
            && forall|i:int| 0<=i<ranks.len() ==> #[trigger] order@[i]==ranked[ranks[i] as int],
        Err(hash)=>has_conflict(members,ranked,rank,slot,hash),
    }
}

pub(crate) open spec fn has_conflict<H,S,R:Fn(H)->Option<usize>,F:Fn(usize)->(S,bool)>(members:Seq<H>,ranked:Seq<H>,rank:R,slot:F,hash:H)->bool {
    exists|ranks:Seq<usize>,index:usize| #[trigger] reports(members,ranked,rank,slot,ranks,index,hash)
}

pub(crate) proof fn scan_ready<H,S,R:Fn(H)->Option<usize>,F:Fn(usize)->(S,bool)>(members:Seq<H>,ranked:Seq<H>,rank:R,slot:F,ranks:Seq<usize>)
    requires rank_inputs(members,ranked,rank,slot),ranks.no_duplicates(),exactly_selected(members,rank,members.len() as int,ranks),
    ensures chosen_ranks(members,ranked,rank,ranks),forall|i:int| 0<=i<ranks.len() ==> modeled(slot,#[trigger] ranks[i]),
{
    assert forall|i:int| 0<=i<ranks.len() implies (#[trigger] ranks[i])<ranked.len() && modeled(slot,ranks[i]) by {
        assert(ranks.contains(ranks[i]));
        let j=choose|j:int| 0<=j<members.len() && model(rank,#[trigger] members[j])==Some(ranks[i]);
        assert(model(rank,members[j])==Some(ranks[i]));
    }
}

pub(crate) proof fn conflict_at<H,S,R:Fn(H)->Option<usize>,F:Fn(usize)->(S,bool)>(members:Seq<H>,ranked:Seq<H>,rank:R,slot:F,ranks:Seq<usize>,index:usize)
    requires chosen_ranks(members,ranked,rank,ranks),conflict_result(ranks,slot,Some(index)),
    ensures index<ranked.len(),schedule_result(members,ranked,rank,slot,Err(ranked[index as int])),
{
    reveal(schedule_result);
    let i=choose|i:int| 0<=i<ranks.len() && #[trigger] ranks[i]==index && crate::slot_spec::conflicts(ranks,slot,i)
        && consistent(ranks,slot,i);
    assert(ranks.contains(index));
    assert(reports(members,ranked,rank,slot,ranks,index,ranked[index as int]));
    assert(has_conflict(members,ranked,rank,slot,ranked[index as int]));
}

pub(crate) proof fn sorted_ready<H,S,R:Fn(H)->Option<usize>,F:Fn(usize)->(S,bool)>(members:Seq<H>,ranked:Seq<H>,rank:R,slot:F,first:Seq<usize>,sorted:Seq<usize>)
    requires chosen_ranks(members,ranked,rank,first),consistent(first,slot,first.len() as int),
        sorted.no_duplicates(),sorted.to_set()==first.to_set(),increasing(sorted),
    ensures ranked_order(members,ranked,rank,slot,sorted),
{
    assert forall|r:usize| #[trigger] sorted.contains(r) <==> first.contains(r) by {
        assert(sorted.to_set().contains(r) <==> sorted.contains(r));
        assert(first.to_set().contains(r) <==> first.contains(r));
    }
    assert forall|i:int| 0<=i<sorted.len() implies first.contains(sorted[i]) && (#[trigger] sorted[i])<ranked.len() by {
        assert(sorted.contains(sorted[i]));
        assert(first.contains(sorted[i]));
        let j=choose|j:int| 0<=j<first.len() && #[trigger] first[j]==sorted[i];
        assert(first[j]<ranked.len());
    }
    assert forall|r:usize| #[trigger] sorted.contains(r) <==> exists|j:int| 0<=j<members.len()
        && #[trigger] model(rank,members[j])==Some(r) by { assert(sorted.contains(r)==first.contains(r)); }
    assert(exactly_selected(members,rank,members.len() as int,sorted));
    assert(chosen_ranks(members,ranked,rank,sorted));
    consistency_is_inherited(first,sorted,slot);
}

pub(crate) proof fn finish_order<H,S,R:Fn(H)->Option<usize>,F:Fn(usize)->(S,bool)>(members:Seq<H>,ranked:Seq<H>,rank:R,slot:F,ranks:Seq<usize>,ordered:Vec<H>)
    requires ranked_order(members,ranked,rank,slot,ranks),ordered@.len()==ranks.len(),
        forall|i:int| 0<=i<ranks.len() ==> #[trigger] ordered@[i]==ranked[ranks[i] as int],
    ensures schedule_result(members,ranked,rank,slot,Ok(ordered)),
{ reveal(schedule_result); }

} // verus!

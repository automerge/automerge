use crate::admission::FragmentView;
use crate::lookup_spec::{model, modeled};
use crate::prepare_plan::{Plan, Projection, Verified};
use crate::projection_spec::greatest;
use crate::schedule_spec::{rank_inputs, schedule_result};
use vstd::prelude::*;

verus! {

broadcast use vstd::seq_lib::group_seq_properties;

pub(crate) open spec fn views<'a,'m,C:'a,M:'m,F,P,H,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H>(
    candidates:Seq<C>, verified:Seq<Verified<F,P>>, excluded:Seq<bool>, contents:R, hash:K,
)->bool {
    &&& verified.len()==excluded.len()<=usize::MAX
    &&& forall|i:int| 0<=i<verified.len() ==> (#[trigger] verified[i]).0<candidates.len()
    &&& forall|i:int| 0<=i<verified.len() && !excluded[i] ==>
        #[trigger] modeled(contents,&candidates[verified[i].0 as int])
    &&& forall|i:int,j:int| 0<=i<verified.len() && !excluded[i]
        && 0<=j<model(contents,&candidates[verified[i].0 as int])@.len() ==>
        #[trigger] modeled(hash,&(model(contents,&candidates[verified[i].0 as int])@)[j])
}

pub(crate) open spec fn row<'m,M:'m,H,K:Fn(&'m M)->H>(members:Seq<M>,hash:K)->Seq<H> {
    Seq::new(members.len(),|i:int| model(hash,&members[i]))
}

#[verifier::opaque]
pub(crate) open spec fn selected_proofs<F,P>(verified:Seq<Verified<F,P>>,excluded:Seq<bool>,end:int)->Seq<P>
    decreases end,
{
    if end<=0 {Seq::empty()}
    else if excluded[end-1] {selected_proofs(verified,excluded,end-1)}
    else {selected_proofs(verified,excluded,end-1).push(verified[end-1].1.1)}
}

#[verifier::opaque]
pub(crate) open spec fn accepted_members<'a,'m,C:'a,M:'m,F,P,H,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,excluded:Seq<bool>,contents:R,hash:K,end:int,
)->Seq<H>
    decreases end,
{
    if end<=0 {Seq::empty()}
    else if excluded[end-1] {accepted_members(candidates,verified,excluded,contents,hash,end-1)}
    else {accepted_members(candidates,verified,excluded,contents,hash,end-1)
        + row(model(contents,&candidates[verified[end-1].0 as int])@,hash)}
}

pub(crate) open spec fn excluded_occurrence<F,P>(verified:Seq<Verified<F,P>>,excluded:Seq<bool>,end:int,candidate:usize)->bool {
    exists|i:int| 0<=i<end && excluded[i] && (#[trigger] verified[i]).0==candidate
}

pub(crate) open spec fn blocked<F,P>(verified:Seq<Verified<F,P>>,excluded:Seq<bool>,waiting:Seq<usize>,end:int,out:Set<usize>)->bool {
    forall|candidate:usize| #[trigger] out.contains(candidate)<==>waiting.contains(candidate)
        || excluded_occurrence(verified,excluded,end,candidate)
}

// One exact occurrence projection, with three mutually aligned results.
pub(crate) open spec fn exact<'a,'m,C:'a,M:'m,F,P,H,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,excluded:Seq<bool>,waiting:Seq<usize>,contents:R,hash:K,end:int,out:&Projection<H,P>,
)->bool {
    &&& out.proofs@==selected_proofs(verified,excluded,end)
    &&& out.members@==accepted_members(candidates,verified,excluded,contents,hash,end)
    &&& blocked(verified,excluded,waiting,end,out.remaining@)
}

pub(crate) proof fn initialize<'a,'m,C:'a,M:'m,F,P,H,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,excluded:Seq<bool>,waiting:Seq<usize>,contents:R,hash:K,out:&Projection<H,P>,
)
    requires out.proofs@.len()==0,out.members@.len()==0,out.remaining@==waiting.to_set(),
    ensures exact(candidates,verified,excluded,waiting,contents,hash,0,out),
{
    reveal(selected_proofs);
    reveal(accepted_members);
    assert forall|candidate:usize| #[trigger] out.remaining@.contains(candidate) <==> waiting.contains(candidate) by {};
}

pub(crate) proof fn extend_row<'m,M:'m,H,K:Fn(&'m M)->H>(members:Seq<M>,j:int,hash:K)
    requires 0<=j<members.len(),
    ensures row(members.take(j+1),hash) =~= row(members.take(j),hash).push(model(hash,&members[j])),
{}

pub(crate) proof fn extend_projection<'a,'m,C:'a,M:'m,F,P,H,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,excluded:Seq<bool>,waiting:Seq<usize>,contents:R,hash:K,i:int,
    prior_proofs:Seq<P>,prior_members:Seq<H>,prior_remaining:Set<usize>,out:&Projection<H,P>,
)
    requires 0<=i<verified.len()==excluded.len(),
        prior_proofs==selected_proofs(verified,excluded,i),
        prior_members==accepted_members(candidates,verified,excluded,contents,hash,i),
        blocked(verified,excluded,waiting,i,prior_remaining),
        if excluded[i] {
            &&& out.proofs@==prior_proofs && out.members@==prior_members
            &&& out.remaining@==prior_remaining.insert(verified[i].0)
        } else {
            &&& out.proofs@==prior_proofs.push(verified[i].1.1)
            &&& out.members@==prior_members+row(model(contents,&candidates[verified[i].0 as int])@,hash)
            &&& out.remaining@==prior_remaining
        },
    ensures exact(candidates,verified,excluded,waiting,contents,hash,i+1,out),
{
    reveal(selected_proofs);
    reveal(accepted_members);
    assert forall|c:usize| #[trigger] out.remaining@.contains(c)<==>waiting.contains(c) || excluded_occurrence(verified,excluded,i+1,c) by {
        assert(excluded_occurrence(verified,excluded,i+1,c)<==>
            excluded_occurrence(verified,excluded,i,c) || (excluded[i] && verified[i].0==c));
    }
}

pub(crate) open spec fn ready<'a,'m,C:'a,M:'m,F,P,H,S,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,T:Fn(H)->Option<usize>,U:Fn(usize)->(S,bool)>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,contents:R,hash:K,ranked:Seq<H>,rank:T,slot:U,
)->bool {
    &&& ranked.no_duplicates()
    &&& verified.len()<=usize::MAX
    &&& forall|i:int| 0<=i<verified.len() ==> (#[trigger] verified[i]).0<candidates.len()
        && modeled(contents,&candidates[verified[i].0 as int])
        && rank_inputs(row(model(contents,&candidates[verified[i].0 as int])@,hash),ranked,rank,slot)
    &&& forall|i:int,j:int| 0<=i<verified.len() && 0<=j<model(contents,&candidates[verified[i].0 as int])@.len() ==>
        #[trigger] modeled(hash,&(model(contents,&candidates[verified[i].0 as int])@)[j])
}

pub(crate) proof fn selected_ready<'a,'m,C:'a,M:'m,F,P,H,S,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,T:Fn(H)->Option<usize>,U:Fn(usize)->(S,bool)>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,mask:Seq<bool>,contents:R,hash:K,ranked:Seq<H>,rank:T,slot:U,
)
    requires ready(candidates,verified,contents,hash,ranked,rank,slot),mask.len()==verified.len(),
    ensures views(candidates,verified,mask,contents,hash),
        rank_inputs(accepted_members(candidates,verified,mask,contents,hash,verified.len() as int),ranked,rank,slot),
{
    assert_prefix_ready(candidates,verified,mask,contents,hash,ranked,rank,slot,verified.len() as int);
}

proof fn assert_prefix_ready<'a,'m,C:'a,M:'m,F,P,H,S,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,T:Fn(H)->Option<usize>,U:Fn(usize)->(S,bool)>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,mask:Seq<bool>,contents:R,hash:K,ranked:Seq<H>,rank:T,slot:U,end:int,
)
    requires ready(candidates,verified,contents,hash,ranked,rank,slot),mask.len()==verified.len(),0<=end<=verified.len(),
    ensures rank_inputs(accepted_members(candidates,verified,mask,contents,hash,end),ranked,rank,slot),
    decreases end,
{
    reveal(accepted_members);
    if end>0 {
        assert_prefix_ready(candidates,verified,mask,contents,hash,ranked,rank,slot,end-1);
        let prior=accepted_members(candidates,verified,mask,contents,hash,end-1);
        let row= row(model(contents,&candidates[verified[end-1].0 as int])@,hash);
        if !mask[end-1] {
            assert(rank_inputs(row,ranked,rank,slot));
            let both=prior+row;
            assert forall|i:int| 0<=i<both.len() implies modeled(rank,#[trigger] both[i]) && match model(rank,both[i]) {
                None=>true,
                Some(r)=>r<ranked.len() && ranked[r as int]==both[i] && modeled(slot,r),
            } by {
                if i<prior.len() {assert(both[i]==prior[i]);} else {assert(both[i]==row[i-prior.len()]);}
            }
        }
    }
}

pub(crate) open spec fn projected_order<'a,'m,C:'a,M:'m,F,P,H:'static,S,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>,A:Fn(H)->bool,T:Fn(H)->Option<usize>,U:Fn(usize)->(S,bool)>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,waiting:Seq<usize>,view:V,accepted:A,contents:R,hash:K,
    ranked:Seq<H>,rank:T,slot:U,mask:Seq<bool>,projection:Projection<H,P>,order:Result<Vec<H>,H>,
)->bool {
    &&& greatest(verified,view,accepted,mask)
    &&& exact(candidates,verified,mask,waiting,contents,hash,verified.len() as int,&projection)
    &&& vstd::std_specs::hash::hash_set_inv(&projection.remaining)
    &&& schedule_result(projection.members@,ranked,rank,slot,order)
}

#[verifier::opaque]
pub(crate) open spec fn from_selection<'a,'m,C:'a,M:'m,F,P,H:'static,S,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>,A:Fn(H)->bool,T:Fn(H)->Option<usize>,U:Fn(usize)->(S,bool)>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,waiting:Seq<usize>,view:V,accepted:A,contents:R,hash:K,
    ranked:Seq<H>,rank:T,slot:U,out:Result<Plan<H,P>,H>,
)->bool {
    match out {
        Ok(plan)=>built_selection(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,plan),
        Err(conflict)=>failed_selection(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,conflict),
    }
}

pub(crate) open spec fn built_selection<'a,'m,C:'a,M:'m,F,P,H:'static,S,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>,A:Fn(H)->bool,T:Fn(H)->Option<usize>,U:Fn(usize)->(S,bool)>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,waiting:Seq<usize>,view:V,accepted:A,contents:R,hash:K,
    ranked:Seq<H>,rank:T,slot:U,plan:Plan<H,P>,
)->bool {
    exists|mask:Seq<bool>| #[trigger] projected_order(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,mask,plan.projection,Ok(plan.ordered))
}

pub(crate) open spec fn failed_selection<'a,'m,C:'a,M:'m,F,P,H:'static,S,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>,A:Fn(H)->bool,T:Fn(H)->Option<usize>,U:Fn(usize)->(S,bool)>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,waiting:Seq<usize>,view:V,accepted:A,contents:R,hash:K,
    ranked:Seq<H>,rank:T,slot:U,conflict:H,
)->bool {
    exists|mask:Seq<bool>,projection:Projection<H,P>| #[trigger] projected_order(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,mask,projection,Err(conflict))
}

pub(crate) proof fn record_plan<'a,'m,C:'a,M:'m,F,P,H:'static,S,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>,A:Fn(H)->bool,T:Fn(H)->Option<usize>,U:Fn(usize)->(S,bool)>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,waiting:Seq<usize>,view:V,accepted:A,contents:R,hash:K,
    ranked:Seq<H>,rank:T,slot:U,mask:Seq<bool>,projection:Projection<H,P>,ordered:Vec<H>,
)
    requires projected_order(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,mask,projection,Ok(ordered)),
    ensures from_selection(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,Ok(Plan{projection,ordered})),
{
    reveal(from_selection);
    let plan=Plan{projection,ordered};
    assert(projected_order(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,mask,plan.projection,Ok(plan.ordered)));
    assert(built_selection(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,plan));
}

pub(crate) proof fn record_conflict<'a,'m,C:'a,M:'m,F,P,H:'static,S,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>,A:Fn(H)->bool,T:Fn(H)->Option<usize>,U:Fn(usize)->(S,bool)>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,waiting:Seq<usize>,view:V,accepted:A,contents:R,hash:K,
    ranked:Seq<H>,rank:T,slot:U,mask:Seq<bool>,projection:Projection<H,P>,conflict:H,
)
    requires projected_order(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,mask,projection,Err(conflict)),
    ensures from_selection(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,Err(conflict)),
{ reveal(from_selection); assert(failed_selection(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,conflict)); }

} // verus!

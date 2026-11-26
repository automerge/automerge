use crate::admission::FragmentView;
use crate::admission_spec::{ancestry, Fragments};
use crate::candidate_origin::Candidate;
use crate::composition::{
    occurrence_alignment, prepared_plan_is_causally_exact, rank_dependencies, rank_truth,
};
use crate::document_spec::certified_plan;
use crate::effect_model::Document;
use crate::history_spec::History;
use crate::lookup_spec::model;
use crate::payload_spec::{at, covered, covered_hash, source_pos};
use crate::prepare_entry::Prepared;
use crate::prepare_plan::Verified;
use crate::prepare_spec::{accepted_members, from_selection, projected_order, row};
use vstd::prelude::*;

verus! {

broadcast use {vstd::set::group_set_lemmas, vstd::seq_lib::group_seq_properties};

// Each hash in the joint projection came from a specific chosen delivery
// row. Recover its original payload without a fresh graph lookup or a
// second, independently selected list of deliveries.
proof fn member_has_payload<'a,G:'a,T:'a,F,P,H,R:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H>(
    candidates:Seq<Candidate<G>>,verified:Seq<Verified<F,P>>,mask:Seq<bool>,members:R,key:K,end:int,hash:H,
)
    requires 0<=end<=verified.len()==mask.len(),
        forall|i:int| 0<=i<verified.len() ==> (#[trigger] verified[i]).0<candidates.len(),
        accepted_members(candidates,verified,mask,members,key,end).contains(hash),
    ensures covered_hash(candidates,members,key,hash),
    decreases end,
{
    reveal(accepted_members);
    assert(end>0);
    let before=accepted_members(candidates,verified,mask,members,key,end-1);
    if before.contains(hash) {
        member_has_payload(candidates,verified,mask,members,key,end-1,hash);
    } else {
        assert(!mask[end-1]);
        let source=verified[end-1].0 as int;
        let changes=model(members,&candidates[source])@;
        let ids=row(changes,key);
        assert(ids.contains(hash));
        let j=choose|j:int| 0<=j<ids.len() && #[trigger] ids[j]==hash;
        assert(model(key,at(candidates,members,source,j))==hash);
        assert(source_pos(candidates,members,key,source,j,candidates.len() as int,0,hash));
    }
}

/// Feed the actual plan compiler's result to the transaction boundary.
/// In particular the apply-source lookup precondition is a consequence
/// of the verified selected-member projection, not a trusted lookup claim.
pub(crate) proof fn compiled_plan_has_transaction_inputs<'a,G:'a,T:'a,F,P,H:'static,W,C,D,R:Fn(&'a Candidate<G>)->&'a[T],K:Fn(&'a T)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>,E:Fn(H)->bool,L:Fn(H)->Option<usize>,S,Slot:Fn(usize)->(S,bool)>(
    history:History<H,usize>,fragments:Fragments<H>,before:Document<H,W,C,D>,prepared:Prepared<G,crate::prepare_plan::Plan<H,P>>,
    verified:Seq<Verified<F,P>>,waiting:Seq<usize>,view:V,head:spec_fn(F)->H,accepted:E,members:R,key:K,limit:nat,ranked:Seq<H>,rank:L,slot:Slot,
)
    requires from_selection(prepared.candidates@,verified,waiting,view,accepted,members,key,ranked,rank,slot,Ok(prepared.plan)),
        verified.len()<=usize::MAX,occurrence_alignment(prepared.candidates@,verified,fragments,head,view,members,key),
        ancestry(history,fragments),before.accepted.subset_of(history.nodes),before.accepted.subset_of(before.applied),
        forall|h:H| #[trigger] before.accepted.contains(h) ==> (fragments.past)(h).subset_of(before.accepted),
        forall|h:H| #[trigger] model(accepted,h)==before.accepted.contains(h),
        forall|i:int| 0<=i<verified.len() ==> history.nodes.contains(head((#[trigger] verified[i]).1.0))
            && (fragments.level)(head(verified[i].1.0))<=limit,
        rank_truth(prepared.plan.projection.members@,before.applied,ranked,rank),rank_dependencies(history,before.applied,ranked),
    ensures certified_plan(before,history,prepared),covered(prepared.candidates@,prepared.plan.ordered@,members,key),
{
    prepared_plan_is_causally_exact(history,fragments,before.accepted,before.applied,prepared.candidates@,verified,waiting,view,
        head,accepted,members,key,limit,ranked,rank,slot,prepared.plan);
    reveal(from_selection);
    let plan=prepared.plan;
    let mask=choose|mask:Seq<bool>| #[trigger] projected_order(prepared.candidates@,verified,waiting,view,accepted,members,key,
        ranked,rank,slot,mask,plan.projection,Ok(plan.ordered));
    assert forall|i:int| 0<=i<plan.ordered@.len() implies covered_hash(prepared.candidates@,members,key,#[trigger] plan.ordered@[i]) by {
        let hash=plan.ordered@[i];
        assert(plan.ordered@.contains(hash));
        assert(plan.ordered@.to_set().contains(hash));
        assert(plan.ordered@.to_set()==plan.projection.members@.to_set().difference(before.applied));
        assert(plan.projection.members@.to_set().contains(hash));
        assert(plan.projection.members@.contains(hash));
        member_has_payload(prepared.candidates@,verified,mask,members,key,verified.len() as int,hash);
    }
}

} // verus!

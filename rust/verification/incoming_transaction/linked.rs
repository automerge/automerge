use crate::admission::FragmentView;
use crate::admission_spec::{ancestry, Fragments};
use crate::admission_txn::{DeliveryRead, Effects, Outcome, SigningEffects};
use crate::candidate_origin::Candidate;
use crate::composition::{occurrence_alignment, rank_dependencies, rank_truth};
use crate::document_spec::{
    certified_plan, document_contract, document_laws, incoming_document_contract,
};
use crate::effect_model::{Document, Model};
use crate::history_spec::History;
use crate::lookup_spec::model;
use crate::payload_spec::covered;
use crate::plan_bridge::compiled_plan_has_transaction_inputs;
use crate::prepare_entry::Prepared;
use crate::prepare_plan::{Plan, Verified};
use crate::prepare_spec::from_selection;
use crate::publication::Publish;
use crate::transaction_spec::{transaction, was_prepared};
use vstd::prelude::*;

verus! {

// Proof-facing observations of the evaluated cohort and graph projections.
// No receipt is added to the runtime document or to the prepared plan.
#[verifier::reject_recursive_types(F)]
#[verifier::reject_recursive_types(H)]
pub(crate) ghost struct CompilerView<H,F,P,View,Accepted,Rank,Slot> {
    pub verified: Seq<Verified<F,P>>,
    pub waiting: Seq<usize>,
    pub view: View,
    pub head: spec_fn(F)->H,
    pub accepted: Accepted,
    pub ranked: Seq<H>,
    pub rank: Rank,
    pub slot: Slot,
    pub fragments: Fragments<H>,
    pub level_limit: nat,
}

pub(crate) open spec fn observed_compilation<G,T,H:'static,F,P,W,C,D,SKey,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,V:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>,E:Fn(H)->bool,R:Fn(H)->Option<usize>,L:Fn(usize)->(SKey,bool)>(
    history:History<H,usize>,before:Document<H,W,C,D>,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,source:CompilerView<H,F,P,V,E,R,L>,
)->bool {
    &&& from_selection(p.candidates@,source.verified,source.waiting,source.view,source.accepted,read.members,read.key,source.ranked,source.rank,source.slot,Ok(p.plan))
    &&& source.verified.len()<=usize::MAX
    &&& occurrence_alignment(p.candidates@,source.verified,source.fragments,source.head,source.view,read.members,read.key)
    &&& ancestry(history,source.fragments)
    &&& before.accepted.subset_of(history.nodes) && before.accepted.subset_of(before.applied)
    &&& forall|h:H| #[trigger] before.accepted.contains(h) ==> (source.fragments.past)(h).subset_of(before.accepted)
    &&& forall|h:H| #[trigger] model(source.accepted,h)==before.accepted.contains(h)
    &&& forall|i:int| 0<=i<source.verified.len() ==> history.nodes.contains((source.head)((#[trigger] source.verified[i]).1.0))
        && (source.fragments.level)((source.head)(source.verified[i].1.0))<=source.level_limit
    &&& rank_truth(p.plan.projection.members@,before.applied,source.ranked,source.rank)
    &&& rank_dependencies(history,before.applied,source.ranked)
}

/// The linked observational contract. It takes the checked compiler result
/// and the canonical evaluation/graph alignment facts, not a promise of
/// safe application, causal closure, or exact publication. All three are
/// derived before interpreting the primitive document effects.
pub(crate) proof fn linked_document_contract<S,V,G,T,H:'static,F,P:'static,E,W,C,D,SlotKey,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&T>)->(S,Result<(),E>),O:FnOnce(S)->V,Effect:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V,View:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>,Accepted:Fn(H)->bool,Rank:Fn(H)->Option<usize>,Slot:Fn(usize)->(SlotKey,bool)>(
    initial:S,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<Effect,U,Q,I>>,out:Outcome<S,V,E>,meaning:Model<S,V,H,G,P,W,C,D>,
    planned_history:History<H,usize>,compiled:spec_fn(Prepared<G,Plan<H,P>>)->CompilerView<H,F,P,View,Accepted,Rank,Slot>,
)
    requires transaction(initial,read,effects,out),document_laws(read,effects,meaning,planned_history),
        forall|tmp:S,p:Prepared<G,Plan<H,P>>| #[trigger] was_prepared(initial,effects.prepare,tmp,p) ==>
            observed_compilation(planned_history,(meaning.before)(initial),p,read,compiled(p)),
    ensures document_contract(initial,read,effects,out,meaning),
{
    assert forall|tmp:S,p:Prepared<G,Plan<H,P>>| #[trigger] was_prepared(initial,effects.prepare,tmp,p)
        implies certified_plan((meaning.before)(initial),planned_history,p)
            && covered(p.candidates@,p.plan.ordered@,read.members,read.key) by {
        let source=compiled(p);
        compiled_plan_has_transaction_inputs(planned_history,source.fragments,(meaning.before)(initial),p,
            source.verified,source.waiting,source.view,source.head,source.accepted,read.members,read.key,
            source.level_limit,source.ranked,source.rank,source.slot);
    }
    incoming_document_contract(initial,read,effects,out,meaning,planned_history);
}

} // verus!

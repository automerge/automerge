use crate::admission_txn::{DeliveryRead, Effects, Outcome, SigningEffects};
use crate::candidate_origin::Candidate;
use crate::clock_model::{accepted_clock, aligned_slots};
use crate::composition::safe_schedule;
use crate::effect_model::{
    publish_laws, published_effect, selected_witness, Document, Model, SigningView,
};
use crate::history_spec::{actor_chains, causal_set, History};
use crate::lookup_spec::model;
use crate::origin_spec::groups_of;
use crate::pending_spec::retention;
use crate::prepare_entry::Prepared;
use crate::prepare_plan::Plan;
use crate::publication::Publish;
use crate::transaction_spec::*;
use vstd::prelude::*;

verus! {

broadcast use {vstd::set::group_set_lemmas,vstd::seq_lib::group_seq_properties};

/// Only the primitive operations cross a trusted boundary. The proof does
/// not assume a successful whole apply-and-complete operation is correct.
pub(crate) open spec fn document_laws<S,V,G,T,H:'static,P:'static,E,W,C,D,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&T>)->(S,Result<(),E>),O:FnOnce(S)->V,F:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<F,U,Q,I>>,meaning:Model<S,V,H,G,P,W,C,D>,planned_history:History<H,usize>,
)->bool {
    let before=meaning.before;
    let view=meaning.observe;
    // Preparation is read-only even on failure.
    &&& forall|s:S,t:S,r:Result<Prepared<G,Plan<H,P>>,E>| #[trigger] effects.prepare.ensures((s,),(t,r)) ==> before(t)==before(s)
    // All document/CRDT application semantics are at this leaf; the proof
    // verifies that exactly the scheduled payloads are passed to it.
    &&& forall|s:S,t:S,data:Vec<&T>,r:Result<(),E>| #[trigger] effects.apply.ensures((s,data),(t,r)) ==> {
        let hashes=data@.map_values(|c:&T| model(read.key,c));
        if r is Ok {
            &&& before(t).applied==before(s).applied.union(hashes.to_set())
            &&& before(t).accepted==before(s).accepted
            &&& before(t).evidence==before(s).evidence
            &&& before(t).content==(meaning.import)(before(s).content,hashes)
        } else {before(t)==before(s)}
    }
    &&& forall|content:C| #[trigger] (meaning.import)(content,Seq::<H>::empty())==content
    // Opening a fresh view interprets graph, clocks and evidence in the
    // post-apply actor layout, preserving membership by hash, not by slot.
    &&& forall|s:S,v:V| #[trigger] effects.open_signing.ensures((s,),v) ==> {
        let snapshot=view(v);
        &&& snapshot.graph.nodes==before(s).applied
        &&& aligned_slots(snapshot.graph,snapshot.slots) && actor_chains(snapshot.graph)
        &&& accepted_clock(snapshot.graph,snapshot.slots)==before(s).accepted
        &&& snapshot.evidence==before(s).evidence && snapshot.content==before(s).content
        &&& forall|h:H| #[trigger] snapshot.graph.nodes.contains(h) ==> (snapshot.graph.parents)(h)==(planned_history.parents)(h)
    }
    &&& publish_laws(view,meaning.proof_records,effects.signing.publish)
    &&& forall|v:V,groups:Vec<G>,after:V| #[trigger] effects.signing.install_pending.ensures((v,groups),after) ==>
        view(after)==SigningView{pending:groups@,..view(v)}
}

// The plan compiler and canonical-input theorem establish this premise.
// It mentions the planned graph, whose actor slots need not survive apply.
pub(crate) open spec fn certified_plan<H,G,P,W,C,D>(before:Document<H,W,C,D>,history:History<H,usize>,p:Prepared<G,Plan<H,P>>)->bool {
    &&& before.accepted.subset_of(before.applied)
    &&& safe_schedule(history,p.plan.projection.members@,before.applied,p.plan.ordered@)
    &&& causal_set(history,before.accepted.union(p.plan.projection.members@.to_set()))
}

pub(crate) open spec fn exact_apply_view<S,G,T,H,P,E,W,C,D,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,A:FnOnce(S,Vec<&T>)->(S,Result<(),E>)>(
    prior:S,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,apply:A,after:S,before:spec_fn(S)->Document<H,W,C,D>,import:spec_fn(C,Seq<H>)->C,
)->bool {
    let old=before(prior);
    let new=before(after);
    &&& new.applied == old.applied.union(p.plan.ordered@.to_set())
    &&& new.accepted==old.accepted && new.evidence==old.evidence
    &&& new.content==import(old.content,p.plan.ordered@)
}

proof fn interpret_apply<S,V,G,T,H:'static,P:'static,E,W,C,D,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&T>)->(S,Result<(),E>),O:FnOnce(S)->V,F:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    prior:S,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<F,U,Q,I>>,after:S,
    meaning:Model<S,V,H,G,P,W,C,D>,planned_history:History<H,usize>,
)
    requires document_laws(read,effects,meaning,planned_history),successful_apply(prior,p,read,effects.apply,after),
    ensures exact_apply_view(prior,p,read,effects.apply,after,meaning.before,meaning.import),
{
    let hashes=p.plan.ordered@;
    if hashes.len()>0 {
        let (data,result)=choose|data:Vec<&T>,result:Result<(),E>| #[trigger] applies(prior,p,read,effects.apply,data,after,result) && result is Ok;
        let observed=data@.map_values(|c:&T| model(read.key,c));
        assert(observed =~= hashes);
        assert(effects.apply.ensures((prior,data),(after,result)));
        assert(result is Ok);
        assert((meaning.before)(after).applied==(meaning.before)(prior).applied.union(observed.to_set()));
        assert((meaning.before)(after).content==(meaning.import)((meaning.before)(prior).content,observed));
    } else {
        assert(after==prior);
        assert(hashes =~= Seq::<H>::empty());
        let content=(meaning.before)(prior).content;
        assert((meaning.import)(content,Seq::<H>::empty())==content);
        assert((meaning.before)(prior).applied.union(hashes.to_set()) =~= (meaning.before)(prior).applied);
    }
}

pub(crate) open spec fn committed_document<S,V,G,H,P,W,C,D,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool>(
    initial:S,p:Prepared<G,Plan<H,P>>,published:V,done:V,meaning:Model<S,V,H,G,P,W,C,D>,viable:U,same:Q,
)->bool {
    let before=(meaning.before)(initial);
    let end_state=(meaning.observe)(done);
    &&& end_state.graph.nodes =~= before.applied.union(p.plan.ordered@.to_set())
    &&& end_state.content==(meaning.import)(before.content,p.plan.ordered@)
    &&& accepted_clock(end_state.graph,end_state.slots) =~= before.accepted.union(p.plan.projection.members@.to_set())
    &&& forall|w:W| #[trigger] end_state.evidence.contains(w) ==> before.evidence.contains(w)
        || selected_witness(p.plan.projection.proofs@,meaning.proof_records,p.plan.projection.proofs@.len() as int,w)
    &&& exists|kept:Seq<Candidate<G>>| #[trigger] retention(p.candidates@,p.plan.projection.remaining@,&published,viable,same,kept)
        && end_state.pending==groups_of(kept)
}

// A single observational theorem over the production transaction trace.
// The plan safety premise is discharged by prepared_plan_is_causally_exact
// using the checked prepare result and the actual canonical/slot projections.
pub(crate) open spec fn document_contract<S,V,G,T,H:'static,P:'static,E,W,C,D,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&T>)->(S,Result<(),E>),O:FnOnce(S)->V,F:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    initial:S,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<F,U,Q,I>>,out:Outcome<S,V,E>,
    meaning:Model<S,V,H,G,P,W,C,D>,
)
->bool {
    match out {
        Outcome::Aborted(state,_) => (meaning.before)(state)==(meaning.before)(initial),
        Outcome::Committed(done) => exists|tmp:S,p:Prepared<G,Plan<H,P>>,applied:S,signing:V,published:V,kept:Seq<Candidate<G>>,groups:Vec<G>|
            #[trigger] opened_after_apply(initial,tmp,p,read,effects,applied,signing)
            && #[trigger] pruned_plan(signing,p,&effects.signing,published,kept,groups)
            && committed_document(initial,p,published,done,meaning,effects.signing.viable,effects.signing.same_delivery),
    }
}

pub(crate) proof fn incoming_document_contract<S,V,G,T,H:'static,P:'static,E,W,C,D,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&T>)->(S,Result<(),E>),O:FnOnce(S)->V,F:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    initial:S,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<F,U,Q,I>>,out:Outcome<S,V,E>,
    meaning:Model<S,V,H,G,P,W,C,D>,planned_history:History<H,usize>,
)
    requires transaction(initial,read,effects,out),document_laws(read,effects,meaning,planned_history),
        forall|tmp:S,p:Prepared<G,Plan<H,P>>| #[trigger] was_prepared(initial,effects.prepare,tmp,p)
            ==> certified_plan((meaning.before)(initial),planned_history,p),
    ensures document_contract(initial,read,effects,out,meaning),
{
    reveal(transaction);
    let before=meaning.before;
    let view=meaning.observe;
    match out {
        Outcome::Aborted(state,error)=>{
            if !effects.prepare.ensures((initial,),(state,Err(error))) {
                let (tmp,p,data)=choose|tmp:S,p:Prepared<G,Plan<H,P>>,data:Vec<&T>|
                    #[trigger] was_prepared(initial,effects.prepare,tmp,p) && #[trigger] applies(tmp,p,read,effects.apply,data,state,Err(error));
                assert(before(tmp)==before(initial));
                assert(before(state)==before(tmp));
            }
        }
        Outcome::Committed(done)=>{
            let (tmp,p,applied,signing)=choose|tmp:S,p:Prepared<G,Plan<H,P>>,applied:S,signing:V|
                #[trigger] opened_after_apply(initial,tmp,p,read,effects,applied,signing) && completion(signing,p,&effects.signing,done);
            assert(before(tmp)==before(initial));
            interpret_apply(tmp,p,read,effects,applied,meaning,planned_history);
            assert(effects.open_signing.ensures((applied,),signing));
            let graph=view(signing).graph;
            let members=p.plan.projection.members@;
            let accepted=before(initial).accepted.union(members.to_set());
            assert(certified_plan(before(initial),planned_history,p));
            assert(accepted.subset_of(graph.nodes));
            assert(causal_set(graph,accepted)) by {
                assert forall|h:H| #[trigger] accepted.contains(h) implies (graph.parents)(h).subset_of(accepted) by {
                    assert(graph.nodes.contains(h));
                    assert((graph.parents)(h)==(planned_history.parents)(h));
                }
            }
            let (published,kept,groups)=choose|published:V,kept:Seq<Candidate<G>>,groups:Vec<G>|
                #[trigger] pruned_plan(signing,p,&effects.signing,published,kept,groups)
                && effects.signing.install_pending.ensures((published,groups),done);
            let members_arg=choose|input:&[H]| input@==members
                && #[trigger] crate::publication_spec::publication_trace(signing,p.plan.projection.proofs@,input,effects.signing.publish,published);
            published_effect(signing,p.plan.projection.proofs@,members_arg,effects.signing.publish,published,view,meaning.proof_records);
            assert(committed_document(initial,p,published,done,meaning,effects.signing.viable,effects.signing.same_delivery));
        }
    }
}

} // verus!

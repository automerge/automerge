use crate::admission_txn::{DeliveryRead, Effects, Outcome, SigningEffects};
use crate::candidate_origin::Candidate;
use crate::origin_spec::groups_of;
use crate::payload_spec::{covered, exact_payloads, hash_laws, sources};
use crate::pending_spec::{callbacks, retention};
use crate::prepare_entry::Prepared;
use crate::prepare_plan::Plan;
use crate::publication::Publish;
use crate::publication_spec::{can_publish, publication_trace};
use std::hash::Hash;
use vstd::{prelude::*, std_specs::hash::hash_set_inv};

verus! {

pub(crate) open spec fn published_plan<V,G,H:'static,P:'static,W:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U,D,I>(
    v:V, p:Prepared<G,Plan<H,P>>, effects:&SigningEffects<W,U,D,I>, after:V,
)->bool {
    exists|members:&[H]| members@==p.plan.projection.members@
        && #[trigger] publication_trace(v,p.plan.projection.proofs@,members,effects.publish,after)
}

pub(crate) open spec fn pruned_plan<V,G,H:'static,P:'static,W:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,D:Fn(&Candidate<G>,&Candidate<G>)->bool,I>(
    v:V, p:Prepared<G,Plan<H,P>>, effects:&SigningEffects<W,U,D,I>, after:V, kept:Seq<Candidate<G>>, groups:Vec<G>,
)->bool {
    &&& published_plan(v,p,effects,after)
    &&& retention(p.candidates@,p.plan.projection.remaining@,&after,effects.viable,effects.same_delivery,kept)
    &&& groups@==groups_of(kept)
}

pub(crate) open spec fn can_finish<V,G,H:'static,P:'static,W:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,D:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    v:V, p:Prepared<G,Plan<H,P>>, effects:&SigningEffects<W,U,D,I>,
)->bool {
    &&& hash_set_inv(&p.plan.projection.remaining)
    &&& forall|members:&[H]| (#[trigger] members@)==p.plan.projection.members@
        ==> can_publish(v,p.plan.projection.proofs@,members,effects.publish)
    &&& forall|after:V| #[trigger] published_plan(v,p,effects,after) ==>
        callbacks(p.candidates@,p.plan.projection.remaining@,&after,effects.viable,effects.same_delivery)
    &&& forall|after:V,kept:Seq<Candidate<G>>,groups:Vec<G>| #[trigger] pruned_plan(v,p,effects,after,kept,groups)
        ==> effects.install_pending.requires((after,groups))
}

pub(crate) open spec fn completion<V,G,H:'static,P:'static,W:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,D:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    v:V, p:Prepared<G,Plan<H,P>>, effects:&SigningEffects<W,U,D,I>, done:V,
)->bool {
    exists|published:V,kept:Seq<Candidate<G>>,groups:Vec<G>| #[trigger] pruned_plan(v,p,effects,published,kept,groups)
        && effects.install_pending.ensures((published,groups),done)
}

pub(crate) proof fn record_completion<V,G,H:'static,P:'static,W:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,D:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    v:V, p:Prepared<G,Plan<H,P>>, effects:&SigningEffects<W,U,D,I>, published:V,kept:Seq<Candidate<G>>,groups:Vec<G>,done:V,
)
    requires pruned_plan(v,p,effects,published,kept,groups),effects.install_pending.ensures((published,groups),done),
    ensures completion(v,p,effects,done),
{}

pub(crate) open spec fn was_prepared<S,G,H,P,E,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>)>(initial:S,prepare:B,tmp:S,p:Prepared<G,Plan<H,P>>)->bool {
    prepare.ensures((initial,),(tmp,Ok(p)))
}

pub(crate) open spec fn applies<S,G,C,H,P,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,A:FnOnce(S,Vec<&C>)->(S,Result<(),E>)>(
    prior:S,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,apply:A,data:Vec<&C>,after:S,result:Result<(),E>,
)->bool {
    &&& data@.len()>0
    &&& exact_payloads(p.candidates@,p.plan.ordered@,read.members,read.key,data@)
    &&& apply.ensures((prior,data),(after,result))
}

pub(crate) open spec fn successful_apply<S,G,C,H,P,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,A:FnOnce(S,Vec<&C>)->(S,Result<(),E>)>(
    prior:S,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,apply:A,after:S,
)->bool {
    if p.plan.ordered@.len()==0 {after==prior}
    else {exists|data:Vec<&C>,result:Result<(),E>| #[trigger] applies(prior,p,read,apply,data,after,result) && result is Ok}
}

pub(crate) proof fn record_apply<S,G,C,H,P,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,A:FnOnce(S,Vec<&C>)->(S,Result<(),E>)>(
    prior:S,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,apply:A,data:Vec<&C>,after:S,result:Result<(),E>,
)
    requires applies(prior,p,read,apply,data,after,result),result is Ok,
    ensures successful_apply(prior,p,read,apply,after),
{}

pub(crate) open spec fn reached_apply<S,G,C,H,P,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&C>)->(S,Result<(),E>),O,W>(
    initial:S,tmp:S,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,W>,applied:S,
)->bool {
    was_prepared(initial,effects.prepare,tmp,p) && successful_apply(tmp,p,read,effects.apply,applied)
}

pub(crate) open spec fn opened_after_apply<S,V,G,C,H,P,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&C>)->(S,Result<(),E>),O:FnOnce(S)->V,W>(
    initial:S,tmp:S,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,W>,applied:S,signing:V,
)->bool {
    reached_apply(initial,tmp,p,read,effects,applied) && effects.open_signing.ensures((applied,),signing)
}

pub(crate) open spec fn can_run<S,V,G,C,H:Eq+Hash+'static,P:'static,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&C>)->(S,Result<(),E>),O:FnOnce(S)->V,W:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,D:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    initial:S,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<W,U,D,I>>,
)->bool {
    &&& hash_laws::<H>()
    &&& effects.prepare.requires((initial,))
    &&& forall|tmp:S,p:Prepared<G,Plan<H,P>>| #[trigger] was_prepared(initial,effects.prepare,tmp,p) ==> {
        &&& sources(p.candidates@,read.members,read.key)
        &&& covered(p.candidates@,p.plan.ordered@,read.members,read.key)
        &&& forall|data:Vec<&C>| data@.len()>0 && #[trigger] exact_payloads(p.candidates@,p.plan.ordered@,read.members,read.key,data@)
            ==> effects.apply.requires((tmp,data))
    }
    &&& forall|tmp:S,p:Prepared<G,Plan<H,P>>,applied:S| #[trigger] reached_apply(initial,tmp,p,read,effects,applied)
        ==> effects.open_signing.requires((applied,))
    &&& forall|tmp:S,p:Prepared<G,Plan<H,P>>,applied:S,signing:V| #[trigger] opened_after_apply(initial,tmp,p,read,effects,applied,signing)
        ==> can_finish(signing,p,&effects.signing)
}

pub(crate) open spec fn rejected_apply<S,G,C,H,P,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&C>)->(S,Result<(),E>),O,W>(
    initial:S,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,W>,state:S,error:E,
)->bool {
    exists|tmp:S,p:Prepared<G,Plan<H,P>>,data:Vec<&C>| #[trigger] was_prepared(initial,effects.prepare,tmp,p)
        && #[trigger] applies(tmp,p,read,effects.apply,data,state,Err(error))
}

#[verifier::opaque]
pub(crate) open spec fn transaction<S,V,G,C,H:'static,P:'static,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&C>)->(S,Result<(),E>),O:FnOnce(S)->V,W:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,D:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    initial:S,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<W,U,D,I>>,out:Outcome<S,V,E>,
)->bool {
    match out {
        Outcome::Aborted(state,error)=>effects.prepare.ensures((initial,),(state,Err(error))) || rejected_apply(initial,read,effects,state,error),
        Outcome::Committed(done)=>exists|tmp:S,p:Prepared<G,Plan<H,P>>,applied:S,signing:V|
            #[trigger] opened_after_apply(initial,tmp,p,read,effects,applied,signing) && completion(signing,p,&effects.signing,done),
    }
}

pub(crate) proof fn record_rejection<S,V,G,C,H:'static,P:'static,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&C>)->(S,Result<(),E>),O:FnOnce(S)->V,W:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,D:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    initial:S,tmp:S,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<W,U,D,I>>,data:Vec<&C>,state:S,error:E,
)
    requires was_prepared(initial,effects.prepare,tmp,p),applies(tmp,p,read,effects.apply,data,state,Err(error)),
    ensures transaction(initial,read,effects,Outcome::Aborted(state,error)),
{ reveal(transaction); assert(rejected_apply(initial,read,effects,state,error)); }

pub(crate) proof fn record_commit<S,V,G,C,H:'static,P:'static,E,M:for<'a> Fn(&'a Candidate<G>)->&'a[C],K:Fn(&C)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&C>)->(S,Result<(),E>),O:FnOnce(S)->V,W:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,D:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V>(
    initial:S,tmp:S,p:Prepared<G,Plan<H,P>>,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<W,U,D,I>>,applied:S,signing:V,done:V,
)
    requires opened_after_apply(initial,tmp,p,read,effects,applied,signing),completion(signing,p,&effects.signing,done),
    ensures transaction(initial,read,effects,Outcome::Committed(done)),
{ reveal(transaction); }

} // verus!

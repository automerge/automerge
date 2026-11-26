//! Project the linked production proof onto the reviewable abstract contract.

use crate::abstract_spec as abstract_admission;
use crate::admission::FragmentView;
use crate::admission_txn::{DeliveryRead, Effects, Outcome, SigningEffects};
use crate::candidate_origin::Candidate;
use crate::clock_model::accepted_clock;
use crate::composition::{exact_members, kept, safe_schedule};
use crate::document_spec::{certified_plan, committed_document, document_contract, document_laws};
use crate::effect_model::{selected_witness, Document, Model, SigningView};
use crate::history_spec::History;
use crate::linked::{linked_document_contract, observed_compilation, CompilerView};
use crate::lookup_spec::model;
use crate::origin_spec::groups_of;
use crate::pending_spec::{equivalent, first, observed_viability, retained, retention};
use crate::plan_bridge::compiled_plan_has_transaction_inputs;
use crate::prepare_entry::Prepared;
use crate::prepare_plan::{Plan, Verified};
use crate::prepare_spec::{accepted_members, from_selection, projected_order, selected_proofs};
use crate::projection_spec::{greatest, raw_closed, raw_supplies};
use crate::publication::Publish;
use crate::transaction_spec::{opened_after_apply, pruned_plan, transaction, was_prepared};
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

pub(crate) open spec fn abstract_before<H,W,C,D>(before: Document<H,W,C,D>) -> abstract_admission::History<H,W,C> {
    abstract_admission::History { applied: before.applied, accepted: before.accepted, evidence: before.evidence, content: before.content }
}

pub(crate) open spec fn abstract_after<H,G,W,C>(after: SigningView<H,G,W,C>) -> abstract_admission::History<H,W,C> {
    abstract_admission::History {
        applied: after.graph.nodes, accepted: accepted_clock(after.graph, after.slots), evidence: after.evidence, content: after.content,
    }
}

pub(crate) open spec fn selection_view<H:'static,F,P,W,V:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>>(
    verified: Seq<Verified<F,P>>, mask: Seq<bool>, view: V, records: spec_fn(P)->Set<W>,
) -> abstract_admission::Selection<usize,H,W> {
    abstract_admission::Selection {
        chosen: kept(mask),
        verified: Map::new(Set::range(0usize, verified.len() as usize), |id:usize| {
            let fragment=model(view,&verified[id as int]);
            abstract_admission::Fragment {
                members: fragment.members@.to_set(), boundary: fragment.boundary@.to_set(), evidence: records(verified[id as int].1.1),
            }
        }),
    }
}

proof fn abstract_suppliers<H:'static,F,P,W,V:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>>(
    verified:Seq<Verified<F,P>>, mask:Seq<bool>, view:V, records:spec_fn(P)->Set<W>, chosen:Set<usize>, h:H,
)
    requires verified.len()<=usize::MAX, forall|i:usize| #[trigger] chosen.contains(i) ==> i<verified.len(),
    ensures raw_supplies(verified,view,chosen,h) <==> abstract_admission::supplies(selection_view(verified,mask,view,records).verified,chosen,h),
{
    let groups=selection_view(verified,mask,view,records).verified;
    if raw_supplies(verified,view,chosen,h) {
        let id=choose|id:usize| id<verified.len() && chosen.contains(id) && #[trigger] model(view,&verified[id as int]).members@.contains(h);
        assert(groups.dom().contains(id));
        assert(model(view,&verified[id as int]).members@.to_set().contains(h));
        assert(groups[id].members.contains(h));
    }
    if abstract_admission::supplies(groups,chosen,h) {
        let id=choose|id:usize| #[trigger] chosen.contains(id) && groups[id].members.contains(h);
        assert(id<verified.len());
        assert(groups.dom().contains(id));
        assert(model(view,&verified[id as int]).members@.contains(h));
    }
}

proof fn abstract_closure<H:'static,F,P,W,V:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>,A:Fn(H)->bool>(
    verified:Seq<Verified<F,P>>,mask:Seq<bool>,view:V,accepted:A,history:Set<H>,records:spec_fn(P)->Set<W>,chosen:Set<usize>,
)
    requires verified.len()<=usize::MAX,forall|h:H| #[trigger] model(accepted,h)==history.contains(h),
    ensures raw_closed(verified,view,accepted,chosen) <==>
        abstract_admission::closed(selection_view(verified,mask,view,records).verified,chosen,history),
{
    let groups=selection_view(verified,mask,view,records).verified;
    if raw_closed(verified,view,accepted,chosen) {
        assert(chosen.subset_of(groups.dom()));
        assert forall|i:usize,h:H| #[trigger] chosen.contains(i) && #[trigger] groups[i].boundary.contains(h)
            implies history.contains(h) || abstract_admission::supplies(groups,chosen,h) by {
            assert(i<verified.len());
            let view_row=model(view,&verified[i as int]);
            assert(view_row.boundary@.contains(h));
            abstract_suppliers(verified,mask,view,records,chosen,h);
        }
    }
    if abstract_admission::closed(groups,chosen,history) {
        assert forall|i:usize| #[trigger] chosen.contains(i) implies i<verified.len() by { assert(groups.dom().contains(i)); }
        assert forall|i:usize,h:H| #[trigger] chosen.contains(i) && #[trigger] model(view,&verified[i as int]).boundary@.contains(h)
            implies model(accepted,h) || raw_supplies(verified,view,chosen,h) by {
            assert(groups[i].boundary.contains(h));
            abstract_suppliers(verified,mask,view,records,chosen,h);
        }
    }
}

proof fn abstract_greatest<H:'static,F,P,W,V:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>,A:Fn(H)->bool>(
    verified:Seq<Verified<F,P>>,mask:Seq<bool>,view:V,accepted:A,history:Set<H>,records:spec_fn(P)->Set<W>,
)
    requires greatest(verified,view,accepted,mask),verified.len()<=usize::MAX,
        forall|h:H| #[trigger] model(accepted,h)==history.contains(h),
    ensures abstract_admission::greatest(selection_view(verified,mask,view,records),history),
{
    let selection=selection_view(verified,mask,view,records);
    abstract_closure(verified,mask,view,accepted,history,records,selection.chosen);
    assert forall|other:Set<usize>| #[trigger] abstract_admission::closed(selection.verified,other,history)
        implies other.subset_of(selection.chosen) by {
        abstract_closure(verified,mask,view,accepted,history,records,other);
        assert(raw_closed(verified,view,accepted,other));
    }
}

proof fn resulting_witness_came_from_selection<F,P,W>(
    verified:Seq<Verified<F,P>>,mask:Seq<bool>,end:int,records:spec_fn(P)->Set<W>,w:W,
)
    requires 0<=end<=verified.len()==mask.len()<=usize::MAX,
        selected_witness(selected_proofs(verified,mask,end),records,selected_proofs(verified,mask,end).len() as int,w),
    ensures exists|id:usize| 0<=id<end && !mask[id as int] && #[trigger] records(verified[id as int].1.1).contains(w),
    decreases end,
{
    reveal(selected_proofs);
    let proofs=selected_proofs(verified,mask,end);
    let i=choose|i:int| 0<=i<proofs.len() && #[trigger] records(proofs[i]).contains(w);
    assert(end>0);
    let prior=selected_proofs(verified,mask,end-1);
    if i<prior.len() {
        assert(records(prior[i]).contains(w));
        resulting_witness_came_from_selection(verified,mask,end-1,records,w);
    } else {
        assert(!mask[end-1]);
        assert(proofs[i]==verified[end-1].1.1);
        assert(records(verified[end-1].1.1).contains(w));
    }
}

pub(crate) open spec fn queue_view<G>(candidates:Seq<Candidate<G>>,blocked:Set<usize>)->abstract_admission::Queue<int,G> {
    let order=Seq::new(candidates.len(),|i:int| i);
    abstract_admission::Queue {
        order,
        deliveries: Map::new(order.to_set(),|i:int| candidates[i].group),
        blocked: order.to_set().filter(|i:int| blocked.contains(i as usize)),
    }
}

pub(crate) open spec fn viable_view<G,V,U:Fn(&V,&Candidate<G>)->bool>(candidates:Seq<Candidate<G>>,state:&V,viable:U)->spec_fn(int)->bool {
    |i:int| observed_viability(viable,state,&candidates[i])
}

pub(crate) open spec fn same_view<G,Q:Fn(&Candidate<G>,&Candidate<G>)->bool>(candidates:Seq<Candidate<G>>,same:Q)->spec_fn(int,int)->bool {
    |i:int,j:int| equivalent(same,candidates[i],candidates[j])
}

proof fn queue_indices(order:Seq<int>,n:nat)
    requires order==Seq::new(n,|i:int| i),
    ensures order.no_duplicates(),forall|i:int| #[trigger] order.contains(i)<==>0<=i<n,
        forall|i:int| 0<=i<n ==> #[trigger] order.index_of(i)==i,
{
    assert forall|i:int| #[trigger] order.contains(i)<==>0<=i<n by {
        if order.contains(i) {
            let j=choose|j:int| 0<=j<order.len() && order[j]==i;
            assert(order[j]==j);
        } else if 0<=i<n { assert(order[i]==i); }
    }
    assert forall|i:int| 0<=i<n implies #[trigger] order.index_of(i)==i by {
        assert(order[i]==i);
        assert(order.contains(i));
        let j=order.index_of(i);
        assert(0<=j<order.len() && order[j]==i);
        assert(order[j]==j);
    }
}

proof fn first_matches<G,V,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool>(
    input:Seq<Candidate<G>>,blocked:Set<usize>,state:&V,viable:U,same:Q,i:int,
)
    requires 0<=i<input.len(),
    ensures first(input,blocked,state,viable,same,i) <==>
        abstract_admission::first_viable(queue_view(input,blocked),i,viable_view(input,state,viable),same_view(input,same)),
{
    let queue=queue_view(input,blocked);
    let vi=viable_view(input,state,viable);
    let eq=same_view(input,same);
    queue_indices(queue.order,input.len());
    assert(queue.order.contains(i));
    assert(queue.order.index_of(i)==i);
    if first(input,blocked,state,viable,same,i) {
        assert(!(exists|j:int| #[trigger] queue.order.contains(j) && queue.order.index_of(j)<queue.order.index_of(i)
            && queue.blocked.contains(j) && vi(j) && eq(j,i))) by {
            if exists|j:int| #[trigger] queue.order.contains(j) && queue.order.index_of(j)<queue.order.index_of(i)
                && queue.blocked.contains(j) && vi(j) && eq(j,i) {
                let j=choose|j:int| #[trigger] queue.order.contains(j) && queue.order.index_of(j)<queue.order.index_of(i)
                    && queue.blocked.contains(j) && vi(j) && eq(j,i);
                assert(0<=j<i && blocked.contains(j as usize));
                assert(!equivalent(same,input[j],input[i]));
            }
        }
    }
    if abstract_admission::first_viable(queue,i,vi,eq) {
        assert forall|j:int| 0<=j<i && crate::pending_spec::qualifies(input,blocked,state,viable,j)
            implies !equivalent(same,#[trigger] input[j],input[i]) by {
            assert(queue.order.contains(j));
            assert(queue.blocked.contains(j));
            assert(queue.order.index_of(j)==j);
        }
    }
}

proof fn abstract_pending<G,V,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool>(
    input:Seq<Candidate<G>>,blocked:Set<usize>,state:&V,viable:U,same:Q,kept:Seq<Candidate<G>>,pending:Seq<G>,
)
    requires retention(input,blocked,state,viable,same,kept),pending==groups_of(kept),
    ensures abstract_admission::retains_pending(queue_view(input,blocked),viable_view(input,state,viable),same_view(input,same),pending),
{
    let queue=queue_view(input,blocked);
    let vi=viable_view(input,state,viable);
    let eq=same_view(input,same);
    let positions=choose|positions:Seq<int>| #[trigger] retained(input,blocked,state,viable,same,input.len() as int,kept,positions);
    queue_indices(queue.order,input.len());
    assert forall|i:int| #[trigger] positions.contains(i) <==> abstract_admission::first_viable(queue,i,vi,eq) by {
        if 0<=i<input.len() { first_matches(input,blocked,state,viable,same,i); }
        else if positions.contains(i) {
            let j=choose|j:int| 0<=j<positions.len() && #[trigger] positions[j]==i;
            assert(0<=positions[j]<input.len());
        }
    }
    assert forall|i:int,j:int| 0<=i<j<positions.len() implies
        queue.order.index_of(#[trigger] positions[i])<queue.order.index_of(#[trigger] positions[j]) by {
        assert(positions[i]<positions[j]);
    }
    assert(positions.no_duplicates());
    assert(queue.deliveries.dom()==queue.order.to_set());
    assert(pending =~= positions.map_values(|i:int| queue.deliveries[i]));
}

proof fn abstract_publication<S,V,G,T,H:'static,F,P:'static,W,C,D,SlotKey,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool,View:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>,Accepted:Fn(H)->bool,Rank:Fn(H)->Option<usize>,Slot:Fn(usize)->(SlotKey,bool)>(
    initial:S,prepared:Prepared<G,Plan<H,P>>,published:V,done:V,read:&DeliveryRead<M,K>,meaning:Model<S,V,H,G,P,W,C,D>,
    history:History<H,usize>,origin:CompilerView<H,F,P,View,Accepted,Rank,Slot>,mask:Seq<bool>,viable:U,same:Q,
)
    requires observed_compilation(history,(meaning.before)(initial),prepared,read,origin),
        projected_order(prepared.candidates@,origin.verified,origin.waiting,origin.view,origin.accepted,read.members,read.key,
            origin.ranked,origin.rank,origin.slot,mask,prepared.plan.projection,Ok(prepared.plan.ordered)),
        committed_document(initial,prepared,published,done,meaning,viable,same),
    ensures abstract_admission::publishes(
        abstract_before((meaning.before)(initial)),abstract_after((meaning.observe)(done)),
        selection_view(origin.verified,mask,origin.view,meaning.proof_records),prepared.plan.projection.members@.to_set(),
        prepared.plan.ordered@,history.parents,meaning.import),
{
    let before=(meaning.before)(initial);
    let after=(meaning.observe)(done);
    let records=meaning.proof_records;
    let verified=origin.verified;
    let plan=prepared.plan;
    let chosen=selection_view(verified,mask,origin.view,records);
    compiled_plan_has_transaction_inputs(history,origin.fragments,before,prepared,verified,origin.waiting,
        origin.view,origin.head,origin.accepted,read.members,read.key,origin.level_limit,origin.ranked,origin.rank,origin.slot);
    assert(certified_plan(before,history,prepared));
    abstract_greatest(verified,mask,origin.view,origin.accepted,before.accepted,records);
    exact_members(prepared.candidates@,verified,mask,origin.fragments,origin.head,origin.view,read.members,read.key,verified.len() as int);
    assert forall|h:H| #[trigger] plan.projection.members@.to_set().contains(h)
        <==> abstract_admission::supplies(chosen.verified,chosen.chosen,h) by {
        abstract_suppliers(verified,mask,origin.view,records,chosen.chosen,h);
        assert(plan.projection.members@.contains(h)==plan.projection.members@.to_set().contains(h));
        if plan.projection.members@.contains(h) {
            let i=choose|i:int| 0<=i<verified.len() && !mask[i] && #[trigger] model(origin.view,&verified[i]).members@.contains(h);
            assert(chosen.chosen.contains(i as usize));
            assert(raw_supplies(verified,origin.view,chosen.chosen,h));
        }
        if raw_supplies(verified,origin.view,chosen.chosen,h) {
            let i=choose|i:usize| i<verified.len() && chosen.chosen.contains(i)
                && #[trigger] model(origin.view,&verified[i as int]).members@.contains(h);
            assert(!mask[i as int]);
        }
    }
    assert(abstract_admission::legal_schedule(plan.ordered@,plan.projection.members@.to_set(),before.applied,history.parents)) by {
        assert(safe_schedule(history,plan.projection.members@,before.applied,plan.ordered@));
        assert forall|i:int| 0<=i<plan.ordered@.len() implies
            (history.parents)(#[trigger] plan.ordered@[i]).subset_of(before.applied.union(plan.ordered@.take(i).to_set())) by {
            assert forall|h:H| #[trigger] (history.parents)(plan.ordered@[i]).contains(h) implies
                before.applied.union(plan.ordered@.take(i).to_set()).contains(h) by {
                if !before.applied.contains(h) {
                    assert(plan.ordered@.take(i).contains(h));
                    assert(plan.ordered@.take(i).to_set().contains(h));
                }
            }
        }
    }
    assert forall|w:W| #[trigger] after.evidence.contains(w) implies before.evidence.contains(w)
        || (exists|id:usize| #[trigger] chosen.chosen.contains(id) && chosen.verified[id].evidence.contains(w)) by {
        if !before.evidence.contains(w) {
            resulting_witness_came_from_selection(verified,mask,verified.len() as int,records,w);
            let id=choose|id:usize| id<verified.len() && !mask[id as int] && #[trigger] records(verified[id as int].1.1).contains(w);
            assert(chosen.chosen.contains(id));
            assert(chosen.verified.dom().contains(id));
            assert(chosen.verified[id].evidence.contains(w));
        }
    }
}

pub(crate) open spec fn abstract_commit<S,V,G,T,H:'static,F,P:'static,W,C,D,SlotKey,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool,View:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>,Accepted:Fn(H)->bool,Rank:Fn(H)->Option<usize>,Slot:Fn(usize)->(SlotKey,bool)>(
    initial:S,p:Prepared<G,Plan<H,P>>,published:V,done:V,read:&DeliveryRead<M,K>,meaning:Model<S,V,H,G,P,W,C,D>,
    history:History<H,usize>,source:CompilerView<H,F,P,View,Accepted,Rank,Slot>,mask:Seq<bool>,viable:U,same:Q,
)->bool {
    &&& projected_order(p.candidates@,source.verified,source.waiting,source.view,source.accepted,read.members,read.key,
        source.ranked,source.rank,source.slot,mask,p.plan.projection,Ok(p.plan.ordered))
    &&& abstract_admission::publishes(abstract_before((meaning.before)(initial)),abstract_after((meaning.observe)(done)),
        selection_view(source.verified,mask,source.view,meaning.proof_records),p.plan.projection.members@.to_set(),p.plan.ordered@,
        history.parents,meaning.import)
    &&& abstract_admission::retains_pending(queue_view(p.candidates@,p.plan.projection.remaining@),
        viable_view(p.candidates@,&published,viable),same_view(p.candidates@,same),(meaning.observe)(done).pending)
}

// The conclusion names *only* the abstract predicates for admission effects;
// the concrete witnesses here explain what those abstract inputs denote.
pub(crate) open spec fn abstracts_transaction<S,V,G,T,H:'static,F,P:'static,E,W,C,D,SlotKey,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&T>)->(S,Result<(),E>),O:FnOnce(S)->V,Effect:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V,View:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>,Accepted:Fn(H)->bool,Rank:Fn(H)->Option<usize>,Slot:Fn(usize)->(SlotKey,bool)>(
    initial:S,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<Effect,U,Q,I>>,out:Outcome<S,V,E>,meaning:Model<S,V,H,G,P,W,C,D>,
    planned_history:History<H,usize>,compiled:spec_fn(Prepared<G,Plan<H,P>>)->CompilerView<H,F,P,View,Accepted,Rank,Slot>,
)->bool {
    let before=(meaning.before)(initial);
    match out {
        Outcome::Aborted(state,_)=>abstract_admission::aborts(before.identity,(meaning.before)(state).identity),
        Outcome::Committed(done)=>exists|tmp:S,p:Prepared<G,Plan<H,P>>,applied:S,signing:V,published:V,kept:Seq<Candidate<G>>,groups:Vec<G>,mask:Seq<bool>|
            #[trigger] opened_after_apply(initial,tmp,p,read,effects,applied,signing)
            && #[trigger] pruned_plan(signing,p,&effects.signing,published,kept,groups)
            && #[trigger] abstract_commit(initial,p,published,done,read,meaning,planned_history,compiled(p),mask,effects.signing.viable,effects.signing.same_delivery),
    }
}

/// The production transaction refines `spec.rs` under the existing leaf
/// contracts. No additional assumptions about planning or effects are used.
pub(crate) proof fn refines_abstract_spec<S,V,G,T,H:'static,F,P:'static,E,W,C,D,SlotKey,M:for<'a> Fn(&'a Candidate<G>)->&'a[T],K:Fn(&T)->H,B:FnOnce(S)->(S,Result<Prepared<G,Plan<H,P>>,E>),A:FnOnce(S,Vec<&T>)->(S,Result<(),E>),O:FnOnce(S)->V,Effect:for<'a> Fn(V,Publish<&'a P,&'a[H]>)->V,U:Fn(&V,&Candidate<G>)->bool,Q:Fn(&Candidate<G>,&Candidate<G>)->bool,I:FnOnce(V,Vec<G>)->V,View:for<'a> Fn(&'a Verified<F,P>)->FragmentView<'a,H>,Accepted:Fn(H)->bool,Rank:Fn(H)->Option<usize>,Slot:Fn(usize)->(SlotKey,bool)>(
    initial:S,read:&DeliveryRead<M,K>,effects:&Effects<B,A,O,SigningEffects<Effect,U,Q,I>>,out:Outcome<S,V,E>,meaning:Model<S,V,H,G,P,W,C,D>,
    planned_history:History<H,usize>,compiled:spec_fn(Prepared<G,Plan<H,P>>)->CompilerView<H,F,P,View,Accepted,Rank,Slot>,
)
    requires transaction(initial,read,effects,out),document_laws(read,effects,meaning,planned_history),
        forall|tmp:S,p:Prepared<G,Plan<H,P>>| #[trigger] was_prepared(initial,effects.prepare,tmp,p) ==>
            observed_compilation(planned_history,(meaning.before)(initial),p,read,compiled(p)),
    ensures abstracts_transaction(initial,read,effects,out,meaning,planned_history,compiled),
{
    linked_document_contract(initial,read,effects,out,meaning,planned_history,compiled);
    match out {
        Outcome::Aborted(state,_)=>{
            assert((meaning.before)(state)==(meaning.before)(initial));
        }
        Outcome::Committed(done)=>{
            let (tmp,p,applied,signing,published,kept,groups)=choose|tmp:S,p:Prepared<G,Plan<H,P>>,applied:S,signing:V,published:V,kept:Seq<Candidate<G>>,groups:Vec<G>|
                #[trigger] opened_after_apply(initial,tmp,p,read,effects,applied,signing)
                && #[trigger] pruned_plan(signing,p,&effects.signing,published,kept,groups)
                && committed_document(initial,p,published,done,meaning,effects.signing.viable,effects.signing.same_delivery);
            let source=compiled(p);
            assert(was_prepared(initial,effects.prepare,tmp,p));
            assert(observed_compilation(planned_history,(meaning.before)(initial),p,read,source));
            reveal(from_selection);
            let mask=choose|mask:Seq<bool>| #[trigger] projected_order(p.candidates@,source.verified,source.waiting,source.view,source.accepted,read.members,read.key,
                source.ranked,source.rank,source.slot,mask,p.plan.projection,Ok(p.plan.ordered));
            abstract_publication(initial,p,published,done,read,meaning,planned_history,source,mask,effects.signing.viable,effects.signing.same_delivery);
            let pending=(meaning.observe)(done).pending;
            let actual=choose|retained_deliveries:Seq<Candidate<G>>|
                #[trigger] retention(p.candidates@,p.plan.projection.remaining@,&published,effects.signing.viable,effects.signing.same_delivery,retained_deliveries)
                && pending==groups_of(retained_deliveries);
            abstract_pending(p.candidates@,p.plan.projection.remaining@,&published,effects.signing.viable,effects.signing.same_delivery,actual,pending);
            assert(abstract_commit(initial,p,published,done,read,meaning,planned_history,compiled(p),mask,effects.signing.viable,effects.signing.same_delivery));
        }
    }
}

} // verus!

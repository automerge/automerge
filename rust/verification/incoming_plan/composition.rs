use crate::admission::FragmentView;
use crate::admission_proof::selection_preserves_causal_closure;
use crate::admission_spec::{ancestry, assembly, canonical, selected_member, Fragments};
use crate::history_spec::{causal_set, History};
use crate::lookup_spec::model;
use crate::prepare_plan::{Plan, Verified};
use crate::prepare_spec::{accepted_members, from_selection, projected_order, row};
use crate::projection_spec::{greatest, raw_closed, raw_supplies};
use crate::schedule_spec::{ranked_order, schedule_result};
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties,vstd::set::group_set_lemmas};

pub(crate) open spec fn occurrence_alignment<'a,'m,C:'a,M:'m,F,P,H:'static,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,fragments:Fragments<H>,head:spec_fn(F)->H,view:V,contents:R,hash:K,
)->bool {
    forall|i:int| 0<=i<verified.len() ==> {
        let v=#[trigger] verified[i];
        let fragment=model(view,&verified[i]);
        let h=head(v.1.0);
        // Matching delivered membership was checked at group evaluation.
        &&& v.0<candidates.len()
        &&& row(model(contents,&candidates[v.0 as int])@,hash).to_set()==fragment.members@.to_set()
        &&& fragment.members@.to_set()==(fragments.members)(h)
        &&& fragment.boundary@.to_set()==(fragments.boundary)(h)
        &&& canonical(fragments,h)
    }
}

pub(crate) open spec fn kept(mask:Seq<bool>)->Set<usize> {
    Set::range(0usize,mask.len() as usize).filter(|i:usize| !mask[i as int])
}

pub(crate) open spec fn chosen_heads<F,P,H>(verified:Seq<Verified<F,P>>,mask:Seq<bool>,head:spec_fn(F)->H)->Set<H> {
    kept(mask).map(|i:usize| head(verified[i as int].1.0))
}

pub(crate) proof fn exact_members<'a,'m,C:'a,M:'m,F,P,H:'static,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>>(
    candidates:Seq<C>,verified:Seq<Verified<F,P>>,mask:Seq<bool>,fragments:Fragments<H>,head:spec_fn(F)->H,view:V,contents:R,hash:K,end:int,
)
    requires 0<=end<=verified.len()==mask.len(),occurrence_alignment(candidates,verified,fragments,head,view,contents,hash),
    ensures forall|h:H| #[trigger] accepted_members(candidates,verified,mask,contents,hash,end).contains(h) <==>
        exists|i:int| 0<=i<end && !mask[i] && #[trigger] model(view,&verified[i]).members@.contains(h),
    decreases end,
{
    reveal(accepted_members);
    if end>0 {
        exact_members(candidates,verified,mask,fragments,head,view,contents,hash,end-1);
        let original=model(view,&verified[end-1]).members@;
        let added=row(model(contents,&candidates[verified[end-1].0 as int])@,hash);
        let before=accepted_members(candidates,verified,mask,contents,hash,end-1);
        let after=accepted_members(candidates,verified,mask,contents,hash,end);
        assert(added.to_set()==original.to_set());
        assert forall|h:H| #[trigger] after.contains(h) <==> exists|i:int| 0<=i<end && !mask[i]
            && #[trigger] model(view,&verified[i]).members@.contains(h) by {
            assert(added.to_set().contains(h)==added.contains(h));
            assert(original.to_set().contains(h)==original.contains(h));
            assert(added.to_set().contains(h)==original.to_set().contains(h));
            assert(after.contains(h)==(before.contains(h) || (!mask[end-1] && original.contains(h))));
        }
    }
}

// Translate an occurrence-based selection to the head-indexed closure
// theorem. Two independently proved occurrences with the same head can
// both be selected, with distinct proof payloads and the same descriptor.
pub(crate) proof fn selected_history_is_closed<'a,'m,C:'a,M:'m,F,P,H:'static,A,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>,E:Fn(H)->bool>(
    history:History<H,A>,fragments:Fragments<H>,before:Set<H>,candidates:Seq<C>,verified:Seq<Verified<F,P>>,mask:Seq<bool>,
    view:V,head:spec_fn(F)->H,accepted:E,contents:R,hash:K,limit:nat,selected:Seq<H>,
)
    requires greatest(verified,view,accepted,mask),verified.len()<=usize::MAX,
        occurrence_alignment(candidates,verified,fragments,head,view,contents,hash),
        ancestry(history,fragments),before.subset_of(history.nodes),
        forall|h:H| #[trigger] before.contains(h) ==> (fragments.past)(h).subset_of(before),
        forall|h:H| #[trigger] model(accepted,h)==before.contains(h),
        forall|i:int| 0<=i<verified.len() ==> history.nodes.contains(head((#[trigger] verified[i]).1.0))
            && (fragments.level)(head(verified[i].1.0))<=limit,
        selected==accepted_members(candidates,verified,mask,contents,hash,verified.len() as int),
    ensures causal_set(history,before.union(selected.to_set())),
{
    let included=kept(mask);
    let to_head=|i:usize| head(verified[i as int].1.0);
    let heads=chosen_heads(verified,mask,head);
    let after=before.union(selected.to_set());
    exact_members(candidates,verified,mask,fragments,head,view,contents,hash,verified.len() as int);
    assert(raw_closed(verified,view,accepted,included));
    assert forall|h:H| #[trigger] heads.contains(h) <==> exists|i:usize| #[trigger] included.contains(i) && to_head(i)==h by {
        included.lemma_map_contains(to_head,h);
    }
    assert forall|h:H| #[trigger] heads.contains(h) implies history.nodes.contains(h)
        && (fragments.level)(h)<=limit && canonical(fragments,h) by {
        let i=choose|i:usize| #[trigger] included.contains(i) && to_head(i)==h;
        assert(i<verified.len());
        assert(head(verified[i as int].1.0)==h);
    }
    assert forall|h:H,b:H| #[trigger] heads.contains(h) && #[trigger] (fragments.boundary)(h).contains(b)
        implies before.contains(b) || selected_member(fragments,heads,b) by {
        let i=choose|i:usize| #[trigger] included.contains(i) && to_head(i)==h;
        assert(i<verified.len());
        let body=model(view,&verified[i as int]);
        assert(body.boundary@.to_set()==(fragments.boundary)(h));
        assert(body.boundary@.contains(b));
        if !before.contains(b) {
            assert(raw_supplies(verified,view,included,b));
            let j=choose|j:usize| j<verified.len() && included.contains(j) && #[trigger] model(view,&verified[j as int]).members@.contains(b);
            assert(heads.contains(to_head(j)));
            assert((fragments.members)(to_head(j)).contains(b));
        }
    }
    assert forall|h:H| #[trigger] selected.contains(h) <==> selected_member(fragments,heads,h) by {
        if selected.contains(h) {
            let i=choose|i:int| 0<=i<verified.len() && !mask[i] && #[trigger] model(view,&verified[i]).members@.contains(h);
            assert(included.contains(i as usize));
            assert(heads.contains(to_head(i as usize)));
            assert((fragments.members)(to_head(i as usize)).contains(h));
        }
        if selected_member(fragments,heads,h) {
            let t=choose|t:H| #[trigger] heads.contains(t) && (fragments.members)(t).contains(h);
            let j=choose|j:usize| #[trigger] included.contains(j) && to_head(j)==t;
            assert(j<verified.len());
            assert(!mask[j as int]);
            assert(model(view,&verified[j as int]).members@.contains(h));
        }
    }
    assert forall|h:H| #[trigger] after.contains(h) <==> before.contains(h) || selected_member(fragments,heads,h) by {
        assert(selected.to_set().contains(h)==selected.contains(h));
    }
    assert(assembly(history,fragments,heads,before,after,limit));
    selection_preserves_causal_closure(history,fragments,heads,before,after,limit);
}

pub(crate) open spec fn rank_truth<H,R:Fn(H)->Option<usize>>(members:Seq<H>,applied:Set<H>,ranked:Seq<H>,rank:R)->bool {
    &&& ranked.no_duplicates()
    &&& forall|i:int| 0<=i<members.len() ==> match #[trigger] model(rank,members[i]) {
        None=>applied.contains(members[i]),
        Some(r)=>!applied.contains(members[i]) && r<ranked.len() && ranked[r as int]==members[i],
    }
}

pub(crate) open spec fn rank_dependencies<H,A>(history:History<H,A>,applied:Set<H>,ranked:Seq<H>)->bool {
    forall|i:int,p:H| 0<=i<ranked.len() && #[trigger] (history.parents)(ranked[i]).contains(p) && !applied.contains(p)
        ==> exists|j:int| 0<=j<i && #[trigger] ranked[j]==p
}

pub(crate) open spec fn safe_schedule<H,A>(history:History<H,A>,members:Seq<H>,applied:Set<H>,order:Seq<H>)->bool {
    &&& order.no_duplicates()
    &&& order.to_set() =~= members.to_set().difference(applied)
    &&& forall|i:int,p:H| 0<=i<order.len() && #[trigger] (history.parents)(order[i]).contains(p)
        ==> applied.contains(p) || order.take(i).contains(p)
}

proof fn ordered_effect<H,A,Key,R:Fn(H)->Option<usize>,S:Fn(usize)->(Key,bool)>(
    history:History<H,A>,before:Set<H>,applied:Set<H>,members:Seq<H>,ranked:Seq<H>,rank:R,slot:S,order:Vec<H>,
)
    requires before.subset_of(applied),causal_set(history,before.union(members.to_set())),
        rank_truth(members,applied,ranked,rank),rank_dependencies(history,applied,ranked),
        schedule_result(members,ranked,rank,slot,Ok(order)),
    ensures safe_schedule(history,members,applied,order@),
{
    reveal(schedule_result);
    let ranks=choose|rs:Seq<usize>| #[trigger] ranked_order(members,ranked,rank,slot,rs)
        && order@.len()==rs.len()
        && forall|i:int| 0<=i<rs.len() ==> #[trigger] order@[i]==ranked[rs[i] as int];
    assert forall|i:int,j:int| 0<=i<j<order@.len() implies order@[i]!=order@[j] by {
        assert(ranks[i]<ranks[j]);
        assert(ranked[ranks[i] as int]!=ranked[ranks[j] as int]);
    }
    assert forall|h:H| #[trigger] order@.contains(h) <==> members.contains(h) && !applied.contains(h) by {
        if order@.contains(h) {
            let i=choose|i:int| 0<=i<order@.len() && #[trigger] order@[i]==h;
            let r=ranks[i];
            assert(ranks.contains(r));
            let j=choose|j:int| 0<=j<members.len() && #[trigger] model(rank,members[j])==Some(r);
            assert(members[j]==h);
            assert(members.contains(h));
        }
        if members.contains(h) && !applied.contains(h) {
            let j=choose|j:int| 0<=j<members.len() && #[trigger] members[j]==h;
            let r=model(rank,h).unwrap();
            assert(ranks.contains(r));
            let i=choose|i:int| 0<=i<ranks.len() && #[trigger] ranks[i]==r;
            assert(order@[i]==h);
        }
    }
    assert(order@.to_set() =~= members.to_set().difference(applied));
    let after=before.union(members.to_set());
    assert forall|i:int,p:H| 0<=i<order@.len() && #[trigger] (history.parents)(order@[i]).contains(p)
        implies applied.contains(p) || order@.take(i).contains(p) by {
        if !applied.contains(p) {
            assert(order@.contains(order@[i]));
            assert(after.contains(order@[i]));
            assert(after.contains(p));
            assert(members.contains(p));
            assert(order@.contains(p));
            let j=choose|j:int| 0<=j<order@.len() && #[trigger] order@[j]==p;
            let earlier=choose|r:int| 0<=r<(ranks[i] as int) && #[trigger] ranked[r]==p;
            assert(ranks[j]==earlier);
            assert(j<i);
            assert(order@.take(i)[j]==p);
        }
    }
}

/// Compose successful production plan assembly with the group result and
/// graph observations. No dependency-closed selection or legal apply order
/// is assumed: both follow from the selector, the matched descriptions and
/// the exact projection of the selected occurrence mask.
pub(crate) proof fn prepared_plan_is_causally_exact<'a,'m,C:'a,M:'m,F,P,H:'static,A,Key,R:Fn(&'a C)->&'m[M],K:Fn(&'m M)->H,V:for<'v> Fn(&'v Verified<F,P>)->FragmentView<'v,H>,E:Fn(H)->bool,T:Fn(H)->Option<usize>,U:Fn(usize)->(Key,bool)>(
    history:History<H,A>,fragments:Fragments<H>,before:Set<H>,applied:Set<H>,candidates:Seq<C>,verified:Seq<Verified<F,P>>,waiting:Seq<usize>,
    view:V,head:spec_fn(F)->H,accepted:E,contents:R,hash:K,limit:nat,ranked:Seq<H>,rank:T,slot:U,plan:Plan<H,P>,
)
    requires from_selection(candidates,verified,waiting,view,accepted,contents,hash,ranked,rank,slot,Ok(plan)),verified.len()<=usize::MAX,
        occurrence_alignment(candidates,verified,fragments,head,view,contents,hash),
        ancestry(history,fragments),before.subset_of(history.nodes),before.subset_of(applied),
        forall|h:H| #[trigger] before.contains(h) ==> (fragments.past)(h).subset_of(before),
        forall|h:H| #[trigger] model(accepted,h)==before.contains(h),
        forall|i:int| 0<=i<verified.len() ==> history.nodes.contains(head((#[trigger] verified[i]).1.0))
            && (fragments.level)(head(verified[i].1.0))<=limit,
        rank_truth(plan.projection.members@,applied,ranked,rank),rank_dependencies(history,applied,ranked),
    ensures causal_set(history,before.union(plan.projection.members@.to_set())),
        safe_schedule(history,plan.projection.members@,applied,plan.ordered@),
{
    reveal(from_selection);
    let mask=choose|mask:Seq<bool>| #[trigger] projected_order(candidates,verified,waiting,view,accepted,contents,hash,
        ranked,rank,slot,mask,plan.projection,Ok(plan.ordered));
    selected_history_is_closed(history,fragments,before,candidates,verified,mask,view,head,accepted,contents,hash,limit,plan.projection.members@);
    ordered_effect(history,before,applied,plan.projection.members@,ranked,rank,slot,plan.ordered);
}

} // verus!

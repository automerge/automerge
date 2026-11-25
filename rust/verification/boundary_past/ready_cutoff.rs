use crate::actor_chains::{Candidate, Dependency, Resolution};
use crate::admission_spec::canonical;
use crate::boundary_memo::Node;
use crate::canonical_composition::{descriptor, hard_descriptor_is_canonical, ranked_scope};
use crate::chain_spec::{classified, well_formed};
use crate::cone_spec::{
    aligned_cones, ready_cone_supports_actor_clocks, restrict, slot_projection,
};
use crate::cutoff_spec::{applied_part, cutoff, cutoff_is_causal_past, positions};
use crate::gate_spec::cone_result;
use crate::history_spec::{actor_chains, causal_set, maxima, History};
use crate::live_spec::{is_live, live_node_has_live_parents, row_projection};
use crate::memo_spec::{actor_seq, covers, dependencies, past_prefix, readable, supports_all};
use crate::overlay_bridge::{from_seeds, same_overlay};
use crate::spec::SummaryView;
use crate::walk_spec::{contains_walk, expands, keeps, reached, walk_result, Graph};
use vstd::prelude::*;

verus! {

broadcast use {vstd::set::group_set_lemmas, vstd::seq_lib::group_seq_properties};

pub(crate) open spec fn ready_nodes<K,A>(history:History<K,A>,full:Graph<K>,head:K)->Set<K> {
    history.nodes.filter(|k:K| reached(full,seq![head],k))
}

// Only primitive graph observations are needed here: the memo observes
// actual applied membership, and candidates report the same direct parents
// and actor/sequence metadata as the full snapshot.
pub(crate) open spec fn same_snapshot<'a,K:'a,A,F:Fn(K)->Node<'a,A,K>>(
    history:History<K,A>,live:Set<K>,applied:Set<K>,read:&F,full:Graph<K>,
)->bool {
    &&& forall|k:K| #[trigger] (full.visit)(k) is KeepAndExpand
    &&& forall|k:K| #[trigger] live.contains(k) ==> {
        &&& readable(read,k)
        &&& if applied.contains(k) { actor_seq(read,k).is_none() }
            else {actor_seq(read,k).is_some() && actor_seq(read,k).unwrap().0==(history.actor)(k)
                && actor_seq(read,k).unwrap().1==(history.sequence)(k) && dependencies(read,k)==(full.parents)(k)}
    }
}

proof fn boundary_roots_in_scope<K,A>(history:History<K,A>,full:Graph<K>,bounds:Graph<K>,head:K,start:Seq<K>,roots:Seq<K>)
    requires causal_set(history,ready_nodes(history,full,head)),ready_nodes(history,full,head).contains(head),
        start.to_set()==(history.parents)(head),walk_result(bounds,start,roots),
        forall|k:K| #[trigger] ready_nodes(history,full,head).contains(k) ==>
            expands((full.visit)(k)) && (full.parents)(k).to_set()==(history.parents)(k),
        forall|k:K| #[trigger] ready_nodes(history,full,head).contains(k) && expands((bounds.visit)(k)) ==>
            (bounds.parents)(k).to_set()==(history.parents)(k),
    ensures roots.to_set().subset_of(ready_nodes(history,full,head)),
{
    let available=ready_nodes(history,full,head);
    assert(contains_walk(bounds,start,available));
    assert forall|k:K| #[trigger] roots.contains(k) implies available.contains(k) by {};
}

// The applied rank is a graph observation. Candidate ranks are the
// already-computed topological positions following all applied ranks.
pub(crate) open spec fn snapshot_ranks<K,A>(history:History<K,A>,applied:Set<K>,hashes:Seq<K>,offset:nat,rank:spec_fn(K)->nat)->bool {
    &&& forall|h:K| #[trigger] applied.contains(h) ==> rank(h)<offset
        && forall|p:K| #[trigger] (history.parents)(h).contains(p) ==> rank(p)<rank(h)
    &&& forall|i:int| 0<=i<hashes.len() ==> rank(#[trigger] hashes[i])==offset+i
}

proof fn ranks_follow_original_edges<K,A>(history:History<K,A>,applied:Set<K>,deps:spec_fn(K)->Seq<K>,hashes:Seq<K>,rows:Seq<Candidate<A>>,outcomes:Seq<Resolution>,offset:nat,rank:spec_fn(K)->nat,h:K,p:K)
    requires row_projection(history,applied,deps,hashes,rows),well_formed(rows),is_live(applied,hashes,outcomes,h),
        snapshot_ranks(history,applied,hashes,offset,rank),(history.parents)(h).contains(p),
    ensures rank(p)<rank(h),
{
    if !applied.contains(h) {
        let i=choose|i:int| 0<=i<hashes.len() && #[trigger] hashes[i]==h && outcomes[i] is Linked;
        let j=choose|j:int| 0<=j<deps(h).len() && #[trigger] deps(h)[j]==p;
        assert(rank(hashes[i])==offset+i);
        match rows[i].deps@[j] {
            Dependency::Applied{..}=>assert(applied.contains(p)),
            Dependency::Candidate(q)=>{
                assert(q<i);
                assert(hashes[q as int]==p);
                assert(rank(hashes[q as int])==offset+q);
            },
        }
    }
}

/// Start from the live, clean cone produced by group eligibility, not an
/// assumed global actor invariant on the batch. Compose the actual level
/// walk, the memo union and the two clock queries on that same cone.
pub(crate) proof fn ready_cutoff_is_exact<'a,'b,K:'a,A:'b,S:Fn(K)->((A,u64),bool),F:Fn(K)->Node<'a,A,K>,P:Fn(K)->Option<(usize,u32)>,C:Fn(usize)->Option<u32>,Q:Fn(K)->(&'b A,u64)>(
    history:History<K,A>,applied:Set<K>,deps:spec_fn(K)->Seq<K>,hashes:Seq<K>,rows:Seq<Candidate<A>>,outcomes:Seq<Resolution>,
    full:Graph<K>,cut:Graph<K>,bounds:Graph<K>,head:K,slot:S,start:Seq<K>,boundary:Seq<K>,read:&F,summary:SummaryView<A,K>,
    position:P,clock:C,candidate:Q,applied_clock:Map<A,nat>,
)
    requires row_projection(history,applied,deps,hashes,rows),rows.len()==outcomes.len(),well_formed(rows),classified(rows,outcomes),
        causal_set(history,applied),actor_chains(restrict(history,applied)),is_live(applied,hashes,outcomes,head),
        aligned_cones(history,history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k)),applied,full,cut),
        slot_projection(history,history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k)),applied,slot),
        cone_result(cut,seq![head],slot,None),
        same_snapshot(history,history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k)),applied,read,full),
        start.to_set()==(history.parents)(head),walk_result(bounds,start,boundary),
        forall|k:K| #[trigger] ready_nodes(history,full,head).contains(k) && expands((bounds.visit)(k)) ==>
            (bounds.parents)(k).to_set()==(history.parents)(k),
        covers(read,|k:K| past_prefix(read,boundary,boundary.len() as int,k),summary),
        positions(history,read,position,clock,candidate,applied_clock),
        maxima(history,ready_nodes(history,full,head).filter(|k:K| from_seeds(full,summary,k)),
            applied_part(ready_nodes(history,full,head),read),applied_clock),
    ensures causal_set(history,ready_nodes(history,full,head)),ready_nodes(history,full,head).contains(head),
        forall|k:K| #[trigger] ready_nodes(history,full,head).contains(k) ==>
        (cutoff(summary,k,position,clock,candidate) <==> reached(full,boundary,k)),
{
    let live=history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k));
    let restricted=ready_cone_supports_actor_clocks(history,applied,deps,hashes,rows,outcomes,full,cut,head,slot);
    let scope=restricted.nodes;
    assert(scope==ready_nodes(history,full,head));
    assert(scope.subset_of(live)) by {
        crate::cone_spec::closed_live(history,applied,deps,hashes,rows,outcomes);
        live_node_has_live_parents(history,applied,deps,hashes,rows,outcomes,head);
        assert(live.contains(head));
        assert(contains_walk(full,seq![head],live));
        assert forall|k:K| #[trigger] scope.contains(k) implies live.contains(k) by {};
    }
    assert(reached(full,seq![head],head)) by {
        assert(seq![head].contains(head));
        assert forall|s:Set<K>| #[trigger] contains_walk(full,seq![head],s) implies s.contains(head) by {};
    }
    live_node_has_live_parents(history,applied,deps,hashes,rows,outcomes,head);
    assert(scope.contains(head));
    boundary_roots_in_scope(history,full,bounds,head,start,boundary);
    assert(supports_all(scope,read,boundary)) by {
        assert forall|k:K| #[trigger] scope.contains(k) implies readable(read,k) && dependencies(read,k).to_set().subset_of(scope) by {
            assert(live.contains(k));
            if !applied.contains(k) { assert((history.parents)(k).subset_of(scope)); }
        }
    }
    assert(same_overlay(scope,read,full)) by {
        assert forall|k:K| #[trigger] scope.contains(k) implies (full.parents)(k).to_set().subset_of(scope)
            && if actor_seq(read,k).is_some() {(full.parents)(k)==dependencies(read,k)}
            else {forall|p:K| #[trigger] (full.parents)(k).contains(p) ==> actor_seq(read,p).is_none()} by {
            assert(live.contains(k));
            assert((history.parents)(k).subset_of(scope));
            if applied.contains(k) {
                assert((history.parents)(k).subset_of(applied));
                assert forall|p:K| #[trigger] (full.parents)(k).contains(p) implies actor_seq(read,p).is_none() by {
                    assert(scope.contains(p));
                    assert(live.contains(p));
                }
            }
        }
    }
    cutoff_is_causal_past(restricted,read,full,boundary,summary,position,clock,candidate,applied_clock);
}

// Expose the composition used by planning: an eligible hard descriptor is
// canonical, with the verified cutoff interpretation discharged on its own
// clean head cone. The member-level premise is the result of intrinsic
// validation plus the checked delivered-set comparison.
pub(crate) proof fn ready_hard_descriptor_is_canonical<'a,'b,K:'a,A:'b,S:Fn(K)->((A,u64),bool),F:Fn(K)->Node<'a,A,K>,P:Fn(K)->Option<(usize,u32)>,C:Fn(usize)->Option<u32>,Q:Fn(K)->(&'b A,u64)>(
    history:History<K,A>,applied:Set<K>,deps:spec_fn(K)->Seq<K>,hashes:Seq<K>,rows:Seq<Candidate<A>>,outcomes:Seq<Resolution>,
    full:Graph<K>,cut:Graph<K>,bounds:Graph<K>,head:K,slot:S,start:Seq<K>,boundary:Seq<K>,read:&F,summary:SummaryView<A,K>,
    position:P,clock:C,candidate:Q,applied_clock:Map<A,nat>,
    level:spec_fn(K)->nat,offset:nat,rank:spec_fn(K)->nat,body:Graph<K>,members:Seq<K>,
)
    requires row_projection(history,applied,deps,hashes,rows),rows.len()==outcomes.len(),well_formed(rows),classified(rows,outcomes),
        causal_set(history,applied),actor_chains(restrict(history,applied)),is_live(applied,hashes,outcomes,head),
        aligned_cones(history,history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k)),applied,full,cut),
        slot_projection(history,history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k)),applied,slot),
        cone_result(cut,seq![head],slot,None),
        same_snapshot(history,history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k)),applied,read,full),
        start.to_set()==(history.parents)(head),walk_result(bounds,start,boundary),
        forall|k:K| #[trigger] ready_nodes(history,full,head).contains(k) && expands((bounds.visit)(k)) ==>
            (bounds.parents)(k).to_set()==(history.parents)(k),
        covers(read,|k:K| past_prefix(read,boundary,boundary.len() as int,k),summary),
        positions(history,read,position,clock,candidate,applied_clock),
        maxima(history,ready_nodes(history,full,head).filter(|k:K| from_seeds(full,summary,k)),
            applied_part(ready_nodes(history,full,head),read),applied_clock),
        snapshot_ranks(history,applied,hashes,offset,rank), level(head)>0,
        walk_result(body,seq![head],members),
        forall|k:K| #[trigger] ready_nodes(history,full,head).contains(k) ==> (keeps((bounds.visit)(k)) <==> level(k)>=level(head)),
        forall|k:K| #[trigger] ready_nodes(history,full,head).contains(k) ==> {
            let included=k==head || !cutoff(summary,k,position,clock,candidate);
            &&& expands((body.visit)(k))==included && keeps((body.visit)(k))==included
            &&& included ==> (body.parents)(k).to_set()==(full.parents)(k).to_set()
        },
        forall|k:K| #[trigger] members.contains(k) ==> k==head || level(k)<level(head),
    ensures canonical(descriptor(full,ready_nodes(history,full,head),head,level,rank,boundary,members),head),
{
    ready_cutoff_is_exact(history,applied,deps,hashes,rows,outcomes,full,cut,bounds,head,slot,start,boundary,
        read,summary,position,clock,candidate,applied_clock);
    let scope=ready_nodes(history,full,head);
    let live=history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k));
    crate::cone_spec::closed_live(history,applied,deps,hashes,rows,outcomes);
    live_node_has_live_parents(history,applied,deps,hashes,rows,outcomes,head);
    assert(live.contains(head));
    assert(contains_walk(full,seq![head],live)) by { assert(seq![head].contains(head)); }
    assert((full.parents)(head).to_set()==(history.parents)(head));
    assert forall|k:K| #[trigger] scope.contains(k) && expands((bounds.visit)(k)) implies
        (bounds.parents)(k).to_set()==(full.parents)(k).to_set() by {
        assert(live.contains(k));
        assert((full.parents)(k).to_set()==(history.parents)(k));
    }
    assert(ranked_scope(full,head,scope,rank)) by {
        assert forall|k:K| #[trigger] scope.contains(k) implies reached(full,seq![head],k)
            && expands((full.visit)(k)) && (full.parents)(k).to_set().subset_of(scope)
            && forall|p:K| #[trigger] (full.parents)(k).contains(p) ==> rank(p)<rank(k) by {
            assert(live.contains(k));
            assert((history.parents)(k)==(full.parents)(k).to_set());
            assert forall|p:K| #[trigger] (full.parents)(k).contains(p) implies rank(p)<rank(k) by {
                ranks_follow_original_edges(history,applied,deps,hashes,rows,outcomes,offset,rank,k,p);
            }
        }
    }
    hard_descriptor_is_canonical(full,bounds,body,scope,head,level,rank,start,boundary,members);
}

} // verus!

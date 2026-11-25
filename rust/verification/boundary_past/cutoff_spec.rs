use crate::boundary_memo::Node;
use crate::history_proof::{chain_prefix, exact_clock};
use crate::history_spec::{actor_chains, causal_set, covers as clock_covers, maxima, History};
use crate::lookup_spec::{model, modeled};
use crate::memo_spec::*;
use crate::overlay_bridge::{from_seeds, same_overlay, split_past_is_exact};
use crate::spec::SummaryView;
use crate::walk_spec::{contains_walk, reached, Graph};
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

pub(crate) open spec fn usable_cutoff<'a,A:'a,K,P:Fn(K)->Option<(usize,u32)>,C:Fn(usize)->Option<u32>,Q:Fn(K)->(&'a A,u64)>(k:K,position:P,clock:C,candidate:Q)->bool {
    &&& modeled(position,k)
    &&& match model(position,k) {Some((slot,_))=>modeled(clock,slot),None=>modeled(candidate,k)}
}

pub(crate) open spec fn cutoff<'a,A:'a,K,P:Fn(K)->Option<(usize,u32)>,C:Fn(usize)->Option<u32>,Q:Fn(K)->(&'a A,u64)>(summary:SummaryView<A,K>,k:K,position:P,clock:C,candidate:Q)->bool {
    match model(position,k) {
        Some((slot,seq))=>match model(clock,slot) {Some(upper)=>seq<=upper,None=>false},
        None=>{
            let (actor,seq)=model(candidate,k);
            summary.maxima.contains_key(*actor) && seq<=summary.maxima[*actor]
        }
    }
}

pub(crate) open spec fn applied_part<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F)->Set<K> {
    nodes.filter(|k:K| actor_seq(read,k).is_none())
}

// Normalize table indices and candidate IDs to the same abstract actor key.
// The clock contract summarizes only the applied seeds' full past; the
// candidate projection reads the exact metadata used by the memo.
pub(crate) open spec fn positions<'a,'b,A:'b,K:'a,F:Fn(K)->Node<'a,A,K>,P:Fn(K)->Option<(usize,u32)>,C:Fn(usize)->Option<u32>,Q:Fn(K)->(&'b A,u64)>(
    history:History<K,A>,read:&F,position:P,clock:C,candidate:Q,applied_clock:Map<A,nat>,
)->bool {
    forall|k:K| #[trigger] history.nodes.contains(k) ==> match (actor_seq(read,k),model(position,k)) {
        (None,Some((slot,seq)))=>seq==(history.sequence)(k) &&
            (match model(clock,slot) {
                Some(upper)=>applied_clock.contains_key((history.actor)(k)) && applied_clock[(history.actor)(k)]==upper,
                None=>!applied_clock.contains_key((history.actor)(k)),
            }),
        (Some((actor,seq)),None)=>{
            let (id,s)=model(candidate,k);
            &&& *id==actor && s==seq
            &&& actor==(history.actor)(k) && seq==(history.sequence)(k)
        },
        _=>false,
    }
}

proof fn full_region_closed<K,A>(history:History<K,A>,full:Graph<K>,roots:Seq<K>,region:Set<K>)
    requires forall|k:K| #[trigger] history.nodes.contains(k) ==> (history.parents)(k)==(full.parents)(k).to_set()
        && (history.parents)(k).subset_of(history.nodes) && (full.visit)(k) is KeepAndExpand,
        region==history.nodes.filter(|k:K| reached(full,roots,k)),
    ensures causal_set(history,region),
{
    assert forall|k:K| #[trigger] region.contains(k) implies (history.parents)(k).subset_of(region) by {
        assert forall|p:K| #[trigger] (history.parents)(k).contains(p) implies region.contains(p) by {
            assert forall|s:Set<K>| #[trigger] contains_walk(full,roots,s) implies s.contains(p) by { assert(s.contains(k)); }
        }
    }
}

/// Given the executable memo union and the applied-clock boundary contract,
/// the exact branch of the cutoff test matches membership in the full past.
/// Actor-chain closure is essential: an attained maximum alone is not enough.
pub(crate) proof fn cutoff_is_causal_past<'a,'b,A:'b,K:'a,F:Fn(K)->Node<'a,A,K>,P:Fn(K)->Option<(usize,u32)>,C:Fn(usize)->Option<u32>,Q:Fn(K)->(&'b A,u64)>(
    history:History<K,A>,read:&F,full:Graph<K>,roots:Seq<K>,summary:SummaryView<A,K>,
    position:P,clock:C,candidate:Q,applied_clock:Map<A,nat>,
)
    requires supports_all(history.nodes,read,roots),same_overlay(history.nodes,read,full),actor_chains(history),
        forall|k:K| #[trigger] history.nodes.contains(k) ==> (history.parents)(k)==(full.parents)(k).to_set()
            && (full.visit)(k) is KeepAndExpand,
        covers(read,|k:K| past_prefix(read,roots,roots.len() as int,k),summary),
        positions(history,read,position,clock,candidate,applied_clock),
        maxima(history,history.nodes.filter(|k:K| from_seeds(full,summary,k)),applied_part(history.nodes,read),applied_clock),
    ensures forall|k:K| #[trigger] history.nodes.contains(k) ==> (cutoff(summary,k,position,clock,candidate) <==> reached(full,roots,k)),
{
    split_past_is_exact(history.nodes,read,full,roots,summary);
    let region=history.nodes.filter(|k:K| reached(full,roots,k));
    let applied=applied_part(history.nodes,read);
    let seeded=history.nodes.filter(|k:K| from_seeds(full,summary,k));
    full_region_closed(history,full,roots,region);
    assert(region.intersect(applied) =~= seeded.intersect(applied));
    assert(maxima(history,region,applied,applied_clock)) by {
        assert forall|k:K| #[trigger] region.contains(k) && #[trigger] applied.contains(k) implies clock_covers(history,applied_clock,k) by {
            assert(seeded.contains(k));
        }
        assert forall|a:A| #[trigger] applied_clock.dom().contains(a) implies exists|w:K| #[trigger] region.contains(w)
            && applied.contains(w) && (history.actor)(w)==a && (history.sequence)(w)==applied_clock[a] by {
            let w=choose|w:K| #[trigger] seeded.contains(w) && applied.contains(w) && (history.actor)(w)==a
                && (history.sequence)(w)==applied_clock[a];
            assert(region.contains(w));
        }
    }
    exact_clock(history,region,applied,applied_clock);
    reveal(covers);
    let ov=|k:K| past_prefix(read,roots,roots.len() as int,k);
    assert forall|k:K| #[trigger] history.nodes.contains(k) implies (cutoff(summary,k,position,clock,candidate) <==> reached(full,roots,k)) by {
        if actor_seq(read,k).is_none() {
            assert(applied.contains(k));
            assert(cutoff(summary,k,position,clock,candidate)==clock_covers(history,applied_clock,k));
        } else {
            let (a,seq)=actor_seq(read,k).unwrap();
            if reached(full,roots,k) { assert(ov(k)); }
            if cutoff(summary,k,position,clock,candidate) {
                let w=choose|w:K| #[trigger] ov(w) && actor_seq(read,w)==Some((a,summary.maxima[a]));
                assert(reached(full,roots,w));
                assert(contains_walk(full,roots,history.nodes));
                assert(history.nodes.contains(w));
                assert(region.contains(w));
                chain_prefix(history,region,k,w);
            }
        }
    }
}

} // verus!

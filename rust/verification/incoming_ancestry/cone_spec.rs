use crate::actor_chains::{Candidate, Resolution};
use crate::chain_spec::{classified, well_formed};
use crate::gate_spec::{bad_slot, cone_result, in_cone};
use crate::history_spec::{actor_chains, causal_set, History};
use crate::live_spec::{
    is_live, live_candidate_has_actor_edge, live_node_has_live_parents, row_projection,
};
use crate::lookup_spec::model;
use crate::walk_spec::{contains_walk, expands, keeps, reached, Graph};
use vstd::prelude::*;

verus! {

broadcast use {vstd::set::group_set_lemmas, vstd::seq_lib::group_seq_properties};

pub(crate) open spec fn restrict<K,A>(history:History<K,A>,nodes:Set<K>)->History<K,A> {
    History {nodes,parents:history.parents,actor:history.actor,sequence:history.sequence}
}

// Both walks use the same hash dependencies. The conflict walk stops at
// applied history; a boundary query later expands it using ChangeGraph.
pub(crate) open spec fn aligned_cones<K,A>(history:History<K,A>,live:Set<K>,applied:Set<K>,full:Graph<K>,cut:Graph<K>)->bool {
    forall|k:K| #[trigger] live.contains(k) ==> {
        &&& expands((full.visit)(k)) && (full.parents)(k).to_set()==(history.parents)(k)
        &&& if applied.contains(k) { !expands((cut.visit)(k)) && !keeps((cut.visit)(k)) }
        else { expands((cut.visit)(k)) && keeps((cut.visit)(k)) && (cut.parents)(k).to_set()==(history.parents)(k) }
    }
}

// The occupancy flag must cover every already-applied claimant, even if
// that claimant is not an ancestor of the head. This is exactly the leaf
// contract needed from the graph's contiguous actor prefixes.
pub(crate) open spec fn slot_projection<K,A,F:Fn(K)->((A,u64),bool)>(history:History<K,A>,live:Set<K>,applied:Set<K>,slot:F)->bool {
    forall|k:K| #[trigger] live.contains(k) && !applied.contains(k) ==> {
        let (key,occupied)=model(slot,k);
        &&& key.0==(history.actor)(k) && key.1==(history.sequence)(k)
        &&& !occupied ==> forall|p:K| #[trigger] applied.contains(p) ==>
            ((history.actor)(p)!=(history.actor)(k) || (history.sequence)(p)!=(history.sequence)(k))
    }
}

pub(crate) proof fn closed_live<K,A>(history:History<K,A>,applied:Set<K>,deps:spec_fn(K)->Seq<K>,hashes:Seq<K>,rows:Seq<Candidate<A>>,outcomes:Seq<Resolution>)
    requires row_projection(history,applied,deps,hashes,rows), rows.len()==outcomes.len(), well_formed(rows), classified(rows,outcomes),
        causal_set(history,applied),
    ensures causal_set(history,history.nodes.filter(|h:K| is_live(applied,hashes,outcomes,h))),
{
    let live=history.nodes.filter(|h:K| is_live(applied,hashes,outcomes,h));
    assert forall|h:K| #[trigger] live.contains(h) implies (history.parents)(h).subset_of(live) by {
        live_node_has_live_parents(history,applied,deps,hashes,rows,outcomes,h);
        assert forall|p:K| #[trigger] (history.parents)(h).contains(p) implies live.contains(p) by {
            live_node_has_live_parents(history,applied,deps,hashes,rows,outcomes,p);
        }
    }
}

pub(crate) proof fn compare_cones<K,A>(history:History<K,A>,live:Set<K>,applied:Set<K>,full:Graph<K>,cut:Graph<K>,head:K)
    requires aligned_cones(history,live,applied,full,cut),causal_set(history,live),causal_set(history,applied),
        applied.subset_of(live),live.contains(head),
    ensures forall|k:K| #[trigger] reached(full,seq![head],k) ==> live.contains(k),
        forall|k:K| #[trigger] in_cone(cut,seq![head],k) <==> (reached(full,seq![head],k) && !applied.contains(k)),
{
    let roots=seq![head];
    assert(roots.contains(head));
    assert(contains_walk(full,roots,live));
    assert(contains_walk(cut,roots,live));
    assert forall|k:K| #[trigger] reached(full,roots,k) implies live.contains(k) by {};
    assert forall|k:K| #[trigger] in_cone(cut,roots,k) <==> (reached(full,roots,k) && !applied.contains(k)) by {
        if in_cone(cut,roots,k) {
            assert(live.contains(k));
            assert(!applied.contains(k));
            assert forall|s:Set<K>| #[trigger] contains_walk(full,roots,s) implies s.contains(k) by {
                let known=s.intersect(live);
                assert(contains_walk(cut,roots,known)) by {
                    assert forall|v:K| #[trigger] known.contains(v) && expands((cut.visit)(v))
                        implies (cut.parents)(v).to_set().subset_of(known) by {
                        assert((full.parents)(v).to_set().subset_of(s));
                        assert((history.parents)(v).subset_of(live));
                    }
                }
                assert(known.contains(k));
            }
        }
        if reached(full,roots,k) && !applied.contains(k) {
            assert(live.contains(k));
            assert(keeps((cut.visit)(k)));
            assert forall|s:Set<K>| #[trigger] contains_walk(cut,roots,s) implies s.contains(k) by {
                let joined=s.union(applied).intersect(live);
                assert(contains_walk(full,roots,joined)) by {
                    assert forall|v:K| #[trigger] joined.contains(v) && expands((full.visit)(v))
                        implies (full.parents)(v).to_set().subset_of(joined) by {
                        assert((history.parents)(v).subset_of(live));
                        if applied.contains(v) { assert((history.parents)(v).subset_of(applied)); }
                        else { assert((cut.parents)(v).to_set().subset_of(s)); }
                    }
                }
                assert(joined.contains(k));
            }
        }
    }
}

// Compose the live actor pass and the runtime cone guard. No assumption
// about conflicts or resolvability in unrelated pending groups is needed.
pub(crate) proof fn ready_cone_supports_actor_clocks<K,A,F:Fn(K)->((A,u64),bool)>(
    history:History<K,A>,applied:Set<K>,deps:spec_fn(K)->Seq<K>,hashes:Seq<K>,rows:Seq<Candidate<A>>,outcomes:Seq<Resolution>,
    full:Graph<K>,cut:Graph<K>,head:K,slot:F,
) -> (restricted:History<K,A>)
    requires row_projection(history,applied,deps,hashes,rows), rows.len()==outcomes.len(),well_formed(rows),classified(rows,outcomes),
        causal_set(history,applied),actor_chains(restrict(history,applied)),is_live(applied,hashes,outcomes,head),
        aligned_cones(history,history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k)),applied,full,cut),
        slot_projection(history,history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k)),applied,slot),
        cone_result(cut,seq![head],slot,None),
    ensures restricted==restrict(history,history.nodes.filter(|k:K| reached(full,seq![head],k))),
        causal_set(history,restricted.nodes), actor_chains(restricted),
{
    let roots=seq![head];
    let live=history.nodes.filter(|k:K| is_live(applied,hashes,outcomes,k));
    live_node_has_live_parents(history,applied,deps,hashes,rows,outcomes,head);
    assert(live.contains(head));
    closed_live(history,applied,deps,hashes,rows,outcomes);
    compare_cones(history,live,applied,full,cut,head);
    let cone=history.nodes.filter(|k:K| reached(full,roots,k));
    let result=restrict(history,cone);
    let old=restrict(history,applied);
    assert(actor_chains(old));
    assert forall|k:K| #[trigger] cone.contains(k) implies (history.parents)(k).subset_of(cone) by {
        assert(live.contains(k));
        assert forall|p:K| #[trigger] (history.parents)(k).contains(p) implies cone.contains(p) by {
            assert forall|s:Set<K>| #[trigger] contains_walk(full,roots,s) implies s.contains(p) by { assert(s.contains(k)); }
        }
    }
    assert forall|k:K| #[trigger] cone.contains(k) implies (result.sequence)(k)>=1 by {
        assert(live.contains(k));
        if !applied.contains(k) {
            let i=choose|i:int| 0<=i<hashes.len() && #[trigger] hashes[i]==k && outcomes[i] is Linked;
            assert(0<rows[i].seq);
            assert((result.sequence)(k)==rows[i].seq);
        } else {
            assert(old.nodes.contains(k));
            assert((old.sequence)(k)>=1);
        }
    }
    assert forall|k:K,p:K| #[trigger] cone.contains(k) && #[trigger] cone.contains(p)
        && (result.actor)(k)==(result.actor)(p) && (result.sequence)(k)==(result.sequence)(p) implies k==p by {
        assert(live.contains(k));
        assert(live.contains(p));
        if !applied.contains(k) {
            assert(in_cone(cut,roots,k));
            assert(!bad_slot(cut,roots,slot,k));
            if !applied.contains(p) {
                assert(in_cone(cut,roots,p));
                assert(model(slot,k).0==model(slot,p).0);
            } else { assert(model(slot,k).1); }
        } else if !applied.contains(p) {
            assert(in_cone(cut,roots,p));
            assert(!bad_slot(cut,roots,slot,p));
            assert(model(slot,p).1);
        } else {
            assert(old.nodes.contains(k) && old.nodes.contains(p));
            assert((old.actor)(k)==(old.actor)(p) && (old.sequence)(k)==(old.sequence)(p));
            assert(k==p);
        }
    }
    assert forall|k:K| #[trigger] cone.contains(k) && (result.sequence)(k)>1 implies exists|p:K| {
        &&& #[trigger] (result.parents)(k).contains(p)
        &&& (result.actor)(p)==(result.actor)(k)
        &&& (result.sequence)(p)+1==(result.sequence)(k)
    } by {
        let parent = if applied.contains(k) {
            assert(old.nodes.contains(k));
            assert((old.sequence)(k)>1);
            choose|p:K| #[trigger] (old.parents)(k).contains(p) && (old.actor)(p)==(old.actor)(k)
                && (old.sequence)(p)+1==(old.sequence)(k)
        } else {
            assert(live.contains(k));
            live_candidate_has_actor_edge(history,applied,deps,hashes,rows,outcomes,k);
            choose|p:K| #[trigger] (history.parents)(k).contains(p) && (history.actor)(p)==(history.actor)(k)
                && (history.sequence)(p)+1==(history.sequence)(k)
        };
        assert((result.parents)(k).contains(parent));
        assert((result.actor)(parent)==(result.actor)(k));
        assert((result.sequence)(parent)+1==(result.sequence)(k));
    }
    result
}

} // verus!

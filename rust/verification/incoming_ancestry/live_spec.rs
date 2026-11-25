use crate::actor_chains::{Candidate, Dependency, Resolution};
use crate::chain_spec::{binding, classification, classified, no_broken, well_formed};
use crate::evaluation_spec::{all_ready, status_result};
use crate::history_spec::History;
use crate::lookup_spec::model;
use crate::projection_spec::materialized;
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

// The original hash dependencies are preserved position by position in the
// actor pass. Only the storage representation changes: an earlier rank for
// a candidate predecessor, or aligned metadata from the applied graph.
pub(crate) open spec fn row_projection<K,A>(history:History<K,A>,applied:Set<K>,dependencies:spec_fn(K)->Seq<K>,hashes:Seq<K>,rows:Seq<Candidate<A>>)->bool {
    &&& hashes.len()==rows.len() && hashes.no_duplicates()
    &&& forall|i:int| 0<=i<rows.len() ==> {
        let hash=hashes[i]; let row=#[trigger] rows[i];
        &&& history.nodes.contains(hash) && !applied.contains(hash)
        &&& row.actor==(history.actor)(hash) && row.seq==(history.sequence)(hash)
        &&& (dependencies)(hash).to_set()==(history.parents)(hash)
        &&& row.deps@.len()==(dependencies)(hash).len()
    }
    &&& forall|i:int,j:int| 0<=i<rows.len() && 0<=j<rows[i].deps@.len() ==> {
        let edge=#[trigger] rows[i].deps@[j];
        let parent=(dependencies)(hashes[i])[j];
        match edge {
            Dependency::Candidate(p)=> p<hashes.len() && hashes[p as int]==parent,
            Dependency::Applied{actor,seq,..}=>applied.contains(parent)
                && actor==(history.actor)(parent) && seq==(history.sequence)(parent),
        }
    }
}

pub(crate) open spec fn is_live<K>(applied:Set<K>,hashes:Seq<K>,outcomes:Seq<Resolution>,hash:K)->bool {
    applied.contains(hash) || exists|i:int| 0<=i<hashes.len() && #[trigger] hashes[i]==hash && outcomes[i] is Linked
}

// The exact materialized valid_authors domain is the live candidate set.
// A present None (linked but authorless) is still live.
pub(crate) proof fn auth_domain_is_liveness<K,T,I:Fn(usize)->Option<T>,A:Fn(usize)->Option<T>>(applied:Set<K>,hashes:Seq<K>,outcomes:Seq<Resolution>,initial:I,history:A,authors:Map<K,Option<T>>,broken:Map<K,K>)
    requires outcomes.len()==hashes.len(),hashes.no_duplicates(),materialized(hashes,outcomes,initial,history,hashes.len() as int,authors,broken),
    ensures forall|hash:K| #[trigger] is_live(applied,hashes,outcomes,hash) <==> (applied.contains(hash) || authors.contains_key(hash)),
{
    assert(hashes.take(hashes.len() as int) =~= hashes);
    assert forall|h:K| #[trigger] is_live(applied,hashes,outcomes,h) <==> (applied.contains(h) || authors.contains_key(h)) by {
        if !applied.contains(h) {
            if is_live(applied,hashes,outcomes,h) {
                let j=choose|j:int| 0<=j<hashes.len() && hashes[j]==h && #[trigger] outcomes[j] is Linked;
                assert(authors.contains_key(hashes[j]));
            }
            if authors.contains_key(h) {
                assert(authors.dom().union(broken.dom()).contains(h));
                assert(hashes.to_set().contains(h));
                assert(hashes.contains(h));
                let j=choose|j:int| 0<=j<hashes.len() && #[trigger] hashes[j]==h;
                assert(outcomes[j] is Linked);
            }
        }
    }
}

// In the production readiness scan, Ok(true) is for a group all of whose
// members are applied or have an entry in valid_authors. Its own head is
// one of those members, established by the intrinsic shape check.
pub(crate) proof fn ready_head_is_live<K,M,E,F:Fn(&M)->Result<bool,E>,H:Fn(&M)->K>(groups:Seq<M>,observe:F,key:H,head:K,applied:Set<K>,authors:Set<K>)
    requires status_result(groups,observe,Ok(true)),
        exists|i:int| 0<=i<groups.len() && #[trigger] model(key,&groups[i])==head,
        forall|i:int| 0<=i<groups.len() && #[trigger] model(observe,&groups[i])==Ok(true) ==>
            applied.contains(model(key,&groups[i])) || authors.contains(model(key,&groups[i])),
    ensures applied.contains(head) || authors.contains(head),
{
    assert(all_ready(groups,observe,groups.len() as int));
    let i=choose|i:int| 0<=i<groups.len() && #[trigger] model(key,&groups[i])==head;
    assert(model(observe,&groups[i])==Ok(true));
}

// Compose the actual materializer and readiness scan instead of taking a
// live head on faith. The single-mark resolver is a leaf observation of
// graph membership and the two materialized maps.
pub(crate) proof fn checked_head_is_live<K,T,M,E,I:Fn(usize)->Option<T>,A:Fn(usize)->Option<T>,F:Fn(&M)->Result<bool,E>,H:Fn(&M)->K>(
    members:Seq<M>, observe:F, key:H, head:K, applied:Set<K>, hashes:Seq<K>, outcomes:Seq<Resolution>,
    initial:I, history:A, authors:Map<K,Option<T>>, broken:Map<K,K>,
)
    requires outcomes.len()==hashes.len(),hashes.no_duplicates(),
        materialized(hashes,outcomes,initial,history,hashes.len() as int,authors,broken),
        status_result(members,observe,Ok(true)),
        exists|i:int| 0<=i<members.len() && #[trigger] model(key,&members[i])==head,
        forall|i:int| 0<=i<members.len() && #[trigger] model(observe,&members[i])==Ok(true) ==>
            applied.contains(model(key,&members[i])) || authors.contains_key(model(key,&members[i])),
    ensures is_live(applied,hashes,outcomes,head),
{
    auth_domain_is_liveness(applied,hashes,outcomes,initial,history,authors,broken);
    ready_head_is_live(members,observe,key,head,applied,authors.dom());
}

proof fn read_edge<K,A>(history:History<K,A>,applied:Set<K>,deps:spec_fn(K)->Seq<K>,hashes:Seq<K>,rows:Seq<Candidate<A>>,outcomes:Seq<Resolution>,i:int,j:int)
    requires row_projection(history,applied,deps,hashes,rows), rows.len()==outcomes.len(),well_formed(rows),classified(rows,outcomes),
        0<=i<rows.len(),0<=j<rows[i].deps@.len(),outcomes[i] is Linked,
    ensures is_live(applied,hashes,outcomes,deps(hashes[i])[j]),
{
    let row=rows[i];
    let earlier=outcomes.take(i);
    assert(classification(rows,earlier,i,outcomes[i]));
    assert(no_broken(row.deps@,earlier,row.deps@.len() as int));
    if let Dependency::Candidate(p)=row.deps@[j] {
        assert(p<i); assert(earlier[p as int]==outcomes[p as int]);
        assert(hashes[p as int]==deps(hashes[i])[j]);
        assert(outcomes[p as int] is Linked);
    }
}

pub(crate) proof fn live_node_has_live_parents<K,A>(history:History<K,A>,applied:Set<K>,deps:spec_fn(K)->Seq<K>,hashes:Seq<K>,rows:Seq<Candidate<A>>,outcomes:Seq<Resolution>,hash:K)
    requires row_projection(history,applied,deps,hashes,rows), rows.len()==outcomes.len(),well_formed(rows),classified(rows,outcomes),
        applied.subset_of(history.nodes),forall|h:K| #[trigger] applied.contains(h) ==> (history.parents)(h).subset_of(applied),
        is_live(applied,hashes,outcomes,hash),
    ensures history.nodes.contains(hash),forall|parent:K| #[trigger] (history.parents)(hash).contains(parent) ==> is_live(applied,hashes,outcomes,parent),
{
    if applied.contains(hash) {
        assert((history.parents)(hash).subset_of(applied));
    } else {
        let i=choose|i:int| 0<=i<hashes.len() && #[trigger] hashes[i]==hash && outcomes[i] is Linked;
        assert forall|p:K| #[trigger] (history.parents)(hash).contains(p) implies is_live(applied,hashes,outcomes,p) by {
            let j=choose|j:int| 0<=j<deps(hash).len() && #[trigger] deps(hash)[j]==p;
            read_edge(history,applied,deps,hashes,rows,outcomes,i,j);
        }
    }
}

/// A linked chain read from the ranked pass has the direct same-actor
/// predecessor required by the actor-clock theorem, even across groups.
pub(crate) proof fn live_candidate_has_actor_edge<K,A>(history:History<K,A>,applied:Set<K>,deps:spec_fn(K)->Seq<K>,hashes:Seq<K>,rows:Seq<Candidate<A>>,outcomes:Seq<Resolution>,hash:K)
    requires row_projection(history,applied,deps,hashes,rows),rows.len()==outcomes.len(),well_formed(rows),classified(rows,outcomes),
        is_live(applied,hashes,outcomes,hash),!applied.contains(hash),(history.sequence)(hash)>1,
    ensures exists|p:K| #[trigger] (history.parents)(hash).contains(p) && (history.actor)(p)==(history.actor)(hash)
        && (history.sequence)(p)+1==(history.sequence)(hash),
{
    let i=choose|i:int| 0<=i<hashes.len() && #[trigger] hashes[i]==hash && outcomes[i] is Linked;
    let row=rows[i];
    let source=match outcomes[i] {Resolution::Linked(s)=>s,_=>arbitrary()};
    assert(classification(rows,outcomes.take(i),i,outcomes[i]));
    let j=choose|j:int| 0<=j<row.deps@.len() && binding(rows,outcomes.take(i),i,#[trigger] row.deps@[j])==Some(source);
    let p=deps(hash)[j];
    assert(deps(hash).contains(p));
    if let Dependency::Candidate(rank)=row.deps@[j] { assert(hashes[rank as int]==p); }
    assert((history.parents)(hash).contains(p));
    assert((history.actor)(p)==(history.actor)(hash));
    assert((history.sequence)(p)+1==(history.sequence)(hash));
}

} // verus!

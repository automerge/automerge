use crate::boundary_memo::{Cache, Node};
use crate::boundary_summary::Summary;
use crate::spec::{actor_view, laws, seed_view, summary_inv, view, SummaryView};
use crate::walk::Visit;
use crate::walk_spec::{reached, Graph};
use std::hash::Hash;
use vstd::laws_eq::obeys_concrete_eq;
use vstd::pervasive::obeys_clone_eq;
use vstd::prelude::*;
use vstd::std_specs::hash::{hash_map_inv, obeys_key_model};

verus! {

pub(crate) open spec fn memo_laws<A: Clone + Eq + Hash, K: Copy + Ord + Hash>() -> bool {
    &&& laws::<A,K>()
    &&& obeys_key_model::<K>()
    &&& obeys_concrete_eq::<K>()
    &&& obeys_clone_eq::<K>()
}

pub(crate) open spec fn projected<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,key:K)->Node<'a,A,K> {
    choose|n:Node<'a,A,K>| (*read).ensures((key,),n)
}

pub(crate) open spec fn readable<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,key:K)->bool {
    &&& (*read).requires((key,))
    &&& (*read).ensures((key,),projected(read,key))
    &&& forall|n:Node<'a,A,K>| #[trigger] (*read).ensures((key,),n) ==> n==projected(read,key)
}

pub(crate) open spec fn dependencies<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,key:K)->Seq<K> {
    match projected(read,key) {Node::Applied=>Seq::empty(), Node::Candidate{parents,..}=>parents@}
}

pub(crate) open spec fn actor_seq<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,key:K)->Option<(A,u64)> {
    match projected(read,key) {Node::Applied=>None, Node::Candidate{actor,seq,..}=>Some((actor,seq))}
}

// Applied changes are leaves in this overlay. The model is deliberately
// independent of a particular finite universe used to bound a walk.
pub(crate) open spec fn overlay<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F)->Graph<K> {
    Graph { nodes:Set::empty(), parents:|k:K| dependencies(read,k), visit:|k:K| Visit::KeepAndExpand }
}

pub(crate) open spec fn past<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,root:K,key:K)->bool {
    reached(overlay(read),seq![root],key)
}

pub(crate) open spec fn supports<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,root:K)->bool {
    &&& nodes.contains(root)
    &&& forall|k:K| #[trigger] nodes.contains(k) ==> readable(read,k) && dependencies(read,k).to_set().subset_of(nodes)
}

pub(crate) open spec fn supports_all<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,roots:Seq<K>)->bool {
    &&& roots.to_set().subset_of(nodes)
    &&& forall|k:K| #[trigger] nodes.contains(k) ==> readable(read,k) && dependencies(read,k).to_set().subset_of(nodes)
}

pub(crate) open spec fn past_prefix<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,roots:Seq<K>,end:int,k:K)->bool {
    exists|i:int| 0<=i<end && past(read,#[trigger] roots[i],k)
}

pub(crate) open spec fn universe<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,root:K)->Set<K> {
    choose|nodes:Set<K>| #[trigger] supports(nodes,read,root)
}

pub(crate) open spec fn region<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,root:K)->Set<K> {
    nodes.filter(|k:K| past(read,root,k))
}

pub(crate) open spec fn candidate_part<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,k:K)->SummaryView<A,K> {
    match projected(read,k) { Node::Applied=>seed_view(k), Node::Candidate{actor,seq,..}=>actor_view(actor,seq) }
}

// The applied seeds are exactly the reachable applied leaves; each candidate
// maximum is both an upper bound and attained by a reachable candidate.
#[verifier::opaque]
pub(crate) open spec fn covers<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,area:spec_fn(K)->bool,out:SummaryView<A,K>)->bool {
    &&& forall|k:K| #[trigger] out.seeds.contains(k) <==> area(k) && actor_seq(read,k).is_none()
    &&& forall|k:K| #[trigger] area(k) && actor_seq(read,k).is_some() ==>
        out.maxima.contains_key(actor_seq(read,k).unwrap().0)
        && out.maxima[actor_seq(read,k).unwrap().0]>=actor_seq(read,k).unwrap().1
    &&& forall|a:A| #[trigger] out.maxima.contains_key(a) ==> exists|k:K| #[trigger] area(k)
        && actor_seq(read,k)==Some((a,out.maxima[a]))
}

pub(crate) open spec fn exact<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,root:K,out:SummaryView<A,K>)->bool {
    covers(read,|k:K| past(read,root,k),out)
}

pub(crate) open spec fn precise_entries<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(map:Map<K,Summary<A,K>>,read:&F)->bool
    where A:Eq+Hash,K:Ord {
    forall|root:K| #[trigger] map.contains_key(root) ==> summary_inv(&map[root]) && exact(read,root,view(&map[root]))
}

pub(crate) open spec fn valid_entries<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(memo:&Cache<A,K>,read:&F)->bool
    where A:Eq+Hash,K:Ord+Hash {
    hash_map_inv(&memo.entries) && precise_entries(memo.entries@,read)
}

pub(crate) open spec fn cache_transition<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(
    before:Map<K,Summary<A,K>>,after:Map<K,Summary<A,K>>,root:K,read:&F,out:SummaryView<A,K>,
)->bool {
    if before.contains_key(root) { after==before && out==view(&before[root]) }
    else if actor_seq(read,root).is_none() { after==before }
    else { after.contains_key(root) && view(&after[root])==out && after==before.insert(root,after[root]) }
}

pub(crate) open spec fn preserves<A,K>(original:Map<K,Summary<A,K>>,next:Map<K,Summary<A,K>>)->bool {
    forall|k:K| #[trigger] original.contains_key(k) ==> next.contains_key(k) && next[k]==original[k]
}

pub(crate) open spec fn prefix_work<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(
    nodes:Set<K>,read:&F,root:K,order:Seq<K>,seen:Set<K>,before:Seq<K>,covered:Set<K>,out:SummaryView<A,K>,
)->bool {
    &&& order.no_duplicates() && seen==order.to_set() && seen.subset_of(nodes) && seen.contains(root)
    &&& before.to_set().subset_of(seen) && before.to_set().subset_of(covered)
    &&& covered.subset_of(nodes)
    &&& forall|k:K| #[trigger] seen.contains(k) ==> past(read,root,k)
    &&& forall|k:K| #[trigger] covered.contains(k) ==> past(read,root,k)
        && dependencies(read,k).to_set().subset_of(seen.union(covered))
    &&& covers(read,|k:K| covered.contains(k),out)
}

pub(crate) open spec fn settled<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(
    nodes:Set<K>,read:&F,root:K,order:Seq<K>,seen:Set<K>,cursor:int,covered:Set<K>,out:SummaryView<A,K>,
)->bool {
    prefix_work(nodes,read,root,order,seen,order.take(cursor),covered,out)
}

} // verus!

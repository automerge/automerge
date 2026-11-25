use crate::boundary_memo::Node;
use crate::boundary_summary::Summary;
use crate::memo_spec::*;
use crate::spec::{empty_view, join, maximum, seed_view, summary_inv, value, view, SummaryView};
use crate::walk_spec::contains_walk;
use std::hash::Hash;
use vstd::prelude::*;
use vstd::set_lib::lemma_len_subset;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

pub(crate) proof fn root_in_past<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,root:K)
    ensures past(read,root,root),
{
    assert(seq![root].contains(root));
    assert forall|s:Set<K>| #[trigger] contains_walk(overlay(read),seq![root],s) implies s.contains(root) by {};
}

pub(crate) proof fn parent_in_past<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,root:K,key:K,parent:K)
    requires past(read,root,key), dependencies(read,key).contains(parent),
    ensures past(read,root,parent),
{
    assert forall|s:Set<K>| #[trigger] contains_walk(overlay(read),seq![root],s) implies s.contains(parent) by {
        assert(s.contains(key));
    }
}

proof fn past_is_transitive<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,root:K,mid:K,k:K)
    requires past(read,root,mid), past(read,mid,k),
    ensures past(read,root,k),
{
    assert forall|s:Set<K>| #[trigger] contains_walk(overlay(read),seq![root],s) implies s.contains(k) by {
        assert(s.contains(mid));
        assert(contains_walk(overlay(read),seq![mid],s));
    }
}

proof fn region_exact<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,key:K)
    requires supports(nodes,read,key),
    ensures forall|k:K| #[trigger] region(nodes,read,key).contains(k) <==> past(read,key,k),
{
    assert(contains_walk(overlay(read),seq![key],nodes));
    assert forall|k:K| #[trigger] region(nodes,read,key).contains(k) <==> past(read,key,k) by {
        if past(read,key,k) { assert(nodes.contains(k)); }
    }
}

proof fn covers_part<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,key:K)
    ensures covers(read,|k:K| k==key,candidate_part(read,key)),
{
    reveal(covers);
    let area=|k:K| k==key;
    let out=candidate_part(read,key);
    if let Node::Candidate{actor,seq,..} = projected(read,key) {
        assert forall|a:A| #[trigger] out.maxima.contains_key(a) implies exists|k:K| #[trigger] area(k)
            && actor_seq(read,k)==Some((a,out.maxima[a])) by {
            assert(actor_seq(read,key)==Some((actor,seq)));
            assert(area(key));
        }
    }
    assert forall|k:K| #[trigger] out.seeds.contains(k) <==> area(k) && actor_seq(read,k).is_none() by {};
    assert forall|k:K| #[trigger] area(k) && actor_seq(read,k).is_some() implies
        out.maxima.contains_key(actor_seq(read,k).unwrap().0)
        && out.maxima[actor_seq(read,k).unwrap().0]>=actor_seq(read,k).unwrap().1 by {};
    assert(covers(read,area,out));
}

proof fn union_covers<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,left:Set<K>,right:Set<K>,old:SummaryView<A,K>,added:SummaryView<A,K>,new:SummaryView<A,K>)
    requires covers(read,|k:K| left.contains(k),old), covers(read,|k:K| right.contains(k),added), join(old,added,new),
    ensures covers(read,|k:K| left.union(right).contains(k),new),
{
    let l = |k:K| left.contains(k);
    let r = |k:K| right.contains(k);
    merge_areas(read,l,r,old,added,new);
    assert((|k:K| l(k) || r(k)) =~= (|k:K| left.union(right).contains(k)));
}

proof fn merge_areas<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,l:spec_fn(K)->bool,r:spec_fn(K)->bool,old:SummaryView<A,K>,added:SummaryView<A,K>,new:SummaryView<A,K>)
    requires covers(read,l,old),covers(read,r,added),join(old,added,new),
    ensures covers(read,|k:K| l(k) || r(k),new),
{
    reveal(covers);
    let area = |k:K| l(k) || r(k);
    assert forall|k:K| #[trigger] new.seeds.contains(k) <==> area(k) && actor_seq(read,k).is_none() by {
        assert(old.seeds.contains(k) <==> l(k) && actor_seq(read,k).is_none());
        assert(added.seeds.contains(k) <==> r(k) && actor_seq(read,k).is_none());
    }
    assert forall|k:K| #[trigger] area(k) && actor_seq(read,k).is_some() implies
        new.maxima.contains_key(actor_seq(read,k).unwrap().0)
        && new.maxima[actor_seq(read,k).unwrap().0]>=actor_seq(read,k).unwrap().1 by {
        let a=actor_seq(read,k).unwrap().0;
        assert(l(k) || r(k));
        assert(value(new.maxima,a)==maximum(value(old.maxima,a),value(added.maxima,a)));
    }
    assert forall|a:A| #[trigger] new.maxima.contains_key(a) implies exists|k:K| #[trigger] area(k)
        && actor_seq(read,k)==Some((a,new.maxima[a])) by {
        assert(value(new.maxima,a)==maximum(value(old.maxima,a),value(added.maxima,a)));
        if old.maxima.contains_key(a) && old.maxima[a]==new.maxima[a] {
            let k=choose|k:K| #[trigger] l(k) && actor_seq(read,k)==Some((a,old.maxima[a]));
            assert(area(k));
        } else {
            let k=choose|k:K| #[trigger] r(k) && actor_seq(read,k)==Some((a,added.maxima[a]));
            assert(area(k));
        }
    }
}

pub(crate) proof fn terminal_root<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,root:K,out:SummaryView<A,K>)
    requires projected(read,root) is Applied, out==seed_view(root),
    ensures exact(read,root,out),
{
    root_in_past(read,root);
    let singleton=Set::empty().insert(root);
    assert(contains_walk(overlay(read),seq![root],singleton));
    assert forall|k:K| #[trigger] past(read,root,k) <==> k==root by {};
    covers_part(read,root);
    assert((|k:K| k==root) =~= (|k:K| past(read,root,k)));
}

pub(crate) proof fn inserted<'a,A:Eq+Hash,K:Ord+'a,F:Fn(K)->Node<'a,A,K>>(prior:Map<K,Summary<A,K>>,out:Map<K,Summary<A,K>>,read:&F,root:K,part:SummaryView<A,K>)
    requires precise_entries(prior,read), out.contains_key(root), out==prior.insert(root,out[root]),
        exact(read,root,part), view(&out[root])==part, summary_inv(&out[root]),
    ensures precise_entries(out,read),
{
    assert forall|k:K| #[trigger] out.contains_key(k) implies summary_inv(&out[k]) && exact(read,k,view(&out[k])) by {
        if k!=root { assert(prior.contains_key(k)); }
    }
}

pub(crate) proof fn start<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,root:K,order:Seq<K>,seen:Set<K>,out:SummaryView<A,K>)
    requires supports(nodes,read,root),order==seq![root],seen==Set::empty().insert(root),out==empty_view(),
    ensures settled(nodes,read,root,order,seen,0,Set::empty(),out),
{
    root_in_past(read,root);
    reveal(covers);
    assert(order.take(0) =~= Seq::<K>::empty());
    assert(order.contains(root));
    assert(order.to_set() =~= seen);
    assert(covers(read,|k:K| Set::empty().contains(k),out));
}

pub(crate) proof fn begin_node<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,root:K,order:Seq<K>,seen:Set<K>,cursor:int,covered:Set<K>,out:SummaryView<A,K>)
    requires settled(nodes,read,root,order,seen,cursor,covered,out),0<=cursor<order.len(),
    ensures seen.contains(order[cursor]), nodes.contains(order[cursor]),
        order.take(cursor+1)==order.take(cursor).push(order[cursor]),
{
    assert(order.contains(order[cursor]));
    order.lemma_take_succ_push(cursor);
}

pub(crate) proof fn enqueue_parent<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,root:K,key:K,j:int,order:Seq<K>,seen:Set<K>,before:Seq<K>,covered:Set<K>,out:SummaryView<A,K>,next:Seq<K>,seen_after:Set<K>)
    requires prefix_work(nodes,read,root,order,seen,before,covered,out), supports(nodes,read,root),seen.contains(key),
        0<=j<dependencies(read,key).len(),
        {
            let p=dependencies(read,key)[j];
            seen_after==seen.insert(p) && next==if seen.contains(p) {order} else {order.push(p)}
        },
    ensures prefix_work(nodes,read,root,next,seen_after,before,covered,out),
{
    let p=dependencies(read,key)[j];
    assert(dependencies(read,key).contains(p));
    assert(nodes.contains(key));
    assert(nodes.contains(p));
    parent_in_past(read,root,key,p);
    if !seen.contains(p) {
        assert forall|i:int,k:int| 0<=i<k<next.len() implies next[i]!=next[k] by {
            if k==order.len() {assert(order.contains(order[i]));}
        }
    }
    assert(seen_after =~= next.to_set());
    assert forall|k:K| #[trigger] seen_after.contains(k) implies past(read,root,k) by {
        if k!=p {assert(seen.contains(k));}
    }
    assert forall|k:K| #[trigger] covered.contains(k) implies dependencies(read,k).to_set().subset_of(seen_after.union(covered)) by {
        assert(dependencies(read,k).to_set().subset_of(seen.union(covered)));
    }
}

pub(crate) proof fn add_one<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,root:K,node:K,order:Seq<K>,seen:Set<K>,before:Seq<K>,after:Seq<K>,covered:Set<K>,old:SummaryView<A,K>,out:SummaryView<A,K>)
    requires prefix_work(nodes,read,root,order,seen,before,covered,old), seen.contains(node),after==before.push(node),
        dependencies(read,node).to_set().subset_of(seen),join(old,candidate_part(read,node),out),
    ensures prefix_work(nodes,read,root,order,seen,after,covered.insert(node),out),
{
    covers_part(read,node);
    let singleton=Set::empty().insert(node);
    assert((|k:K| singleton.contains(k)) =~= (|k:K| k==node));
    union_covers(read,covered,singleton,old,candidate_part(read,node),out);
    assert(covered.union(singleton) =~= covered.insert(node));
    let area=covered.insert(node);
    assert forall|k:K| #[trigger] area.contains(k) implies past(read,root,k)
        && dependencies(read,k).to_set().subset_of(seen.union(area)) by {
        if k!=node {assert(dependencies(read,k).to_set().subset_of(seen.union(covered)));}
    }
}

pub(crate) proof fn add_cached<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,root:K,node:K,order:Seq<K>,seen:Set<K>,before:Seq<K>,after:Seq<K>,covered:Set<K>,old:SummaryView<A,K>,cached:SummaryView<A,K>,out:SummaryView<A,K>) -> (combined:Set<K>)
    requires prefix_work(nodes,read,root,order,seen,before,covered,old),supports(nodes,read,root),seen.contains(node),after==before.push(node),
        exact(read,node,cached),join(old,cached,out),
    ensures combined==covered.union(region(nodes,read,node)), prefix_work(nodes,read,root,order,seen,after,combined,out),
{
    assert(nodes.contains(node));
    root_in_past(read,node);
    region_exact(nodes,read,node);
    let ancestors=region(nodes,read,node);
    assert(ancestors.contains(node));
    assert((|k:K| ancestors.contains(k)) =~= (|k:K| past(read,node,k)));
    union_covers(read,covered,ancestors,old,cached,out);
    let area=covered.union(ancestors);
    assert forall|k:K| #[trigger] area.contains(k) implies past(read,root,k)
        && dependencies(read,k).to_set().subset_of(seen.union(area)) by {
        if covered.contains(k) {
            assert(dependencies(read,k).to_set().subset_of(seen.union(covered)));
        } else {
            past_is_transitive(read,root,node,k);
            assert forall|p:K| #[trigger] dependencies(read,k).contains(p) implies ancestors.contains(p) by {
                parent_in_past(read,node,k,p);
            }
        }
    }
    area
}

pub(crate) proof fn bound_order<K>(nodes:Set<K>,order:Seq<K>,seen:Set<K>)
    requires order.no_duplicates(),seen==order.to_set(),seen.subset_of(nodes),
    ensures order.len()<=nodes.len(),
{
    order.unique_seq_to_set();
    lemma_len_subset(seen,nodes);
}

pub(crate) proof fn finish<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,root:K,order:Seq<K>,seen:Set<K>,covered:Set<K>,out:SummaryView<A,K>)
    requires prefix_work(nodes,read,root,order,seen,order,covered,out),
    ensures exact(read,root,out),
{
    assert(seen.subset_of(covered));
    assert(seq![root].contains(root));
    assert(seen.union(covered) =~= covered);
    assert(contains_walk(overlay(read),seq![root],covered));
    assert forall|k:K| #[trigger] covered.contains(k) <==> past(read,root,k) by {
        if past(read,root,k) { assert(covered.contains(k)); }
    }
    assert((|k:K| covered.contains(k)) =~= (|k:K| past(read,root,k)));
}

pub(crate) proof fn empty_prefix<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,roots:Seq<K>,out:SummaryView<A,K>)
    requires out==empty_view(),
    ensures covers(read,|k:K| past_prefix(read,roots,0,k),out),
{
    reveal(covers);
}

pub(crate) proof fn extend_prefix<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(read:&F,roots:Seq<K>,i:int,prior:SummaryView<A,K>,part:SummaryView<A,K>,out:SummaryView<A,K>)
    requires 0<=i<roots.len(),covers(read,|k:K| past_prefix(read,roots,i,k),prior),
        exact(read,roots[i],part),join(prior,part,out),
    ensures covers(read,|k:K| past_prefix(read,roots,i+1,k),out),
{
    let left=|k:K| past_prefix(read,roots,i,k);
    let right=|k:K| past(read,roots[i],k);
    merge_areas(read,left,right,prior,part,out);
    assert forall|k:K| #[trigger] past_prefix(read,roots,i+1,k) <==> left(k) || right(k) by {};
    assert((|k:K| left(k) || right(k)) =~= (|k:K| past_prefix(read,roots,i+1,k)));
}

pub(crate) proof fn extend_cache<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(original:Map<K,Summary<A,K>>,prior:Map<K,Summary<A,K>>,out:Map<K,Summary<A,K>>,root:K,read:&F,part:SummaryView<A,K>)
    requires preserves(original,prior),cache_transition(prior,out,root,read,part),
    ensures preserves(original,out),
{
    assert forall|k:K| #[trigger] original.contains_key(k) implies out.contains_key(k) && out[k]==original[k] by {
        assert(prior.contains_key(k));
    }
}

} // verus!

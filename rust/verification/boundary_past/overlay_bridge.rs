use crate::boundary_memo::Node;
use crate::memo_spec::*;
use crate::spec::SummaryView;
use crate::walk_spec::{contains_walk, expands, reached, Graph};
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

// Decode the same dependency graph twice: the memo walks the unapplied
// part up to applied leaves, while the applied clock follows those leaves
// into immutable applied history. Applied history has no unapplied parents.
pub(crate) open spec fn same_overlay<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,full:Graph<K>)->bool {
    &&& forall|k:K| #[trigger] expands((full.visit)(k))
    &&& forall|k:K| #[trigger] nodes.contains(k) ==> (full.parents)(k).to_set().subset_of(nodes)
        && if actor_seq(read,k).is_some() {
            (full.parents)(k)==dependencies(read,k)
        } else {
            forall|p:K| #[trigger] (full.parents)(k).contains(p) ==> actor_seq(read,p).is_none()
        }
}

pub(crate) open spec fn from_seeds<A,K>(full:Graph<K>,summary:SummaryView<A,K>,key:K)->bool {
    exists|seed:K| #[trigger] summary.seeds.contains(seed) && reached(full,seq![seed],key)
}

proof fn root_in_full<K>(full:Graph<K>,root:K)
    ensures reached(full,seq![root],root),
{
    assert(seq![root].contains(root));
    assert forall|s:Set<K>| #[trigger] contains_walk(full,seq![root],s) implies s.contains(root) by {};
}

proof fn follow<K>(full:Graph<K>,roots:Seq<K>,node:K,parent:K)
    requires reached(full,roots,node), expands((full.visit)(node)), (full.parents)(node).contains(parent),
    ensures reached(full,roots,parent),
{
    assert forall|s:Set<K>| #[trigger] contains_walk(full,roots,s) implies s.contains(parent) by { assert(s.contains(node)); }
}

proof fn overlay_in_full<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,full:Graph<K>,roots:Seq<K>,k:K)
    requires supports_all(nodes,read,roots),same_overlay(nodes,read,full),past_prefix(read,roots,roots.len() as int,k),
    ensures nodes.contains(k),reached(full,roots,k),
{
    let i=choose|i:int| 0<=i<roots.len() && past(read,#[trigger] roots[i],k);
    let root=roots[i];
    assert(roots.contains(root));
    assert(contains_walk(overlay(read),seq![root],nodes));
    assert(nodes.contains(k));
    assert forall|s:Set<K>| #[trigger] contains_walk(full,roots,s) implies s.contains(k) by {
        let known=s.intersect(nodes);
        assert(contains_walk(overlay(read),seq![root],known)) by {
            assert forall|n:K| #[trigger] known.contains(n) implies dependencies(read,n).to_set().subset_of(known) by {
                if actor_seq(read,n).is_some() {
                    assert(dependencies(read,n)==(full.parents)(n));
                    assert((full.parents)(n).to_set().subset_of(s));
                    assert((full.parents)(n).to_set().subset_of(nodes));
                }
            }
        }
        assert(known.contains(k));
    }
}

proof fn seed_is_applied<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,full:Graph<K>,roots:Seq<K>,summary:SummaryView<A,K>,seed:K,k:K)
    requires supports_all(nodes,read,roots),same_overlay(nodes,read,full),
        covers(read,|x:K| past_prefix(read,roots,roots.len() as int,x),summary),
        summary.seeds.contains(seed),reached(full,seq![seed],k),
    ensures nodes.contains(k),actor_seq(read,k).is_none(),reached(full,roots,k),
{
    reveal(covers);
    assert(actor_seq(read,seed).is_none());
    overlay_in_full(nodes,read,full,roots,seed);
    let applied=nodes.filter(|n:K| actor_seq(read,n).is_none());
    assert(contains_walk(full,seq![seed],applied));
    assert(applied.contains(k));
    assert forall|s:Set<K>| #[trigger] contains_walk(full,roots,s) implies s.contains(k) by {
        assert(s.contains(seed));
        assert(contains_walk(full,seq![seed],s));
    }
}

/// Interpret the exact summary from the production memo against the full
/// graph. No applied seed can represent an unapplied change, and no candidate
/// ancestor is lost by stopping at the first applied changes.
pub(crate) proof fn split_past_is_exact<'a,A,K:'a,F:Fn(K)->Node<'a,A,K>>(nodes:Set<K>,read:&F,full:Graph<K>,roots:Seq<K>,summary:SummaryView<A,K>)
    requires supports_all(nodes,read,roots),same_overlay(nodes,read,full),
        covers(read,|k:K| past_prefix(read,roots,roots.len() as int,k),summary),
    ensures forall|k:K| #[trigger] reached(full,roots,k) <==> (
        if actor_seq(read,k).is_some() { past_prefix(read,roots,roots.len() as int,k) }
        else { from_seeds(full,summary,k) }),
{
    reveal(covers);
    let ov = |k:K| past_prefix(read,roots,roots.len() as int,k);
    let ap = |k:K| from_seeds(full,summary,k);
    let joined=nodes.filter(|k:K| ov(k) || ap(k));
    assert forall|k:K| #[trigger] ov(k) implies nodes.contains(k) && reached(full,roots,k) by {
        overlay_in_full(nodes,read,full,roots,k);
    }
    assert forall|k:K| #[trigger] ap(k) implies nodes.contains(k) && actor_seq(read,k).is_none() && reached(full,roots,k) by {
        let seed=choose|s:K| #[trigger] summary.seeds.contains(s) && reached(full,seq![s],k);
        seed_is_applied(nodes,read,full,roots,summary,seed,k);
    }
    assert(roots.to_set().subset_of(joined)) by {
        assert forall|k:K| #[trigger] roots.to_set().contains(k) implies joined.contains(k) by {
            let i=choose|i:int| 0<=i<roots.len() && roots[i]==k;
            crate::memo_proof::root_in_past(read,k);
            assert(past(read,roots[i],k));
            assert(ov(k));
        }
    }
    assert(contains_walk(full,roots,joined)) by {
        assert forall|k:K| #[trigger] joined.contains(k) && expands((full.visit)(k)) implies
            (full.parents)(k).to_set().subset_of(joined) by {
            assert forall|p:K| #[trigger] (full.parents)(k).contains(p) implies joined.contains(p) by {
                if ov(k) && actor_seq(read,k).is_some() {
                    let i=choose|i:int| 0<=i<roots.len() && past(read,#[trigger] roots[i],k);
                    crate::memo_proof::parent_in_past(read,roots[i],k,p);
                    assert(ov(p));
                } else {
                    let seed=if ov(k) {
                        assert(summary.seeds.contains(k));
                        root_in_full(full,k);
                        k
                    } else {
                        choose|s:K| #[trigger] summary.seeds.contains(s) && reached(full,seq![s],k)
                    };
                    follow(full,seq![seed],k,p);
                    assert(ap(p));
                }
            }
        }
    }
    assert forall|k:K| #[trigger] reached(full,roots,k) <==> (
        if actor_seq(read,k).is_some() { ov(k) } else { ap(k) }) by {
        if reached(full,roots,k) {
            assert(joined.contains(k));
            if ov(k) && actor_seq(read,k).is_none() {
                assert(summary.seeds.contains(k));
                root_in_full(full,k);
                assert(ap(k));
            }
        }
    }
}

} // verus!

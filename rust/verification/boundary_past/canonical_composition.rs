use crate::admission_spec::{canonical, Fragments};
use crate::walk_spec::{contains_walk, expands, keeps, reached, walk_result, Graph};
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

// Scope is the exact full past of the ready head. The full graph never
// stops inside it, and all its original edges point down the DAG rank.
pub(crate) open spec fn ranked_scope<K>(full:Graph<K>,head:K,scope:Set<K>,rank:spec_fn(K)->nat)->bool {
    &&& scope.contains(head)
    &&& forall|k:K| #[trigger] scope.contains(k) ==> reached(full,seq![head],k)
        && expands((full.visit)(k)) && (full.parents)(k).to_set().subset_of(scope)
        && forall|p:K| #[trigger] (full.parents)(k).contains(p) ==> rank(p)<rank(k)
}

pub(crate) open spec fn descriptor<K>(full:Graph<K>,scope:Set<K>,head:K,level:spec_fn(K)->nat,rank:spec_fn(K)->nat,boundary:Seq<K>,members:Seq<K>)->Fragments<K> {
    Fragments {
        past: |h:K| scope.filter(|k:K| reached(full,seq![h],k)),
        level, rank,
        members: |h:K| if h==head {members.to_set()} else {Set::empty()},
        boundary: |h:K| if h==head {boundary.to_set()} else {Set::empty()},
    }
}

proof fn follows<K>(full:Graph<K>,roots:Seq<K>,node:K,parent:K)
    requires reached(full,roots,node),expands((full.visit)(node)),(full.parents)(node).contains(parent),
    ensures reached(full,roots,parent),
{
    assert forall|s:Set<K>| #[trigger] contains_walk(full,roots,s) implies s.contains(parent) by { assert(s.contains(node)); }
}

proof fn roots_are_reached<K>(graph:Graph<K>,roots:Seq<K>,key:K)
    requires roots.contains(key),ensures reached(graph,roots,key),
{
    assert forall|s:Set<K>| #[trigger] contains_walk(graph,roots,s) implies s.contains(key) by {};
}

proof fn past_is_union<K>(full:Graph<K>,scope:Set<K>,roots:Seq<K>)
    requires contains_walk(full,roots,scope),forall|k:K| #[trigger] scope.contains(k) ==> expands((full.visit)(k)),
    ensures forall|k:K| #[trigger] reached(full,roots,k) <==> (scope.contains(k) && exists|h:K| #[trigger] roots.contains(h) && reached(full,seq![h],k)),
{
    let covered=scope.filter(|k:K| exists|h:K| #[trigger] roots.contains(h) && reached(full,seq![h],k));
    assert(roots.to_set().subset_of(covered)) by {
        assert forall|h:K| #[trigger] roots.contains(h) implies covered.contains(h) by { roots_are_reached(full,seq![h],h); }
    }
    assert(contains_walk(full,roots,covered)) by {
        assert forall|k:K| #[trigger] covered.contains(k) && expands((full.visit)(k)) implies
            (full.parents)(k).to_set().subset_of(covered) by {
            let h=choose|h:K| #[trigger] roots.contains(h) && reached(full,seq![h],k);
            assert forall|p:K| #[trigger] (full.parents)(k).contains(p) implies covered.contains(p) by {
                follows(full,seq![h],k,p);
            }
        }
    }
    assert forall|k:K| #[trigger] reached(full,roots,k) <==> covered.contains(k) by {
        if covered.contains(k) {
            let h=choose|h:K| #[trigger] roots.contains(h) && reached(full,seq![h],k);
            assert forall|s:Set<K>| #[trigger] contains_walk(full,roots,s) implies s.contains(k) by {
                assert(contains_walk(full,seq![h],s));
            }
        }
    }
}

proof fn members_minus_cut<K>(full:Graph<K>,body:Graph<K>,scope:Set<K>,head:K,boundary:Seq<K>,members:Seq<K>)
    requires scope.contains(head),contains_walk(full,boundary,scope),contains_walk(full,seq![head],scope),
        !reached(full,boundary,head),walk_result(body,seq![head],members),
        forall|k:K| #[trigger] scope.contains(k) ==> expands((full.visit)(k))
            && (expands((body.visit)(k)) <==> !reached(full,boundary,k))
            && (keeps((body.visit)(k)) <==> !reached(full,boundary,k))
            && (expands((body.visit)(k)) ==> (body.parents)(k).to_set()==(full.parents)(k).to_set()),
    ensures forall|k:K| #[trigger] members.contains(k) <==> (reached(full,seq![head],k) && !reached(full,boundary,k)),
{
    let roots=seq![head];
    let excluded=scope.filter(|k:K| reached(full,boundary,k));
    assert(contains_walk(body,roots,scope));
    assert forall|k:K| #[trigger] excluded.contains(k) implies (full.parents)(k).to_set().subset_of(excluded) by {
        assert forall|p:K| #[trigger] (full.parents)(k).contains(p) implies excluded.contains(p) by {
            follows(full,boundary,k,p);
        }
    }
    assert forall|k:K| #[trigger] members.contains(k) <==> (reached(full,roots,k) && !reached(full,boundary,k)) by {
        if members.contains(k) {
            assert(scope.contains(k));
            assert forall|s:Set<K>| #[trigger] contains_walk(full,roots,s) implies s.contains(k) by {
                let candidate=s.intersect(scope);
                assert(contains_walk(body,roots,candidate));
                assert(candidate.contains(k));
            }
        }
        if reached(full,roots,k) && !reached(full,boundary,k) {
            assert(scope.contains(k));
            assert forall|s:Set<K>| #[trigger] contains_walk(body,roots,s) implies s.contains(k) by {
                let total=s.union(excluded).intersect(scope);
                assert(contains_walk(full,roots,total)) by {
                    assert forall|v:K| #[trigger] total.contains(v) && expands((full.visit)(v)) implies
                        (full.parents)(v).to_set().subset_of(total) by {
                        if !excluded.contains(v) { assert((body.parents)(v).to_set().subset_of(s)); }
                    }
                }
                assert(total.contains(k));
            }
        }
    }
}

proof fn finalize_canonical<K>(full:Graph<K>,scope:Set<K>,head:K,level:spec_fn(K)->nat,rank:spec_fn(K)->nat,boundary:Seq<K>,members:Seq<K>)
    requires ranked_scope(full,head,scope,rank),
        forall|b:K| #[trigger] boundary.contains(b) ==> scope.contains(b) && level(b)>=level(head) && rank(b)<rank(head),
        forall|k:K| #[trigger] members.contains(k) ==> k==head || level(k)<level(head),
        forall|k:K| #[trigger] members.contains(k) <==> (scope.contains(k) && !reached(full,boundary,k)),
        forall|k:K| #[trigger] reached(full,boundary,k) <==> (scope.contains(k)
            && exists|b:K| #[trigger] boundary.contains(b) && reached(full,seq![b],k)),
    ensures canonical(descriptor(full,scope,head,level,rank,boundary,members),head),
{
    let result=descriptor(full,scope,head,level,rank,boundary,members);
    assert((result.past)(head) =~= scope);
    let m=(result.members)(head);
    let b=(result.boundary)(head);
    assert forall|k:K| #[trigger] m.contains(k) <==> (result.past)(head).contains(k)
        && !(exists|x:K| #[trigger] b.contains(x) && (result.past)(x).contains(k)) by {
        assert(members.contains(k)==m.contains(k));
        if exists|x:K| #[trigger] b.contains(x) && (result.past)(x).contains(k) {
            let x=choose|x:K| #[trigger] b.contains(x) && (result.past)(x).contains(k);
            assert(boundary.contains(x));
            assert(reached(full,seq![x],k));
            assert(reached(full,boundary,k));
        }
        if reached(full,boundary,k) {
            let x=choose|x:K| #[trigger] boundary.contains(x) && reached(full,seq![x],k);
            assert(b.contains(x) && (result.past)(x).contains(k));
        }
    }
}

/// Compose checked boundary/member walks and the exact cutoff. The member
/// level premise comes from the intrinsic level check and delivered-set
/// equality, not from assuming canonical membership.
pub(crate) proof fn hard_descriptor_is_canonical<K>(full:Graph<K>,bounds:Graph<K>,body:Graph<K>,scope:Set<K>,head:K,
    level:spec_fn(K)->nat,rank:spec_fn(K)->nat,start:Seq<K>,boundary:Seq<K>,members:Seq<K>)
    requires ranked_scope(full,head,scope,rank),level(head)>0,start.to_set()==(full.parents)(head).to_set(),
        walk_result(bounds,start,boundary),walk_result(body,seq![head],members),
        forall|k:K| #[trigger] scope.contains(k) ==> (keeps((bounds.visit)(k)) <==> level(k)>=level(head))
            && (expands((bounds.visit)(k)) ==> (bounds.parents)(k).to_set()==(full.parents)(k).to_set()),
        forall|k:K| #[trigger] scope.contains(k) ==> (expands((body.visit)(k)) <==> (k==head || !reached(full,boundary,k)))
            && (keeps((body.visit)(k)) <==> (k==head || !reached(full,boundary,k)))
            && (expands((body.visit)(k)) ==> (body.parents)(k).to_set()==(full.parents)(k).to_set()),
        forall|k:K| #[trigger] members.contains(k) ==> k==head || level(k)<level(head),
    ensures canonical(descriptor(full,scope,head,level,rank,boundary,members),head),
{
    let below=scope.filter(|k:K| rank(k)<rank(head));
    assert(start.to_set().subset_of(below));
    assert(contains_walk(bounds,start,below));
    assert forall|k:K| #[trigger] boundary.contains(k) implies below.contains(k) && level(k)>=level(head) by {};
    assert(contains_walk(full,boundary,below));
    assert(!reached(full,boundary,head));
    assert(contains_walk(full,boundary,scope));
    assert(seq![head].contains(head));
    assert(contains_walk(full,seq![head],scope));
    assert forall|k:K| #[trigger] scope.contains(k) implies (expands((body.visit)(k)) <==> !reached(full,boundary,k))
        && (keeps((body.visit)(k)) <==> !reached(full,boundary,k)) by {};
    members_minus_cut(full,body,scope,head,boundary,members);
    past_is_union(full,scope,boundary);
    finalize_canonical(full,scope,head,level,rank,boundary,members);
}

pub(crate) proof fn loose_descriptor_is_canonical<K>(full:Graph<K>,scope:Set<K>,head:K,level:spec_fn(K)->nat,rank:spec_fn(K)->nat,boundary:Seq<K>)
    requires ranked_scope(full,head,scope,rank),level(head)==0,boundary.to_set()==(full.parents)(head).to_set(),
    ensures canonical(descriptor(full,scope,head,level,rank,boundary,seq![head]),head),
{
    let below=scope.filter(|k:K| rank(k)<rank(head));
    assert(contains_walk(full,boundary,below));
    assert(!reached(full,boundary,head));
    assert(contains_walk(full,boundary,scope));
    past_is_union(full,scope,boundary);
    let ancestors=scope.filter(|k:K| reached(full,boundary,k));
    let all=ancestors.insert(head);
    assert(contains_walk(full,seq![head],all)) by {
        assert(seq![head].contains(head));
        assert forall|k:K| #[trigger] all.contains(k) && expands((full.visit)(k)) implies (full.parents)(k).to_set().subset_of(all) by {
            assert forall|p:K| #[trigger] (full.parents)(k).contains(p) implies ancestors.contains(p) by {
                if k==head {
                    assert(boundary.to_set().contains(p));
                    assert(boundary.contains(p));
                    roots_are_reached(full,boundary,p);
                } else {follows(full,boundary,k,p);}
            }
        }
    }
    roots_are_reached(full,seq![head],head);
    assert forall|k:K| #[trigger] reached(full,seq![head],k) && !reached(full,boundary,k) implies k==head by {
        assert(all.contains(k));
    }
    let members=seq![head];
    assert forall|k:K| #[trigger] members.contains(k) <==> (scope.contains(k) && !reached(full,boundary,k)) by {
        if members.contains(k) { assert(k==head); }
    }
    finalize_canonical(full,scope,head,level,rank,boundary,members);
}

} // verus!

use crate::intrinsic_shape::LevelFault;
use crate::lookup_spec::{model, modeled};
use std::hash::Hash;
use vstd::laws_eq::obeys_concrete_eq;
use vstd::prelude::*;
use vstd::std_specs::hash::obeys_key_model;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

pub(crate) open spec fn hash_laws<K: Eq + Hash>() -> bool { obeys_key_model::<K>() && obeys_concrete_eq::<K>() }

pub(crate) open spec fn edge_view<'a,K:'a,P:Fn(K)->&'a[K],G:Fn(K)->bool>(members:Seq<K>, parents:P, in_group:G) -> bool {
    &&& forall|i:int| 0 <= i < members.len() ==> modeled(parents, #[trigger] members[i])
    &&& forall|p:K| #[trigger] modeled(in_group, p)
    &&& forall|p:K| #[trigger] model(in_group, p) == members.contains(p)
}

pub(crate) open spec fn dependent<'a,K:'a,P:Fn(K)->&'a[K]>(members:Seq<K>, parents:P, end:int, p:K) -> bool {
    members.contains(p) && exists|i:int| 0 <= i < end && model(parents, #[trigger] members[i])@.contains(p)
}

pub(crate) open spec fn marked<'a,K:'a,P:Fn(K)->&'a[K]>(members:Seq<K>, parents:P, end:int, seen:Set<K>) -> bool {
    forall|p:K| #[trigger] seen.contains(p) <==> dependent(members, parents, end, p)
}

pub(crate) open spec fn row_prefix<K>(members:Seq<K>, deps:Seq<K>, end:int, prior:Set<K>, seen:Set<K>) -> bool {
    forall|p:K| #[trigger] seen.contains(p) <==> prior.contains(p) || (members.contains(p) && deps.take(end).contains(p))
}

pub(crate) proof fn mark_one<K>(members:Seq<K>, deps:Seq<K>, j:int, prior:Set<K>, before:Set<K>, after:Set<K>)
    requires 0 <= j < deps.len(), row_prefix(members, deps, j, prior, before),
        after == if members.contains(deps[j]) { before.insert(deps[j]) } else { before },
    ensures row_prefix(members, deps, j+1, prior, after),
{
    deps.lemma_take_succ_push(j);
}

pub(crate) proof fn end_row<'a,K:'a,P:Fn(K)->&'a[K]>(members:Seq<K>, parents:P, i:int, prior:Set<K>, seen:Set<K>)
    requires 0 <= i < members.len(), marked(members, parents, i, prior),
        row_prefix(members, model(parents,members[i])@, model(parents,members[i])@.len() as int, prior, seen),
    ensures marked(members, parents, i+1, seen),
{
    let deps = model(parents,members[i])@;
    assert(deps.take(deps.len() as int) =~= deps);
    assert forall|p:K| #[trigger] seen.contains(p) <==> dependent(members,parents,i+1,p) by {
        assert(seen.contains(p) <==> prior.contains(p) || (members.contains(p) && deps.contains(p)));
    }
}

pub(crate) open spec fn sole_unmarked<K>(members:Seq<K>, seen:Set<K>, end:int, head:Option<K>) -> bool {
    &&& (match head {
        None => true,
        Some(h) => !seen.contains(h) && exists|i:int| 0 <= i < end && #[trigger] members[i] == h,
    })
    &&& forall|i:int| 0 <= i < end && !seen.contains(#[trigger] members[i]) ==> head == Some(members[i])
}

pub(crate) open spec fn sole_head<'a,K:'a,P:Fn(K)->&'a[K]>(members:Seq<K>, parents:P, h:K) -> bool {
    &&& members.contains(h)
    &&& !dependent(members, parents, members.len() as int, h)
    &&& forall|k:K| #[trigger] members.contains(k) && !dependent(members, parents, members.len() as int, k) ==> k == h
}

pub(crate) open spec fn head_result<'a,K:'a,P:Fn(K)->&'a[K]>(members:Seq<K>, parents:P, head:Option<K>) -> bool {
    match head {
        None => forall|h:K| !#[trigger] sole_head(members, parents, h),
        Some(h) => sole_head(members, parents, h),
    }
}

pub(crate) proof fn two_heads<'a,K:'a,P:Fn(K)->&'a[K]>(members:Seq<K>, parents:P, seen:Set<K>, i:int, head:Option<K>)
    requires marked(members, parents, members.len() as int, seen), members.no_duplicates(),
        0 <= i < members.len(), !seen.contains(members[i]), head.is_some(), sole_unmarked(members, seen, i, head),
    ensures head_result(members, parents, None),
{
    let prev = head.unwrap();
    let j = choose|j:int| 0 <= j < i && members[j] == prev;
    assert(members[i] != prev);
    assert(members.contains(prev));
    assert(members.contains(members[i]));
}

pub(crate) proof fn end_heads<'a,K:'a,P:Fn(K)->&'a[K]>(members:Seq<K>, parents:P, seen:Set<K>, head:Option<K>)
    requires marked(members, parents, members.len() as int, seen), sole_unmarked(members, seen, members.len() as int, head),
    ensures head_result(members, parents, head),
{
    assert forall|k:K| #[trigger] members.contains(k) && !seen.contains(k) implies head == Some(k) by {
        let i = choose|i:int| 0 <= i < members.len() && members[i] == k;
        assert(head == Some(members[i]));
    }
    if let Some(h) = head {
        let i = choose|i:int| 0 <= i < members.len() && members[i] == h;
        assert(members.contains(h));
    }
}

pub(crate) open spec fn level_prefix<K,F:Fn(K)->usize>(members:Seq<K>, head:K, level:F, end:int) -> bool {
    forall|i:int| 0 <= i < end && #[trigger] members[i] != head ==> model(level, members[i]) < model(level, head)
}

pub(crate) open spec fn level_result<K,F:Fn(K)->usize>(members:Seq<K>, head:K, level:F, result:Result<(),LevelFault<K>>) -> bool {
    match result {
        Ok(_) => if model(level,head)==0 { members.len()==1 } else { level_prefix(members,head,level,members.len() as int) },
        Err(LevelFault::NonSingletonLoose) => model(level,head)==0 && members.len()!=1,
        Err(LevelFault::Member(k)) => model(level,head)>0 && members.contains(k) && k!=head && model(level,k)>=model(level,head),
    }
}

} // verus!

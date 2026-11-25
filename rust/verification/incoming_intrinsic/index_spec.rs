use crate::intrinsic_index::{Fault, Member};
use crate::lookup_spec::{model, modeled};
use std::hash::Hash;
use vstd::laws_eq::obeys_concrete_eq;
use vstd::prelude::*;
use vstd::std_specs::hash::obeys_key_model;

verus! {

pub(crate) open spec fn laws<A: Eq + Hash, H: Eq + Hash>() -> bool {
    &&& obeys_key_model::<H>()
    &&& obeys_concrete_eq::<H>()
    &&& obeys_key_model::<(A, u64)>()
    &&& obeys_concrete_eq::<(A, u64)>()
}

pub(crate) open spec fn observations<'a, M: 'a, A, H, F: Fn(&'a M) -> Member<A, H>>(changes: Seq<M>, project: F) -> bool {
    &&& changes.len() <= usize::MAX
    &&& forall|i: int| 0 <= i < changes.len() ==> #[trigger] modeled(project, &changes[i])
}

pub(crate) open spec fn key<A, H>(row: Member<A, H>) -> (A, u64) { (row.actor, row.seq) }

pub(crate) open spec fn intrinsic<A,H>(row: Member<A,H>) -> bool {
    &&& 0 < row.seq <= u32::MAX
    &&& row.has_author ==> row.seq == 1
    &&& !row.conflicts_with_applied
}

pub(crate) open spec fn issue<'a, M:'a, A,H>(row: Member<A,H>, members: Map<H,&'a M>, batch: Map<(A,u64),H>) -> Option<Fault> {
    if !(0 < row.seq <= u32::MAX) { Some(Fault::Sequence) }
    else if row.has_author && row.seq != 1 { Some(Fault::MisplacedAuthor) }
    else if members.contains_key(row.hash) { Some(Fault::DuplicateHash) }
    else if row.conflicts_with_applied || (batch.contains_key(key(row)) && batch[key(row)] != row.hash) {
        Some(Fault::ConflictingSlot)
    } else { None }
}

pub(crate) open spec fn records<'a,M:'a,A,H>(members: Map<H,&'a M>, batch: Map<(A,u64),H>, row: Member<A,H>, value: &'a M) -> bool {
    &&& intrinsic(row)
    &&& members.contains_key(row.hash)
    &&& members[row.hash] == value
    &&& batch.contains_key(key(row))
    &&& batch[key(row)] == row.hash
}

// This prefix has unique hashes and no actor-slot conflicts. The two maps
// contain precisely the indexed members and the accumulated slot bindings.
pub(crate) open spec fn indexed<'a, M:'a,A,H,F:Fn(&'a M)->Member<A,H>>(
    changes: Seq<M>, project: F, end: int, members: Map<H,&'a M>, initial: Map<(A,u64),H>, batch: Map<(A,u64),H>,
) -> bool {
    &&& 0 <= end <= changes.len()
    &&& forall|i:int| 0 <= i < end ==> records(members, batch, #[trigger] model(project, &changes[i]), &changes[i])
    &&& forall|i:int,j:int| 0 <= i < j < end ==> #[trigger] model(project, &changes[i]).hash != #[trigger] model(project, &changes[j]).hash
    &&& forall|h:H| #[trigger] members.contains_key(h) ==> exists|i:int| 0 <= i < end
        && #[trigger] model(project, &changes[i]).hash == h
    &&& forall|k:(A,u64)| #[trigger] initial.contains_key(k) ==> batch.contains_key(k) && batch[k] == initial[k]
    &&& forall|k:(A,u64)| #[trigger] batch.contains_key(k) && !initial.contains_key(k) ==> exists|i:int| 0 <= i < end
        && key(#[trigger] model(project, &changes[i])) == k
}

pub(crate) open spec fn index_result<'a,M:'a,A:Eq+Hash,H:Eq+Hash,F:Fn(&'a M)->Member<A,H>>(
    changes:Seq<M>, project:F, initial:Map<(A,u64),H>, batch:Map<(A,u64),H>, result:Result<std::collections::HashMap<H,&'a M>,(usize,Fault)>,
) -> bool {
    match result {
        Ok(members) => indexed(changes, project, changes.len() as int, members@, initial, batch),
        Err((index, fault)) => index < changes.len() && exists|prior: Map<H,&'a M>|
            #[trigger] indexed(changes, project, index as int, prior, initial, batch)
                && issue(model(project, &changes[index as int]), prior, batch) == Some(fault),
    }
}

pub(crate) proof fn stop<'a,M:'a,A:Eq+Hash,H:Eq+Hash,F:Fn(&'a M)->Member<A,H>>(
    changes:Seq<M>, project:F, i:int, members:Map<H,&'a M>, initial:Map<(A,u64),H>, batch:Map<(A,u64),H>, fault:Fault,
)
    requires 0 <= i < changes.len() <= usize::MAX, indexed(changes, project, i, members, initial, batch),
        issue(model(project, &changes[i]), members, batch) == Some(fault),
    ensures index_result(changes, project, initial, batch, Err((i as usize, fault))),
{}

pub(crate) proof fn extend<'a,M:'a,A,H,F:Fn(&'a M)->Member<A,H>>(
    changes:Seq<M>, project:F, i:int, before:Map<H,&'a M>, members:Map<H,&'a M>,
    initial:Map<(A,u64),H>, old_batch:Map<(A,u64),H>, batch:Map<(A,u64),H>,
)
    requires 0 <= i < changes.len(), indexed(changes, project, i, before, initial, old_batch),
        issue(model(project, &changes[i]), before, old_batch).is_none(),
        members == before.insert(model(project, &changes[i]).hash, &changes[i]),
        batch == old_batch.insert(key(model(project, &changes[i])), model(project, &changes[i]).hash),
    ensures indexed(changes, project, i + 1, members, initial, batch),
{
    let new = model(project, &changes[i]);
    assert forall|j:int| 0 <= j < i+1 implies records(members, batch, #[trigger] model(project, &changes[j]), &changes[j]) by {
        if j < i {
            assert(records(before, old_batch, model(project, &changes[j]), &changes[j]));
            assert(model(project, &changes[j]).hash != new.hash);
        }
    }
    assert forall|j:int,k:int| 0 <= j < k < i+1 implies
        #[trigger] model(project, &changes[j]).hash != #[trigger] model(project, &changes[k]).hash by {
        if k == i { assert(before.contains_key(model(project, &changes[j]).hash)); }
    }
    assert forall|h:H| #[trigger] members.contains_key(h) implies exists|j:int| 0 <= j < i+1
        && #[trigger] model(project, &changes[j]).hash == h by {
        if h != new.hash { assert(before.contains_key(h)); }
    }
    assert forall|k:(A,u64)| #[trigger] batch.contains_key(k) && !initial.contains_key(k)
        implies exists|j:int| 0 <= j < i+1 && key(#[trigger] model(project, &changes[j])) == k by {
        if k != key(new) { assert(old_batch.contains_key(k)); }
    }
}

} // verus!

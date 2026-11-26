//! Abstract incoming admission contract. This file defines the result to
//! be proved, not how to compute it. Changes, deliveries, witnesses, and
//! document content are abstract values. The result predicates are
//! `aborts`, `publishes`, and `retains_pending`.

use vstd::prelude::*;

verus! {

pub ghost struct History<Change, Witness, Content> {
    pub applied: Set<Change>,
    pub accepted: Set<Change>,
    pub evidence: Set<Witness>,
    pub content: Content,
}

/// A canonical fragment with its own verified witnesses.
pub ghost struct Fragment<Change, Witness> {
    pub members: Set<Change>,
    pub boundary: Set<Change>,
    pub evidence: Set<Witness>,
}

/// Occurrences are distinct even when their fragment contents are equal.
pub ghost struct Selection<Occurrence, Change, Witness> {
    pub verified: Map<Occurrence, Fragment<Change, Witness>>,
    pub chosen: Set<Occurrence>,
}

pub open spec fn supplies<Id, Change, Witness>(
    verified: Map<Id, Fragment<Change, Witness>>, chosen: Set<Id>, change: Change,
) -> bool {
    exists|id: Id| chosen.contains(id) && verified[id].members.contains(change)
}

pub open spec fn closed<Id, Change, Witness>(
    verified: Map<Id, Fragment<Change, Witness>>, chosen: Set<Id>, accepted: Set<Change>,
) -> bool {
    &&& chosen.subset_of(verified.dom())
    &&& forall|id: Id, boundary: Change|
        chosen.contains(id) && verified[id].boundary.contains(boundary)
            ==> accepted.contains(boundary) || supplies(verified, chosen, boundary)
}

pub open spec fn greatest<Id, Change, Witness>(
    selection: Selection<Id, Change, Witness>, accepted: Set<Change>,
) -> bool {
    &&& closed(selection.verified, selection.chosen, accepted)
    &&& forall|other: Set<Id>| closed(selection.verified, other, accepted)
        ==> other.subset_of(selection.chosen)
}

pub open spec fn legal_schedule<Change>(
    apply: Seq<Change>, selected: Set<Change>, applied: Set<Change>,
    parents: spec_fn(Change) -> Set<Change>,
) -> bool {
    &&& apply.no_duplicates()
    &&& apply.to_set() == selected.difference(applied)
    &&& forall|i: int| 0 <= i < apply.len() ==>
        parents(apply[i]).subset_of(applied.union(apply.take(i).to_set()))
}

/// Aborting preserves the whole abstract document, not the caller's log.
pub open spec fn aborts<Document>(before: Document, after: Document) -> bool {
    before == after
}

/// Admission applies and accepts exactly the selection. Reclamation may
/// remove evidence, but may not introduce a witness from another source.
pub open spec fn publishes<Id, Change, Witness, Content>(
    before: History<Change, Witness, Content>, after: History<Change, Witness, Content>,
    selection: Selection<Id, Change, Witness>, selected: Set<Change>, apply: Seq<Change>,
    parents: spec_fn(Change) -> Set<Change>, import: spec_fn(Content, Seq<Change>) -> Content,
) -> bool {
    &&& greatest(selection, before.accepted)
    &&& forall|change: Change| selected.contains(change)
        <==> supplies(selection.verified, selection.chosen, change)
    &&& legal_schedule(apply, selected, before.applied, parents)
    &&& after.applied == before.applied.union(apply.to_set())
    &&& after.accepted == before.accepted.union(selected)
    &&& after.content == import(before.content, apply)
    &&& forall|w: Witness| after.evidence.contains(w) ==>
        before.evidence.contains(w)
        || (exists|id: Id| selection.chosen.contains(id)
            && selection.verified[id].evidence.contains(w))
}

/// `order` distinguishes the original delivery occurrences. `blocked`
/// identifies the occurrences still eligible for the pending queue.
pub ghost struct Queue<Occurrence, Delivery> {
    pub order: Seq<Occurrence>,
    pub deliveries: Map<Occurrence, Delivery>,
    pub blocked: Set<Occurrence>,
}

pub open spec fn first_viable<Id, Delivery>(
    queue: Queue<Id, Delivery>, id: Id, viable: spec_fn(Id) -> bool, same: spec_fn(Id, Id) -> bool,
) -> bool {
    &&& queue.order.contains(id) && queue.blocked.contains(id) && viable(id)
    &&& !(exists|earlier: Id| {
        &&& queue.order.contains(earlier)
        &&& queue.order.index_of(earlier) < queue.order.index_of(id)
        &&& queue.blocked.contains(earlier) && viable(earlier) && same(earlier, id)
    })
}

/// After publication, retain exactly the first viable blocked copies, in
/// delivery order. `same` compares delivery identity, including attachment.
pub open spec fn retains_pending<Id, Delivery>(
    queue: Queue<Id, Delivery>, viable: spec_fn(Id) -> bool, same: spec_fn(Id, Id) -> bool,
    pending: Seq<Delivery>,
) -> bool {
    &&& queue.order.no_duplicates() && queue.deliveries.dom() == queue.order.to_set()
    &&& exists|kept: Seq<Id>| {
        &&& kept.no_duplicates()
        &&& forall|id: Id| kept.contains(id) <==> first_viable(queue, id, viable, same)
        &&& forall|i: int, j: int| 0 <= i < j < kept.len()
            ==> queue.order.index_of(kept[i]) < queue.order.index_of(kept[j])
        &&& pending == kept.map_values(|id: Id| queue.deliveries[id])
    }
}

} // verus!

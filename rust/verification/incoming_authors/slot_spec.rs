use crate::spec::model;
use vstd::prelude::*;

verus! {

pub(crate) open spec fn conflicts<K, S, F: Fn(K) -> (S, bool)>(nodes: Seq<K>, slot: F, i: int) -> bool {
    let (key, applied) = model(slot, nodes[i]);
    applied || exists|j: int| 0 <= j < i && model(slot, #[trigger] nodes[j]).0 == key
}

pub(crate) open spec fn consistent<K, S, F: Fn(K) -> (S, bool)>(nodes: Seq<K>, slot: F, end: int) -> bool {
    forall|i: int| 0 <= i < end ==> !#[trigger] conflicts(nodes, slot, i)
}

pub(crate) open spec fn conflict_result<K, S, F: Fn(K) -> (S, bool)>(nodes: Seq<K>, slot: F, conflict: Option<K>) -> bool {
    match conflict {
        None => consistent(nodes, slot, nodes.len() as int),
        Some(hash) => exists|i: int| 0 <= i < nodes.len() && #[trigger] nodes[i] == hash
            && conflicts(nodes, slot, i) && consistent(nodes, slot, i),
    }
}

pub(crate) open spec fn remembers<K, S, F: Fn(K) -> (S, bool)>(nodes: Seq<K>, slot: F, end: int, seen: Set<S>) -> bool {
    forall|key: S| #[trigger] seen.contains(key) <==> exists|j: int| 0 <= j < end
        && model(slot, #[trigger] nodes[j]).0 == key
}

pub(crate) proof fn report_conflict<K, S, F: Fn(K) -> (S, bool)>(
    nodes: Seq<K>, slot: F, i: int, seen: Set<S>, applied: bool,
)
    requires 0 <= i < nodes.len(), consistent(nodes, slot, i), remembers(nodes, slot, i, seen),
        applied == model(slot, nodes[i]).1, applied || seen.contains(model(slot, nodes[i]).0),
    ensures conflict_result(nodes, slot, Some(nodes[i])),
{
    assert(conflicts(nodes, slot, i));
}

pub(crate) proof fn no_conflict<K, S, F: Fn(K) -> (S, bool)>(
    nodes: Seq<K>, slot: F, i: int, seen: Set<S>, next: Set<S>,
)
    requires 0 <= i < nodes.len(), consistent(nodes, slot, i), remembers(nodes, slot, i, seen),
        !seen.contains(model(slot, nodes[i]).0), !model(slot, nodes[i]).1,
        next == seen.insert(model(slot, nodes[i]).0),
    ensures consistent(nodes, slot, i + 1), remembers(nodes, slot, i + 1, next),
{
    assert(!conflicts(nodes, slot, i));
}

// A successful batch-wide scan permits skipping the narrower ancestry scan.
// If the batch conflicts, nothing follows about an individual ancestry.
pub(crate) proof fn consistency_is_inherited<K, S, F: Fn(K) -> (S, bool)>(
    all: Seq<K>, subset: Seq<K>, slot: F,
)
    requires consistent(all, slot, all.len() as int), subset.no_duplicates(),
        forall|i: int| 0 <= i < subset.len() ==> all.contains(#[trigger] subset[i]),
    ensures consistent(subset, slot, subset.len() as int),
{
    assert forall|i: int| 0 <= i < subset.len() implies !#[trigger] conflicts(subset, slot, i) by {
        let p = choose|p: int| 0 <= p < all.len() && all[p] == subset[i];
        assert(!conflicts(all, slot, p));
        if conflicts(subset, slot, i) {
            let j = choose|j: int| 0 <= j < i && model(slot, #[trigger] subset[j]).0 == model(slot, subset[i]).0;
            assert(subset[i] != subset[j]);
            let q = choose|q: int| 0 <= q < all.len() && all[q] == subset[j];
            if q < p {
                assert(conflicts(all, slot, p));
            } else {
                assert(q > p);
                assert(conflicts(all, slot, q));
            }
        }
    }
}

} // verus!

use crate::spec::*;
use vstd::prelude::*;

verus! {

broadcast use {vstd::seq_lib::group_seq_properties, vstd::set::group_set_lemmas};

pub(crate) open spec fn max_prefix<A>(
    source: Map<A, u64>, entries: Seq<(&A, &u64)>, end: int, base: Map<A, u64>, merged: Map<A, u64>,
) -> bool {
    forall|a: A| #[trigger] value(merged, a) == maximum(value(base, a),
        if source.contains_key(a) && entries.take(end).contains((&a, &source[a])) { Some(source[a]) }
        else { None })
}

pub(crate) proof fn max_step<A>(
    source: Map<A, u64>, entries: Seq<(&A, &u64)>, i: int,
    base: Map<A, u64>, prior: Map<A, u64>, after: Map<A, u64>,
)
    requires 0 <= i < entries.len(), max_prefix(source, entries, i, base, prior),
        source.contains_key(*entries[i].0), source[*entries[i].0] == *entries[i].1,
        max_join(prior, Map::empty().insert(*entries[i].0, *entries[i].1), after),
    ensures max_prefix(source, entries, i + 1, base, after),
{
    entries.lemma_take_succ_push(i);
    assert forall|a: A| #[trigger] value(after, a) == maximum(value(base, a),
        if source.contains_key(a) && entries.take(i + 1).contains((&a, &source[a])) { Some(source[a]) } else { None }) by {
        if a == *entries[i].0 {
            assert(entries.take(i + 1).contains((&a, &source[a])));
            assert(value(after, a) == maximum(value(prior, a), Some(source[a])));
            assert(maximum(maximum(value(base, a), Some(source[a])), Some(source[a])) == maximum(value(base, a), Some(source[a])));
        } else {
            assert(entries.take(i + 1).contains((&a, &source[a])) == entries.take(i).contains((&a, &source[a])));
        }
    }
}

pub(crate) proof fn seed_step<K>(base: Set<K>, entries: Seq<&K>, i: int, before: Set<K>, after: Set<K>)
    requires 0 <= i < entries.len(), before == base.union(entries.take(i).unref().to_set()),
        after == before.insert(*entries[i]),
    ensures after =~= base.union(entries.take(i + 1).unref().to_set()),
{
    entries.lemma_take_succ_push(i);
    assert(entries.take(i + 1).unref() =~= entries.take(i).unref().push(*entries[i]));
}

} // verus!

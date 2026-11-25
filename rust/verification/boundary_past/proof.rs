use crate::boundary_summary::Contribution;
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

pub(crate) proof fn aggregate_step<'a, A: 'a, K: 'a, F: Fn(K) -> Contribution<'a, A, K>>(
    nodes: Seq<K>, project: F, i: int, before: SummaryView<A, K>, next: SummaryView<A, K>,
)
    requires 0 <= i < nodes.len(), aggregate(nodes, project, i, before),
        join(before, term(project, nodes[i]), next),
    ensures aggregate(nodes, project, i + 1, next),
{
    let last = term(project, nodes[i]);
    assert forall|j: int, a: A| 0 <= j < i + 1 && #[trigger] term(project, nodes[j]).maxima.contains_key(a)
        implies next.maxima.contains_key(a) && next.maxima[a] >= term(project, nodes[j]).maxima[a] by {
        assert(value(next.maxima, a) == maximum(value(before.maxima, a), value(last.maxima, a)));
    }
    assert forall|a: A| #[trigger] next.maxima.contains_key(a) implies exists|j: int| 0 <= j < i + 1
        && #[trigger] term(project, nodes[j]).maxima.contains_key(a)
        && term(project, nodes[j]).maxima[a] == next.maxima[a] by {
        assert(value(next.maxima, a) == maximum(value(before.maxima, a), value(last.maxima, a)));
        if before.maxima.contains_key(a) && before.maxima[a] == next.maxima[a] {
            let j = choose|j: int| 0 <= j < i && #[trigger] term(project, nodes[j]).maxima.contains_key(a)
                && term(project, nodes[j]).maxima[a] == before.maxima[a];
            assert(term(project, nodes[j]).maxima[a] == next.maxima[a]);
        } else {
            assert(last.maxima.contains_key(a) && last.maxima[a] == next.maxima[a]);
        }
    }
    assert forall|k: K| #[trigger] next.seeds.contains(k) <==> exists|j: int| 0 <= j < i + 1
        && #[trigger] term(project, nodes[j]).seeds.contains(k) by {
        assert(next.seeds.contains(k) <==> before.seeds.contains(k) || last.seeds.contains(k));
    }
}

} // verus!

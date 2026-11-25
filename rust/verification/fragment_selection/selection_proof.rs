use crate::proof::{partition_edges, split_tail, zero_edges};
use crate::selection_spec::*;
use crate::spec::candidate_edges;
use vstd::prelude::*;
use vstd::set_lib::{lemma_len_subset, range_set_properties};

verus! {

broadcast use {vstd::set::group_set_lemmas, vstd::seq_lib::group_seq_properties,
    vstd::seq_lib::group_to_multiset_ensures, vstd::multiset::group_multiset_axioms};

pub open spec fn impossible(members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, f: usize) -> bool {
    forall|choice: Set<usize>| #[trigger] closed(members, required_by, choice) ==> !choice.contains(f)
}

pub open spec fn safe_queue(members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, todo: Seq<usize>) -> bool {
    forall|f: usize| #[trigger] todo.contains(f) ==> f < members.len() && impossible(members, required_by, f)
}

pub open spec fn scheduled(
    required_by: Seq<Vec<usize>>, counts: Seq<usize>, todo: Seq<usize>, done: Seq<usize>, up_to: int,
) -> bool {
    forall|h: int, f: usize| 0 <= h < up_to && counts[h] == 0
        && #[trigger] required_by[h]@.contains(f) ==> done.contains(f) || todo.contains(f)
}

pub open spec fn mask(members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, excluded: Seq<bool>, done: Seq<usize>) -> bool {
    &&& excluded.len() == members.len()
    &&& done.no_duplicates()
    &&& done.len() <= members.len()
    &&& forall|f: usize| #[trigger] done.contains(f) ==> f < members.len() && impossible(members, required_by, f)
    &&& forall|f: usize| f < excluded.len() ==> (#[trigger] excluded[f as int] <==> done.contains(f))
}

pub open spec fn counters(members: Seq<Vec<usize>>, counts: Seq<usize>, done: Seq<usize>, tail: Seq<usize>) -> bool {
    forall|h: usize| h < counts.len() ==> #[trigger] counts[h as int] == candidate_edges(members, done, h)
        + tail.to_multiset().count(h)
}

pub open spec fn state(
    members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, counts: Seq<usize>, todo: Seq<usize>,
    excluded: Seq<bool>, done: Seq<usize>, tail: Seq<usize>,
) -> bool {
    &&& mask(members, required_by, excluded, done)
    &&& counters(members, counts, done, tail)
    &&& safe_queue(members, required_by, todo)
    &&& scheduled(required_by, counts, todo, done, counts.len() as int)
}

proof fn unsupplied(
    members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, done: Seq<usize>, h: usize, f: usize,
)
    requires members.len() <= usize::MAX, h < required_by.len(), required_by[h as int]@.contains(f),
        candidate_edges(members, done, h) == 0,
        forall|x: usize| #[trigger] done.contains(x) ==> impossible(members, required_by, x),
    ensures impossible(members, required_by, f),
{
    zero_edges(members, done, h);
    assert forall|choice: Set<usize>| #[trigger] closed(members, required_by, choice) implies !choice.contains(f) by {
        if choice.contains(f) {
            assert(supplies(members, choice, h));
            let p = choose|p: usize| p < members.len() && choice.contains(p)
                && #[trigger] members[p as int]@.contains(h);
            assert(done.contains(p));
            assert(impossible(members, required_by, p));
        }
    }
}

pub proof fn seed(
    members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, counts: Seq<usize>,
    before: Seq<usize>, todo: Seq<usize>, h: usize,
)
    requires well_formed(members, required_by, counts), h < counts.len(),
        safe_queue(members, required_by, before), scheduled(required_by, counts, before, Seq::empty(), h as int),
        todo == if counts[h as int] == 0 { before + required_by[h as int]@ } else { before },
    ensures safe_queue(members, required_by, todo), scheduled(required_by, counts, todo, Seq::empty(), h as int + 1),
{
    assert forall|f: usize| #[trigger] todo.contains(f) implies f < members.len()
        && impossible(members, required_by, f) by {
        if !before.contains(f) {
            let row = required_by[h as int]@;
            assert(row.contains(f));
            let i = choose|i: int| 0 <= i < row.len() && row[i] == f;
            assert(required_by[h as int]@[i] < members.len());
            unsupplied(members, required_by, Seq::empty(), h, f);
        }
    }
}

pub proof fn init(members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, counts: Seq<usize>, excluded: Seq<bool>)
    requires well_formed(members, required_by, counts), excluded.len() == members.len(),
        forall|i: int| 0 <= i < excluded.len() ==> !#[trigger] excluded[i],
    ensures mask(members, required_by, excluded, Seq::empty()), counters(members, counts, Seq::empty(), Seq::empty()),
{
    assert forall|h: usize| h < counts.len() implies #[trigger] counts[h as int]
        == candidate_edges(members, Seq::empty(), h) + Seq::<usize>::empty().to_multiset().count(h) by {
        assert(Seq::<usize>::empty().to_multiset().count(h) == 0);
    }
}

pub proof fn dequeue(members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, before: Seq<usize>, todo: Seq<usize>, f: usize)
    requires safe_queue(members, required_by, before), before =~= todo.push(f),
    ensures safe_queue(members, required_by, todo), f < members.len(), impossible(members, required_by, f),
{
    assert(before.contains(f));
    assert forall|g: usize| #[trigger] todo.contains(g) implies g < members.len()
        && impossible(members, required_by, g) by {
        assert(before.contains(g));
    }
}

pub proof fn already_done(required_by: Seq<Vec<usize>>, counts: Seq<usize>, before: Seq<usize>, todo: Seq<usize>, done: Seq<usize>, f: usize)
    requires scheduled(required_by, counts, before, done, counts.len() as int),
        before =~= todo.push(f), done.contains(f),
    ensures scheduled(required_by, counts, todo, done, counts.len() as int),
{}

pub proof fn start_exclusion(
    members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, counts: Seq<usize>, before: Seq<usize>, todo: Seq<usize>,
    old_mask: Seq<bool>, excluded: Seq<bool>, done: Seq<usize>, f: usize,
)
    requires members.len() <= usize::MAX, state(members, required_by, counts, before, old_mask, done, Seq::empty()),
        before =~= todo.push(f), f < members.len(), !old_mask[f as int], excluded == old_mask.update(f as int, true),
    ensures state(members, required_by, counts, todo, excluded, done.push(f), members[f as int]@),
{
    dequeue(members, required_by, before, todo, f);
    let next = done.push(f);
    assert forall|v: usize| #[trigger] next.contains(v) implies v < members.len()
        && impossible(members, required_by, v) by {};
    assert forall|i: int, j: int| 0 <= i < j < next.len() implies next[i] != next[j] by {
        if j == done.len() {
            assert(done.contains(done[i]));
        }
    }
    let domain = Set::range(0usize, members.len() as usize);
    assert(next.to_set().subset_of(domain));
    next.unique_seq_to_set();
    range_set_properties(0usize, members.len() as usize);
    lemma_len_subset(next.to_set(), domain);
    assert forall|h: usize| h < counts.len() implies #[trigger] counts[h as int]
        == candidate_edges(members, next, h) + members[f as int]@.to_multiset().count(h) by {
        partition_edges(members, done, f, h);
        assert(Seq::<usize>::empty().to_multiset().count(h) == 0);
    }
}

pub proof fn can_decrement(members: Seq<Vec<usize>>, counts: Seq<usize>, done: Seq<usize>, tail: Seq<usize>, h: usize)
    requires counters(members, counts, done, tail), tail.len() > 0, tail.first() == h, h < counts.len(),
    ensures counts[h as int] > 0,
{
    split_tail(tail, h);
}

pub proof fn update(
    members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, initial: Seq<usize>, before: Seq<usize>, counts: Seq<usize>,
    old_queue: Seq<usize>, todo: Seq<usize>, excluded: Seq<bool>, done: Seq<usize>, tail: Seq<usize>, h: usize,
)
    requires well_formed(members, required_by, initial), initial.len() == before.len(),
        state(members, required_by, before, old_queue, excluded, done, tail),
        tail.len() > 0, tail.first() == h, h < before.len(), before[h as int] > 0,
        counts == before.update(h as int, (before[h as int] - 1) as usize),
        todo == if counts[h as int] == 0 { old_queue + required_by[h as int]@ } else { old_queue },
    ensures state(members, required_by, counts, todo, excluded, done, tail.drop_first()),
{
    assert forall|k: usize| k < counts.len() implies #[trigger] counts[k as int]
        == candidate_edges(members, done, k) + tail.drop_first().to_multiset().count(k) by {
        split_tail(tail, k);
        assert(before[k as int] == counts[k as int] + if k == h { 1int } else { 0int });
    }
    assert forall|f: usize| #[trigger] todo.contains(f) implies f < members.len()
        && impossible(members, required_by, f) by {
        if !old_queue.contains(f) {
            let row = required_by[h as int]@;
            assert(row.contains(f));
            let i = choose|i: int| 0 <= i < row.len() && row[i] == f;
            assert(required_by[h as int]@[i] < members.len());
            unsupplied(members, required_by, done, h, f);
        }
    }
}

pub proof fn finish(
    members: Seq<Vec<usize>>, required_by: Seq<Vec<usize>>, counts: Seq<usize>,
    todo: Seq<usize>, excluded: Seq<bool>, done: Seq<usize>,
)
    requires members.len() <= usize::MAX, required_by.len() == counts.len(),
        state(members, required_by, counts, todo, excluded, done, Seq::empty()), todo.len() == 0,
    ensures biggest(members, required_by, excluded),
{
    let selected = Set::range(0usize, members.len() as usize).filter(|f: usize| !excluded[f as int]);
    assert forall|h: usize, f: usize| h < required_by.len() && selected.contains(f)
        && #[trigger] required_by[h as int]@.contains(f) implies supplies(members, selected, h) by {
        assert(!done.contains(f));
        assert(counts[h as int] != 0);
        assert(Seq::<usize>::empty().to_multiset().count(h) == 0);
        zero_edges(members, done, h);
        let p = choose|p: usize| p < members.len() && #[trigger] members[p as int]@.contains(h) && !done.contains(p);
        assert(selected.contains(p));
    }
    assert forall|alternative: Set<usize>| #[trigger] closed(members, required_by, alternative)
        implies alternative.subset_of(selected) by {
        assert forall|f: usize| #[trigger] alternative.contains(f) implies selected.contains(f) by {
            if done.contains(f) {
                assert(impossible(members, required_by, f));
            }
        }
    }
}

} // verus!

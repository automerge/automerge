use crate::lookup_spec::{model, modeled};
use vstd::prelude::*;

verus! {

broadcast use vstd::seq_lib::group_seq_properties;

pub(crate) open spec fn for_members<T, F: Fn(&T) -> bool>(members: Seq<T>, test: F) -> bool {
    forall|i: int| 0 <= i < members.len() ==> #[trigger] modeled(test, &members[i])
}

pub(crate) open spec fn every<T, F: Fn(&T) -> bool>(members: Seq<T>, test: F, end: int) -> bool {
    forall|i: int| 0 <= i < end ==> #[trigger] model(test, &members[i])
}

pub(crate) open spec fn proof_result<F: Fn() -> bool>(check: F) -> bool {
    choose|b: bool| check.ensures((), b)
}

pub(crate) open spec fn stable_proof_check<F: Fn() -> bool>(check: F) -> bool {
    &&& check.requires(())
    &&& check.ensures((), proof_result(check))
    &&& forall|b: bool| #[trigger] check.ensures((), b) ==> b == proof_result(check)
}

pub(crate) open spec fn viable_result<T, A: Fn(&T) -> bool, U: Fn(&T) -> bool, F: Fn() -> bool>(
    members: Seq<T>, accepted: A, unconflicted: U, check: F,
) -> bool {
    &&& !every(members, accepted, members.len() as int)
    &&& every(members, unconflicted, members.len() as int)
    &&& proof_result(check)
}

pub(crate) open spec fn equivalent<T, D: Fn(&T, &T) -> bool>(same: D, a: T, b: T) -> bool {
    choose|r: bool| same.ensures((&a, &b), r)
}

pub(crate) open spec fn comparable<T, D: Fn(&T, &T) -> bool>(same: D, a: T, b: T) -> bool {
    &&& same.requires((&a, &b))
    &&& same.ensures((&a, &b), equivalent(same, a, b))
    &&& forall|r: bool| #[trigger] same.ensures((&a, &b), r) ==> r == equivalent(same, a, b)
}

pub(crate) open spec fn callbacks<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool, D: Fn(&T, &T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, same: D,
) -> bool {
    &&& input.len() <= usize::MAX
    &&& forall|i: usize| i < input.len() ==> #[trigger] modeled(remaining, i)
    &&& forall|i: usize| i < input.len() && #[trigger] model(remaining, i) ==> modeled(viable, &input[i as int])
    &&& forall|t: T| #[trigger] input.contains(t) ==> equivalent(same, t, t)
    &&& forall|a: T, b: T| #[trigger] input.contains(a) && #[trigger] input.contains(b) ==> comparable(same, a, b)
    &&& forall|a: T, b: T, c: T| input.contains(a) && input.contains(b) && input.contains(c)
        && #[trigger] equivalent(same, a, b) && #[trigger] equivalent(same, b, c) ==> equivalent(same, a, c)
}

pub(crate) open spec fn qualifies<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, i: int,
) -> bool {
    model(remaining, i as usize) && model(viable, &input[i])
}

pub(crate) open spec fn first<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool, D: Fn(&T, &T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, same: D, i: int,
) -> bool {
    &&& qualifies(input, remaining, viable, i)
    &&& forall|j: int| 0 <= j < i && qualifies(input, remaining, viable, j)
        ==> !equivalent(same, #[trigger] input[j], input[i])
}

pub(crate) open spec fn represented<T, D: Fn(&T, &T) -> bool>(kept: Seq<T>, same: D, candidate: T) -> bool {
    exists|j: int| 0 <= j < kept.len() && equivalent(same, #[trigger] kept[j], candidate)
}

// Output positions enumerate, in input order, exactly the first viable
// occurrence of each still-eligible delivery. Equality includes prior group
// provenance (the actual attachment), not the provisional proof cache.
pub(crate) open spec fn retained<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool, D: Fn(&T, &T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, same: D, end: int, kept: Seq<T>, positions: Seq<int>,
) -> bool {
    &&& 0 <= end <= input.len()
    &&& kept.len() == positions.len()
    &&& forall|j: int| 0 <= j < kept.len() ==> 0 <= #[trigger] positions[j] < end && kept[j] == input[positions[j]]
    &&& forall|j: int, k: int| 0 <= j < k < positions.len() ==> #[trigger] positions[j] < #[trigger] positions[k]
    &&& forall|i: int| 0 <= i < end ==> (#[trigger] positions.contains(i) <==> first(input, remaining, viable, same, i))
}

pub(crate) open spec fn retention<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool, D: Fn(&T, &T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, same: D, kept: Seq<T>,
) -> bool {
    exists|positions: Seq<int>| #[trigger] retained(input, remaining, viable, same, input.len() as int, kept, positions)
}

proof fn first_witness<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool, D: Fn(&T, &T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, same: D, i: int,
)
    requires 0 <= i < input.len(), callbacks(input, remaining, viable, same), qualifies(input, remaining, viable, i),
    ensures exists|j: int| 0 <= j <= i && first(input, remaining, viable, same, j)
        && equivalent(same, #[trigger] input[j], input[i]),
    decreases i,
{
    assert(input.contains(input[i]));
    if !first(input, remaining, viable, same, i) {
        let j = choose|j: int| 0 <= j < i && qualifies(input, remaining, viable, j)
            && equivalent(same, #[trigger] input[j], input[i]);
        assert(input.contains(input[j]));
        first_witness(input, remaining, viable, same, j);
        let k = choose|k: int| 0 <= k <= j && first(input, remaining, viable, same, k)
            && equivalent(same, #[trigger] input[k], input[j]);
        assert(input.contains(input[k]));
        assert(equivalent(same, input[k], input[i]));
    }
}

pub(crate) proof fn compare_kept<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool, D: Fn(&T, &T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, same: D, i: int, kept: Seq<T>, positions: Seq<int>,
)
    requires retained(input, remaining, viable, same, i, kept, positions), callbacks(input, remaining, viable, same),
        i < input.len(),
    ensures forall|j: int| 0 <= j < kept.len() ==> comparable(same, #[trigger] kept[j], input[i]),
{
    assert(input.contains(input[i]));
    assert forall|j: int| 0 <= j < kept.len() implies comparable(same, #[trigger] kept[j], input[i]) by {
        assert(input.contains(input[positions[j]]));
    }
}

pub(crate) proof fn keep_one<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool, D: Fn(&T, &T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, same: D, i: int, kept: Seq<T>, positions: Seq<int>,
)
    requires retained(input, remaining, viable, same, i, kept, positions), i < input.len(),
        callbacks(input, remaining, viable, same), qualifies(input, remaining, viable, i),
        !represented(kept, same, input[i]),
    ensures retained(input, remaining, viable, same, i + 1, kept.push(input[i]), positions.push(i)),
{
    if !first(input, remaining, viable, same, i) {
        first_witness(input, remaining, viable, same, i);
        let j = choose|j: int| 0 <= j < i && first(input, remaining, viable, same, j)
            && equivalent(same, #[trigger] input[j], input[i]);
        assert(positions.contains(j));
        let k = choose|k: int| 0 <= k < kept.len() && positions[k] == j;
        assert(equivalent(same, kept[k], input[i]));
        assert(represented(kept, same, input[i]));
    }
    assert(first(input, remaining, viable, same, i));
    let updated = positions.push(i);
    let new_kept = kept.push(input[i]);
    assert forall|j: int, k: int| 0 <= j < k < updated.len() implies #[trigger] updated[j] < #[trigger] updated[k] by {
        if k == positions.len() { assert(positions[j] < i); }
    }
    assert forall|j: int| 0 <= j < new_kept.len()
        implies 0 <= #[trigger] updated[j] < i + 1 && new_kept[j] == input[updated[j]] by {};
    assert forall|j: int| 0 <= j < i + 1
        implies (#[trigger] updated.contains(j) <==> first(input, remaining, viable, same, j)) by {
        assert(updated.contains(j) <==> positions.contains(j) || j == i);
    }
}

pub(crate) proof fn skip_one<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool, D: Fn(&T, &T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, same: D, i: int, kept: Seq<T>, positions: Seq<int>,
)
    requires retained(input, remaining, viable, same, i, kept, positions), i < input.len(),
        !qualifies(input, remaining, viable, i) || represented(kept, same, input[i]),
    ensures retained(input, remaining, viable, same, i + 1, kept, positions),
{
    if represented(kept, same, input[i]) {
        let j = choose|j: int| 0 <= j < kept.len() && equivalent(same, #[trigger] kept[j], input[i]);
        assert(positions.contains(positions[j]));
        assert(qualifies(input, remaining, viable, positions[j]));
        assert(!first(input, remaining, viable, same, i));
    }
    assert(!positions.contains(i));
}

pub(crate) proof fn finish<T, R: Fn(usize) -> bool, V: Fn(&T) -> bool, D: Fn(&T, &T) -> bool>(
    input: Seq<T>, remaining: R, viable: V, same: D, kept: Seq<T>, positions: Seq<int>,
)
    requires retained(input, remaining, viable, same, input.len() as int, kept, positions),
    ensures retention(input, remaining, viable, same, kept),
{}

} // verus!

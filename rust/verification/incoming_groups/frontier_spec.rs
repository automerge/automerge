use crate::lookup_spec::model;
use vstd::laws_cmp::{obeys_cmp, obeys_cmp_concrete_eq};
use vstd::prelude::*;
use vstd::std_specs::btree::key_obeys_cmp_spec;

verus! {

pub(crate) open spec fn actor_order<A: Ord>() -> bool {
    obeys_cmp::<A>() && obeys_cmp_concrete_eq::<A>() && key_obeys_cmp_spec::<A>()
}

pub(crate) open spec fn with_member<A, H>(
    prior: Map<A, (u64, H)>, actor: A, seq: u64, hash: H,
) -> Map<A, (u64, H)> {
    if prior.contains_key(actor) && prior[actor].0 >= seq { prior }
    else { prior.insert(actor, (seq, hash)) }
}

pub(crate) open spec fn bounds<A, H>(choice: Map<A, (u64, H)>, member: (A, u64, H)) -> bool {
    let (actor, seq, _) = member;
    choice.contains_key(actor) && choice[actor].0 >= seq
}

// Each actor in the input has an entry. Its sequence bounds all of that
// actor's members, and (sequence, hash) comes from one actual input member.
pub(crate) open spec fn exact_frontiers<'m, M: 'm, A, H, F: Fn(&'m M) -> (A, u64, H)>(
    members: Seq<M>, describe: F, end: int, choice: Map<A, (u64, H)>,
) -> bool {
    &&& forall|i: int| 0 <= i < end ==> bounds(choice, #[trigger] model(describe, &members[i]))
    &&& forall|actor: A| #[trigger] choice.contains_key(actor) ==> exists|i: int| 0 <= i < end
        && #[trigger] model(describe, &members[i]) == (actor, choice[actor].0, choice[actor].1)
}

pub(crate) proof fn added_member<'m, M: 'm, A, H, F: Fn(&'m M) -> (A, u64, H)>(
    members: Seq<M>, describe: F, i: int, before: Map<A, (u64, H)>, next: Map<A, (u64, H)>,
)
    requires 0 <= i < members.len(), exact_frontiers(members, describe, i, before),
        {
            let (actor, seq, hash) = model(describe, &members[i]);
            next == with_member(before, actor, seq, hash)
        },
    ensures exact_frontiers(members, describe, i + 1, next),
{
    let (new_actor, new_seq, new_hash) = model(describe, &members[i]);
    assert forall|j: int| 0 <= j < i + 1 implies bounds(next, #[trigger] model(describe, &members[j])) by {};
    assert forall|actor: A| #[trigger] next.contains_key(actor) implies exists|j: int| 0 <= j < i + 1
        && #[trigger] model(describe, &members[j]) == (actor, next[actor].0, next[actor].1) by {
        if actor == new_actor && next[actor] == (new_seq, new_hash) {
            assert(model(describe, &members[i]) == (actor, next[actor].0, next[actor].1));
        } else {
            assert(before.contains_key(actor));
            assert(before[actor] == next[actor]);
        }
    }
}

pub(crate) proof fn frontier_hashes_unique<'m, M: 'm, A, H, F: Fn(&'m M) -> (A, u64, H)>(
    members: Seq<M>, describe: F, choice: Map<A, (u64, H)>, left: A, right: A,
)
    requires exact_frontiers(members, describe, members.len() as int, choice),
        forall|i: int, j: int| 0 <= i < j < members.len() ==>
            #[trigger] model(describe, &members[i]).2 != #[trigger] model(describe, &members[j]).2,
        choice.contains_key(left), choice.contains_key(right), choice[left].1 == choice[right].1,
    ensures left == right,
{
    let i = choose|i: int| 0 <= i < members.len()
        && #[trigger] model(describe, &members[i]) == (left, choice[left].0, choice[left].1);
    let j = choose|j: int| 0 <= j < members.len()
        && #[trigger] model(describe, &members[j]) == (right, choice[right].0, choice[right].1);
    assert(model(describe, &members[i]).2 == model(describe, &members[j]).2);
    if i < j {
        assert(model(describe, &members[i]).2 != model(describe, &members[j]).2);
    } else if j < i {
        assert(model(describe, &members[j]).2 != model(describe, &members[i]).2);
    }
}

} // verus!

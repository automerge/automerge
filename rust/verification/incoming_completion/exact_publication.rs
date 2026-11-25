use crate::admission_proof::selection_preserves_causal_closure;
use crate::admission_spec::{assembly, selected_member, Fragments};
use crate::clock_spec::{maxima, number};
use crate::history_spec::{actor_chains, History};
use crate::lookup_spec::model;
use crate::proof::chain_prefix;
use std::num::NonZeroU32;
use vstd::prelude::*;

verus! {

pub(crate) open spec fn covered<K>(history: History<K, usize>, slots: Seq<Option<NonZeroU32>>, k: K) -> bool {
    (history.sequence)(k) <= number(slots[(history.actor)(k) as int])
}

// Connect the executable slot fold to the canonical-fragment theorem. The
// indexed views are of the graph and actor table *after* successful apply.
// Given a faithful selected-member projection, no other applied change can
// become accepted by the increase of an actor maximum.
pub(crate) proof fn exact_after_clock_update<K, F: Fn(K) -> (usize, u32)>(
    history: History<K, usize>, fragments: Fragments<K>, selected: Set<K>, before: Set<K>, after: Set<K>,
    max_level: nat, members: Seq<K>, lookup: F, old: Seq<Option<NonZeroU32>>, new: Seq<Option<NonZeroU32>>,
)
    requires assembly(history, fragments, selected, before, after, max_level), actor_chains(history),
        maxima(old, members, lookup, members.len() as int, new),
        forall|k: K| #[trigger] history.nodes.contains(k) ==> (history.actor)(k) < old.len()
            && (covered(history, old, k) <==> before.contains(k)),
        forall|k: K| #[trigger] members.contains(k) <==> selected_member(fragments, selected, k),
        forall|i: int| 0 <= i < members.len() ==> {
            let k = #[trigger] members[i];
            let (actor, seq) = model(lookup, k);
            history.nodes.contains(k) && actor == (history.actor)(k) && seq == (history.sequence)(k)
        },
    ensures forall|k: K| #[trigger] history.nodes.contains(k) ==> (covered(history, new, k) <==> after.contains(k)),
{
    selection_preserves_causal_closure(history, fragments, selected, before, after, max_level);
    assert forall|k: K| #[trigger] history.nodes.contains(k) implies (covered(history, new, k) <==> after.contains(k)) by {
        let actor = (history.actor)(k);
        let seq = (history.sequence)(k);
        assert(number(new[actor as int]) >= number(old[actor as int]));
        if after.contains(k) && !before.contains(k) {
            assert(selected_member(fragments, selected, k));
            assert(members.contains(k));
            let i = choose|i: int| 0 <= i < members.len() && members[i] == k;
            assert(model(lookup, members[i]).0 == actor);
            assert(number(new[actor as int]) >= seq);
        }
        if covered(history, new, k) && !before.contains(k) {
            assert(number(new[actor as int]) > number(old[actor as int]));
            let i = choose|i: int| 0 <= i < members.len()
                && model(lookup, #[trigger] members[i]) == (actor, number(new[actor as int]));
            let witness = members[i];
            assert(members.contains(witness));
            assert(selected_member(fragments, selected, witness));
            assert(after.contains(witness));
            chain_prefix(history, after, k, witness);
        }
    }
}

} // verus!

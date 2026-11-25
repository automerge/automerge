use crate::admission_spec::*;
use crate::history_spec::*;
use crate::proof::exact_clock;
use vstd::prelude::*;

verus! {

broadcast use vstd::set::group_set_lemmas;

// Going to a boundary's supplier either increases head level (the boundary
// is an interior member), or moves to a strictly earlier head at the same
// level (the boundary is itself the supplier's head).
proof fn selected_head_covers_ancestors<K, A>(
    history: History<K, A>, fragments: Fragments<K>, heads: Set<K>, before: Set<K>, after: Set<K>,
    max_level: nat, head: K, node: K,
)
    requires assembly(history, fragments, heads, before, after, max_level), heads.contains(head),
        (fragments.past)(head).contains(node),
    ensures after.contains(node),
    decreases max_level - (fragments.level)(head), (fragments.rank)(head),
{
    assert(canonical(fragments, head));
    if (fragments.members)(head).contains(node) {
        assert(selected_member(fragments, heads, node));
    } else {
        let b = choose|b: K| #[trigger] (fragments.boundary)(head).contains(b) && (fragments.past)(b).contains(node);
        assert((fragments.boundary)(head).contains(b));
        if before.contains(b) {
            assert(before.contains(node));
        } else {
            assert(selected_member(fragments, heads, b));
            let supplier = choose|h: K| #[trigger] heads.contains(h) && (fragments.members)(h).contains(b);
            assert(canonical(fragments, supplier));
            assert((fragments.past)(supplier).contains(b));
            assert((fragments.past)(b).subset_of((fragments.past)(supplier)));
            selected_head_covers_ancestors(history, fragments, heads, before, after, max_level, supplier, node);
        }
    }
}

pub proof fn selection_preserves_causal_closure<K, A>(
    history: History<K, A>, fragments: Fragments<K>, heads: Set<K>, before: Set<K>, after: Set<K>, max_level: nat,
)
    requires assembly(history, fragments, heads, before, after, max_level),
    ensures causal_set(history, after),
{
    assert forall|k: K| #[trigger] after.contains(k) implies history.nodes.contains(k)
        && (history.parents)(k).subset_of(after) by {
        if !before.contains(k) {
            assert(selected_member(fragments, heads, k));
            let h = choose|h: K| #[trigger] heads.contains(h) && (fragments.members)(h).contains(k);
            assert(canonical(fragments, h));
            assert((fragments.past)(h).contains(k));
            assert((fragments.past)(k).subset_of((fragments.past)(h)));
            assert forall|p: K| #[trigger] (history.parents)(k).contains(p) implies after.contains(p) by {
                selected_head_covers_ancestors(history, fragments, heads, before, after, max_level, h, p);
            }
        }
    }
}

// Given the canonical/selection contracts and the clock-update contract,
// publication cannot introduce an unselected change via a sequence maximum.
pub proof fn publication_is_exact<K, A>(
    history: History<K, A>, fragments: Fragments<K>, heads: Set<K>, before: Set<K>, members: Set<K>, after: Set<K>,
    max_level: nat, old: Map<A, nat>, selected: Map<A, nat>, updated: Map<A, nat>,
)
    requires assembly(history, fragments, heads, before, after, max_level), actor_chains(history),
        forall|k: K| #[trigger] members.contains(k) <==> selected_member(fragments, heads, k),
        maxima(history, before, history.nodes, old), maxima(history, members, history.nodes, selected),
        joins(old, selected, updated),
    ensures forall|k: K| #[trigger] history.nodes.contains(k) ==> (covers(history, updated, k)
        <==> before.contains(k) || selected_member(fragments, heads, k)),
{
    selection_preserves_causal_closure(history, fragments, heads, before, after, max_level);
    assert(maxima(history, after, history.nodes, updated)) by {
        assert forall|k: K| #[trigger] after.contains(k) && #[trigger] history.nodes.contains(k)
            implies covers(history, updated, k) by {
            if before.contains(k) {
                assert(covers(history, old, k));
            } else {
                assert(members.contains(k));
                assert(covers(history, selected, k));
            }
        }
        assert forall|a: A| #[trigger] updated.dom().contains(a) implies exists|k: K| {
            &&& #[trigger] after.contains(k)
            &&& history.nodes.contains(k)
            &&& (history.actor)(k) == a
            &&& (history.sequence)(k) == updated[a]
        } by {
            if !selected.dom().contains(a) || (old.dom().contains(a) && old[a] >= selected[a]) {
                assert(old.dom().contains(a));
                let witness = choose|k: K| #[trigger] before.contains(k) && history.nodes.contains(k)
                    && (history.actor)(k) == a && (history.sequence)(k) == old[a];
                assert(after.contains(witness));
            } else {
                let witness = choose|k: K| #[trigger] members.contains(k) && history.nodes.contains(k)
                    && (history.actor)(k) == a && (history.sequence)(k) == selected[a];
                assert(after.contains(witness));
            }
        }
    }
    exact_clock(history, after, history.nodes, updated);
}

} // verus!

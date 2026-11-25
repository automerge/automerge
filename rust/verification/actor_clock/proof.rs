use crate::spec::*;
use vstd::prelude::*;

verus! {

// Each actor's exact predecessor is a graph edge. Follow those edges
// until reaching the target sequence, then use uniqueness to identify x.
proof fn chain_prefix<K, A>(history: History<K, A>, past: Set<K>, x: K, witness: K)
    requires actor_chains(history), causal_set(history, past), history.nodes.contains(x),
        past.contains(witness), (history.actor)(x) == (history.actor)(witness),
        (history.sequence)(x) <= (history.sequence)(witness),
    ensures past.contains(x),
    decreases (history.sequence)(witness),
{
    assert(history.nodes.contains(witness));
    if (history.sequence)(x) == (history.sequence)(witness) {
        assert(x == witness);
    } else {
        let pred = choose|p: K| #[trigger] (history.parents)(witness).contains(p)
            && (history.actor)(p) == (history.actor)(witness)
            && (history.sequence)(p) + 1 == (history.sequence)(witness);
        assert(past.contains(pred));
        chain_prefix(history, past, x, pred);
    }
}

pub proof fn exact_clock<K, A>(history: History<K, A>, past: Set<K>, side: Set<K>, clock: Map<A, nat>)
    requires actor_chains(history), causal_set(history, past), maxima(history, past, side, clock),
    ensures forall|node: K| history.nodes.contains(node) && #[trigger] side.contains(node)
        ==> (covers(history, clock, node) <==> past.contains(node)),
{
    assert forall|node: K| history.nodes.contains(node) && #[trigger] side.contains(node)
        implies (covers(history, clock, node) <==> past.contains(node)) by {
        if covers(history, clock, node) {
            let actor = (history.actor)(node);
            let witness = choose|w: K| #[trigger] past.contains(w) && side.contains(w)
                && (history.actor)(w) == actor && (history.sequence)(w) == clock[actor];
            chain_prefix(history, past, node, witness);
        }
    }
}

} // verus!

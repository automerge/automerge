//! Kahn's worklist for the unapplied portion of the change graph.

// Verification compiles this same file in the standalone Verus harness.
// Ordinary Rust builds do not depend on Verus.
#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use crate::{proof::*, spec::*};
#[cfg(automerge_verus)]
use vstd::prelude::*;
#[cfg(automerge_verus)]
verus! {
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// Return the candidates that can be resolved, in dependency order.
/// `waiting` counts all unapplied dependency occurrences (including missing
/// ones); `dependents` lists the reverse edges between candidates.
#[cfg_attr(automerge_verus, verus_spec(order =>
    requires well_formed(dependents@, waiting@),
    ensures resolution(dependents@, waiting@, order@),
))]
pub(super) fn resolve(dependents: &[Vec<usize>], mut waiting: Vec<usize>) -> Vec<usize> {
    #[cfg(automerge_verus)]
    proof_decl! { let ghost initial = waiting@; }

    // The result doubles as the FIFO queue. The cursor separates vertices
    // whose outgoing edges have been visited from vertices still queued.
    let mut order = Vec::with_capacity(waiting.len());
    let mut v = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant
            well_formed(dependents@, initial),
            waiting@ == initial,
            v <= waiting@.len(),
            valid_order(dependents@, initial, order@),
            forall|u: usize| #[trigger] order@.contains(u) <==> u < v && waiting@[u as int] == 0,
        decreases waiting@.len() - v,
    ))]
    while v < waiting.len() {
        #[cfg(automerge_verus)]
        proof_decl! { let ghost before = order@; }
        if waiting[v] == 0 {
            #[cfg(automerge_verus)]
            proof! { seed(dependents@, initial, order@, v); }
            order.push(v);
        }
        #[cfg(automerge_verus)]
        proof! {
            assert forall|u: usize| #[trigger] order@.contains(u) <==> u < v + 1 && waiting@[u as int] == 0 by {
                assert(before.contains(u) <==> u < v && waiting@[u as int] == 0);
            }
        }
        v += 1;
    }

    let mut cursor = 0;
    #[cfg(automerge_verus)]
    proof! {
        bound_order(dependents@, initial, order@);
        assert(order@.take(0) =~= Seq::<usize>::empty());
    }
    #[cfg_attr(automerge_verus, verus_spec(
        invariant
            well_formed(dependents@, initial),
            cursor <= order@.len() <= dependents@.len(),
            worklist(dependents@, initial, waiting@, order@, order@.take(cursor as int), Seq::empty()),
        decreases dependents@.len() - cursor,
    ))]
    while cursor < order.len() {
        let parent = order[cursor];
        let children = &dependents[parent];
        cursor += 1;

        #[cfg(automerge_verus)]
        proof_decl! {
            let ghost done = order@.take(cursor as int);
            begin_parent(dependents@, initial, waiting@, order@, cursor as int);
            assert(children@.skip(0) =~= children@);
        }

        let mut j = 0;
        #[cfg_attr(automerge_verus, verus_spec(
            invariant
                well_formed(dependents@, initial),
                cursor <= order@.len(),
                done == order@.take(cursor as int),
                parent < dependents@.len(),
                children@ == dependents@[parent as int]@,
                j <= children@.len(),
                worklist(dependents@, initial, waiting@, order@, done, children@.skip(j as int)),
            decreases children@.len() - j,
        ))]
        while j < children.len() {
            let child = children[j];
            #[cfg(automerge_verus)]
            proof_decl! {
                let ghost before = waiting@;
                let ghost old_order = order@;
                let ghost remaining_children = children@.skip(j as int);
                assert(child == dependents@[parent as int]@[j as int]);
                child_is_waiting(dependents@, initial, before, old_order, done, remaining_children, child);
            }
            waiting[child] -= 1;
            if waiting[child] == 0 {
                #[cfg(automerge_verus)]
                proof! { enqueue(dependents@, initial, before, order@, done, remaining_children, child); }
                order.push(child);
            }
            #[cfg(automerge_verus)]
            proof! {
                consume_child(dependents@, initial, before, waiting@, old_order, order@, done, remaining_children, child);
                assert(remaining_children.drop_first() =~= children@.skip(j as int + 1));
            }
            j += 1;
        }
        #[cfg(automerge_verus)]
        proof! {
            assert(children@.skip(j as int) =~= Seq::<usize>::empty());
            bound_order(dependents@, initial, order@);
        }
    }

    #[cfg(automerge_verus)]
    proof! {
        assert(order@.take(cursor as int) =~= order@);
        finish(dependents@, initial, waiting@, order@);
    }
    order
}

#[cfg(test)]
#[path = "topology/tests.rs"]
mod tests;

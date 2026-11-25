//! A worklist walk over an arbitrary view of change dependencies.

#![allow(unexpected_cfgs)]

use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{walk_proof::*, walk_spec::*};
#[cfg(automerge_verus)]
use vstd::laws_eq::obeys_concrete_eq;
#[cfg(automerge_verus)]
use vstd::prelude::*;
#[cfg(automerge_verus)]
use vstd::std_specs::hash::{hash_set_inv, obeys_key_model};

#[cfg_attr(automerge_verus, verus_verify)]
#[derive(Clone, Copy)]
pub(crate) enum Visit {
    Ignore,
    Keep,
    Expand,
    KeepAndExpand,
}

#[cfg_attr(automerge_verus, verus_spec(
    requires obeys_key_model::<K>(), obeys_concrete_eq::<K>(), hash_set_inv(old(seen)),
    ensures hash_set_inv(final(seen)), final(seen)@ == old(seen)@.insert(node),
        final(queue)@ == if old(seen)@.contains(node) { old(queue)@ } else { old(queue)@.push(node) },
))]
fn enqueue<K: Copy + Hash + Eq>(node: K, seen: &mut HashSet<K>, queue: &mut VecDeque<K>) {
    if seen.insert(node) {
        queue.push_back(node);
    }
}

/// Visit the roots and follow dependencies of expanded nodes, returning only
/// the kept nodes. A parent lookup is never made for a stopped or ignored node.
#[cfg_attr(automerge_verus, verus_spec(kept =>
    requires obeys_key_model::<K>(), obeys_concrete_eq::<K>(),
        exists|nodes: Set<K>| #[trigger] supports(roots@, parents, classify, nodes),
    ensures walk_result(model(roots@, parents, classify), roots@, kept@),
))]
pub(super) fn walk<K: Copy + Hash + Eq>(
    roots: Vec<K>,
    parents: impl Fn(K) -> Vec<K>,
    classify: impl Fn(K) -> Visit,
) -> Vec<K> {
    let mut seen = HashSet::new();
    let mut queue = VecDeque::new();
    let mut kept = Vec::new();
    let mut i = 0;
    #[cfg_attr(automerge_verus, verus_spec(
        invariant i <= roots@.len(), hash_set_inv(&seen), obeys_key_model::<K>(), obeys_concrete_eq::<K>(),
            queue@.no_duplicates(), seen@ == queue@.to_set(), seen@ == roots@.take(i as int).to_set(),
        decreases roots@.len() - i,
    ))]
    while i < roots.len() {
        let node = roots[i];
        #[cfg(automerge_verus)]
        proof_decl! { let ghost before = queue@; let ghost previous = seen@; }
        enqueue(node, &mut seen, &mut queue);
        #[cfg(automerge_verus)]
        proof! {
            enqueue_model(node, previous, before, seen@, queue@);
            prefix_step(roots@, i as int);
        }
        i += 1;
    }

    #[cfg(automerge_verus)]
    proof_decl! {
        let ghost graph = model(roots@, parents, classify);
        assert(models(graph, roots@, parents, classify));
        let ghost mut done = Set::<K>::empty();
        assert(roots@.take(i as int) =~= roots@);
        init_walk(graph, roots@, seen@, queue@, kept@);
    }
    #[cfg_attr(automerge_verus, verus_spec(
        invariant models(graph, roots@, parents, classify), graph == model(roots@, parents, classify),
            hash_set_inv(&seen), obeys_key_model::<K>(), obeys_concrete_eq::<K>(),
            state(graph, roots@, seen@, queue@, done, kept@),
        decreases graph.nodes.len() - done.len(),
    ))]
    loop {
        #[cfg(automerge_verus)]
        proof_decl! { let ghost prev_queue = queue@; }
        let Some(node) = queue.pop_front() else {
            #[cfg(automerge_verus)]
            proof! { finish_walk(graph, roots@, seen@, queue@, done, kept@); }
            return kept;
        };
        #[cfg(automerge_verus)]
        proof! {
            take_next(graph, roots@, seen@, prev_queue, queue@, done, kept@, node);
            assert(seen@.contains(node));
            assert(graph.nodes.contains(node));
            assert(classify.requires((node,)));
        }
        let step = classify(node);
        #[cfg(automerge_verus)]
        proof_decl! { let ghost kept_before = kept@; }
        if matches!(step, Visit::Keep | Visit::KeepAndExpand) {
            kept.push(node);
        }
        #[cfg(automerge_verus)]
        proof! { keep_node(graph, done, kept_before, kept@, node); }
        if matches!(step, Visit::Expand | Visit::KeepAndExpand) {
            #[cfg(automerge_verus)]
            proof! {
                assert(parents.requires((node,)));
                assert((graph.parents)(node).to_set().subset_of(graph.nodes));
            }
            let adjacent = parents(node);
            let mut j = 0;
            #[cfg_attr(automerge_verus, verus_spec(
                invariant j <= adjacent@.len(), adjacent@ == (graph.parents)(node), hash_set_inv(&seen),
                    obeys_key_model::<K>(), obeys_concrete_eq::<K>(),
                    adjacent@.to_set().subset_of(graph.nodes), expands((graph.visit)(node)),
                    active(graph, roots@, seen@, queue@, done, node), output(graph, done.insert(node), kept@),
                    adjacent@.take(j as int).to_set().subset_of(seen@),
                decreases adjacent@.len() - j,
            ))]
            while j < adjacent.len() {
                let parent = adjacent[j];
                #[cfg(automerge_verus)]
                proof_decl! {
                    let ghost before = queue@;
                    let ghost previous = seen@;
                    assert(seen@.contains(node));
                    assert(graph.nodes.contains(node));
                    assert(reached(graph, roots@, node));
                    follow_edge(graph, roots@, seen@, node, adjacent@, j as int);
                }
                enqueue(parent, &mut seen, &mut queue);
                #[cfg(automerge_verus)]
                proof! {
                    extend_walk(graph, roots@, previous, before, seen@, queue@, done, node, parent);
                    prefix_step(adjacent@, j as int);
                }
                j += 1;
            }
            #[cfg(automerge_verus)]
            proof! { assert(adjacent@.take(j as int) =~= adjacent@); }
        }
        #[cfg(automerge_verus)]
        proof! {
            finish_node(graph, roots@, seen@, queue@, done, kept@, node);
            done = done.insert(node);
        }
    }
}

#[cfg(test)]
#[path = "walk/tests.rs"]
mod tests;

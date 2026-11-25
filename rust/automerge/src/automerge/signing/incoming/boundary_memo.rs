//! Memoize causal summaries on the graph that stops at applied changes.

#![allow(unexpected_cfgs)]

use super::boundary_summary::Summary;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

#[cfg(automerge_verus)]
use crate::{
    memo_proof::*,
    memo_spec::*,
    spec::{join, summary_inv, view},
};
#[cfg(automerge_verus)]
use vstd::{
    prelude::*,
    std_specs::hash::{hash_map_inv, hash_set_inv},
};

/// A read-only observation of one change. Applied changes are terminal here:
/// their inclusive past is obtained later from the applied graph's clock.
#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) enum Node<'a, A, K> {
    Applied,
    Candidate {
        actor: A,
        seq: u64,
        parents: &'a [K],
    },
}

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Cache<A, K> {
    pub(crate) entries: HashMap<K, Summary<A, K>>,
}

#[cfg_attr(automerge_verus, verus_verify)]
impl<A: Clone + Eq + Hash, K: Copy + Ord + Hash> Cache<A, K> {
    #[cfg_attr(automerge_verus, verus_spec(out => ensures hash_map_inv(&out.entries), out.entries@.is_empty()))]
    pub(super) fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Summarize the union of the pasts of exactly these boundary roots.
    #[cfg_attr(automerge_verus, verus_spec(out =>
        requires memo_laws::<A,K>(), valid_entries(old(self),read),
            exists|nodes:Set<K>| #[trigger] supports_all(nodes,read,roots@),
        ensures valid_entries(final(self),read), summary_inv(&out),
            covers(read,|k:K| past_prefix(read,roots@,roots@.len() as int,k),view(&out)),
            preserves(old(self).entries@,final(self).entries@),
    ))]
    pub(super) fn union<'a>(
        &mut self,
        roots: &[K],
        read: &impl Fn(K) -> Node<'a, A, K>,
    ) -> Summary<A, K>
    where
        K: 'a,
    {
        #[cfg(automerge_verus)]
        proof_decl! {
            let ghost original=self.entries@;
            let ghost nodes=choose|nodes:Set<K>| #[trigger] supports_all(nodes,read,roots@);
        }
        let mut result = Summary::new();
        let mut i = 0;
        #[cfg(automerge_verus)]
        proof! { empty_prefix(read,roots@,view(&result)); }
        #[cfg_attr(automerge_verus, verus_spec(
            invariant memo_laws::<A,K>(), valid_entries(self,read), summary_inv(&result), i<=roots@.len(),
                original==old(self).entries@, supports_all(nodes,read,roots@), preserves(original,self.entries@),
                covers(read,|k:K| past_prefix(read,roots@,i as int,k),view(&result)),
            decreases roots@.len()-i,
        ))]
        while i < roots.len() {
            #[cfg(automerge_verus)]
            proof_decl! {
                let ghost prior=view(&result);
                let ghost cached=self.entries@;
                assert(roots@.contains(roots@[i as int]));
                assert(supports(nodes,read,roots@[i as int]));
            }
            let part = self.at(roots[i], read);
            result.merge(&part);
            #[cfg(automerge_verus)]
            proof! {
                extend_prefix(read,roots@,i as int,prior,view(&part),view(&result));
                extend_cache(original,cached,self.entries@,roots@[i as int],read,view(&part));
            }
            i += 1;
        }
        result
    }

    /// A hit reuses only the summary for this exact root. A miss is computed
    /// before extending the memo; no other entry is replaced.
    #[cfg_attr(automerge_verus, verus_spec(out =>
        requires memo_laws::<A,K>(), valid_entries(old(self), read),
            exists|nodes: Set<K>| #[trigger] supports(nodes, read, root),
        ensures valid_entries(final(self), read), summary_inv(&out), exact(read, root, view(&out)),
            cache_transition(old(self).entries@, final(self).entries@, root, read, view(&out)),
    ))]
    pub(super) fn at<'a>(&mut self, root: K, read: &impl Fn(K) -> Node<'a, A, K>) -> Summary<A, K>
    where
        K: 'a,
    {
        if let Some(saved) = self.entries.get(&root) {
            return saved.copy_exact();
        }
        #[cfg(automerge_verus)]
        proof_decl! { let ghost nodes = universe(read, root); assert(supports(nodes, read, root)); }
        #[cfg(automerge_verus)]
        proof! { assert(readable(read, root)); }
        let first = (*read)(root);
        #[cfg(automerge_verus)]
        proof! { assert(first == projected(read, root)); }
        if matches!(first, Node::Applied) {
            let mut result = Summary::new();
            result.seed(root);
            #[cfg(automerge_verus)]
            proof! { terminal_root(read, root, view(&result)); }
            return result;
        }

        let result = self.compute(root, read);
        #[cfg(automerge_verus)]
        proof_decl! { let ghost prior = self.entries@; }
        let saved = result.copy_exact();
        self.entries.insert(root, saved);
        #[cfg(automerge_verus)]
        proof! { inserted(prior, self.entries@, read, root, view(&result)); }
        result
    }

    #[cfg_attr(automerge_verus, verus_spec(out =>
        requires memo_laws::<A,K>(), valid_entries(self, read),
            exists|nodes: Set<K>| #[trigger] supports(nodes, read, root),
        ensures summary_inv(&out), exact(read, root, view(&out)),
    ))]
    fn compute<'a>(&self, root: K, read: &impl Fn(K) -> Node<'a, A, K>) -> Summary<A, K>
    where
        K: 'a,
    {
        #[cfg(automerge_verus)]
        proof_decl! {
            let ghost nodes = universe(read, root);
            assert(supports(nodes, read, root));
            let ghost mut covered = Set::<K>::empty();
        }
        let mut result = Summary::new();
        let mut order = Vec::new();
        let mut seen = HashSet::new();
        order.push(root);
        seen.insert(root);
        let mut cursor = 0;
        #[cfg(automerge_verus)]
        proof! { start(nodes, read, root, order@, seen@, view(&result)); }
        #[cfg_attr(automerge_verus, verus_spec(
            invariant memo_laws::<A,K>(), valid_entries(self,read), supports(nodes,read,root),
                hash_set_inv(&seen), summary_inv(&result), cursor <= order@.len() <= nodes.len(),
                settled(nodes,read,root,order@,seen@,cursor as int,covered,view(&result)),
            decreases nodes.len()-cursor,
        ))]
        while cursor < order.len() {
            let node = order[cursor];
            #[cfg(automerge_verus)]
            proof_decl! {
                let ghost old = view(&result);
                let ghost before = order@.take(cursor as int);
                let ghost after = order@.take(cursor as int + 1);
                begin_node(nodes, read, root, order@, seen@, cursor as int, covered, old);
            }
            cursor += 1;
            if let Some(saved) = self.entries.get(&node) {
                result.merge(saved);
                #[cfg(automerge_verus)]
                proof! {
                    covered = add_cached(nodes, read, root, node, order@, seen@, before, after, covered, old, view(saved), view(&result));
                }
            } else {
                #[cfg(automerge_verus)]
                proof! { assert(readable(read, node)); }
                let observed = (*read)(node);
                #[cfg(automerge_verus)]
                proof! { assert(observed == projected(read, node)); }
                match observed {
                    Node::Applied => {
                        result.seed(node);
                        #[cfg(automerge_verus)]
                        proof! {
                            add_one(nodes,read,root,node,order@,seen@,before,after,covered,old,view(&result));
                            covered = covered.insert(node);
                        }
                    }
                    Node::Candidate {
                        actor,
                        seq,
                        parents,
                    } => {
                        result.candidate(actor, seq);
                        let mut j = 0;
                        #[cfg_attr(automerge_verus, verus_spec(
                            invariant memo_laws::<A,K>(), supports(nodes,read,root), hash_set_inv(&seen),
                                summary_inv(&result), j <= parents@.len(), parents@ == dependencies(read,node),
                                prefix_work(nodes,read,root,order@,seen@,before,covered,old),
                                before.push(node) == after, order@.take(cursor as int) == after, cursor == after.len(), cursor <= order@.len(),
                                seen@.contains(node), join(old, candidate_part(read,node), view(&result)),
                                parents@.take(j as int).to_set().subset_of(seen@),
                            decreases parents@.len()-j,
                        ))]
                        while j < parents.len() {
                            let parent = parents[j];
                            #[cfg(automerge_verus)]
                            proof_decl! { let ghost prior_order = order@; let ghost prior_seen = seen@; }
                            if seen.insert(parent) {
                                order.push(parent);
                            }
                            #[cfg(automerge_verus)]
                            proof! {
                                enqueue_parent(nodes,read,root,node,j as int,prior_order,prior_seen,before,covered,old,order@,seen@);
                                parents@.lemma_take_succ_push(j as int);
                                assert(order@.len() >= prior_order.len());
                                assert(order@.take(cursor as int) =~= prior_order.take(cursor as int));
                            }
                            j += 1;
                        }
                        #[cfg(automerge_verus)]
                        proof! {
                            assert(parents@.take(j as int) =~= parents@);
                            add_one(nodes,read,root,node,order@,seen@,before,after,covered,old,view(&result));
                            covered = covered.insert(node);
                        }
                    }
                }
            }
            #[cfg(automerge_verus)]
            proof! { bound_order(nodes, order@, seen@); }
        }
        #[cfg(automerge_verus)]
        proof! {
            assert(order@.take(cursor as int) =~= order@);
            finish(nodes, read, root, order@, seen@, covered, view(&result));
        }
        result
    }
}

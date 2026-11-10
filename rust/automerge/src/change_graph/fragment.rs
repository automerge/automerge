//! Fragment boundaries and their cached causal coverage.

use std::collections::{HashMap, HashSet};
use std::ops::RangeBounds;

use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::clock::SeqClock;
use crate::ChangeHash;

use super::{ChangeGraph, Fragment, NodeIdx};

#[derive(Debug, Clone)]
pub(crate) struct FragmentCache {
    top: SeqClock,
    fragments: Vec<FragmentNode>,
}

pub(super) struct Candidate {
    fragment: FragmentNode,
    supersede: Vec<usize>,
}

impl FragmentCache {
    pub(super) fn new(actors: &ActorTable) -> Self {
        Self {
            top: SeqClock::new(actors),
            fragments: vec![],
        }
    }

    /// Construct an empty clock with the same actor slots as this cache.
    pub(super) fn empty_clock(&self) -> SeqClock {
        self.top.empty_like()
    }

    pub(super) fn insert_actor(&mut self, shift: &ActorShift) {
        self.top.shift_actor(shift);
        for fragment in &mut self.fragments {
            fragment.clock.shift_actor(shift);
        }
    }

    pub(super) fn remove_actor(&mut self, removal: &ActorRemoval) {
        self.top.remove_actor(removal);
        for fragment in &mut self.fragments {
            fragment.clock.remove_actor(removal);
        }
    }

    pub(super) fn candidate(
        &self,
        graph: &ChangeGraph,
        head: NodeIdx,
        deps: Vec<NodeIdx>,
    ) -> Candidate {
        let level = graph.hashes[head.0 as usize].fragment_level();
        debug_assert!(level > 0);
        let mut supersede = vec![];
        let clock = graph.calculate_clock([head].into());
        for (i, fragment) in self.fragments.iter().enumerate().rev() {
            // Cache compaction is independent of DAG-derived boundaries.
            if clock.covers(&fragment.clock)
                && graph.hashes[fragment.head.0 as usize].fragment_level() < level
            {
                supersede.push(i);
            }
        }
        Candidate {
            fragment: FragmentNode { head, deps, clock },
            supersede,
        }
    }

    pub(super) fn install(&mut self, candidate: Candidate) {
        for i in candidate.supersede {
            self.fragments.remove(i);
        }
        SeqClock::merge(&mut self.top, &candidate.fragment.clock);
        self.fragments.push(candidate.fragment);
    }

    pub(crate) fn get_fragment(&self, graph: &ChangeGraph, head: ChangeHash) -> Option<Fragment> {
        let n = graph.nodes_by_hash.get(&head)?;
        assert!(self.fragments.is_sorted_by(|a, b| a.head.0 < b.head.0));
        self.fragments
            .binary_search_by_key(&n.0, |f| f.head.0)
            .ok()
            .map(|i| self.fragments[i].export(graph))
    }

    pub(crate) fn fragments<'a, R: RangeBounds<usize> + 'a>(
        &'a self,
        graph: &'a ChangeGraph,
        levels: R,
    ) -> impl Iterator<Item = Fragment> + 'a {
        self.fragments_matching(graph, levels, |_| true)
    }

    pub(crate) fn fragments_matching<'a, R: RangeBounds<usize> + 'a>(
        &'a self,
        graph: &'a ChangeGraph,
        levels: R,
        include: impl Fn(ChangeHash) -> bool + 'a,
    ) -> impl Iterator<Item = Fragment> + 'a {
        let mut loose = levels
            .contains(&0)
            .then(|| self.loose_fragments(graph))
            .into_iter()
            .flatten();
        let mut cached = self.fragments.iter().rev();
        std::iter::from_fn(move || {
            // Selection must not prune traversal to a rejected head's ancestors.
            for fragment in loose.by_ref() {
                if include(fragment.head) {
                    return Some(fragment);
                }
            }
            for fragment in cached.by_ref() {
                let head = graph.hashes[fragment.head.0 as usize];
                if levels.contains(&head.fragment_level()) && include(head) {
                    return Some(fragment.export(graph));
                }
            }
            None
        })
    }

    fn loose_fragments<'a>(
        &'a self,
        graph: &'a ChangeGraph,
    ) -> impl Iterator<Item = Fragment> + 'a {
        let nodes = graph
            .heads()
            .filter_map(|h| graph.nodes_by_hash.get(&h).copied())
            .filter(|n| !self.covers(graph, *n));
        graph
            .bfs_until_clock(nodes, &self.top)
            .filter_map(|n| graph.loose_commit(n))
    }

    fn covers(&self, graph: &ChangeGraph, n: NodeIdx) -> bool {
        let actor = graph.actors[n.0 as usize].into();
        self.top
            .get_for_actor(&actor)
            .is_some_and(|seq| seq.get() >= graph.seq[n.0 as usize])
    }
}

/// A tracker for which fragments we have the necessary signatures to export
#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct FragmentReadiness {
    missing: HashMap<ChangeHash, HashSet<ChangeHash>>,
    waiting: HashMap<ChangeHash, HashSet<ChangeHash>>,
}

impl FragmentReadiness {
    /// Rebuild from scratch as an oracle for incremental-maintenance tests.
    #[cfg(test)]
    pub(crate) fn new(graph: &ChangeGraph, has_signature: impl FnMut(&ChangeHash) -> bool) -> Self {
        let mut readiness = Self::default();
        readiness.reconcile(graph, has_signature);
        readiness
    }

    /// Synchronize membership with the canonical cache without recomputing any
    /// surviving fragment's boundary clock or signature requirements. Existing
    /// requirements change only through `signature_added`, not later history.
    pub(crate) fn reconcile(
        &mut self,
        graph: &ChangeGraph,
        mut has_signature: impl FnMut(&ChangeHash) -> bool,
    ) {
        let current: HashSet<_> = graph
            .fragment_cache
            .fragments
            .iter()
            .map(|fragment| graph.hashes[fragment.head.0 as usize])
            .collect();
        let waiting = &mut self.waiting;
        self.missing.retain(|head, missing| {
            if current.contains(head) {
                return true;
            }
            for frontier in missing.iter() {
                let heads = waiting.get_mut(frontier).unwrap();
                heads.remove(head);
                if heads.is_empty() {
                    waiting.remove(frontier);
                }
            }
            false
        });

        // Always use the graph's canonical descriptors, including boundaries
        // whose own fragments cannot currently be attested.
        for fragment in &graph.fragment_cache.fragments {
            let head = graph.hashes[fragment.head.0 as usize];
            if self.missing.contains_key(&head) {
                continue;
            }
            let boundary = graph.calculate_clock(fragment.deps.iter().copied().collect());
            let mut missing = HashSet::new();
            for (actor, nodes) in graph.seq_index.iter().enumerate() {
                let seq = fragment.clock.get_for_actor(&actor);
                if seq <= boundary.get_for_actor(&actor) {
                    continue;
                }
                // Actor chains are linear. The maximum included sequence
                // attests all earlier included members of that actor.
                let frontier = graph.hashes[nodes[seq.unwrap().get() as usize - 1].0 as usize];
                if !has_signature(&frontier) {
                    self.waiting.entry(frontier).or_default().insert(head);
                    missing.insert(frontier);
                }
            }
            self.missing.insert(head, missing);
        }
    }

    /// Record first eligible evidence on a hash. No graph traversal or fragment
    /// reconstruction is needed; unrelated or duplicate notifications do nothing.
    pub(crate) fn signature_added(&mut self, hash: &ChangeHash) {
        if let Some(heads) = self.waiting.remove(hash) {
            for head in heads {
                let removed = self.missing.get_mut(&head).unwrap().remove(hash);
                debug_assert!(removed);
            }
        }
    }

    pub(crate) fn is_ready(&self, head: &ChangeHash) -> bool {
        self.missing.get(head).is_some_and(HashSet::is_empty)
    }

    pub(crate) fn fragments<'a, R: RangeBounds<usize> + 'a>(
        &'a self,
        graph: &'a ChangeGraph,
        levels: R,
    ) -> impl Iterator<Item = Fragment> + 'a {
        graph
            .fragment_cache
            .fragments_matching(graph, levels, |head| {
                head.fragment_level() == 0 || self.is_ready(&head)
            })
    }
}

#[derive(Debug, PartialEq, Clone)]
struct FragmentNode {
    head: NodeIdx,
    deps: Vec<NodeIdx>,
    clock: SeqClock,
}

impl FragmentNode {
    fn export(&self, graph: &ChangeGraph) -> Fragment {
        let head = graph.hashes[self.head.0 as usize];
        let level = head.fragment_level();
        let boundary = self
            .deps
            .iter()
            .map(|d| graph.hashes[d.0 as usize])
            .collect();
        let clock = graph.calculate_clock(self.deps.iter().copied().collect());
        let members: Vec<_> = graph.fragment_content(self.head, &clock).collect();
        let checkpoints = members
            .iter()
            .copied()
            .filter(|h| *h != head && h.fragment_level() > 0)
            .collect();
        Fragment {
            head,
            level,
            boundary,
            checkpoints,
            members,
        }
    }
}

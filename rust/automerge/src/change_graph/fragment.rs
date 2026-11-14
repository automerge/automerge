//! Fragment boundaries and their cached causal coverage.

use std::borrow::Cow;
use std::ops::RangeBounds;

use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::clock::SeqClock;
use crate::signature::FragmentRequirements;
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

    fn canonical_at(&self, head: NodeIdx) -> Option<&FragmentNode> {
        assert!(self.fragments.is_sorted_by(|a, b| a.head.0 < b.head.0));
        self.fragments
            .binary_search_by_key(&head, |f| f.head)
            .ok()
            .map(|i| &self.fragments[i])
    }

    fn historical_at(&self, graph: &ChangeGraph, head: NodeIdx) -> Cow<'_, FragmentNode> {
        if let Some(node) = self.canonical_at(head) {
            return Cow::Borrowed(node);
        }
        let level = graph.hashes[head.0 as usize].fragment_level();
        debug_assert!(level > 0);
        Cow::Owned(FragmentNode {
            head,
            deps: graph.fragment_boundary_slow(head, level),
            clock: graph.calculate_clock([head].into()),
        })
    }

    pub(crate) fn get_fragment(&self, graph: &ChangeGraph, head: ChangeHash) -> Option<Fragment> {
        let node = *graph.nodes_by_hash.get(&head)?;
        self.canonical_at(node).map(|f| f.export(graph))
    }

    pub(super) fn requirements(
        &self,
        graph: &ChangeGraph,
        head: ChangeHash,
    ) -> Option<FragmentRequirements> {
        let node = *graph.nodes_by_hash.get(&head)?;
        Some(self.historical_at(graph, node).requirements(graph))
    }

    #[allow(dead_code)]
    pub(super) fn get_historical_fragment(
        &self,
        graph: &ChangeGraph,
        head: ChangeHash,
    ) -> Option<Fragment> {
        let node = *graph.nodes_by_hash.get(&head)?;
        if let Some(cached) = self.canonical_at(node) {
            return Some(cached.export(graph));
        }
        let level = graph.hashes[node.0 as usize].fragment_level();
        debug_assert!(level > 0);
        let deps = graph.fragment_boundary_slow(node, level);
        Some(export_fragment(graph, node, &deps))
    }

    #[allow(dead_code)]
    pub(super) fn historical_members_all(
        &self,
        graph: &ChangeGraph,
        head: ChangeHash,
        predicate: impl FnMut(ChangeHash) -> bool,
    ) -> bool {
        let Some(&node) = graph.nodes_by_hash.get(&head) else {
            return false;
        };
        let deps: Cow<'_, [NodeIdx]> = if let Some(cached) = self.canonical_at(node) {
            Cow::Borrowed(&cached.deps)
        } else {
            let level = graph.hashes[node.0 as usize].fragment_level();
            debug_assert!(level > 0);
            Cow::Owned(graph.fragment_boundary_slow(node, level))
        };
        let boundary_clock = graph.calculate_clock(deps.iter().copied().collect());
        let covered = graph.fragment_content(node, &boundary_clock).all(predicate);
        covered
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

    pub(super) fn covers(&self, graph: &ChangeGraph, n: NodeIdx) -> bool {
        let actor = graph.actors[n.0 as usize].into();
        self.top
            .get_for_actor(&actor)
            .is_some_and(|seq| seq.get() >= graph.seq[n.0 as usize])
    }
}

#[derive(Debug, PartialEq, Clone)]
struct FragmentNode {
    head: NodeIdx,
    deps: Vec<NodeIdx>,
    clock: SeqClock,
}

impl FragmentNode {
    fn requirements(&self, graph: &ChangeGraph) -> FragmentRequirements {
        let boundary = graph.calculate_clock(self.deps.iter().copied().collect());
        // A fragment is its head's causal past minus its boundary's causal past.
        // Linear actor chains make each nonempty difference an actor interval.
        // Its maximum is sufficient; no member contents need reconstructing.
        FragmentRequirements::for_members(graph.seq_index.iter().enumerate().filter_map(
            |(actor, nodes)| {
                let seq = self.clock.get_for_actor(&actor)?;
                if Some(seq) <= boundary.get_for_actor(&actor) {
                    return None;
                }
                let hash = graph.hashes[nodes[seq.get() as usize - 1].0 as usize];
                Some((actor, u64::from(seq.get()), hash))
            },
        ))
    }

    fn export(&self, graph: &ChangeGraph) -> Fragment {
        export_fragment(graph, self.head, &self.deps)
    }
}

fn export_fragment(graph: &ChangeGraph, head_node: NodeIdx, deps: &[NodeIdx]) -> Fragment {
    let head = graph.hashes[head_node.0 as usize];
    let level = head.fragment_level();
    let boundary = deps.iter().map(|d| graph.hashes[d.0 as usize]).collect();
    let clock = graph.calculate_clock(deps.iter().copied().collect());
    let members: Vec<_> = graph.fragment_content(head_node, &clock).collect();
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

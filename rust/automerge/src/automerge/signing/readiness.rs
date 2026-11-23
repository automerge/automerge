//! Signing readiness tracking and outgoing fragment cover selection.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::change_graph::ChangeGraph;
use crate::{ChangeHash, Fragment};

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
struct CandidateFrontiers {
    required: Vec<ChangeHash>,
    missing: HashSet<ChangeHash>,
}

/// Tracker for which hard-head fragments have their required frontier signatures.
#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(super) struct FragmentReadiness {
    /// For each registered fragment head, its fixed required frontiers and remaining missing subset.
    candidates_by_head: HashMap<ChangeHash, CandidateFrontiers>,
    /// Reverse index from each unsigned frontier hash to the fragment heads waiting on it.
    heads_waiting_for_frontier: HashMap<ChangeHash, HashSet<ChangeHash>>,
}

impl FragmentReadiness {
    /// Register a newly applied hard head without scanning existing candidates.
    pub(super) fn register(
        &mut self,
        graph: &ChangeGraph,
        head: ChangeHash,
        mut has_signature: impl FnMut(&ChangeHash) -> bool,
    ) {
        if head.fragment_level() == 0 || self.candidates_by_head.contains_key(&head) {
            return;
        }
        let requirements = graph
            .fragment_requirements(head)
            .expect("registered fragment belongs to the applied graph");
        let required: Vec<ChangeHash> = requirements.hashes().copied().collect();
        let mut missing = HashSet::new();
        for &frontier in &required {
            if !has_signature(&frontier) {
                self.heads_waiting_for_frontier
                    .entry(frontier)
                    .or_default()
                    .insert(head);
                missing.insert(frontier);
            }
        }
        self.candidates_by_head
            .insert(head, CandidateFrontiers { required, missing });
    }

    fn forget(&mut self, head: &ChangeHash) {
        if let Some(candidate) = self.candidates_by_head.remove(head) {
            for frontier in candidate.missing {
                let heads = self.heads_waiting_for_frontier.get_mut(&frontier).unwrap();
                heads.remove(head);
                if heads.is_empty() {
                    self.heads_waiting_for_frontier.remove(&frontier);
                }
            }
        }
    }

    /// Forget candidates whose complete membership is represented by the
    /// *published* proved cover, returning that cover's member set.
    ///
    /// A candidate that is merely signature-ready may still be exportable, but
    /// it cannot retire an older candidate: only a cover whose every member has
    /// been published (per `is_published`) counts as a replacement. Callers
    /// invoke this on every reclamation pass, so publication-only progress (no
    /// new evidence) still prunes.
    pub(super) fn prune_covered(
        &mut self,
        graph: &ChangeGraph,
        is_published: impl FnMut(&ChangeHash) -> bool,
    ) -> HashSet<ChangeHash> {
        let (selected, represented) =
            OutgoingSelection::new(graph, self).select_cover(is_published);
        let selected_heads: HashSet<_> = selected.iter().map(|fragment| fragment.head).collect();
        let obsolete: Vec<_> = self
            .candidates_by_head
            .keys()
            .filter(|head| represented.contains(head) && !selected_heads.contains(head))
            .filter(|head| {
                graph.historical_members_all(**head, |member| represented.contains(&member))
            })
            .copied()
            .collect();
        for head in obsolete {
            self.forget(&head);
        }
        represented
    }

    /// Iterate over every required frontier hash across all currently registered candidates.
    pub(super) fn required_frontiers(&self) -> impl Iterator<Item = &ChangeHash> {
        self.candidates_by_head
            .values()
            .flat_map(|candidate| candidate.required.iter())
    }

    /// Record first eligible evidence for `frontier`.
    pub(super) fn signature_added(&mut self, frontier: &ChangeHash) {
        if let Some(heads) = self.heads_waiting_for_frontier.remove(frontier) {
            for head in heads {
                let candidate = self.candidates_by_head.get_mut(&head).unwrap();
                let removed = candidate.missing.remove(frontier);
                debug_assert!(removed);
            }
        }
    }

    pub(super) fn is_ready(&self, head: &ChangeHash) -> bool {
        self.candidates_by_head
            .get(head)
            .is_some_and(|c| c.missing.is_empty())
    }

    #[cfg(test)]
    pub(super) fn from_graph(
        graph: &ChangeGraph,
        mut has_signature: impl FnMut(&ChangeHash) -> bool,
    ) -> Self {
        let mut readiness = Self::default();
        for head in graph.applied_hashes() {
            readiness.register(graph, *head, &mut has_signature);
        }
        readiness
    }
}

/// Select an independently proved outgoing cover (newest to oldest) without
/// mutating structural fragment decomposition.
pub(super) struct OutgoingSelection<'a> {
    graph: &'a ChangeGraph,
    readiness: &'a FragmentReadiness,
}

impl<'a> OutgoingSelection<'a> {
    pub(super) fn new(graph: &'a ChangeGraph, readiness: &'a FragmentReadiness) -> Self {
        Self { graph, readiness }
    }

    fn select_cover(
        &self,
        mut eligible: impl FnMut(&ChangeHash) -> bool,
    ) -> (Vec<Fragment>, HashSet<ChangeHash>) {
        let mut candidates: Vec<_> = self.readiness.candidates_by_head.keys().copied().collect();
        candidates.sort_unstable_by_key(|head| Reverse(self.graph.node_rank(head).unwrap()));
        let mut selected = Vec::new();
        let mut represented = HashSet::new();
        for head in candidates {
            if !self.readiness.is_ready(&head) || !eligible(&head) {
                continue;
            }
            let fragment = self.graph.get_historical_fragment(head).unwrap();
            if fragment
                .members
                .iter()
                .any(|hash| !represented.contains(hash))
            {
                represented.extend(fragment.members.iter().copied());
                selected.push(fragment);
            }
        }
        (selected, represented)
    }

    pub(super) fn collect(&self, has_signature: impl FnMut(&ChangeHash) -> bool) -> Vec<Fragment> {
        self.collect_from(self.graph.heads(), |_| true, has_signature)
    }

    /// Collect a proved cover within an eligibility boundary, starting from its
    /// heads. Filtering precedes cover selection, preserving published fallbacks.
    pub(super) fn collect_from(
        &self,
        heads: impl IntoIterator<Item = ChangeHash>,
        mut eligible: impl FnMut(&ChangeHash) -> bool,
        mut has_signature: impl FnMut(&ChangeHash) -> bool,
    ) -> Vec<Fragment> {
        let (mut result, represented) = self.select_cover(&mut eligible);
        let seeds = heads.into_iter().chain(
            result
                .iter()
                .flat_map(|fragment| fragment.boundary.iter().copied()),
        );
        let mut pending: BinaryHeap<(u32, ChangeHash)> = seeds
            .map(|hash| (self.graph.node_rank(&hash).unwrap(), hash))
            .collect();
        let mut visited = HashSet::new();
        while let Some((_, hash)) = pending.pop() {
            if !eligible(&hash) || represented.contains(&hash) || !visited.insert(hash) {
                continue;
            }
            for parent in self.graph.deps(&hash) {
                pending.push((self.graph.node_rank(&parent).unwrap(), parent));
            }
            if hash.fragment_level() == 0 && has_signature(&hash) {
                result.push(self.graph.get_fragment(hash).unwrap());
            }
        }
        result.sort_unstable_by_key(|fragment| Reverse(self.graph.node_rank(&fragment.head)));
        result
    }
}

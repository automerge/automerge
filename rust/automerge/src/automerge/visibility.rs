//! The [`FrontierVisibility`] that is derived from a [`WriteFrontier`].

use std::collections::HashMap;

use super::view::Mask;
use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::author::Authors;
use crate::change_graph::{ChangeGraph, NodeIdx};
use crate::clock::Clock;
use crate::hash_retention::HashRetention;
use crate::types::OpId;
use crate::write_frontier::{Boundary, WriteFrontier};

/// The visibility derived from a write-frontier policy.
#[derive(Clone, Debug, Default)]
pub(crate) struct FrontierVisibility {
    /// `None` when the write-frontier restricts no actor.
    pub(super) mask: Option<Mask>,
    /// Boundaries named by the policy that the graph does not hold yet.
    pub(super) pending: Vec<Boundary>,
}

impl FrontierVisibility {
    /// For each masked author, the author's actors are bounded at their max op
    /// in the history of the boundaries `graph` holds (`0` when they have
    /// none, including when it holds none of them), and the boundaries it
    /// does not hold are pending. Unmasked actors are unrestricted.
    pub(super) fn new<H: HashRetention>(
        policy: &WriteFrontier,
        graph: &ChangeGraph<H>,
        authors: &Authors,
        actors: &ActorTable,
    ) -> FrontierVisibility {
        Self::resolving(policy, graph, authors, actors, |_, _| true)
    }

    /// As [`Self::new`], counting a boundary the graph holds as pending unless
    /// `resolved` accepts it.
    pub(super) fn resolving<H: HashRetention>(
        policy: &WriteFrontier,
        graph: &ChangeGraph<H>,
        authors: &Authors,
        actors: &ActorTable,
        resolved: impl Fn(&Boundary, NodeIdx) -> bool,
    ) -> FrontierVisibility {
        if policy.is_empty() {
            return FrontierVisibility::default();
        }
        let mut pending = Vec::new();
        let mut bounds = vec![u32::MAX; actors.len()];
        // authors are commonly bounded at the same heads, and each clock is a
        // graph traversal
        let mut clocks: HashMap<Vec<NodeIdx>, Clock> = HashMap::new();
        for (author, heads) in policy.iter() {
            let mut nodes = Vec::with_capacity(heads.len());
            for boundary in heads {
                match node_for_boundary(boundary, graph, actors)
                    .filter(|node| resolved(boundary, *node))
                {
                    Some(node) => nodes.push(node),
                    None => pending.push(boundary.clone()),
                }
            }
            nodes.sort_unstable();
            nodes.dedup();
            let clock = clocks
                .entry(nodes)
                .or_insert_with_key(|nodes| graph.clock_for_nodes(nodes.clone()));
            for actor in authors.get_actors_for_author(author) {
                bounds[actor] = clock.max_op(actor) as u32;
            }
        }
        // a policy whose authors have no actors yet restricts nothing
        let mask = bounds
            .iter()
            .any(|b| *b != u32::MAX)
            .then(|| Mask::new(Clock::from_actor_fn(actors, |actor| bounds[actor])));
        FrontierVisibility { mask, pending }
    }

    pub(super) fn mask(&self) -> Option<&Mask> {
        self.mask.as_ref()
    }

    pub(super) fn hides(&self, id: &OpId) -> bool {
        self.mask.as_ref().is_some_and(|m| m.hides(id))
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(super) fn insert_actor(&mut self, shift: &ActorShift) {
        if let Some(mask) = self.mask.as_mut() {
            mask.insert_actor(shift);
        }
    }

    pub(super) fn remove_actor(&mut self, removal: &ActorRemoval) {
        if let Some(mask) = self.mask.as_mut() {
            mask.remove_actor(removal);
        }
    }
}

pub(super) fn node_for_boundary<H: HashRetention>(
    boundary: &Boundary,
    graph: &ChangeGraph<H>,
    actors: &ActorTable,
) -> Option<NodeIdx> {
    match boundary {
        Boundary::Id(id) => graph.node_for_change_id(id, actors),
        Boundary::Hash(hash) => graph.node_for_hash(hash),
    }
}

#[cfg(test)]
mod tests {
    //! Unit and lockstep tests for [`FrontierVisibility::new`], driven with
    //! real change graphs built through [`AutoCommit`]: the expected bounds
    //! are computed independently from the generated commit history (the max op
    //! counter of the change at the boundary), never by calling the graph's own
    //! clock translation.

    use super::FrontierVisibility;
    use crate::automerge::view::Mask;
    use crate::clock::Clock;
    use crate::next::transaction::Transactable;
    use crate::next::AuditedAutoCommit as AutoCommit;
    use crate::write_frontier::{Boundary, WriteFrontier};
    use crate::{Author, ChangeHash, ROOT};
    use proptest::prelude::*;
    use std::collections::{HashMap, HashSet};

    fn pending_hashes(derived: &FrontierVisibility) -> HashSet<ChangeHash> {
        derived
            .pending
            .iter()
            .map(|b| match b {
                Boundary::Hash(h) => *h,
                Boundary::Id(_) => panic!("only hash boundaries in these policies"),
            })
            .collect()
    }

    /// The [`Author`] for author number `n` (a one-byte identity).
    fn author(n: usize) -> Author<'static> {
        Author::from(vec![n as u8 + 1])
    }

    /// A hash that no generated change can plausibly collide with.
    fn unknown_hash(n: u8) -> ChangeHash {
        ChangeHash([n; 32])
    }

    /// A document built from real commits: author `a`'s changes are
    /// contiguous, so each author maps to exactly one actor.
    struct Built {
        doc: AutoCommit,
        /// Per commit, in order: (author number, change hash, max op).
        commits: Vec<(usize, ChangeHash, u32)>,
    }

    /// Commit `counts[a]` single-op changes for each author `a`, in order.
    fn build(counts: &[usize]) -> Built {
        let mut doc = crate::next::AutoCommit::new().enable_audit_mode().unwrap();
        let mut commits = Vec::new();
        let mut value = 0i64;
        for (a, &n) in counts.iter().enumerate() {
            doc.set_author(Some(author(a)));
            for _ in 0..n {
                value += 1;
                doc.put(ROOT, "k", value).unwrap();
                doc.commit();
                let head = doc.get_heads()[0].clone();
                let hash = doc.document().get_hash(&head).unwrap();
                let max_op = doc.document().change_graph.max_op() as u32;
                commits.push((a, hash, max_op));
            }
        }
        Built { doc, commits }
    }

    impl Built {
        /// The single actor index of author `a` (commits are contiguous).
        fn actor_of(&mut self, a: usize) -> usize {
            let actors: Vec<usize> = self
                .doc
                .document()
                .authors
                .get_actors_for_author(&author(a))
                .collect();
            assert_eq!(actors.len(), 1, "one actor per author by construction");
            actors[0]
        }

        /// Run [`FrontierVisibility::new`] over the real graph with the given policy.
        fn frontier_visibility(&mut self, policy: &WriteFrontier) -> FrontierVisibility {
            let doc = self.doc.document();
            FrontierVisibility::new(policy, &doc.change_graph, &doc.authors, &doc.ops.actors)
        }

        fn num_actors(&mut self) -> usize {
            self.doc.document().ops.actors.len()
        }
    }

    fn policy(entries: Vec<(usize, Vec<ChangeHash>)>) -> WriteFrontier {
        WriteFrontier::from_hashes(
            entries
                .into_iter()
                .map(|(a, heads)| (author(a), heads))
                .collect::<HashMap<_, _>>(),
        )
    }

    /// A known single-head boundary bounds the author's actor at the
    /// boundary change's max op; other authors stay unrestricted.
    #[test]
    fn known_boundary_bounds_actor_at_the_changes_max_op() {
        let mut built = build(&[3, 1]);
        let (_, c2, c2_max_op) = built.commits[1];
        let derived = built.frontier_visibility(&policy(vec![(0, vec![c2])]));
        let mut expected = vec![u32::MAX; built.num_actors()];
        expected[built.actor_of(0)] = c2_max_op;
        assert_eq!(
            derived.mask,
            Some(Mask::new(Clock::from_counters(
                expected.into_iter().map(Some)
            )))
        );
        assert!(derived.pending.is_empty());
    }

    /// A boundary head missing from the graph hides the author entirely
    /// and is reported as pending.
    #[test]
    fn unknown_boundary_hides_author_and_is_pending() {
        let mut built = build(&[3, 1]);
        let unknown = unknown_hash(0xEE);
        let derived = built.frontier_visibility(&policy(vec![(0, vec![unknown])]));
        let mut expected = vec![u32::MAX; built.num_actors()];
        expected[built.actor_of(0)] = 0;
        assert_eq!(
            derived.mask,
            Some(Mask::new(Clock::from_counters(
                expected.into_iter().map(Some)
            )))
        );
        assert_eq!(pending_hashes(&derived), HashSet::from([unknown]));
    }

    /// Partially known heads reveal only the author's known boundary history.
    #[test]
    fn partially_unknown_heads_bound_author_at_known_heads() {
        let mut built = build(&[3, 1]);
        let (_, c2, c2_max_op) = built.commits[1];
        let unknown = unknown_hash(0xEE);
        let derived = built.frontier_visibility(&policy(vec![(0, vec![c2, unknown])]));
        let mut expected = vec![u32::MAX; built.num_actors()];
        expected[built.actor_of(0)] = c2_max_op;
        assert_eq!(
            derived.mask,
            Some(Mask::new(Clock::from_counters(
                expected.into_iter().map(Some)
            )))
        );
        assert_eq!(pending_hashes(&derived), HashSet::from([unknown]));
    }

    /// No write-frontier means no mask and nothing pending.
    #[test]
    fn empty_policy_derives_no_mask() {
        let mut built = build(&[3, 1]);
        let derived = built.frontier_visibility(&WriteFrontier::new());
        assert_eq!(derived.mask, None);
        assert!(derived.pending.is_empty());
    }

    /// A masked author with no actors contributes no entry, but the
    /// mask is still `Some` when another author is masked.
    #[test]
    fn author_without_actors_contributes_no_entry() {
        let mut built = build(&[3, 1]);
        let (_, c2, c2_max_op) = built.commits[1];
        let derived = built.frontier_visibility(&policy(vec![(0, vec![c2]), (7, vec![c2])]));
        let mut expected = vec![u32::MAX; built.num_actors()];
        expected[built.actor_of(0)] = c2_max_op;
        assert_eq!(
            derived.mask,
            Some(Mask::new(Clock::from_counters(
                expected.into_iter().map(Some)
            )))
        );
        assert!(derived.pending.is_empty());
    }

    /// One boundary head in a generated policy: either a known commit
    /// (by index into the linear history) or a hash absent from the graph.
    #[derive(Debug, Clone)]
    enum Head {
        Known(usize),
        Unknown(u8),
    }

    fn gen_heads() -> impl Strategy<Value = Vec<Head>> {
        proptest::collection::vec(
            prop_oneof![
                any::<usize>().prop_map(Head::Known),
                (0..8u8).prop_map(Head::Unknown),
            ],
            1..=2,
        )
    }

    /// 2–3 authors × 1–4 changes each, plus a per-author optional boundary.
    fn gen_case() -> impl Strategy<Value = (Vec<usize>, Vec<Option<Vec<Head>>>)> {
        proptest::collection::vec(1..=4usize, 2..=3).prop_flat_map(|counts| {
            let n = counts.len();
            (
                Just(counts),
                proptest::collection::vec(proptest::option::of(gen_heads()), n),
            )
        })
    }

    proptest! {
        /// Lockstep over real graphs: the expected bound for a masked
        /// author is the max op of their last change at or before a known
        /// boundary head (the history is linear, so a multi-head boundary
        /// reduces to its latest known commit), or 0 when they have none;
        /// unmasked authors are unrestricted.
        #[test]
        fn lockstep_matches_generated_history((counts, boundaries) in gen_case()) {
            let mut built = build(&counts);
            let mut map = HashMap::new();
            let mut expected_pending = HashSet::new();
            let mut expected_bounds: HashMap<usize, u32> = HashMap::new();
            for (a, heads) in boundaries.iter().enumerate() {
                let Some(heads) = heads else { continue };
                let mut resolved = Vec::new();
                let mut boundary_idx = None;
                for head in heads {
                    match head {
                        Head::Known(i) => {
                            let i = i % built.commits.len();
                            resolved.push(built.commits[i].1);
                            boundary_idx =
                                Some(boundary_idx.map_or(i, |b: usize| b.max(i)));
                        }
                        Head::Unknown(n) => {
                            let hash = unknown_hash(*n);
                            resolved.push(hash);
                            expected_pending.insert(hash);
                        }
                    }
                }
                // The last change by `a` at or before any known head.
                let bound = boundary_idx
                    .and_then(|idx| {
                        built.commits[..=idx]
                            .iter()
                            .rev()
                            .find(|(ca, _, _)| *ca == a)
                    })
                    .map(|(_, _, max_op)| *max_op)
                    .unwrap_or(0);
                expected_bounds.insert(a, bound);
                map.insert(author(a), resolved);
            }

            let is_empty = map.is_empty();
            let derived = built.frontier_visibility(&WriteFrontier::from_hashes(map));
            if is_empty {
                prop_assert_eq!(derived.mask, None);
                prop_assert!(derived.pending.is_empty());
            } else {
                let mut expected = vec![u32::MAX; built.num_actors()];
                for (a, bound) in expected_bounds {
                    expected[built.actor_of(a)] = bound;
                }
                prop_assert_eq!(derived.mask.clone(), Some(Mask::new(Clock::from_counters(expected.into_iter().map(Some)))));
                prop_assert_eq!(pending_hashes(&derived), expected_pending);
            }
        }
    }
}

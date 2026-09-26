//! The [`FrontierVisibility`] that is derived from a [`WriteFrontier`].

use std::collections::HashSet;

use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::clock::{Clock, Mask};
use crate::types::OpId;
use crate::write_frontier::WriteFrontier;
use crate::ChangeHash;

/// The visibility derived from a write-frontier policy.
#[derive(Clone, Debug)]
pub(super) struct FrontierVisibility {
    /// When `mask` is `None`, this implies that there is no write-frontier.
    pub(super) mask: Option<Mask>,
    /// Boundary hashes referenced by the policy and absent from the graph.
    pub(super) pending: HashSet<ChangeHash>,
}

impl FrontierVisibility {
    /// Derive the visibility for `policy` over the document's graph and
    /// authors. For each author and heads pair in the policy:
    ///
    /// - If any head is missing from `graph`, then every actor connected to
    ///   `author` gets bound to `0`. This means that they are hidden.
    ///   The missing hashes are added to [`FrontierVisibility::pending`].
    /// - Otherwise, the bound is the max op of the actor's change at the
    ///   boundary sequence number, or `0` when the actor has no change at or
    ///   before the boundary, i.e. it has no visible ops.
    /// - Actors of unmasked authors are unrestricted.
    pub(super) fn new(
        policy: &WriteFrontier,
        graph: &ChangeGraph,
        authors: &Authors,
        actors: &ActorTable,
    ) -> FrontierVisibility {
        if policy.is_empty() {
            return FrontierVisibility {
                mask: None,
                pending: HashSet::new(),
            };
        }
        let mut pending = HashSet::new();
        let mut bounds = vec![u32::MAX; actors.len()];
        for (author, heads) in policy.get_write_frontier() {
            let missing: Vec<ChangeHash> = graph.missing_hashes(heads).collect();
            if missing.is_empty() {
                let clock = graph.seq_clock_for_heads(heads);
                for actor in authors.get_actors_for_author(author) {
                    bounds[actor] = match clock.get_for_actor(&actor) {
                        // A seq the graph handed out always names a change, so
                        // the translation cannot miss; 0 is unreachable here
                        // but hides rather than reveals.
                        Some(seq) => graph.max_op_for_seq(actor, seq).unwrap_or(0),
                        // No change at or before the boundary: nothing visible.
                        None => 0,
                    };
                }
            } else {
                pending.extend(missing);
                for actor in authors.get_actors_for_author(author) {
                    bounds[actor] = 0;
                }
            }
        }
        let mask = Mask::new(Clock::from_actor_fn(actors, |actor| bounds[actor]));
        FrontierVisibility {
            mask: Some(mask),
            pending,
        }
    }

    pub(super) fn mask(&self) -> Option<&Mask> {
        self.mask.as_ref()
    }

    pub(super) fn hides(&self, id: &OpId) -> bool {
        self.mask.as_ref().is_some_and(|m| m.hides(id))
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

    pub(super) fn is_pending(&self, hash: &ChangeHash) -> bool {
        self.pending.contains(hash)
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
    use crate::clock::{Clock, Mask};
    use crate::transaction::Transactable;
    use crate::write_frontier::WriteFrontier;
    use crate::{Author, AutoCommit, ChangeHash, ROOT};
    use proptest::prelude::*;
    use std::collections::{HashMap, HashSet};

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
        let mut doc = AutoCommit::new();
        let mut commits = Vec::new();
        let mut value = 0i64;
        for (a, &n) in counts.iter().enumerate() {
            doc.set_author(Some(author(a)));
            for _ in 0..n {
                value += 1;
                doc.put(ROOT, "k", value).unwrap();
                doc.commit();
                let hash = doc.get_heads()[0];
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

        /// Run [`derive`] over the real graph with the given policy.
        fn frontier_visibility(&mut self, policy: &WriteFrontier) -> FrontierVisibility {
            let doc = self.doc.document();
            FrontierVisibility::new(policy, &doc.change_graph, &doc.authors, doc.actors())
        }

        fn num_actors(&mut self) -> usize {
            self.doc.document().actors().len()
        }
    }

    fn policy(entries: Vec<(usize, Vec<ChangeHash>)>) -> WriteFrontier {
        WriteFrontier::from(
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
        assert_eq!(derived.pending, HashSet::from([unknown]));
    }

    /// Partially known heads hide the author entirely: deriving from
    /// the known subset would partially reveal them.
    #[test]
    fn partially_unknown_heads_hide_author_entirely() {
        let mut built = build(&[3, 1]);
        let (_, c2, _) = built.commits[1];
        let unknown = unknown_hash(0xEE);
        let derived = built.frontier_visibility(&policy(vec![(0, vec![c2, unknown])]));
        let mut expected = vec![u32::MAX; built.num_actors()];
        expected[built.actor_of(0)] = 0;
        assert_eq!(
            derived.mask,
            Some(Mask::new(Clock::from_counters(
                expected.into_iter().map(Some)
            )))
        );
        assert_eq!(derived.pending, HashSet::from([unknown]));
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
        /// author is the max op of their last change at or before the
        /// boundary (the history is linear, so a multi-head boundary
        /// reduces to its latest commit), 0 when they have none or when
        /// any head is unknown; unmasked authors are unrestricted.
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
                let mut missing = false;
                for head in heads {
                    match head {
                        Head::Known(i) => {
                            let i = i % built.commits.len();
                            resolved.push(built.commits[i].1);
                            boundary_idx =
                                Some(boundary_idx.map_or(i, |b: usize| b.max(i)));
                        }
                        Head::Unknown(n) => {
                            resolved.push(unknown_hash(*n));
                            missing = true;
                        }
                    }
                }
                let bound = if missing {
                    for h in &resolved {
                        if !built.commits.iter().any(|(_, ch, _)| ch == h) {
                            expected_pending.insert(*h);
                        }
                    }
                    0
                } else {
                    // The last change by `a` at or before the boundary.
                    built.commits[..=boundary_idx.unwrap()]
                        .iter()
                        .rev()
                        .find(|(ca, _, _)| *ca == a)
                        .map(|(_, _, max_op)| *max_op)
                        .unwrap_or(0)
                };
                expected_bounds.insert(a, bound);
                map.insert(author(a), resolved);
            }

            let is_empty = map.is_empty();
            let derived = built.frontier_visibility(&WriteFrontier::from(map));
            if is_empty {
                prop_assert_eq!(derived.mask, None);
                prop_assert!(derived.pending.is_empty());
            } else {
                let mut expected = vec![u32::MAX; built.num_actors()];
                for (a, bound) in expected_bounds {
                    expected[built.actor_of(a)] = bound;
                }
                prop_assert_eq!(derived.mask, Some(Mask::new(Clock::from_counters(expected.into_iter().map(Some)))));
                prop_assert_eq!(derived.pending, expected_pending);
            }
        }
    }
}

use std::collections::{HashSet, VecDeque};

use amp::ChangeHash;
use automerge_protocol as amp;

use super::Backend;
use crate::{vector_clock::VectorClock, Change};

impl Backend {
    /// Get the list of changes not covered by `have_deps`.
    ///
    /// This _fast_ variant makes use of the topological sort of the graph to handle cases where
    /// there are either no concurrent changes since the `have_deps` or all of the concurrent
    /// changes are to the right of them in the sort.
    ///
    /// This strategy avoids us having to do lots of work when we may be playing catchup (no
    /// changes ourelves but just streaming them from someone else).
    pub(super) fn get_changes_fast(&self, have_deps: &[amp::ChangeHash]) -> Option<Vec<&Change>> {
        if have_deps.is_empty() {
            return Some(self.history.iter().collect());
        }

        let lowest_idx = have_deps
            .iter()
            .filter_map(|h| self.history_index.get(h))
            .min()?
            + 1;

        let mut missing_changes = vec![];
        let mut has_seen: HashSet<_> = have_deps.iter().collect();
        for change in &self.history[lowest_idx..] {
            let deps_seen = change.deps.iter().filter(|h| has_seen.contains(h)).count();
            if deps_seen > 0 {
                if deps_seen != change.deps.len() {
                    // future change depends on something we haven't seen - fast path cant work
                    return None;
                }
                missing_changes.push(change);
                has_seen.insert(&change.hash);
            }
        }

        // if we get to the end and there is a head we haven't seen then fast path cant work
        if self.get_heads().iter().all(|h| has_seen.contains(h)) {
            Some(missing_changes)
        } else {
            None
        }
    }

    /// Get the list of changes that are not transitive dependencies of `heads` using a vector
    /// clock.
    ///
    /// This aims to be more predictable than versions that operate on the hash graph which
    /// sometimes need to do big graph traversals.
    pub(super) fn get_changes_vector_clock(&self, heads: &[amp::ChangeHash]) -> Vec<&Change> {
        // get the vector clock representing the state of the graph with the given heads
        //
        // heads that are not in our graph do not contribute to the clock
        let clock = self.get_vector_clock_at(heads);

        let mut change_indices = Vec::new();

        for (actor, indices) in &self.states {
            if let Some(index) = clock.get_seq(actor) {
                change_indices.extend(indices[index as usize..].iter().copied());
            } else {
                change_indices.extend(indices);
            }
        }

        // make them into topological sorted order
        change_indices.sort_unstable();

        change_indices
            .into_iter()
            .map(|i| &self.history[i])
            .collect()
    }

    /// Get the vector clock for the state of the graph with these heads.
    pub(super) fn get_vector_clock_at(&self, heads: &[amp::ChangeHash]) -> VectorClock {
        let mut clock = VectorClock::default();

        // get the clock for each head individually and combine them
        for hash in heads {
            let found_clock = self.get_vector_clock_for_hash(hash);
            clock += found_clock;
        }

        clock
    }

    /// Get the vector clock for the given hash.
    pub(super) fn get_vector_clock_for_hash(&self, hash: &amp::ChangeHash) -> VectorClock {
        // we need to do a BFS through the hash graph to ensure we get nodes at the highest seq for
        // each actor id
        let mut queue = VecDeque::new();
        queue.push_back(hash);

        let mut has_seen = HashSet::new();

        let mut clock = VectorClock::default();

        // we'll be needing this for the duration so just lock it once
        let mut clocks_cache = self.clocks_cache.lock().unwrap();

        while let Some(hash) = queue.pop_front() {
            // since the graph is immutable the vector clock will not be changing so we can use the
            // cached clock of a hash to skip having to traverse its dependencies
            if let Some(cached_clock) = clocks_cache.get(hash) {
                clock += cached_clock;
                // don't add the changes dependencies to the queue as we already have the clock
                // contribution from this subgraph
                continue;
            } else if has_seen.contains(&hash) {
                continue;
            }

            if let Some(change) = self
                .history_index
                .get(hash)
                .and_then(|i| self.history.get(*i))
            {
                queue.extend(change.deps.iter());
                clock.update(change.actor_id(), change.seq);
            }

            if clock.len() == self.states.len() {
                // we've found all the actors so can stop
                //
                // This prevents us from going down all the way to the root of the graph
                // unnecessarily when we have clock entries for everyone
                break;
            }

            has_seen.insert(hash);
        }

        // cache the clock for this hash
        clocks_cache.insert(*hash, clock.clone());

        clock
    }

    /// Filter the changes down to those that are not transitive dependencies of the heads.
    ///
    /// Thus a graph with these heads has not seen the remaining changes.
    pub(crate) fn filter_changes(
        &self,
        heads: &[amp::ChangeHash],
        changes: &mut HashSet<amp::ChangeHash>,
    ) {
        // Reduce the working set to find to those which we may be able to find.
        // This filters out those hashes that are successors of or concurrent with all of the
        // heads.
        // This can help in avoiding traversing the entire graph back to the roots when we try to
        // search for a hash we can know won't be found there.
        let max_head_index = heads
            .iter()
            .map(|h| self.history_index.get(h).unwrap_or(&0))
            .max()
            .unwrap_or(&0);
        let may_find: HashSet<ChangeHash> = changes
            .iter()
            .filter(|hash| {
                let change_index = self.history_index.get(hash).unwrap_or(&0);
                change_index <= max_head_index
            })
            .copied()
            .collect();

        if may_find.is_empty() {
            return;
        }

        let clock = self.get_vector_clock_at(heads);
        for hash in may_find {
            if let Some(change) = self.get_change_by_hash(&hash) {
                if let Some(s) = clock.get_seq(change.actor_id()) {
                    if change.seq <= s {
                        changes.remove(&hash);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use amp::SortedVec;
    use automerge_protocol::{ActorId, ObjectId, Op, OpType};

    use super::*;

    #[test]
    fn test_get_changes_fast_behavior() {
        let actor_a: ActorId = "7b7723afd9e6480397a4d467b7693156".try_into().unwrap();
        let actor_b: ActorId = "37704788917a499cb0206fa8519ac4d9".try_into().unwrap();
        let change_a1: Change = amp::Change {
            actor_id: actor_a.clone(),
            seq: 1,
            start_op: 1,
            time: 0,
            message: None,
            hash: None,
            deps: Vec::new(),
            operations: vec![Op {
                obj: ObjectId::Root,
                action: OpType::Set("magpie".into()),
                key: "bird".into(),
                insert: false,
                pred: SortedVec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let change_a2: Change = amp::Change {
            actor_id: actor_a,
            seq: 2,
            start_op: 2,
            time: 0,
            message: None,
            hash: None,
            deps: vec![change_a1.hash],
            operations: vec![Op {
                obj: ObjectId::Root,
                action: OpType::Set("ant".into()),
                key: "bug".into(),
                insert: false,
                pred: SortedVec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let change_b1: Change = amp::Change {
            actor_id: actor_b.clone(),
            seq: 1,
            start_op: 1,
            time: 0,
            message: None,
            hash: None,
            deps: vec![],
            operations: vec![Op {
                obj: ObjectId::Root,
                action: OpType::Set("dove".into()),
                key: "bird".into(),
                insert: false,
                pred: SortedVec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let change_b2: Change = amp::Change {
            actor_id: actor_b.clone(),
            seq: 2,
            start_op: 2,
            time: 0,
            message: None,
            hash: None,
            deps: vec![change_b1.hash],
            operations: vec![Op {
                obj: ObjectId::Root,
                action: OpType::Set("stag beetle".into()),
                key: "bug".into(),
                insert: false,
                pred: SortedVec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let change_b3: Change = amp::Change {
            actor_id: actor_b,
            seq: 3,
            start_op: 3,
            time: 0,
            message: None,
            hash: None,
            deps: vec![change_a2.hash, change_b2.hash],
            operations: vec![Op {
                obj: ObjectId::Root,
                action: OpType::Set("bugs and birds".into()),
                key: "title".into(),
                insert: false,
                pred: SortedVec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let mut backend = Backend::new();

        backend
            .apply_changes(vec![change_a1.clone(), change_a2.clone()])
            .unwrap();

        assert_eq!(
            backend.get_changes_fast(&[]),
            Some(vec![&change_a1, &change_a2])
        );
        assert_eq!(
            backend.get_changes_fast(&[change_a1.hash]),
            Some(vec![&change_a2])
        );
        assert_eq!(backend.get_heads(), vec![change_a2.hash]);

        backend
            .apply_changes(vec![change_b1.clone(), change_b2.clone()])
            .unwrap();

        assert_eq!(
            backend.get_changes_fast(&[]),
            Some(vec![&change_a1, &change_a2, &change_b1, &change_b2])
        );
        assert_eq!(backend.get_changes_fast(&[change_a1.hash]), None);
        assert_eq!(backend.get_changes_fast(&[change_a2.hash]), None);
        assert_eq!(
            backend.get_changes_fast(&[change_a1.hash, change_b1.hash]),
            Some(vec![&change_a2, &change_b2])
        );
        assert_eq!(
            backend.get_changes_fast(&[change_a2.hash, change_b1.hash]),
            Some(vec![&change_b2])
        );
        assert_eq!(backend.get_heads(), vec![change_b2.hash, change_a2.hash]);

        backend.apply_changes(vec![change_b3.clone()]).unwrap();

        assert_eq!(backend.get_heads(), vec![change_b3.hash]);
        assert_eq!(
            backend.get_changes_fast(&[]),
            Some(vec![
                &change_a1, &change_a2, &change_b1, &change_b2, &change_b3
            ])
        );
        assert_eq!(backend.get_changes_fast(&[change_a1.hash]), None);
        assert_eq!(backend.get_changes_fast(&[change_a2.hash]), None);
        assert_eq!(backend.get_changes_fast(&[change_b1.hash]), None);
        assert_eq!(backend.get_changes_fast(&[change_b2.hash]), None);
        assert_eq!(
            backend.get_changes_fast(&[change_a1.hash, change_b1.hash]),
            Some(vec![&change_a2, &change_b2, &change_b3])
        );
        assert_eq!(
            backend.get_changes_fast(&[change_a2.hash, change_b1.hash]),
            Some(vec![&change_b2, &change_b3])
        );
        assert_eq!(backend.get_changes_fast(&[change_b3.hash]), Some(vec![]));
    }
}

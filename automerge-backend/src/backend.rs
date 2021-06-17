use core::cmp::max;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
};

use amp::ChangeHash;
use automerge_protocol as amp;

use crate::{
    actor_map::ActorMap,
    change::encode_document,
    error::AutomergeError,
    event_handlers::{EventHandlerId, EventHandlers},
    op_handle::OpHandle,
    op_set::OpSet,
    patches::{generate_from_scratch_diff, IncrementalPatch},
    Change, EventHandler,
};

#[derive(Debug, Default, Clone)]
pub struct Backend {
    queue: Vec<Change>,
    op_set: OpSet,
    states: HashMap<amp::ActorId, Vec<usize>>,
    actors: ActorMap,
    history: Vec<Change>,
    history_index: HashMap<amp::ChangeHash, usize>,
    event_handlers: EventHandlers,
}

impl Backend {
    pub fn new() -> Self {
        Self::default()
    }

    fn make_patch(
        &self,
        diffs: amp::RootDiff,
        actor_seq: Option<(amp::ActorId, u64)>,
    ) -> Result<amp::Patch, AutomergeError> {
        let mut deps: Vec<_> = if let Some((ref actor, ref seq)) = actor_seq {
            let last_hash = self.get_hash(actor, *seq)?;
            self.op_set
                .deps
                .iter()
                .filter(|&dep| dep != &last_hash)
                .copied()
                .collect()
        } else {
            self.op_set.deps.iter().copied().collect()
        };
        deps.sort_unstable();
        let pending_changes = self.get_missing_deps(&[]).len();
        Ok(amp::Patch {
            diffs,
            deps,
            max_op: self.op_set.max_op,
            clock: self
                .states
                .iter()
                .map(|(k, v)| (k.clone(), v.len() as u64))
                .collect(),
            actor: actor_seq.clone().map(|(actor, _)| actor),
            seq: actor_seq.map(|(_, seq)| seq),
            pending_changes,
        })
    }

    pub fn load_changes(&mut self, changes: Vec<Change>) -> Result<(), AutomergeError> {
        self.apply(changes, None)?;
        Ok(())
    }

    pub fn apply_changes(&mut self, changes: Vec<Change>) -> Result<amp::Patch, AutomergeError> {
        self.apply(changes, None)
    }

    pub fn get_heads(&self) -> Vec<amp::ChangeHash> {
        self.op_set.heads()
    }

    fn apply(
        &mut self,
        changes: Vec<Change>,
        actor: Option<(amp::ActorId, u64)>,
    ) -> Result<amp::Patch, AutomergeError> {
        let mut patch = IncrementalPatch::new();

        for change in changes {
            self.add_change(change, actor.is_some(), &mut patch)?;
        }

        let workshop = self.op_set.patch_workshop(&self.actors);
        let diffs = patch.finalize(&workshop);
        self.make_patch(diffs, actor)
    }

    fn get_hash(&self, actor: &amp::ActorId, seq: u64) -> Result<amp::ChangeHash, AutomergeError> {
        self.states
            .get(actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|&i| self.history.get(i))
            .map(|c| c.hash)
            .ok_or(AutomergeError::InvalidSeq(seq))
    }

    pub fn apply_local_change(
        &mut self,
        mut change: amp::Change,
    ) -> Result<(amp::Patch, Change), AutomergeError> {
        self.check_for_duplicate(&change)?; // Change has already been applied

        let actor_seq = (change.actor_id.clone(), change.seq);

        if change.seq > 1 {
            let last_hash = self.get_hash(&change.actor_id, change.seq - 1)?;
            if !change.deps.contains(&last_hash) {
                change.deps.push(last_hash)
            }
        }

        let bin_change: Change = change.into();
        let patch: amp::Patch = self.apply(vec![bin_change.clone()], Some(actor_seq))?;

        Ok((patch, bin_change))
    }

    fn check_for_duplicate(&self, change: &amp::Change) -> Result<(), AutomergeError> {
        if self
            .states
            .get(&change.actor_id)
            .map_or(0, |v| v.len() as u64)
            >= change.seq
        {
            return Err(AutomergeError::DuplicateChange(format!(
                "Change request has already been applied {}:{}",
                change.actor_id.to_hex_string(),
                change.seq
            )));
        }
        Ok(())
    }

    fn add_change(
        &mut self,
        change: Change,
        local: bool,
        diffs: &mut IncrementalPatch,
    ) -> Result<(), AutomergeError> {
        if local {
            self.apply_change(change, diffs)
        } else {
            self.queue.push(change);
            self.apply_queued_ops(diffs)
        }
    }

    fn apply_queued_ops(&mut self, diffs: &mut IncrementalPatch) -> Result<(), AutomergeError> {
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            self.apply_change(next_change, diffs)?;
        }
        Ok(())
    }

    fn apply_change(
        &mut self,
        change: Change,
        diffs: &mut IncrementalPatch,
    ) -> Result<(), AutomergeError> {
        if self.history_index.contains_key(&change.hash) {
            return Ok(());
        }

        self.event_handlers.before_apply_change(&change);

        let change_index = self.update_history(change);

        // SAFETY: change_index is the index for the change we've just added so this can't (and
        // shouldn't) panic. This is to get around the borrow checker.
        let change = &self.history[change_index];

        let op_set = &mut self.op_set;

        let start_op = change.start_op;

        op_set.update_deps(change);

        let ops = OpHandle::extract(change, &mut self.actors);

        op_set.max_op = max(
            op_set.max_op,
            (start_op + (ops.len() as u64)).saturating_sub(1),
        );

        op_set.apply_ops(ops, diffs, &mut self.actors)?;

        self.event_handlers.after_apply_change(change);

        Ok(())
    }

    fn update_history(&mut self, change: Change) -> usize {
        let history_index = self.history.len();

        self.states
            .entry(change.actor_id().clone())
            .or_default()
            .push(history_index);

        self.history_index.insert(change.hash, history_index);
        self.history.push(change);

        history_index
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Change> {
        let mut index = 0;
        while index < self.queue.len() {
            let change = self.queue.get(index).unwrap();
            if change
                .deps
                .iter()
                .all(|d| self.history_index.contains_key(d))
            {
                return Some(self.queue.swap_remove(index));
            }
            index += 1
        }
        None
    }

    pub fn get_patch(&self) -> Result<amp::Patch, AutomergeError> {
        let workshop = self.op_set.patch_workshop(&self.actors);
        let diffs = generate_from_scratch_diff(&workshop);
        self.make_patch(diffs, None)
    }

    pub fn get_changes_for_actor_id(
        &self,
        actor_id: &amp::ActorId,
    ) -> Result<Vec<&Change>, AutomergeError> {
        Ok(self
            .states
            .get(actor_id)
            .map(|vec| vec.iter().filter_map(|&i| self.history.get(i)).collect())
            .unwrap_or_default())
    }

    fn get_changes_fast(&self, have_deps: &[amp::ChangeHash]) -> Option<Vec<&Change>> {
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

    fn get_changes_slow(&self, have_deps: &[amp::ChangeHash]) -> Vec<&Change> {
        let mut stack: Vec<_> = have_deps.iter().collect();
        let mut has_seen = HashSet::new();
        while let Some(hash) = stack.pop() {
            if has_seen.contains(&hash) {
                continue;
            }
            if let Some(change) = self
                .history_index
                .get(hash)
                .and_then(|i| self.history.get(*i))
            {
                stack.extend(change.deps.iter());
            }
            has_seen.insert(hash);
        }
        self.history
            .iter()
            .filter(|change| !has_seen.contains(&change.hash))
            .collect()
    }

    pub fn get_changes(&self, have_deps: &[amp::ChangeHash]) -> Vec<&Change> {
        if let Some(changes) = self.get_changes_fast(have_deps) {
            changes
        } else {
            self.get_changes_slow(have_deps)
        }
    }

    pub fn save(&self) -> Result<Vec<u8>, AutomergeError> {
        let changes: Vec<amp::Change> = self.history.iter().map(Change::decode).collect();
        //self.history.iter().map(|change| change.decode()).collect();
        Ok(encode_document(&changes)?)
    }

    // allow this for API reasons
    #[allow(clippy::needless_pass_by_value)]
    pub fn load(data: Vec<u8>) -> Result<Self, AutomergeError> {
        let changes = Change::load_document(&data)?;
        let mut backend = Self::new();
        backend.load_changes(changes)?;
        Ok(backend)
    }

    pub fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<amp::ChangeHash> {
        let in_queue: HashSet<_> = self.queue.iter().map(|change| change.hash).collect();
        let mut missing = HashSet::new();

        for head in self.queue.iter().flat_map(|change| &change.deps) {
            if !self.history_index.contains_key(head) {
                missing.insert(head);
            }
        }

        for head in heads {
            if !self.history_index.contains_key(head) {
                missing.insert(head);
            }
        }

        let mut missing = missing
            .into_iter()
            .filter(|hash| !in_queue.contains(hash))
            .copied()
            .collect::<Vec<_>>();
        missing.sort();
        missing
    }

    pub fn get_change_by_hash(&self, hash: &amp::ChangeHash) -> Option<&Change> {
        self.history_index
            .get(hash)
            .and_then(|index| self.history.get(*index))
    }

    /**
     * Returns all changes that are present in `self` but not present in `other`.
     */
    pub fn get_changes_added<'a>(&self, other: &'a Self) -> Vec<&'a Change> {
        // Depth-first traversal from the heads through the dependency graph,
        // until we reach a change that is already present in other
        let mut stack: Vec<_> = other.op_set.deps.iter().collect();
        let mut seen_hashes = HashSet::new();
        let mut added_change_hashes = Vec::new();
        while let Some(hash) = stack.pop() {
            if !seen_hashes.contains(&hash) && self.get_change_by_hash(hash).is_none() {
                seen_hashes.insert(hash);
                added_change_hashes.push(hash);
                if let Some(change) = other.get_change_by_hash(hash) {
                    stack.extend(&change.deps);
                }
            }
        }
        // Return those changes in the reverse of the order in which the depth-first search
        // found them. This is not necessarily a topological sort, but should usually be close.
        added_change_hashes
            .into_iter()
            .filter_map(|h| other.get_change_by_hash(h))
            .collect()
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
        let mut may_find: HashSet<ChangeHash> = changes
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

        let mut queue: VecDeque<_> = heads.iter().collect();
        let mut seen = HashSet::new();
        while let Some(hash) = queue.pop_front() {
            if seen.contains(hash) {
                continue;
            }
            seen.insert(hash);

            let removed = may_find.remove(hash);
            changes.remove(hash);
            if may_find.is_empty() {
                break;
            }

            for dep in self
                .history_index
                .get(hash)
                .and_then(|i| self.history.get(*i))
                .map(|c| c.deps.as_slice())
                .unwrap_or_default()
            {
                // if we just removed something from our hashes then it is likely there is more
                // down here so do a quick inspection on the children.
                // When we don't remove anything it is less likely that there is something down
                // that chain so delay it.
                if removed {
                    queue.push_front(dep)
                } else {
                    queue.push_back(dep)
                }
            }
        }
    }

    /// Adds the event handler and returns the id of the handler.
    pub fn add_event_handler(&mut self, handler: EventHandler) -> EventHandlerId {
        self.event_handlers.add_handler(handler)
    }

    /// Remove the handler with the given id, returning whether it removed a handler or not.
    pub fn remove_event_handler(&mut self, id: EventHandlerId) -> bool {
        self.event_handlers.remove_handler(id)
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

use crate::actor_map::ActorMap;
use crate::change::encode_document;
use crate::error::AutomergeError;
use crate::internal::ObjectId;
use crate::op_handle::OpHandle;
use crate::op_set::OpSet;
use crate::pending_diff::PendingDiff;
use crate::Change;
use automerge_protocol as amp;
use core::cmp::max;
use std::collections::{HashMap, HashSet};

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    queue: Vec<Change>,
    op_set: OpSet,
    states: HashMap<amp::ActorId, Vec<Change>>,
    actors: ActorMap,
    history: Vec<Change>,
    history_index: HashMap<amp::ChangeHash, usize>,
}

impl Backend {
    pub fn init() -> Backend {
        let op_set = OpSet::init();
        Backend {
            op_set,
            queue: Vec::new(),
            actors: ActorMap::new(),
            states: HashMap::new(),
            history: Vec::new(),
            history_index: HashMap::new(),
        }
    }

    fn make_patch(
        &self,
        diffs: Option<amp::Diff>,
        actor_seq: Option<(amp::ActorId, u64)>,
    ) -> Result<amp::Patch, AutomergeError> {
        let mut deps: Vec<_> = if let Some((ref actor, ref seq)) = actor_seq {
            let last_hash = self.get_hash(actor, *seq)?;
            self.op_set
                .deps
                .iter()
                .cloned()
                .filter(|dep| dep != &last_hash)
                .collect()
        } else {
            self.op_set.deps.iter().cloned().collect()
        };
        deps.sort_unstable();
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
        let mut pending_diffs = HashMap::new();

        for change in changes.into_iter() {
            self.add_change(change, actor.is_some(), &mut pending_diffs)?;
        }

        let op_set = &mut self.op_set;
        let diffs = op_set.finalize_diffs(pending_diffs, &self.actors)?;
        self.make_patch(diffs, actor)
    }

    fn get_hash(&self, actor: &amp::ActorId, seq: u64) -> Result<amp::ChangeHash, AutomergeError> {
        self.states
            .get(actor)
            .and_then(|v| v.get(seq as usize - 1))
            .map(|c| c.hash)
            .ok_or(AutomergeError::InvalidSeq(seq))
    }

    pub fn apply_local_change(
        &mut self,
        mut change: amp::UncompressedChange,
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

    fn check_for_duplicate(&self, change: &amp::UncompressedChange) -> Result<(), AutomergeError> {
        if self
            .states
            .get(&change.actor_id)
            .map(|v| v.len() as u64)
            .unwrap_or(0)
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
        diffs: &mut HashMap<ObjectId, Vec<PendingDiff>>,
    ) -> Result<(), AutomergeError> {
        if local {
            self.apply_change(change, diffs)
        } else {
            self.queue.push(change);
            self.apply_queued_ops(diffs)
        }
    }

    fn apply_queued_ops(
        &mut self,
        diffs: &mut HashMap<ObjectId, Vec<PendingDiff>>,
    ) -> Result<(), AutomergeError> {
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            self.apply_change(next_change, diffs)?;
        }
        Ok(())
    }

    fn apply_change(
        &mut self,
        change: Change,
        diffs: &mut HashMap<ObjectId, Vec<PendingDiff>>,
    ) -> Result<(), AutomergeError> {
        if self.history_index.contains_key(&change.hash) {
            return Ok(());
        }

        self.update_history(&change);

        let op_set = &mut self.op_set;

        let start_op = change.start_op;

        op_set.update_deps(&change);

        let ops = OpHandle::extract(change, &mut self.actors);

        op_set.max_op = max(op_set.max_op, start_op + (ops.len() as u64) - 1);

        op_set.apply_ops(ops, diffs, &mut self.actors)?;

        Ok(())
    }

    fn update_history(&mut self, change: &Change) {
        self.states
            .entry(change.actor_id().clone())
            .or_default()
            .push(change.clone());

        self.history_index.insert(change.hash, self.history.len());
        self.history.push(change.clone());
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
                return Some(self.queue.remove(index));
            }
            index += 1
        }
        None
    }

    pub fn get_patch(&self) -> Result<amp::Patch, AutomergeError> {
        let diffs = self
            .op_set
            .construct_object(&ObjectId::Root, &self.actors)?;
        self.make_patch(Some(diffs), None)
    }

    pub fn get_changes_for_actor_id(
        &self,
        actor_id: &amp::ActorId,
    ) -> Result<Vec<&Change>, AutomergeError> {
        Ok(self
            .states
            .get(actor_id)
            .map(|vec| vec.iter().collect())
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
        let mut stack = have_deps.to_owned();
        let mut has_seen = HashSet::new();
        while let Some(hash) = stack.pop() {
            if let Some(idx) = self.history_index.get(&hash) {
                stack.extend(self.history[*idx].deps.clone());
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
        let changes: Vec<amp::UncompressedChange> = self.history.iter().map(|r| r.into()).collect();
        encode_document(changes)
    }

    pub fn load(data: Vec<u8>) -> Result<Self, AutomergeError> {
        let changes = Change::load_document(&data)?;
        let mut backend = Self::init();
        backend.load_changes(changes)?;
        Ok(backend)
    }

    pub fn get_missing_deps(&self) -> Vec<amp::ChangeHash> {
        let in_queue: Vec<_> = self.queue.iter().map(|change| &change.hash).collect();
        self.queue
            .iter()
            .flat_map(|change| change.deps.clone())
            .filter(|h| !in_queue.contains(&h))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge_protocol::{ActorId, ObjectId, Op, OpType, UncompressedChange};
    use std::convert::TryInto;

    #[test]
    fn test_add() {
        let actor_a: ActorId = "7b7723afd9e6480397a4d467b7693156".try_into().unwrap();
        let actor_b: ActorId = "37704788917a499cb0206fa8519ac4d9".try_into().unwrap();
        let change_a1: Change = UncompressedChange {
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
                pred: Vec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let change_a2: Change = UncompressedChange {
            actor_id: actor_a.clone(),
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
                pred: Vec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let change_b1: Change = UncompressedChange {
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
                pred: Vec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let change_b2: Change = UncompressedChange {
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
                pred: Vec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let change_b3: Change = UncompressedChange {
            actor_id: actor_b.clone(),
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
                pred: Vec::new(),
            }],
            extra_bytes: Vec::new(),
        }
        .try_into()
        .unwrap();
        let mut backend = Backend::init();

        backend
            .apply_changes(vec![change_a1.clone(), change_a2.clone()])
            .unwrap();

        assert_eq!(
            backend.get_changes_fast(&vec![]),
            Some(vec![&change_a1, &change_a2])
        );
        assert_eq!(
            backend.get_changes_fast(&vec![change_a1.hash]),
            Some(vec![&change_a2])
        );
        assert_eq!(backend.get_heads(), vec![change_a2.hash]);

        backend
            .apply_changes(vec![change_b1.clone(), change_b2.clone()])
            .unwrap();

        assert_eq!(
            backend.get_changes_fast(&vec![]),
            Some(vec![&change_a1, &change_a2, &change_b1, &change_b2])
        );
        assert_eq!(backend.get_changes_fast(&vec![change_a1.hash]), None);
        assert_eq!(backend.get_changes_fast(&vec![change_a2.hash]), None);
        assert_eq!(
            backend.get_changes_fast(&vec![change_a1.hash, change_b1.hash]),
            Some(vec![&change_a2, &change_b2])
        );
        assert_eq!(
            backend.get_changes_fast(&vec![change_a2.hash, change_b1.hash]),
            Some(vec![&change_b2])
        );
        assert_eq!(backend.get_heads(), vec![change_b2.hash, change_a2.hash]);

        backend.apply_changes(vec![change_b3.clone()]).unwrap();

        assert_eq!(backend.get_heads(), vec![change_b3.hash]);
        assert_eq!(
            backend.get_changes_fast(&vec![]),
            Some(vec![
                &change_a1, &change_a2, &change_b1, &change_b2, &change_b3
            ])
        );
        assert_eq!(backend.get_changes_fast(&vec![change_a1.hash]), None);
        assert_eq!(backend.get_changes_fast(&vec![change_a2.hash]), None);
        assert_eq!(backend.get_changes_fast(&vec![change_b1.hash]), None);
        assert_eq!(backend.get_changes_fast(&vec![change_b2.hash]), None);
        assert_eq!(
            backend.get_changes_fast(&vec![change_a1.hash, change_b1.hash]),
            Some(vec![&change_a2, &change_b2, &change_b3])
        );
        assert_eq!(
            backend.get_changes_fast(&vec![change_a2.hash, change_b1.hash]),
            Some(vec![&change_b2, &change_b3])
        );
        assert_eq!(
            backend.get_changes_fast(&vec![change_b3.hash]),
            Some(vec![])
        );
    }
}

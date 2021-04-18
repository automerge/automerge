use crate::actor_map::ActorMap;
use crate::change::encode_document;
use crate::error::AutomergeError;
use crate::internal::ObjectId;
use crate::op_handle::OpHandle;
use crate::op_set::OpSet;
use crate::pending_diff::PendingDiff;
use crate::Change;
use amp::ChangeHash;
use automerge_protocol as amp;
use core::cmp::max;
use std::collections::{HashMap, HashSet};

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    queue: Vec<Change>,
    op_set: OpSet,
    states: HashMap<amp::ActorId, Vec<Change>>,
    actors: ActorMap,
    hashes: HashMap<amp::ChangeHash, Change>,
    history: Vec<amp::ChangeHash>,
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
            hashes: HashMap::new(),
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
        if self.hashes.contains_key(&change.hash) {
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

        self.history.push(change.hash);
        self.hashes.insert(change.hash, change.clone());
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Change> {
        let mut index = 0;
        while index < self.queue.len() {
            let change = self.queue.get(index).unwrap();
            if change.deps.iter().all(|d| self.hashes.contains_key(d)) {
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

    pub fn get_changes(&self, have_deps: &[amp::ChangeHash]) -> Vec<&Change> {
        let mut stack = have_deps.to_owned();
        let mut has_seen = HashSet::new();
        while let Some(hash) = stack.pop() {
            if let Some(change) = self.hashes.get(&hash) {
                stack.extend(change.deps.clone());
            }
            has_seen.insert(hash);
        }
        self.history
            .iter()
            .filter(|hash| !has_seen.contains(hash))
            .filter_map(|hash| self.hashes.get(hash))
            .collect()
    }

    pub fn save(&self) -> Result<Vec<u8>, AutomergeError> {
        let changes: Vec<amp::UncompressedChange> = self
            .history
            .iter()
            .filter_map(|hash| self.hashes.get(&hash))
            .map(|r| r.into())
            .collect();
        encode_document(changes)
    }

    pub fn load(data: Vec<u8>) -> Result<Self, AutomergeError> {
        let changes = Change::load_document(&data)?;
        let mut backend = Self::init();
        backend.load_changes(changes)?;
        Ok(backend)
    }

    pub fn get_missing_deps(
        &self,
        changes: &[Change],
        heads: &[ChangeHash],
    ) -> Vec<amp::ChangeHash> {
        let in_queue: HashSet<_> = self
            .queue
            .iter()
            .chain(changes.iter())
            .map(|change| change.hash)
            .collect();
        let mut missing = HashSet::new();

        for head in self
            .queue
            .iter()
            .chain(changes.iter())
            .flat_map(|change| change.deps.clone())
        {
            if !self.hashes.contains_key(&head) {
                missing.insert(head);
            }
        }

        for head in heads {
            if !self.hashes.contains_key(&head) {
                missing.insert(*head);
            }
        }

        let mut missing = missing
            .into_iter()
            .filter(|hash| !in_queue.contains(hash))
            .collect::<Vec<_>>();
        missing.sort();
        missing
    }

    pub fn get_change_by_hash(&self, hash: &amp::ChangeHash) -> Option<&Change> {
        self.hashes.get(hash)
    }
}

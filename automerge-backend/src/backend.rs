use crate::actor_states::ActorStates;
use crate::error::AutomergeError;
use crate::op_handle::OpHandle;
use crate::op_set::{OpSet, Version};
use crate::patch::{Diff, Patch, PendingDiff};
use crate::protocol::{
    DataType, ObjAlias, ObjType, ObjectID, OpType, Operation, ReqOpType, UndoOperation,
};
use crate::time;
use crate::{ActorID, Change, ChangeRequest, ChangeRequestType, Clock, OpID};
use std::cmp::max;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    versions: Vec<Version>,
    queue: Vec<Rc<Change>>,
    op_set: OpSet,
    states: ActorStates,
    obj_alias: ObjAlias,
    max_op: u64,
    undo_pos: usize,
    pub clock: Clock,
    pub undo_stack: Vec<Vec<UndoOperation>>,
    pub redo_stack: Vec<Vec<UndoOperation>>,
}

impl Backend {
    pub fn init() -> Backend {
        let mut versions = Vec::new();
        versions.push(Version {
            version: 0,
            local_only: true,
            op_set: Rc::new(OpSet::init()),
        });
        Backend {
            versions,
            op_set: OpSet::init(),
            queue: Vec::new(),
            obj_alias: ObjAlias::new(),
            states: ActorStates::new(),
            clock: Clock::empty(),
            max_op: 0,
            undo_stack: Vec::new(),
            undo_pos: 0,
            redo_stack: Vec::new(),
        }
    }

    fn process_request(
        &mut self,
        request: &ChangeRequest,
        op_set: &OpSet,
        start_op: u64,
    ) -> Result<Rc<Change>, AutomergeError> {
        let time = time::unix_timestamp();
        let actor_id = request.actor.clone();
        let mut operations: Vec<Operation> = Vec::new();
        // this is a local cache of elemids that I can manipulate as i insert and edit so the
        // index's stay consistent as I walk through the ops
        let mut elemid_cache: HashMap<ObjectID, Vec<OpID>> = HashMap::new();
        if let Some(ops) = &request.ops {
            for rop in ops.iter() {
                let id = OpID::ID(start_op + (operations.len() as u64), actor_id.0.clone());
                let insert = rop.insert;
                let object_id = self.obj_alias.get(&rop.obj);
                let child = object_id.clone(); // FIXME

                if let Some(child) = &rop.child {
                    self.obj_alias.insert(child.clone(), &id);
                }

                let mut elemids = elemid_cache.entry(object_id.clone()).or_insert_with(|| {
                    op_set
                        .get_elem_ids(&object_id)
                        .map(|c| c.clone())
                        .unwrap_or_default()
                });

                let key = rop.resolve_key(&id, &mut elemids)?;
                let pred = op_set.get_pred(&object_id, &key, insert);
                let action = match rop.action {
                    ReqOpType::MakeMap => OpType::Make(ObjType::Map),
                    ReqOpType::MakeTable => OpType::Make(ObjType::Table),
                    ReqOpType::MakeList => OpType::Make(ObjType::List),
                    ReqOpType::MakeText => OpType::Make(ObjType::Text),
                    ReqOpType::Del => OpType::Del,
                    ReqOpType::Link => OpType::Link(child),
                    ReqOpType::Inc => OpType::Inc(rop.number_value()?),
                    ReqOpType::Set => OpType::Set(
                        rop.primitive_value(),
                        rop.datatype.clone().unwrap_or(DataType::Undefined),
                    ),
                };

                let op = Operation {
                    action,
                    obj: object_id.clone(),
                    key: key.clone(),
                    pred: pred.clone(),
                    insert,
                };

                if op.is_basic_assign() {
                    if let Some(index) = operations.iter().position(|old| op.can_merge(old)) {
                        operations[index].merge(op);
                        continue;
                    }
                }
                operations.push(op);
            }
        }
        Ok(Rc::new(Change {
            start_op,
            message: request.message.clone(),
            actor_id: request.actor.clone(),
            seq: request.seq,
            deps: request.deps.clone().unwrap_or_default(),
            time,
            operations,
        }))
    }

    fn make_patch(
        &self,
        diffs: Option<Diff>,
        request: Option<&ChangeRequest>,
    ) -> Result<Patch, AutomergeError> {
        Ok(Patch {
            version: self.versions.last().map(|v| v.version).unwrap_or(0),
            can_undo: self.can_undo(),
            can_redo: self.can_redo(),
            diffs,
            clock: self.clock.clone(),
            actor: request.map(|r| r.actor.clone()),
            seq: request.map(|r| r.seq),
        })
    }

    fn undo(
        &mut self,
        request: &ChangeRequest,
        start_op: u64,
    ) -> Result<Rc<Change>, AutomergeError> {
        let undo_pos = self.undo_pos;

        if undo_pos < 1 || self.undo_stack.len() < undo_pos {
            return Err(AutomergeError::InvalidChange(
                "Cannot undo: there is nothing to be undone".to_string(),
            ));
        }

        let mut undo_ops = self.undo_stack.get(undo_pos - 1).unwrap().clone();
        let mut redo_ops = Vec::new();

        let operations = undo_ops
            .drain(0..)
            .map(|undo_op| {
                if let Some(field_ops) = self.op_set.get_field_ops(&undo_op.obj, &undo_op.key) {
                    let pred = field_ops.iter().map(|op| op.id.clone()).collect();
                    let op = undo_op.into_operation(pred);
                    redo_ops.extend(op.generate_redos(&field_ops));
                    op
                } else {
                    let op = undo_op.into_operation(Vec::new());
                    redo_ops.extend(op.generate_redos(&Vec::new()));
                    op
                }
            })
            .collect();

        let change = Rc::new(Change {
            actor_id: request.actor.clone(),
            seq: request.seq,
            start_op,
            deps: request.deps.clone().unwrap_or_default(),
            message: request.message.clone(),
            time: time::unix_timestamp(),
            operations,
        });

        self.undo_pos -= 1;
        self.redo_stack.push(redo_ops);

        Ok(change)
    }

    fn redo(
        &mut self,
        request: &ChangeRequest,
        start_op: u64,
    ) -> Result<Rc<Change>, AutomergeError> {
        let mut redo_ops = self
            .redo_stack
            .pop()
            .ok_or_else(|| AutomergeError::InvalidChange("no redo ops".to_string()))?;

        let operations = redo_ops
            .drain(0..)
            .map(|redo_op| {
                if let Some(field_ops) = self.op_set.get_field_ops(&redo_op.obj, &redo_op.key) {
                    redo_op.into_operation(field_ops.iter().map(|op| op.id.clone()).collect())
                } else {
                    redo_op.into_operation(Vec::new())
                }
            })
            .collect();

        let change = Rc::new(Change {
            actor_id: request.actor.clone(),
            seq: request.seq,
            start_op,
            deps: request.deps.clone().unwrap_or_default(),
            message: request.message.clone(),
            time: time::unix_timestamp(),
            operations,
        });

        self.undo_pos += 1;

        Ok(change)
    }

    pub fn load_changes(&mut self, mut changes: Vec<Change>) -> Result<(), AutomergeError> {
        let changes = changes.drain(0..).map(Rc::new).collect();
        self.apply(changes, None, false, false)?;
        Ok(())
    }

    pub fn apply_changes(&mut self, mut changes: Vec<Change>) -> Result<Patch, AutomergeError> {
        self.versions.iter_mut().for_each(|v| v.local_only = false);
        let changes = changes.drain(0..).map(Rc::new).collect();
        self.apply(changes, None, false, true)
    }

    fn get_version(&self, version: u64) -> Result<&Version, AutomergeError> {
        self.versions
            .iter()
            .find(|v| v.version == version)
            .ok_or_else(|| AutomergeError::UnknownVersion(version))
    }

    fn apply(
        &mut self,
        mut changes: Vec<Rc<Change>>,
        request: Option<&ChangeRequest>,
        undoable: bool,
        incremental: bool,
    ) -> Result<Patch, AutomergeError> {
        let mut pending_diffs = Vec::new();

        for change in changes.drain(0..) {
            let diffs = self.add_change(change, request.is_some(), undoable)?;
            pending_diffs.extend(diffs);
        }

        if incremental {
            let version = self.versions.last().map(|v| v.version).unwrap_or(0) + 1;
            let version_obj = Version {
                version,
                local_only: true,
                op_set: Rc::new(self.op_set.clone()),
            };
            self.versions.push(version_obj);
        } else {
            let version_obj = Version {
                version: 0,
                local_only: true,
                op_set: Rc::new(self.op_set.clone()),
            };
            self.versions.clear();
            self.versions.push(version_obj);
        }

        let diffs = self.op_set.finalize_diffs(pending_diffs)?;

        self.make_patch(diffs, request)
    }

    pub fn apply_local_change(
        &mut self,
        mut request: ChangeRequest,
    ) -> Result<Patch, AutomergeError> {
        self.check_for_duplicate(&request)?; // Change has already been applied

        let ver = self.get_version(request.version)?.clone();

        let actor = request.actor.clone();
        request
            .deps
            .get_or_insert_with(|| ver.op_set.deps.without(&actor));

        let start_op = self.max_op + 1;
        let change = match request.request_type {
            ChangeRequestType::Change => self.process_request(&request, &ver.op_set, start_op)?,
            ChangeRequestType::Undo => self.undo(&request, start_op)?,
            ChangeRequestType::Redo => self.redo(&request, start_op)?,
        };

        let undoable = request.request_type == ChangeRequestType::Change && request.undoable;

        let patch = self.apply(vec![change.clone()], Some(&request), undoable, true)?;

        self.finalize_version(request.version, change)?;

        Ok(patch)
    }

    pub fn check_for_duplicate(&self, request: &ChangeRequest) -> Result<(), AutomergeError> {
        if self.clock.get(&request.actor) >= request.seq {
            return Err(AutomergeError::DuplicateChange(format!(
                "Change request has already been applied {}:{}",
                request.actor.0, request.seq
            )));
        }
        Ok(())
    }

    fn add_change(
        &mut self,
        change: Rc<Change>,
        local: bool,
        undoable: bool,
    ) -> Result<Vec<PendingDiff>, AutomergeError> {
        if local {
            self.apply_change(change, local, undoable)
        } else {
            self.queue.push(change);
            self.apply_queued_ops()
        }
    }

    fn apply_queued_ops(&mut self) -> Result<Vec<PendingDiff>, AutomergeError> {
        let mut all_diffs = Vec::new();
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            let diffs = self.apply_change(next_change, false, false)?;
            all_diffs.extend(diffs)
        }
        Ok(all_diffs)
    }

    fn apply_change(
        &mut self,
        change: Rc<Change>,
        _local: bool,
        undoable: bool,
    ) -> Result<Vec<PendingDiff>, AutomergeError> {
        if let Some(all_deps) = self.states.add_change(&change)? {
            self.clock.set(&change.actor_id, change.seq);
            self.op_set.deps.subtract(&all_deps);
            self.op_set.deps.set(&change.actor_id, change.seq);
        } else {
            return Ok(Vec::new());
        }
        self.max_op = max(self.max_op, change.max_op());

        let (undo_ops, diffs) = self.op_set.apply_ops(OpHandle::extract(change), undoable)?;

        if undoable {
            self.push_undo_ops(undo_ops);
        };

        Ok(diffs)
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Rc<Change>> {
        let mut index = 0;
        while index < self.queue.len() {
            let change = self.queue.get(index).unwrap();
            let deps = change.deps.with(&change.actor_id, change.seq - 1);
            if deps <= self.clock {
                return Some(self.queue.remove(index));
            }
            index += 1
        }
        None
    }

    fn finalize_version(
        &mut self,
        request_version: u64,
        change: Rc<Change>,
    ) -> Result<(), AutomergeError> {
        // remove all versions older than this one
        let mut i = 0;
        while i != self.versions.len() {
            if self.versions[i].version < request_version {
                self.versions.remove(i);
            } else {
                i += 1;
            }
        }

        for v in self.versions.iter_mut() {
            if v.local_only {
                v.op_set = Rc::new(self.op_set.clone());
            } else {
                Rc::make_mut(&mut v.op_set).apply_ops(OpHandle::extract(change.clone()), false)?;
            }
        }

        Ok(())
    }

    pub fn history(&self) -> Vec<&Change> {
        self.states.history.iter().map(|rc| rc.as_ref()).collect()
    }

    pub fn get_patch(&self) -> Result<Patch, AutomergeError> {
        let diffs = self.op_set.construct_object(&ObjectID::Root)?;
        self.make_patch(Some(diffs), None)
    }

    pub fn get_changes<'a>(&self, other: &'a Backend) -> Result<Vec<&'a Change>, AutomergeError> {
        if self.clock.divergent(&other.clock) {
            return Err(AutomergeError::DivergedState(
                "Cannot diff two states that have diverged".to_string(),
            ));
        }
        Ok(other.get_missing_changes(&self.clock))
    }

    pub fn get_changes_for_actor_id(&self, actor_id: &ActorID) -> Vec<&Change> {
        self.states.get(actor_id)
    }

    pub fn get_missing_changes(&self, since: &Clock) -> Vec<&Change> {
        self.states
            .history
            .iter()
            .map(|rc| rc.as_ref())
            .filter(|change| change.seq > since.get(&change.actor_id))
            .collect()
    }

    pub fn can_undo(&self) -> bool {
        self.undo_pos > 0
    }

    pub fn can_redo(&self) -> bool {
        !self.redo_stack.is_empty()
    }

    pub fn get_missing_deps(&self) -> Clock {
        let mut clock = Clock::empty();
        for change in self.queue.iter() {
            clock.merge(&change.deps.with(&change.actor_id, change.seq - 1))
        }
        clock
    }

    pub fn get_elem_ids(&self, object_id: &ObjectID) -> Result<Vec<OpID>, AutomergeError> {
        Ok(self.op_set.get_elem_ids(object_id)?.to_vec())
    }

    pub fn merge(&mut self, remote: &Backend) -> Result<Patch, AutomergeError> {
        let missing_changes = remote
            .get_missing_changes(&self.clock)
            .iter()
            .cloned()
            .cloned()
            .collect();
        self.apply_changes(missing_changes)
    }

    fn push_undo_ops(&mut self, undo_ops: Vec<UndoOperation>) {
        self.undo_stack.truncate(self.undo_pos);
        self.undo_stack.push(undo_ops);
        self.undo_pos += 1;
    }
}

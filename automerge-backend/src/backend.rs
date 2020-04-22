use crate::actor_states::ActorStates;
use crate::columnar::{ bin_to_changes, changes_to_bin};
use crate::error::AutomergeError;
use crate::op_handle::OpHandle;
use crate::op_set::OpSet;
use crate::ordered_set::{OrdDelta, OrderedSet};
use crate::patch::{Diff, Patch, PendingDiff};
use crate::protocol::{ ObjType, ObjectID, OpType, Operation, ReqOpType, UndoOperation};
use crate::time;
use crate::{ActorID, Change, ChangeRequest, ChangeRequestType, Clock, OpID};
use std::borrow::BorrowMut;
use std::cmp::max;
use std::collections::HashMap;
use std::rc::Rc;
use std::str::FromStr;

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    versions: Vec<Version>,
    queue: Vec<Rc<Change>>,
    op_set: Rc<OpSet>,
    states: ActorStates,
    obj_alias: HashMap<String, ObjectID>,
    max_op: u64,
    undo_pos: usize,
    pub clock: Clock,
    pub undo_stack: Vec<Vec<UndoOperation>>,
    pub redo_stack: Vec<Vec<UndoOperation>>,
}

impl Backend {
    pub fn init() -> Backend {
        let mut versions = Vec::new();
        let op_set = Rc::new(OpSet::init());
        versions.push(Version {
            version: 0,
            local_state: None,
            queue: Vec::new(),
        });
        Backend {
            versions,
            op_set,
            queue: Vec::new(),
            obj_alias: HashMap::new(),
            states: ActorStates::new(),
            clock: Clock::empty(),
            max_op: 0,
            undo_stack: Vec::new(),
            undo_pos: 0,
            redo_stack: Vec::new(),
        }
    }

    fn str_to_object(&self, name: &str) -> Result<ObjectID, AutomergeError> {
        ObjectID::from_str(name).or_else(|_| {
            self.obj_alias
                .get(name)
                .cloned()
                .ok_or_else(|| AutomergeError::MissingChildID(name.to_string()))
        })
    }

    fn process_request(
        &mut self,
        request: &ChangeRequest,
        op_set: Rc<OpSet>,
        start_op: u64,
    ) -> Result<Rc<Change>, AutomergeError> {
        let time = request.time.unwrap_or_else(|| time::unix_timestamp());
        let actor_id = request.actor.clone();
        let mut operations: Vec<Operation> = Vec::new();
        // this is a local cache of elemids that I can manipulate as i insert and edit so the
        // index's stay consistent as I walk through the ops
        let mut elemid_cache: HashMap<ObjectID, Box<dyn OrderedSet<OpID>>> = HashMap::new();
        if let Some(ops) = &request.ops {
            for rop in ops.iter() {
                let id = OpID(start_op + (operations.len() as u64), actor_id.0.clone());
                let insert = rop.insert;
                let object_id = self.str_to_object(&rop.obj)?;

                let child = match &rop.child {
                    Some(child) => {
                        self.obj_alias
                            .insert(child.clone(), ObjectID::ID(id.clone()));
                        Some(self.str_to_object(&child)?)
                    }
                    None => None,
                };

                // Ok - this madness is that 30% of the execution time for lists was spent
                // in resolve_key making tiny throw away edits to object.seq
                // OrdDelta offered a huge speedup but this would blow up for
                // huge bulk load changes so this way I do one vs the other
                // I should run benchmarks and figure out where the correct break point
                // really is
                // !!!
                // Idea - maybe the correct fast path here is feed the ops into op_set
                // as they are generated so I dont need to make these list ops twice
                // and when the version is out of date - i need to apply ops to that anyway...
                let elemids = elemid_cache.entry(object_id.clone()).or_insert_with(|| {
                    if ops.len() > 2000 {
                        Box::new(
                            op_set
                                .get_obj(&object_id)
                                .map(|o| o.seq.clone())
                                .ok()
                                .unwrap_or_default(),
                        )
                    } else {
                        Box::new(OrdDelta::new(
                            op_set.get_obj(&object_id).map(|o| &o.seq).ok(),
                        ))
                    }
                });
                let elemids2: &mut dyn OrderedSet<OpID> = elemids.borrow_mut(); // I dont understand why I need to do this

                let key = rop.resolve_key(&id, elemids2)?;
                let pred = op_set.get_pred(&object_id, &key, insert);
                let action = match rop.action {
                    ReqOpType::MakeMap => OpType::Make(ObjType::Map),
                    ReqOpType::MakeTable => OpType::Make(ObjType::Table),
                    ReqOpType::MakeList => OpType::Make(ObjType::List),
                    ReqOpType::MakeText => OpType::Make(ObjType::Text),
                    ReqOpType::Del => OpType::Del,
                    ReqOpType::Link => OpType::Link(
                        child.ok_or_else(|| AutomergeError::LinkMissingChild(id.clone()))?,
                    ),
                    ReqOpType::Inc => OpType::Inc(rop.to_i64()?),
                    ReqOpType::Set => OpType::Set(rop.primitive_value()),
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
            return Err(AutomergeError::InvalidChangeRequest(
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
            .ok_or_else(|| AutomergeError::InvalidChangeRequest("no redo ops".to_string()))?;

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

    pub fn load_changes_binary(&mut self, data: Vec<Vec<u8>>) -> Result<(), AutomergeError> {
        let mut changes = Vec::new();
        for d in data { changes.extend(bin_to_changes(&d)?); }
        self.load_changes(changes)

    }

    pub fn load_changes(&mut self, mut changes: Vec<Change>) -> Result<(), AutomergeError> {
        let changes = changes.drain(0..).map(Rc::new).collect();
        self.apply(changes, None, false, false)?;
        Ok(())
    }

    pub fn apply_changes_binary(&mut self, data: Vec<Vec<u8>>) -> Result<Patch, AutomergeError> {
        let mut changes = Vec::new();
        for d in data { changes.extend(bin_to_changes(&d)?); }
        self.apply_changes(changes)
    }

    pub fn apply_changes(&mut self, mut changes: Vec<Change>) -> Result<Patch, AutomergeError> {
        let op_set = Some(self.op_set.clone());

        self.versions.iter_mut().for_each(|v| {
            if v.local_state == None {
                v.local_state = op_set.clone()
            }
        });

        let changes = changes.drain(0..).map(Rc::new).collect();
        self.apply(changes, None, false, true)
    }

    fn get_version(&mut self, version: u64) -> Result<Rc<OpSet>, AutomergeError> {
        let v = self
            .versions
            .iter_mut()
            .find(|v| v.version == version)
            .ok_or_else(|| AutomergeError::UnknownVersion(version))?;
        if let Some(ref mut op_set) = v.local_state {
            // apply the queued ops lazily b/c hopefully these
            // can be thrown away before they are applied
            for change in v.queue.drain(0..) {
                Rc::make_mut(op_set)
                    .apply_ops(OpHandle::extract(change.clone()), false)
                    .unwrap();
            }
            return Ok(op_set.clone());
        }
        Ok(self.op_set.clone())
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
                queue: Vec::new(),
                local_state: None,
            };
            self.versions.push(version_obj);
        } else {
            let version_obj = Version {
                version: 0,
                queue: Vec::new(),
                local_state: None,
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

        let ver = self.get_version(request.version)?;

        let actor = request.actor.clone();
        request.deps.get_or_insert_with(|| ver.deps.without(&actor));

        let start_op = self.max_op + 1;
        let change = match request.request_type {
            ChangeRequestType::Change => self.process_request(&request, ver, start_op)?,
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
            self.apply_change(change, undoable)
        } else {
            self.queue.push(change);
            self.apply_queued_ops()
        }
    }

    fn apply_queued_ops(&mut self) -> Result<Vec<PendingDiff>, AutomergeError> {
        let mut all_diffs = Vec::new();
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            let diffs = self.apply_change(next_change, false)?;
            all_diffs.extend(diffs)
        }
        Ok(all_diffs)
    }

    fn apply_change(
        &mut self,
        change: Rc<Change>,
        undoable: bool,
    ) -> Result<Vec<PendingDiff>, AutomergeError> {
        let all_deps = self.states.add_change(&change)?;

        if all_deps.is_none() {
            return Ok(Vec::new());
        }

        let all_deps = all_deps.unwrap();

        let op_set = Rc::make_mut(&mut self.op_set);

        self.clock.set(&change.actor_id, change.seq);

        op_set.deps.subtract(&all_deps);
        op_set.deps.set(&change.actor_id, change.seq);

        self.max_op = max(self.max_op, change.max_op());

        let (undo_ops, diffs) = op_set.apply_ops(OpHandle::extract(change), undoable)?;

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
            if v.local_state.is_some() {
                v.queue.push(change.clone())
            }
        }

        Ok(())
    }

    pub fn history(&self) -> Vec<&Change> {
        // FIXME
        self.states.history.iter().map(|rc| rc.as_ref()).collect()
    }

    pub fn get_patch(&self) -> Result<Patch, AutomergeError> {
        let diffs = self.op_set.construct_object(&ObjectID::Root)?;
        self.make_patch(Some(diffs), None)
    }

    pub fn get_changes_for_actor_id(&self, actor_id: &ActorID) -> Result<Vec<Vec<u8>>,AutomergeError> {
        changes_to_bin(&self.states.get(actor_id))
    }

    pub fn get_missing_changes(&self, since: &Clock) -> Result<Vec<Vec<u8>>,AutomergeError> {
        let changes : Vec<&Change> = self.states.history.iter()
            .map(|change| change.as_ref())
            .filter(|change| change.seq > since.get(&change.actor_id))
            .collect();
        changes_to_bin(&changes)
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

    /*
    pub fn merge(&mut self, remote: &Backend) -> Result<Patch, AutomergeError> {
        let missing_changes = remote.get_missing_changes(&self.clock);
        self.apply_changes_binary(missing_changes)
    }
    */

    fn push_undo_ops(&mut self, undo_ops: Vec<UndoOperation>) {
        self.undo_stack.truncate(self.undo_pos);
        self.undo_stack.push(undo_ops);
        self.undo_pos += 1;
    }
}

#[derive(Debug, PartialEq, Clone)]
struct Version {
    version: u64,
    local_state: Option<Rc<OpSet>>,
    queue: Vec<Rc<Change>>,
}

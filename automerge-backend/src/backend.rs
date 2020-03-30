use crate::error::AutomergeError;
use crate::op_set::{OpSet, Version};
use crate::patch::{Diff, Patch};
use crate::protocol::{
    DataType, ObjAlias, ObjType, ObjectID, OpType, Operation, ReqOpType, UndoOperation,
};
use crate::time;
use crate::{ActorID, Change, ChangeRequest, ChangeRequestType, Clock, OpID};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    versions: Vec<Version>,
    op_set: OpSet,
    obj_alias: ObjAlias,
}

impl Backend {
    pub fn init() -> Backend {
        let mut versions = Vec::new();
        versions.push(Version {
            version: 0,
            local_only: true,
            op_set: OpSet::init(),
        });
        Backend {
            versions,
            op_set: OpSet::init(),
            obj_alias: ObjAlias::new(),
        }
    }

    fn process_request(
        &mut self,
        request: &ChangeRequest,
        op_set: &OpSet,
        start_op: u64,
    ) -> Result<Change, AutomergeError> {
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

                let mut elemids = elemid_cache
                    .entry(object_id.clone())
                    .or_insert_with(|| op_set.get_elem_ids(&object_id));

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
        Ok(Change {
            start_op,
            message: request.message.clone(),
            actor_id: request.actor.clone(),
            seq: request.seq,
            deps: request.deps.clone().unwrap_or_default(),
            time,
            operations,
        })
    }

    fn make_patch(
        &self,
        diffs: Option<Diff>,
        request: Option<&ChangeRequest>,
        incremental: bool,
    ) -> Result<Patch, AutomergeError> {
        Ok(Patch {
            version: self.versions.last().map(|v| v.version).unwrap_or(0),
            can_undo: self.op_set.can_undo(),
            can_redo: self.op_set.can_redo(),
            diffs,
            clock: if incremental {
                None
            } else {
                Some(self.op_set.clock.clone())
            },
            actor: request.map(|r| r.actor.clone()),
            seq: request.map(|r| r.seq),
        })
    }

    fn undo(&mut self, request: &ChangeRequest, start_op: u64) -> Result<Change, AutomergeError> {
        let undo_pos = self.op_set.undo_pos;

        if undo_pos < 1 || self.op_set.undo_stack.len() < undo_pos {
            return Err(AutomergeError::InvalidChange(
                "Cannot undo: there is nothing to be undone".to_string(),
            ));
        }

        let mut undo_ops = self.op_set.undo_stack.remove(undo_pos - 1);
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

        let change = Change {
            actor_id: request.actor.clone(),
            seq: request.seq,
            start_op,
            deps: request.deps.clone().unwrap_or_default(),
            message: request.message.clone(),
            time: time::unix_timestamp(),
            operations,
        };

        self.op_set.undo_pos -= 1;
        self.op_set.redo_stack.push(redo_ops);

        Ok(change)
    }

    fn redo(&mut self, request: &ChangeRequest, start_op: u64) -> Result<Change, AutomergeError> {
        let mut redo_ops = self
            .op_set
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

        let change = Change {
            actor_id: request.actor.clone(),
            seq: request.seq,
            start_op,
            deps: request.deps.clone().unwrap_or_default(),
            message: request.message.clone(),
            time: time::unix_timestamp(),
            operations,
        };

        self.op_set.undo_pos += 1;

        Ok(change)
    }

    pub fn load_changes(&mut self, changes: Vec<Change>) -> Result<(), AutomergeError> {
        self.apply(changes, None, false, false)?;
        Ok(())
    }

    pub fn apply_changes(&mut self, changes: Vec<Change>) -> Result<Patch, AutomergeError> {
        self.versions.iter_mut().for_each(|v| v.local_only = false);
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
        mut changes: Vec<Change>,
        request: Option<&ChangeRequest>,
        undoable: bool,
        incremental: bool,
    ) -> Result<Patch, AutomergeError> {
        let mut pending_diffs = Vec::new();

        for change in changes.drain(0..) {
            let diffs = self
                .op_set
                .add_change(change, request.is_some(), undoable)?;
            pending_diffs.extend(diffs);
        }

        if incremental {
            let version = self.versions.last().map(|v| v.version).unwrap_or(0) + 1;
            let version_obj = Version {
                version,
                local_only: true,
                op_set: self.op_set.clone(),
            };
            self.versions.push(version_obj);
        } else {
            let version_obj = Version {
                version: 0,
                local_only: true,
                op_set: self.op_set.clone(),
            };
            self.versions.clear();
            self.versions.push(version_obj);
        }

        let diffs = self.op_set.finalize_diffs(pending_diffs)?;

        self.make_patch(diffs, request, true)
    }

    pub fn apply_local_change(
        &mut self,
        mut request: ChangeRequest,
    ) -> Result<Patch, AutomergeError> {
        self.op_set.check_for_duplicate(&request)?; // Change has already been applied

        let version = self.get_version(request.version)?.clone();

        let actor = request.actor.clone();
        request
            .deps
            .get_or_insert_with(|| version.op_set.deps.without(&actor));

        let start_op = self.op_set.max_op + 1;
        let change = match request.request_type {
            ChangeRequestType::Change => {
                self.process_request(&request, &version.op_set, start_op)?
            }
            ChangeRequestType::Undo => self.undo(&request, start_op)?,
            ChangeRequestType::Redo => self.redo(&request, start_op)?,
        };

        let undoable = request.request_type == ChangeRequestType::Change && request.undoable;

        let patch = self.apply(vec![change.clone()], Some(&request), undoable, true)?;

        self.finalize_version(request.version, change)?;

        Ok(patch)

        //        Ok(self.make_patch(diffs.unwrap(), Some(&tmp_request), true)?)
    }

    fn finalize_version(
        &mut self,
        request_version: u64,
        change: Change,
    ) -> Result<(), AutomergeError> {
        // remove all versions older than this one
        // i wish i had drain filter
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
                v.op_set = self.op_set.clone()
            } else {
                v.op_set = self.op_set.clone();
                v.op_set.add_change(change.clone(), true, false)?;
            }
        }

        Ok(())
    }

    pub fn undo_stack(&self) -> &Vec<Vec<UndoOperation>> {
        &self.op_set.undo_stack
    }

    pub fn redo_stack(&self) -> &Vec<Vec<UndoOperation>> {
        &self.op_set.redo_stack
    }

    pub fn history(&self) -> Vec<&Change> {
        self.op_set
            .states
            .history
            .iter()
            .map(|rc| rc.as_ref())
            .collect()
    }

    pub fn get_patch(&self) -> Result<Patch, AutomergeError> {
        let diffs = self.op_set.construct_object(&ObjectID::Root)?;
        self.make_patch(Some(diffs), None, false)
    }

    /// Get changes which are in `other` but not in this backend
    pub fn get_changes<'a>(&self, other: &'a Backend) -> Result<Vec<&'a Change>, AutomergeError> {
        if self.clock().divergent(&other.clock()) {
            return Err(AutomergeError::DivergedState(
                "Cannot diff two states that have diverged".to_string(),
            ));
        }
        Ok(other.op_set.get_missing_changes(&self.op_set.clock))
    }

    pub fn get_changes_for_actor_id(&self, actor_id: &ActorID) -> Vec<&Change> {
        self.op_set.states.get(actor_id)
    }

    pub fn get_missing_changes(&self, clock: Clock) -> Vec<&Change> {
        self.op_set.get_missing_changes(&clock)
    }

    pub fn get_missing_deps(&self) -> Clock {
        self.op_set.get_missing_deps()
    }

    pub fn get_elem_ids(&self, object_id: &ObjectID) -> Vec<OpID> {
        self.op_set.get_elem_ids(object_id)
    }

    pub fn merge(&mut self, remote: &Backend) -> Result<Patch, AutomergeError> {
        let missing_changes = remote
            .get_missing_changes(self.op_set.clock.clone())
            .iter()
            .cloned()
            .cloned()
            .collect();
        self.apply_changes(missing_changes)
    }

    pub fn clock(&self) -> &Clock {
        &self.op_set.clock
    }
}

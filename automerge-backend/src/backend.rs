use crate::actor_map::ActorMap;
use crate::error::AutomergeError;
use crate::internal::{InternalOpType, InternalOperation, InternalUndoOperation, ObjectID, OpID};
use crate::op::Operation;
use crate::op_handle::OpHandle;
use crate::op_set::OpSet;
use crate::op_type::OpType;
use crate::ordered_set::OrderedSet;
use crate::pending_diff::PendingDiff;
use crate::time;
use crate::undo_operation::UndoOperation;
use crate::{Change, UnencodedChange};
use automerge_protocol as amp;
use std::borrow::BorrowMut;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::str::FromStr;

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    versions: Vec<Version>,
    queue: Vec<Rc<Change>>,
    op_set: Rc<OpSet>,
    states: HashMap<amp::ActorID, Vec<Rc<Change>>>,
    actors: ActorMap,
    obj_alias: HashMap<String, amp::ObjectID>,
    undo_pos: usize,
    hashes: HashMap<amp::ChangeHash, Rc<Change>>,
    history: Vec<amp::ChangeHash>,
    internal_undo_stack: Vec<Vec<InternalUndoOperation>>,
    internal_redo_stack: Vec<Vec<InternalUndoOperation>>,
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
            actors: ActorMap::new(),
            obj_alias: HashMap::new(),
            states: HashMap::new(),
            history: Vec::new(),
            hashes: HashMap::new(),
            internal_undo_stack: Vec::new(),
            undo_pos: 0,
            internal_redo_stack: Vec::new(),
        }
    }

    fn str_to_object(&self, name: &str) -> Result<amp::ObjectID, AutomergeError> {
        self.obj_alias
            .get(name)
            .cloned()
            .or_else(|| amp::ObjectID::from_str(name).ok())
            .ok_or_else(|| AutomergeError::MissingChildID(name.to_string()))
    }

    fn process_request(
        &mut self,
        request: &amp::Request,
        op_set: Rc<OpSet>,
        start_op: u64,
    ) -> Result<Rc<Change>, AutomergeError> {
        let time = request.time.unwrap_or_else(time::unix_timestamp);
        let actor_id = request.actor.clone();
        let mut operations: Vec<Operation> = Vec::new();
        // this is a local cache of elemids that I can manipulate as i insert and edit so the
        // index's stay consistent as I walk through the ops
        let mut elemid_cache: HashMap<ObjectID, Box<dyn OrderedSet<OpID>>> = HashMap::new();
        if let Some(ops) = &request.ops {
            for rop in ops.iter() {
                let external_id = amp::OpID::new(start_op + (operations.len() as u64), &actor_id);
                let internal_id = self.actors.import_opid(external_id.clone());
                let insert = rop.insert;
                let object_id = self.str_to_object(&rop.obj)?;
                let internal_object_id = self.actors.import_obj(object_id.clone());

                let child = match &rop.child {
                    Some(child) => {
                        self.obj_alias
                            .insert(child.clone(), amp::ObjectID::ID(external_id.clone()));
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
                let elemids = elemid_cache.entry(internal_object_id).or_insert_with(|| {
                    //if ops.len() > 2000 {
                    Box::new(
                        op_set
                            .get_obj(&internal_object_id)
                            .map(|o| o.seq.clone())
                            .ok()
                            .unwrap_or_default(),
                    )
                    /*
                    } else {
                        Box::new(OrdDelta::new(
                            op_set.get_obj(&internal_object_id).map(|o| &o.seq).ok(),
                        ))
                    }
                    */
                });
                let elemids2: &mut dyn OrderedSet<OpID> = elemids.borrow_mut(); // I dont understand why I need to do this

                let external_key = resolve_key(rop, &internal_id, &self.actors, elemids2)?;
                let internal_key = self.actors.import_key(external_key.clone());
                let pred = op_set.get_pred(&internal_object_id, &internal_key, insert);
                let action = match rop.action {
                    amp::OpType::MakeMap => OpType::Make(amp::ObjType::map()),
                    amp::OpType::MakeTable => OpType::Make(amp::ObjType::table()),
                    amp::OpType::MakeList => OpType::Make(amp::ObjType::list()),
                    amp::OpType::MakeText => OpType::Make(amp::ObjType::text()),
                    amp::OpType::Del => OpType::Del,
                    amp::OpType::Link => OpType::Link(
                        child
                            .ok_or_else(|| AutomergeError::LinkMissingChild(external_id.clone()))?,
                    ),
                    amp::OpType::Inc => OpType::Inc(
                        rop.to_i64()
                            .ok_or_else(|| AutomergeError::MissingNumberValue(rop.clone()))?,
                    ),
                    amp::OpType::Set => OpType::Set(rop.primitive_value()),
                };

                let op = Operation {
                    action,
                    obj: object_id.clone(),
                    key: external_key,
                    pred: pred.iter().map(|id| self.actors.export_opid(&id)).collect(),
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
        Ok(Rc::new(
            UnencodedChange {
                start_op,
                message: request.message.clone(),
                actor_id: request.actor.clone(),
                seq: request.seq,
                deps: request.deps.clone().unwrap_or_default(),
                time,
                operations,
            }
            .into(),
        ))
    }

    fn make_patch(
        &self,
        diffs: Option<amp::Diff>,
        request: Option<&amp::Request>,
    ) -> Result<amp::Patch, AutomergeError> {
        let mut deps: Vec<_> = self.op_set.deps.iter().cloned().collect();
        deps.sort_unstable();
        Ok(amp::Patch {
            version: self.versions.last().map(|v| v.version).unwrap_or(0),
            can_undo: self.can_undo(),
            can_redo: self.can_redo(),
            diffs,
            deps,
            clock: self
                .states
                .iter()
                .map(|(k, v)| (k.clone(), v.len() as u64))
                .collect(),
            actor: request.map(|r| r.actor.clone()),
            seq: request.map(|r| r.seq),
        })
    }

    fn undo(
        &mut self,
        request: &amp::Request,
        start_op: u64,
    ) -> Result<Rc<Change>, AutomergeError> {
        let undo_pos = self.undo_pos;

        if undo_pos < 1 || self.internal_undo_stack.len() < undo_pos {
            return Err(AutomergeError::NoUndo);
        }

        let mut undo_ops = self.internal_undo_stack.get(undo_pos - 1).unwrap().clone();
        let mut redo_ops = Vec::new();

        let operations = undo_ops
            .drain(0..)
            .map(|undo_op| {
                if let Some(field_ops) = self.op_set.get_field_ops(&undo_op.obj, &undo_op.key) {
                    let pred = field_ops.iter().map(|op| op.id).collect();
                    let op = undo_op.into_operation(pred);
                    redo_ops.extend(op.generate_redos(&field_ops));
                    op
                } else {
                    let op = undo_op.into_operation(Vec::new());
                    redo_ops.extend(op.generate_redos(&Vec::new()));
                    op
                }
            })
            .map(|op| self.actors.export_op(&op)) // FIXME
            .collect();

        let change = UnencodedChange {
            actor_id: request.actor.clone(),
            seq: request.seq,
            start_op,
            deps: request.deps.clone().unwrap_or_default(),
            message: request.message.clone(),
            time: time::unix_timestamp(),
            operations,
        }
        .into();

        self.undo_pos -= 1;
        self.internal_redo_stack.push(redo_ops);

        Ok(Rc::new(change))
    }

    fn redo(
        &mut self,
        request: &amp::Request,
        start_op: u64,
    ) -> Result<Rc<Change>, AutomergeError> {
        let mut redo_ops = self
            .internal_redo_stack
            .pop()
            .ok_or(AutomergeError::NoRedo)?;

        let operations = redo_ops
            .drain(0..)
            .map(|redo_op| {
                if let Some(field_ops) = self.op_set.get_field_ops(&redo_op.obj, &redo_op.key) {
                    redo_op.into_operation(field_ops.iter().map(|op| op.id).collect())
                } else {
                    redo_op.into_operation(Vec::new())
                }
            })
            .map(|op| self.actors.export_op(&op)) // FIXME
            .collect();

        let change = UnencodedChange {
            actor_id: request.actor.clone(),
            seq: request.seq,
            start_op,
            deps: request.deps.clone().unwrap_or_default(),
            message: request.message.clone(),
            time: time::unix_timestamp(),
            operations,
        }
        .into();

        self.undo_pos += 1;

        Ok(Rc::new(change))
    }

    pub fn load_changes(&mut self, mut changes: Vec<Change>) -> Result<(), AutomergeError> {
        let changes = changes.drain(0..).map(Rc::new).collect();
        self.apply(changes, None, false, false)?;
        Ok(())
    }

    pub fn apply_changes(
        &mut self,
        mut changes: Vec<Change>,
    ) -> Result<amp::Patch, AutomergeError> {
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
                let mut m = HashMap::new();
                Rc::make_mut(op_set)
                    .apply_ops(
                        OpHandle::extract(change, &mut self.actors),
                        false,
                        &mut m,
                        &self.actors,
                    )
                    .unwrap();
            }
            return Ok(op_set.clone());
        }
        Ok(self.op_set.clone())
    }

    fn apply(
        &mut self,
        mut changes: Vec<Rc<Change>>,
        request: Option<&amp::Request>,
        undoable: bool,
        incremental: bool,
    ) -> Result<amp::Patch, AutomergeError> {
        let mut pending_diffs = HashMap::new();

        for change in changes.drain(..) {
            self.add_change(change, request.is_some(), undoable, &mut pending_diffs)?;
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

        let diffs = self.op_set.finalize_diffs(pending_diffs, &self.actors)?;

        self.make_patch(diffs, request)
    }

    pub fn apply_local_change(
        &mut self,
        mut request: amp::Request,
    ) -> Result<(amp::Patch,Rc<Change>), AutomergeError> {
        self.check_for_duplicate(&request)?; // Change has already been applied

        let ver = self.get_version(request.version)?;

        request
            .deps
            .get_or_insert_with(|| ver.deps.iter().cloned().collect());

        let start_op = ver.max_op + 1;
        let change = match request.request_type {
            amp::RequestType::Change => self.process_request(&request, ver, start_op)?,
            amp::RequestType::Undo => self.undo(&request, start_op)?,
            amp::RequestType::Redo => self.redo(&request, start_op)?,
        };

        let undoable = request.request_type == amp::RequestType::Change && request.undoable;

        let patch = self.apply(vec![change.clone()], Some(&request), undoable, true)?;

        self.finalize_version(request.version, change.clone())?;

        Ok((patch,change))
    }

    fn check_for_duplicate(&self, request: &amp::Request) -> Result<(), AutomergeError> {
        if self
            .states
            .get(&request.actor)
            .map(|v| v.len() as u64)
            .unwrap_or(0)
            >= request.seq
        {
            return Err(AutomergeError::DuplicateChange(format!(
                "Change request has already been applied {}:{}",
                request.actor.to_hex_string(),
                request.seq
            )));
        }
        Ok(())
    }

    fn add_change(
        &mut self,
        change: Rc<Change>,
        local: bool,
        undoable: bool,
        diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
    ) -> Result<(), AutomergeError> {
        if local {
            self.apply_change(change, undoable, diffs)
        } else {
            self.queue.push(change);
            self.apply_queued_ops(diffs)
        }
    }

    fn apply_queued_ops(
        &mut self,
        diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
    ) -> Result<(), AutomergeError> {
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            self.apply_change(next_change, false, diffs)?;
        }
        Ok(())
    }

    fn apply_change(
        &mut self,
        change: Rc<Change>,
        undoable: bool,
        diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
    ) -> Result<(), AutomergeError> {
        if self.hashes.contains_key(&change.hash) {
            return Ok(());
        }

        self.states
            .entry(change.actor_id())
            .or_default()
            .push(change.clone());

        self.hashes.insert(change.hash, change.clone());

        self.history.push(change.hash);

        let op_set = Rc::make_mut(&mut self.op_set);

        op_set.max_op = max(op_set.max_op, change.max_op());

        for d in change.deps.iter() {
            op_set.deps.remove(d);
        }
        op_set.deps.insert(change.hash);

        let undo_ops = op_set.apply_ops(
            OpHandle::extract(change, &mut self.actors),
            undoable,
            diffs,
            &self.actors,
        )?;

        if undoable {
            self.push_undo_ops(undo_ops);
        };

        Ok(())
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Rc<Change>> {
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

    pub fn get_patch(&self) -> Result<amp::Patch, AutomergeError> {
        let diffs = self
            .op_set
            .construct_object(&ObjectID::Root, &self.actors)?;
        self.make_patch(Some(diffs), None)
    }

    pub fn get_changes_for_actor_id(
        &self,
        actor_id: &amp::ActorID,
    ) -> Result<Vec<&Change>, AutomergeError> {
        Ok(self
            .states
            .get(actor_id)
            .map(|vec| vec.iter().map(|c| c.as_ref()).collect())
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
            .map(|rc| rc.as_ref())
            .collect()
    }

    fn can_undo(&self) -> bool {
        self.undo_pos > 0
    }

    fn can_redo(&self) -> bool {
        !self.internal_redo_stack.is_empty()
    }

    pub fn save(&self) -> Result<Vec<u8>, AutomergeError> {
        let bytes: Vec<&[u8]> = self
            .history
            .iter()
            .filter_map(|hash| self.hashes.get(&hash))
            .map(|r| r.bytes.as_slice())
            .collect();
        Ok(bytes.concat())
    }

    pub fn load(data: Vec<u8>) -> Result<Self, AutomergeError> {
        let changes = Change::parse(&data)?;
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

    fn push_undo_ops(&mut self, undo_ops: Vec<InternalUndoOperation>) {
        self.internal_undo_stack.truncate(self.undo_pos);
        self.internal_undo_stack.push(undo_ops);
        self.undo_pos += 1;
    }

    pub fn undo_stack(&self) -> Vec<Vec<UndoOperation>> {
        self.internal_undo_stack
            .iter()
            .map(|ops| ops.iter().map(|op| self.actors.export_undo(op)).collect())
            .collect()
    }

    pub fn redo_stack(&self) -> Vec<Vec<UndoOperation>> {
        self.internal_redo_stack
            .iter()
            .map(|ops| ops.iter().map(|op| self.actors.export_undo(op)).collect())
            .collect()
    }
}

#[derive(Debug, PartialEq, Clone)]
struct Version {
    version: u64,
    local_state: Option<Rc<OpSet>>,
    queue: Vec<Rc<Change>>,
}

fn resolve_key(
    rop: &amp::Op,
    id: &OpID,
    actors: &ActorMap,
    ids: &mut dyn OrderedSet<OpID>,
) -> Result<amp::Key, AutomergeError> {
    let key = &rop.key;
    let insert = rop.insert;
    let del = rop.action == amp::OpType::Del;
    match key {
        amp::RequestKey::Str(s) => Ok(amp::Key::Map(s.clone())),
        amp::RequestKey::Num(n) => {
            let n: usize = *n as usize;
            (if insert {
                if n == 0 {
                    ids.insert_index(0, *id);
                    Some(amp::Key::head())
                } else {
                    ids.insert_index(n, *id);
                    ids.key_of(n - 1).map(|i| actors.export_opid(&i).into())
                }
            } else if del {
                ids.remove_index(n).map(|k| actors.export_opid(&k).into())
            } else {
                ids.key_of(n).map(|i| actors.export_opid(&i).into())
            })
            .ok_or(AutomergeError::IndexOutOfBounds(n))
        }
    }
}

/// Extension trait adding a few helper methods with backend specific logic
/// to `Operation`
trait OpExt {
    fn generate_redos(&self, overwritten: &[OpHandle]) -> Vec<InternalUndoOperation>;
    //fn can_merge(&self, other: &InternalOperation) -> bool;
    //fn merge(&mut self, other: InternalOperation);
}

impl OpExt for InternalOperation {
    fn generate_redos(&self, overwritten: &[OpHandle]) -> Vec<InternalUndoOperation> {
        let key = self.key.clone();

        if let InternalOpType::Inc(value) = self.action {
            vec![InternalUndoOperation {
                action: InternalOpType::Inc(-value),
                obj: self.obj,
                key,
            }]
        } else if overwritten.is_empty() {
            vec![InternalUndoOperation {
                action: InternalOpType::Del,
                obj: self.obj,
                key,
            }]
        } else {
            overwritten.iter().map(|o| o.invert(&key)).collect()
        }
    }

    /*
    fn can_merge(&self, other: &InternalOperation) -> bool {
        !self.insert && !other.insert && other.obj == self.obj && other.key == self.key
    }

    fn merge(&mut self, other: InternalOperation) {
        if let OpType::Inc(delta) = other.action {
            match self.action {
                OpType::Set(amp::Value::Counter(number)) => {
                    self.action = OpType::Set(amp::Value::Counter(number + delta))
                }
                OpType::Inc(number) => self.action = OpType::Inc(number + delta),
                _ => {}
            } // error?
        } else {
            match other.action {
                OpType::Set(_) | OpType::Link(_) | OpType::Del => self.action = other.action,
                _ => {}
            }
        }
    }
    */
}

use crate::actor_map::ActorMap;
use crate::error::AutomergeError;
use crate::internal::{InternalOp, InternalUndoOperation, Key, ObjectID, OpID};
use crate::obj_alias::ObjAlias;
use crate::op::{compress_ops, Operation};
use crate::op_handle::OpHandle;
use crate::op_set::OpSet;
use crate::op_type::OpType;
use crate::ordered_set::{OrderedSet, SkipList};
use crate::pending_diff::PendingDiff;
use crate::time;
use crate::undo_operation::UndoOperation;
use crate::{Change, UnencodedChange};
use automerge_protocol as amp;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    versions: Vec<Version>,
    queue: Vec<Rc<Change>>,
    op_set: Rc<OpSet>,
    states: HashMap<amp::ActorID, Vec<Rc<Change>>>,
    actors: ActorMap,
    obj_alias: ObjAlias,
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
        versions.push(Version::new(0));
        Backend {
            versions,
            op_set,
            queue: Vec::new(),
            actors: ActorMap::new(),
            obj_alias: ObjAlias::new(),
            states: HashMap::new(),
            history: Vec::new(),
            hashes: HashMap::new(),
            internal_undo_stack: Vec::new(),
            undo_pos: 0,
            internal_redo_stack: Vec::new(),
        }
    }


    fn process_request(
        &mut self,
        request: amp::Request,
    ) -> Result<(amp::Patch, Rc<Change>), AutomergeError> {
        let mut all_undo_ops = Vec::new();
        let mut new_objects: HashSet<ObjectID> = HashSet::new();
        let mut operations: Vec<Operation> = Vec::new();
        let mut pending_diffs: HashMap<ObjectID, Vec<PendingDiff>> = HashMap::new();

        let start_op = self.get_start_op(request.version);
        let actor_seq = Some((request.actor.clone(), request.seq));
        self.lazy_update_version(request.version)?;
        let version_local_state = self
            .versions
            .iter_mut()
            .find(|v| v.version == request.version)
            .and_then(|v| v.local_state.as_mut());
        let not_head = version_local_state.is_some();
        let op_set = Rc::make_mut(version_local_state.unwrap_or(&mut self.op_set));
        if let Some(ops) = &request.ops {
            let ops = compress_ops(ops);
            for rop in ops.iter() {
                let op_counter = start_op + (operations.len() as u64);

                let (op,internal_op) = rop_to_op(&mut self.actors, &mut self.obj_alias, &op_set, &request, &rop, op_counter)?;
                let (pending_diff, undo_ops) = op_set.apply_op(internal_op.clone(), &self.actors)?;
                handle_undo(internal_op, pending_diff, undo_ops, request.undoable, &mut pending_diffs, &mut all_undo_ops, &mut new_objects);    

                operations.push(op);
            }
        }
        let change: Rc<Change> = Rc::new(
            UnencodedChange {
                start_op,
                message: request.message,
                actor_id: request.actor,
                seq: request.seq,
                deps: request.deps.unwrap_or_default(),
                time: request.time.unwrap_or_else(time::unix_timestamp),
                operations,
            }
            .into(),
        );

        op_set.update_deps(&change);

        if not_head {
            pending_diffs.clear();
            self.apply_change(change.clone(), request.undoable, &mut pending_diffs)?;
        } else {
            self.update_history(&change);
            if request.undoable {
                self.push_undo_ops(all_undo_ops);
            }
        }

        self.bump_version();
        let diffs = self.op_set.finalize_diffs(pending_diffs, &self.actors)?;
        let patch = self.make_patch(diffs, actor_seq)?;
        Ok((patch, change))
    }

    fn make_patch(
        &self,
        diffs: Option<amp::Diff>,
        actor_seq: Option<(amp::ActorID, u64)>,
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
            actor: actor_seq.clone().map(|(actor, _)| actor),
            seq: actor_seq.map(|(_, seq)| seq),
        })
    }

    fn undo(&mut self, request: amp::Request) -> Result<(amp::Patch, Rc<Change>), AutomergeError> {
        let actor_seq = Some((request.actor.clone(), request.seq));
        let start_op = self.get_start_op(request.version);
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
            message: request.message,
            time: time::unix_timestamp(),
            operations,
        }
        .into();

        self.undo_pos -= 1;
        self.internal_redo_stack.push(redo_ops);

        let change: Rc<Change> = Rc::new(change);
        let patch = self.apply(vec![change.clone()], actor_seq, false, true)?;
        Ok((patch, change))
    }

    fn redo(&mut self, request: amp::Request) -> Result<(amp::Patch, Rc<Change>), AutomergeError> {
        let actor_seq = Some((request.actor.clone(), request.seq));
        let start_op = self.get_start_op(request.version);
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
            message: request.message,
            time: time::unix_timestamp(),
            operations,
        }
        .into();

        self.undo_pos += 1;

        let change: Rc<Change> = Rc::new(change);
        let patch = self.apply(vec![change.clone()], actor_seq, false, true)?;
        Ok((patch, change))
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

    fn lazy_update_version(&mut self, version: u64) -> Result<(), AutomergeError> {
        let v = self
            .versions
            .iter_mut()
            .find(|v| v.version == version)
            .ok_or(AutomergeError::UnknownVersion(version))?;
        if let Some(ref mut op_set) = v.local_state {
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
        }
        Ok(())
    }

    fn get_deps(&mut self, version: u64) -> Vec<amp::ChangeHash> {
        let local_state = self
            .versions
            .iter_mut()
            .find(|v| v.version == version)
            .and_then(|v| v.local_state.as_ref());
        if let Some(ref op_set) = local_state {
            op_set.deps.iter().cloned().collect()
        } else {
            self.op_set.deps.iter().cloned().collect()
        }
    }

    fn get_start_op(&mut self, version: u64) -> u64 {
        let local_state = self
            .versions
            .iter_mut()
            .find(|v| v.version == version)
            .and_then(|v| v.local_state.as_ref());
        if let Some(ref op_set) = local_state {
            op_set.max_op + 1
        } else {
            self.op_set.max_op + 1
        }
    }

    fn apply(
        &mut self,
        mut changes: Vec<Rc<Change>>,
        actor: Option<(amp::ActorID, u64)>,
        undoable: bool,
        incremental: bool,
    ) -> Result<amp::Patch, AutomergeError> {
        let mut pending_diffs = HashMap::new();

        for change in changes.drain(..) {
            self.add_change(change, actor.is_some(), undoable, &mut pending_diffs)?;
        }

        if incremental {
            self.bump_version();
        } else {
            self.versions.clear();
            self.versions.push(Version::new(0));
        }

        let diffs = self.op_set.finalize_diffs(pending_diffs, &self.actors)?;
        self.make_patch(diffs, actor)
    }

    pub fn apply_local_change(
        &mut self,
        mut request: amp::Request,
    ) -> Result<(amp::Patch, Rc<Change>), AutomergeError> {
        self.check_for_duplicate(&request)?; // Change has already been applied

        let ver_no = request.version;

        request.deps.get_or_insert_with(|| self.get_deps(ver_no));

        let (patch, change) = match request.request_type {
            amp::RequestType::Change => self.process_request(request)?,
            amp::RequestType::Undo => self.undo(request)?,
            amp::RequestType::Redo => self.redo(request)?,
        };

        self.finalize_version(ver_no, change.clone());

        Ok((patch, change))
    }

    fn bump_version(&mut self) {
        let next_version = self.versions.last().map(|v| v.version).unwrap_or(0) + 1;
        self.versions.push(Version::new(next_version));
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

        self.update_history(&change);

        let op_set = Rc::make_mut(&mut self.op_set);

        op_set.update_deps(&change);

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

    fn update_history(&mut self, change: &Rc<Change>) {
        self.states
            .entry(change.actor_id().clone())
            .or_default()
            .push(change.clone());

        self.history.push(change.hash);
        self.hashes.insert(change.hash, change.clone());
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

    fn finalize_version(&mut self, request_version: u64, change: Rc<Change>) {
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

impl Version {
    fn new(version: u64) -> Self {
        Version {
            version,
            local_state: None,
            queue: Vec::new(),
        }
    }
}

fn resolve_key_onepass(
    rop: &amp::Op,
    seq: &Option<&SkipList<OpID>>,
) -> Result<Key, AutomergeError> {
    //log!("rop={:?}",rop);
    //log!("seq={:?}",seq);
    match &rop.key {
        amp::RequestKey::Str(s) => Ok(Key::Map(s.clone())),
        amp::RequestKey::Num(n) => {
            let n: usize = *n as usize;
            (if rop.insert {
                if n == 0 {
                    Some(Key::head())
                } else {
                    seq.and_then(|ids| ids.key_of(n - 1)).map(|i| (*i).into())
                }
            } else {
                seq.and_then(|ids| ids.key_of(n)).map(|i| (*i).into())
            })
            .ok_or(AutomergeError::IndexOutOfBounds(n))
        }
    }
}

fn rop_to_optype(rop: &amp::Op, child: Option<amp::ObjectID>) -> Result<OpType, AutomergeError> {
    Ok(match rop.action {
        amp::OpType::MakeMap => OpType::Make(amp::ObjType::map()),
        amp::OpType::MakeTable => OpType::Make(amp::ObjType::table()),
        amp::OpType::MakeList => OpType::Make(amp::ObjType::list()),
        amp::OpType::MakeText => OpType::Make(amp::ObjType::text()),
        amp::OpType::Del => OpType::Del,
        amp::OpType::Link => OpType::Link(child.ok_or(AutomergeError::LinkMissingChild)?),
        amp::OpType::Inc => OpType::Inc(rop.to_i64().ok_or(AutomergeError::MissingNumberValue)?),
        amp::OpType::Set => OpType::Set(rop.primitive_value()),
    })
}

fn rop_to_op(actors: &mut ActorMap, obj_alias: &mut ObjAlias, op_set: &OpSet, request: &amp::Request, rop: &amp::Op, op_counter: u64) -> Result<(Operation, OpHandle),AutomergeError> {
            let external_id = amp::OpID::new(op_counter, &request.actor);
            let internal_id = actors.import_opid(&external_id);
            let external_object_id = obj_alias.fetch(&rop.obj)?;
            let internal_object_id = actors.import_obj(&external_object_id);
            let child = obj_alias.cache(&rop.child, &external_id);

            let skip_list = op_set.get_obj(&internal_object_id).map(|o| &o.seq).ok();
            let internal_key = resolve_key_onepass(&rop, &skip_list)?;
            let external_key = actors.export_key(&internal_key);

            let internal_pred = op_set.get_pred(&internal_object_id, &internal_key, rop.insert);
            let mut pred = Vec::with_capacity(internal_pred.len());
            for id in internal_pred.iter() {
                pred.push(actors.export_opid(&id));
            }
            let action = rop_to_optype(&rop, child)?;
            let internal_action = actors.import_optype(&action);

            let external_op = Operation {
                action,
                obj: external_object_id,
                key: external_key,
                pred,
                insert: rop.insert,
            };

            let internal_op = OpHandle {
                id: internal_id,
                op: InternalOp {
                    action: internal_action,
                    obj: internal_object_id,
                    key: internal_key,
                    insert: rop.insert,
                    pred: internal_pred,
                },
                delta: 0,
            };
            Ok((external_op,internal_op))
}

fn handle_undo(internal_op: OpHandle, pending_diff: Option<PendingDiff>, undo_ops: Vec<InternalUndoOperation>, undoable: bool, pending_diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>, all_undo_ops: &mut Vec<InternalUndoOperation>, new_objects: &mut HashSet<ObjectID>) {
    if internal_op.is_make() {
        new_objects.insert(internal_op.id.into());
    }

    let use_undo = undoable && !(new_objects.contains(&internal_op.obj));

    if let Some(d) = pending_diff {
        pending_diffs.entry(internal_op.obj).or_default().push(d);
    }

    if use_undo {
        all_undo_ops.extend(undo_ops);
    }                
}

use crate::actor_map::ActorMap;
use crate::error::AutomergeError;
use crate::internal::{InternalOp, Key, ObjectID, OpID};
use crate::obj_alias::ObjAlias;
use crate::op::{compress_ops, Operation};
use crate::op_handle::OpHandle;
use crate::op_set::OpSet;
use crate::op_type::OpType;
use crate::ordered_set::{OrderedSet, SkipList};
use crate::pending_diff::PendingDiff;
use crate::time;
use crate::{Change, UnencodedChange};
use automerge_protocol as amp;
use std::collections::{HashMap, HashSet};
use core::cmp::max;
use std::rc::Rc;

#[derive(Debug, PartialEq, Clone)]
pub struct Backend {
    versions: Vec<Version>,
    queue: Vec<Rc<Change>>,
    op_set: Rc<OpSet>,
    states: HashMap<amp::ActorID, Vec<Rc<Change>>>,
    actors: ActorMap,
    obj_alias: ObjAlias,
    hashes: HashMap<amp::ChangeHash, Rc<Change>>,
    history: Vec<amp::ChangeHash>,
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
        }
    }


    fn process_request(
        &mut self,
        request: amp::Request,
    ) -> Result<(amp::Patch, Rc<Change>), AutomergeError> {
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
                let pending_diff = op_set.apply_op(internal_op.clone(), &self.actors)?;
                if let Some(d) = pending_diff {
                  pending_diffs.entry(internal_op.obj).or_default().push(d);
                }

                operations.push(op);
            }
        }

        let num_ops = operations.len() as u64;

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

        op_set.max_op = max(op_set.max_op, change.start_op + num_ops - 1);
        op_set.update_deps(&change);

        if not_head {
            pending_diffs.clear();
            self.apply_change(change.clone(), &mut pending_diffs)?;
        } else {
            self.update_history(&change);
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

    pub fn load_changes(&mut self, mut changes: Vec<Change>) -> Result<(), AutomergeError> {
        let changes = changes.drain(0..).map(Rc::new).collect();
        self.apply(changes, None, false)?;
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
        self.apply(changes, None, true)
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
        incremental: bool,
    ) -> Result<amp::Patch, AutomergeError> {
        let mut pending_diffs = HashMap::new();

        for change in changes.drain(..) {
            self.add_change(change, actor.is_some(), &mut pending_diffs)?;
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

        let (patch, change) = self.process_request(request)?;

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
        diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
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
        diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
    ) -> Result<(), AutomergeError> {
        while let Some(next_change) = self.pop_next_causally_ready_change() {
            self.apply_change(next_change, diffs)?;
        }
        Ok(())
    }

    fn apply_change(
        &mut self,
        change: Rc<Change>,
        diffs: &mut HashMap<ObjectID, Vec<PendingDiff>>,
    ) -> Result<(), AutomergeError> {
        if self.hashes.contains_key(&change.hash) {
            return Ok(());
        }

        self.update_history(&change);

        let op_set = Rc::make_mut(&mut self.op_set);

        let start_op = change.start_op;

        op_set.update_deps(&change);

        let ops = OpHandle::extract(change, &mut self.actors);

        op_set.max_op = max(op_set.max_op, start_op + (ops.len() as u64) - 1);

        op_set.apply_ops(
            ops,
            diffs,
            &self.actors,
        )?;

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


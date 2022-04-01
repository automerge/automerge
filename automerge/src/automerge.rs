use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroU64;
use std::ops::RangeBounds;

use crate::change::encode_document;
use crate::exid::ExId;
use crate::keys::Keys;
use crate::op_set::OpSet;
use crate::range::Range;
use crate::transaction::{self, CommitOptions, Failure, Success, Transaction, TransactionInner};
use crate::types::{
    ActorId, AssignPatch, ChangeHash, Clock, ElemId, Export, Exportable, Key, ObjId, Op, OpId,
    OpType, Patch, ScalarValue, Value,
};
use crate::KeysAt;
use crate::{legacy, query, types, ObjType};
use crate::{AutomergeError, Change, Prop};
use serde::Serialize;
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Actor {
    Unused(ActorId),
    Cached(usize),
}

/// An automerge document.
#[derive(Debug, Clone)]
pub struct Automerge {
    /// The list of unapplied changes that are not causally ready.
    pub(crate) queue: Vec<Change>,
    /// The history of changes that form this document, topologically sorted too.
    pub(crate) history: Vec<Change>,
    /// Mapping from change hash to index into the history list.
    pub(crate) history_index: HashMap<ChangeHash, usize>,
    /// Mapping from actor index to list of seqs seen for them.
    pub(crate) states: HashMap<usize, Vec<usize>>,
    /// Current dependencies of this document (heads hashes).
    pub(crate) deps: HashSet<ChangeHash>,
    /// Heads at the last save.
    pub(crate) saved: Vec<ChangeHash>,
    /// The set of operations that form this document.
    pub(crate) ops: OpSet,
    /// The current actor.
    pub(crate) actor: Actor,
    /// The maximum operation counter this document has seen.
    pub(crate) max_op: u64,
    pub(crate) patches: Option<Vec<Patch>>,
}

impl Automerge {
    /// Create a new document with a random actor id.
    pub fn new() -> Self {
        Automerge {
            queue: vec![],
            history: vec![],
            history_index: HashMap::new(),
            states: HashMap::new(),
            ops: Default::default(),
            deps: Default::default(),
            saved: Default::default(),
            actor: Actor::Unused(ActorId::random()),
            max_op: 0,
            patches: None,
        }
    }

    /// Set the actor id for this document.
    pub fn with_actor(mut self, actor: ActorId) -> Self {
        self.actor = Actor::Unused(actor);
        self
    }

    /// Set the actor id for this document.
    pub fn set_actor(&mut self, actor: ActorId) -> &mut Self {
        self.actor = Actor::Unused(actor);
        self
    }

    /// Get the current actor id of this document.
    pub fn get_actor(&self) -> &ActorId {
        match &self.actor {
            Actor::Unused(actor) => actor,
            Actor::Cached(index) => self.ops.m.actors.get(*index),
        }
    }

    pub(crate) fn get_actor_index(&mut self) -> usize {
        match &mut self.actor {
            Actor::Unused(actor) => {
                let index = self
                    .ops
                    .m
                    .actors
                    .cache(std::mem::replace(actor, ActorId::from(&[][..])));
                self.actor = Actor::Cached(index);
                index
            }
            Actor::Cached(index) => *index,
        }
    }

    pub fn enable_patches(&mut self, enable: bool) {
        match (enable, &self.patches) {
            (true, None) => self.patches = Some(vec![]),
            (false, Some(_)) => self.patches = None,
            _ => (),
        }
    }

    pub fn pop_patches(&mut self) -> Vec<Patch> {
        if let Some(patches) = self.patches.take() {
            self.patches = Some(Vec::new());
            patches
        } else {
            Vec::new()
        }
    }

    /// Start a transaction.
    pub fn transaction(&mut self) -> Transaction {
        Transaction {
            inner: Some(self.transaction_inner()),
            doc: self,
        }
    }

    pub(crate) fn transaction_inner(&mut self) -> TransactionInner {
        let actor = self.get_actor_index();
        let seq = self.states.get(&actor).map_or(0, |v| v.len()) as u64 + 1;
        let mut deps = self.get_heads();
        if seq > 1 {
            let last_hash = self.get_hash(actor, seq - 1).unwrap();
            if !deps.contains(&last_hash) {
                deps.push(last_hash);
            }
        }

        TransactionInner {
            actor,
            seq,
            // SAFETY: this unwrap is safe as we always add 1
            start_op: NonZeroU64::new(self.max_op + 1).unwrap(),
            time: 0,
            message: None,
            extra_bytes: Default::default(),
            hash: None,
            operations: vec![],
            deps,
        }
    }

    /// Run a transaction on this document in a closure, automatically handling commit or rollback
    /// afterwards.
    pub fn transact<F, O, E>(&mut self, f: F) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction) -> Result<O, E>,
    {
        let mut tx = self.transaction();
        let result = f(&mut tx);
        match result {
            Ok(result) => Ok(Success {
                result,
                hash: tx.commit(),
            }),
            Err(error) => Err(Failure {
                error,
                cancelled: tx.rollback(),
            }),
        }
    }

    /// Like [`Self::transact`] but with a function for generating the commit options.
    pub fn transact_with<F, O, E, C>(&mut self, c: C, f: F) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions,
    {
        let mut tx = self.transaction();
        let result = f(&mut tx);
        match result {
            Ok(result) => {
                let commit_options = c(&result);
                Ok(Success {
                    result,
                    hash: tx.commit_with(commit_options),
                })
            }
            Err(error) => Err(Failure {
                error,
                cancelled: tx.rollback(),
            }),
        }
    }

    /// Fork this document at the current point for use by a different actor.
    pub fn fork(&self) -> Self {
        let mut f = self.clone();
        f.set_actor(ActorId::random());
        f
    }

    fn insert_op(&mut self, obj: &ObjId, op: Op) {
        let q = self.ops.search(obj, query::SeekOp::new(&op));

        let succ = q.succ;
        let pos = q.pos;
        for i in succ {
            self.ops.replace(obj, i, |old_op| old_op.add_succ(&op));
        }

        if !op.is_delete() {
            self.ops.insert(pos, obj, op);
        }
    }

    fn insert_op_with_patch(&mut self, obj: &ObjId, op: Op) {
        let q = self.ops.search(obj, query::SeekOpWithPatch::new(&op));

        let query::SeekOpWithPatch {
            pos,
            succ,
            seen,
            values,
            had_value_before,
            ..
        } = q;

        let ex_obj = self.id_to_exid(obj.0);
        let key = match op.key {
            Key::Map(index) => self.ops.m.props[index].clone().into(),
            Key::Seq(_) => seen.into(),
        };

        let patch = if op.insert {
            let value = (op.clone_value(), self.id_to_exid(op.id));
            Patch::Insert(ex_obj, seen, value)
        } else if op.is_delete() {
            if let Some(winner) = &values.last() {
                let value = (winner.clone_value(), self.id_to_exid(winner.id));
                let conflict = values.len() > 1;
                Patch::Assign(AssignPatch {
                    obj: ex_obj,
                    key,
                    value,
                    conflict,
                })
            } else {
                Patch::Delete(ex_obj, key)
            }
        } else {
            let winner = if let Some(last_value) = values.last() {
                if self.ops.m.lamport_cmp(op.id, last_value.id) == Ordering::Greater {
                    &op
                } else {
                    last_value
                }
            } else {
                &op
            };
            let value = (winner.clone_value(), self.id_to_exid(winner.id));
            if op.is_list_op() && !had_value_before {
                Patch::Insert(ex_obj, seen, value)
            } else {
                Patch::Assign(AssignPatch {
                    obj: ex_obj,
                    key,
                    value,
                    conflict: !values.is_empty(),
                })
            }
        };

        if let Some(patches) = &mut self.patches {
            patches.push(patch);
        }

        for i in succ {
            self.ops.replace(obj, i, |old_op| old_op.add_succ(&op));
        }

        if !op.is_delete() {
            self.ops.insert(pos, obj, op);
        }
    }

    // KeysAt::()
    // LenAt::()
    // PropAt::()
    // NthAt::()

    /// Get the object id of the object that contains this object and the prop that this object is
    /// at in that object.
    pub fn parent_object<O: AsRef<ExId>>(&self, obj: O) -> Option<(ExId, Prop)> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            if obj == ObjId::root() {
                // root has no parent
                None
            } else {
                self.ops
                    .parent_object(&obj)
                    .map(|(id, key)| (self.id_to_exid(id.0), self.export_key(id, key)))
            }
        } else {
            None
        }
    }

    pub fn path_to_object<O: AsRef<ExId>>(&self, obj: O) -> Vec<(ExId, Prop)> {
        let mut path = Vec::new();
        let mut obj = obj.as_ref().clone();
        while let Some(parent) = self.parent_object(obj) {
            obj = parent.0.clone();
            path.push(parent);
        }
        path.reverse();
        path
    }

    /// Export a key to a prop.
    fn export_key(&self, obj: ObjId, key: Key) -> Prop {
        match key {
            Key::Map(m) => Prop::Map(self.ops.m.props.get(m).into()),
            Key::Seq(opid) => {
                let i = self
                    .ops
                    .search(&obj, query::ElemIdPos::new(opid))
                    .index()
                    .unwrap();
                Prop::Seq(i)
            }
        }
    }

    /// Get the keys of the object `obj`.
    ///
    /// For a map this returns the keys of the map.
    /// For a list this returns the element ids (opids) encoded as strings.
    pub fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            let iter_keys = self.ops.keys(obj);
            Keys::new(self, iter_keys)
        } else {
            Keys::new(self, None)
        }
    }

    /// Historical version of [`keys`](Self::keys).
    pub fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> KeysAt {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            let clock = self.clock_at(heads);
            KeysAt::new(self, self.ops.keys_at(obj, clock))
        } else {
            KeysAt::new(self, None)
        }
    }

    /// Iterate over the keys and values of the object `obj`.
    ///
    /// For a map the keys are the keys of the map.
    /// For a list the keys are the element ids (opids) encoded as strings.
    pub fn range<O: AsRef<ExId>, R: RangeBounds<Prop>>(&self, obj: O, range: R) -> Range<R> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            let iter_range = self.ops.range(obj, range);
            Range::new(self, iter_range)
        } else {
            Range::new(self, None)
        }
    }

    /// Get the length of the given object.
    pub fn length<O: AsRef<ExId>>(&self, obj: O) -> usize {
        if let Ok(inner_obj) = self.exid_to_obj(obj.as_ref()) {
            match self.ops.object_type(&inner_obj) {
                Some(ObjType::Map) | Some(ObjType::Table) => self.keys(obj).count(),
                Some(ObjType::List) | Some(ObjType::Text) => {
                    self.ops.search(&inner_obj, query::Len::new()).len
                }
                None => 0,
            }
        } else {
            0
        }
    }

    /// Historical version of [`length`](Self::length).
    pub fn length_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> usize {
        if let Ok(inner_obj) = self.exid_to_obj(obj.as_ref()) {
            let clock = self.clock_at(heads);
            match self.ops.object_type(&inner_obj) {
                Some(ObjType::Map) | Some(ObjType::Table) => self.keys_at(obj, heads).count(),
                Some(ObjType::List) | Some(ObjType::Text) => {
                    self.ops.search(&inner_obj, query::LenAt::new(clock)).len
                }
                None => 0,
            }
        } else {
            0
        }
    }

    /// Get the type of this object, if it is an object.
    pub fn object_type<O: AsRef<ExId>>(&self, obj: O) -> Option<ObjType> {
        let obj = self.exid_to_obj(obj.as_ref()).ok()?;
        self.ops.object_type(&obj)
    }

    pub(crate) fn exid_to_obj(&self, id: &ExId) -> Result<ObjId, AutomergeError> {
        match id {
            ExId::Root => Ok(ObjId::root()),
            ExId::Id(ctr, actor, idx) => {
                // do a direct get here b/c this could be foriegn and not be within the array
                // bounds
                if self.ops.m.actors.cache.get(*idx) == Some(actor) {
                    Ok(ObjId(OpId(*ctr, *idx)))
                } else {
                    // FIXME - make a real error
                    let idx = self
                        .ops
                        .m
                        .actors
                        .lookup(actor)
                        .ok_or(AutomergeError::Fail)?;
                    Ok(ObjId(OpId(*ctr, idx)))
                }
            }
        }
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(id.0, self.ops.m.actors.cache[id.1].clone(), id.1)
        }
    }

    /// Get the string represented by the given text object.
    pub fn text<O: AsRef<ExId>>(&self, obj: O) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let query = self.ops.search(&obj, query::ListVals::new());
        let mut buffer = String::new();
        for q in &query.ops {
            if let OpType::Put(ScalarValue::Str(s)) = &q.action {
                buffer.push_str(s);
            } else {
                buffer.push('\u{fffc}');
            }
        }
        Ok(buffer)
    }

    /// Historical version of [`text`](Self::text).
    pub fn text_at<O: AsRef<ExId>>(
        &self,
        obj: O,
        heads: &[ChangeHash],
    ) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = self.clock_at(heads);
        let query = self.ops.search(&obj, query::ListValsAt::new(clock));
        let mut buffer = String::new();
        for q in &query.ops {
            if let OpType::Put(ScalarValue::Str(s)) = &q.action {
                buffer.push_str(s);
            } else {
                buffer.push('\u{fffc}');
            }
        }
        Ok(buffer)
    }

    // TODO - I need to return these OpId's here **only** to get
    // the legacy conflicts format of { [opid]: value }
    // Something better?
    /// Get a value out of the document.
    ///
    /// Returns both the value and the id of the operation that created it, useful for handling
    /// conflicts and serves as the object id if the value is an object.
    pub fn value<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        Ok(self.values(obj, prop.into())?.last().cloned())
    }

    /// Historical version of [`value`](Self::value).
    pub fn value_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        Ok(self.values_at(obj, prop, heads)?.last().cloned())
    }

    /// Get all values out of the document at this prop that conflict.
    ///
    /// Returns both the value and the id of the operation that created it, useful for handling
    /// conflicts and serves as the object id if the value is an object.
    pub fn values<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        let obj = self.exid_to_obj(obj.as_ref())?;
        let result = match prop.into() {
            Prop::Map(p) => {
                let prop = self.ops.m.props.lookup(&p);
                if let Some(p) = prop {
                    self.ops
                        .search(&obj, query::Prop::new(p))
                        .ops
                        .into_iter()
                        .map(|o| (o.value(), self.id_to_exid(o.id)))
                        .collect()
                } else {
                    vec![]
                }
            }
            Prop::Seq(n) => self
                .ops
                .search(&obj, query::Nth::new(n))
                .ops
                .into_iter()
                .map(|o| (o.value(), self.id_to_exid(o.id)))
                .collect(),
        };
        Ok(result)
    }

    /// Historical version of [`values`](Self::values).
    pub fn values_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        let prop = prop.into();
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = self.clock_at(heads);
        let result = match prop {
            Prop::Map(p) => {
                let prop = self.ops.m.props.lookup(&p);
                if let Some(p) = prop {
                    self.ops
                        .search(&obj, query::PropAt::new(p, clock))
                        .ops
                        .into_iter()
                        .map(|o| (o.clone_value(), self.id_to_exid(o.id)))
                        .collect()
                } else {
                    vec![]
                }
            }
            Prop::Seq(n) => self
                .ops
                .search(&obj, query::NthAt::new(n, clock))
                .ops
                .into_iter()
                .map(|o| (o.clone_value(), self.id_to_exid(o.id)))
                .collect(),
        };
        Ok(result)
    }

    /// Load a document.
    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        let changes = Change::load_document(data)?;
        let mut doc = Self::new();
        doc.apply_changes(changes)?;
        Ok(doc)
    }

    /// Load an incremental save of a document.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        let changes = Change::load_document(data)?;
        let start = self.ops.len();
        self.apply_changes(changes)?;
        let delta = self.ops.len() - start;
        Ok(delta)
    }

    fn duplicate_seq(&self, change: &Change) -> bool {
        let mut dup = false;
        if let Some(actor_index) = self.ops.m.actors.lookup(change.actor_id()) {
            if let Some(s) = self.states.get(&actor_index) {
                dup = s.len() >= change.seq as usize;
            }
        }
        dup
    }

    /// Apply changes to this document.
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<(), AutomergeError> {
        for c in changes {
            if !self.history_index.contains_key(&c.hash) {
                if self.duplicate_seq(&c) {
                    return Err(AutomergeError::DuplicateSeqNumber(
                        c.seq,
                        c.actor_id().clone(),
                    ));
                }
                if self.is_causally_ready(&c) {
                    self.apply_change(c);
                } else {
                    self.queue.push(c);
                }
            }
        }
        while let Some(c) = self.pop_next_causally_ready_change() {
            self.apply_change(c);
        }
        Ok(())
    }

    fn apply_change(&mut self, change: Change) {
        let ops = self.import_ops(&change);
        self.update_history(change, ops.len());
        if self.patches.is_some() {
            for (obj, op) in ops {
                self.insert_op_with_patch(&obj, op);
            }
        } else {
            for (obj, op) in ops {
                self.insert_op(&obj, op);
            }
        }
    }

    fn is_causally_ready(&self, change: &Change) -> bool {
        change
            .deps
            .iter()
            .all(|d| self.history_index.contains_key(d))
    }

    fn pop_next_causally_ready_change(&mut self) -> Option<Change> {
        let mut index = 0;
        while index < self.queue.len() {
            if self.is_causally_ready(&self.queue[index]) {
                return Some(self.queue.swap_remove(index));
            }
            index += 1;
        }
        None
    }

    fn import_ops(&mut self, change: &Change) -> Vec<(ObjId, Op)> {
        change
            .iter_ops()
            .enumerate()
            .map(|(i, c)| {
                let actor = self.ops.m.actors.cache(change.actor_id().clone());
                let id = OpId(change.start_op.get() + i as u64, actor);
                let obj = match c.obj {
                    legacy::ObjectId::Root => ObjId::root(),
                    legacy::ObjectId::Id(id) => ObjId(OpId(id.0, self.ops.m.actors.cache(id.1))),
                };
                let pred = c
                    .pred
                    .iter()
                    .map(|i| OpId(i.0, self.ops.m.actors.cache(i.1.clone())))
                    .collect();
                let key = match &c.key {
                    legacy::Key::Map(n) => Key::Map(self.ops.m.props.cache(n.to_string())),
                    legacy::Key::Seq(legacy::ElementId::Head) => Key::Seq(types::HEAD),
                    legacy::Key::Seq(legacy::ElementId::Id(i)) => {
                        Key::Seq(ElemId(OpId(i.0, self.ops.m.actors.cache(i.1.clone()))))
                    }
                };
                (
                    obj,
                    Op {
                        id,
                        action: c.action,
                        key,
                        succ: Default::default(),
                        pred,
                        insert: c.insert,
                    },
                )
            })
            .collect()
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge(&mut self, other: &mut Self) -> Result<Vec<ChangeHash>, AutomergeError> {
        // TODO: Make this fallible and figure out how to do this transactionally
        let changes = self
            .get_changes_added(other)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        self.apply_changes(changes)?;
        Ok(self.get_heads())
    }

    /// Save the entirety of this document in a compact form.
    pub fn save(&mut self) -> Vec<u8> {
        let heads = self.get_heads();
        let c = self.history.iter();
        let ops = self.ops.iter();
        let bytes = encode_document(heads, c, ops, &self.ops.m.actors, &self.ops.m.props.cache);
        self.saved = self.get_heads();
        bytes
    }

    /// Save the changes since last save in a compact form.
    pub fn save_incremental(&mut self) -> Vec<u8> {
        let changes = self.get_changes(self.saved.as_slice());
        let mut bytes = vec![];
        for c in changes {
            bytes.extend(c.raw_bytes());
        }
        if !bytes.is_empty() {
            self.saved = self.get_heads()
        }
        bytes
    }

    /// Filter the changes down to those that are not transitive dependencies of the heads.
    ///
    /// Thus a graph with these heads has not seen the remaining changes.
    pub(crate) fn filter_changes(&self, heads: &[ChangeHash], changes: &mut HashSet<ChangeHash>) {
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
                    queue.push_front(dep);
                } else {
                    queue.push_back(dep);
                }
            }
        }
    }

    /// Get the hashes of the changes in this document that aren't transitive dependencies of the
    /// given `heads`.
    pub fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
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

    fn get_changes_fast(&self, have_deps: &[ChangeHash]) -> Option<Vec<&Change>> {
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

    fn get_changes_slow(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
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

    /// Get the last change this actor made to the document.
    pub fn get_last_local_change(&self) -> Option<&Change> {
        return self
            .history
            .iter()
            .rev()
            .find(|c| c.actor_id() == self.get_actor());
    }

    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        if let Some(changes) = self.get_changes_fast(have_deps) {
            changes
        } else {
            self.get_changes_slow(have_deps)
        }
    }

    fn clock_at(&self, heads: &[ChangeHash]) -> Clock {
        let mut clock = Clock::new();
        let mut seen = HashSet::new();
        let mut to_see = heads.to_vec();
        // FIXME - faster
        while let Some(hash) = to_see.pop() {
            if let Some(c) = self.get_change_by_hash(&hash) {
                for h in &c.deps {
                    if !seen.contains(h) {
                        to_see.push(*h);
                    }
                }
                let actor = self.ops.m.actors.lookup(c.actor_id()).unwrap();
                clock.include(actor, c.max_op());
                seen.insert(hash);
            }
        }
        clock
    }

    /// Get a change by its hash.
    pub fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&Change> {
        self.history_index
            .get(hash)
            .and_then(|index| self.history.get(*index))
    }

    /// Get the changes that the other document added compared to this document.
    pub fn get_changes_added<'a>(&self, other: &'a Self) -> Vec<&'a Change> {
        // Depth-first traversal from the heads through the dependency graph,
        // until we reach a change that is already present in other
        let mut stack: Vec<_> = other.get_heads();
        let mut seen_hashes = HashSet::new();
        let mut added_change_hashes = Vec::new();
        while let Some(hash) = stack.pop() {
            if !seen_hashes.contains(&hash) && self.get_change_by_hash(&hash).is_none() {
                seen_hashes.insert(hash);
                added_change_hashes.push(hash);
                if let Some(change) = other.get_change_by_hash(&hash) {
                    stack.extend(&change.deps);
                }
            }
        }
        // Return those changes in the reverse of the order in which the depth-first search
        // found them. This is not necessarily a topological sort, but should usually be close.
        added_change_hashes.reverse();
        added_change_hashes
            .into_iter()
            .filter_map(|h| other.get_change_by_hash(&h))
            .collect()
    }

    /// Get the heads of this document.
    pub fn get_heads(&self) -> Vec<ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    fn get_hash(&self, actor: usize, seq: u64) -> Result<ChangeHash, AutomergeError> {
        self.states
            .get(&actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|&i| self.history.get(i))
            .map(|c| c.hash)
            .ok_or(AutomergeError::InvalidSeq(seq))
    }

    pub(crate) fn update_history(&mut self, change: Change, num_ops: usize) -> usize {
        self.max_op = std::cmp::max(self.max_op, change.start_op.get() + num_ops as u64 - 1);

        self.update_deps(&change);

        let history_index = self.history.len();

        self.states
            .entry(self.ops.m.actors.cache(change.actor_id().clone()))
            .or_default()
            .push(history_index);

        self.history_index.insert(change.hash, history_index);
        self.history.push(change);

        history_index
    }

    fn update_deps(&mut self, change: &Change) {
        for d in &change.deps {
            self.deps.remove(d);
        }
        self.deps.insert(change.hash);
    }

    pub fn import(&self, s: &str) -> Result<ExId, AutomergeError> {
        if s == "_root" {
            Ok(ExId::Root)
        } else {
            let n = s
                .find('@')
                .ok_or_else(|| AutomergeError::InvalidOpId(s.to_owned()))?;
            let counter = s[0..n]
                .parse()
                .map_err(|_| AutomergeError::InvalidOpId(s.to_owned()))?;
            let actor = ActorId::from(hex::decode(&s[(n + 1)..]).unwrap());
            let actor = self
                .ops
                .m
                .actors
                .lookup(&actor)
                .ok_or_else(|| AutomergeError::ForeignObjId(s.to_owned()))?;
            Ok(ExId::Id(
                counter,
                self.ops.m.actors.cache[actor].clone(),
                actor,
            ))
        }
    }

    pub(crate) fn to_string<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => format!("{}@{}", id.counter(), self.ops.m.actors[id.actor()]),
            Export::Prop(index) => self.ops.m.props[index].clone(),
            Export::Special(s) => s,
        }
    }

    pub fn dump(&self) {
        log!(
            "  {:12} {:12} {:12} {} {} {}",
            "id",
            "obj",
            "key",
            "value",
            "pred",
            "succ"
        );
        for (obj, op) in self.ops.iter() {
            let id = self.to_string(op.id);
            let obj = self.to_string(obj);
            let key = match op.key {
                Key::Map(n) => self.ops.m.props[n].clone(),
                Key::Seq(n) => self.to_string(n),
            };
            let value: String = match &op.action {
                OpType::Put(value) => format!("{}", value),
                OpType::Make(obj) => format!("make({})", obj),
                OpType::Increment(obj) => format!("inc({})", obj),
                OpType::Delete => format!("del{}", 0),
            };
            let pred: Vec<_> = op.pred.iter().map(|id| self.to_string(*id)).collect();
            let succ: Vec<_> = op.succ.iter().map(|id| self.to_string(*id)).collect();
            log!(
                "  {:12} {:12} {:12} {} {:?} {:?}",
                id,
                obj,
                key,
                value,
                pred,
                succ
            );
        }
    }

    #[cfg(feature = "optree-visualisation")]
    pub fn visualise_optree(&self) -> String {
        self.ops.visualise()
    }
}

impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct SpanInfo {
    pub id: ExId,
    pub time: i64,
    pub start: usize,
    pub end: usize,
    #[serde(rename = "type")]
    pub span_type: String,
    pub value: ScalarValue,
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::op_tree::B;
    use crate::transaction::Transactable;
    use crate::*;
    use std::convert::TryInto;

    #[test]
    fn insert_op() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::random());
        let mut tx = doc.transaction();
        tx.put(ROOT, "hello", "world")?;
        tx.value(ROOT, "hello")?;
        tx.commit();
        Ok(())
    }

    #[test]
    fn test_set() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        // setting a scalar value shouldn't return an opid as no object was created.
        tx.put(ROOT, "a", 1)?;

        // setting the same value shouldn't return an opid as there is no change.
        tx.put(ROOT, "a", 1)?;

        assert_eq!(tx.pending_ops(), 1);

        let map = tx.put_object(ROOT, "b", ObjType::Map)?;
        // object already exists at b but setting a map again overwrites it so we get an opid.
        tx.put(map, "a", 2)?;

        tx.put_object(ROOT, "b", ObjType::Map)?;

        assert_eq!(tx.pending_ops(), 4);
        let map = tx.value(ROOT, "b").unwrap().unwrap().1;
        assert_eq!(tx.value(&map, "a")?, None);

        tx.commit();
        Ok(())
    }

    #[test]
    fn test_list() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::random());
        let mut tx = doc.transaction();
        let list_id = tx.put_object(ROOT, "items", ObjType::List)?;
        tx.put(ROOT, "zzz", "zzzval")?;
        assert!(tx.value(ROOT, "items")?.unwrap().1 == list_id);
        tx.insert(&list_id, 0, "a")?;
        tx.insert(&list_id, 0, "b")?;
        tx.insert(&list_id, 2, "c")?;
        tx.insert(&list_id, 1, "d")?;
        assert!(tx.value(&list_id, 0)?.unwrap().0 == "b".into());
        assert!(tx.value(&list_id, 1)?.unwrap().0 == "d".into());
        assert!(tx.value(&list_id, 2)?.unwrap().0 == "a".into());
        assert!(tx.value(&list_id, 3)?.unwrap().0 == "c".into());
        assert!(tx.length(&list_id) == 4);
        tx.commit();
        doc.save();
        Ok(())
    }

    #[test]
    fn test_del() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::random());
        let mut tx = doc.transaction();
        tx.put(ROOT, "xxx", "xxx")?;
        assert!(!tx.values(ROOT, "xxx")?.is_empty());
        tx.delete(ROOT, "xxx")?;
        assert!(tx.values(ROOT, "xxx")?.is_empty());
        tx.commit();
        Ok(())
    }

    #[test]
    fn test_inc() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(ROOT, "counter", ScalarValue::counter(10))?;
        assert!(tx.value(ROOT, "counter")?.unwrap().0 == Value::counter(10));
        tx.increment(ROOT, "counter", 10)?;
        assert!(tx.value(ROOT, "counter")?.unwrap().0 == Value::counter(20));
        tx.increment(ROOT, "counter", -5)?;
        assert!(tx.value(ROOT, "counter")?.unwrap().0 == Value::counter(15));
        tx.commit();
        Ok(())
    }

    #[test]
    fn test_save_incremental() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();

        let mut tx = doc.transaction();
        tx.put(ROOT, "foo", 1)?;
        tx.commit();

        let save1 = doc.save();

        let mut tx = doc.transaction();
        tx.put(ROOT, "bar", 2)?;
        tx.commit();

        let save2 = doc.save_incremental();

        let mut tx = doc.transaction();
        tx.put(ROOT, "baz", 3)?;
        tx.commit();

        let save3 = doc.save_incremental();

        let mut save_a: Vec<u8> = vec![];
        save_a.extend(&save1);
        save_a.extend(&save2);
        save_a.extend(&save3);

        assert!(doc.save_incremental().is_empty());

        let save_b = doc.save();

        assert!(save_b.len() < save_a.len());

        let mut doc_a = Automerge::load(&save_a)?;
        let mut doc_b = Automerge::load(&save_b)?;

        assert!(doc_a.values(ROOT, "baz")? == doc_b.values(ROOT, "baz")?);

        assert!(doc_a.save() == doc_b.save());

        Ok(())
    }

    #[test]
    fn test_save_text() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", ObjType::Text)?;
        tx.commit();
        let heads1 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.splice_text(&text, 0, 0, "hello world")?;
        tx.commit();
        let heads2 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.splice_text(&text, 6, 0, "big bad ")?;
        tx.commit();
        let heads3 = doc.get_heads();

        assert!(&doc.text(&text)? == "hello big bad world");
        assert!(&doc.text_at(&text, &heads1)?.is_empty());
        assert!(&doc.text_at(&text, &heads2)? == "hello world");
        assert!(&doc.text_at(&text, &heads3)? == "hello big bad world");

        Ok(())
    }

    #[test]
    fn test_props_vals_at() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor("aaaa".try_into().unwrap());
        let mut tx = doc.transaction();
        tx.put(ROOT, "prop1", "val1")?;
        tx.commit();
        doc.get_heads();
        let heads1 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.put(ROOT, "prop1", "val2")?;
        tx.commit();
        doc.get_heads();
        let heads2 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.put(ROOT, "prop2", "val3")?;
        tx.commit();
        doc.get_heads();
        let heads3 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.delete(ROOT, "prop1")?;
        tx.commit();
        doc.get_heads();
        let heads4 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.put(ROOT, "prop3", "val4")?;
        tx.commit();
        doc.get_heads();
        let heads5 = doc.get_heads();
        assert!(doc.keys_at(ROOT, &heads1).collect_vec() == vec!["prop1".to_owned()]);
        assert_eq!(doc.length_at(ROOT, &heads1), 1);
        assert!(doc.value_at(ROOT, "prop1", &heads1)?.unwrap().0 == Value::str("val1"));
        assert!(doc.value_at(ROOT, "prop2", &heads1)? == None);
        assert!(doc.value_at(ROOT, "prop3", &heads1)? == None);

        assert!(doc.keys_at(ROOT, &heads2).collect_vec() == vec!["prop1".to_owned()]);
        assert_eq!(doc.length_at(ROOT, &heads2), 1);
        assert!(doc.value_at(ROOT, "prop1", &heads2)?.unwrap().0 == Value::str("val2"));
        assert!(doc.value_at(ROOT, "prop2", &heads2)? == None);
        assert!(doc.value_at(ROOT, "prop3", &heads2)? == None);

        assert!(
            doc.keys_at(ROOT, &heads3).collect_vec()
                == vec!["prop1".to_owned(), "prop2".to_owned()]
        );
        assert_eq!(doc.length_at(ROOT, &heads3), 2);
        assert!(doc.value_at(ROOT, "prop1", &heads3)?.unwrap().0 == Value::str("val2"));
        assert!(doc.value_at(ROOT, "prop2", &heads3)?.unwrap().0 == Value::str("val3"));
        assert!(doc.value_at(ROOT, "prop3", &heads3)? == None);

        assert!(doc.keys_at(ROOT, &heads4).collect_vec() == vec!["prop2".to_owned()]);
        assert_eq!(doc.length_at(ROOT, &heads4), 1);
        assert!(doc.value_at(ROOT, "prop1", &heads4)? == None);
        assert!(doc.value_at(ROOT, "prop2", &heads4)?.unwrap().0 == Value::str("val3"));
        assert!(doc.value_at(ROOT, "prop3", &heads4)? == None);

        assert!(
            doc.keys_at(ROOT, &heads5).collect_vec()
                == vec!["prop2".to_owned(), "prop3".to_owned()]
        );
        assert_eq!(doc.length_at(ROOT, &heads5), 2);
        assert_eq!(doc.length(ROOT), 2);
        assert!(doc.value_at(ROOT, "prop1", &heads5)? == None);
        assert!(doc.value_at(ROOT, "prop2", &heads5)?.unwrap().0 == Value::str("val3"));
        assert!(doc.value_at(ROOT, "prop3", &heads5)?.unwrap().0 == Value::str("val4"));

        assert_eq!(doc.keys_at(ROOT, &[]).count(), 0);
        assert_eq!(doc.length_at(ROOT, &[]), 0);
        assert!(doc.value_at(ROOT, "prop1", &[])? == None);
        assert!(doc.value_at(ROOT, "prop2", &[])? == None);
        assert!(doc.value_at(ROOT, "prop3", &[])? == None);
        Ok(())
    }

    #[test]
    fn test_len_at() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor("aaaa".try_into().unwrap());

        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", ObjType::List)?;
        tx.commit();
        let heads1 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.insert(&list, 0, 10)?;
        tx.commit();
        let heads2 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.put(&list, 0, 20)?;
        tx.insert(&list, 0, 30)?;
        tx.commit();
        let heads3 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.put(&list, 1, 40)?;
        tx.insert(&list, 1, 50)?;
        tx.commit();
        let heads4 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.delete(&list, 2)?;
        tx.commit();
        let heads5 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.delete(&list, 0)?;
        tx.commit();
        let heads6 = doc.get_heads();

        assert!(doc.length_at(&list, &heads1) == 0);
        assert!(doc.value_at(&list, 0, &heads1)?.is_none());

        assert!(doc.length_at(&list, &heads2) == 1);
        assert!(doc.value_at(&list, 0, &heads2)?.unwrap().0 == Value::int(10));

        assert!(doc.length_at(&list, &heads3) == 2);
        //doc.dump();
        log!("{:?}", doc.value_at(&list, 0, &heads3)?.unwrap().0);
        assert!(doc.value_at(&list, 0, &heads3)?.unwrap().0 == Value::int(30));
        assert!(doc.value_at(&list, 1, &heads3)?.unwrap().0 == Value::int(20));

        assert!(doc.length_at(&list, &heads4) == 3);
        assert!(doc.value_at(&list, 0, &heads4)?.unwrap().0 == Value::int(30));
        assert!(doc.value_at(&list, 1, &heads4)?.unwrap().0 == Value::int(50));
        assert!(doc.value_at(&list, 2, &heads4)?.unwrap().0 == Value::int(40));

        assert!(doc.length_at(&list, &heads5) == 2);
        assert!(doc.value_at(&list, 0, &heads5)?.unwrap().0 == Value::int(30));
        assert!(doc.value_at(&list, 1, &heads5)?.unwrap().0 == Value::int(50));

        assert!(doc.length_at(&list, &heads6) == 1);
        assert!(doc.length(&list) == 1);
        assert!(doc.value_at(&list, 0, &heads6)?.unwrap().0 == Value::int(50));

        Ok(())
    }

    #[test]
    fn keys_iter_map() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 3).unwrap();
        tx.put(ROOT, "b", 4).unwrap();
        tx.put(ROOT, "c", 5).unwrap();
        tx.put(ROOT, "d", 6).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 7).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 8).unwrap();
        tx.put(ROOT, "d", 9).unwrap();
        tx.commit();
        assert_eq!(doc.keys(ROOT).count(), 4);

        let mut keys = doc.keys(ROOT);
        assert_eq!(keys.next(), Some("a".into()));
        assert_eq!(keys.next(), Some("b".into()));
        assert_eq!(keys.next(), Some("c".into()));
        assert_eq!(keys.next(), Some("d".into()));
        assert_eq!(keys.next(), None);

        let mut keys = doc.keys(ROOT);
        assert_eq!(keys.next_back(), Some("d".into()));
        assert_eq!(keys.next_back(), Some("c".into()));
        assert_eq!(keys.next_back(), Some("b".into()));
        assert_eq!(keys.next_back(), Some("a".into()));
        assert_eq!(keys.next_back(), None);

        let mut keys = doc.keys(ROOT);
        assert_eq!(keys.next(), Some("a".into()));
        assert_eq!(keys.next_back(), Some("d".into()));
        assert_eq!(keys.next_back(), Some("c".into()));
        assert_eq!(keys.next_back(), Some("b".into()));
        assert_eq!(keys.next_back(), None);

        let mut keys = doc.keys(ROOT);
        assert_eq!(keys.next_back(), Some("d".into()));
        assert_eq!(keys.next(), Some("a".into()));
        assert_eq!(keys.next(), Some("b".into()));
        assert_eq!(keys.next(), Some("c".into()));
        assert_eq!(keys.next(), None);
        let keys = doc.keys(ROOT);
        assert_eq!(keys.collect::<Vec<_>>(), vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn keys_iter_seq() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let list = tx.set_object(ROOT, "list", ObjType::List).unwrap();
        tx.insert(&list, 0, 3).unwrap();
        tx.insert(&list, 1, 4).unwrap();
        tx.insert(&list, 2, 5).unwrap();
        tx.insert(&list, 3, 6).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.set(&list, 0, 7).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.set(&list, 0, 8).unwrap();
        tx.set(&list, 3, 9).unwrap();
        tx.commit();
        let actor = doc.get_actor();
        assert_eq!(doc.keys(&list).count(), 4);

        let mut keys = doc.keys(&list);
        assert_eq!(keys.next(), Some(format!("2@{}", actor)));
        assert_eq!(keys.next(), Some(format!("3@{}", actor)));
        assert_eq!(keys.next(), Some(format!("4@{}", actor)));
        assert_eq!(keys.next(), Some(format!("5@{}", actor)));
        assert_eq!(keys.next(), None);

        let mut keys = doc.keys(&list);
        assert_eq!(keys.next_back(), Some(format!("5@{}", actor)));
        assert_eq!(keys.next_back(), Some(format!("4@{}", actor)));
        assert_eq!(keys.next_back(), Some(format!("3@{}", actor)));
        assert_eq!(keys.next_back(), Some(format!("2@{}", actor)));
        assert_eq!(keys.next_back(), None);

        let mut keys = doc.keys(&list);
        assert_eq!(keys.next(), Some(format!("2@{}", actor)));
        assert_eq!(keys.next_back(), Some(format!("5@{}", actor)));
        assert_eq!(keys.next_back(), Some(format!("4@{}", actor)));
        assert_eq!(keys.next_back(), Some(format!("3@{}", actor)));
        assert_eq!(keys.next_back(), None);

        let mut keys = doc.keys(&list);
        assert_eq!(keys.next_back(), Some(format!("5@{}", actor)));
        assert_eq!(keys.next(), Some(format!("2@{}", actor)));
        assert_eq!(keys.next(), Some(format!("3@{}", actor)));
        assert_eq!(keys.next(), Some(format!("4@{}", actor)));
        assert_eq!(keys.next(), None);

        let keys = doc.keys(&list);
        assert_eq!(
            keys.collect::<Vec<_>>(),
            vec![
                format!("2@{}", actor),
                format!("3@{}", actor),
                format!("4@{}", actor),
                format!("5@{}", actor)
            ]
        );
    }

    #[test]
    fn range_iter_map() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.set(ROOT, "a", 3).unwrap();
        tx.set(ROOT, "b", 4).unwrap();
        tx.set(ROOT, "c", 5).unwrap();
        tx.set(ROOT, "d", 6).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.set(ROOT, "a", 7).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.set(ROOT, "a", 8).unwrap();
        tx.set(ROOT, "d", 9).unwrap();
        tx.commit();
        let actor = doc.get_actor();
        assert_eq!(doc.range(ROOT, ..).count(), 4);

        let mut range = doc.range(ROOT, Prop::Map("b".into()).."d".into());
        assert_eq!(
            range.next(),
            Some(("b".into(), 4.into(), ExId::Id(2, actor.clone(), 0)))
        );
        assert_eq!(
            range.next(),
            Some(("c".into(), 5.into(), ExId::Id(3, actor.clone(), 0)))
        );
        assert_eq!(range.next(), None);

        let mut range = doc.range(ROOT, Prop::Map("b".into())..="d".into());
        assert_eq!(
            range.next(),
            Some(("b".into(), 4.into(), ExId::Id(2, actor.clone(), 0)))
        );
        assert_eq!(
            range.next(),
            Some(("c".into(), 5.into(), ExId::Id(3, actor.clone(), 0)))
        );
        assert_eq!(
            range.next(),
            Some(("d".into(), 9.into(), ExId::Id(7, actor.clone(), 0)))
        );
        assert_eq!(range.next(), None);

        let mut range = doc.range(ROOT, ..=Prop::Map("c".into()));
        assert_eq!(
            range.next(),
            Some(("a".into(), 8.into(), ExId::Id(6, actor.clone(), 0)))
        );
        assert_eq!(
            range.next(),
            Some(("b".into(), 4.into(), ExId::Id(2, actor.clone(), 0)))
        );
        assert_eq!(
            range.next(),
            Some(("c".into(), 5.into(), ExId::Id(3, actor.clone(), 0)))
        );
        assert_eq!(range.next(), None);

        let range = doc.range(ROOT, Prop::Map("a".into())..);
        assert_eq!(
            range.collect::<Vec<_>>(),
            vec![
                ("a".into(), 8.into(), ExId::Id(6, actor.clone(), 0)),
                ("b".into(), 4.into(), ExId::Id(2, actor.clone(), 0)),
                ("c".into(), 5.into(), ExId::Id(3, actor.clone(), 0)),
                ("d".into(), 9.into(), ExId::Id(7, actor.clone(), 0)),
            ]
        );
    }

    #[test]
    fn range_iter_seq() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let list = tx.set_object(ROOT, "list", ObjType::List).unwrap();
        tx.insert(&list, 0, 3).unwrap();
        tx.insert(&list, 1, 4).unwrap();
        tx.insert(&list, 2, 5).unwrap();
        tx.insert(&list, 3, 6).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.set(&list, 0, 7).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.set(&list, 0, 8).unwrap();
        tx.set(&list, 3, 9).unwrap();
        tx.commit();
        let actor = doc.get_actor();
        assert_eq!(doc.range(&list, ..).count(), 4);

        let mut range = doc.range(&list, Prop::Seq(1)..3.into());
        assert_eq!(
            range.next(),
            Some((
                format!("3@{}", actor),
                4.into(),
                ExId::Id(3, actor.clone(), 0)
            ))
        );
        assert_eq!(
            range.next(),
            Some((
                format!("4@{}", actor),
                5.into(),
                ExId::Id(4, actor.clone(), 0)
            ))
        );
        assert_eq!(range.next(), None);

        let mut range = doc.range(&list, Prop::Seq(1)..=3.into());
        assert_eq!(
            range.next(),
            Some((
                format!("3@{}", actor),
                4.into(),
                ExId::Id(3, actor.clone(), 0)
            ))
        );
        assert_eq!(
            range.next(),
            Some((
                format!("4@{}", actor),
                5.into(),
                ExId::Id(4, actor.clone(), 0)
            ))
        );
        assert_eq!(
            range.next(),
            Some((
                format!("5@{}", actor),
                9.into(),
                ExId::Id(8, actor.clone(), 0)
            ))
        );
        assert_eq!(range.next(), None);

        let mut range = doc.range(&list, ..Prop::Seq(3));
        assert_eq!(
            range.next(),
            Some((
                format!("2@{}", actor),
                8.into(),
                ExId::Id(7, actor.clone(), 0)
            ))
        );
        assert_eq!(
            range.next(),
            Some((
                format!("3@{}", actor),
                4.into(),
                ExId::Id(3, actor.clone(), 0)
            ))
        );
        assert_eq!(
            range.next(),
            Some((
                format!("4@{}", actor),
                5.into(),
                ExId::Id(4, actor.clone(), 0)
            ))
        );
        assert_eq!(range.next(), None);

        let range = doc.range(&list, ..);
        assert_eq!(
            range.collect::<Vec<_>>(),
            vec![
                (
                    format!("2@{}", actor),
                    8.into(),
                    ExId::Id(7, actor.clone(), 0)
                ),
                (
                    format!("3@{}", actor),
                    4.into(),
                    ExId::Id(3, actor.clone(), 0)
                ),
                (
                    format!("4@{}", actor),
                    5.into(),
                    ExId::Id(4, actor.clone(), 0)
                ),
                (
                    format!("5@{}", actor),
                    9.into(),
                    ExId::Id(8, actor.clone(), 0)
                ),
            ]
        );
    }

    #[test]
    fn rolling_back_transaction_has_no_effect() {
        let mut doc = Automerge::new();
        let old_states = doc.states.clone();
        let bytes = doc.save();
        let tx = doc.transaction();
        tx.rollback();
        let new_states = doc.states.clone();
        assert_eq!(old_states, new_states);
        let new_bytes = doc.save();
        assert_eq!(bytes, new_bytes);
    }

    #[test]
    fn mutate_old_objects() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        // create a map
        let map1 = tx.put_object(ROOT, "a", ObjType::Map).unwrap();
        tx.put(&map1, "b", 1).unwrap();
        // overwrite the first map with a new one
        let map2 = tx.put_object(ROOT, "a", ObjType::Map).unwrap();
        tx.put(&map2, "c", 2).unwrap();
        tx.commit();

        // we can get the new map by traversing the tree
        let map = doc.value(&ROOT, "a").unwrap().unwrap().1;
        assert_eq!(doc.value(&map, "b").unwrap(), None);
        // and get values from it
        assert_eq!(
            doc.value(&map, "c").unwrap().map(|s| s.0),
            Some(ScalarValue::Int(2).into())
        );

        // but we can still access the old one if we know the ID!
        assert_eq!(doc.value(&map1, "b").unwrap().unwrap().0, Value::int(1));
        // and even set new things in it!
        let mut tx = doc.transaction();
        tx.put(&map1, "c", 3).unwrap();
        tx.commit();

        assert_eq!(doc.value(&map1, "c").unwrap().unwrap().0, Value::int(3));
    }

    #[test]
    fn delete_nothing_in_map_is_noop() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        // deleting a missing key in a map should just be a noop
        assert!(tx.delete(ROOT, "a").is_ok());
        tx.commit();
        let last_change = doc.get_last_local_change().unwrap();
        assert_eq!(last_change.len(), 0);

        let bytes = doc.save();
        assert!(Automerge::load(&bytes).is_ok());

        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.commit();
        let last_change = doc.get_last_local_change().unwrap();
        assert_eq!(last_change.len(), 1);

        let mut tx = doc.transaction();
        // a real op
        tx.delete(ROOT, "a").unwrap();
        // a no-op
        tx.delete(ROOT, "a").unwrap();
        tx.commit();
        let last_change = doc.get_last_local_change().unwrap();
        assert_eq!(last_change.len(), 1);
    }

    #[test]
    fn delete_nothing_in_list_returns_error() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        // deleting an element in a list that does not exist is an error
        assert!(tx.delete(ROOT, 0).is_err());
    }

    #[test]
    fn loaded_doc_changes_have_hash() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(ROOT, "a", 1).unwrap();
        tx.commit();
        let hash = doc.get_last_local_change().unwrap().hash;
        let bytes = doc.save();
        let doc = Automerge::load(&bytes).unwrap();
        assert_eq!(doc.get_change_by_hash(&hash).unwrap().hash, hash);
    }

    #[test]
    fn load_change_with_zero_start_op() {
        let bytes = &[
            133, 111, 74, 131, 202, 50, 52, 158, 2, 96, 163, 163, 83, 255, 255, 255, 50, 50, 50,
            50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 255, 255, 245, 53, 1, 0, 0, 0, 0, 0, 0, 4,
            233, 245, 239, 255, 1, 0, 0, 0, 133, 111, 74, 131, 163, 96, 0, 0, 2, 10, 202, 144, 125,
            19, 48, 89, 133, 49, 10, 10, 67, 91, 111, 10, 74, 131, 96, 0, 163, 131, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 1, 153, 0, 0, 246, 255, 255, 255, 157, 157, 157, 157,
            157, 157, 157, 157, 157, 157, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 48, 254, 208,
        ];
        let _ = Automerge::load(bytes);
    }

    #[test]
    fn load_broken_list() {
        enum Action {
            InsertText(usize, char),
            DelText(usize),
        }
        use Action::*;
        let actions = [
            InsertText(0, 'a'),
            InsertText(0, 'b'),
            DelText(1),
            InsertText(0, 'c'),
            DelText(1),
            DelText(0),
            InsertText(0, 'd'),
            InsertText(0, 'e'),
            InsertText(1, 'f'),
            DelText(2),
            DelText(1),
            InsertText(0, 'g'),
            DelText(1),
            DelText(0),
            InsertText(0, 'h'),
            InsertText(1, 'i'),
            DelText(1),
            DelText(0),
            InsertText(0, 'j'),
            InsertText(0, 'k'),
            DelText(1),
            DelText(0),
            InsertText(0, 'l'),
            DelText(0),
            InsertText(0, 'm'),
            InsertText(0, 'n'),
            DelText(1),
            DelText(0),
            InsertText(0, 'o'),
            DelText(0),
            InsertText(0, 'p'),
            InsertText(1, 'q'),
            InsertText(1, 'r'),
            InsertText(1, 's'),
            InsertText(3, 't'),
            InsertText(5, 'u'),
            InsertText(0, 'v'),
            InsertText(3, 'w'),
            InsertText(4, 'x'),
            InsertText(0, 'y'),
            InsertText(6, 'z'),
            InsertText(11, '1'),
            InsertText(0, '2'),
            InsertText(0, '3'),
            InsertText(0, '4'),
            InsertText(13, '5'),
            InsertText(11, '6'),
            InsertText(17, '7'),
        ];
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
        for action in actions {
            match action {
                Action::InsertText(index, c) => {
                    println!("inserting {} at {}", c, index);
                    tx.insert(&list, index, c).unwrap();
                }
                Action::DelText(index) => {
                    println!("deleting at {} ", index);
                    tx.delete(&list, index).unwrap();
                }
            }
        }
        tx.commit();
        let bytes = doc.save();
        println!("doc2 time");
        let mut doc2 = Automerge::load(&bytes).unwrap();
        let bytes2 = doc2.save();
        assert_eq!(doc.text(&list).unwrap(), doc2.text(&list).unwrap());

        assert_eq!(doc.queue, doc2.queue);
        assert_eq!(doc.history, doc2.history);
        assert_eq!(doc.history_index, doc2.history_index);
        assert_eq!(doc.states, doc2.states);
        assert_eq!(doc.deps, doc2.deps);
        assert_eq!(doc.saved, doc2.saved);
        assert_eq!(doc.ops, doc2.ops);
        assert_eq!(doc.max_op, doc2.max_op);

        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn load_broken_list_short() {
        // breaks when the B constant in OpSet is 3
        enum Action {
            InsertText(usize, char),
            DelText(usize),
        }
        use Action::*;
        let actions = [
            InsertText(0, 'a'),
            InsertText(1, 'b'),
            DelText(1),
            InsertText(1, 'c'),
            InsertText(2, 'd'),
            InsertText(2, 'e'),
            InsertText(0, 'f'),
            DelText(4),
            InsertText(4, 'g'),
        ];
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
        for action in actions {
            match action {
                Action::InsertText(index, c) => {
                    println!("inserting {} at {}", c, index);
                    tx.insert(&list, index, c).unwrap();
                }
                Action::DelText(index) => {
                    println!("deleting at {} ", index);
                    tx.delete(&list, index).unwrap();
                }
            }
        }
        tx.commit();
        let bytes = doc.save();
        println!("doc2 time");
        let mut doc2 = Automerge::load(&bytes).unwrap();
        let bytes2 = doc2.save();
        assert_eq!(doc.text(&list).unwrap(), doc2.text(&list).unwrap());

        assert_eq!(doc.queue, doc2.queue);
        assert_eq!(doc.history, doc2.history);
        assert_eq!(doc.history_index, doc2.history_index);
        assert_eq!(doc.states, doc2.states);
        assert_eq!(doc.deps, doc2.deps);
        assert_eq!(doc.saved, doc2.saved);
        assert_eq!(doc.ops, doc2.ops);
        assert_eq!(doc.max_op, doc2.max_op);

        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn compute_list_indexes_correctly_when_list_element_is_split_across_tree_nodes() {
        let max = B as u64 * 2;
        let actor1 = ActorId::from(b"aaaa");
        let mut doc1 = AutoCommit::new().with_actor(actor1.clone());
        let actor2 = ActorId::from(b"bbbb");
        let mut doc2 = AutoCommit::new().with_actor(actor2.clone());
        let list = doc1.put_object(ROOT, "list", ObjType::List).unwrap();
        doc1.insert(&list, 0, 0).unwrap();
        doc2.load_incremental(&doc1.save_incremental()).unwrap();
        for i in 1..=max {
            doc1.put(&list, 0, i).unwrap()
        }
        for i in 1..=max {
            doc2.put(&list, 0, i).unwrap()
        }
        let change1 = doc1.save_incremental();
        let change2 = doc2.save_incremental();
        doc2.load_incremental(&change1).unwrap();
        doc1.load_incremental(&change2).unwrap();
        assert_eq!(doc1.length(&list), 1);
        assert_eq!(doc2.length(&list), 1);
        assert_eq!(
            doc1.values(&list, 0).unwrap(),
            vec![
                (max.into(), ExId::Id(max + 2, actor1.clone(), 0)),
                (max.into(), ExId::Id(max + 2, actor2.clone(), 1))
            ]
        );
        assert_eq!(
            doc2.values(&list, 0).unwrap(),
            vec![
                (max.into(), ExId::Id(max + 2, actor1, 0)),
                (max.into(), ExId::Id(max + 2, actor2, 1))
            ]
        );
        assert!(doc1.value(&list, 1).unwrap().is_none());
        assert!(doc2.value(&list, 1).unwrap().is_none());
    }

    #[test]
    fn get_parent_objects() {
        let mut doc = AutoCommit::new();
        let map = doc.put_object(ROOT, "a", ObjType::Map).unwrap();
        let list = doc.insert_object(&map, 0, ObjType::List).unwrap();
        doc.insert(&list, 0, 2).unwrap();
        let text = doc.put_object(&list, 0, ObjType::Text).unwrap();

        assert_eq!(doc.parent_object(&map), Some((ROOT, Prop::Map("a".into()))));
        assert_eq!(doc.parent_object(&list), Some((map, Prop::Seq(0))));
        assert_eq!(doc.parent_object(&text), Some((list, Prop::Seq(0))));
    }

    #[test]
    fn get_path_to_object() {
        let mut doc = AutoCommit::new();
        let map = doc.put_object(ROOT, "a", ObjType::Map).unwrap();
        let list = doc.insert_object(&map, 0, ObjType::List).unwrap();
        doc.insert(&list, 0, 2).unwrap();
        let text = doc.put_object(&list, 0, ObjType::Text).unwrap();

        assert_eq!(
            doc.path_to_object(&map),
            vec![(ROOT, Prop::Map("a".into()))]
        );
        assert_eq!(
            doc.path_to_object(&list),
            vec![(ROOT, Prop::Map("a".into())), (map.clone(), Prop::Seq(0)),]
        );
        assert_eq!(
            doc.path_to_object(&text),
            vec![
                (ROOT, Prop::Map("a".into())),
                (map, Prop::Seq(0)),
                (list, Prop::Seq(0)),
            ]
        );
    }

    #[test]
    fn can_insert_a_grapheme_into_text() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
        let polar_bear = "🐻‍❄️";
        tx.insert(&text, 0, polar_bear).unwrap();
        tx.commit();
        let s = doc.text(&text).unwrap();
        assert_eq!(s, polar_bear);
        let len = doc.length(&text);
        assert_eq!(len, 1); // just one grapheme
    }

    #[test]
    fn can_insert_long_string_into_text() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
        let polar_bear = "🐻‍❄️";
        let polar_bear_army = polar_bear.repeat(100);
        tx.insert(&text, 0, &polar_bear_army).unwrap();
        tx.commit();
        let s = doc.text(&text).unwrap();
        assert_eq!(s, polar_bear_army);
        let len = doc.length(&text);
        assert_eq!(len, 1); // many graphemes
    }

    #[test]
    fn splice_text_uses_unicode_scalars() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
        let polar_bear = "🐻‍❄️";
        tx.splice_text(&text, 0, 0, polar_bear).unwrap();
        tx.commit();
        let s = doc.text(&text).unwrap();
        assert_eq!(s, polar_bear);
        let len = doc.length(&text);
        assert_eq!(len, 4); // 4 chars
    }
}

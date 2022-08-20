use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::ops::RangeBounds;

use crate::change::encode_document;
use crate::clock::ClockData;
use crate::exid::ExId;
use crate::keys::Keys;
use crate::op_observer::OpObserver;
use crate::op_set::OpSet;
use crate::parents::Parents;
use crate::transaction::{self, CommitOptions, Failure, Success, Transaction, TransactionInner};
use crate::types::{
    ActorId, ChangeHash, Clock, ElemId, Export, Exportable, Key, ObjId, Op, OpId, OpType,
    ScalarValue, Value,
};
use crate::KeysAt;
use crate::{
    legacy, query, types, ApplyOptions, ListRange, ListRangeAt, MapRange, MapRangeAt, ObjType,
    Values,
};
use crate::{AutomergeError, Change, Prop};
use serde::Serialize;

#[cfg(test)]
mod tests;

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
    /// Mapping from change hash to vector clock at this state.
    pub(crate) clocks: HashMap<ChangeHash, Clock>,
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
}

impl Automerge {
    /// Create a new document with a random actor id.
    pub fn new() -> Self {
        Automerge {
            queue: vec![],
            history: vec![],
            history_index: HashMap::new(),
            clocks: HashMap::new(),
            states: HashMap::new(),
            ops: Default::default(),
            deps: Default::default(),
            saved: Default::default(),
            actor: Actor::Unused(ActorId::random()),
            max_op: 0,
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

    /// Start a transaction.
    pub fn transaction(&mut self) -> Transaction<'_> {
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
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
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
    pub fn transact_with<'a, F, O, E, C, Obs>(&mut self, c: C, f: F) -> transaction::Result<O, E>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<O, E>,
        C: FnOnce(&O) -> CommitOptions<'a, Obs>,
        Obs: 'a + OpObserver,
    {
        let mut tx = self.transaction();
        let result = f(&mut tx);
        match result {
            Ok(result) => {
                let commit_options = c(&result);
                let hash = tx.commit_with(commit_options);
                Ok(Success { result, hash })
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

    /// Fork this document at the give heads
    pub fn fork_at(&self, heads: &[ChangeHash]) -> Result<Self, AutomergeError> {
        let mut seen = heads.iter().cloned().collect::<HashSet<_>>();
        let mut heads = heads.to_vec();
        let mut changes = vec![];
        while let Some(hash) = heads.pop() {
            if let Some(idx) = self.history_index.get(&hash) {
                let change = &self.history[*idx];
                for dep in change.deps() {
                    if !seen.contains(dep) {
                        heads.push(*dep);
                    }
                }
                changes.push(change);
                seen.insert(hash);
            } else {
                return Err(AutomergeError::InvalidHash(hash));
            }
        }
        let mut f = Self::new();
        f.set_actor(ActorId::random());
        f.apply_changes(changes.into_iter().rev().cloned())?;
        Ok(f)
    }

    // KeysAt::()
    // LenAt::()
    // PropAt::()
    // NthAt::()

    /// Get the object id of the object that contains this object and the prop that this object is
    /// at in that object.
    pub(crate) fn parent_object(&self, obj: ObjId) -> Option<(ObjId, Key)> {
        if obj == ObjId::root() {
            // root has no parent
            None
        } else {
            self.ops.parent_object(&obj)
        }
    }

    /// Get the parents of an object in the document tree.
    ///
    /// ### Errors
    ///
    /// Returns an error when the id given is not the id of an object in this document.
    /// This function does not get the parents of scalar values contained within objects.
    ///
    /// ### Experimental
    ///
    /// This function may in future be changed to allow getting the parents from the id of a scalar
    /// value.
    pub fn parents<O: AsRef<ExId>>(&self, obj: O) -> Result<Parents<'_>, AutomergeError> {
        let obj_id = self.exid_to_obj(obj.as_ref())?;
        Ok(Parents {
            obj: obj_id,
            doc: self,
        })
    }

    pub fn path_to_object<O: AsRef<ExId>>(
        &self,
        obj: O,
    ) -> Result<Vec<(ExId, Prop)>, AutomergeError> {
        let mut path = self.parents(obj.as_ref().clone())?.collect::<Vec<_>>();
        path.reverse();
        Ok(path)
    }

    /// Export a key to a prop.
    pub(crate) fn export_key(&self, obj: ObjId, key: Key) -> Prop {
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
    pub fn keys<O: AsRef<ExId>>(&self, obj: O) -> Keys<'_, '_> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            let iter_keys = self.ops.keys(obj);
            Keys::new(self, iter_keys)
        } else {
            Keys::new(self, None)
        }
    }

    /// Historical version of [`keys`](Self::keys).
    pub fn keys_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> KeysAt<'_, '_> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            if let Ok(clock) = self.clock_at(heads) {
                return KeysAt::new(self, self.ops.keys_at(obj, clock));
            }
        }
        KeysAt::new(self, None)
    }

    /// Iterate over the keys and values of the map `obj` in the given range.
    pub fn map_range<O: AsRef<ExId>, R: RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
    ) -> MapRange<'_, R> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            MapRange::new(self, self.ops.map_range(obj, range))
        } else {
            MapRange::new(self, None)
        }
    }

    /// Historical version of [`map_range`](Self::map_range).
    pub fn map_range_at<O: AsRef<ExId>, R: RangeBounds<String>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> MapRangeAt<'_, R> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            if let Ok(clock) = self.clock_at(heads) {
                let iter_range = self.ops.map_range_at(obj, range, clock);
                return MapRangeAt::new(self, iter_range);
            }
        }
        MapRangeAt::new(self, None)
    }

    /// Iterate over the indexes and values of the list `obj` in the given range.
    pub fn list_range<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
    ) -> ListRange<'_, R> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            ListRange::new(self, self.ops.list_range(obj, range))
        } else {
            ListRange::new(self, None)
        }
    }

    /// Historical version of [`list_range`](Self::list_range).
    pub fn list_range_at<O: AsRef<ExId>, R: RangeBounds<usize>>(
        &self,
        obj: O,
        range: R,
        heads: &[ChangeHash],
    ) -> ListRangeAt<'_, R> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            if let Ok(clock) = self.clock_at(heads) {
                let iter_range = self.ops.list_range_at(obj, range, clock);
                return ListRangeAt::new(self, iter_range);
            }
        }
        ListRangeAt::new(self, None)
    }

    pub fn values<O: AsRef<ExId>>(&self, obj: O) -> Values<'_> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            match self.ops.object_type(&obj) {
                Some(t) if t.is_sequence() => Values::new(self, self.ops.list_range(obj, ..)),
                Some(_) => Values::new(self, self.ops.map_range(obj, ..)),
                None => Values::empty(self),
            }
        } else {
            Values::empty(self)
        }
    }

    pub fn values_at<O: AsRef<ExId>>(&self, obj: O, heads: &[ChangeHash]) -> Values<'_> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            if let Ok(clock) = self.clock_at(heads) {
                return match self.ops.object_type(&obj) {
                    Some(ObjType::Map) | Some(ObjType::Table) => {
                        let iter_range = self.ops.map_range_at(obj, .., clock);
                        Values::new(self, iter_range)
                    }
                    Some(ObjType::List) | Some(ObjType::Text) => {
                        let iter_range = self.ops.list_range_at(obj, .., clock);
                        Values::new(self, iter_range)
                    }
                    None => Values::empty(self),
                };
            }
        }
        Values::empty(self)
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
            if let Ok(clock) = self.clock_at(heads) {
                return match self.ops.object_type(&inner_obj) {
                    Some(ObjType::Map) | Some(ObjType::Table) => self.keys_at(obj, heads).count(),
                    Some(ObjType::List) | Some(ObjType::Text) => {
                        self.ops.search(&inner_obj, query::LenAt::new(clock)).len
                    }
                    None => 0,
                };
            }
        }
        0
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
                let obj = if self.ops.m.actors.cache.get(*idx) == Some(actor) {
                    ObjId(OpId(*ctr, *idx))
                } else {
                    // FIXME - make a real error
                    let idx = self
                        .ops
                        .m
                        .actors
                        .lookup(actor)
                        .ok_or(AutomergeError::Fail)?;
                    ObjId(OpId(*ctr, idx))
                };
                if self.ops.object_type(&obj).is_some() {
                    Ok(obj)
                } else {
                    Err(AutomergeError::NotAnObject)
                }
            }
        }
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        self.ops.id_to_exid(id)
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
        let clock = self.clock_at(heads)?;
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
    pub fn get<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        Ok(self.get_all(obj, prop.into())?.last().cloned())
    }

    /// Historical version of [`get`](Self::get).
    pub fn get_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value<'_>, ExId)>, AutomergeError> {
        Ok(self.get_all_at(obj, prop, heads)?.last().cloned())
    }

    /// Get all conflicting values out of the document at this prop that conflict.
    ///
    /// Returns both the value and the id of the operation that created it, useful for handling
    /// conflicts and serves as the object id if the value is an object.
    pub fn get_all<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
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

    /// Historical version of [`get_all`](Self::get_all).
    pub fn get_all_at<O: AsRef<ExId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value<'_>, ExId)>, AutomergeError> {
        let prop = prop.into();
        let obj = self.exid_to_obj(obj.as_ref())?;
        let clock = self.clock_at(heads)?;
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
        Self::load_with::<()>(data, ApplyOptions::default())
    }

    /// Load a document.
    pub fn load_with<Obs: OpObserver>(
        data: &[u8],
        options: ApplyOptions<'_, Obs>,
    ) -> Result<Self, AutomergeError> {
        let changes = Change::load_document(data)?;
        let mut doc = Self::new();
        doc.apply_changes_with(changes, options)?;
        Ok(doc)
    }

    /// Load an incremental save of a document.
    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        self.load_incremental_with::<()>(data, ApplyOptions::default())
    }

    /// Load an incremental save of a document.
    pub fn load_incremental_with<Obs: OpObserver>(
        &mut self,
        data: &[u8],
        options: ApplyOptions<'_, Obs>,
    ) -> Result<usize, AutomergeError> {
        let changes = Change::load_document(data)?;
        let start = self.ops.len();
        self.apply_changes_with(changes, options)?;
        let delta = self.ops.len() - start;
        Ok(delta)
    }

    fn duplicate_seq(&self, change: &Change) -> bool {
        let mut dup = false;
        if let Some(actor_index) = self.ops.m.actors.lookup(change.actor_id()) {
            if let Some(s) = self.states.get(&actor_index) {
                dup = s.len() >= change.seq() as usize;
            }
        }
        dup
    }

    /// Apply changes to this document.
    pub fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_with::<_, ()>(changes, ApplyOptions::default())
    }

    /// Apply changes to this document.
    pub fn apply_changes_with<I: IntoIterator<Item = Change>, Obs: OpObserver>(
        &mut self,
        changes: I,
        mut options: ApplyOptions<'_, Obs>,
    ) -> Result<(), AutomergeError> {
        for c in changes {
            if !self.history_index.contains_key(&c.hash()) {
                if self.duplicate_seq(&c) {
                    return Err(AutomergeError::DuplicateSeqNumber(
                        c.seq(),
                        c.actor_id().clone(),
                    ));
                }
                if self.is_causally_ready(&c) {
                    self.apply_change(c, &mut options.op_observer);
                } else {
                    self.queue.push(c);
                }
            }
        }
        while let Some(c) = self.pop_next_causally_ready_change() {
            if !self.history_index.contains_key(&c.hash()) {
                self.apply_change(c, &mut options.op_observer);
            }
        }
        Ok(())
    }

    fn apply_change<Obs: OpObserver>(&mut self, change: Change, observer: &mut Option<&mut Obs>) {
        let ops = self.import_ops(&change);
        self.update_history(change, ops.len());
        if let Some(observer) = observer {
            for (obj, op) in ops {
                self.ops.insert_op_with_observer(&obj, op, *observer);
            }
        } else {
            for (obj, op) in ops {
                self.ops.insert_op(&obj, op);
            }
        }
    }

    fn is_causally_ready(&self, change: &Change) -> bool {
        change
            .deps()
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
                let pred = self.ops.m.import_opids(c.pred);
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
        self.merge_with::<()>(other, ApplyOptions::default())
    }

    /// Takes all the changes in `other` which are not in `self` and applies them
    pub fn merge_with<'a, Obs: OpObserver>(
        &mut self,
        other: &mut Self,
        options: ApplyOptions<'a, Obs>,
    ) -> Result<Vec<ChangeHash>, AutomergeError> {
        // TODO: Make this fallible and figure out how to do this transactionally
        let changes = self
            .get_changes_added(other)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        tracing::trace!(changes=?changes.iter().map(|c| c.hash()).collect::<Vec<_>>(), "merging new changes");
        self.apply_changes_with(changes, options)?;
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
        let changes = self
            .get_changes(self.saved.as_slice())
            .expect("Should only be getting changes using previously saved heads");
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
    pub(crate) fn filter_changes(
        &self,
        heads: &[ChangeHash],
        changes: &mut BTreeSet<ChangeHash>,
    ) -> Result<(), AutomergeError> {
        let heads = heads
            .iter()
            .filter(|hash| self.history_index.contains_key(hash))
            .copied()
            .collect::<Vec<_>>();
        let heads_clock = self.clock_at(&heads)?;

        // keep the hashes that are concurrent or after the heads
        changes.retain(|hash| {
            self.clocks
                .get(hash)
                .unwrap()
                .partial_cmp(&heads_clock)
                .map_or(true, |o| o == Ordering::Greater)
        });

        Ok(())
    }

    /// Get the hashes of the changes in this document that aren't transitive dependencies of the
    /// given `heads`.
    pub fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<ChangeHash> {
        let in_queue: HashSet<_> = self.queue.iter().map(|change| change.hash()).collect();
        let mut missing = HashSet::new();

        for head in self.queue.iter().flat_map(|change| change.deps()) {
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

    /// Get the changes since `have_deps` in this document using a clock internally.
    fn get_changes_clock(&self, have_deps: &[ChangeHash]) -> Result<Vec<&Change>, AutomergeError> {
        // get the clock for the given deps
        let clock = self.clock_at(have_deps)?;

        // get the documents current clock

        let mut change_indexes: Vec<usize> = Vec::new();
        // walk the state from the given deps clock and add them into the vec
        for (actor_index, actor_changes) in &self.states {
            if let Some(clock_data) = clock.get_for_actor(actor_index) {
                // find the change in this actors sequence of changes that corresponds to the max_op
                // recorded for them in the clock
                change_indexes.extend(&actor_changes[clock_data.seq as usize..]);
            } else {
                change_indexes.extend(&actor_changes[..]);
            }
        }

        // ensure the changes are still in sorted order
        change_indexes.sort_unstable();

        Ok(change_indexes
            .into_iter()
            .map(|i| &self.history[i])
            .collect())
    }

    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Result<Vec<&Change>, AutomergeError> {
        self.get_changes_clock(have_deps)
    }

    /// Get the last change this actor made to the document.
    pub fn get_last_local_change(&self) -> Option<&Change> {
        return self
            .history
            .iter()
            .rev()
            .find(|c| c.actor_id() == self.get_actor());
    }

    fn clock_at(&self, heads: &[ChangeHash]) -> Result<Clock, AutomergeError> {
        if let Some(first_hash) = heads.first() {
            let mut clock = self
                .clocks
                .get(first_hash)
                .ok_or(AutomergeError::MissingHash(*first_hash))?
                .clone();

            for hash in &heads[1..] {
                let c = self
                    .clocks
                    .get(hash)
                    .ok_or(AutomergeError::MissingHash(*hash))?;
                clock.merge(c);
            }

            Ok(clock)
        } else {
            Ok(Clock::new())
        }
    }

    /// Get a change by its hash.
    pub fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&Change> {
        self.history_index
            .get(hash)
            .and_then(|index| self.history.get(*index))
    }

    /// Get the changes that the other document added compared to this document.
    #[tracing::instrument(skip(self, other))]
    pub fn get_changes_added<'a>(&self, other: &'a Self) -> Vec<&'a Change> {
        // Depth-first traversal from the heads through the dependency graph,
        // until we reach a change that is already present in other
        let mut stack: Vec<_> = other.get_heads();
        tracing::trace!(their_heads=?stack, "finding changes to merge");
        let mut seen_hashes = HashSet::new();
        let mut added_change_hashes = Vec::new();
        while let Some(hash) = stack.pop() {
            if !seen_hashes.contains(&hash) && self.get_change_by_hash(&hash).is_none() {
                seen_hashes.insert(hash);
                added_change_hashes.push(hash);
                if let Some(change) = other.get_change_by_hash(&hash) {
                    stack.extend(change.deps());
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
            .map(|c| c.hash())
            .ok_or(AutomergeError::InvalidSeq(seq))
    }

    pub(crate) fn update_history(&mut self, change: Change, num_ops: usize) -> usize {
        self.max_op = std::cmp::max(self.max_op, change.start_op().get() + num_ops as u64 - 1);

        self.update_deps(&change);

        let history_index = self.history.len();

        let actor_index = self.ops.m.actors.cache(change.actor_id().clone());
        self.states
            .entry(actor_index)
            .or_default()
            .push(history_index);

        let mut clock = Clock::new();
        for hash in change.deps() {
            let c = self
                .clocks
                .get(hash)
                .expect("Change's deps should already be in the document");
            clock.merge(c);
        }
        clock.include(
            actor_index,
            ClockData {
                max_op: change.max_op(),
                seq: change.seq(),
            },
        );
        self.clocks.insert(change.hash(), clock);

        self.history_index.insert(change.hash(), history_index);
        self.history.push(change);

        history_index
    }

    fn update_deps(&mut self, change: &Change) {
        for d in change.deps() {
            self.deps.remove(d);
        }
        self.deps.insert(change.hash());
    }

    pub fn import(&self, s: &str) -> Result<ExId, AutomergeError> {
        if s == "_root" {
            Ok(ExId::Root)
        } else {
            let n = s
                .find('@')
                .ok_or_else(|| AutomergeError::InvalidObjIdFormat(s.to_owned()))?;
            let counter = s[0..n]
                .parse()
                .map_err(|_| AutomergeError::InvalidObjIdFormat(s.to_owned()))?;
            let actor = ActorId::from(hex::decode(&s[(n + 1)..]).unwrap());
            let actor = self
                .ops
                .m
                .actors
                .lookup(&actor)
                .ok_or_else(|| AutomergeError::InvalidObjId(s.to_owned()))?;
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
            "  {:12} {:12} {:12} {:12} {:12} {:12}",
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
            let succ: Vec<_> = op.succ.into_iter().map(|id| self.to_string(*id)).collect();
            log!(
                "  {:12} {:12} {:12} {:12} {:12?} {:12?}",
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
pub(crate) struct SpanInfo {
    pub(crate) id: ExId,
    pub(crate) time: i64,
    pub(crate) start: usize,
    pub(crate) end: usize,
    #[serde(rename = "type")]
    pub(crate) span_type: String,
    pub(crate) value: ScalarValue,
}

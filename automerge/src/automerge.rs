use std::collections::{HashMap, HashSet, VecDeque};

use crate::change::encode_document;
use crate::exid::ExId;
use crate::keys::Keys;
use crate::op_set::OpSet;
use crate::transaction::{
    CommitOptions, Transaction, TransactionFailure, TransactionInner, TransactionResult,
    TransactionSuccess,
};
use crate::types::{
    ActorId, ChangeHash, Clock, ElemId, Export, Exportable, Key, ObjId, Op, OpId, OpType, Patch,
    ScalarValue, Value,
};
use crate::KeysAt;
use crate::{legacy, query, types, ObjType};
use crate::{AutomergeError, Change, Prop};
use serde::Serialize;

#[derive(Debug, Clone)]
pub(crate) enum Actor {
    Unused(ActorId),
    Cached(usize),
}

/// An automerge document.
#[derive(Debug, Clone)]
pub struct Automerge {
    pub(crate) queue: Vec<Change>,
    pub(crate) history: Vec<Change>,
    pub(crate) history_index: HashMap<ChangeHash, usize>,
    pub(crate) states: HashMap<usize, Vec<usize>>,
    pub(crate) deps: HashSet<ChangeHash>,
    pub(crate) saved: Vec<ChangeHash>,
    pub(crate) ops: OpSet,
    pub(crate) actor: Actor,
    pub(crate) max_op: u64,
}

impl Automerge {
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
        }
    }

    pub fn with_actor(mut self, actor: ActorId) -> Self {
        self.actor = Actor::Unused(actor);
        self
    }

    pub fn set_actor(&mut self, actor: ActorId) -> &mut Self {
        self.actor = Actor::Unused(actor);
        self
    }

    pub fn get_actor(&self) -> &ActorId {
        match &self.actor {
            Actor::Unused(actor) => actor,
            Actor::Cached(index) => self.ops.m.actors.get(*index),
        }
    }

    pub(crate) fn get_actor_index(&mut self) -> usize {
        match &mut self.actor {
            Actor::Unused(actor) => {
                let index = self.ops.m.actors.cache(std::mem::take(actor));
                self.actor = Actor::Cached(index);
                index
            }
            Actor::Cached(index) => *index,
        }
    }

    /// Start a transaction.
    pub fn transaction(&mut self) -> Transaction {
        let actor = self.get_actor_index();
        let seq = self.states.entry(actor).or_default().len() as u64 + 1;
        let mut deps = self.get_heads();
        if seq > 1 {
            let last_hash = self.get_hash(actor, seq - 1).unwrap();
            if !deps.contains(&last_hash) {
                deps.push(last_hash);
            }
        }

        let tx_inner = TransactionInner {
            actor,
            seq,
            start_op: self.max_op + 1,
            time: 0,
            message: None,
            extra_bytes: Default::default(),
            hash: None,
            operations: vec![],
            deps,
        };
        Transaction {
            inner: Some(tx_inner),
            doc: self,
        }
    }

    /// Run a transaction on this document in a closure, automatically handling commit or rollback
    /// afterwards.
    pub fn transact<F, O, E>(&mut self, f: F) -> TransactionResult<O, E>
    where
        F: FnOnce(&mut Transaction) -> Result<O, E>,
    {
        let mut tx = self.transaction();
        let result = f(&mut tx);
        match result {
            Ok(result) => Ok(TransactionSuccess {
                result,
                heads: tx.commit(),
            }),
            Err(error) => Err(TransactionFailure {
                error,
                cancelled: tx.rollback(),
            }),
        }
    }

    /// Like [`Self::transact`] but with a function for generating the commit options.
    pub fn transact_with<F, O, E, C>(&mut self, c: C, f: F) -> TransactionResult<O, E>
    where
        F: FnOnce(&mut Transaction) -> Result<O, E>,
        C: FnOnce() -> CommitOptions,
    {
        let mut tx = self.transaction();
        let result = f(&mut tx);
        match result {
            Ok(result) => Ok(TransactionSuccess {
                result,
                heads: tx.commit_with(c()),
            }),
            Err(error) => Err(TransactionFailure {
                error,
                cancelled: tx.rollback(),
            }),
        }
    }

    pub fn fork(&self) -> Self {
        let mut f = self.clone();
        f.set_actor(ActorId::random());
        f
    }

    fn insert_op(&mut self, op: Op) -> Op {
        let q = self.ops.search(op.obj, query::SeekOp::new(&op));

        for i in q.succ {
            self.ops.replace(op.obj, i, |old_op| old_op.add_succ(&op));
        }

        if !op.is_del() {
            self.ops.insert(q.pos, op.clone());
        }
        op
    }

    // KeysAt::()
    // LenAt::()
    // PropAt::()
    // NthAt::()

    /// Get the keys of the object `obj`.
    ///
    /// For a map this returns the keys of the map.
    /// For a list this returns the element ids (opids) encoded as strings.
    pub fn keys(&self, obj: &ExId) -> Keys {
        if let Ok(obj) = self.exid_to_obj(obj) {
            let iter_keys = self.ops.keys(obj);
            Keys::new(self, iter_keys)
        } else {
            Keys::new(self, None)
        }
    }

    /// Historical version of [`keys`](Self::keys).
    pub fn keys_at(&self, obj: &ExId, heads: &[ChangeHash]) -> KeysAt {
        if let Ok(obj) = self.exid_to_obj(obj) {
            let clock = self.clock_at(heads);
            KeysAt::new(self, self.ops.keys_at(obj, clock))
        } else {
            KeysAt::new(self, None)
        }
    }

    pub fn length(&self, obj: &ExId) -> usize {
        if let Ok(inner_obj) = self.exid_to_obj(obj) {
            match self.ops.object_type(&inner_obj) {
                Some(ObjType::Map) | Some(ObjType::Table) => self.keys(obj).count(),
                Some(ObjType::List) | Some(ObjType::Text) => {
                    self.ops.search(inner_obj, query::Len::new()).len
                }
                None => 0,
            }
        } else {
            0
        }
    }

    pub fn length_at(&self, obj: &ExId, heads: &[ChangeHash]) -> usize {
        if let Ok(inner_obj) = self.exid_to_obj(obj) {
            let clock = self.clock_at(heads);
            match self.ops.object_type(&inner_obj) {
                Some(ObjType::Map) | Some(ObjType::Table) => self.keys_at(obj, heads).count(),
                Some(ObjType::List) | Some(ObjType::Text) => {
                    self.ops.search(inner_obj, query::LenAt::new(clock)).len
                }
                None => 0,
            }
        } else {
            0
        }
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
        ExId::Id(id.0, self.ops.m.actors.cache[id.1].clone(), id.1)
    }

    pub fn text(&self, obj: &ExId) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let query = self.ops.search(obj, query::ListVals::new());
        let mut buffer = String::new();
        for q in &query.ops {
            if let OpType::Set(ScalarValue::Str(s)) = &q.action {
                buffer.push_str(s);
            }
        }
        Ok(buffer)
    }

    pub fn text_at(&self, obj: &ExId, heads: &[ChangeHash]) -> Result<String, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let clock = self.clock_at(heads);
        let query = self.ops.search(obj, query::ListValsAt::new(clock));
        let mut buffer = String::new();
        for q in &query.ops {
            if let OpType::Set(ScalarValue::Str(s)) = &q.action {
                buffer.push_str(s);
            }
        }
        Ok(buffer)
    }

    // TODO - I need to return these OpId's here **only** to get
    // the legacy conflicts format of { [opid]: value }
    // Something better?
    pub fn value<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        Ok(self.values(obj, prop.into())?.last().cloned())
    }

    pub fn value_at<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, ExId)>, AutomergeError> {
        Ok(self.values_at(obj, prop, heads)?.last().cloned())
    }

    pub fn values<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        let result = match prop.into() {
            Prop::Map(p) => {
                let prop = self.ops.m.props.lookup(&p);
                if let Some(p) = prop {
                    self.ops
                        .search(obj, query::Prop::new(p))
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
                .search(obj, query::Nth::new(n))
                .ops
                .into_iter()
                .map(|o| (o.value(), self.id_to_exid(o.id)))
                .collect(),
        };
        Ok(result)
    }

    pub fn values_at<P: Into<Prop>>(
        &self,
        obj: &ExId,
        prop: P,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, ExId)>, AutomergeError> {
        let prop = prop.into();
        let obj = self.exid_to_obj(obj)?;
        let clock = self.clock_at(heads);
        let result = match prop {
            Prop::Map(p) => {
                let prop = self.ops.m.props.lookup(&p);
                if let Some(p) = prop {
                    self.ops
                        .search(obj, query::PropAt::new(p, clock))
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
                .search(obj, query::NthAt::new(n, clock))
                .ops
                .into_iter()
                .map(|o| (o.value(), self.id_to_exid(o.id)))
                .collect(),
        };
        Ok(result)
    }

    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        let changes = Change::load_document(data)?;
        let mut doc = Self::new();
        doc.apply_changes(&changes)?;
        Ok(doc)
    }

    pub fn load_incremental(&mut self, data: &[u8]) -> Result<usize, AutomergeError> {
        let changes = Change::load_document(data)?;
        let start = self.ops.len();
        self.apply_changes(&changes)?;
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

    pub fn apply_changes(&mut self, changes: &[Change]) -> Result<Patch, AutomergeError> {
        for c in changes {
            if !self.history_index.contains_key(&c.hash) {
                if self.duplicate_seq(c) {
                    return Err(AutomergeError::DuplicateSeqNumber(
                        c.seq,
                        c.actor_id().clone(),
                    ));
                }
                if self.is_causally_ready(c) {
                    self.apply_change(c.clone());
                } else {
                    self.queue.push(c.clone());
                }
            }
        }
        while let Some(c) = self.pop_next_causally_ready_change() {
            self.apply_change(c);
        }
        Ok(Patch {})
    }

    pub fn apply_change(&mut self, change: Change) {
        let ops = self.import_ops(&change, self.history.len());
        self.update_history(change);
        for op in ops {
            self.insert_op(op);
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

    fn import_ops(&mut self, change: &Change, change_id: usize) -> Vec<Op> {
        change
            .iter_ops()
            .enumerate()
            .map(|(i, c)| {
                let actor = self.ops.m.actors.cache(change.actor_id().clone());
                let id = OpId(change.start_op + i as u64, actor);
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
                Op {
                    change: change_id,
                    id,
                    action: c.action,
                    obj,
                    key,
                    succ: Default::default(),
                    pred,
                    insert: c.insert,
                }
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
        self.apply_changes(&changes)?;
        Ok(self.get_heads())
    }

    pub fn save(&mut self) -> Result<Vec<u8>, AutomergeError> {
        let heads = self.get_heads();
        let c = self.history.iter();
        let ops = self.ops.iter();
        // TODO - can we make encode_document error free
        let bytes = encode_document(heads, c, ops, &self.ops.m.actors, &self.ops.m.props.cache);
        if bytes.is_ok() {
            self.saved = self.get_heads();
        }
        bytes
    }

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

    pub fn get_change_by_hash(&self, hash: &ChangeHash) -> Option<&Change> {
        self.history_index
            .get(hash)
            .and_then(|index| self.history.get(*index))
    }

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

    pub(crate) fn update_history(&mut self, change: Change) -> usize {
        self.max_op = std::cmp::max(self.max_op, change.start_op + change.len() as u64 - 1);

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
                .ok_or_else(|| AutomergeError::InvalidOpId(s.to_owned()))?;
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
        for i in self.ops.iter() {
            let id = self.to_string(i.id);
            let obj = self.to_string(i.obj);
            let key = match i.key {
                Key::Map(n) => self.ops.m.props[n].clone(),
                Key::Seq(n) => self.to_string(n),
            };
            let value: String = match &i.action {
                OpType::Set(value) => format!("{}", value),
                OpType::Make(obj) => format!("make({})", obj),
                OpType::Inc(obj) => format!("inc({})", obj),
                OpType::Del => format!("del{}", 0),
            };
            let pred: Vec<_> = i.pred.iter().map(|id| self.to_string(*id)).collect();
            let succ: Vec<_> = i.succ.iter().map(|id| self.to_string(*id)).collect();
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
    use crate::transaction::Transactable;
    use crate::*;
    use std::convert::TryInto;

    #[test]
    fn insert_op() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::random());
        let mut tx = doc.transaction();
        tx.set(&ROOT, "hello", "world")?;
        tx.value(&ROOT, "hello")?;
        tx.commit();
        Ok(())
    }

    #[test]
    fn test_set() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        // setting a scalar value shouldn't return an opid as no object was created.
        assert!(tx.set(&ROOT, "a", 1)?.is_none());
        // setting the same value shouldn't return an opid as there is no change.
        assert!(tx.set(&ROOT, "a", 1)?.is_none());

        assert!(tx.set(&ROOT, "b", Value::map())?.is_some());
        // object already exists at b but setting a map again overwrites it so we get an opid.
        assert!(tx.set(&ROOT, "b", Value::map())?.is_some());
        tx.commit();
        Ok(())
    }

    #[test]
    fn test_list() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::random());
        let mut tx = doc.transaction();
        let list_id = tx.set(&ROOT, "items", Value::list())?.unwrap();
        tx.set(&ROOT, "zzz", "zzzval")?;
        assert!(tx.value(&ROOT, "items")?.unwrap().1 == list_id);
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
        doc.save()?;
        Ok(())
    }

    #[test]
    fn test_del() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(ActorId::random());
        let mut tx = doc.transaction();
        tx.set(&ROOT, "xxx", "xxx")?;
        assert!(!tx.values(&ROOT, "xxx")?.is_empty());
        tx.del(&ROOT, "xxx")?;
        assert!(tx.values(&ROOT, "xxx")?.is_empty());
        tx.commit();
        Ok(())
    }

    #[test]
    fn test_inc() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.set(&ROOT, "counter", Value::counter(10))?;
        assert!(tx.value(&ROOT, "counter")?.unwrap().0 == Value::counter(10));
        tx.inc(&ROOT, "counter", 10)?;
        assert!(tx.value(&ROOT, "counter")?.unwrap().0 == Value::counter(20));
        tx.inc(&ROOT, "counter", -5)?;
        assert!(tx.value(&ROOT, "counter")?.unwrap().0 == Value::counter(15));
        tx.commit();
        Ok(())
    }

    #[test]
    fn test_save_incremental() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();

        let mut tx = doc.transaction();
        tx.set(&ROOT, "foo", 1)?;
        tx.commit();

        let save1 = doc.save().unwrap();

        let mut tx = doc.transaction();
        tx.set(&ROOT, "bar", 2)?;
        tx.commit();

        let save2 = doc.save_incremental();

        let mut tx = doc.transaction();
        tx.set(&ROOT, "baz", 3)?;
        tx.commit();

        let save3 = doc.save_incremental();

        let mut save_a: Vec<u8> = vec![];
        save_a.extend(&save1);
        save_a.extend(&save2);
        save_a.extend(&save3);

        assert!(doc.save_incremental().is_empty());

        let save_b = doc.save().unwrap();

        assert!(save_b.len() < save_a.len());

        let mut doc_a = Automerge::load(&save_a)?;
        let mut doc_b = Automerge::load(&save_b)?;

        assert!(doc_a.values(&ROOT, "baz")? == doc_b.values(&ROOT, "baz")?);

        assert!(doc_a.save().unwrap() == doc_b.save().unwrap());

        Ok(())
    }

    #[test]
    fn test_save_text() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let text = tx.set(&ROOT, "text", Value::text())?.unwrap();
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
        tx.set(&ROOT, "prop1", "val1")?;
        tx.commit();
        doc.get_heads();
        let heads1 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.set(&ROOT, "prop1", "val2")?;
        tx.commit();
        doc.get_heads();
        let heads2 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.set(&ROOT, "prop2", "val3")?;
        tx.commit();
        doc.get_heads();
        let heads3 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.del(&ROOT, "prop1")?;
        tx.commit();
        doc.get_heads();
        let heads4 = doc.get_heads();
        let mut tx = doc.transaction();
        tx.set(&ROOT, "prop3", "val4")?;
        tx.commit();
        doc.get_heads();
        let heads5 = doc.get_heads();
        assert!(doc.keys_at(&ROOT, &heads1).collect_vec() == vec!["prop1".to_owned()]);
        assert_eq!(doc.length_at(&ROOT, &heads1), 1);
        assert!(doc.value_at(&ROOT, "prop1", &heads1)?.unwrap().0 == Value::str("val1"));
        assert!(doc.value_at(&ROOT, "prop2", &heads1)? == None);
        assert!(doc.value_at(&ROOT, "prop3", &heads1)? == None);

        assert!(doc.keys_at(&ROOT, &heads2).collect_vec() == vec!["prop1".to_owned()]);
        assert_eq!(doc.length_at(&ROOT, &heads2), 1);
        assert!(doc.value_at(&ROOT, "prop1", &heads2)?.unwrap().0 == Value::str("val2"));
        assert!(doc.value_at(&ROOT, "prop2", &heads2)? == None);
        assert!(doc.value_at(&ROOT, "prop3", &heads2)? == None);

        assert!(
            doc.keys_at(&ROOT, &heads3).collect_vec()
                == vec!["prop1".to_owned(), "prop2".to_owned()]
        );
        assert_eq!(doc.length_at(&ROOT, &heads3), 2);
        assert!(doc.value_at(&ROOT, "prop1", &heads3)?.unwrap().0 == Value::str("val2"));
        assert!(doc.value_at(&ROOT, "prop2", &heads3)?.unwrap().0 == Value::str("val3"));
        assert!(doc.value_at(&ROOT, "prop3", &heads3)? == None);

        assert!(doc.keys_at(&ROOT, &heads4).collect_vec() == vec!["prop2".to_owned()]);
        assert_eq!(doc.length_at(&ROOT, &heads4), 1);
        assert!(doc.value_at(&ROOT, "prop1", &heads4)? == None);
        assert!(doc.value_at(&ROOT, "prop2", &heads4)?.unwrap().0 == Value::str("val3"));
        assert!(doc.value_at(&ROOT, "prop3", &heads4)? == None);

        assert!(
            doc.keys_at(&ROOT, &heads5).collect_vec()
                == vec!["prop2".to_owned(), "prop3".to_owned()]
        );
        assert_eq!(doc.length_at(&ROOT, &heads5), 2);
        assert_eq!(doc.length(&ROOT), 2);
        assert!(doc.value_at(&ROOT, "prop1", &heads5)? == None);
        assert!(doc.value_at(&ROOT, "prop2", &heads5)?.unwrap().0 == Value::str("val3"));
        assert!(doc.value_at(&ROOT, "prop3", &heads5)?.unwrap().0 == Value::str("val4"));

        assert_eq!(doc.keys_at(&ROOT, &[]).count(), 0);
        assert_eq!(doc.length_at(&ROOT, &[]), 0);
        assert!(doc.value_at(&ROOT, "prop1", &[])? == None);
        assert!(doc.value_at(&ROOT, "prop2", &[])? == None);
        assert!(doc.value_at(&ROOT, "prop3", &[])? == None);
        Ok(())
    }

    #[test]
    fn test_len_at() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor("aaaa".try_into().unwrap());

        let mut tx = doc.transaction();
        let list = tx.set(&ROOT, "list", Value::list())?.unwrap();
        tx.commit();
        let heads1 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.insert(&list, 0, Value::int(10))?;
        tx.commit();
        let heads2 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.set(&list, 0, Value::int(20))?;
        tx.insert(&list, 0, Value::int(30))?;
        tx.commit();
        let heads3 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.set(&list, 1, Value::int(40))?;
        tx.insert(&list, 1, Value::int(50))?;
        tx.commit();
        let heads4 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.del(&list, 2)?;
        tx.commit();
        let heads5 = doc.get_heads();

        let mut tx = doc.transaction();
        tx.del(&list, 0)?;
        tx.commit();
        let heads6 = doc.get_heads();

        assert!(doc.length_at(&list, &heads1) == 0);
        assert!(doc.value_at(&list, 0, &heads1)?.is_none());

        assert!(doc.length_at(&list, &heads2) == 1);
        assert!(doc.value_at(&list, 0, &heads2)?.unwrap().0 == Value::int(10));

        assert!(doc.length_at(&list, &heads3) == 2);
        doc.dump();
        //log!("{:?}", doc.value_at(&list, 0, &heads3)?.unwrap().0);
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
    fn keys_iter() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.set(&ROOT, "a", 3).unwrap();
        tx.set(&ROOT, "b", 4).unwrap();
        tx.set(&ROOT, "c", 5).unwrap();
        tx.set(&ROOT, "d", 6).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.set(&ROOT, "a", 7).unwrap();
        tx.commit();
        let mut tx = doc.transaction();
        tx.set(&ROOT, "a", 8).unwrap();
        tx.set(&ROOT, "d", 9).unwrap();
        tx.commit();
        assert_eq!(doc.keys(&ROOT).count(), 4);

        let mut keys = doc.keys(&ROOT);
        assert_eq!(keys.next(), Some("a".into()));
        assert_eq!(keys.next(), Some("b".into()));
        assert_eq!(keys.next(), Some("c".into()));
        assert_eq!(keys.next(), Some("d".into()));
        assert_eq!(keys.next(), None);

        let mut keys = doc.keys(&ROOT);
        assert_eq!(keys.next_back(), Some("d".into()));
        assert_eq!(keys.next_back(), Some("c".into()));
        assert_eq!(keys.next_back(), Some("b".into()));
        assert_eq!(keys.next_back(), Some("a".into()));
        assert_eq!(keys.next_back(), None);

        let mut keys = doc.keys(&ROOT);
        assert_eq!(keys.next(), Some("a".into()));
        assert_eq!(keys.next_back(), Some("d".into()));
        assert_eq!(keys.next_back(), Some("c".into()));
        assert_eq!(keys.next_back(), Some("b".into()));
        assert_eq!(keys.next_back(), None);

        let mut keys = doc.keys(&ROOT);
        assert_eq!(keys.next_back(), Some("d".into()));
        assert_eq!(keys.next(), Some("a".into()));
        assert_eq!(keys.next(), Some("b".into()));
        assert_eq!(keys.next(), Some("c".into()));
        assert_eq!(keys.next(), None);
        let keys = doc.keys(&ROOT);
        assert_eq!(keys.collect::<Vec<_>>(), vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn rolling_back_transaction_has_no_effect() {
        let mut doc = Automerge::new();
        let old_states = doc.states.clone();
        let bytes = doc.save().unwrap();
        let tx = doc.transaction();
        tx.rollback();
        let new_states = doc.states.clone();
        assert_eq!(old_states, new_states);
        let new_bytes = doc.save().unwrap();
        assert_eq!(bytes, new_bytes);
    }
}

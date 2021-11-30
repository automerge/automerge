extern crate hex;
extern crate uuid;
extern crate web_sys;

// compute Succ Pred
// implement del

// this is needed for print debugging via WASM
#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

mod change;
mod columnar;
mod decoding;
mod encoding;
mod indexed_cache;
mod sync;

mod error;
mod expanded_op;
mod internal;
mod protocol;
//mod sequence_tree;
//use sequence_tree::SequenceTree;
mod op_tree;

use op_tree::OpTree;

use automerge_protocol as amp;
use change::{encode_document, export_change};
//use core::ops::Range;
pub use error::AutomergeError;
use indexed_cache::IndexedCache;
use nonzero_ext::nonzero;
use protocol::Key;
use protocol::Op;
pub use protocol::{
    ElemId, Export, Exportable, Importable, ObjId, OpId, Patch, Peer, Prop, Value, HEAD, ROOT,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use sync::BloomFilter;

pub use amp::ChangeHash;
pub use change::{decode_change, Change};
pub use sync::{SyncMessage, SyncState};

pub use amp::{ActorId, ObjType, ScalarValue};

#[derive(Debug, Clone)]
pub struct Automerge {
    actors: IndexedCache<amp::ActorId>,
    queue: Vec<Change>,
    props: IndexedCache<String>,
    history: Vec<Change>,
    history_index: HashMap<ChangeHash, usize>,
    states: HashMap<usize, Vec<usize>>,
    deps: HashSet<ChangeHash>,
    //ops: Vec<Op>,
    //ops: SequenceTree<Op>,
    ops: OpTree,
    actor: Option<usize>,
    max_op: u64,
    transaction: Option<Transaction>,
}

impl Automerge {
    pub fn new() -> Self {
        Automerge {
            actors: IndexedCache::from(vec![]),
            props: IndexedCache::new(),
            queue: vec![],
            history: vec![],
            history_index: HashMap::new(),
            states: HashMap::new(),
            ops: Default::default(),
            deps: Default::default(),
            actor: None,
            max_op: 0,
            transaction: None,
        }
    }

    pub fn set_actor(&mut self, actor: amp::ActorId) {
        self.actor = Some(self.actors.cache(actor))
    }

    fn random_actor(&mut self) -> amp::ActorId {
        let actor = amp::ActorId::from(uuid::Uuid::new_v4().as_bytes().to_vec());
        self.actor = Some(self.actors.cache(actor.clone()));
        actor
    }

    pub fn get_actor(&mut self) -> amp::ActorId {
        if let Some(actor) = self.actor {
            self.actors[actor].clone()
        } else {
            self.random_actor()
        }
    }

    fn get_actor_index(&mut self) -> usize {
        if let Some(actor) = self.actor {
            actor
        } else {
            self.random_actor();
            self.actor.unwrap() // random_actor always sets actor to is_some()
        }
    }

    pub fn new_with_actor_id(actor: amp::ActorId) -> Self {
        Automerge {
            actors: IndexedCache::from(vec![actor]),
            props: IndexedCache::new(),
            queue: vec![],
            history: vec![],
            history_index: HashMap::new(),
            states: HashMap::new(),
            ops: Default::default(),
            deps: Default::default(),
            actor: None,
            max_op: 0,
            transaction: None,
        }
    }

    pub fn pending_ops(&self) -> u64 {
        match &self.transaction {
            Some(t) => t.operations.len() as u64,
            None => 0,
        }
    }

    pub fn begin(&mut self) -> Result<(), AutomergeError> {
        self.begin_with_opts(None, None)
    }

    pub fn begin_with_opts(
        &mut self,
        message: Option<String>,
        time: Option<i64>,
    ) -> Result<(), AutomergeError> {
        if self.transaction.is_some() {
            return Err(AutomergeError::MismatchedBegin);
        }

        let actor = self.get_actor_index();

        let seq = self.states.entry(actor).or_default().len() as u64 + 1;
        let mut deps = self.get_heads();
        if seq > 1 {
            let last_hash = self.get_hash(actor, seq - 1)?;
            if !deps.contains(&last_hash) {
                deps.push(last_hash);
            }
        }

        self.transaction = Some(Transaction {
            actor,
            seq,
            start_op: self.max_op + 1,
            time: time.unwrap_or(0),
            message,
            extra_bytes: Default::default(),
            hash: None,
            operations: vec![],
            deps,
        });

        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), AutomergeError> {
        if let Some(tx) = self.transaction.take() {
            self.update_history(export_change(&tx, &self.actors, &self.props));
            Ok(())
        } else {
            Err(AutomergeError::MismatchedCommit)
        }
    }

    pub fn rollback(&mut self) {
        if let Some(tx) = self.transaction.take() {
            for op in &tx.operations {
                for pred_id in &op.pred {
                    if let Some(p) = self.ops.iter().position(|o| o.id == *pred_id) {
                        if let Some(o) = self.ops.get_mut(p) {
                            o.succ.retain(|i| i != pred_id);
                        }
                    }
                }
                if let Some(pos) = self.ops.iter().position(|o| o.id == op.id) {
                    self.ops.remove(pos);
                }
            }
        }
    }

    fn prop_to_key(&self, obj: &ObjId, prop: Prop, insert: bool) -> Result<Key, AutomergeError> {
        match prop {
            Prop::Map(s) => {
                if s.is_empty() {
                    return Err(AutomergeError::EmptyStringKey);
                }
                Ok(Key::Map(
                    self.props
                        .lookup(s.clone())
                        .ok_or(AutomergeError::InvalidProp(s))?,
                ))
            }
            Prop::Seq(n) => {
                if insert {
                    self.insert_pos_for_index(obj, n)
                        .ok_or(AutomergeError::InvalidIndex(n))
                } else {
                    self.set_pos_for_index(obj, n)
                        .ok_or(AutomergeError::InvalidIndex(n))
                }
            }
        }
    }

    fn import_prop(
        &mut self,
        obj: &ObjId,
        prop: Prop,
        insert: bool,
    ) -> Result<Key, AutomergeError> {
        match prop {
            Prop::Map(s) => {
                if s.is_empty() {
                    return Err(AutomergeError::EmptyStringKey);
                }
                Ok(Key::Map(self.props.cache(s)))
            }
            Prop::Seq(n) => {
                if insert {
                    self.insert_pos_for_index(obj, n)
                        .ok_or(AutomergeError::InvalidIndex(n))
                } else {
                    self.set_pos_for_index(obj, n)
                        .ok_or(AutomergeError::InvalidIndex(n))
                }
            }
        }
    }
    fn key_cmp(&self, left: &Key, right: &Key) -> Option<Ordering> {
        match (left, right) {
            (Key::Map(a), Key::Map(b)) => Some(self.props[*a].cmp(&self.props[*b])),
            _ => None,
        }
    }

    fn make_op(
        &mut self,
        obj: &ObjId,
        key: Key,
        action: amp::OpType,
        insert: bool,
    ) -> Result<OpId, AutomergeError> {
        if let Some(mut tx) = self.transaction.take() {
            let id = OpId(tx.start_op + tx.operations.len() as u64, tx.actor);
            let op = Op {
                change: self.history.len(),
                id,
                action,
                obj: *obj,
                key,
                succ: vec![],
                pred: vec![],
                insert,
            };
            let op = self.insert_op(op, true);
            tx.operations.push(op);
            self.transaction = Some(tx);
            Ok(id)
        } else {
            Err(AutomergeError::OpOutsideOfTransaction)
        }
    }

    fn scan_to_obj(&self, obj: &ObjId, pos: &mut usize) {
        for op in self.ops.iter().skip(*pos) {
            if lamport_cmp(&self.actors, obj.0, op.obj.0) != Ordering::Greater {
                break;
            }
            *pos += 1;
        }
    }

    fn scan_to_prop_start(&self, obj: &ObjId, key: &Key, pos: &mut usize) {
        for op in self.ops.iter().skip(*pos) {
            if &op.obj != obj || self.key_cmp(key, &op.key) != Some(Ordering::Greater) {
                break;
            }
            *pos += 1;
        }
    }

    fn scan_to_visible(&self, obj: &ObjId, pos: &mut usize) {
        let mut counters = Default::default();
        for op in self.ops.iter().skip(*pos) {
            if &op.obj != obj || is_visible(op, *pos, &mut counters) {
                break;
            }
            *pos += 1
        }
    }

    fn scan_to_nth_visible(&self, obj: &ObjId, n: usize, pos: &mut usize) -> Vec<Op> {
        let mut seen = 0;
        let mut seen_visible = false;
        let mut counters = Default::default();
        let mut result = vec![];
        for op in self.ops.iter().skip(*pos) {
            if &op.obj != obj {
                break;
            }
            if op.insert {
                seen_visible = false;
            }
            if !seen_visible && is_visible(op, *pos, &mut counters) {
                seen += 1;
                seen_visible = true;
            }
            if seen == n + 1 {
                let vop = visible_op(op, &counters);
                result.push(vop.clone())
            }
            if seen > n + 1 {
                break;
            }
            *pos += 1;
        }
        result
    }

    fn scan_visible(&self, obj: &ObjId, pos: &mut usize) -> usize {
        let mut seen = 0;
        let mut seen_visible = false;
        let mut counters = Default::default();
        for op in self.ops.iter().skip(*pos) {
            if &op.obj != obj {
                break;
            }
            if op.insert {
                seen_visible = false;
            }
            if !seen_visible && is_visible(op, *pos, &mut counters) {
                seen += 1;
                seen_visible = true;
            }
            *pos += 1;
        }
        seen
    }

    fn scan_to_next_prop(&self, obj: &ObjId, key: &Key, pos: &mut usize) {
        for op in self.ops.iter().skip(*pos) {
            if !(&op.obj == obj && &op.key == key) {
                break;
            }
            *pos += 1
        }
    }

    fn scan_to_prop_insertion_point(&mut self, next: &mut Op, local: bool, pos: &mut usize) {
        let mut counters = Default::default();
        let mut succ = vec![];
        for op in self.ops.iter().skip(*pos) {
            if !(op.obj == next.obj
                && op.key == next.key
                && lamport_cmp(&self.actors, next.id, op.id) == Ordering::Greater)
            {
                break;
            }
            // FIXME if i increment pos x and it has a counter and a non counter do i take both or one pred
            if local {
                if is_visible(op, *pos, &mut counters) {
                    succ.push((true, visible_pos(op, *pos, &counters)));
                }
            } else if next.pred.iter().any(|i| i == &op.id) {
                succ.push((false, *pos));
            }
            *pos += 1
        }

        for (local, vpos) in succ {
            if let Some(op) = self.ops.get_mut(vpos) {
                op.succ.push(next.id);
                if local {
                    next.pred.push(op.id);
                }
            }
        }
    }

    fn scan_to_prop_value(
        &self,
        obj: &ObjId,
        key: &Key,
        _clock: &Clock,
        pos: &mut usize,
    ) -> Vec<Op> {
        let mut counters = Default::default();
        let mut result = vec![];
        for op in self.ops.iter().skip(*pos) {
            if !(&op.obj == obj && &op.key == key) {
                break;
            }
            if is_visible(op, *pos, &mut counters) {
                let vop = visible_op(op, &counters);
                result.push(vop.clone())
            }
            *pos += 1
        }
        result
    }

    fn scan_to_elem_insert_op1(
        &self,
        obj: &ObjId,
        elem: &ElemId,
        pos: &mut usize,
        seen: &mut usize,
    ) {
        if *elem == HEAD {
            return;
        }

        let mut seen_key = None;
        let mut counters = Default::default();

        for op in self.ops.iter().skip(*pos) {
            if &op.obj != obj {
                break;
            }
            if op.elemid() != seen_key && is_visible(op, *pos, &mut counters) {
                *seen += 1;
                seen_key = op.elemid(); // only count each elemid once
            }

            *pos += 1;

            if op.insert && op.id == elem.0 {
                break;
            }
        }
    }

    fn scan_to_elem_insert_op2(
        &self,
        obj: &ObjId,
        elem: &ElemId,
        pos: &mut usize,
        seen: &mut usize,
    ) {
        if *elem == HEAD {
            return;
        }

        let mut seen_key = None;
        let mut counters = Default::default();

        for op in self.ops.iter().skip(*pos) {
            if &op.obj != obj {
                break;
            }
            if op.elemid() != seen_key && is_visible(op, *pos, &mut counters) {
                *seen += 1;
                seen_key = op.elemid(); // only count each elemid once
            }

            if op.insert && op.id == elem.0 {
                break;
            }

            *pos += 1;
        }
    }

    fn scan_to_elem_update_pos(&mut self, next: &mut Op, local: bool, pos: &mut usize) {
        let mut counters = Default::default();
        let mut succ = vec![];
        for op in self.ops.iter().skip(*pos) {
            if !(op.obj == next.obj
                && op.elemid() == next.elemid()
                && lamport_cmp(&self.actors, next.id, op.id) == Ordering::Greater)
            {
                break;
            }
            if local {
                if op.elemid() == next.elemid() && is_visible(op, *pos, &mut counters) {
                    succ.push((true, visible_pos(op, *pos, &counters)));
                }
            // FIXME - do I need a visible check here?
            } else if op.visible()
                && op.elemid() == next.elemid()
                && next.pred.iter().any(|i| i == &op.id)
            {
                succ.push((false, *pos));
            }
            *pos += 1
        }

        for (local, vpos) in succ {
            if let Some(op) = self.ops.get_mut(vpos) {
                op.succ.push(next.id);
                if local {
                    next.pred.push(op.id);
                }
            }
        }
    }

    fn scan_to_lesser_insert(&self, next: &Op, pos: &mut usize, seen: &mut usize) {
        let mut seen_key = None;
        let mut counters = Default::default();

        for op in self.ops.iter().skip(*pos) {
            if op.obj != next.obj {
                break;
            }

            if op.elemid() != seen_key && is_visible(op, *pos, &mut counters) {
                *seen += 1;
                seen_key = op.elemid(); // only count each elemid once
            }

            if next.insert && lamport_cmp(&self.actors, next.id, op.id) == Ordering::Greater {
                break;
            }

            *pos += 1
        }
    }

    fn seek_to_update_elem(&mut self, op: &mut Op, elem: &ElemId, local: bool) -> Cursor {
        let mut pos = 0;
        let mut seen = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op2(&op.obj, elem, &mut pos, &mut seen);
        self.scan_to_elem_update_pos(op, local, &mut pos);
        Cursor { pos, seen }
    }

    fn seek_to_insert_elem(&self, op: &Op, elem: &ElemId) -> Cursor {
        let mut pos = 0;
        let mut seen = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op1(&op.obj, elem, &mut pos, &mut seen);
        self.scan_to_lesser_insert(op, &mut pos, &mut seen);
        Cursor { pos, seen }
    }

    fn seek_to_map_op(&mut self, op: &mut Op, local: bool) -> Cursor {
        let mut pos = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_prop_start(&op.obj, &op.key, &mut pos);
        self.scan_to_prop_insertion_point(op, local, &mut pos);
        Cursor { pos, seen: 0 }
    }

    fn seek_to_op(&mut self, op: &mut Op, local: bool) -> Cursor {
        match (op.key, op.insert) {
            (Key::Map(_), _) => self.seek_to_map_op(op, local),
            (Key::Seq(elem), true) => self.seek_to_insert_elem(op, &elem),
            (Key::Seq(elem), false) => self.seek_to_update_elem(op, &elem, local),
        }
    }

    fn insert_op(&mut self, mut op: Op, local: bool) -> Op {
        let cursor = self.seek_to_op(&mut op, local); //mut to collect pred
        if !op.is_del() {
            self.ops.insert(cursor.pos, op.clone());
        }
        op
    }

    pub fn keys(&self, obj: &ObjId) -> Vec<String> {
        let mut pos = 0;
        let mut result = vec![];
        self.scan_to_obj(obj, &mut pos);
        self.scan_to_visible(obj, &mut pos);
        while let Some(op) = self.ops.get(pos) {
            // we reached the next object
            if &op.obj != obj {
                break;
            }
            let key = &op.key;
            result.push(self.export(*key));
            self.scan_to_next_prop(obj, key, &mut pos);
            self.scan_to_visible(obj, &mut pos);
        }
        result
    }

    pub fn length(&self, obj: &ObjId) -> usize {
        let mut pos = 0;
        self.scan_to_obj(obj, &mut pos);
        self.scan_visible(obj, &mut pos)
    }

    // TODO ? really export this ?
    pub fn insert_pos_for_index(&self, obj: &ObjId, index: usize) -> Option<Key> {
        if index == 0 {
            Some(HEAD.into())
        } else {
            self.set_pos_for_index(obj, index - 1)
        }
    }

    fn set_pos_for_index(&self, obj: &ObjId, index: usize) -> Option<Key> {
        let mut pos = 0;
        self.scan_to_obj(obj, &mut pos);
        let ops = self.scan_to_nth_visible(obj, index, &mut pos);
        ops.get(0).and_then(|o| o.elemid()).map(|e| e.into())
    }

    // idea!
    // set(obj, prop, value) - value can be scalar or objtype
    // insert(obj, prop, value)
    // del(obj, prop)
    // inc(obj, prop)
    // what about AT?

    pub fn set(&mut self, obj: &ObjId, prop: Prop, value: Value) -> Result<OpId, AutomergeError> {
        let key = self.import_prop(obj, prop, false)?;
        match value {
            Value::Object(o) => self.make_op(obj, key, amp::OpType::Make(o), false),
            Value::Scalar(s) => self.make_op(obj, key, amp::OpType::Set(s), false),
        }
    }

    pub fn insert(
        &mut self,
        obj: &ObjId,
        index: usize,
        value: Value,
    ) -> Result<OpId, AutomergeError> {
        let key = self.import_prop(obj, index.into(), true)?;
        match value {
            Value::Object(o) => self.make_op(obj, key, amp::OpType::Make(o), true),
            Value::Scalar(s) => self.make_op(obj, key, amp::OpType::Set(s), true),
        }
    }

    pub fn inc(&mut self, obj: &ObjId, prop: Prop, value: i64) -> Result<(), AutomergeError> {
        let key = self.import_prop(obj, prop, false)?;
        self.make_op(obj, key, amp::OpType::Inc(value), false)?;
        Ok(())
    }

    pub fn del(&mut self, obj: &ObjId, prop: Prop) -> Result<(), AutomergeError> {
        if let Ok(key) = self.prop_to_key(obj, prop, false) {
            self.make_op(obj, key, amp::OpType::Del(nonzero!(1_u32)), false)?;
        }
        Ok(())
    }

    pub fn splice(
        &mut self,
        obj: &ObjId,
        mut pos: usize,
        del: usize,
        vals: Vec<Value>,
    ) -> Result<(), AutomergeError> {
        for _ in 0..del {
            self.del(obj, pos.into())?;
        }
        for v in vals {
            self.insert(obj, pos, v)?;
            pos += 1;
        }
        Ok(())
    }

    pub fn text(&self, _path: &str) -> String {
        unimplemented!()
    }

    pub fn value(&self, obj: &ObjId, prop: Prop) -> Result<Option<(Value, OpId)>, AutomergeError> {
        Ok(self.values_at(obj, prop, &[])?.first().cloned())
    }

    pub fn value_at(
        &self,
        obj: &ObjId,
        prop: Prop,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, OpId)>, AutomergeError> {
        Ok(self.values_at(obj, prop, heads)?.first().cloned())
    }

    pub fn values(&self, obj: &ObjId, prop: Prop) -> Result<Vec<(Value, OpId)>, AutomergeError> {
        self.values_at(obj, prop, &[])
    }

    pub fn values_at(
        &self,
        obj: &ObjId,
        prop: Prop,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, OpId)>, AutomergeError> {
        let clock = self.clock_at(heads);
        let result = match prop {
            Prop::Map(p) => {
                let mut pos = 0;
                let prop = self.props.lookup(p);
                if let Some(p) = prop {
                    let prop = Key::Map(p);
                    self.scan_to_obj(obj, &mut pos);
                    self.scan_to_prop_start(obj, &prop, &mut pos);
                    let ops = self.scan_to_prop_value(obj, &prop, &clock, &mut pos);
                    ops.into_iter().map(|o| o.into()).collect()
                } else {
                    vec![]
                }
            }
            Prop::Seq(index) => {
                let mut pos = 0;
                self.scan_to_obj(obj, &mut pos);
                let ops = self.scan_to_nth_visible(obj, index, &mut pos);
                ops.into_iter().map(|o| o.into()).collect()
            }
        };
        Ok(result)
    }

    pub fn load(data: &[u8]) -> Result<Self, AutomergeError> {
        let changes = Change::load_document(data)?;
        let mut doc = Self::new();
        doc.apply_changes(&changes)?;
        Ok(doc)
    }

    pub fn apply_changes(&mut self, changes: &[Change]) -> Result<Patch, AutomergeError> {
        for c in changes {
            if self.is_causally_ready(c) {
                self.apply_change(c.clone());
            } else {
                self.queue.push(c.clone());
                while let Some(c) = self.pop_next_causally_ready_change() {
                    self.apply_change(c);
                }
            }
        }
        Ok(Patch {})
    }

    pub fn apply_change(&mut self, change: Change) {
        let ops = self.import_ops(&change, self.history.len());
        self.update_history(change);
        for op in ops {
            self.insert_op(op, false);
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
                let actor = self.actors.cache(change.actor_id().clone());
                let id = OpId(change.start_op + i as u64, actor);
                // FIXME dont need to_string()
                let obj: ObjId = self.import(&c.obj.to_string()).unwrap();
                let pred = c
                    .pred
                    .iter()
                    .map(|i| self.import(&i.to_string()).unwrap())
                    .collect();
                let key = match &c.key.as_ref() {
                    amp::Key::Map(n) => Key::Map(self.props.cache(n.to_string())),
                    amp::Key::Seq(amp::ElementId::Head) => Key::Seq(HEAD),
                    // FIXME dont need to_string()
                    amp::Key::Seq(amp::ElementId::Id(i)) => {
                        Key::Seq(self.import(&i.to_string()).unwrap())
                    }
                };
                Op {
                    change: change_id,
                    id,
                    action: c.action.into(),
                    obj,
                    key,
                    succ: vec![],
                    pred,
                    insert: c.insert,
                }
            })
            .collect()
    }

    pub fn save(&self) -> Result<Vec<u8>, AutomergeError> {
        let c: Vec<_> = self.history.iter().map(|c| c.decode()).collect();
        // FIXME
        let ops: Vec<_> = self.ops.iter().cloned().collect();
        encode_document(&c, ops.as_slice(), &self.actors, &self.props.cache)
    }

    pub fn save_incremental(&mut self) -> Vec<u8> {
        unimplemented!()
    }

    /// Filter the changes down to those that are not transitive dependencies of the heads.
    ///
    /// Thus a graph with these heads has not seen the remaining changes.
    pub(crate) fn filter_changes(
        &self,
        heads: &[amp::ChangeHash],
        changes: &mut HashSet<amp::ChangeHash>,
    ) {
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

    pub fn get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<amp::ChangeHash> {
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
        if let Some(actor) = &self.actor {
            let actor = &self.actors[*actor];
            return self.history.iter().rev().find(|c| c.actor_id() == actor);
        }
        None
    }

    pub fn get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        if let Some(changes) = self.get_changes_fast(have_deps) {
            changes
        } else {
            self.get_changes_slow(have_deps)
        }
    }

    fn clock_at(&self, heads: &[ChangeHash]) -> Clock {
        if heads.is_empty() {
            return Clock::Head;
        }
        // FIXME - could be way faster
        let mut clock = HashMap::new();
        for c in self.get_changes(heads) {
            let actor = self.actors.lookup(c.actor_id().clone()).unwrap();
            if let Some(val) = clock.get(&actor) {
                if val < &c.seq {
                    clock.insert(actor, c.seq);
                }
            } else {
                clock.insert(actor, c.seq);
            }
        }
        Clock::At(clock)
    }

    pub fn get_change_by_hash(&self, hash: &amp::ChangeHash) -> Option<&Change> {
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

    pub fn get_heads(&self) -> Vec<ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    fn get_hash(&self, actor: usize, seq: u64) -> Result<amp::ChangeHash, AutomergeError> {
        self.states
            .get(&actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|&i| self.history.get(i))
            .map(|c| c.hash)
            .ok_or(AutomergeError::InvalidSeq(seq))
    }

    fn update_history(&mut self, change: Change) -> usize {
        self.max_op = std::cmp::max(self.max_op, change.start_op + change.len() as u64 - 1);

        self.update_deps(&change);

        let history_index = self.history.len();

        self.states
            .entry(self.actors.cache(change.actor_id().clone()))
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

    pub fn import<I: Importable>(&self, s: &str) -> Result<I, AutomergeError> {
        if let Some(x) = I::from(s) {
            Ok(x)
        } else {
            let n = s
                .find('@')
                .ok_or_else(|| AutomergeError::InvalidOpId(s.to_owned()))?;
            let counter = s[0..n]
                .parse()
                .map_err(|_| AutomergeError::InvalidOpId(s.to_owned()))?;
            let actor = amp::ActorId::from(hex::decode(&s[(n + 1)..]).unwrap());
            let actor = self
                .actors
                .lookup(actor)
                .ok_or_else(|| AutomergeError::InvalidOpId(s.to_owned()))?;
            Ok(I::wrap(OpId(counter, actor)))
        }
    }

    pub fn export<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => format!("{}@{}", id.counter(), self.actors[id.actor()]),
            Export::Prop(index) => self.props[index].clone(),
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
            let id = self.export(i.id);
            let obj = self.export(i.obj);
            let key = match i.key {
                Key::Map(n) => self.props[n].clone(),
                Key::Seq(n) => self.export(n),
            };
            let value: String = match &i.action {
                amp::OpType::Set(value) => format!("{}", value),
                amp::OpType::Make(obj) => format!("make{}", obj),
                _ => unimplemented!(),
            };
            let pred: Vec<_> = i.pred.iter().map(|id| self.export(*id)).collect();
            let succ: Vec<_> = i.succ.iter().map(|id| self.export(*id)).collect();
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

    pub fn dump2(&self) {
        println!("  {:12} {:12} {:12} value pred succ", "id", "obj", "key",);
        for i in self.ops.iter() {
            let id = &self.export(i.id)[0..8];
            let obj = &self.export(i.obj)[0..5];
            let key = &self.export(i.key)[0..5];
            /*
                        let key = match i.key {
                            Key::Map(n) => &self.props[n],
                            Key::Seq(n) => unimplemented!(),
                        };
            */
            println!("{:?}", i.action);
            let value: String = match &i.action {
                amp::OpType::Set(value) => format!("{}", value),
                amp::OpType::Make(obj) => format!("make{}", obj),
                amp::OpType::Inc(val) => format!("inc{}", val),
                _ => unimplemented!(),
            };
            let pred: Vec<_> = i.pred.iter().map(|id| self.export(*id)).collect();
            let succ: Vec<_> = i.succ.iter().map(|id| self.export(*id)).collect();
            println!(
                "  {:12} {:12} {:12} {} {:?} {:?}",
                id, obj, key, value, pred, succ
            );
        }
    }
}

#[derive(Debug, Clone)]
struct CounterData {
    pos: usize,
    val: i64,
    succ: HashSet<OpId>,
    op: Op,
}

#[derive(Debug, Clone)]
pub(crate) struct Transaction {
    pub actor: usize,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    pub message: Option<String>,
    pub extra_bytes: Vec<u8>,
    pub hash: Option<ChangeHash>,
    pub deps: Vec<ChangeHash>,
    pub operations: Vec<Op>,
}

#[derive(Debug)]
enum Clock {
    Head,
    At(HashMap<usize, u64>),
}

#[derive(Debug, Clone)]
struct Cursor {
    pos: usize,
    seen: usize,
}

impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for OpId {
    fn default() -> Self {
        OpId(0, 0)
    }
}

impl Default for ObjId {
    fn default() -> Self {
        ObjId(Default::default())
    }
}

impl Default for ElemId {
    fn default() -> Self {
        ElemId(Default::default())
    }
}

fn lamport_cmp(actors: &IndexedCache<amp::ActorId>, left: OpId, right: OpId) -> Ordering {
    match (left, right) {
        (OpId(0, _), OpId(0, _)) => Ordering::Equal,
        (OpId(0, _), OpId(_, _)) => Ordering::Less,
        (OpId(_, _), OpId(0, _)) => Ordering::Greater,
        // FIXME - this one seems backwards to me - why - is values() returning in the wrong order
        (OpId(a, x), OpId(b, y)) if a == b => actors[y].cmp(&actors[x]),
        (OpId(a, _), OpId(b, _)) => a.cmp(&b),
    }
}

fn visible_pos(op: &Op, pos: usize, counters: &HashMap<OpId, CounterData>) -> usize {
    for pred in &op.pred {
        if let Some(entry) = counters.get(pred) {
            return entry.pos;
        }
    }
    pos
}

fn is_visible(op: &Op, pos: usize, counters: &mut HashMap<OpId, CounterData>) -> bool {
    let mut visible = false;
    match op.action {
        amp::OpType::Set(amp::ScalarValue::Counter(val)) => {
            counters.insert(
                op.id,
                CounterData {
                    pos,
                    val,
                    succ: op.succ.iter().cloned().collect(),
                    op: op.clone(),
                },
            );
            if op.succ.is_empty() {
                visible = true;
            }
        }
        amp::OpType::Inc(inc_val) => {
            for id in &op.pred {
                if let Some(mut entry) = counters.get_mut(id) {
                    entry.succ.remove(&op.id);
                    entry.val += inc_val;
                    entry.op.action = amp::OpType::Set(ScalarValue::Counter(entry.val));
                    if entry.succ.is_empty() {
                        visible = true;
                    }
                }
            }
        }
        _ => {
            if op.succ.is_empty() {
                visible = true;
            }
        }
    };
    visible
}

fn visible_op(op: &Op, counters: &HashMap<OpId, CounterData>) -> Op {
    for pred in &op.pred {
        if let Some(entry) = counters.get(pred) {
            return entry.op.clone();
        }
    }
    op.clone()
}

#[cfg(test)]
mod tests {
    use crate::{Automerge, AutomergeError, ObjId, Value, ROOT};
    use automerge_protocol as amp;

    #[test]
    fn insert_op() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        doc.begin()?;
        doc.set(&ROOT, "hello".into(), "world".into())?;
        assert!(doc.pending_ops() == 1);
        doc.commit()?;
        doc.value(&ROOT, "hello".into())?;
        Ok(())
    }

    #[test]
    fn test_list() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        doc.begin()?;
        let list_id: ObjId = doc
            .set(&ROOT, "items".into(), amp::ObjType::List.into())?
            .into();
        doc.set(&ROOT, "zzz".into(), "zzzval".into())?;
        assert!(doc.value(&ROOT, "items".into())?.unwrap().1 == list_id.0);
        doc.insert(&list_id, 0, "a".into())?;
        doc.insert(&list_id, 0, "b".into())?;
        doc.insert(&list_id, 2, "c".into())?;
        doc.insert(&list_id, 1, "d".into())?;
        assert!(doc.value(&list_id, 0.into())?.unwrap().0 == "b".into());
        assert!(doc.value(&list_id, 1.into())?.unwrap().0 == "d".into());
        assert!(doc.value(&list_id, 2.into())?.unwrap().0 == "a".into());
        assert!(doc.value(&list_id, 3.into())?.unwrap().0 == "c".into());
        assert!(doc.length(&list_id) == 4);
        doc.commit()?;
        doc.save()?;
        Ok(())
    }

    #[test]
    fn test_del() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        doc.begin()?;
        doc.set(&ROOT, "xxx".into(), "xxx".into())?;
        assert!(doc.values(&ROOT, "xxx".into())?.len() > 0);
        doc.del(&ROOT, "xxx".into())?;
        assert!(doc.values(&ROOT, "xxx".into())?.len() == 0);
        doc.commit()?;
        Ok(())
    }

    #[test]
    fn test_inc() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.begin()?;
        let id = doc.set(
            &ROOT,
            "counter".into(),
            Value::Scalar(amp::ScalarValue::Counter(10)),
        )?;
        assert!(
            doc.value(&ROOT, "counter".into())?
                == Some((Value::Scalar(amp::ScalarValue::Counter(10)), id))
        );
        doc.inc(&ROOT, "counter".into(), 10)?;
        assert!(
            doc.value(&ROOT, "counter".into())?
                == Some((Value::Scalar(amp::ScalarValue::Counter(20)), id))
        );
        doc.inc(&ROOT, "counter".into(), -5)?;
        assert!(
            doc.value(&ROOT, "counter".into())?
                == Some((Value::Scalar(amp::ScalarValue::Counter(15)), id))
        );
        doc.commit()?;
        Ok(())
    }
}

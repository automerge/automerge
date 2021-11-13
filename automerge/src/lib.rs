
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

use automerge_protocol as amp;
use std::collections::VecDeque;
use change::{encode_document, export_change};
use core::ops::Range;
use error::AutomergeError;
use indexed_cache::IndexedCache;
use nonzero_ext::nonzero;
use protocol::Op;
use sync::BloomFilter;
pub use protocol::{
    ElemId, Export, Exportable, Importable, Key, ObjId, OpId, Value, Peer, HEAD, ROOT, Patch
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

pub use amp::ChangeHash;
pub use change::{ decode_change, Change };

pub use amp::{ActorId, ObjType, ScalarValue};

#[derive(Debug, Clone)]
pub struct Automerge {
    actors: IndexedCache<amp::ActorId>,
    queue: Vec<Change>,
    props: IndexedCache<String>,
    history: Vec<Change>,
    history_index: HashMap<ChangeHash, usize>,
    states: HashMap<usize,Vec<usize>>,
    deps: HashSet<ChangeHash>,
    ops: Vec<Op>,
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

    pub fn get_actor(&self) -> Option<amp::ActorId> {
        self.actor.map(|a| self.actors[a].clone())
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

    pub fn begin(
        &mut self,
        message: Option<String>,
        time: Option<i64>,
    ) -> Result<(), AutomergeError> {
        if self.transaction.is_some() {
            return Err(AutomergeError::MismatchedBegin);
        }

        let actor = self.actor.ok_or(AutomergeError::ActorNotSet)?;

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
                    self.ops.iter_mut().find(|o| o.id == *pred_id).map(|o| o.succ.retain(|i| i != pred_id));
                }
                if let Some(pos) = self.ops.iter().position(|o| o.id == op.id) {
                    self.ops.remove(pos);
                }
            }
        }
    }

    fn lamport_cmp(&self, left: OpId, right: OpId) -> Ordering {
        match (left, right) {
            (OpId(0, _), OpId(0, _)) => Ordering::Equal,
            (OpId(0, _), OpId(_, _)) => Ordering::Less,
            (OpId(_, _), OpId(0, _)) => Ordering::Greater,
            (OpId(a, x), OpId(b, y)) if a == b => self.actors[x].cmp(&self.actors[y]),
            (OpId(a, _), OpId(b, _)) => a.cmp(&b),
        }
    }

    pub fn prop_to_key(&mut self, prop: String) -> Result<Key,AutomergeError> {
        if prop == "" {
            return Err(AutomergeError::EmptyStringKey)
        }
        Ok(Key::Map(self.props.cache(prop)))
    }

    fn key_cmp(&self, left: &Key, right: &Key) -> Option<Ordering> {
        match (left, right) {
            (Key::Map(a), Key::Map(b)) => Some(self.props[*a].cmp(&self.props[*b])),
            _ => None,
        }
    }

    fn make_op(
        &mut self,
        obj: ObjId,
        key: Key,
        action: amp::OpType,
        insert: bool,
    ) -> Result<OpId, AutomergeError> {
        if let Some(mut tx) = self.transaction.take() {
            let id = OpId(tx.start_op + tx.operations.len() as u64, 0);
            let op = Op {
                change: self.history.len(),
                id,
                action,
                obj,
                key,
                succ: vec![],
                pred: vec![],
                insert,
            };
            self.insert_op(op.clone(), true);
            tx.operations.push(op);
            self.transaction = Some(tx);
            Ok(id)
        } else {
            Err(AutomergeError::OpOutsideOfTransaction)
        }
    }

    fn scan_to_obj(&self, obj: &ObjId, pos: &mut usize) {
        while *pos < self.ops.len()
            && self.lamport_cmp(obj.0, self.ops[*pos].obj.0) == Ordering::Greater
        {
            *pos += 1
        }
    }

    fn scan_to_prop_start(&self, obj: &ObjId, key: &Key, pos: &mut usize) {
        while *pos < self.ops.len()
            && &self.ops[*pos].obj == obj
            && self.key_cmp(key, &self.ops[*pos].key) == Some(Ordering::Greater)
        {
            *pos += 1
        }
    }

    fn scan_to_visible(&self, obj: &ObjId, pos: &mut usize) {
        while *pos < self.ops.len() && &self.ops[*pos].obj == obj && !self.ops[*pos].visible() {
            *pos += 1
        }
    }

    fn scan_to_nth_visible(&self, obj: &ObjId, n: usize, pos: &mut usize) -> Vec<&Op> {
        let mut seen = 0;
        let mut seen_visible = false;
        let mut result = vec![];
        while *pos < self.ops.len() {
            let op = &self.ops[*pos];
            if &op.obj != obj {
                break;
            }
            if op.insert {
                seen_visible = false;
            }
            if op.visible() && !seen_visible {
                seen += 1;
                seen_visible = true;
            }
            if seen == n + 1 {
                result.push(op)
            }
            if seen > n + 1 {
                break
            }
            *pos += 1;
        }
        result
    }

    fn scan_visible(&self, obj: &ObjId, pos: &mut usize) -> usize {
        let mut seen = 0;
        let mut seen_visible = false;
        while *pos < self.ops.len() && &self.ops[*pos].obj == obj {
            let op = &self.ops[*pos];
            if op.insert {
                seen_visible = false;
            }
            if op.visible() && !seen_visible {
                seen += 1;
                seen_visible = true;
            }
            *pos += 1;
        }
        seen
    }

    fn scan_to_next_prop(&self, obj: &ObjId, key: &Key, pos: &mut usize) {
        while *pos < self.ops.len() && &self.ops[*pos].obj == obj && &self.ops[*pos].key == key {
            *pos += 1
        }
    }

    fn scan_to_next_visible_prop(&self, obj: &ObjId, key: &Key, pos: &mut usize) {
        self.scan_to_next_prop(obj, key, pos);
        self.scan_to_visible(obj, pos);
    }

    fn scan_to_prop_insertion_point(&mut self, op: &mut Op, local: bool, pos: &mut usize) {
        while *pos < self.ops.len()
            && self.ops[*pos].obj == op.obj
            && self.ops[*pos].key == op.key
            && self.lamport_cmp(op.id, self.ops[*pos].id) == Ordering::Greater
        {
            if local {
                if self.ops[*pos].visible() {
                    self.ops[*pos].succ.push(op.id);
                    op.pred.push(self.ops[*pos].id);
                }
            } else if op.pred.iter().any(|i| i == &op.id) {
                self.ops[*pos].succ.push(op.id);
            }
            *pos += 1
        }
    }

    fn scan_to_prop_value(&self, obj: &ObjId, key: &Key, _clock: &Clock, pos: &mut usize) -> Vec<&Op> {
        let mut result = vec![];
        while *pos < self.ops.len()
            && &self.ops[*pos].obj == obj
            && &self.ops[*pos].key == key
        {
            if self.ops[*pos].visible() {
                result.push(&self.ops[*pos])
            }
            *pos += 1
        }
        result
    }

    fn scan_to_elem_insert_op1(&self, op: &Op, elem: &ElemId, pos: &mut usize, seen: &mut usize) {
        if *elem == HEAD {
            return;
        }

        let mut seen_key = None;

        while *pos < self.ops.len() && self.ops[*pos].obj == op.obj {
            let i = &self.ops[*pos];
            if i.visible() && i.elemid() != seen_key {
                *seen += 1;
                seen_key = i.elemid(); // only count each elemid once
            }

            *pos += 1;

            if i.insert && i.id == elem.0 {
                break;
            }
        }
    }

    fn scan_to_elem_insert_op2(&self, op: &Op, elem: &ElemId, pos: &mut usize, seen: &mut usize) {
        if *elem == HEAD {
            return;
        }

        let mut seen_key = None;

        while *pos < self.ops.len() && self.ops[*pos].obj == op.obj {
            let i = &self.ops[*pos];
            if i.visible() && i.elemid() != seen_key {
                *seen += 1;
                seen_key = i.elemid(); // only count each elemid once
            }

            if i.insert && i.id == elem.0 {
                break;
            }

            *pos += 1;
        }
    }

    fn scan_to_elem_update_pos(
        &mut self,
        op: &mut Op,
        local: bool,
        pos: &mut usize,
    ) {
        while *pos < self.ops.len()
            && self.ops[*pos].obj == op.obj
            && self.ops[*pos].elemid() == op.elemid()
            && self.lamport_cmp(op.id, self.ops[*pos].id) == Ordering::Greater
        {
            if local {
                if self.ops[*pos].visible() && self.ops[*pos].elemid() == op.elemid() {
                    self.ops[*pos].succ.push(op.id);
                    op.pred.push(self.ops[*pos].id);
                }
            } else if self.ops[*pos].visible()
                && self.ops[*pos].elemid() == op.elemid()
                && op.pred.iter().any(|i| i == &op.id)
            {
                self.ops[*pos].succ.push(op.id);
            }
            *pos += 1
        }
    }

    fn scan_to_lesser_insert(&self, op: &Op, pos: &mut usize, seen: &mut usize) {
        let mut seen_key = None;

        while *pos < self.ops.len() && self.ops[*pos].obj == op.obj {
            let i = &self.ops[*pos];

            if i.visible() && i.elemid() != seen_key {
                *seen += 1;
                seen_key = i.elemid(); // only count each elemid once
            }

            if op.insert && self.lamport_cmp(op.id, self.ops[*pos].id) == Ordering::Greater {
                break;
            }

            *pos += 1
        }
    }

    fn seek_to_update_elem(&mut self, op: &mut Op, elem: &ElemId, local: bool) -> Cursor {
        let mut pos = 0;
        let mut seen = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op2(op, elem, &mut pos, &mut seen);
        self.scan_to_elem_update_pos(op, local, &mut pos);
        Cursor { pos, seen }
    }

    fn seek_to_insert_elem(&self, op: &Op, elem: &ElemId) -> Cursor {
        let mut pos = 0;
        let mut seen = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op1(op, elem, &mut pos, &mut seen);
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

    fn insert_op(&mut self, mut op: Op, local: bool) {
        let cursor = self.seek_to_op(&mut op, local); //mut to collect pred
        self.ops.insert(cursor.pos, op);
    }

    pub fn keys(&self, obj: &ObjId) -> Vec<Key> {
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
            result.push(*key);
            self.scan_to_next_visible_prop(obj, key, &mut pos);
        }
        result
    }

    pub fn map_value(&self, obj: &ObjId, prop: &str) -> Vec<Value> {
        self.map_value_at(obj, prop, &[])
    }

    pub fn map_value_at(&self, obj: &ObjId, prop: &str, heads: &[amp::ChangeHash]) -> Vec<Value> {
        let clock = self.clock_at(heads);
        let mut pos = 0;
        let prop = self.props.lookup(prop.to_owned());
        if prop.is_none() {
            return vec![]
        }
        let prop = Key::Map(prop.unwrap());
        self.scan_to_obj(obj, &mut pos);
        self.scan_to_prop_start(obj, &prop, &mut pos);
        let ops = self.scan_to_prop_value(obj, &prop, &clock, &mut pos);
        ops.into_iter().map(|o| o.into()).collect()
    }

    pub fn list_value(&self, obj: &ObjId, index: usize) -> Vec<Value> {
        let mut pos = 0;
        self.scan_to_obj(obj, &mut pos);
        let ops = self.scan_to_nth_visible(obj, index, &mut pos);
        ops.into_iter().map(|o| o.into()).collect()
    }

    pub fn list_length(&self, obj: &ObjId) -> usize {
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

    pub fn set_pos_for_index(&self, obj: &ObjId, index: usize) -> Option<Key> {
        let mut pos = 0;
        self.scan_to_obj(obj, &mut pos);
        let ops = self.scan_to_nth_visible(obj, index, &mut pos);
        ops.get(0).and_then(|o| o.elemid()).map(|e| e.into())
    }

    pub fn make(
        &mut self,
        obj: ObjId,
        key: Key,
        obj_type: amp::ObjType,
        insert: bool,
    ) -> Result<ObjId, AutomergeError> {
        Ok(ObjId(self.make_op(
            obj,
            key,
            amp::OpType::Make(obj_type),
            insert,
        )?))
    }

    /*
    pub fn map_make(
        &mut self,
        obj: ObjId,
        prop: &str,
        obj_type: amp::ObjType,
    ) -> Result<ObjId, AutomergeError> {
        let key = self.prop_to_key(prop.into())?;
        Ok(ObjId(self.make_op(
            obj,
            key,
            amp::OpType::Make(obj_type),
            false,
        )?))
    }
    */

    pub fn map_set(
        &mut self,
        obj: ObjId,
        prop: &str,
        value: amp::ScalarValue,
    ) -> Result<OpId, AutomergeError> {
        let key = self.prop_to_key(prop.into())?;
        self.make_op(obj, key, amp::OpType::Set(value), false)
    }

    pub fn set(
        &mut self,
        obj: ObjId,
        key: Key,
        value: amp::ScalarValue,
        insert: bool,
    ) -> Result<OpId, AutomergeError> {
        self.make_op(obj, key, amp::OpType::Set(value), insert)
    }

    pub fn set_at(
        &mut self,
        obj: ObjId,
        index: usize,
        value: amp::ScalarValue,
    ) -> Result<OpId, AutomergeError> {
        if let Some(key) = self.set_pos_for_index(&obj, index) {
            self.make_op(obj, key, amp::OpType::Set(value), false)
        } else {
            Err(AutomergeError::InvalidListAt(self.export(obj), index))
        }
    }

    pub fn insert_at(
        &mut self,
        obj: ObjId,
        index: usize,
        value: amp::ScalarValue,
    ) -> Result<OpId, AutomergeError> {
        if let Some(key) = self.insert_pos_for_index(&obj, index) {
            self.make_op(obj, key, amp::OpType::Set(value), true)
        } else {
            Err(AutomergeError::InvalidListAt(self.export(obj), index))
        }
    }

    pub fn inc(&mut self, _obj: ObjId, _key: Key, _value: i64) -> Result<(), AutomergeError> {
        unimplemented!()
    }

    pub fn del(&mut self, obj: ObjId, key: Key) -> Result<(), AutomergeError> {
        self.make_op(obj, key, amp::OpType::Del(nonzero!(1_u32)), false)?;
        Ok(())
    }

    pub fn splice(&mut self, _path: &str, _range: Range<usize>, _replace: Vec<amp::ScalarValue>) {
        unimplemented!()
    }

    pub fn text(&self, _path: &str) -> String {
        unimplemented!()
    }

    pub fn value(&self, _path: &str) -> Value {
        unimplemented!()
    }

    pub fn load(_data: &[u8]) -> Self {
        unimplemented!()
    }

    pub fn apply_changes(&mut self, changes: &[Change]) -> Result<Patch, AutomergeError> {
        for c in changes {
            self.apply_change(c.clone())
        }
        Ok(Patch {})
    }

    pub fn apply_change(&mut self, change: Change) {
        let ops = self.import_ops(&change, self.history.len());
        self.update_history(change);
        for op in ops {
            self.insert_op(op, false)
        }
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
                    amp::Key::Seq(amp::ElementId::Id(i)) => Key::Seq(self.import(&i.to_string()).unwrap()),
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
        encode_document(&c, &self.ops, &self.actors, &self.props.cache)
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
            return self.history.iter().rev().find(|c| c.actor_id() == actor)
        }
        return None
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
            return Clock::Head
        }
        // FIXME - could be way faster
        let changes = self.get_changes(heads);
        let mut clock = HashMap::new();
        for c in changes {
            let actor = self.actors.lookup(c.actor_id().clone()).unwrap();
            if let Some(val) = clock.get(&actor) {
                if val < &c.seq {
                    clock.insert(actor,c.seq);
                }
            } else {
                clock.insert(actor,c.seq);
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
    At(HashMap<usize,u64>),
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

#[cfg(test)]
mod tests {
    use crate::{Automerge, Key, HEAD, ROOT};
    use automerge_protocol as amp;

    #[test]
    fn insert_op() {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        doc.begin(None, None).unwrap();
        let key = doc.prop_to_key("hello".into()).unwrap();
        doc.set(ROOT, key, "world".into(), false).unwrap();
        //doc.map_set(ROOT, "&hello", "world".into()).unwrap();
        assert!(doc.pending_ops() == 1);
        doc.commit().unwrap();
        doc.map_value(&ROOT, "hello").get(0).unwrap();
    }

    #[test]
    fn test_list() {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        doc.begin(None, None).unwrap();
        let key = doc.prop_to_key("items".into()).unwrap();
        let list_id = doc.make(ROOT, key, amp::ObjType::List, false).unwrap();
        doc.map_set(ROOT, "zzz", "zzzval".into()).unwrap();
        assert!(doc.map_value(&ROOT, "items")[0].to_obj_id() ==Some(list_id));
        let aid = doc.set(list_id, Key::Seq(HEAD), "a".into(), true).unwrap();
        let bid = doc.set(list_id, HEAD.into(), "b".into(), true).unwrap();
        doc.set(list_id, aid.into(), "c".into(), true).unwrap();
        doc.set(list_id, bid.into(), "d".into(), true).unwrap();
        //doc.dump2();
        //println!("0 {:?}",doc.list_value(&list_id, 0));
        //println!("1 {:?}",doc.list_value(&list_id, 1));
        // FIXME - this is terrible
        assert!(doc.list_value(&list_id, 0)[0].to_string() == Some("\"b\"".into()));
        assert!(doc.list_value(&list_id, 1)[0].to_string() == Some("\"d\"".into()));
        assert!(doc.list_value(&list_id, 2)[0].to_string() == Some("\"a\"".into()));
        assert!(doc.list_value(&list_id, 3)[0].to_string() == Some("\"c\"".into()));
        assert!(doc.list_length(&list_id) == 4);
        doc.commit().unwrap();
        doc.save().unwrap();
    }
}

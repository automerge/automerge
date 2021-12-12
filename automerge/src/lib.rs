extern crate hex;
extern crate uuid;
extern crate web_sys;

macro_rules! log {
     ( $( $t:tt )* ) => {
          use $crate::__log;
          __log!( $( $t )* );
     }
 }

#[cfg(target_family = "wasm")]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         web_sys::console::log_1(&format!( $( $t )* ).into());
     }
 }

#[cfg(not(target_family = "wasm"))]
#[macro_export]
macro_rules! __log {
     ( $( $t:tt )* ) => {
         println!( $( $t )* );
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
mod op_tree;
mod query;
mod types;

use op_tree::OpTree;

use automerge_protocol as amp;
use change::{encode_document, export_change};
pub use error::AutomergeError;
use indexed_cache::IndexedCache;
use nonzero_ext::nonzero;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use sync::BloomFilter;
use types::Op;
pub use types::{
    ElemId, Export, Exportable, Importable, Key, ObjId, OpId, Patch, Peer, Prop, Value, HEAD, ROOT,
};
use unicode_segmentation::UnicodeSegmentation;

pub use amp::ChangeHash;
pub use change::{decode_change, Change};
pub use sync::{SyncMessage, SyncState};

pub use amp::{ActorId, ObjType, ScalarValue};

#[derive(Debug, Clone)]
pub struct Automerge {
    queue: Vec<Change>,
    history: Vec<Change>,
    history_index: HashMap<ChangeHash, usize>,
    states: HashMap<usize, Vec<usize>>,
    deps: HashSet<ChangeHash>,
    saved: Vec<ChangeHash>,
    ops: OpTree,
    actor: Option<usize>,
    max_op: u64,
    transaction: Option<Transaction>,
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
            actor: None,
            max_op: 0,
            transaction: None,
        }
    }

    pub fn set_actor(&mut self, actor: amp::ActorId) {
        self.ensure_transaction_closed();
        self.actor = Some(self.ops.m.actors.cache(actor))
    }

    fn random_actor(&mut self) -> amp::ActorId {
        let actor = amp::ActorId::from(uuid::Uuid::new_v4().as_bytes().to_vec());
        self.actor = Some(self.ops.m.actors.cache(actor.clone()));
        actor
    }

    pub fn get_actor(&mut self) -> amp::ActorId {
        if let Some(actor) = self.actor {
            self.ops.m.actors[actor].clone()
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
            queue: vec![],
            history: vec![],
            history_index: HashMap::new(),
            states: HashMap::new(),
            ops: OpTree::with_actor(actor),
            deps: Default::default(),
            saved: Default::default(),
            actor: None,
            max_op: 0,
            transaction: None,
        }
    }

    pub fn pending_ops(&self) -> u64 {
        self.transaction
            .as_ref()
            .map(|t| t.operations.len() as u64)
            .unwrap_or(0)
    }

    fn tx(&mut self) -> &mut Transaction {
        if self.transaction.is_none() {
            let actor = self.get_actor_index();

            let seq = self.states.entry(actor).or_default().len() as u64 + 1;
            let mut deps = self.get_heads();
            if seq > 1 {
                let last_hash = self.get_hash(actor, seq - 1).unwrap();
                if !deps.contains(&last_hash) {
                    deps.push(last_hash);
                }
            }

            self.transaction = Some(Transaction {
                actor,
                seq,
                start_op: self.max_op + 1,
                time: 0,
                message: None,
                extra_bytes: Default::default(),
                hash: None,
                operations: vec![],
                deps,
            });
        }

        self.transaction.as_mut().unwrap()
    }

    /*
        pub fn begin(&mut self) -> Result<(), AutomergeError> {
            unimplemented!()
            //self.begin_with_opts(None, None)
        }
    */
    /*
     */

    /*
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
    */

    pub fn commit(&mut self, message: Option<String>, time: Option<i64>) -> usize {
        let tx = self.tx();

        if message.is_some() {
            tx.message = message;
        }

        if let Some(t) = time {
            tx.time = t;
        }

        let ops = tx.operations.len();

        self.ensure_transaction_closed();

        ops
    }

    pub fn ensure_transaction_closed(&mut self) {
        if let Some(tx) = self.transaction.take() {
            self.update_history(export_change(&tx, &self.ops.m.actors, &self.ops.m.props));
        }
    }

    pub fn rollback(&mut self) -> usize {
        if let Some(tx) = self.transaction.take() {
            let num = tx.operations.len();
            for op in &tx.operations {
                // FIXME - use query to make this fast
                for pred_id in &op.pred {
                    if let Some(p) = self.ops.iter().position(|o| o.id == *pred_id) {
                        self.ops.replace(p, |o| o.succ.retain(|i| i != pred_id));
                    }
                }
                if let Some(pos) = self.ops.iter().position(|o| o.id == op.id) {
                    self.ops.remove(pos);
                }
            }
            num
        } else {
            0
        }
    }

    fn next_id(&mut self) -> OpId {
        let tx = self.tx();
        OpId(tx.start_op + tx.operations.len() as u64, tx.actor)
    }

    fn insert_local_op(&mut self, op: Op, pos: usize, succ_pos: &[usize]) {
        for succ in succ_pos {
            self.ops.replace(*succ, |old_op| {
                old_op.succ.push(op.id);
            });
        }

        if !op.is_del() {
            self.ops.insert(pos, op.clone());
        }

        self.tx().operations.push(op);
    }

    fn scan_to_obj(&self, obj: &ObjId, pos: &mut usize) {
        for op in self.ops.iter().skip(*pos) {
            if lamport_cmp(&self.ops.m.actors, obj.0, op.obj.0) != Ordering::Greater {
                break;
            }
            *pos += 1;
        }
    }

    fn scan_to_prop_start(&self, obj: &ObjId, key: &Key, pos: &mut usize) {
        for op in self.ops.iter().skip(*pos) {
            if &op.obj != obj || key_cmp(key, &op.key, &self.ops.m.props) != Ordering::Greater {
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
                && lamport_cmp(&self.ops.m.actors, next.id, op.id) == Ordering::Greater)
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
            self.ops.replace(vpos, |op| {
                op.succ.push(next.id);
                if local {
                    next.pred.push(op.id);
                }
            });
        }
    }

    fn scan_to_elem_insert_op1(&self, obj: &ObjId, elem: &ElemId, pos: &mut usize) {
        if *elem == HEAD {
            return;
        }

        for op in self.ops.iter().skip(*pos) {
            if &op.obj != obj {
                break;
            }

            *pos += 1;

            if op.insert && op.id == elem.0 {
                break;
            }
        }
    }

    fn scan_to_elem_insert_op2(&self, obj: &ObjId, elem: &ElemId, pos: &mut usize) {
        for op in self.ops.iter().skip(*pos) {
            if &op.obj != obj {
                break;
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
                && lamport_cmp(&self.ops.m.actors, next.id, op.id) == Ordering::Greater)
            {
                break;
            }
            if local {
                if op.elemid() == next.elemid() && is_visible(op, *pos, &mut counters) {
                    succ.push((true, visible_pos(op, *pos, &counters)));
                }
            } else if op.elemid() == next.elemid() && next.pred.iter().any(|i| i == &op.id) {
                succ.push((false, *pos));
            }
            *pos += 1
        }

        for (local, vpos) in succ {
            if let Some(op) = self.ops.get(vpos) {
                let mut op = op.clone();
                op.succ.push(next.id);
                if local {
                    next.pred.push(op.id);
                }
                self.ops.set(vpos, op);
            }
        }
    }

    fn scan_to_lesser_insert(&self, next: &Op, pos: &mut usize) {
        for op in self.ops.iter().skip(*pos) {
            if op.obj != next.obj {
                break;
            }

            if next.insert && lamport_cmp(&self.ops.m.actors, next.id, op.id) == Ordering::Greater {
                break;
            }

            *pos += 1
        }
    }

    fn seek_to_update_elem(
        &mut self,
        op: &mut Op,
        elem: &ElemId,
        local: bool,
        mut pos: usize,
    ) -> usize {
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op2(&op.obj, elem, &mut pos);
        self.scan_to_elem_update_pos(op, local, &mut pos);
        pos
    }

    fn seek_to_insert_elem(&self, op: &Op, elem: &ElemId, mut pos: usize) -> usize {
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op1(&op.obj, elem, &mut pos);
        self.scan_to_lesser_insert(op, &mut pos);
        pos
    }

    fn seek_to_map_op(&mut self, op: &mut Op, local: bool) -> usize {
        let mut pos = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_prop_start(&op.obj, &op.key, &mut pos);
        self.scan_to_prop_insertion_point(op, local, &mut pos);
        pos
    }

    fn seek_to_op(&mut self, op: &mut Op, local: bool) -> usize {
        match (op.key, op.insert) {
            (Key::Map(_), _) => self.seek_to_map_op(op, local),
            (Key::Seq(elem), true) => self.seek_to_insert_elem(op, &elem, 0),
            (Key::Seq(elem), false) => self.seek_to_update_elem(op, &elem, local, 0),
        }
    }

    fn insert_op(&mut self, mut op: Op, local: bool) -> Op {
        // TODO - write a fast query
        let pos = self.seek_to_op(&mut op, local); //mut to collect pred
        if !op.is_del() {
            self.ops.insert(pos, op.clone());
        }
        op
    }

    pub fn keys(&self, obj: ObjId) -> Vec<String> {
        // TODO - use index, add _at(clock)
        let mut pos = 0;
        let mut result = vec![];
        self.scan_to_obj(&obj, &mut pos);
        self.scan_to_visible(&obj, &mut pos);
        while let Some(op) = self.ops.get(pos) {
            // we reached the next object
            if op.obj != obj {
                break;
            }
            let key = &op.key;
            result.push(self.export(*key));
            self.scan_to_next_prop(&obj, key, &mut pos);
            self.scan_to_visible(&obj, &mut pos);
        }
        result
    }

    pub fn length(&self, obj: ObjId) -> usize {
        // TODO - use index
        // add _at(clock)
        self.ops.list_len(&obj)
    }

    // set(obj, prop, value) - value can be scalar or objtype
    // del(obj, prop)
    // inc(obj, prop, value)
    // insert(obj, index, value)

    pub fn set(&mut self, obj: ObjId, prop: Prop, value: Value) -> Result<OpId, AutomergeError> {
        self.local_op(obj, prop, value.into())
    }

    pub fn insert(
        &mut self,
        obj: ObjId,
        index: usize,
        value: Value,
    ) -> Result<OpId, AutomergeError> {
        let id = self.next_id();

        let query = self.ops.search(query::InsertNth::new(obj, index));

        let key = query.key()?;

        let op = Op {
            change: self.history.len(),
            id,
            action: value.into(),
            obj,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        };

        self.insert_local_op(op, query.pos, &[]);

        Ok(id)
    }

    pub fn inc(&mut self, obj: ObjId, prop: Prop, value: i64) -> Result<OpId, AutomergeError> {
        self.local_op(obj, prop, amp::OpType::Inc(value))
    }

    pub fn del(&mut self, obj: ObjId, prop: Prop) -> Result<OpId, AutomergeError> {
        self.local_op(obj, prop, amp::OpType::Del(nonzero!(1_u32)))
    }

    pub fn splice(
        &mut self,
        obj: ObjId,
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

    pub fn splice_text(
        &mut self,
        obj: ObjId,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        let mut vals = vec![];
        for c in text.to_owned().graphemes(true) {
            vals.push(c.into());
        }
        self.splice(obj, pos, del, vals)
    }

    pub fn text(&self, obj: ObjId) -> Result<String, AutomergeError> {
        let query = self.ops.search(query::ListVals::new(obj));
        let mut buffer = String::new();
        for q in &query.ops {
            if let amp::OpType::Set(amp::ScalarValue::Str(s)) = &q.action {
                buffer.push_str(s);
            }
        }
        Ok(buffer)
    }

    // TODO - I need to return these OpId's here **only** to get
    // the legacy conflicts format of { [opid]: value }
    // Something better?
    pub fn value(&self, obj: ObjId, prop: Prop) -> Result<Option<(Value, OpId)>, AutomergeError> {
        Ok(self.values_at(obj, prop, &[])?.first().cloned())
    }

    pub fn value_at(
        &self,
        obj: ObjId,
        prop: Prop,
        heads: &[ChangeHash],
    ) -> Result<Option<(Value, OpId)>, AutomergeError> {
        Ok(self.values_at(obj, prop, heads)?.first().cloned())
    }

    pub fn values(&self, obj: ObjId, prop: Prop) -> Result<Vec<(Value, OpId)>, AutomergeError> {
        self.values_at(obj, prop, &[])
    }

    pub fn values_at(
        &self,
        obj: ObjId,
        prop: Prop,
        heads: &[ChangeHash],
    ) -> Result<Vec<(Value, OpId)>, AutomergeError> {
        let _clock = self.clock_at(heads);
        let result = match prop {
            Prop::Map(p) => {
                let prop = self.ops.m.props.lookup(p);
                if let Some(p) = prop {
                    self.ops
                        .search(query::Prop::new(obj, p))
                        .ops
                        .into_iter()
                        .map(|o| o.into())
                        .collect()
                } else {
                    vec![]
                }
            }
            Prop::Seq(n) => self
                .ops
                .search(query::Nth::new(obj, n))
                .ops
                .into_iter()
                .map(|o| o.into())
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

    pub fn apply_changes(&mut self, changes: &[Change]) -> Result<Patch, AutomergeError> {
        self.ensure_transaction_closed();
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
        self.ensure_transaction_closed();
        let ops = self.import_ops(&change, self.history.len());
        self.update_history(change);
        for op in ops {
            self.insert_op(op, false);
        }
    }

    fn local_op(
        &mut self,
        obj: ObjId,
        prop: Prop,
        action: amp::OpType,
    ) -> Result<OpId, AutomergeError> {
        match prop {
            Prop::Map(s) => self.local_map_op(obj, s, action),
            Prop::Seq(n) => self.local_list_op(obj, n, action),
        }
    }

    fn local_map_op(
        &mut self,
        obj: ObjId,
        prop: String,
        action: amp::OpType,
    ) -> Result<OpId, AutomergeError> {
        if prop.is_empty() {
            return Err(AutomergeError::EmptyStringKey);
        }

        let id = self.next_id();
        let prop = self.ops.m.props.cache(prop);
        let query = self.ops.search(query::Prop::new(obj, prop));

        let pred = query.ops.iter().map(|op| op.id).collect();

        let op = Op {
            change: self.history.len(),
            id,
            action,
            obj,
            key: Key::Map(prop),
            succ: Default::default(),
            pred,
            insert: false,
        };

        self.insert_local_op(op, query.pos, &query.ops_pos);

        Ok(id)
    }

    fn local_list_op(
        &mut self,
        obj: ObjId,
        index: usize,
        action: amp::OpType,
    ) -> Result<OpId, AutomergeError> {
        let query = self.ops.search(query::Nth::new(obj, index));

        let id = self.next_id();
        let pred = query.ops.iter().map(|op| op.id).collect();
        let key = query.key()?;

        let op = Op {
            change: self.history.len(),
            id,
            action,
            obj,
            key,
            succ: Default::default(),
            pred,
            insert: false,
        };

        self.insert_local_op(op, query.pos, &query.ops_pos);

        Ok(id)
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
                // FIXME dont need to_string()
                let obj: ObjId = self.import(&c.obj.to_string()).unwrap();
                let pred = c
                    .pred
                    .iter()
                    .map(|i| self.import(&i.to_string()).unwrap())
                    .collect();
                let key = match &c.key.as_ref() {
                    amp::Key::Map(n) => Key::Map(self.ops.m.props.cache(n.to_string())),
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
                    succ: Default::default(),
                    pred,
                    insert: c.insert,
                }
            })
            .collect()
    }

    pub fn save(&mut self) -> Result<Vec<u8>, AutomergeError> {
        self.ensure_transaction_closed();
        // TODO - would be nice if I could pass an iterator instead of a collection here
        let c: Vec<_> = self.history.iter().map(|c| c.decode()).collect();
        let ops: Vec<_> = self.ops.iter().cloned().collect();
        // TODO - can we make encode_document error free
        let bytes = encode_document(
            &c,
            ops.as_slice(),
            &self.ops.m.actors,
            &self.ops.m.props.cache,
        );
        if bytes.is_ok() {
            self.saved = self.get_heads().iter().copied().collect();
        }
        bytes
    }

    // should this return an empty vec instead of None?
    pub fn save_incremental(&mut self) -> Option<Vec<u8>> {
        self.ensure_transaction_closed();
        let changes = self._get_changes(self.saved.as_slice());
        let mut bytes = vec![];
        for c in changes {
            bytes.extend(c.raw_bytes());
        }
        if !bytes.is_empty() {
            self.saved = self._get_heads().iter().copied().collect();
            Some(bytes)
        } else {
            None
        }
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

    pub fn get_missing_deps(&mut self, heads: &[ChangeHash]) -> Vec<amp::ChangeHash> {
        self.ensure_transaction_closed();
        self._get_missing_deps(heads)
    }

    fn _get_missing_deps(&self, heads: &[ChangeHash]) -> Vec<amp::ChangeHash> {
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
        if self._get_heads().iter().all(|h| has_seen.contains(h)) {
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

    pub fn get_last_local_change(&mut self) -> Option<&Change> {
        self.ensure_transaction_closed();
        if let Some(actor) = &self.actor {
            let actor = &self.ops.m.actors[*actor];
            return self.history.iter().rev().find(|c| c.actor_id() == actor);
        }
        None
    }

    pub fn get_changes(&mut self, have_deps: &[ChangeHash]) -> Vec<&Change> {
        self.ensure_transaction_closed();
        self._get_changes(have_deps)
    }

    fn _get_changes(&self, have_deps: &[ChangeHash]) -> Vec<&Change> {
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
        for c in self._get_changes(heads) {
            let actor = self.ops.m.actors.lookup(c.actor_id().clone()).unwrap();
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

    pub fn get_change_by_hash(&mut self, hash: &amp::ChangeHash) -> Option<&Change> {
        self.ensure_transaction_closed();
        self._get_change_by_hash(hash)
    }

    fn _get_change_by_hash(&self, hash: &amp::ChangeHash) -> Option<&Change> {
        self.history_index
            .get(hash)
            .and_then(|index| self.history.get(*index))
    }

    pub fn get_changes_added<'a>(&mut self, other: &'a Self) -> Vec<&'a Change> {
        self.ensure_transaction_closed();
        self._get_changes_added(other)
    }

    fn _get_changes_added<'a>(&self, other: &'a Self) -> Vec<&'a Change> {
        // Depth-first traversal from the heads through the dependency graph,
        // until we reach a change that is already present in other
        let mut stack: Vec<_> = other._get_heads();
        let mut seen_hashes = HashSet::new();
        let mut added_change_hashes = Vec::new();
        while let Some(hash) = stack.pop() {
            if !seen_hashes.contains(&hash) && self._get_change_by_hash(&hash).is_none() {
                seen_hashes.insert(hash);
                added_change_hashes.push(hash);
                if let Some(change) = other._get_change_by_hash(&hash) {
                    stack.extend(&change.deps);
                }
            }
        }
        // Return those changes in the reverse of the order in which the depth-first search
        // found them. This is not necessarily a topological sort, but should usually be close.
        added_change_hashes.reverse();
        added_change_hashes
            .into_iter()
            .filter_map(|h| other._get_change_by_hash(&h))
            .collect()
    }

    pub fn get_heads(&mut self) -> Vec<ChangeHash> {
        self.ensure_transaction_closed();
        self._get_heads()
    }

    fn _get_heads(&self) -> Vec<ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    fn get_hash(&mut self, actor: usize, seq: u64) -> Result<amp::ChangeHash, AutomergeError> {
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
                .ops
                .m
                .actors
                .lookup(actor)
                .ok_or_else(|| AutomergeError::InvalidOpId(s.to_owned()))?;
            Ok(I::wrap(OpId(counter, actor)))
        }
    }

    pub fn export<E: Exportable>(&self, id: E) -> String {
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
            let id = self.export(i.id);
            let obj = self.export(i.obj);
            let key = match i.key {
                Key::Map(n) => self.ops.m.props[n].clone(),
                Key::Seq(n) => self.export(n),
            };
            let value: String = match &i.action {
                amp::OpType::Set(value) => format!("{}", value),
                amp::OpType::Make(obj) => format!("make{}", obj),
                amp::OpType::Inc(obj) => format!("inc{}", obj),
                amp::OpType::Del(_) => format!("del{}", 0),
                amp::OpType::MultiSet(_) => format!("multiset{}", 0),
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

impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn key_cmp(left: &Key, right: &Key, props: &IndexedCache<String>) -> Ordering {
    match (left, right) {
        (Key::Map(a), Key::Map(b)) => props[*a].cmp(&props[*b]),
        _ => panic!("can only compare map keys"),
    }
}

pub(crate) fn lamport_cmp(
    actors: &IndexedCache<amp::ActorId>,
    left: OpId,
    right: OpId,
) -> Ordering {
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

/*
fn visible_op(op: &Op, counters: &HashMap<OpId, CounterData>) -> Op {
    for pred in &op.pred {
        // FIXME - delete a counter? - entry.succ.empty()?
        if let Some(entry) = counters.get(pred) {
            return entry.op.clone();
        }
    }
    op.clone()
}
*/

#[cfg(test)]
mod tests {
    use crate::{Automerge, AutomergeError, ObjId, Value, ROOT};
    use automerge_protocol as amp;

    #[test]
    fn insert_op() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        doc.set(ROOT, "hello".into(), "world".into())?;
        assert!(doc.pending_ops() == 1);
        doc.value(ROOT, "hello".into())?;
        Ok(())
    }

    #[test]
    fn test_list() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        let list_id: ObjId = doc
            .set(ROOT, "items".into(), amp::ObjType::List.into())?
            .into();
        doc.set(ROOT, "zzz".into(), "zzzval".into())?;
        assert!(doc.value(ROOT, "items".into())?.unwrap().1 == list_id.0);
        doc.insert(list_id, 0, "a".into())?;
        doc.insert(list_id, 0, "b".into())?;
        doc.insert(list_id, 2, "c".into())?;
        doc.insert(list_id, 1, "d".into())?;
        assert!(doc.value(list_id, 0.into())?.unwrap().0 == "b".into());
        assert!(doc.value(list_id, 1.into())?.unwrap().0 == "d".into());
        assert!(doc.value(list_id, 2.into())?.unwrap().0 == "a".into());
        assert!(doc.value(list_id, 3.into())?.unwrap().0 == "c".into());
        assert!(doc.length(list_id) == 4);
        doc.save()?;
        Ok(())
    }

    #[test]
    fn test_del() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        doc.set(ROOT, "xxx".into(), "xxx".into())?;
        assert!(doc.values(ROOT, "xxx".into())?.len() > 0);
        doc.del(ROOT, "xxx".into())?;
        assert!(doc.values(ROOT, "xxx".into())?.len() == 0);
        Ok(())
    }

    #[test]
    fn test_inc() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        let id = doc.set(
            ROOT,
            "counter".into(),
            Value::Scalar(amp::ScalarValue::Counter(10)),
        )?;
        assert!(
            doc.value(ROOT, "counter".into())?
                == Some((Value::Scalar(amp::ScalarValue::Counter(10)), id))
        );
        doc.inc(ROOT, "counter".into(), 10)?;
        assert!(
            doc.value(ROOT, "counter".into())?
                == Some((Value::Scalar(amp::ScalarValue::Counter(20)), id))
        );
        doc.inc(ROOT, "counter".into(), -5)?;
        assert!(
            doc.value(ROOT, "counter".into())?
                == Some((Value::Scalar(amp::ScalarValue::Counter(15)), id))
        );
        Ok(())
    }

    #[test]
    fn test_save_incremental() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();

        doc.set(ROOT, "foo".into(), 1.into())?;

        let save1 = doc.save().unwrap();

        doc.set(ROOT, "bar".into(), 2.into())?;

        let save2 = doc.save_incremental().unwrap();

        doc.set(ROOT, "baz".into(), 3.into())?;

        let save3 = doc.save_incremental().unwrap();

        let mut save_a: Vec<u8> = vec![];
        save_a.extend(&save1);
        save_a.extend(&save2);
        save_a.extend(&save3);

        assert!(doc.save_incremental().is_none());

        let save_b = doc.save().unwrap();

        assert!(save_b.len() < save_a.len());

        let mut doc_a = Automerge::load(&save_a)?;
        let mut doc_b = Automerge::load(&save_b)?;

        assert!(doc_a.values(ROOT, "baz".into())? == doc_b.values(ROOT, "baz".into())?);

        assert!(doc_a.save().unwrap() == doc_b.save().unwrap());

        Ok(())
    }

    #[test]
    fn test_save_text() -> Result<(), AutomergeError> {
        let mut doc = Automerge::new();
        let text = doc.set(ROOT, "text".into(), amp::ObjType::Text.into())?;
        let text: ObjId = text.into();
        doc.splice_text(text, 0, 0, "hello world")?;
        doc.splice_text(text, 6, 0, "big bad ")?;
        assert!(&doc.text(text)? == "hello big bad world");
        Ok(())
    }
}

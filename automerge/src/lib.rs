#![allow(unused_variables)]
#![allow(dead_code)]

extern crate hex;
extern crate web_sys;

// this is needed for print debugging via WASM
#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

use core::ops::Range;
use std::cmp::{Eq, Ordering};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Index;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AutomergeError {
    #[error("begin() called inside of a transaction")]
    MismatchedBegin,
    #[error("commit() called outside of a transaction")]
    MismatchedCommit,
    #[error("change made outside of a transaction")]
    OpOutsideOfTransaction,
}

#[derive(Debug, Clone)]
struct Cursor {
    pos: usize,
    seen: usize,
}

#[derive(Debug, Clone)]
pub enum Value {
    Object(ObjType, ObjId),
    Scalar(ScalarValue),
}

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Bytes(Vec<u8>),
    Str(String),
    Int(i64),
    Uint(u64),
    F64(f64),
    Counter(i64),
    Timestamp(i64),
    //    Cursor(OpId),
    Boolean(bool),
    Null,
}

impl From<&str> for ScalarValue {
    fn from(s: &str) -> Self {
        ScalarValue::Str(s.to_owned())
    }
}

impl ScalarValue {
    pub fn datatype(&self) -> String {
        match self {
            ScalarValue::Bytes(_) => "bytes".into(),
            ScalarValue::Str(_) => "str".into(),
            ScalarValue::Int(_) => "int".into(),
            ScalarValue::Uint(_) => "uint".into(),
            ScalarValue::F64(_) => "f64".into(),
            ScalarValue::Counter(_) => "counter".into(),
            ScalarValue::Timestamp(_) => "timestamp".into(),
            ScalarValue::Boolean(_) => "boolean".into(),
            ScalarValue::Null => "null".into(),
        }
    }
}

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarValue::Bytes(v) => write!(f, "Bytes({})", hex::encode(v)),
            ScalarValue::Str(v) => write!(f,"{}",v),
            ScalarValue::Int(v) => write!(f,"{}",v),
            ScalarValue::Uint(v) => write!(f,"{}",v),
            ScalarValue::F64(v) => write!(f,"{}",v),
            ScalarValue::Counter(v) => write!(f,"Counter({})",v),
            ScalarValue::Timestamp(v) => write!(f,"Counter({})",v),
            ScalarValue::Boolean(v) => write!(f,"{}",v),
            ScalarValue::Null => write!(f,"null"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ObjType {
    Map,
    List,
    Table,
    Text,
}

impl ObjType {
    pub fn to_string(&self) -> String {
        match self {
            ObjType::Map => "map".into(),
            ObjType::List => "list".into(),
            ObjType::Table => "table".into(),
            ObjType::Text => "text".into(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum OpType {
    Make(ObjType),
    Del,
    Inc(i64),
    Set(ScalarValue),
}

#[derive(Debug, Clone)]
struct IndexedCache<T> {
    cache: Vec<T>,
    lookup: HashMap<T, usize>,
}

impl<T> IndexedCache<T>
where
    T: Clone + Eq + Hash,
{
    fn new() -> Self {
        IndexedCache {
            cache: Default::default(),
            lookup: Default::default(),
        }
    }

    fn from(cache: Vec<T>) -> Self {
        let lookup = cache
            .iter()
            .enumerate()
            .map(|(i, v)| (v.clone(), i))
            .collect();
        IndexedCache { cache, lookup }
    }

    fn cache(&mut self, item: T) -> usize {
        if let Some(n) = self.lookup.get(&item) {
            *n
        } else {
            let n = self.cache.len();
            self.cache.push(item.clone());
            self.lookup.insert(item, n);
            n
        }
    }

    fn lookup(&self, item: T) -> Option<usize> {
        self.lookup.get(&item).cloned()
    }

    fn get(&self, index: usize) -> &T {
        &self.cache[index]
    }
}

impl<T> Index<usize> for IndexedCache<T> {
    type Output = T;
    fn index(&self, i: usize) -> &T {
        &self.cache[i]
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy)]
pub enum Key {
    Map(usize),
    Seq(ElemId),
}

impl Key {
    fn elemid(&self) -> Option<ElemId> {
        match self {
            Key::Map(_) => None,
            Key::Seq(id) => Some(*id),
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, Eq, PartialEq, Copy)]
pub struct OpId(u64, usize);

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord)]
pub struct ObjId(OpId);

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord)]
pub struct ElemId(OpId);

pub const HEAD: ElemId = ElemId(OpId(0, 0));
pub const ROOT: ObjId = ObjId(OpId(0, 0));

#[derive(Debug, Clone)]
pub(crate) struct Op {
    pub change: usize,
    pub id: OpId,
    //    pub actor: usize,
    //    pub ctr: u64,
    pub action: OpType,
    pub obj: ObjId,
    pub key: Key,
    pub succ: Vec<OpId>,
    pub pred: Vec<OpId>,
    pub insert: bool,
}

impl Op {
    fn visible(&self) -> bool {
        self.succ.is_empty()
    }

    fn elemid(&self) -> Option<ElemId> {
        if self.insert {
            Some(ElemId(self.id))
        } else {
            self.key.elemid()
        }
    }

    fn ordering_key(&self) -> Key {
        if self.insert {
            Key::Seq(ElemId(self.id))
        } else {
            self.key
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Change {
    pub actor: usize,
    pub seq: u64,
    pub max_op: u64,
    pub time: i64,
    pub message: Option<String>,
    pub extra_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Actor(Vec<u8>);

impl std::fmt::Display for Actor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

#[derive(Debug, Clone)]
pub struct Peer {}

#[derive(Debug, Clone)]
pub struct Automerge {
    actors: IndexedCache<Actor>,
    props: IndexedCache<String>,
    changes: Vec<Change>,
    ops: Vec<Op>,
    seq: u64,
    max_op: u64,
    transaction: Option<Change>,
}

impl Automerge {
    pub fn new() -> Self {
        Automerge {
            actors: IndexedCache::from(vec![Actor(hex::decode("aabbccdd").unwrap())]),
            props: IndexedCache::new(),
            changes: Default::default(),
            ops: Default::default(),
            seq: 0,
            max_op: 0,
            transaction: None,
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

        self.transaction = Some(Change {
            actor: 0,
            seq: self.seq + 1,
            max_op: self.max_op,
            time: time.unwrap_or(0),
            message,
            extra_bytes: Default::default(),
        });

        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), AutomergeError> {
        if let Some(tx) = self.transaction.take() {
            self.changes.push(tx);
            self.seq += 1;
            Ok(())
        } else {
            Err(AutomergeError::MismatchedCommit)
        }
    }

    pub fn rollback(&mut self) {
        // remove all ops where change == self.changes.len()
        // remove all pred where (id >= self.max_op, 0)
        self.transaction = None
    }

    fn lamport_cmp(&self, left: OpId, right: OpId) -> Ordering {
        match (left, right) {
            (OpId(0, _), OpId(0, _)) => Ordering::Equal,
            (OpId(0, _), OpId(_, _)) => Ordering::Less,
            (OpId(_, _), OpId(0, _)) => Ordering::Greater,
            (OpId(a, x), OpId(b, y)) if a == b => self.actors[x].0.cmp(&self.actors[y].0),
            (OpId(a, _), OpId(b, _)) => a.cmp(&b),
        }
    }

    pub fn prop_to_key(&mut self, prop: String) -> Key {
        Key::Map(self.props.cache(prop))
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
        action: OpType,
        insert: bool,
    ) -> Result<(), AutomergeError> {
        if let Some(mut tx) = self.transaction.take() {
            tx.max_op += 1;
            self.insert_op(Op {
                change: self.changes.len(),
                id: OpId(tx.max_op, 0),
                action,
                obj,
                key,
                succ: vec![],
                pred: vec![],
                insert,
            });
            self.transaction = Some(tx);
            Ok(())
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

    fn scan_to_prop_insertion_point(&self, op: &Op, pos: &mut usize) {
        while *pos < self.ops.len()
            && self.ops[*pos].obj == op.obj
            && self.ops[*pos].key == op.key
            && self.lamport_cmp(op.id, self.ops[*pos].id) == Ordering::Greater
        {
            *pos += 1
        }
    }

    fn scan_to_prop_value(&self, obj: &ObjId, key: &Key, pos: &mut usize) {
        while *pos < self.ops.len()
            && &self.ops[*pos].obj == obj
            && &self.ops[*pos].key == key
            && !self.ops[*pos].succ.is_empty()
        {
            *pos += 1
        }
    }

    fn scan_to_elem_insert_op(&self, op: &Op, elem: &ElemId, pos: &mut usize, seen: &mut usize) {
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

            *pos += 1
        }
    }

    fn scan_to_elem_update_pos(&self, op: &Op, elem: &ElemId, pos: &mut usize) {
        *pos += 1; // always step over the insert=true op
        while *pos < self.ops.len()
            && self.ops[*pos].obj == op.obj
            && self.ops[*pos].key == op.key
            && !self.ops[*pos].insert
            && self.lamport_cmp(op.id, self.ops[*pos].id) == Ordering::Greater
        {
            *pos += 1
        }
    }

    fn scan_to_lesser_insert(&self, op: &Op, elem: &ElemId, pos: &mut usize, seen: &mut usize) {
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

    fn seek_to_elem(&self, op: &Op, elem: &ElemId) -> Cursor {
        let mut pos = 0;
        let mut seen = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op(op, elem, &mut pos, &mut seen);
        self.scan_to_elem_update_pos(op, elem, &mut pos);
        Cursor { pos, seen }
    }

    fn seek_to_insert_elem(&self, op: &Op, elem: &ElemId) -> Cursor {
        let mut pos = 0;
        let mut seen = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op(op, elem, &mut pos, &mut seen);
        self.scan_to_lesser_insert(op, elem, &mut pos, &mut seen);
        Cursor { pos, seen }
    }

    fn seek_to_map_op(&self, op: &Op) -> Cursor {
        let mut pos = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_prop_start(&op.obj, &op.key, &mut pos);
        self.scan_to_prop_insertion_point(op, &mut pos);
        Cursor { pos, seen: 0 }
    }

    fn seek_to_op(&self, op: &Op) -> Cursor {
        match (&op.key, op.insert) {
            (Key::Map(_), _) => self.seek_to_map_op(op),
            (Key::Seq(elem), true) => self.seek_to_insert_elem(op, elem),
            (Key::Seq(elem), false) => self.seek_to_elem(op, elem),
        }
    }

    fn insert_op(&mut self, op: Op) {
        let cursor = self.seek_to_op(&op);
        self.ops.insert(cursor.pos, op);
        // FIXME : update succ?
        // FIXME : gen patch info
    }

    pub fn map_value(&self, obj: &ObjId, prop: &str) -> Option<Value> {
        let mut pos = 0;
        let prop = Key::Map(self.props.lookup(prop.to_owned())?);
        self.scan_to_obj(obj, &mut pos);
        self.scan_to_prop_start(obj, &prop, &mut pos);
        self.scan_to_prop_value(obj, &prop, &mut pos);
        match &self.ops[pos].action {
            OpType::Make(obj_type) => Some(Value::Object(*obj_type, ObjId(self.ops[pos].id))),
            OpType::Set(scalar) => Some(Value::Scalar(scalar.clone())),
            _ => None,
        }
    }

    fn list_value(&self, index: usize) -> Value {
        unimplemented!()
    }

    pub fn make(
        &mut self,
        obj: ObjId,
        key: Key,
        obj_type: ObjType,
        insert: bool,
    ) -> Result<(), AutomergeError> {
        self.make_op(obj, key, OpType::Make(obj_type), insert)
    }

    pub fn set(
        &mut self,
        obj: ObjId,
        key: Key,
        value: ScalarValue,
        insert: bool,
    ) -> Result<(), AutomergeError> {
        self.make_op(obj, key, OpType::Set(value), insert)
    }

    pub fn inc(&mut self, obj: ObjId, key: Key, value: i64) -> Result<(), AutomergeError> {
        unimplemented!()
    }

    pub fn del(&mut self, obj: ObjId, key: Key) -> Result<(), AutomergeError> {
        self.make_op(obj, key, OpType::Del, false)
    }

    pub fn splice(&mut self, path: &str, range: Range<usize>, replace: Vec<ScalarValue>) {
        unimplemented!()
    }

    pub fn text(&self, path: &str) -> String {
        unimplemented!()
    }

    pub fn value(&self, path: &str) -> Value {
        unimplemented!()
    }

    pub fn generate_sync_message(&self, peer: &Peer) -> Option<Vec<u8>> {
        unimplemented!()
    }
    pub fn receive_sync_message(&mut self, peer: &Peer, msg: &[u8]) {
        unimplemented!()
    }

    pub fn load(data: &[u8]) -> Self {
        unimplemented!()
    }

    pub fn apply(&mut self, data: &[u8]) {
        unimplemented!()
    }

    pub fn save(&mut self) -> Vec<u8> {
        unimplemented!()
    }
    pub fn save_incremental(&mut self) -> Vec<u8> {
        unimplemented!()
    }

    fn export(&self, id: &OpId) -> String {
        format!("{}@{}",id.0, self.actors[id.1])
    }

    pub fn dump(&self) {
        log!("  {:12} {:12} {:12} {}" , "id", "obj", "key", "value");
        for i in self.ops.iter() {
            let id = self.export(&i.id);
            let obj = self.export(&i.obj.0);
            let key = match i.key {
                Key::Map(n) => &self.props[n],
                Key::Seq(n) => unimplemented!(),
            };
            let value = match &i.action {
                OpType::Set(value) => value,
                _ => unimplemented!(),
            };
            log!("  {:12} {:12} {:12} {}" , id, obj, key, value);
        }
    }
}

impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Automerge, ROOT};
    #[test]
    fn insert_op() {
        let mut doc = Automerge::new();
        doc.begin(None, None).unwrap();
        let key = doc.prop_to_key("hello".into());
        doc.set(ROOT, key, "world".into(), false).unwrap();
        doc.commit().unwrap();
        doc.map_value(&ROOT, "hello").unwrap();
    }
}

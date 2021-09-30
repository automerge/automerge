#![allow(unused_variables)]
#![allow(dead_code)]

extern crate hex;
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

use std::fmt::Display;
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
    #[error("invalid opid format `{0}`")]
    InvalidOpId(String),
}

#[derive(Debug)]
pub enum Export {
    Id(OpId),
    Special(String),
    Prop(usize),
}

pub trait Exportable {
    fn export(&self) -> Export;
}

pub trait Importable {
    fn wrap(id: OpId) -> Self;
    fn from(s: &str) -> Option<Self>  where Self: std::marker::Sized;
}


impl OpId {
    #[inline]
    fn counter(&self) -> u64 {
        self.0
    }
    #[inline]
    fn actor(&self) -> usize {
        self.1
    }
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

pub const HEAD: ElemId = ElemId(OpId(0, 0));
pub const ROOT: ObjId = ObjId(OpId(0, 0));

const ROOT_STR : &str = "_root";
const HEAD_STR : &str = "_head";


impl Exportable for ObjId {
    fn export(&self) -> Export {
        if self == &ROOT { Export::Special(ROOT_STR.to_owned()) } else { Export::Id(self.0) } 
    }
/*
    fn special(&self) -> ExportSpecial { if self == &ROOT { ExportSpecial::Str(ROOT_STR.to_owned()) } else { ExportSpecial::None } }
    fn counter(&self) -> u64 { self.0.counter() }
    fn actor(&self) -> usize { self.0.actor() }
    */
}

impl Exportable for ElemId {
    fn export(&self) -> Export {
        if self == &HEAD { Export::Special(HEAD_STR.to_owned()) } else { Export::Id(self.0) }
    }
/*
    fn special(&self) -> ExportSpecial { if self == &HEAD { ExportSpecial::Str(HEAD_STR.to_owned()) } else { ExportSpecial::None } }
    fn counter(&self) -> u64 { self.0.counter() }
    fn actor(&self) -> usize { self.0.actor() }
    */
}

impl Exportable for OpId {
    fn export(&self) -> Export { Export::Id(*self) }
    /*
    fn special(&self) -> ExportSpecial { ExportSpecial::None }
    fn counter(&self) -> u64 { self.counter() }
    fn actor(&self) -> usize { self.actor() }
    */
}

impl Exportable for Key {
    fn export(&self) -> Export { 
        match self {
            Key::Map(p) => Export::Prop(*p),
            Key::Seq(e) => e.export(),
        }
    }
}

impl Importable for ObjId {
    fn wrap(id: OpId) -> Self { ObjId(id) }
    fn from(s: &str) -> Option<Self> { if s == ROOT_STR { Some(ROOT) } else { None } }
}

impl Importable for ElemId {
    fn wrap(id: OpId) -> Self { ElemId(id) }
    fn from(s: &str) -> Option<Self> { if s == HEAD_STR { Some(HEAD) } else { None } }
}

impl Importable for OpId {
    fn wrap(id: OpId) -> Self { id }
    fn from(s: &str) -> Option<Self> { None }
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

impl Display for ObjType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjType::Map => write!(f, "map"),
            ObjType::List => write!(f, "list"),
            ObjType::Table => write!(f, "table"),
            ObjType::Text => write!(f, "text"),
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
            self.max_op = tx.max_op;
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

    fn calc_pred(&self, obj: &ObjId, key: &Key, insert: bool) -> Vec<OpId> {
        Default::default()
    }

    fn make_op(
        &mut self,
        obj: ObjId,
        key: Key,
        action: OpType,
        insert: bool,
    ) -> Result<OpId, AutomergeError> {
        if let Some(mut tx) = self.transaction.take() {
            tx.max_op += 1;
            let id = OpId(tx.max_op, 0);
            self.insert_op(Op {
                change: self.changes.len(),
                id,
                action,
                obj,
                key,
                succ: vec![],
                pred: vec![],
                insert,
            });
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
        while *pos < self.ops.len()
            && &self.ops[*pos].obj == obj
            && !self.ops[*pos].visible()
        {
            *pos += 1
        }
    }

    fn scan_to_next_prop(&self, obj: &ObjId, key: &Key, pos: &mut usize) {
        while *pos < self.ops.len()
            && &self.ops[*pos].obj == obj
            && &self.ops[*pos].key == key
        {
            *pos += 1
        }
    }

    fn scan_to_next_visible_prop(&self, obj: &ObjId, key: &Key, pos: &mut usize) {
        self.scan_to_next_prop(obj, key, pos);
        self.scan_to_visible(obj, pos);
    }

    fn scan_to_prop_insertion_point(&mut self, op: &mut Op, pos: &mut usize) {
        while *pos < self.ops.len()
            && self.ops[*pos].obj == op.obj
            && self.ops[*pos].key == op.key
            && self.lamport_cmp(op.id, self.ops[*pos].id) == Ordering::Greater
        {
            if self.ops[*pos].succ.is_empty() {
                self.ops[*pos].succ.push(op.id);
                op.pred.push(self.ops[*pos].id);
            }
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

    fn seek_to_map_op(&mut self, op: &mut Op) -> Cursor {
        let mut pos = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_prop_start(&op.obj, &op.key, &mut pos);
        self.scan_to_prop_insertion_point(op, &mut pos);
        Cursor { pos, seen: 0 }
    }

    fn seek_to_op(&mut self, op: &mut Op) -> Cursor {
        match (&op.key, op.insert) {
            (Key::Map(_), _) => self.seek_to_map_op(op),
            (Key::Seq(elem), true) => self.seek_to_insert_elem(op, elem),
            (Key::Seq(elem), false) => self.seek_to_elem(op, elem),
        }
    }

    fn insert_op(&mut self, mut op: Op) {
        let cursor = self.seek_to_op(&mut op); //mut to collect pred
        /*
        if !op.insert  {
            let mut pos = cursor.pos;
            let mut pred : Vec<OpId> = Vec::new();
            while pos < self.ops.len()
                && self.ops[pos].obj == op.obj
                && self.ops[pos].key == op.key
            {
                if self.ops[pos].succ.is_empty() {
                    self.ops[pos].succ.push(op.id);
                    pred.push(self.ops[pos].id);
                }
                pos += 1
            }
            op.pred = pred;
        }
        */
        self.ops.insert(cursor.pos, op);
    }

    pub fn keys(&self, obj: &ObjId) -> Vec<Key> {
        let mut pos = 0;
        let mut result = vec![];
        self.scan_to_obj(obj, &mut pos);
        self.scan_to_visible(obj, &mut pos);
        loop {
            if let Some(op) = self.ops.get(pos) {
                // we reached the next object
                if &op.obj != obj {
                    break;
                }
                let key = &op.key;
                result.push(key.clone());
                self.scan_to_next_visible_prop(obj, key, &mut pos);
            } else {
                // we reached the end of document
               break 
            }
        }
        result
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
    ) -> Result<ObjId, AutomergeError> {
        Ok(ObjId(self.make_op(obj, key, OpType::Make(obj_type), insert)?))
    }

    pub fn set(
        &mut self,
        obj: ObjId,
        key: Key,
        value: ScalarValue,
        insert: bool,
    ) -> Result<(), AutomergeError> {
        self.make_op(obj, key, OpType::Set(value), insert)?;
        Ok(())
    }

    pub fn inc(&mut self, obj: ObjId, key: Key, value: i64) -> Result<(), AutomergeError> {
        unimplemented!()
    }

    pub fn del(&mut self, obj: ObjId, key: Key) -> Result<(), AutomergeError> {
        self.make_op(obj, key, OpType::Del, false)?;
        Ok(())
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

    pub fn import<I: Importable>(&self, s: &str) -> Result<I,AutomergeError> {
        if let Some(x) = I::from(s) {
            Ok(x) 
        } else {
            let n = s.find('@').ok_or_else(|| AutomergeError::InvalidOpId(s.to_owned()))?;
            let counter = s[0..n].parse().map_err(|_| AutomergeError::InvalidOpId(s.to_owned()))?;
            // - FIXME - unneeded to_vec()
            let actor = Actor(hex::decode(&s[(n + 1)..]).unwrap());
            let actor = self.actors.lookup(actor).ok_or_else(|| AutomergeError::InvalidOpId(s.to_owned()))?;
            Ok(I::wrap(OpId(counter,actor)))
        }
    }

    pub fn export<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => format!("{}@{}",id.counter(), self.actors[id.actor()]),
            Export::Prop(index) => self.props[index].clone(),
            Export::Special(s) => s,
        }
    }

    pub fn dump(&self) {
        log!("  {:12} {:12} {:12} {} {} {}" , "id", "obj", "key", "value", "pred", "succ");
        for i in self.ops.iter() {
            let id = self.export(i.id);
            let obj = self.export(i.obj);
            let key = match i.key {
                Key::Map(n) => &self.props[n],
                Key::Seq(n) => unimplemented!(),
            };
            let value : String = match &i.action {
                OpType::Set(value) => format!("{}",value),
                OpType::Make(obj) => format!("make{}",obj),
                _ => unimplemented!(),
            };
            let pred : Vec<_>= i.pred.iter().map(|id| self.export(*id)).collect();
            let succ : Vec<_>= i.succ.iter().map(|id| self.export(*id)).collect();
            log!("  {:12} {:12} {:12} {} {:?} {:?}" , id, obj, key, value, pred,succ);
        }
    }
}

impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for OpId {
    fn default() -> Self {
        OpId(0,0)
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
    #[test]
    fn exports() {
        let mut doc = Automerge::new();
    }
}

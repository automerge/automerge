#![allow(unused_variables)]
#![allow(dead_code)]

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

mod expanded_op;
mod internal;

use automerge_protocol as amp;
use change::{encode_document, export_change};
use core::ops::Range;
use indexed_cache::IndexedCache;
use nonzero_ext::nonzero;
use std::cmp::{Eq, Ordering};
use std::collections::HashMap;
use std::collections::HashSet;
use thiserror::Error;

pub use change::EncodedChange;

pub use amp::{ActorId, ObjType, ScalarValue};

#[derive(Error, Debug)]
pub enum AutomergeError {
    #[error("begin() called inside of a transaction")]
    MismatchedBegin,
    #[error("commit() called outside of a transaction")]
    MismatchedCommit,
    #[error("change made outside of a transaction")]
    OpOutsideOfTransaction,
    #[error("begin() called with actor not set")]
    ActorNotSet,
    #[error("invalid opid format `{0}`")]
    InvalidOpId(String),
    #[error("invalid actor format `{0}`")]
    InvalidActor(String),
    #[error("invalid list pos `{0}:{1}`")]
    InvalidListAt(String, usize),
    #[error("there was an encoding problem")]
    Encoding,
}

impl From<std::io::Error> for AutomergeError {
    fn from(e: std::io::Error) -> Self {
        AutomergeError::Encoding
    }
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
    fn from(s: &str) -> Option<Self>
    where
        Self: std::marker::Sized;
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

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Object(amp::ObjType, ObjId),
    Scalar(amp::ScalarValue),
}

pub const HEAD: ElemId = ElemId(OpId(0, 0));
pub const ROOT: ObjId = ObjId(OpId(0, 0));

const ROOT_STR: &str = "_root";
const HEAD_STR: &str = "_head";

impl Exportable for ObjId {
    fn export(&self) -> Export {
        if self == &ROOT {
            Export::Special(ROOT_STR.to_owned())
        } else {
            Export::Id(self.0)
        }
    }
}

impl Exportable for ElemId {
    fn export(&self) -> Export {
        if self == &HEAD {
            Export::Special(HEAD_STR.to_owned())
        } else {
            Export::Id(self.0)
        }
    }
}

impl Exportable for OpId {
    fn export(&self) -> Export {
        Export::Id(*self)
    }
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
    fn wrap(id: OpId) -> Self {
        ObjId(id)
    }
    fn from(s: &str) -> Option<Self> {
        if s == ROOT_STR {
            Some(ROOT)
        } else {
            None
        }
    }
}

impl Importable for ElemId {
    fn wrap(id: OpId) -> Self {
        ElemId(id)
    }
    fn from(s: &str) -> Option<Self> {
        if s == HEAD_STR {
            Some(HEAD)
        } else {
            None
        }
    }
}

impl Importable for OpId {
    fn wrap(id: OpId) -> Self {
        id
    }
    fn from(s: &str) -> Option<Self> {
        None
    }
}

impl From<OpId> for Key {
    fn from(id: OpId) -> Self {
        Key::Seq(ElemId(id))
    }
}

impl From<ElemId> for Key {
    fn from(e: ElemId) -> Self {
        Key::Seq(e)
    }
}

impl From<&Op> for Value {
    fn from(op: &Op) -> Self {
        match &op.action {
            amp::OpType::Make(obj_type) => Value::Object(*obj_type, ObjId(op.id)),
            amp::OpType::Set(scalar) => Value::Scalar(scalar.clone()),
            _ => panic!("cant convert op into a value"),
        }
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
    pub action: amp::OpType,
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
pub(crate) struct Transaction {
    pub actor: usize,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    pub message: Option<String>,
    pub extra_bytes: Vec<u8>,
    pub hash: Option<amp::ChangeHash>,
    pub deps: Vec<amp::ChangeHash>,
    pub operations: Vec<Op>,
    pub len: usize,
}

#[derive(Debug, Clone)]
pub struct Peer {}

#[derive(Debug, Clone)]
pub struct Automerge {
    actors: IndexedCache<amp::ActorId>,
    props: IndexedCache<String>,
    history: Vec<EncodedChange>,
    history_index: HashMap<amp::ChangeHash, usize>,
    deps: HashSet<amp::ChangeHash>,
    ops: Vec<Op>,
    actor: Option<usize>,
    seq: u64,
    max_op: u64,
    transaction: Option<Transaction>,
}

impl Automerge {
    pub fn new() -> Self {
        Automerge {
            actors: IndexedCache::from(vec![]),
            props: IndexedCache::new(),
            history: vec![],
            history_index: HashMap::new(),
            ops: Default::default(),
            deps: Default::default(),
            actor: None,
            seq: 0,
            max_op: 0,
            transaction: None,
        }
    }

    pub fn set_actor(&mut self, actor: amp::ActorId) {
        // TODO - could change seq - need a clock
        self.actor = Some(self.actors.cache(actor))
    }

    pub fn get_actor(self) -> Option<amp::ActorId> {
        self.actor.map(|a| self.actors[a].clone())
    }

    pub fn new_with_actor_id(actor: amp::ActorId) -> Self {
        Automerge {
            actors: IndexedCache::from(vec![actor]),
            props: IndexedCache::new(),
            history: vec![],
            history_index: HashMap::new(),
            ops: Default::default(),
            deps: Default::default(),
            actor: None,
            seq: 0,
            max_op: 0,
            transaction: None,
        }
    }

    pub fn pending_ops(&self) -> u64 {
        self.transaction.as_ref().map(|t| t.len as u64).unwrap_or(0)
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

        // TODO - seq might not start at zero (load)
        self.transaction = Some(Transaction {
            actor: 0,
            seq: self.seq + 1,
            start_op: self.max_op + 1,
            time: time.unwrap_or(0),
            message,
            extra_bytes: Default::default(),
            hash: None,
            operations: vec![],
            len: 0,
            deps: vec![],
        });

        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), AutomergeError> {
        if let Some(tx) = self.transaction.take() {
            // FIXME
            // add change
            // updates clock not seq
            // updates max_op
            self.max_op = tx.start_op + tx.len as u64 - 1;
            self.history
                .push(export_change(&tx, &self.actors, &self.props));
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
            (OpId(a, x), OpId(b, y)) if a == b => self.actors[x].cmp(&self.actors[y]),
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
        action: amp::OpType,
        insert: bool,
    ) -> Result<OpId, AutomergeError> {
        if let Some(mut tx) = self.transaction.take() {
            tx.len += 1;
            let id = OpId(tx.start_op + tx.len as u64, 0);
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

    fn scan_to_nth_visible(&self, obj: &ObjId, n: usize, pos: &mut usize) -> Option<&Op> {
        let mut seen = 0;
        let mut seen_visible = false;
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
            if seen > n {
                return Some(op);
            }
            *pos += 1;
        }
        None
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
            } else if self.ops[*pos].visible() && op.pred.iter().any(|i| i == &op.id) {
                self.ops[*pos].succ.push(op.id);
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
        elem: &ElemId,
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

    fn seek_to_update_elem(&mut self, op: &mut Op, elem: &ElemId, local: bool) -> Cursor {
        let mut pos = 0;
        let mut seen = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op2(op, elem, &mut pos, &mut seen);
        self.scan_to_elem_update_pos(op, elem, local, &mut pos);
        Cursor { pos, seen }
    }

    fn seek_to_insert_elem(&self, op: &Op, elem: &ElemId) -> Cursor {
        let mut pos = 0;
        let mut seen = 0;
        self.scan_to_obj(&op.obj, &mut pos);
        self.scan_to_elem_insert_op1(op, elem, &mut pos, &mut seen);
        self.scan_to_lesser_insert(op, elem, &mut pos, &mut seen);
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

    pub fn map_value(&self, obj: &ObjId, prop: &str) -> Option<Value> {
        let mut pos = 0;
        let prop = Key::Map(self.props.lookup(prop.to_owned())?);
        self.scan_to_obj(obj, &mut pos);
        self.scan_to_prop_start(obj, &prop, &mut pos);
        self.scan_to_prop_value(obj, &prop, &mut pos);
        self.ops.get(pos).map(|o| o.into())
    }

    pub fn list_value(&self, obj: &ObjId, index: usize) -> Option<Value> {
        let mut pos = 0;
        self.scan_to_obj(obj, &mut pos);
        let op = self.scan_to_nth_visible(obj, index, &mut pos);
        op.map(|o| o.into())
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
        let op = self.scan_to_nth_visible(obj, index, &mut pos);
        op.and_then(|o| o.elemid()).map(|e| e.into())
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

    pub fn map_make(
        &mut self,
        obj: ObjId,
        prop: &str,
        obj_type: amp::ObjType,
    ) -> Result<ObjId, AutomergeError> {
        let key = self.prop_to_key(prop.into());
        Ok(ObjId(self.make_op(
            obj,
            key,
            amp::OpType::Make(obj_type),
            false,
        )?))
    }

    pub fn map_set(
        &mut self,
        obj: ObjId,
        prop: &str,
        value: amp::ScalarValue,
    ) -> Result<OpId, AutomergeError> {
        let key = self.prop_to_key(prop.into());
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

    pub fn inc(&mut self, obj: ObjId, key: Key, value: i64) -> Result<(), AutomergeError> {
        unimplemented!()
    }

    pub fn del(&mut self, obj: ObjId, key: Key) -> Result<(), AutomergeError> {
        self.make_op(obj, key, amp::OpType::Del(nonzero!(1_u32)), false)?;
        Ok(())
    }

    pub fn splice(&mut self, path: &str, range: Range<usize>, replace: Vec<amp::ScalarValue>) {
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

    pub fn apply_changes(&mut self, changes: &[amp::Change]) {
        for c in changes {
            self.apply_change(c)
        }
    }

    pub fn apply_change(&mut self, change: &amp::Change) {
        let change_id = self.history.len();
        self.history.push(change.into());
        let ops = self.import_ops(change, change_id);
        for op in ops {
            self.insert_op(op, false)
        }
    }

    fn import_ops(&mut self, change: &amp::Change, change_id: usize) -> Vec<Op> {
        change
            .operations
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let actor = self.actors.cache(change.actor_id.clone());
                let id = OpId(change.start_op + i as u64, actor);
                let obj: ObjId = self.import(&c.obj.to_string()).unwrap();
                let pred = c
                    .pred
                    .iter()
                    .map(|i| self.import(&i.to_string()).unwrap())
                    .collect();
                let key = match &c.key {
                    amp::Key::Map(n) => Key::Map(self.props.cache(n.to_string())),
                    amp::Key::Seq(amp::ElementId::Head) => Key::Seq(HEAD),
                    amp::Key::Seq(amp::ElementId::Id(i)) => Key::Seq(HEAD),
                };
                Op {
                    change: change_id,
                    id,
                    action: c.action.clone(),
                    obj,
                    key,
                    succ: vec![],
                    pred,
                    insert: c.insert,
                }
            })
            .collect()
    }

    pub fn apply(&mut self, data: &[u8]) {
        unimplemented!()
    }

    pub fn save(&self) -> Result<Vec<u8>, AutomergeError> {
        let c: Vec<_> = self.history.iter().map(|c| c.decode()).collect();
        encode_document(&c, &self.ops, &self.actors, &self.props.cache)
    }
    pub fn save_incremental(&mut self) -> Vec<u8> {
        unimplemented!()
    }

    fn get_changes_fast(&self, have_deps: &[amp::ChangeHash]) -> Option<Vec<&EncodedChange>> {
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

    fn get_changes_slow(&self, have_deps: &[amp::ChangeHash]) -> Vec<&EncodedChange> {
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

    pub fn get_changes(&self, have_deps: &[amp::ChangeHash]) -> Vec<&EncodedChange> {
        if let Some(changes) = self.get_changes_fast(have_deps) {
            changes
        } else {
            self.get_changes_slow(have_deps)
        }
    }

    pub fn get_heads(&self) -> Vec<amp::ChangeHash> {
        let mut deps: Vec<_> = self.deps.iter().copied().collect();
        deps.sort_unstable();
        deps
    }

    fn update_history(&mut self, change: EncodedChange) -> usize {
        let history_index = self.history.len();

        /*
        self.states
            .entry(change.actor_id().clone())
            .or_default()
            .push(history_index);
            */

        self.history_index.insert(change.hash, history_index);
        self.history.push(change);

        history_index
    }

    fn update_deps(&mut self, change: &EncodedChange) {
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
    use crate::{Automerge, Key, Value, HEAD, ROOT};
    use automerge_protocol as amp;

    #[test]
    fn insert_op() {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        doc.begin(None, None).unwrap();
        let key = doc.prop_to_key("hello".into());
        doc.set(ROOT, key, "world".into(), false).unwrap();
        //doc.map_set(ROOT, "&hello", "world".into()).unwrap();
        assert!(doc.pending_ops() == 1);
        doc.commit().unwrap();
        doc.map_value(&ROOT, "hello").unwrap();
    }

    #[test]
    fn test_list() {
        let mut doc = Automerge::new();
        doc.set_actor(amp::ActorId::random());
        doc.begin(None, None).unwrap();
        let list_id = doc.map_make(ROOT, "items", amp::ObjType::List).unwrap();
        doc.map_set(ROOT, "zzz", "zzzval".into()).unwrap();
        assert!(doc.map_value(&ROOT, "items") == Some(Value::Object(amp::ObjType::List, list_id)));
        let aid = doc.set(list_id, Key::Seq(HEAD), "a".into(), true).unwrap();
        let bid = doc.set(list_id, HEAD.into(), "b".into(), true).unwrap();
        doc.set(list_id, aid.into(), "c".into(), true).unwrap();
        doc.set(list_id, bid.into(), "d".into(), true).unwrap();
        //doc.dump2();
        //println!("0 {:?}",doc.list_value(&list_id, 0));
        //println!("1 {:?}",doc.list_value(&list_id, 1));
        //println!("2 {:?}",doc.list_value(&list_id, 2));
        assert!(doc.list_value(&list_id, 0) == Some(Value::Scalar("b".into())));
        assert!(doc.list_value(&list_id, 1) == Some(Value::Scalar("d".into())));
        assert!(doc.list_value(&list_id, 2) == Some(Value::Scalar("a".into())));
        assert!(doc.list_value(&list_id, 3) == Some(Value::Scalar("c".into())));
        assert!(doc.list_length(&list_id) == 4);
        doc.commit().unwrap();
        doc.save().unwrap();
    }
}

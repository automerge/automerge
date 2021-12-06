//#![allow(unused_variables)]
//#![allow(dead_code)]

extern crate hex;
extern crate uuid;
extern crate web_sys;

use automerge_protocol as amp;
use std::cmp::Eq;

pub const HEAD: ElemId = ElemId(OpId(0, 0));
pub const ROOT: ObjId = ObjId(OpId(0, 0));

const ROOT_STR: &str = "_root";
const HEAD_STR: &str = "_head";

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
    pub fn counter(&self) -> u64 {
        self.0
    }
    #[inline]
    pub fn actor(&self) -> usize {
        self.1
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Object(amp::ObjType),
    Scalar(amp::ScalarValue),
}

impl Value {
    pub fn to_string(&self) -> Option<String> {
        match self {
            Value::Scalar(val) => Some(val.to_string()),
            _ => None,
        }
    }

    pub fn map() -> Value {
        Value::Object(amp::ObjType::Map)
    }

    pub fn list() -> Value {
        Value::Object(amp::ObjType::List)
    }

    pub fn text() -> Value {
        Value::Object(amp::ObjType::Text)
    }

    pub fn table() -> Value {
        Value::Object(amp::ObjType::Table)
    }

    pub fn str(s: &str) -> Value {
        Value::Scalar(amp::ScalarValue::Str(s.into()))
    }
}

impl Exportable for ObjId {
    fn export(&self) -> Export {
        if self == &ROOT {
            Export::Special(ROOT_STR.to_owned())
        } else {
            Export::Id(self.0)
        }
    }
}

impl Exportable for &ObjId {
    fn export(&self) -> Export {
        if self == &&ROOT {
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
    fn from(_: &str) -> Option<Self> {
        None
    }
}

impl From<OpId> for ObjId {
    fn from(o: OpId) -> Self {
        ObjId(o)
    }
}

impl From<String> for Prop {
    fn from(p: String) -> Self {
        Prop::Map(p)
    }
}

impl From<&str> for Prop {
    fn from(p: &str) -> Self {
        Prop::Map(p.to_owned())
    }
}

impl From<usize> for Prop {
    fn from(index: usize) -> Self {
        Prop::Seq(index)
    }
}

impl From<f64> for Prop {
    fn from(index: f64) -> Self {
        Prop::Seq(index as usize)
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

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Scalar(s.into())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Scalar(amp::ScalarValue::Str(s.into()))
    }
}

impl From<amp::ObjType> for Value {
    fn from(o: amp::ObjType) -> Self {
        Value::Object(o)
    }
}

impl From<amp::ScalarValue> for Value {
    fn from(v: amp::ScalarValue) -> Self {
        Value::Scalar(v)
    }
}

impl From<&Op> for (Value, OpId) {
    fn from(op: &Op) -> Self {
        match &op.action {
            amp::OpType::Make(obj_type) => (Value::Object(*obj_type), op.id),
            amp::OpType::Set(scalar) => (Value::Scalar(scalar.clone()), op.id),
            _ => panic!("cant convert op into a value - {:?}", op),
        }
    }
}

impl From<Op> for (Value, OpId) {
    fn from(op: Op) -> Self {
        match &op.action {
            amp::OpType::Make(obj_type) => (Value::Object(*obj_type), op.id),
            amp::OpType::Set(scalar) => (Value::Scalar(scalar.clone()), op.id),
            _ => panic!("cant convert op into a value - {:?}", op),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Hash)]
pub enum Key {
    Map(usize),
    Seq(ElemId),
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub enum Prop {
    Map(String),
    Seq(usize),
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy)]
pub struct Patch {}

impl Key {
    fn elemid(&self) -> Option<ElemId> {
        match self {
            Key::Map(_) => None,
            Key::Seq(id) => Some(*id),
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, Eq, PartialEq, Copy, Hash)]
pub struct OpId(pub u64, pub usize);

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord, Hash)]
pub struct ObjId(pub OpId);

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord, Hash)]
pub struct ElemId(pub OpId);

#[derive(Debug, Clone, PartialEq)]
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
    pub fn is_del(&self) -> bool {
        matches!(self.action, amp::OpType::Del(_))
    }

    pub fn visible(&self) -> bool {
        self.succ.is_empty()
    }

    pub fn elemid(&self) -> Option<ElemId> {
        if self.insert {
            Some(ElemId(self.id))
        } else {
            self.key.elemid()
        }
    }

    #[allow(dead_code)]
    pub fn dump(&self) -> String {
        match &self.action {
            amp::OpType::Set(value) if self.insert => format!("i:{}", value),
            amp::OpType::Set(value) => format!("s:{}", value),
            amp::OpType::Make(obj) => format!("make{}", obj),
            amp::OpType::Inc(val) => format!("inc:{}", val),
            amp::OpType::Del(_) => "del".to_string(),
            amp::OpType::MultiSet(_) => "multiset".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {}

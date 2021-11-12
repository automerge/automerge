#![allow(unused_variables)]
#![allow(dead_code)]

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
    Object(amp::ObjType, ObjId),
    Scalar(amp::ScalarValue),
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

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy)]
pub struct Patch { }


impl Key {
    fn elemid(&self) -> Option<ElemId> {
        match self {
            Key::Map(_) => None,
            Key::Seq(id) => Some(*id),
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, Eq, PartialEq, Copy)]
pub struct OpId(pub u64, pub usize);

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord)]
pub struct ObjId(pub OpId);

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord)]
pub struct ElemId(pub OpId);

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

    pub fn ordering_key(&self) -> Key {
        if self.insert {
            Key::Seq(ElemId(self.id))
        } else {
            self.key
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {}

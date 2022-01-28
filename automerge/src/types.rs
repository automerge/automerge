use crate::error;
use crate::legacy as amp;
use serde::{Deserialize, Serialize};
use std::cmp::Eq;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt;
use std::str::FromStr;
use tinyvec::{ArrayVec, TinyVec};

pub(crate) use crate::clock::Clock;
pub(crate) use crate::value::{Counter, ScalarValue, Value};

pub(crate) const HEAD: ElemId = ElemId(OpId(0, 0));
pub(crate) const ROOT: OpId = OpId(0, 0);

const ROOT_STR: &str = "_root";
const HEAD_STR: &str = "_head";

/// An actor id is a sequence of bytes. By default we use a uuid which can be nicely stack
/// allocated.
///
/// In the event that users want to use their own type of identifier that is longer than a uuid
/// then they will likely end up pushing it onto the heap which is still fine.
///
// Note that change encoding relies on the Ord implementation for the ActorId being implemented in
// terms of the lexicographic ordering of the underlying bytes. Be aware of this if you are
// changing the ActorId implementation in ways which might affect the Ord implementation
#[derive(Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
pub struct ActorId(TinyVec<[u8; 16]>);

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ActorID")
            .field(&hex::encode(&self.0))
            .finish()
    }
}

impl ActorId {
    pub fn random() -> ActorId {
        ActorId(TinyVec::from(*uuid::Uuid::new_v4().as_bytes()))
    }

    pub fn to_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_hex_string(&self) -> String {
        hex::encode(&self.0)
    }

    pub fn op_id_at(&self, seq: u64) -> amp::OpId {
        amp::OpId(seq, self.clone())
    }
}

impl TryFrom<&str> for ActorId {
    type Error = error::InvalidActorId;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        hex::decode(s)
            .map(ActorId::from)
            .map_err(|_| error::InvalidActorId(s.into()))
    }
}

impl From<uuid::Uuid> for ActorId {
    fn from(u: uuid::Uuid) -> Self {
        ActorId(TinyVec::from(*u.as_bytes()))
    }
}

impl From<&[u8]> for ActorId {
    fn from(b: &[u8]) -> Self {
        ActorId(TinyVec::from(b))
    }
}

impl From<&Vec<u8>> for ActorId {
    fn from(b: &Vec<u8>) -> Self {
        ActorId::from(b.as_slice())
    }
}

impl From<Vec<u8>> for ActorId {
    fn from(b: Vec<u8>) -> Self {
        let inner = if let Ok(arr) = ArrayVec::try_from(b.as_slice()) {
            TinyVec::Inline(arr)
        } else {
            TinyVec::Heap(b)
        };
        ActorId(inner)
    }
}

impl FromStr for ActorId {
    type Err = error::InvalidActorId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ActorId::try_from(s)
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex_string())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy, Hash)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ObjType {
    Map,
    Table,
    List,
    Text,
}

impl ObjType {
    pub fn is_sequence(&self) -> bool {
        matches!(self, Self::List | Self::Text)
    }
}

impl From<amp::MapType> for ObjType {
    fn from(other: amp::MapType) -> Self {
        match other {
            amp::MapType::Map => Self::Map,
            amp::MapType::Table => Self::Table,
        }
    }
}

impl From<amp::SequenceType> for ObjType {
    fn from(other: amp::SequenceType) -> Self {
        match other {
            amp::SequenceType::List => Self::List,
            amp::SequenceType::Text => Self::Text,
        }
    }
}

impl fmt::Display for ObjType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjType::Map => write!(f, "map"),
            ObjType::Table => write!(f, "table"),
            ObjType::List => write!(f, "list"),
            ObjType::Text => write!(f, "text"),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum OpType {
    Make(ObjType),
    /// Perform a deletion, expanding the operation to cover `n` deletions (multiOp).
    Del,
    Inc(i64),
    Set(ScalarValue),
    MarkBegin(MarkData),
    MarkEnd(bool),
}

impl OpType {
    pub(crate) fn mark(name: String, expand: bool, value: ScalarValue) -> Self {
        OpType::MarkBegin(MarkData {
            name,
            expand,
            value,
        })
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct MarkData {
    pub name: String,
    pub value: ScalarValue,
    pub expand: bool,
}

#[derive(Debug)]
pub(crate) enum Export {
    Id(OpId),
    Special(String),
    Prop(usize),
}

pub(crate) trait Exportable {
    fn export(&self) -> Export;
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
    #[inline]
    pub fn prev(&self) -> OpId {
        OpId(self.0 - 1, self.1)
    }
}

impl Exportable for ObjId {
    fn export(&self) -> Export {
        if self.0 == ROOT {
            Export::Special(ROOT_STR.to_owned())
        } else {
            Export::Id(self.0)
        }
    }
}

impl Exportable for &ObjId {
    fn export(&self) -> Export {
        if self.0 == ROOT {
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

impl From<OpId> for ObjId {
    fn from(o: OpId) -> Self {
        ObjId(o)
    }
}

impl From<OpId> for ElemId {
    fn from(o: OpId) -> Self {
        ElemId(o)
    }
}

impl From<String> for Prop {
    fn from(p: String) -> Self {
        Prop::Map(p)
    }
}

impl From<&String> for Prop {
    fn from(p: &String) -> Self {
        Prop::Map(p.clone())
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

impl From<Option<ElemId>> for ElemId {
    fn from(e: Option<ElemId>) -> Self {
        e.unwrap_or(HEAD)
    }
}

impl From<Option<ElemId>> for Key {
    fn from(e: Option<ElemId>) -> Self {
        Key::Seq(e.into())
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Hash)]
pub(crate) enum Key {
    Map(usize),
    Seq(ElemId),
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub enum Prop {
    Map(String),
    Seq(usize),
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct Patch {}

impl Key {
    pub fn elemid(&self) -> Option<ElemId> {
        match self {
            Key::Map(_) => None,
            Key::Seq(id) => Some(*id),
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, Eq, PartialEq, Copy, Hash, Default)]
pub(crate) struct OpId(pub u64, pub usize);

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord, Hash, Default)]
pub(crate) struct ObjId(pub OpId);

impl ObjId {
    pub fn root() -> Self {
        ObjId(OpId(0, 0))
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord, Hash, Default)]
pub(crate) struct ElemId(pub OpId);

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Op {
    pub change: usize,
    pub id: OpId,
    pub action: OpType,
    pub obj: ObjId,
    pub key: Key,
    pub succ: Vec<OpId>,
    pub pred: Vec<OpId>,
    pub insert: bool,
}

impl Op {
    pub(crate) fn add_succ(&mut self, op: &Op) {
        self.succ.push(op.id);
        if let OpType::Set(ScalarValue::Counter(Counter {
            current,
            increments,
            ..
        })) = &mut self.action
        {
            if let OpType::Inc(n) = &op.action {
                *current += *n;
                *increments += 1;
            }
        }
    }

    pub(crate) fn remove_succ(&mut self, op: &Op) {
        self.succ.retain(|id| id != &op.id);
        if let OpType::Set(ScalarValue::Counter(Counter {
            current,
            increments,
            ..
        })) = &mut self.action
        {
            if let OpType::Inc(n) = &op.action {
                *current -= *n;
                *increments -= 1;
            }
        }
    }

    pub fn visible(&self) -> bool {
        if self.is_inc() || self.is_mark() {
            false
        } else if self.is_counter() {
            self.succ.len() <= self.incs()
        } else {
            self.succ.is_empty()
        }
    }

    pub fn incs(&self) -> usize {
        if let OpType::Set(ScalarValue::Counter(Counter { increments, .. })) = &self.action {
            *increments
        } else {
            0
        }
    }

    pub fn is_del(&self) -> bool {
        matches!(&self.action, OpType::Del)
    }

    pub fn is_inc(&self) -> bool {
        matches!(&self.action, OpType::Inc(_))
    }

    pub fn valid_mark_anchor(&self) -> bool {
        self.succ.is_empty()
            && matches!(
                &self.action,
                OpType::MarkBegin(MarkData { expand: true, .. }) | OpType::MarkEnd(false)
            )
    }

    pub fn is_mark(&self) -> bool {
        matches!(&self.action, OpType::MarkBegin(_) | OpType::MarkEnd(_))
    }

    pub fn is_counter(&self) -> bool {
        matches!(&self.action, OpType::Set(ScalarValue::Counter(_)))
    }

    pub fn is_noop(&self, action: &OpType) -> bool {
        matches!((&self.action, action), (OpType::Set(n), OpType::Set(m)) if n == m)
    }

    pub fn overwrites(&self, other: &Op) -> bool {
        self.pred.iter().any(|i| i == &other.id)
    }

    pub fn elemid(&self) -> Option<ElemId> {
        if self.insert {
            Some(ElemId(self.id))
        } else {
            self.key.elemid()
        }
    }

    pub fn value(&self) -> Value {
        match &self.action {
            OpType::Make(obj_type) => Value::Object(*obj_type),
            OpType::Set(scalar) => Value::Scalar(scalar.clone()),
            _ => panic!("cant convert op into a value - {:?}", self),
        }
    }

    #[allow(dead_code)]
    pub fn dump(&self) -> String {
        match &self.action {
            OpType::Set(value) if self.insert => format!("i:{}", value),
            OpType::Set(value) => format!("s:{}", value),
            OpType::Make(obj) => format!("make{}", obj),
            OpType::MarkBegin(m) => format!("mark{}={}", m.name, m.value),
            OpType::MarkEnd(_) => "unmark".into(),
            OpType::Inc(val) => format!("inc:{}", val),
            OpType::Del => "del".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {}

#[derive(Eq, PartialEq, Hash, Clone, PartialOrd, Ord, Copy)]
pub struct ChangeHash(pub [u8; 32]);

impl fmt::Debug for ChangeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ChangeHash")
            .field(&hex::encode(&self.0))
            .finish()
    }
}

impl fmt::Display for ChangeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseChangeHashError {
    #[error(transparent)]
    HexDecode(#[from] hex::FromHexError),
    #[error("incorrect length, change hash should be 32 bytes, got {actual}")]
    IncorrectLength { actual: usize },
}

impl FromStr for ChangeHash {
    type Err = ParseChangeHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s)?;
        if bytes.len() == 32 {
            Ok(ChangeHash(bytes.try_into().unwrap()))
        } else {
            Err(ParseChangeHashError::IncorrectLength {
                actual: bytes.len(),
            })
        }
    }
}

impl TryFrom<&[u8]> for ChangeHash {
    type Error = error::InvalidChangeHashSlice;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 32 {
            Err(error::InvalidChangeHashSlice(Vec::from(bytes)))
        } else {
            let mut array = [0; 32];
            array.copy_from_slice(bytes);
            Ok(ChangeHash(array))
        }
    }
}

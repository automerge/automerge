use crate::error;
use crate::legacy as amp;
use crate::ScalarValue;
use serde::{Deserialize, Serialize};
use std::cmp::Eq;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt;
use std::rc::Rc;
use std::str::FromStr;
use tinyvec::{ArrayVec, TinyVec};

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

impl fmt::Display for OpId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", &self.counter, &self.actor)
    }
}

impl fmt::Display for ObjId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjId::Root => write!(f, "_root"),
            ObjId::Id(id) => write!(f, "{}", id),
        }
    }
}

impl fmt::Display for ElemId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ElemId::Head => write!(f, "_head"),
            ElemId::Id(id) => write!(f, "{}", id),
        }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::Map(s) => write!(f, "{}", s),
            Key::Seq(id) => write!(f, "{}", id),
        }
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
}

impl From<OpId> for ObjId {
    fn from(o: OpId) -> Self {
        ObjId::Id(o)
    }
}

impl From<&OpId> for ObjId {
    fn from(o: &OpId) -> Self {
        ObjId::Id(o.clone())
    }
}

impl From<OpId> for amp::OpId {
    fn from(o: OpId) -> Self {
        amp::OpId(o.counter, o.actor.as_ref().clone())
    }
}

impl From<&OpId> for amp::OpId {
    fn from(o: &OpId) -> Self {
        amp::OpId(o.counter, o.actor.as_ref().clone())
    }
}

impl From<Key> for amp::Key {
    fn from(k: Key) -> Self {
        match k {
            Key::Map(s) => amp::Key::Map(s.into()),
            Key::Seq(e) => amp::Key::Seq(e.into()),
        }
    }
}

impl From<&Key> for amp::Key {
    fn from(k: &Key) -> Self {
        match k {
            Key::Map(s) => amp::Key::Map(s.into()),
            Key::Seq(e) => amp::Key::Seq(e.into()),
        }
    }
}

impl From<ObjId> for amp::ObjectId {
    fn from(o: ObjId) -> Self {
        match o {
            ObjId::Root => amp::ObjectId::Root,
            ObjId::Id(id) => amp::ObjectId::Id(id.into()),
        }
    }
}

impl From<&ObjId> for amp::ObjectId {
    fn from(o: &ObjId) -> Self {
        match o {
            ObjId::Root => amp::ObjectId::Root,
            ObjId::Id(id) => amp::ObjectId::Id(id.into()),
        }
    }
}

impl From<&ElemId> for amp::ElementId {
    fn from(o: &ElemId) -> Self {
        match o {
            ElemId::Head => amp::ElementId::Head,
            ElemId::Id(id) => amp::ElementId::Id(id.into()),
        }
    }
}

impl From<ElemId> for amp::ElementId {
    fn from(o: ElemId) -> Self {
        match o {
            ElemId::Head => amp::ElementId::Head,
            ElemId::Id(id) => amp::ElementId::Id(id.into()),
        }
    }
}

impl From<&Op> for amp::Op {
    fn from(op: &Op) -> Self {
        let action = op.action.clone();
        let key = (&op.key).into();
        let obj = (&op.obj).into();
        let pred = op.pred.iter().map(|id| id.into()).collect();
        amp::Op {
            action,
            obj,
            insert: op.insert,
            pred,
            key,
        }
    }
}

impl From<OpId> for ElemId {
    fn from(o: OpId) -> Self {
        ElemId::Id(o)
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
        Key::Seq(ElemId::Id(id))
    }
}

impl From<ElemId> for Key {
    fn from(e: ElemId) -> Self {
        Key::Seq(e)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) enum Key {
    Map(String),
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
            Key::Seq(id) => Some(id.clone()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct OpId {
    pub counter: u64,
    pub actor: Rc<ActorId>,
}

impl OpId {
    #[cfg(test)]
    pub(crate) fn at(counter: u64, actor: &ActorId) -> OpId {
        OpId {
            counter,
            actor: Rc::new(actor.clone()),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum ObjId {
    Root,
    Id(OpId),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) enum ElemId {
    Head,
    Id(OpId),
}

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
    pub fn is_del(&self) -> bool {
        matches!(self.action, OpType::Del)
    }

    pub fn overwrites(&self, other: &Op) -> bool {
        self.pred.iter().any(|i| i == &other.id)
    }

    pub fn elemid(&self) -> Option<ElemId> {
        if self.insert {
            Some(ElemId::Id(self.id.clone()))
        } else {
            self.key.elemid()
        }
    }

    #[allow(dead_code)]
    pub fn dump(&self) -> String {
        match &self.action {
            OpType::Set(value) if self.insert => format!("i:{}", value),
            OpType::Set(value) => format!("s:{}", value),
            OpType::Make(obj) => format!("make{}", obj),
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

impl Ord for OpId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.counter.cmp(&other.counter) {
            Ordering::Equal => other.actor.cmp(&self.actor),
            order => order,
        }
    }
}

impl PartialOrd for OpId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd for ElemId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialOrd for ObjId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ElemId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ElemId::Head, ElemId::Head) => Ordering::Equal,
            (ElemId::Head, _) => Ordering::Less,
            (_, ElemId::Head) => Ordering::Greater,
            (ElemId::Id(a), ElemId::Id(b)) => a.cmp(b),
        }
    }
}

impl Ord for ObjId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ObjId::Root, ObjId::Root) => Ordering::Equal,
            (ObjId::Root, _) => Ordering::Less,
            (_, ObjId::Root) => Ordering::Greater,
            (ObjId::Id(a), ObjId::Id(b)) => a.cmp(b),
        }
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Key::Map(a), Key::Map(b)) => a.cmp(b),
            (Key::Seq(a), Key::Seq(b)) => a.cmp(b),
            (_, _) => panic!("comparing seq key to map key"),
        }
    }
}

use crate::error;
use crate::legacy as amp;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Eq;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;
use tinyvec::{ArrayVec, TinyVec};

mod opids;
pub(crate) use opids::OpIds;

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
}

impl TryFrom<&str> for ActorId {
    type Error = error::InvalidActorId;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        hex::decode(s)
            .map(ActorId::from)
            .map_err(|_| error::InvalidActorId(s.into()))
    }
}

impl TryFrom<String> for ActorId {
    type Error = error::InvalidActorId;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        hex::decode(&s)
            .map(ActorId::from)
            .map_err(|_| error::InvalidActorId(s))
    }
}

impl AsRef<[u8]> for ActorId {
    fn as_ref(&self) -> &[u8] {
        &self.0
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

impl<const N: usize> From<[u8; N]> for ActorId {
    fn from(array: [u8; N]) -> Self {
        ActorId::from(&array)
    }
}

impl<const N: usize> From<&[u8; N]> for ActorId {
    fn from(slice: &[u8; N]) -> Self {
        let inner = if let Ok(arr) = ArrayVec::try_from(slice.as_slice()) {
            TinyVec::Inline(arr)
        } else {
            TinyVec::Heap(slice.to_vec())
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

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Copy, Hash)]
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
    Delete,
    Increment(i64),
    Put(ScalarValue),
}

impl OpType {
    /// The index into the action array as specified in [1]
    ///
    /// [1]: https://alexjg.github.io/automerge-storage-docs/#action-array
    pub(crate) fn action_index(&self) -> u64 {
        match self {
            Self::Make(ObjType::Map) => 0,
            Self::Put(_) => 1,
            Self::Make(ObjType::List) => 2,
            Self::Delete => 3,
            Self::Make(ObjType::Text) => 4,
            Self::Increment(_) => 5,
            Self::Make(ObjType::Table) => 6,
        }
    }

    pub(crate) fn from_index_and_value(
        index: u64,
        value: ScalarValue,
    ) -> Result<OpType, error::InvalidOpType> {
        match index {
            0 => Ok(Self::Make(ObjType::Map)),
            1 => Ok(Self::Put(value)),
            2 => Ok(Self::Make(ObjType::List)),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Make(ObjType::Text)),
            5 => match value {
                ScalarValue::Int(i) => Ok(Self::Increment(i)),
                ScalarValue::Uint(i) => Ok(Self::Increment(i as i64)),
                _ => Err(error::InvalidOpType::NonNumericInc),
            },
            6 => Ok(Self::Make(ObjType::Table)),
            other => Err(error::InvalidOpType::UnknownAction(other)),
        }
    }
}

impl From<ObjType> for OpType {
    fn from(v: ObjType) -> Self {
        OpType::Make(v)
    }
}

impl From<ScalarValue> for OpType {
    fn from(v: ScalarValue) -> Self {
        OpType::Put(v)
    }
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
    pub(crate) fn counter(&self) -> u64 {
        self.0
    }
    #[inline]
    pub(crate) fn actor(&self) -> usize {
        self.1
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

impl From<ObjId> for OpId {
    fn from(o: ObjId) -> Self {
        o.0
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

impl Display for Prop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Prop::Map(s) => write!(f, "{}", s),
            Prop::Seq(i) => write!(f, "{}", i),
        }
    }
}

impl Key {
    pub(crate) fn elemid(&self) -> Option<ElemId> {
        match self {
            Key::Map(_) => None,
            Key::Seq(id) => Some(*id),
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, Eq, PartialEq, Copy, Hash, Default)]
pub(crate) struct OpId(pub(crate) u64, pub(crate) usize);

impl OpId {
    pub(crate) fn new(actor: usize, counter: u64) -> Self {
        Self(counter, actor)
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord, Hash, Default)]
pub(crate) struct ObjId(pub(crate) OpId);

impl ObjId {
    pub(crate) const fn root() -> Self {
        ObjId(OpId(0, 0))
    }

    pub(crate) fn is_root(&self) -> bool {
        self.0.counter() == 0
    }

    pub(crate) fn opid(&self) -> &OpId {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord, Hash, Default)]
pub(crate) struct ElemId(pub(crate) OpId);

impl ElemId {
    pub(crate) fn is_head(&self) -> bool {
        *self == HEAD
    }

    pub(crate) fn head() -> Self {
        Self(OpId(0, 0))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Op {
    pub(crate) id: OpId,
    pub(crate) action: OpType,
    pub(crate) key: Key,
    pub(crate) succ: OpIds,
    pub(crate) pred: OpIds,
    pub(crate) insert: bool,
}

impl Op {
    pub(crate) fn add_succ<F: Fn(&OpId, &OpId) -> std::cmp::Ordering>(&mut self, op: &Op, cmp: F) {
        self.succ.add(op.id, cmp);
        if let OpType::Put(ScalarValue::Counter(Counter {
            current,
            increments,
            ..
        })) = &mut self.action
        {
            if let OpType::Increment(n) = &op.action {
                *current += *n;
                *increments += 1;
            }
        }
    }

    pub(crate) fn remove_succ(&mut self, op: &Op) {
        self.succ.retain(|id| id != &op.id);
        if let OpType::Put(ScalarValue::Counter(Counter {
            current,
            increments,
            ..
        })) = &mut self.action
        {
            if let OpType::Increment(n) = &op.action {
                *current -= *n;
                *increments -= 1;
            }
        }
    }

    pub(crate) fn visible(&self) -> bool {
        if self.is_inc() {
            false
        } else if self.is_counter() {
            self.succ.len() <= self.incs()
        } else {
            self.succ.is_empty()
        }
    }

    pub(crate) fn incs(&self) -> usize {
        if let OpType::Put(ScalarValue::Counter(Counter { increments, .. })) = &self.action {
            *increments
        } else {
            0
        }
    }

    pub(crate) fn is_delete(&self) -> bool {
        matches!(&self.action, OpType::Delete)
    }

    pub(crate) fn is_inc(&self) -> bool {
        matches!(&self.action, OpType::Increment(_))
    }

    pub(crate) fn is_counter(&self) -> bool {
        matches!(&self.action, OpType::Put(ScalarValue::Counter(_)))
    }

    pub(crate) fn is_noop(&self, action: &OpType) -> bool {
        matches!((&self.action, action), (OpType::Put(n), OpType::Put(m)) if n == m)
    }

    pub(crate) fn is_list_op(&self) -> bool {
        matches!(&self.key, Key::Seq(_))
    }

    pub(crate) fn overwrites(&self, other: &Op) -> bool {
        self.pred.iter().any(|i| i == &other.id)
    }

    pub(crate) fn elemid(&self) -> Option<ElemId> {
        self.elemid_or_key().elemid()
    }

    pub(crate) fn elemid_or_key(&self) -> Key {
        if self.insert {
            Key::Seq(ElemId(self.id))
        } else {
            self.key
        }
    }

    pub(crate) fn get_increment_value(&self) -> Option<i64> {
        if let OpType::Increment(i) = self.action {
            Some(i)
        } else {
            None
        }
    }

    pub(crate) fn value(&self) -> Value<'_> {
        match &self.action {
            OpType::Make(obj_type) => Value::Object(*obj_type),
            OpType::Put(scalar) => Value::Scalar(Cow::Borrowed(scalar)),
            _ => panic!("cant convert op into a value - {:?}", self),
        }
    }

    pub(crate) fn clone_value(&self) -> Value<'static> {
        match &self.action {
            OpType::Make(obj_type) => Value::Object(*obj_type),
            OpType::Put(scalar) => Value::Scalar(Cow::Owned(scalar.clone())),
            _ => panic!("cant convert op into a value - {:?}", self),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn dump(&self) -> String {
        match &self.action {
            OpType::Put(value) if self.insert => format!("i:{}", value),
            OpType::Put(value) => format!("s:{}", value),
            OpType::Make(obj) => format!("make{}", obj),
            OpType::Increment(val) => format!("inc:{}", val),
            OpType::Delete => "del".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Peer {}

/// The number of bytes in a change hash.
pub(crate) const HASH_SIZE: usize = 32; // 256 bits = 32 bytes

/// The sha256 hash of a change.
#[derive(Eq, PartialEq, Hash, Clone, PartialOrd, Ord, Copy)]
pub struct ChangeHash(pub [u8; HASH_SIZE]);

impl ChangeHash {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn checksum(&self) -> [u8; 4] {
        [self.0[0], self.0[1], self.0[2], self.0[3]]
    }
}

impl AsRef<[u8]> for ChangeHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

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
    #[error(
        "incorrect length, change hash should be {} bytes, got {actual}",
        HASH_SIZE
    )]
    IncorrectLength { actual: usize },
}

impl FromStr for ChangeHash {
    type Err = ParseChangeHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s)?;
        if bytes.len() == HASH_SIZE {
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
        if bytes.len() != HASH_SIZE {
            Err(error::InvalidChangeHashSlice(Vec::from(bytes)))
        } else {
            let mut array = [0; HASH_SIZE];
            array.copy_from_slice(bytes);
            Ok(ChangeHash(array))
        }
    }
}

#[cfg(feature = "wasm")]
impl From<Prop> for wasm_bindgen::JsValue {
    fn from(prop: Prop) -> Self {
        match prop {
            Prop::Map(key) => key.into(),
            Prop::Seq(index) => (index as f64).into(),
        }
    }
}

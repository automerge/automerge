use crate::error;
use crate::error::AutomergeError;
use crate::legacy as amp;
use crate::op_set2::ActorIdx;
use rand::{
    distr::{Distribution, StandardUniform},
    Rng,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::{Eq, Ordering};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;
use tinyvec::{ArrayVec, TinyVec};

use rustc_hash::FxBuildHasher;
pub(crate) type SmallHasher = FxBuildHasher;
pub(crate) type SmallHashMap<A, B> = HashMap<A, B, SmallHasher>;

// thanks to https://qrng.anu.edu.au/ for some random bytes
pub(crate) const CONCURRENCY_MAGIC_BYTES: [u8; 4] = [0x13, 0xb2, 0x23, 0x09];

pub(crate) use crate::clock::Clock;
pub(crate) use crate::marks::OldMarkData;
pub(crate) use crate::value::{ScalarValue, Value};

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
pub struct ActorId(TinyVec<[u8; 16]>);

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ActorID")
            .field(&hex::encode(&self.0))
            .finish()
    }
}

impl Distribution<ActorId> for StandardUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> ActorId {
        let mut bytes = [0u8; 16];
        rng.fill(&mut bytes);
        ActorId(TinyVec::from(bytes))
    }
}

impl ActorId {
    pub fn random() -> ActorId {
        let mut buf = [0u8; 16];
        // getrandom 0.3 breaks node v18
        // keep using 0.2 until we stop supporting v18
        //getrandom::fill(&mut buf).expect("random number generator failed"); // 0.3 interface
        getrandom::fill(&mut buf).expect("random number generator failed"); // 0.2 interface
        ActorId(TinyVec::from(buf))
    }

    pub fn to_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_hex_string(&self) -> String {
        hex::encode(&self.0)
    }

    pub(crate) fn with_concurrency(&self, level: usize) -> ActorId {
        // 4 for magic bytes , 16 for leb128
        let mut bytes = Vec::with_capacity(self.0.len() + 4 + 16);
        bytes.extend(&CONCURRENCY_MAGIC_BYTES);
        leb128::write::unsigned(&mut bytes, level as u64).unwrap();
        bytes.extend(&self.0);
        ActorId(TinyVec::from(bytes.as_slice()))
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

/// The type of an object
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Copy, Hash, Default)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ObjType {
    /// A map
    #[default]
    Map,
    /// Retained for backwards compatibility, tables are identical to maps
    Table,
    /// A sequence of arbitrary values
    List,
    /// A sequence of characters
    Text,
}

impl ObjType {
    pub fn is_sequence(&self) -> bool {
        matches!(self, Self::List | Self::Text)
    }

    pub(crate) fn as_sequence_type(&self) -> Option<SequenceType> {
        match self {
            ObjType::List => Some(SequenceType::List),
            ObjType::Text => Some(SequenceType::Text),
            _ => None,
        }
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
    MarkBegin(bool, OldMarkData),
    MarkEnd(bool),
}

impl OpType {
    pub(crate) fn validate_action_and_value(
        action: u64,
        value: &ScalarValue,
    ) -> Result<(), error::InvalidOpType> {
        match action {
            0..=4 => Ok(()),
            5 => match value {
                ScalarValue::Int(_) | ScalarValue::Uint(_) => Ok(()),
                _ => Err(error::InvalidOpType::NonNumericInc),
            },
            6 => Ok(()),
            7 => Ok(()),
            _ => Err(error::InvalidOpType::UnknownAction(action)),
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
}

pub(crate) trait Exportable {
    fn export(&self) -> Export;
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

impl From<&OpId> for ObjId {
    fn from(o: &OpId) -> Self {
        ObjId(*o)
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

impl<'a> From<Cow<'a, str>> for Prop {
    fn from(p: Cow<'a, str>) -> Self {
        Prop::Map(p.into_owned())
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

impl From<&usize> for Prop {
    fn from(index: &usize) -> Self {
        Prop::Seq(*index)
    }
}

impl From<f64> for Prop {
    fn from(index: f64) -> Self {
        Prop::Seq(index as usize)
    }
}

/*
impl From<OpId> for Key {
    fn from(id: OpId) -> Self {
        Key::Seq(ElemId(id))
    }
}
*/

/*
impl From<ElemId> for Key {
    fn from(e: ElemId) -> Self {
        Key::Seq(e)
    }
}
*/

impl From<Option<ElemId>> for ElemId {
    fn from(e: Option<ElemId>) -> Self {
        e.unwrap_or(HEAD)
    }
}

/*
impl From<Option<ElemId>> for Key {
    fn from(e: Option<ElemId>) -> Self {
        Key::Seq(e.into())
    }
}
*/

/*
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Hash)]
pub(crate) enum Key {
    Map(usize),
    Seq(ElemId),
}
*/

/// A property of an object
///
/// This is either a string representing a property in a map, or an integer
/// which is the index into a sequence
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub enum Prop {
    /// A property in a map
    Map(String),
    /// An index into a sequence
    Seq(usize),
}

impl Prop {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Prop::Map(s) => Some(s),
            Prop::Seq(_) => None,
        }
    }

    pub fn as_index(&self) -> Option<usize> {
        match self {
            Prop::Map(_) => None,
            Prop::Seq(n) => Some(*n),
        }
    }
}

impl Display for Prop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Prop::Map(s) => write!(f, "{}", s),
            Prop::Seq(i) => write!(f, "{}", i),
        }
    }
}

/*
impl Key {
    pub(crate) fn prop_index(&self) -> Option<usize> {
        match self {
            Key::Map(n) => Some(*n),
            Key::Seq(_) => None,
        }
    }

    pub(crate) fn elemid(&self) -> Option<ElemId> {
        match self {
            Key::Map(_) => None,
            Key::Seq(id) => Some(*id),
        }
    }
}
*/

// FIXME - isn't having ord and partial ord here dangerous?
#[derive(Debug, Clone, PartialOrd, Ord, Eq, PartialEq, Copy, Hash, Default)]
pub(crate) struct OpId(u32, u32);

impl OpId {
    pub(crate) fn with_new_actor(self, idx: usize) -> Self {
        if self.actor() >= idx {
            OpId(self.0, self.1 + 1)
        } else {
            self
        }
    }

    pub(crate) fn without_actor(self, idx: usize) -> Option<Self> {
        match self.actor().cmp(&idx) {
            Ordering::Greater => Some(OpId(self.0, self.1 - 1)),
            Ordering::Equal => None,
            Ordering::Less => Some(self),
        }
    }

    pub(crate) fn new(counter: u64, actor: usize) -> Self {
        Self(counter.try_into().unwrap(), actor.try_into().unwrap())
    }

    pub(crate) fn map(&self, actor_map: &[usize]) -> Result<OpId, AutomergeError> {
        let actor = actor_map.get(self.actor()).cloned();
        let actor = actor.ok_or(AutomergeError::InvalidActorIndex(self.actor()))?;
        Ok(Self(self.0, actor as u32))
    }

    pub(crate) fn counter(&self) -> u64 {
        self.0.into()
    }

    pub(crate) fn icounter(&self) -> i64 {
        self.0.into()
    }

    pub(crate) fn actor(&self) -> usize {
        self.1 as usize
    }

    pub(crate) fn actoridx(&self) -> crate::op_set2::ActorIdx {
        crate::op_set2::ActorIdx(self.1)
    }

    #[inline]
    pub(crate) fn prev(&self) -> OpId {
        OpId(self.0 - 1, self.1)
    }

    #[inline]
    pub(crate) fn minus(&self, n: usize) -> OpId {
        OpId(self.0 - n as u32, self.1)
    }

    #[inline]
    pub(crate) fn next(&self) -> OpId {
        OpId(self.0 + 1, self.1)
    }
}

impl AsRef<OpId> for OpId {
    fn as_ref(&self) -> &OpId {
        self
    }
}

impl AsRef<OpId> for ObjId {
    fn as_ref(&self) -> &OpId {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord, Hash, Default)]
pub(crate) struct ObjId(pub(crate) OpId);

impl ObjId {
    pub(crate) fn with_new_actor(self, idx: usize) -> Self {
        if self.is_root() {
            self
        } else {
            ObjId(self.0.with_new_actor(idx))
        }
    }

    pub(crate) fn without_actor(self, idx: usize) -> Option<Self> {
        if self.is_root() {
            Some(self)
        } else {
            self.0.without_actor(idx).map(ObjId)
        }
    }

    pub(crate) fn load(counter: Option<u64>, actor: Option<ActorIdx>) -> Option<ObjId> {
        match (counter, actor) {
            (Some(c), Some(a)) => Some(ObjId(OpId::new(c, a.into()))),
            (None, None) => Some(ObjId::root()),
            _ => None, // panic? memory corruption here
        }
    }

    pub(crate) fn map(self, actor_map: &[usize]) -> Result<ObjId, AutomergeError> {
        if self.is_root() {
            Ok(self)
        } else {
            Ok(ObjId(self.0.map(actor_map)?))
        }
    }

    pub(crate) const fn root() -> Self {
        ObjId(OpId(0, 0))
    }

    pub(crate) fn is_root(&self) -> bool {
        self.0.counter() == 0
    }

    pub(crate) fn id(&self) -> Option<&OpId> {
        if self.is_root() {
            None
        } else {
            Some(&self.0)
        }
    }

    pub(crate) fn counter(&self) -> Option<u64> {
        if self.is_root() {
            None
        } else {
            Some(self.0.counter())
        }
    }

    pub(crate) fn icounter(&self) -> Option<i64> {
        if self.is_root() {
            None
        } else {
            Some(self.0.icounter())
        }
    }

    pub(crate) fn actor(&self) -> Option<ActorIdx> {
        if self.is_root() {
            None
        } else {
            Some(self.0.actoridx())
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct ObjMeta {
    pub(crate) id: ObjId,
    pub(crate) typ: ObjType,
}

impl From<ObjId> for ObjMeta {
    fn from(id: ObjId) -> Self {
        ObjMeta {
            id,
            typ: ObjType::Map,
        }
    }
}

impl ObjMeta {
    pub(crate) fn root() -> Self {
        Self {
            id: ObjId::root(),
            typ: ObjType::Map,
        }
    }
}

/// How the indexes into a string are counted
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TextEncoding {
    /// Each unicode code point counts as one unit
    UnicodeCodePoint,
    /// Each UTF-8 code unit counts as one unit (i.e. each byte in the utf-8 encoding of the string counts as one unit)
    Utf8CodeUnit,
    /// Each utf-16 code unit counts as one unit, (i.e. each byte in the utf-16 encoding of the string counts as one unit)
    Utf16CodeUnit,
    /// Each grapheme cluster counts as one unit
    GraphemeCluster,
}

impl TextEncoding {
    pub(crate) fn width(&self, s: &str) -> usize {
        match self {
            Self::UnicodeCodePoint => s.chars().count(),
            Self::Utf8CodeUnit => s.len(), // bytes
            Self::Utf16CodeUnit => s.encode_utf16().count(),
            Self::GraphemeCluster => {
                unicode_segmentation::UnicodeSegmentation::graphemes(s, true).count()
            }
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum SequenceType {
    List,
    Text,
}

#[derive(Debug, Clone, Copy, PartialOrd, Eq, PartialEq, Ord, Hash, Default)]
pub(crate) struct ElemId(pub(crate) OpId);

impl ElemId {
    pub(crate) fn map(self, actor_map: &[usize]) -> Result<ElemId, AutomergeError> {
        if self.is_head() {
            Ok(self)
        } else {
            Ok(ElemId(self.0.map(actor_map)?))
        }
    }

    pub(crate) fn icounter(&self) -> i64 {
        self.0.icounter()
    }

    pub(crate) fn actor(&self) -> Option<ActorIdx> {
        if self.is_head() {
            None
        } else {
            Some(self.0.actoridx())
        }
    }

    pub(crate) fn is_head(&self) -> bool {
        *self == HEAD
    }

    pub(crate) fn head() -> Self {
        Self(OpId(0, 0))
    }
}

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
            .field(&hex::encode(self.0))
            .finish()
    }
}

impl fmt::Display for ChangeHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
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

#[cfg(test)]
pub(crate) mod gen {
    //use super::{ChangeHash, ElemId, ObjType, OpId, OpType, ScalarValue, HASH_SIZE};
    //use crate::value::Counter;

    //use proptest::prelude::*;

    /*
        pub(crate) fn gen_hash() -> impl Strategy<Value = ChangeHash> {
            proptest::collection::vec(proptest::bits::u8::ANY, HASH_SIZE)
                .prop_map(|b| ChangeHash::try_from(&b[..]).unwrap())
        }

        pub(crate) fn gen_scalar_value() -> impl Strategy<Value = ScalarValue> {
            prop_oneof![
                proptest::collection::vec(proptest::bits::u8::ANY, 0..200).prop_map(ScalarValue::Bytes),
                "[a-z]{10,500}".prop_map(|s| ScalarValue::Str(s.into())),
                any::<i64>().prop_map(ScalarValue::Int),
                any::<u64>().prop_map(ScalarValue::Uint),
                any::<f64>().prop_map(ScalarValue::F64),
                any::<i64>().prop_map(|c| ScalarValue::Counter(Counter::from(c))),
                any::<i64>().prop_map(ScalarValue::Timestamp),
                any::<bool>().prop_map(ScalarValue::Boolean),
                Just(ScalarValue::Null),
            ]
        }

        pub(crate) fn gen_objtype() -> impl Strategy<Value = ObjType> {
            prop_oneof![
                Just(ObjType::Map),
                Just(ObjType::Table),
                Just(ObjType::List),
                Just(ObjType::Text),
            ]
        }

        pub(crate) fn gen_action() -> impl Strategy<Value = OpType> {
            prop_oneof![
                Just(OpType::Delete),
                any::<i64>().prop_map(OpType::Increment),
                gen_scalar_value().prop_map(OpType::Put),
                gen_objtype().prop_map(OpType::Make)
            ]
        }
    */

    /*
        pub(crate) fn gen_key(key_indices: Vec<usize>) -> impl Strategy<Value = Key> {
            prop_oneof![
                proptest::sample::select(key_indices).prop_map(Key::Map),
                Just(Key::Seq(ElemId(OpId::new(0, 0)))),
            ]
        }
    */

    /*
        /// Generate an arbitrary op
        ///
        /// The generated op will have no preds or succs
        ///
        /// # Arguments
        ///
        /// * `id` - the OpId this op will be given
        /// * `key_prop_indices` - The indices of props which will be used to generate keys of type
        ///    `Key::Map`. I.e. this is what would typically be in `OpSetMetadata::props
        pub(crate) fn gen_op(
            id: OpId,
            key_prop_indices: Vec<usize>,
        ) -> impl Strategy<Value = OpBuilder> {
            (gen_key(key_prop_indices), any::<bool>(), gen_action()).prop_map(
                move |(key, insert, action)| OpBuilder {
                    id,
                    key,
                    insert,
                    action,
                },
            )
        }
    */
}

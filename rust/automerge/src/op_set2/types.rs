use std::borrow::Cow;

use crate::error::AutomergeError;
use crate::types;
use crate::types::{ActorId, ChangeHash, ElemId, ObjType};
use crate::value;
use crate::{hydrate, TextEncoding};

use std::cmp::Ordering;
use std::fmt;

use super::hexane::{PackError, Packable, RleCursor, ScanMeta};
use super::meta::{ValueMeta, ValueType};

pub(crate) use super::meta::MetaCursor;

/// An index into an array of actors stored elsewhere
#[derive(Ord, PartialEq, Eq, Hash, PartialOrd, Debug, Clone, Default, Copy)]
pub(crate) struct ActorIdx(pub(crate) u32); // FIXME - shouldnt this be usize? (wasm is 32bit)

impl fmt::Display for ActorIdx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<usize> for ActorIdx {
    fn from(val: usize) -> Self {
        ActorIdx(val as u32)
    }
}

impl From<&usize> for ActorIdx {
    fn from(val: &usize) -> Self {
        ActorIdx((*val) as u32)
    }
}

impl From<u64> for ActorIdx {
    fn from(val: u64) -> Self {
        ActorIdx(val as u32)
    }
}

impl From<ActorIdx> for u64 {
    fn from(val: ActorIdx) -> Self {
        val.0 as u64
    }
}

impl From<ActorIdx> for usize {
    fn from(val: ActorIdx) -> Self {
        val.0 as usize
    }
}

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct MarkData<'a> {
    pub(crate) name: Cow<'a, str>,
    pub(crate) value: ScalarValue<'a>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
pub(crate) enum Action {
    #[default]
    MakeMap,
    MakeList,
    MakeText,
    Set,
    Delete,
    Increment,
    MakeTable,
    Mark,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MakeMap => write!(f, "MAP"),
            Self::MakeList => write!(f, "LST"),
            Self::MakeText => write!(f, "TXT"),
            Self::Set => write!(f, "SET"),
            Self::Delete => write!(f, "DEL"),
            Self::Increment => write!(f, "INC"),
            Self::MakeTable => write!(f, "TBL"),
            Self::Mark => write!(f, "MRK"),
        }
    }
}

impl From<Action> for u64 {
    fn from(val: Action) -> Self {
        match val {
            Action::MakeMap => 0,
            Action::Set => 1,
            Action::MakeList => 2,
            Action::Delete => 3,
            Action::MakeText => 4,
            Action::Increment => 5,
            Action::MakeTable => 6,
            Action::Mark => 7,
        }
    }
}

impl TryFrom<u64> for Action {
    type Error = PackError;

    fn try_from(action: u64) -> Result<Self, Self::Error> {
        match action {
            0 => Ok(Action::MakeMap),
            1 => Ok(Action::Set),
            2 => Ok(Action::MakeList),
            3 => Ok(Action::Delete),
            4 => Ok(Action::MakeText),
            5 => Ok(Action::Increment),
            6 => Ok(Action::MakeTable),
            7 => Ok(Action::Mark),
            other => Err(PackError::invalid_value(
                "valid action (integer between 0 and 7)",
                format!("unexpected integer: {}", other),
            )),
        }
    }
}

impl TryFrom<Action> for ObjType {
    type Error = AutomergeError;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        match action {
            Action::MakeMap => Ok(ObjType::Map),
            Action::MakeList => Ok(ObjType::List),
            Action::MakeText => Ok(ObjType::Text),
            _ => Err(AutomergeError::Fail),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum OpType<'a> {
    Make(ObjType),
    Delete,
    Increment(i64),
    Put(ScalarValue<'a>),
    MarkBegin(bool, MarkData<'a>),
    MarkEnd(bool),
}

impl<'a> OpType<'a> {
    pub(crate) fn from_action_and_value(
        action: Action,
        value: &ScalarValue<'a>,
        mark_name: &Option<Cow<'a, str>>,
        expand: bool,
    ) -> OpType<'a> {
        match action {
            Action::MakeMap => Self::Make(ObjType::Map),
            Action::MakeList => Self::Make(ObjType::List),
            Action::MakeText => Self::Make(ObjType::Text),
            Action::MakeTable => Self::Make(ObjType::Table),
            Action::Set => Self::Put(value.clone()),
            Action::Delete => Self::Delete,
            Action::Increment => match value {
                ScalarValue::Int(i) => Self::Increment(*i),
                ScalarValue::Uint(i) => Self::Increment(*i as i64),
                _ => unreachable!("validate_action_and_value returned NonNumericInc"),
            },
            Action::Mark => match mark_name {
                Some(name) => Self::MarkBegin(
                    expand,
                    MarkData {
                        name: name.clone(),
                        value: value.clone(),
                    },
                ),
                None => Self::MarkEnd(expand),
            },
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ScalarValue<'a> {
    Bytes(Cow<'a, [u8]>),
    Str(Cow<'a, str>),
    Int(i64),
    Uint(u64),
    F64(f64),
    Counter(i64),
    Timestamp(i64),
    Boolean(bool),
    Unknown { type_code: u8, bytes: Cow<'a, [u8]> },
    Null,
}

impl fmt::Display for ScalarValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Bytes(b) => write!(f, "\"{:?}\"", b),
            ScalarValue::Str(s) => write!(f, "\"{}\"", s),
            ScalarValue::Int(i) => write!(f, "{}", i),
            ScalarValue::Uint(i) => write!(f, "{}", i),
            ScalarValue::F64(n) => write!(f, "{:.2}", n),
            ScalarValue::Counter(c) => write!(f, "Counter: {}", c),
            ScalarValue::Timestamp(i) => write!(f, "Timestamp: {}", i),
            ScalarValue::Boolean(b) => write!(f, "{}", b),
            ScalarValue::Null => write!(f, "null"),
            ScalarValue::Unknown { type_code, .. } => write!(f, "unknown type {}", type_code),
        }
    }
}

impl<'a> From<ScalarValue<'a>> for types::ScalarValue {
    fn from(s: ScalarValue<'a>) -> Self {
        s.into_legacy()
    }
}

impl From<types::ScalarValue> for ScalarValue<'_> {
    fn from(s: types::ScalarValue) -> Self {
        s.into_ref()
    }
}

impl<'a> From<&ScalarValue<'a>> for types::ScalarValue {
    fn from(s: &ScalarValue<'a>) -> Self {
        s.to_owned()
    }
}

impl<'a> From<ValueRef<'a>> for types::Value<'static> {
    fn from(v: ValueRef<'a>) -> Self {
        v.into_value()
    }
}

impl<'a> ScalarValue<'a> {
    pub fn as_i64(&self) -> i64 {
        match self {
            Self::Int(i) | Self::Timestamp(i) => *i,
            Self::Counter(c) => *c,
            Self::Uint(i) => *i as i64,
            _ => 0,
        }
    }

    pub fn str(s: &'a str) -> Self {
        Self::Str(Cow::Borrowed(s))
    }

    pub(crate) fn to_owned(&self) -> types::ScalarValue {
        match self {
            Self::Bytes(b) => types::ScalarValue::Bytes(b.to_vec()),
            Self::Str(s) => types::ScalarValue::Str(s.to_string().into()),
            Self::Int(n) => types::ScalarValue::Int(*n),
            Self::Uint(n) => types::ScalarValue::Uint(*n),
            Self::F64(n) => types::ScalarValue::F64(*n),
            Self::Counter(n) => types::ScalarValue::Counter(n.into()),
            Self::Timestamp(n) => types::ScalarValue::Timestamp(*n),
            Self::Boolean(b) => types::ScalarValue::Boolean(*b),
            Self::Unknown { type_code, bytes } => types::ScalarValue::Unknown {
                type_code: *type_code,
                bytes: bytes.to_vec(),
            },
            Self::Null => types::ScalarValue::Null,
        }
    }

    pub(crate) fn into_legacy(self) -> types::ScalarValue {
        match self {
            Self::Bytes(b) => types::ScalarValue::Bytes(b.to_vec()),
            Self::Str(s) => types::ScalarValue::Str(s.to_string().into()),
            Self::Int(n) => types::ScalarValue::Int(n),
            Self::Uint(n) => types::ScalarValue::Uint(n),
            Self::F64(n) => types::ScalarValue::F64(n),
            Self::Counter(n) => types::ScalarValue::Counter(n.into()),
            Self::Timestamp(n) => types::ScalarValue::Timestamp(n),
            Self::Boolean(b) => types::ScalarValue::Boolean(b),
            Self::Unknown { type_code, bytes } => types::ScalarValue::Unknown {
                type_code,
                bytes: bytes.to_vec(),
            },
            Self::Null => types::ScalarValue::Null,
        }
    }

    pub(crate) fn into_owned(self) -> ScalarValue<'static> {
        match self {
            Self::Bytes(b) => ScalarValue::Bytes(Cow::Owned(b.into_owned())),
            Self::Str(s) => ScalarValue::Str(Cow::Owned(s.into_owned())),
            Self::Int(n) => ScalarValue::Int(n),
            Self::Uint(n) => ScalarValue::Uint(n),
            Self::F64(n) => ScalarValue::F64(n),
            Self::Counter(n) => ScalarValue::Counter(n),
            Self::Timestamp(n) => ScalarValue::Timestamp(n),
            Self::Boolean(b) => ScalarValue::Boolean(b),
            Self::Unknown { type_code, bytes } => ScalarValue::Unknown {
                type_code,
                bytes: Cow::Owned(bytes.into_owned()),
            },
            Self::Null => ScalarValue::Null,
        }
    }

    pub(crate) fn from_raw(
        meta: super::meta::ValueMeta,
        raw: &'a [u8],
    ) -> Result<Self, ReadScalarError> {
        match meta.type_code() {
            ValueType::Null => Ok(ScalarValue::Null),
            ValueType::False => Ok(ScalarValue::Boolean(false)),
            ValueType::True => Ok(ScalarValue::Boolean(true)),
            ValueType::Uleb => Ok(ScalarValue::Uint(parse_uleb128(raw)?)),
            ValueType::Leb => Ok(ScalarValue::Int(parse_leb128(raw)?)),
            ValueType::Float => {
                let float_bytes: [u8; 8] = raw.try_into().map_err(|_| ReadScalarError::Float)?;
                Ok(ScalarValue::F64(f64::from_le_bytes(float_bytes)))
            }
            ValueType::String => {
                let s = std::str::from_utf8(raw).map_err(|_| ReadScalarError::Str)?;
                Ok(ScalarValue::Str(Cow::Borrowed(s)))
            }
            ValueType::Bytes => Ok(ScalarValue::Bytes(Cow::Borrowed(raw))),
            ValueType::Counter => Ok(ScalarValue::Counter(parse_leb128(raw)?)),
            ValueType::Timestamp => Ok(ScalarValue::Timestamp(parse_leb128(raw)?)),
            ValueType::Unknown(u8) => Ok(ScalarValue::Unknown {
                type_code: u8,
                bytes: Cow::Borrowed(raw),
            }),
        }
    }

    pub(crate) fn to_raw(&self) -> Option<Cow<'a, [u8]>> {
        match self {
            Self::Bytes(b) => Some(b.clone()),
            Self::Str(Cow::Borrowed(s)) => Some(Cow::Borrowed(s.as_bytes())),
            Self::Str(Cow::Owned(s)) => Some(Cow::Owned(s.as_bytes().to_vec())),
            Self::Null => None,
            Self::Boolean(_) => None,
            Self::Uint(i) => {
                let mut out = Vec::new();
                leb128::write::unsigned(&mut out, *i).unwrap();
                Some(Cow::Owned(out))
            }
            Self::Int(i) | Self::Counter(i) | Self::Timestamp(i) => {
                let mut out = Vec::new();
                leb128::write::signed(&mut out, *i).unwrap();
                Some(Cow::Owned(out))
            }
            Self::F64(f) => {
                let mut out = Vec::new();
                out.extend_from_slice(&f.to_le_bytes());
                Some(Cow::Owned(out))
            }
            Self::Unknown { bytes, .. } => Some(bytes.clone()),
        }
    }

    pub(super) fn as_raw(&self) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Bytes(Cow::Borrowed(b)) => Some(Cow::Borrowed(b)),
            Self::Bytes(Cow::Owned(b)) => Some(Cow::Borrowed(b.as_slice())),
            Self::Str(Cow::Borrowed(s)) => Some(Cow::Borrowed(s.as_bytes())),
            Self::Str(Cow::Owned(s)) => Some(Cow::Borrowed(s.as_bytes())),
            Self::Null => None,
            Self::Boolean(_) => None,
            Self::Uint(i) => {
                let mut out = Vec::new();
                leb128::write::unsigned(&mut out, *i).unwrap();
                Some(Cow::Owned(out))
            }
            Self::Int(i) | Self::Counter(i) | Self::Timestamp(i) => {
                let mut out = Vec::new();
                leb128::write::signed(&mut out, *i).unwrap();
                Some(Cow::Owned(out))
            }
            Self::F64(f) => {
                let mut out = Vec::new();
                out.extend_from_slice(&f.to_le_bytes());
                Some(Cow::Owned(out))
            }
            Self::Unknown { bytes, .. } => Some(bytes.clone()),
        }
    }

    pub(crate) fn meta(&self) -> ValueMeta {
        ValueMeta::from(self)
    }
}

// FIXME - this is a temporary fix - we ideally want
// to be writing the bytes directly into memory
// vs into a temp vec and then into memory

impl crate::types::OpType {
    pub(crate) fn decompose(
        self,
    ) -> (
        Action,
        ScalarValue<'static>,
        bool,
        Option<Cow<'static, str>>,
    ) {
        match self {
            Self::Make(ObjType::Map) => (Action::MakeMap, ScalarValue::Null, false, None),
            Self::Make(ObjType::List) => (Action::MakeList, ScalarValue::Null, false, None),
            Self::Make(ObjType::Text) => (Action::MakeText, ScalarValue::Null, false, None),
            Self::Make(ObjType::Table) => (Action::MakeTable, ScalarValue::Null, false, None),
            Self::Delete => (Action::Delete, ScalarValue::Null, false, None),
            Self::Increment(i) => (Action::Increment, ScalarValue::Int(i), false, None),
            Self::Put(val) => (Action::Set, val.into_ref(), false, None),
            Self::MarkBegin(expand, md) => (
                Action::Mark,
                md.value.into_ref(),
                expand,
                Some(Cow::Owned(String::from(md.name))),
            ),
            Self::MarkEnd(expand) => (Action::Mark, ScalarValue::Null, expand, None),
        }
    }
}

impl crate::types::ScalarValue {
    pub(crate) fn into_ref(self) -> ScalarValue<'static> {
        match self {
            Self::Bytes(b) => ScalarValue::Bytes(Cow::Owned(b)),
            Self::Str(s) => ScalarValue::Str(Cow::Owned(String::from(s))),
            Self::Int(i) => ScalarValue::Int(i),
            Self::Uint(i) => ScalarValue::Uint(i),
            Self::F64(n) => ScalarValue::F64(n),
            Self::Counter(c) => ScalarValue::Counter(c.into()),
            Self::Timestamp(i) => ScalarValue::Timestamp(i),
            Self::Boolean(b) => ScalarValue::Boolean(b),
            Self::Null => ScalarValue::Null,
            Self::Unknown { type_code, bytes } => ScalarValue::Unknown {
                type_code,
                bytes: Cow::Owned(bytes),
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadScalarError {
    #[error("invalid uleb128")]
    Uleb,
    #[error("invalid leb128")]
    Leb,
    #[error("invalid float")]
    Float,
    #[error("invalid string")]
    Str,
}

impl From<crate::storage::parse::leb128::Error> for ReadScalarError {
    fn from(_: crate::storage::parse::leb128::Error) -> Self {
        ReadScalarError::Uleb
    }
}

fn parse_uleb128(input: &[u8]) -> Result<u64, ReadScalarError> {
    crate::storage::parse::leb128_u64::<ReadScalarError>(crate::storage::parse::Input::new(input))
        .map(|(_, v)| v)
        .map_err(|_| ReadScalarError::Leb)
}

fn parse_leb128(input: &[u8]) -> Result<i64, ReadScalarError> {
    crate::storage::parse::leb128_i64::<ReadScalarError>(crate::storage::parse::Input::new(input))
        .map(|(_, v)| v)
        .map_err(|_| ReadScalarError::Leb)
}

impl PartialEq<ScalarValue<'_>> for types::ScalarValue {
    fn eq(&self, other: &ScalarValue<'_>) -> bool {
        other.eq(self)
    }
}

impl PartialEq<types::ScalarValue> for ScalarValue<'_> {
    fn eq(&self, other: &types::ScalarValue) -> bool {
        match (self, other) {
            (ScalarValue::Bytes(a), types::ScalarValue::Bytes(b)) => a == &b.as_slice(),
            (ScalarValue::Str(a), types::ScalarValue::Str(b)) => **a == **b,
            (ScalarValue::Int(a), types::ScalarValue::Int(b)) => a == b,
            (ScalarValue::Uint(a), types::ScalarValue::Uint(b)) => a == b,
            (ScalarValue::F64(a), types::ScalarValue::F64(b)) => a == b,
            (ScalarValue::Counter(a), types::ScalarValue::Counter(b)) => *a == i64::from(b),
            (ScalarValue::Timestamp(a), types::ScalarValue::Timestamp(b)) => a == b,
            (ScalarValue::Boolean(a), types::ScalarValue::Boolean(b)) => a == b,
            (ScalarValue::Null, types::ScalarValue::Null) => true,
            (
                ScalarValue::Unknown {
                    type_code: a1,
                    bytes: a2,
                },
                types::ScalarValue::Unknown {
                    type_code: b1,
                    bytes: b2,
                },
            ) => a1 == b1 && a2 == b2,
            _ => false,
        }
    }
}

impl PartialEq<types::OldMarkData> for MarkData<'_> {
    fn eq(&self, other: &types::OldMarkData) -> bool {
        *self.name == *other.name && self.value == other.value
    }
}

impl PartialEq<types::OpType> for OpType<'_> {
    fn eq(&self, other: &types::OpType) -> bool {
        match (self, other) {
            (OpType::Make(a), types::OpType::Make(b)) => a == b,
            (OpType::Delete, types::OpType::Delete) => true,
            (OpType::Increment(a), types::OpType::Increment(b)) => a == b,
            (OpType::Put(a), types::OpType::Put(b)) => a == b,
            (OpType::MarkBegin(a1, a2), types::OpType::MarkBegin(b1, b2)) => a1 == b1 && a2 == b2,
            (OpType::MarkEnd(a), types::OpType::MarkEnd(b)) => a == b,
            _ => false,
        }
    }
}

impl<'a> PartialEq<OpType<'a>> for types::OpType {
    fn eq(&self, other: &OpType<'a>) -> bool {
        other.eq(self)
    }
}

impl From<u64> for ScalarValue<'_> {
    fn from(n: u64) -> Self {
        ScalarValue::Uint(n)
    }
}

impl From<u32> for ScalarValue<'_> {
    fn from(n: u32) -> Self {
        ScalarValue::Uint(n as u64)
    }
}

impl From<i64> for ScalarValue<'_> {
    fn from(n: i64) -> Self {
        ScalarValue::Int(n)
    }
}

impl From<i32> for ScalarValue<'_> {
    fn from(n: i32) -> Self {
        ScalarValue::Int(n as i64)
    }
}

impl<'a> From<&'a str> for ScalarValue<'a> {
    fn from(s: &'a str) -> Self {
        ScalarValue::Str(Cow::Borrowed(s))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PropRef<'a> {
    Map(Cow<'a, str>),
    Seq(usize),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyRef<'a> {
    Map(Cow<'a, str>),
    Seq(ElemId),
}

impl PartialOrd for KeyRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Map(s1), Self::Map(s2)) => Some(s1.cmp(s2)),
            (Self::Seq(e1), Self::Seq(e2)) => Some(e1.cmp(e2)),
            _ => None,
        }
    }
}

impl From<ElemId> for KeyRef<'static> {
    fn from(e: ElemId) -> KeyRef<'static> {
        KeyRef::Seq(e)
    }
}

impl From<String> for KeyRef<'static> {
    fn from(s: String) -> KeyRef<'static> {
        KeyRef::Map(Cow::Owned(s))
    }
}

impl<'a> KeyRef<'a> {
    #[allow(dead_code)]
    pub(crate) fn as_ref<'b>(key: &'b KeyRef<'_>) -> KeyRef<'b> {
        match key {
            KeyRef::Map(Cow::Owned(s)) => KeyRef::Map(Cow::Borrowed(s)),
            KeyRef::Map(Cow::Borrowed(s)) => KeyRef::Map(Cow::Borrowed(s)),
            KeyRef::Seq(s) => KeyRef::Seq(*s),
        }
    }

    pub(crate) fn actor(&self) -> Option<ActorIdx> {
        match self {
            KeyRef::Map(_) => None,
            KeyRef::Seq(e) => e.actor(),
        }
    }

    pub(crate) fn icounter(&self) -> Option<i64> {
        match self {
            KeyRef::Map(_) => None,
            KeyRef::Seq(e) => Some(e.icounter()),
        }
    }

    pub(crate) fn key_str(&self) -> Option<Cow<'a, str>> {
        match self {
            Self::Map(s) => Some(s.clone()),
            Self::Seq(_) => None,
        }
    }

    pub(crate) fn elemid(&self) -> Option<ElemId> {
        match self {
            KeyRef::Map(_) => None,
            KeyRef::Seq(e) => Some(*e),
        }
    }
}

impl types::Exportable for KeyRef<'_> {
    fn export(&self) -> types::Export {
        match self {
            KeyRef::Map(p) => types::Export::Special(p.to_string()),
            KeyRef::Seq(e) => e.export(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueRef<'a> {
    Object(ObjType),
    Scalar(ScalarValue<'a>),
}

impl<'a, A: Into<ScalarValue<'a>>> From<A> for ValueRef<'a> {
    fn from(a: A) -> Self {
        ValueRef::Scalar(a.into())
    }
}

impl<'a> ValueRef<'a> {
    pub(crate) fn from_action_value(action: Action, value: ScalarValue<'a>) -> Self {
        match action {
            Action::MakeMap => ValueRef::Object(ObjType::Map),
            Action::MakeList => ValueRef::Object(ObjType::List),
            Action::MakeText => ValueRef::Object(ObjType::Text),
            Action::MakeTable => ValueRef::Object(ObjType::Table),
            _ => ValueRef::Scalar(value),
        }
    }

    pub(crate) fn hydrate(self, encoding: TextEncoding) -> hydrate::Value {
        match self {
            Self::Object(ObjType::Map) => hydrate::Value::map(),
            Self::Object(ObjType::Table) => hydrate::Value::map(),
            Self::Object(ObjType::List) => hydrate::Value::list(),
            Self::Object(ObjType::Text) => hydrate::Value::text(encoding, ""),
            Self::Scalar(s) => hydrate::Value::Scalar(s.into()),
        }
    }

    pub(crate) fn into_owned(self) -> ValueRef<'static> {
        match self {
            Self::Object(o) => ValueRef::Object(o),
            Self::Scalar(s) => ValueRef::Scalar(s.into_owned()),
        }
    }

    pub fn into_value(self) -> value::Value<'static> {
        match self {
            Self::Object(o) => value::Value::Object(o),
            Self::Scalar(s) => value::Value::Scalar(Cow::Owned(s.into_legacy())),
        }
    }

    pub(crate) fn to_value(&self) -> value::Value<'static> {
        match self {
            Self::Object(o) => value::Value::Object(*o),
            Self::Scalar(s) => value::Value::Scalar(Cow::Owned(s.into())),
        }
    }

    pub(crate) fn str(s: &'a str) -> ValueRef<'a> {
        Self::Scalar(ScalarValue::str(s))
    }

    pub(crate) fn is_object(&self) -> bool {
        matches!(self, Self::Object(_))
    }
}

impl Packable for Action {
    fn width(item: &Action) -> usize {
        hexane::ulebsize(u64::from(*item)) as usize
    }

    fn pack(item: &Action, out: &mut Vec<u8>) {
        leb128::write::unsigned(out, u64::from(*item)).unwrap();
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError> {
        let (len, result) = u64::unpack(buff)?;
        let action = Action::try_from(*result)?;
        Ok((len, Cow::Owned(action)))
    }
}

impl Packable for ActorIdx {
    fn width(item: &ActorIdx) -> usize {
        hexane::ulebsize(u64::from(*item)) as usize
    }

    fn pack(item: &ActorIdx, out: &mut Vec<u8>) {
        leb128::write::unsigned(out, u64::from(*item)).unwrap();
    }

    fn validate(val: Option<&Self>, m: &ScanMeta) -> Result<(), PackError> {
        if let Some(&ActorIdx(a)) = val {
            if a >= m.actors as u32 {
                // FIXME - PackError shouldnt know about Actors
                return Err(PackError::ActorIndexOutOfRange(a as u64, m.actors));
            }
        }
        Ok(())
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'static, Self>), PackError> {
        let (len, result) = u64::unpack(buff)?;
        Ok((len, Cow::Owned(ActorIdx::from(*result))))
    }
}

pub(crate) type ActorCursor = RleCursor<64, ActorIdx>;
pub(crate) type ActionCursor = RleCursor<64, Action>;

#[derive(PartialEq, Debug, Clone)]
pub struct ChangeMetadata<'a> {
    pub actor: Cow<'a, ActorId>,
    pub seq: u64,
    pub start_op: u64,
    pub max_op: u64,
    pub timestamp: i64,
    pub message: Option<Cow<'a, str>>,
    pub deps: Vec<ChangeHash>,
    pub hash: ChangeHash,
    pub extra: Cow<'a, [u8]>,
}

impl ChangeMetadata<'_> {
    pub fn into_owned(self) -> ChangeMetadata<'static> {
        ChangeMetadata {
            actor: Cow::Owned(self.actor.into_owned()),
            seq: self.seq,
            start_op: self.start_op,
            max_op: self.max_op,
            timestamp: self.timestamp,
            message: self.message.map(|s| Cow::Owned(s.into_owned())),
            deps: self.deps,
            hash: self.hash,
            extra: Cow::Owned(self.extra.into_owned()),
        }
    }
}

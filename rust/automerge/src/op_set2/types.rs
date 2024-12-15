use std::borrow::Cow;

use crate::error::AutomergeError;
use crate::types;
use crate::types::{ElemId, ObjType, OldMarkData};
use crate::value;

use std::fmt;

use super::meta::ValueType;
use super::packer::{PackError, Packable, RleCursor, ScanMeta, WriteOp};

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

impl<'a> MarkData<'a> {
    fn into_owned(self) -> OldMarkData {
        OldMarkData {
            name: self.name.into(),
            value: self.value.into_owned(),
        }
    }
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

impl crate::types::OpType {
    pub(crate) fn action(&self) -> Action {
        match self {
            Self::Make(ObjType::Map) => Action::MakeMap,
            Self::Put(_) => Action::Set,
            Self::Make(ObjType::List) => Action::MakeList,
            Self::Delete => Action::Delete,
            Self::Make(ObjType::Text) => Action::MakeText,
            Self::Increment(_) => Action::Increment,
            Self::Make(ObjType::Table) => Action::MakeTable,
            Self::MarkBegin(_, _) | Self::MarkEnd(_) => Action::Mark,
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
    pub(crate) fn into_owned(self) -> types::OpType {
        match self {
            Self::Make(t) => types::OpType::Make(t),
            Self::Delete => types::OpType::Delete,
            Self::Increment(i) => types::OpType::Increment(i),
            Self::Put(v) => types::OpType::Put(v.into_owned()),
            Self::MarkBegin(ex, mark) => types::OpType::MarkBegin(ex, mark.into_owned()),
            Self::MarkEnd(ex) => types::OpType::MarkEnd(ex),
        }
    }

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
            //_ => unreachable!("validate_action_and_value returned UnknownAction"),
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

impl<'a> fmt::Display for ScalarValue<'a> {
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
        s.into_owned()
    }
}

impl<'a> From<&ScalarValue<'a>> for types::ScalarValue {
    fn from(s: &ScalarValue<'a>) -> Self {
        s.to_owned()
    }
}

impl<'a> From<Value<'a>> for types::Value<'static> {
    fn from(v: Value<'a>) -> Self {
        v.into_owned()
    }
}

impl<'a> ScalarValue<'a> {
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

    pub(crate) fn into_owned(self) -> types::ScalarValue {
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

    pub(super) fn from_raw(
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

    pub(super) fn to_raw(&self) -> Option<Cow<'a, [u8]>> {
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
                //let mut out = [8; u8];//Vec::new();
                leb128::write::signed(&mut out, *i).unwrap();
                Some(Cow::Owned(out))
            }
            Self::F64(f) => {
                let mut out = Vec::new();
                out.extend_from_slice(&f.to_le_bytes());
                Some(Cow::Owned(out))
            }
            Self::Unknown {
                type_code: _,
                bytes,
            } => Some(bytes.clone()),
        }
    }

    /*
        pub(crate) fn as_i64(&self) -> i64 {
            match self {
                Self::Int(i) | Self::Counter(i) | Self::Timestamp(i) => *i,
                Self::Uint(i) => *i as i64,
                _ => 0,
            }
        }
    */
}

// FIXME - this is a temporary fix - we ideally want
// to be writing the bytes directly into memory
// vs into a temp vec and then into memory

impl crate::types::OpType {
    pub(crate) fn to_raw(&self) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Put(v) => v.to_raw(),
            Self::Increment(i) => {
                let mut out = Vec::new();
                leb128::write::signed(&mut out, *i).unwrap();
                Some(Cow::Owned(out))
            }
            Self::MarkBegin(_, crate::types::OldMarkData { value, .. }) => value.to_raw(),
            _ => None,
        }
    }
}

impl crate::types::ScalarValue {
    pub(super) fn to_raw(&self) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Bytes(b) => Some(Cow::Borrowed(b)),
            Self::Str(s) => Some(Cow::Borrowed(s.as_bytes())),
            Self::Null => None,
            Self::Boolean(_) => None,
            Self::Uint(i) => {
                let mut out = Vec::new();
                leb128::write::unsigned(&mut out, *i).unwrap();
                Some(Cow::Owned(out))
            }
            Self::Counter(i) => {
                let mut out = Vec::new();
                leb128::write::signed(&mut out, i.start).unwrap();
                Some(Cow::Owned(out))
            }
            Self::Int(i) | Self::Timestamp(i) => {
                let mut out = Vec::new();
                leb128::write::signed(&mut out, *i).unwrap();
                Some(Cow::Owned(out))
            }
            Self::F64(f) => {
                let mut out = Vec::new();
                out.extend_from_slice(&f.to_le_bytes());
                Some(Cow::Owned(out))
            }
            Self::Unknown {
                type_code: _,
                bytes,
            } => Some(Cow::Borrowed(bytes)),
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

impl<'a> PartialEq<types::ScalarValue> for ScalarValue<'a> {
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

impl<'a> PartialEq<types::OldMarkData> for MarkData<'a> {
    fn eq(&self, other: &types::OldMarkData) -> bool {
        *self.name == *other.name && self.value == other.value
    }
}

impl<'a> PartialEq<types::OpType> for OpType<'a> {
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

impl<'a> From<u64> for ScalarValue<'a> {
    fn from(n: u64) -> Self {
        ScalarValue::Uint(n)
    }
}

impl<'a> From<i64> for ScalarValue<'a> {
    fn from(n: i64) -> Self {
        ScalarValue::Int(n)
    }
}

#[derive(Clone, Debug, Copy, PartialEq)]
pub(crate) enum PropRef<'a> {
    Map(&'a str),
    Seq(usize),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyRef<'a> {
    Map(Cow<'a, str>),
    Seq(ElemId),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Key {
    Map(String),
    Seq(ElemId),
}

impl Key {
    pub(crate) fn key_str(&self) -> Option<Cow<'static, str>> {
        match self {
            Self::Map(s) => Some(Cow::Owned(s.to_owned())),
            Self::Seq(_) => None,
        }
    }
    pub(crate) fn actor(&self) -> Option<ActorIdx> {
        match self {
            Self::Map(_) => None,
            Self::Seq(e) => e.actor(),
        }
    }

    pub(crate) fn icounter(&self) -> Option<i64> {
        match self {
            Self::Map(_) => None,
            Self::Seq(e) => Some(e.icounter()),
        }
    }
}

impl From<ElemId> for Key {
    fn from(e: ElemId) -> Key {
        Key::Seq(e)
    }
}

impl From<String> for Key {
    fn from(s: String) -> Key {
        Key::Map(s)
    }
}

impl<'a> KeyRef<'a> {
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
    pub(crate) fn into_owned(self) -> Key {
        match self {
            KeyRef::Map(s) => Key::Map(s.to_string()),
            KeyRef::Seq(e) => Key::Seq(e),
        }
    }

    pub(crate) fn key_str(&self) -> Option<Cow<'a, str>> {
        match self {
            KeyRef::Map(s) => Some(s.clone()),
            KeyRef::Seq(_) => None,
        }
    }

    pub(crate) fn elemid(&self) -> Option<ElemId> {
        match self {
            KeyRef::Map(_) => None,
            KeyRef::Seq(e) => Some(*e),
        }
    }
}

impl<'a> types::Exportable for KeyRef<'a> {
    fn export(&self) -> types::Export {
        match self {
            KeyRef::Map(p) => types::Export::Special(p.to_string()),
            KeyRef::Seq(e) => e.export(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Object(ObjType),
    Scalar(ScalarValue<'a>),
}

impl<'a> Value<'a> {
    pub(crate) fn into_owned(self) -> value::Value<'static> {
        match self {
            Self::Object(o) => value::Value::Object(o),
            Self::Scalar(s) => value::Value::Scalar(Cow::Owned(s.into_owned())),
        }
    }
}

impl Packable for Action {
    fn width(item: &Action) -> usize {
        packer::ulebsize(u64::from(*item)) as usize
    }

    fn pack(item: &Action, out: &mut Vec<u8>) {
        leb128::write::unsigned(out, u64::from(*item)).unwrap();
    }

    fn unpack(buff: &[u8]) -> Result<(usize, Cow<'_, Self>), PackError> {
        let (len, result) = u64::unpack(buff)?;
        let action = match *result {
            0 => Action::MakeMap,
            1 => Action::Set,
            2 => Action::MakeList,
            3 => Action::Delete,
            4 => Action::MakeText,
            5 => Action::Increment,
            6 => Action::MakeTable,
            7 => Action::Mark,
            other => {
                return Err(PackError::invalid_value(
                    "valid action (integer between 0 and 7)",
                    format!("unexpected integer: {}", other),
                ))
            }
        };
        Ok((len, Cow::Owned(action)))
    }
}

impl Packable for ActorIdx {
    fn width(item: &ActorIdx) -> usize {
        packer::ulebsize(u64::from(*item)) as usize
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

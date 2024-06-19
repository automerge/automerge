use std::borrow::Cow;

use crate::error::AutomergeError;
use crate::types;
use crate::types::{ElemId, ObjType};

use std::fmt;
use std::ops::{Bound, RangeBounds};

use super::meta::ValueType;

/// An index into an array of actors stored elsewhere
#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) struct ActorIdx(pub(crate) u64); // FIXME - shouldnt this be usize? (wasm is 32bit)

impl From<usize> for ActorIdx {
    fn from(val: usize) -> Self {
        ActorIdx(val as u64)
    }
}

impl From<u64> for ActorIdx {
    fn from(val: u64) -> Self {
        ActorIdx(val)
    }
}

impl From<ActorIdx> for u64 {
    fn from(val: ActorIdx) -> Self {
        val.0
    }
}

impl From<ActorIdx> for usize {
    fn from(val: ActorIdx) -> Self {
        val.0 as usize
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) struct MarkData<'a> {
    pub(crate) name: &'a str,
    pub(crate) value: ScalarValue<'a>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Action {
    MakeMap,
    MakeList,
    MakeText,
    Set,
    Delete,
    Increment,
    MakeTable,
    Mark,
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
        value: ScalarValue<'a>,
        mark_name: Option<&'a str>,
        expand: bool,
    ) -> OpType<'a> {
        match action {
            Action::MakeMap => Self::Make(ObjType::Map),
            Action::MakeList => Self::Make(ObjType::List),
            Action::MakeText => Self::Make(ObjType::Text),
            Action::MakeTable => Self::Make(ObjType::Table),
            Action::Set => Self::Put(value),
            Action::Delete => Self::Delete,
            Action::Increment => match value {
                ScalarValue::Int(i) => Self::Increment(i),
                ScalarValue::Uint(i) => Self::Increment(i as i64),
                _ => unreachable!("validate_action_and_value returned NonNumericInc"),
            },
            Action::Mark => match mark_name {
                Some(name) => Self::MarkBegin(expand, MarkData { name, value }),
                None => Self::MarkEnd(expand),
            },
            //_ => unreachable!("validate_action_and_value returned UnknownAction"),
        }
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum ScalarValue<'a> {
    Bytes(&'a [u8]),
    Str(&'a str),
    Int(i64),
    Uint(u64),
    F64(f64),
    Counter(i64),
    Timestamp(i64),
    Boolean(bool),
    Unknown { type_code: u8, bytes: &'a [u8] },
    Null,
}

impl<'a> fmt::Display for ScalarValue<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Bytes(b) => write!(f, "\"{:?}\"", b),
            ScalarValue::Str(s) => write!(f, "\"{}\"", s),
            ScalarValue::Int(i) => write!(f, "{}", i),
            ScalarValue::Uint(i) => write!(f, "{}", i),
            ScalarValue::F64(n) => write!(f, "{:.324}", n),
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

impl<'a> From<&'a types::ScalarValue> for ScalarValue<'a> {
    fn from(s: &'a types::ScalarValue) -> Self {
        match s {
            types::ScalarValue::Bytes(b) => ScalarValue::Bytes(b.as_slice()),
            types::ScalarValue::Str(s) => ScalarValue::Str(s.as_str()),
            types::ScalarValue::Int(n) => ScalarValue::Int(*n),
            types::ScalarValue::Uint(n) => ScalarValue::Uint(*n),
            types::ScalarValue::F64(n) => ScalarValue::F64(*n),
            types::ScalarValue::Counter(n) => ScalarValue::Counter(n.into()),
            types::ScalarValue::Timestamp(n) => ScalarValue::Timestamp(*n),
            types::ScalarValue::Boolean(b) => ScalarValue::Boolean(*b),
            types::ScalarValue::Unknown { type_code, bytes } => ScalarValue::Unknown {
                type_code: *type_code,
                bytes: bytes.as_slice(),
            },
            types::ScalarValue::Null => ScalarValue::Null,
        }
    }
}

impl<'a> ScalarValue<'a> {
    pub(crate) fn into_owned(&self) -> types::ScalarValue {
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
                let float_bytes: [u8; 8] =
                    raw.try_into().map_err(|_| ReadScalarError::InvalidFloat)?;
                Ok(ScalarValue::F64(f64::from_le_bytes(float_bytes)))
            }
            ValueType::String => {
                let s = std::str::from_utf8(raw).map_err(|_| ReadScalarError::InvalidStr)?;
                Ok(ScalarValue::Str(s))
            }
            ValueType::Bytes => Ok(ScalarValue::Bytes(raw)),
            ValueType::Counter => Ok(ScalarValue::Counter(parse_leb128(raw)?)),
            ValueType::Timestamp => Ok(ScalarValue::Timestamp(parse_leb128(raw)?)),
            ValueType::Unknown(u8) => Ok(ScalarValue::Unknown {
                type_code: u8,
                bytes: raw,
            }),
        }
    }

    pub(super) fn to_raw(&self) -> Option<Cow<'a, [u8]>> {
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
            Self::Unknown {
                type_code: _,
                bytes,
            } => Some(Cow::Borrowed(bytes)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadScalarError {
    #[error("invalid type code: {0}")]
    InvalidTypeCode(u8),
    #[error("invalid uleb128")]
    InvalidUleb,
    #[error("invalid leb128")]
    InvalidLeb,
    #[error("invalid float")]
    InvalidFloat,
    #[error("invalid string")]
    InvalidStr,
}

impl From<crate::storage::parse::leb128::Error> for ReadScalarError {
    fn from(_: crate::storage::parse::leb128::Error) -> Self {
        ReadScalarError::InvalidUleb
    }
}

fn parse_uleb128(input: &[u8]) -> Result<u64, ReadScalarError> {
    crate::storage::parse::leb128_u64::<ReadScalarError>(crate::storage::parse::Input::new(input))
        .map(|(_, v)| v)
        .map_err(|_| ReadScalarError::InvalidLeb)
}

fn parse_leb128(input: &[u8]) -> Result<i64, ReadScalarError> {
    crate::storage::parse::leb128_i64::<ReadScalarError>(crate::storage::parse::Input::new(input))
        .map(|(_, v)| v)
        .map_err(|_| ReadScalarError::InvalidLeb)
}

impl<'a> PartialEq<types::ScalarValue> for ScalarValue<'a> {
    fn eq(&self, other: &types::ScalarValue) -> bool {
        match (self, other) {
            (ScalarValue::Bytes(a), types::ScalarValue::Bytes(b)) => a == &b.as_slice(),
            (ScalarValue::Str(a), types::ScalarValue::Str(b)) => a == &b,
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
pub(crate) enum Key<'a> {
    Map(&'a str), // at this point we don't care if its valid UTF8
    Seq(ElemId),
}

impl<'a> Key<'a> {
    pub(crate) fn map_key(&self) -> Option<&'a str> {
        match self {
            Key::Map(s) => Some(s),
            Key::Seq(_) => None,
        }
    }
}

impl<'a> types::Exportable for Key<'a> {
    fn export(&self) -> types::Export {
        match self {
            Key::Map(p) => types::Export::Special(String::from(*p)),
            Key::Seq(e) => e.export(),
        }
    }
}

pub(crate) fn normalize_range<R: RangeBounds<usize>>(range: R) -> (usize, usize) {
    let start = match range.start_bound() {
        Bound::Unbounded => usize::MIN,
        Bound::Included(n) => *n,
        Bound::Excluded(n) => *n - 1,
    };

    let end = match range.end_bound() {
        Bound::Unbounded => usize::MAX,
        Bound::Included(n) => *n + 1,
        Bound::Excluded(n) => *n,
    };
    (start, end)
}

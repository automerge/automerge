use crate::types;
use crate::types::{ElemId, ObjType};

use super::meta::ValueType;

/// An index into an array of actors stored elsewhere
#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) struct ActorIdx(u64);

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

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) struct MarkData<'a> {
    pub(crate) name: &'a str,
    pub(crate) value: ScalarValue<'a>,
}

impl<'a> PartialEq<types::MarkData> for MarkData<'a> {
    fn eq(&self, other: &types::MarkData) -> bool {
        self.value == other.value && self.name == other.name
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Action {
    MakeMap,
    MakeList,
    MakeText,
    MakeTable,
    Set,
    Delete,
    Increment,
    Mark,
}

pub(crate) enum OpType<'a> {
    Make(ObjType),
    Delete,
    Increment(i64),
    Put(ScalarValue<'a>),
    MarkBegin(bool, MarkData<'a>),
    MarkEnd(bool),
}

impl<'a> PartialEq<types::OpType> for OpType<'a> {
    fn eq(&self, other: &types::OpType) -> bool {
        match (self, other) {
            //(OpType::Map(a) , types::OpType::Map(b)) => a == &b.as_bytes(),
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
            _ => unreachable!("validate_action_and_value returned UnknownAction"),
        }
    }
}

//impl<'a> PartialEq<ExKey<'a>> for Key<'a> {
//fn eq(&self, other: &ExKey<'a>) -> bool {
//match (self, other) {
//(Key::Map(a), ExKey::Map(b)) => a == &b.as_bytes(),
//(Key::Seq(a), ExKey::Seq(b)) => a == b,
//_ => false,
//}
//}
//}

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum ScalarValue<'a> {
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

impl<'a> ScalarValue<'a> {
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
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ReadScalarError {
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
        .map(|(i, v)| v)
        .map_err(|_| ReadScalarError::InvalidLeb)
}

fn parse_leb128(input: &[u8]) -> Result<i64, ReadScalarError> {
    crate::storage::parse::leb128_i64::<ReadScalarError>(crate::storage::parse::Input::new(input))
        .map(|(i, v)| v)
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

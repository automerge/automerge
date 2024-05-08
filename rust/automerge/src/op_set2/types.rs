use crate::op_set::ExKey;
use crate::types;
use crate::types::{ElemId, ObjType};

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) struct MarkData<'a> {
    pub(crate) name: &'a [u8],
    pub(crate) value: ScalarValue<'a>,
}

impl<'a> PartialEq<types::MarkData> for MarkData<'a> {
    fn eq(&self, other: &types::MarkData) -> bool {
        self.value == other.value && self.name == other.name.as_bytes()
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
        action: u64,
        value: ScalarValue<'a>,
        mark_name: Option<&'a [u8]>,
        expand: bool,
    ) -> OpType<'a> {
        match action {
            0 => Self::Make(ObjType::Map),
            1 => Self::Put(value),
            2 => Self::Make(ObjType::List),
            3 => Self::Delete,
            4 => Self::Make(ObjType::Text),
            5 => match value {
                ScalarValue::Int(i) => Self::Increment(i),
                ScalarValue::Uint(i) => Self::Increment(i as i64),
                _ => unreachable!("validate_action_and_value returned NonNumericInc"),
            },
            6 => Self::Make(ObjType::Table),
            7 => match mark_name {
                Some(name) => Self::MarkBegin(expand, MarkData { name, value }),
                None => Self::MarkEnd(expand),
            },
            _ => unreachable!("validate_action_and_value returned UnknownAction"),
        }
    }
}

impl<'a> PartialEq<ExKey<'a>> for Key<'a> {
    fn eq(&self, other: &ExKey<'a>) -> bool {
        match (self, other) {
            (Key::Map(a), ExKey::Map(b)) => a == &b.as_bytes(),
            (Key::Seq(a), ExKey::Seq(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum ScalarValue<'a> {
    Bytes(&'a [u8]),
    Str(&'a [u8]),
    Int(i64),
    Uint(u64),
    F64(f64),
    Counter(i64),
    Timestamp(i64),
    Boolean(bool),
    Unknown { type_code: u8, bytes: &'a [u8] },
    Null,
}

impl<'a> PartialEq<types::ScalarValue> for ScalarValue<'a> {
    fn eq(&self, other: &types::ScalarValue) -> bool {
        match (self, other) {
            (ScalarValue::Bytes(a), types::ScalarValue::Bytes(b)) => a == &b.as_slice(),
            (ScalarValue::Str(a), types::ScalarValue::Str(b)) => a == &b.as_bytes(),
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

#[derive(Clone, Debug, Copy)]
pub(crate) enum Key<'a> {
    Map(&'a [u8]), // at this point we don't care if its valid UTF8
    Seq(ElemId),
}

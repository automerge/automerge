use crate::ScalarValue;

use std::borrow::Cow;

use smol_str::SmolStr;

#[derive(Debug)]
pub(crate) enum CellValue<'a> {
    Uint(u64),
    Int(i64),
    Bool(bool),
    String(Cow<'a, SmolStr>),
    Value(PrimVal<'a>),
    List(Vec<Vec<CellValue<'a>>>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PrimVal<'a> {
    Null,
    Bool(bool),
    Uint(u64),
    Int(i64),
    Float(f64),
    String(Cow<'a, SmolStr>),
    Bytes(Cow<'a, [u8]>),
    Counter(u64),
    Timestamp(u64),
    Unknown { type_code: u8, data: Vec<u8> },
}

impl<'a> PrimVal<'a> {
    pub(crate) fn into_owned(self) -> PrimVal<'static> {
        match self {
            PrimVal::String(s) => PrimVal::String(Cow::Owned(s.into_owned().into())),
            PrimVal::Bytes(b) => PrimVal::Bytes(Cow::Owned(b.to_vec())),
            PrimVal::Null => PrimVal::Null,
            PrimVal::Bool(b) => PrimVal::Bool(b),
            PrimVal::Uint(u) => PrimVal::Uint(u),
            PrimVal::Int(i) => PrimVal::Int(i),
            PrimVal::Float(f) => PrimVal::Float(f),
            PrimVal::Counter(u) => PrimVal::Counter(u),
            PrimVal::Timestamp(u) => PrimVal::Timestamp(u),
            PrimVal::Unknown { type_code, data } => PrimVal::Unknown{ type_code, data},
        }
    }
}

impl<'a> From<PrimVal<'a>> for ScalarValue {
    fn from(p: PrimVal) -> Self {
        match p {
            PrimVal::Null => Self::Null,
            PrimVal::Bool(b) => Self::Boolean(b),
            PrimVal::Uint(u) => Self::Uint(u),
            PrimVal::Int(i) => Self::Int(i),
            PrimVal::Float(f) => Self::F64(f),
            PrimVal::String(s) => Self::Str(s.into_owned()),
            PrimVal::Bytes(b) => Self::Bytes(b.to_vec()),
            PrimVal::Counter(c) => Self::Counter((c as i64).into()),
            PrimVal::Timestamp(t) => Self::Timestamp(t as i64),
            PrimVal::Unknown { data, .. } => Self::Bytes(data),
        }
    }
}

impl<'a> From<ScalarValue> for PrimVal<'static> {
    fn from(s: ScalarValue) -> Self {
        match s {
            ScalarValue::Null => PrimVal::Null,
            ScalarValue::Boolean(b) => PrimVal::Bool(b),
            ScalarValue::Uint(u) => PrimVal::Uint(u),
            ScalarValue::Int(i) => PrimVal::Int(i),
            ScalarValue::F64(f) => PrimVal::Float(f),
            ScalarValue::Str(s) => PrimVal::String(Cow::Owned(s)),
            // This is bad, if there was an unknown type code in the primval we have lost it on the
            // round trip
            ScalarValue::Bytes(b) => PrimVal::Bytes(Cow::Owned(b)),
            ScalarValue::Counter(c) => PrimVal::Counter(c.current as u64),
            ScalarValue::Timestamp(t) => PrimVal::Timestamp(t as u64),
        }
    }
}

impl<'a> From<&ScalarValue> for PrimVal<'static> {
    fn from(s: &ScalarValue) -> Self {
        match s {
            ScalarValue::Null => PrimVal::Null,
            ScalarValue::Boolean(b) => PrimVal::Bool(*b),
            ScalarValue::Uint(u) => PrimVal::Uint(*u),
            ScalarValue::Int(i) => PrimVal::Int(*i),
            ScalarValue::F64(f) => PrimVal::Float(*f),
            ScalarValue::Str(s) => PrimVal::String(Cow::Owned(s.clone())),
            // This is bad, if there was an unknown type code in the primval we have lost it on the
            // round trip
            ScalarValue::Bytes(b) => PrimVal::Bytes(Cow::Owned(b.clone())),
            ScalarValue::Counter(c) => PrimVal::Counter(c.current as u64),
            ScalarValue::Timestamp(t) => PrimVal::Timestamp((*t) as u64),
        }
    }
}

impl<'a> From<&'a [u8]> for PrimVal<'a> {
    fn from(d: &'a [u8]) -> Self {
        PrimVal::Bytes(Cow::Borrowed(d))
    }
}

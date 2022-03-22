use crate::error;
use crate::exid::ExId;
use crate::types::ObjType;
use serde::{Deserialize, Serialize, Serializer};
use smol_str::SmolStr;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Object(ObjType, ExId),
    Scalar(ScalarValue),
}

impl Value {
    pub fn str(s: &str) -> Value {
        Value::Scalar(ScalarValue::Str(s.into()))
    }

    pub fn int(n: i64) -> Value {
        Value::Scalar(ScalarValue::Int(n))
    }

    pub fn uint(n: u64) -> Value {
        Value::Scalar(ScalarValue::Uint(n))
    }

    pub fn counter(n: i64) -> Value {
        Value::Scalar(ScalarValue::counter(n))
    }

    pub fn timestamp(n: i64) -> Value {
        Value::Scalar(ScalarValue::Timestamp(n))
    }

    pub fn f64(n: f64) -> Value {
        Value::Scalar(ScalarValue::F64(n))
    }

    pub fn bytes(b: Vec<u8>) -> Value {
        Value::Scalar(ScalarValue::Bytes(b))
    }

    pub fn is_object(&self) -> bool {
        matches!(&self, Value::Object(_, _))
    }

    pub fn is_scalar(&self) -> bool {
        matches!(&self, Value::Scalar(_))
    }

    pub fn is_bytes(&self) -> bool {
        matches!(self, Self::Scalar(ScalarValue::Bytes(_)))
    }

    pub fn is_str(&self) -> bool {
        matches!(self, Self::Scalar(ScalarValue::Str(_)))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, Self::Scalar(ScalarValue::Int(_)))
    }

    pub fn is_uint(&self) -> bool {
        matches!(self, Self::Scalar(ScalarValue::Uint(_)))
    }

    pub fn is_f64(&self) -> bool {
        matches!(self, Self::Scalar(ScalarValue::F64(_)))
    }

    pub fn is_counter(&self) -> bool {
        matches!(self, Self::Scalar(ScalarValue::Counter(_)))
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(self, Self::Scalar(ScalarValue::Timestamp(_)))
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, Self::Scalar(ScalarValue::Boolean(_)))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Scalar(ScalarValue::Null))
    }

    pub fn into_scalar(self) -> Result<ScalarValue, Self> {
        match self {
            Self::Scalar(s) => Ok(s),
            _ => Err(self),
        }
    }

    pub fn to_scalar(&self) -> Option<&ScalarValue> {
        match self {
            Self::Scalar(s) => Some(s),
            _ => None,
        }
    }

    pub fn to_objtype(&self) -> Option<ObjType> {
        match self {
            Self::Object(o, _) => Some(*o),
            _ => None,
        }
    }

    pub fn into_bytes(self) -> Result<Vec<u8>, Self> {
        match self {
            Value::Scalar(s) => s.into_bytes().map_err(Value::Scalar),
            _ => Err(self),
        }
    }

    pub fn to_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Scalar(s) => s.to_bytes(),
            _ => None,
        }
    }

    pub fn into_string(self) -> Result<String, Self> {
        match self {
            Value::Scalar(s) => s.into_string().map_err(Value::Scalar),
            _ => Err(self),
        }
    }

    pub fn to_str(&self) -> Option<&str> {
        match self {
            Value::Scalar(val) => val.to_str(),
            _ => None,
        }
    }

    /// If this value can be coerced to an i64, return the i64 value
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            Value::Scalar(s) => s.to_i64(),
            _ => None,
        }
    }

    pub fn to_u64(&self) -> Option<u64> {
        match self {
            Value::Scalar(s) => s.to_u64(),
            _ => None,
        }
    }

    pub fn to_f64(&self) -> Option<f64> {
        match self {
            Value::Scalar(s) => s.to_f64(),
            _ => None,
        }
    }

    pub fn to_bool(&self) -> Option<bool> {
        match self {
            Value::Scalar(s) => s.to_bool(),
            _ => None,
        }
    }

    pub fn id(&self) -> Option<ExId> {
        match self {
            Value::Object(_, id) => Some(id.clone()),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Object(o, id) => write!(f, "Object: {} {}", o, id),
            Value::Scalar(s) => write!(f, "Scalar: {}", s),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Scalar(ScalarValue::Str(s.into()))
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Scalar(ScalarValue::Str(s.into()))
    }
}

impl From<char> for Value {
    fn from(c: char) -> Self {
        Value::Scalar(ScalarValue::Str(SmolStr::new(c.to_string())))
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Scalar(ScalarValue::Bytes(v))
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Scalar(ScalarValue::F64(n))
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Scalar(ScalarValue::Int(n))
    }
}

impl From<i32> for Value {
    fn from(n: i32) -> Self {
        Value::Scalar(ScalarValue::Int(n.into()))
    }
}

impl From<u32> for Value {
    fn from(n: u32) -> Self {
        Value::Scalar(ScalarValue::Uint(n.into()))
    }
}

impl From<u64> for Value {
    fn from(n: u64) -> Self {
        Value::Scalar(ScalarValue::Uint(n))
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Scalar(ScalarValue::Boolean(v))
    }
}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Value::Scalar(ScalarValue::Null)
    }
}

impl From<ScalarValue> for Value {
    fn from(v: ScalarValue) -> Self {
        Value::Scalar(v)
    }
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone, Copy)]
pub(crate) enum DataType {
    #[serde(rename = "counter")]
    Counter,
    #[serde(rename = "timestamp")]
    Timestamp,
    #[serde(rename = "bytes")]
    Bytes,
    #[serde(rename = "uint")]
    Uint,
    #[serde(rename = "int")]
    Int,
    #[serde(rename = "float64")]
    F64,
    #[serde(rename = "undefined")]
    Undefined,
}

#[derive(Debug, Clone)]
pub struct Counter {
    pub(crate) start: i64,
    pub(crate) current: i64,
    pub(crate) increments: usize,
}

impl Serialize for Counter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.start)
    }
}

impl fmt::Display for Counter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.current)
    }
}

impl From<i64> for Counter {
    fn from(n: i64) -> Self {
        Counter {
            start: n,
            current: n,
            increments: 0,
        }
    }
}

impl From<&i64> for Counter {
    fn from(n: &i64) -> Self {
        Counter {
            start: *n,
            current: *n,
            increments: 0,
        }
    }
}

impl From<&Counter> for i64 {
    fn from(val: &Counter) -> Self {
        val.current
    }
}

impl From<Counter> for i64 {
    fn from(val: Counter) -> Self {
        val.current
    }
}

impl From<&Counter> for u64 {
    fn from(val: &Counter) -> Self {
        val.current as u64
    }
}

impl From<&Counter> for f64 {
    fn from(val: &Counter) -> Self {
        val.current as f64
    }
}

#[derive(Serialize, PartialEq, Debug, Clone)]
#[serde(untagged)]
pub enum ScalarValue {
    Bytes(Vec<u8>),
    Str(SmolStr),
    Int(i64),
    Uint(u64),
    F64(f64),
    Counter(Counter),
    Timestamp(i64),
    Boolean(bool),
    Null,
}

impl PartialEq for Counter {
    fn eq(&self, other: &Self) -> bool {
        self.current == other.current
    }
}

impl ScalarValue {
    pub(crate) fn as_datatype(
        &self,
        datatype: DataType,
    ) -> Result<ScalarValue, error::InvalidScalarValue> {
        match (datatype, self) {
            (DataType::Counter, ScalarValue::Int(i)) => Ok(ScalarValue::Counter(i.into())),
            (DataType::Counter, ScalarValue::Uint(u)) => match i64::try_from(*u) {
                Ok(i) => Ok(ScalarValue::Counter(i.into())),
                Err(_) => Err(error::InvalidScalarValue {
                    raw_value: self.clone(),
                    expected: "an integer".to_string(),
                    unexpected: "an integer larger than i64::max_value".to_string(),
                    datatype,
                }),
            },
            (DataType::Bytes, ScalarValue::Bytes(bytes)) => Ok(ScalarValue::Bytes(bytes.clone())),
            (DataType::Bytes, v) => Err(error::InvalidScalarValue {
                raw_value: self.clone(),
                expected: "a vector of bytes".to_string(),
                unexpected: v.to_string(),
                datatype,
            }),
            (DataType::Counter, v) => Err(error::InvalidScalarValue {
                raw_value: self.clone(),
                expected: "an integer".to_string(),
                unexpected: v.to_string(),
                datatype,
            }),
            (DataType::Timestamp, ScalarValue::Int(i)) => Ok(ScalarValue::Timestamp(*i)),
            (DataType::Timestamp, ScalarValue::Uint(u)) => match i64::try_from(*u) {
                Ok(i) => Ok(ScalarValue::Timestamp(i)),
                Err(_) => Err(error::InvalidScalarValue {
                    raw_value: self.clone(),
                    expected: "an integer".to_string(),
                    unexpected: "an integer larger than i64::max_value".to_string(),
                    datatype,
                }),
            },
            (DataType::Timestamp, v) => Err(error::InvalidScalarValue {
                raw_value: self.clone(),
                expected: "an integer".to_string(),
                unexpected: v.to_string(),
                datatype,
            }),
            (DataType::Int, v) => Ok(ScalarValue::Int(v.to_i64().ok_or(
                error::InvalidScalarValue {
                    raw_value: self.clone(),
                    expected: "an int".to_string(),
                    unexpected: v.to_string(),
                    datatype,
                },
            )?)),
            (DataType::Uint, v) => Ok(ScalarValue::Uint(v.to_u64().ok_or(
                error::InvalidScalarValue {
                    raw_value: self.clone(),
                    expected: "a uint".to_string(),
                    unexpected: v.to_string(),
                    datatype,
                },
            )?)),
            (DataType::F64, v) => Ok(ScalarValue::F64(v.to_f64().ok_or(
                error::InvalidScalarValue {
                    raw_value: self.clone(),
                    expected: "an f64".to_string(),
                    unexpected: v.to_string(),
                    datatype,
                },
            )?)),
            (DataType::Undefined, _) => Ok(self.clone()),
        }
    }

    /// Returns an Option containing a `DataType` if
    /// `self` represents a numerical scalar value
    /// This is necessary b/c numerical values are not self-describing
    /// (unlike strings / bytes / etc. )
    pub(crate) fn as_numerical_datatype(&self) -> Option<DataType> {
        match self {
            ScalarValue::Counter(..) => Some(DataType::Counter),
            ScalarValue::Timestamp(..) => Some(DataType::Timestamp),
            ScalarValue::Int(..) => Some(DataType::Int),
            ScalarValue::Uint(..) => Some(DataType::Uint),
            ScalarValue::F64(..) => Some(DataType::F64),
            _ => None,
        }
    }

    pub fn is_bytes(&self) -> bool {
        matches!(self, Self::Bytes(_))
    }

    pub fn is_str(&self) -> bool {
        matches!(self, Self::Str(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, Self::Int(_))
    }

    pub fn is_uint(&self) -> bool {
        matches!(self, Self::Uint(_))
    }

    pub fn is_f64(&self) -> bool {
        matches!(self, Self::F64(_))
    }

    pub fn is_counter(&self) -> bool {
        matches!(self, Self::Counter(_))
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(self, Self::Timestamp(_))
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, Self::Boolean(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn into_bytes(self) -> Result<Vec<u8>, Self> {
        match self {
            ScalarValue::Bytes(b) => Ok(b),
            _ => Err(self),
        }
    }

    pub fn to_bytes(&self) -> Option<&[u8]> {
        match self {
            ScalarValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn into_string(self) -> Result<String, Self> {
        match self {
            ScalarValue::Str(s) => Ok(s.to_string()),
            _ => Err(self),
        }
    }

    pub fn to_str(&self) -> Option<&str> {
        match self {
            ScalarValue::Str(s) => Some(s),
            _ => None,
        }
    }

    /// If this value can be coerced to an i64, return the i64 value
    pub fn to_i64(&self) -> Option<i64> {
        match self {
            ScalarValue::Int(n) => Some(*n),
            ScalarValue::Uint(n) => Some(*n as i64),
            ScalarValue::F64(n) => Some(*n as i64),
            ScalarValue::Counter(n) => Some(n.into()),
            ScalarValue::Timestamp(n) => Some(*n),
            _ => None,
        }
    }

    pub fn to_u64(&self) -> Option<u64> {
        match self {
            ScalarValue::Int(n) => Some(*n as u64),
            ScalarValue::Uint(n) => Some(*n),
            ScalarValue::F64(n) => Some(*n as u64),
            ScalarValue::Counter(n) => Some(n.into()),
            ScalarValue::Timestamp(n) => Some(*n as u64),
            _ => None,
        }
    }

    pub fn to_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Int(n) => Some(*n as f64),
            ScalarValue::Uint(n) => Some(*n as f64),
            ScalarValue::F64(n) => Some(*n),
            ScalarValue::Counter(n) => Some(n.into()),
            ScalarValue::Timestamp(n) => Some(*n as f64),
            _ => None,
        }
    }

    pub fn to_bool(&self) -> Option<bool> {
        match self {
            ScalarValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn counter(n: i64) -> ScalarValue {
        ScalarValue::Counter(n.into())
    }
}

impl From<&str> for ScalarValue {
    fn from(s: &str) -> Self {
        ScalarValue::Str(s.into())
    }
}

impl From<String> for ScalarValue {
    fn from(s: String) -> Self {
        ScalarValue::Str(s.into())
    }
}

impl From<Vec<u8>> for ScalarValue {
    fn from(b: Vec<u8>) -> Self {
        ScalarValue::Bytes(b)
    }
}

impl From<i64> for ScalarValue {
    fn from(n: i64) -> Self {
        ScalarValue::Int(n)
    }
}

impl From<f64> for ScalarValue {
    fn from(n: f64) -> Self {
        ScalarValue::F64(n)
    }
}

impl From<u64> for ScalarValue {
    fn from(n: u64) -> Self {
        ScalarValue::Uint(n)
    }
}

impl From<u32> for ScalarValue {
    fn from(n: u32) -> Self {
        ScalarValue::Uint(n.into())
    }
}

impl From<i32> for ScalarValue {
    fn from(n: i32) -> Self {
        ScalarValue::Int(n as i64)
    }
}

impl From<bool> for ScalarValue {
    fn from(b: bool) -> Self {
        ScalarValue::Boolean(b)
    }
}

impl From<()> for ScalarValue {
    fn from(_: ()) -> Self {
        ScalarValue::Null
    }
}

impl From<char> for ScalarValue {
    fn from(c: char) -> Self {
        ScalarValue::Str(SmolStr::new(c.to_string()))
    }
}

impl fmt::Display for ScalarValue {
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
        }
    }
}

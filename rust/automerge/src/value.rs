use crate::error;
use crate::types::ObjType;
use serde::{Deserialize, Serialize, Serializer};
use smol_str::SmolStr;
use std::borrow::Cow;
use std::fmt;

/// The type of values in an automerge document
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    /// A composite object of type [`ObjType`]
    Object(ObjType),
    /// A non composite value
    // TODO: if we don't have to store this in patches any more then it might be able to be just a
    // &'a ScalarValue rather than a Cow
    Scalar(Cow<'a, ScalarValue>),
}

impl<'a> Value<'a> {
    pub fn map() -> Value<'a> {
        Value::Object(ObjType::Map)
    }

    pub fn list() -> Value<'a> {
        Value::Object(ObjType::List)
    }

    pub fn text() -> Value<'a> {
        Value::Object(ObjType::Text)
    }

    pub fn table() -> Value<'a> {
        Value::Object(ObjType::Table)
    }

    pub fn str(s: &str) -> Value<'a> {
        Value::Scalar(Cow::Owned(ScalarValue::Str(s.into())))
    }

    pub fn int(n: i64) -> Value<'a> {
        Value::Scalar(Cow::Owned(ScalarValue::Int(n)))
    }

    pub fn uint(n: u64) -> Value<'a> {
        Value::Scalar(Cow::Owned(ScalarValue::Uint(n)))
    }

    pub fn counter(n: i64) -> Value<'a> {
        Value::Scalar(Cow::Owned(ScalarValue::counter(n)))
    }

    pub fn timestamp(n: i64) -> Value<'a> {
        Value::Scalar(Cow::Owned(ScalarValue::Timestamp(n)))
    }

    pub fn f64(n: f64) -> Value<'a> {
        Value::Scalar(Cow::Owned(ScalarValue::F64(n)))
    }

    pub fn bytes(b: Vec<u8>) -> Value<'a> {
        Value::Scalar(Cow::Owned(ScalarValue::Bytes(b)))
    }

    pub fn is_object(&self) -> bool {
        matches!(&self, Value::Object(_))
    }

    pub fn is_scalar(&self) -> bool {
        matches!(&self, Value::Scalar(_))
    }

    pub fn is_bytes(&self) -> bool {
        if let Self::Scalar(s) = self {
            s.is_bytes()
        } else {
            false
        }
    }

    pub fn is_str(&self) -> bool {
        if let Self::Scalar(s) = self {
            s.is_str()
        } else {
            false
        }
    }

    pub fn is_int(&self) -> bool {
        if let Self::Scalar(s) = self {
            s.is_int()
        } else {
            false
        }
    }

    pub fn is_uint(&self) -> bool {
        if let Self::Scalar(s) = self {
            s.is_uint()
        } else {
            false
        }
    }

    pub fn is_f64(&self) -> bool {
        if let Self::Scalar(s) = self {
            s.is_f64()
        } else {
            false
        }
    }

    pub fn is_counter(&self) -> bool {
        if let Self::Scalar(s) = self {
            s.is_counter()
        } else {
            false
        }
    }

    pub fn is_timestamp(&self) -> bool {
        if let Self::Scalar(s) = self {
            s.is_timestamp()
        } else {
            false
        }
    }

    pub fn is_boolean(&self) -> bool {
        if let Self::Scalar(s) = self {
            s.is_boolean()
        } else {
            false
        }
    }

    pub fn is_null(&self) -> bool {
        if let Self::Scalar(s) = self {
            s.is_null()
        } else {
            false
        }
    }

    pub fn into_scalar(self) -> Result<ScalarValue, Self> {
        match self {
            Self::Scalar(s) => Ok(s.into_owned()),
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
            Self::Object(o) => Some(*o),
            _ => None,
        }
    }

    pub fn into_owned(self) -> Value<'static> {
        match self {
            Value::Object(o) => Value::Object(o),
            Value::Scalar(Cow::Owned(s)) => Value::Scalar(Cow::Owned(s)),
            Value::Scalar(Cow::Borrowed(s)) => Value::Scalar(Cow::Owned((*s).clone())),
        }
    }

    pub fn to_owned(&self) -> Value<'static> {
        match self {
            Value::Object(o) => Value::Object(*o),
            Value::Scalar(Cow::Owned(s)) => Value::Scalar(Cow::Owned(s.clone())),
            Value::Scalar(Cow::Borrowed(s)) => Value::Scalar(Cow::Owned((*s).clone())),
        }
    }

    pub fn into_bytes(self) -> Result<Vec<u8>, Self> {
        match self {
            Value::Scalar(s) => s
                .into_owned()
                .into_bytes()
                .map_err(|v| Value::Scalar(Cow::Owned(v))),
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
            Value::Scalar(s) => s
                .into_owned()
                .into_string()
                .map_err(|v| Value::Scalar(Cow::Owned(v))),
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
}

impl<'a> fmt::Display for Value<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Object(o) => write!(f, "{}", o),
            Value::Scalar(s) => write!(f, "{}", s),
        }
    }
}

impl From<&str> for Value<'static> {
    fn from(s: &str) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Str(s.into())))
    }
}

impl<'a> From<&String> for Value<'a> {
    fn from(s: &String) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Str(s.into())))
    }
}

impl<'a> From<String> for Value<'a> {
    fn from(s: String) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Str(s.into())))
    }
}

impl<'a> From<SmolStr> for Value<'a> {
    fn from(s: SmolStr) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Str(s)))
    }
}

impl<'a> From<char> for Value<'a> {
    fn from(c: char) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Str(SmolStr::new(c.to_string()))))
    }
}

impl<'a> From<Vec<u8>> for Value<'a> {
    fn from(v: Vec<u8>) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Bytes(v)))
    }
}

impl<'a> From<f64> for Value<'a> {
    fn from(n: f64) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::F64(n)))
    }
}

impl<'a> From<i64> for Value<'a> {
    fn from(n: i64) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Int(n)))
    }
}

impl<'a> From<i32> for Value<'a> {
    fn from(n: i32) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Int(n.into())))
    }
}

impl<'a> From<u32> for Value<'a> {
    fn from(n: u32) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Uint(n.into())))
    }
}

impl<'a> From<u64> for Value<'a> {
    fn from(n: u64) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Uint(n)))
    }
}

impl<'a> From<bool> for Value<'a> {
    fn from(v: bool) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Boolean(v)))
    }
}

impl<'a> From<()> for Value<'a> {
    fn from(_: ()) -> Self {
        Value::Scalar(Cow::Owned(ScalarValue::Null))
    }
}

impl<'a> From<ObjType> for Value<'a> {
    fn from(o: ObjType) -> Self {
        Value::Object(o)
    }
}

impl<'a> From<ScalarValue> for Value<'a> {
    fn from(v: ScalarValue) -> Self {
        Value::Scalar(Cow::Owned(v))
    }
}

impl<'a> From<&'a ScalarValue> for Value<'a> {
    fn from(v: &'a ScalarValue) -> Self {
        Value::Scalar(Cow::Borrowed(v))
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
}

impl Counter {
    pub(crate) fn increment(&mut self, inc: i64) {
        self.current += inc;
    }
}

impl Serialize for Counter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.current)
    }
}

impl fmt::Display for Counter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.current)
    }
}

impl From<i64> for Counter {
    fn from(n: i64) -> Self {
        Counter {
            start: n,
            current: n,
        }
    }
}

impl From<&i64> for Counter {
    fn from(n: &i64) -> Self {
        Counter {
            start: *n,
            current: *n,
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

/// A value which is not a composite value
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
    /// A value from a future version of automerge
    Unknown {
        type_code: u8,
        bytes: Vec<u8>,
    },
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

impl From<&String> for ScalarValue {
    fn from(s: &String) -> Self {
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
            ScalarValue::Unknown { type_code, .. } => write!(f, "unknown type {}", type_code),
        }
    }
}

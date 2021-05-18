use std::fmt;

use crate::ScalarValue;

impl From<&str> for ScalarValue {
    fn from(s: &str) -> Self {
        ScalarValue::Str(s.into())
    }
}

impl From<i64> for ScalarValue {
    fn from(n: i64) -> Self {
        ScalarValue::Int(n)
    }
}

impl From<u64> for ScalarValue {
    fn from(n: u64) -> Self {
        ScalarValue::Uint(n)
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

impl From<char> for ScalarValue {
    fn from(c: char) -> Self {
        ScalarValue::Str(c.to_string())
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Bytes(b) => write!(f, "\"{:?}\"", b),
            ScalarValue::Str(s) => write!(f, "\"{}\"", s),
            ScalarValue::Int(i) => write!(f, "{}", i),
            ScalarValue::Uint(i) => write!(f, "{}", i),
            ScalarValue::F32(n) => write!(f, "{:.32}", n),
            ScalarValue::F64(n) => write!(f, "{:.324}", n),
            ScalarValue::Counter(c) => write!(f, "Counter: {}", c),
            ScalarValue::Timestamp(i) => write!(f, "Timestamp: {}", i),
            ScalarValue::Boolean(b) => write!(f, "{}", b),
            ScalarValue::Null => write!(f, "null"),
            ScalarValue::Cursor(elemid) => write!(f, "Cursor: {}", elemid),
        }
    }
}

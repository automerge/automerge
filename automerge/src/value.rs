use crate::legacy as amp;
use crate::types::{Op, OpId};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Object(amp::ObjType),
    Scalar(amp::ScalarValue),
}

impl Value {
    pub fn to_string(&self) -> Option<String> {
        match self {
            Value::Scalar(val) => Some(val.to_string()),
            _ => None,
        }
    }

    pub fn map() -> Value {
        Value::Object(amp::ObjType::Map)
    }

    pub fn list() -> Value {
        Value::Object(amp::ObjType::List)
    }

    pub fn text() -> Value {
        Value::Object(amp::ObjType::Text)
    }

    pub fn table() -> Value {
        Value::Object(amp::ObjType::Table)
    }

    pub fn str(s: &str) -> Value {
        Value::Scalar(amp::ScalarValue::Str(s.into()))
    }

    pub fn int(n: i64) -> Value {
        Value::Scalar(amp::ScalarValue::Int(n))
    }

    pub fn uint(n: u64) -> Value {
        Value::Scalar(amp::ScalarValue::Uint(n))
    }

    pub fn counter(n: i64) -> Value {
        Value::Scalar(amp::ScalarValue::Counter(n))
    }

    pub fn timestamp(n: i64) -> Value {
        Value::Scalar(amp::ScalarValue::Timestamp(n))
    }

    pub fn f64(n: f64) -> Value {
        Value::Scalar(amp::ScalarValue::F64(n))
    }

    pub fn bytes(b: Vec<u8>) -> Value {
        Value::Scalar(amp::ScalarValue::Bytes(b))
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Scalar(s.into())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Scalar(amp::ScalarValue::Str(s.into()))
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Scalar(amp::ScalarValue::Int(n))
    }
}

impl From<i32> for Value {
    fn from(n: i32) -> Self {
        Value::Scalar(amp::ScalarValue::Int(n.into()))
    }
}

impl From<u64> for Value {
    fn from(n: u64) -> Self {
        Value::Scalar(amp::ScalarValue::Uint(n))
    }
}

impl From<amp::ObjType> for Value {
    fn from(o: amp::ObjType) -> Self {
        Value::Object(o)
    }
}

impl From<amp::ScalarValue> for Value {
    fn from(v: amp::ScalarValue) -> Self {
        Value::Scalar(v)
    }
}

impl From<&Op> for (Value, OpId) {
    fn from(op: &Op) -> Self {
        match &op.action {
            amp::OpType::Make(obj_type) => (Value::Object(*obj_type), op.id),
            amp::OpType::Set(scalar) => (Value::Scalar(scalar.clone()), op.id),
            _ => panic!("cant convert op into a value - {:?}", op),
        }
    }
}

impl From<Op> for (Value, OpId) {
    fn from(op: Op) -> Self {
        match &op.action {
            amp::OpType::Make(obj_type) => (Value::Object(*obj_type), op.id),
            amp::OpType::Set(scalar) => (Value::Scalar(scalar.clone()), op.id),
            _ => panic!("cant convert op into a value - {:?}", op),
        }
    }
}

impl From<Value> for amp::OpType {
    fn from(v: Value) -> Self {
        match v {
            Value::Object(o) => amp::OpType::Make(o),
            Value::Scalar(s) => amp::OpType::Set(s),
        }
    }
}

use crate::legacy as amp;
use crate::{ObjType, Op, OpId, ScalarValue};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Object(ObjType),
    Scalar(ScalarValue),
}

impl Value {
    pub fn to_string(&self) -> Option<String> {
        match self {
            Value::Scalar(val) => Some(val.to_string()),
            _ => None,
        }
    }

    pub fn map() -> Value {
        Value::Object(ObjType::Map)
    }

    pub fn list() -> Value {
        Value::Object(ObjType::List)
    }

    pub fn text() -> Value {
        Value::Object(ObjType::Text)
    }

    pub fn table() -> Value {
        Value::Object(ObjType::Table)
    }

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
        Value::Scalar(ScalarValue::Counter(n))
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
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Scalar(s.into())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Scalar(ScalarValue::Str(s.into()))
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

impl From<u64> for Value {
    fn from(n: u64) -> Self {
        Value::Scalar(ScalarValue::Uint(n))
    }
}

impl From<ObjType> for Value {
    fn from(o: ObjType) -> Self {
        Value::Object(o)
    }
}

impl From<ScalarValue> for Value {
    fn from(v: ScalarValue) -> Self {
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

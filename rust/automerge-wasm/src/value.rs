use automerge::{ObjType, ScalarValue, ScalarValueRef, Value, ValueRef};
use wasm_bindgen::prelude::*;

#[derive(Debug, PartialOrd, Ord, Copy, Clone, Hash, Eq, PartialEq)]
pub(crate) enum Datatype {
    Map,
    Table,
    List,
    Text,
    Bytes,
    Str,
    Int,
    Uint,
    F64,
    Counter,
    Timestamp,
    Boolean,
    Null,
    Unknown(u8),
}

impl Datatype {
    pub(crate) fn is_scalar(&self) -> bool {
        !matches!(self, Self::Map | Self::Table | Self::List | Self::Text)
    }
}

impl From<&ObjType> for Datatype {
    fn from(o: &ObjType) -> Self {
        (*o).into()
    }
}

impl From<ObjType> for Datatype {
    fn from(o: ObjType) -> Self {
        match o {
            ObjType::Map => Self::Map,
            ObjType::List => Self::List,
            ObjType::Table => Self::Table,
            ObjType::Text => Self::Text,
        }
    }
}

impl std::fmt::Display for Datatype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", String::from(*self))
    }
}

impl From<&ScalarValue> for Datatype {
    fn from(s: &ScalarValue) -> Self {
        match s {
            ScalarValue::Bytes(_) => Self::Bytes,
            ScalarValue::Str(_) => Self::Str,
            ScalarValue::Int(_) => Self::Int,
            ScalarValue::Uint(_) => Self::Uint,
            ScalarValue::F64(_) => Self::F64,
            ScalarValue::Counter(_) => Self::Counter,
            ScalarValue::Timestamp(_) => Self::Timestamp,
            ScalarValue::Boolean(_) => Self::Boolean,
            ScalarValue::Null => Self::Null,
            ScalarValue::Unknown { type_code, .. } => Self::Unknown(*type_code),
        }
    }
}

impl From<&ScalarValueRef<'_>> for Datatype {
    fn from(s: &ScalarValueRef<'_>) -> Self {
        match s {
            ScalarValueRef::Bytes(_) => Self::Bytes,
            ScalarValueRef::Str(_) => Self::Str,
            ScalarValueRef::Int(_) => Self::Int,
            ScalarValueRef::Uint(_) => Self::Uint,
            ScalarValueRef::F64(_) => Self::F64,
            ScalarValueRef::Counter(_) => Self::Counter,
            ScalarValueRef::Timestamp(_) => Self::Timestamp,
            ScalarValueRef::Boolean(_) => Self::Boolean,
            ScalarValueRef::Null => Self::Null,
            ScalarValueRef::Unknown { type_code, .. } => Self::Unknown(*type_code),
        }
    }
}

impl From<&Value<'_>> for Datatype {
    fn from(v: &Value<'_>) -> Self {
        match v {
            Value::Object(o) => o.into(),
            Value::Scalar(s) => s.as_ref().into(),
        }
    }
}

impl From<&ValueRef<'_>> for Datatype {
    fn from(v: &ValueRef<'_>) -> Self {
        match v {
            ValueRef::Object(o) => o.into(),
            ValueRef::Scalar(s) => s.into(),
        }
    }
}

impl From<Datatype> for String {
    fn from(d: Datatype) -> Self {
        match d {
            Datatype::Map => "map".into(),
            Datatype::Table => "table".into(),
            Datatype::List => "list".into(),
            Datatype::Text => "text".into(),
            Datatype::Bytes => "bytes".into(),
            Datatype::Str => "str".into(),
            Datatype::Int => "int".into(),
            Datatype::Uint => "uint".into(),
            Datatype::F64 => "f64".into(),
            Datatype::Counter => "counter".into(),
            Datatype::Timestamp => "timestamp".into(),
            Datatype::Boolean => "boolean".into(),
            Datatype::Null => "null".into(),
            Datatype::Unknown(type_code) => format!("unknown{}", type_code),
        }
    }
}

impl TryFrom<JsValue> for Datatype {
    type Error = InvalidDatatype;

    fn try_from(datatype: JsValue) -> Result<Self, Self::Error> {
        let datatype = datatype.as_string().ok_or(InvalidDatatype::NotString)?;
        match datatype.as_str() {
            "map" => Ok(Datatype::Map),
            "table" => Ok(Datatype::Table),
            "list" => Ok(Datatype::List),
            "text" => Ok(Datatype::Text),
            "bytes" => Ok(Datatype::Bytes),
            "str" => Ok(Datatype::Str),
            "int" => Ok(Datatype::Int),
            "uint" => Ok(Datatype::Uint),
            "f64" => Ok(Datatype::F64),
            "counter" => Ok(Datatype::Counter),
            "timestamp" => Ok(Datatype::Timestamp),
            "boolean" => Ok(Datatype::Boolean),
            "null" => Ok(Datatype::Null),
            d => {
                if d.starts_with("unknown") {
                    // TODO: handle "unknown{}",
                    Err(InvalidDatatype::UnknownNotImplemented)
                } else {
                    Err(InvalidDatatype::Unknown(d.to_string()))
                }
            }
        }
    }
}

impl From<Datatype> for JsValue {
    fn from(d: Datatype) -> Self {
        String::from(d).into()
    }
}

impl From<Datatype> for Option<Value<'_>> {
    fn from(d: Datatype) -> Self {
        match d {
            Datatype::Map => Some(Value::Object(ObjType::Map)),
            Datatype::List => Some(Value::Object(ObjType::List)),
            Datatype::Text => Some(Value::Object(ObjType::Text)),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidDatatype {
    #[error("unknown datatype")]
    Unknown(String),
    #[error("datatype is not a string")]
    NotString,
    #[error("cannot handle unknown datatype")]
    UnknownNotImplemented,
}

impl From<InvalidDatatype> for JsValue {
    fn from(e: InvalidDatatype) -> Self {
        JsValue::from(e.to_string())
    }
}

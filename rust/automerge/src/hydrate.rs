use crate::op_set2;
use crate::op_set2::{Op, OpType};
use crate::types::{Clock, ObjId, ScalarValue};
use crate::{error::HydrateError, value, ObjType, Patch, PatchAction, Prop};
use std::borrow::Cow;
use std::collections::HashMap;

mod list;
mod map;
mod text;

#[cfg(test)]
mod tests;

pub use list::{List, ListValue};
pub use map::{Map, MapValue};
pub use text::Text;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Scalar(ScalarValue),
    Map(Map),
    List(List),
    Text(Text),
}

/*
#[derive(PartialEq, Debug, Clone)]
pub enum ScalarValue {
    Bytes(Vec<u8>),
    Str(String),
    Int(i64),
    Uint(u64),
    F64(f64),
    Counter(i64),
    Timestamp(i64),
    Boolean(bool),
    Unknown { type_code: u8, bytes: Vec<u8> },
    Null,
}
*/

/*
impl<'a> From<op_set2::ScalarValue<'a>> for ScalarValue {
  fn from(s: op_set2::ScalarValue<'a>) -> Self {
    match s {
      op_set2::ScalarValue::Bytes(b) => ScalarValue::Bytes(b.to_vec()),
      op_set2::ScalarValue::Str(s) => ScalarValue::Str(s.to_string()),
      op_set2::ScalarValue::Int(i) => ScalarValue::Int(i),
      op_set2::ScalarValue::Uint(u) => ScalarValue::Uint(u),
      op_set2::ScalarValue::F64(f) => ScalarValue::F64(f),
      op_set2::ScalarValue::Counter(c) => ScalarValue::Counter(c),
      op_set2::ScalarValue::Timestamp(t) => ScalarValue::Timestamp(t),
      op_set2::ScalarValue::Boolean(b) => ScalarValue::Boolean(b),
      op_set2::ScalarValue::Unknown { type_code, bytes } =. ScalarValue::Unknown { type_code, bytes: bytes.to_vec() },
      op_set2::ScalarValue::Null =. ScalarValue::Null,
    }
  }
}
*/

impl Value {
    pub fn is_scalar(&self) -> bool {
        matches!(self, Value::Scalar(_))
    }

    pub fn is_object(&self) -> bool {
        !self.is_scalar()
    }

    pub fn apply_patches<P: IntoIterator<Item = Patch>>(
        &mut self,
        patches: P,
    ) -> Result<(), HydrateError> {
        for p in patches {
            self.apply(p.path.iter().map(|(_, prop)| prop), p.action)?;
        }
        Ok(())
    }

    pub(crate) fn apply<'a, P: Iterator<Item = &'a Prop>>(
        &mut self,
        mut path: P,
        patch: PatchAction,
    ) -> Result<(), HydrateError> {
        match (path.next(), self) {
            (Some(Prop::Seq(n)), Value::List(list)) => list
                .get_mut(*n)
                .ok_or_else(|| HydrateError::ApplyInvalidProp(patch.clone()))?
                .apply(path, patch),
            (Some(Prop::Map(s)), Value::Map(map)) => map
                .get_mut(s)
                .ok_or_else(|| HydrateError::ApplyInvalidProp(patch.clone()))?
                .apply(path, patch),
            (None, Value::Map(map)) => map.apply(patch),
            (None, Value::List(list)) => list.apply(patch),
            (None, Value::Text(text)) => text.apply(patch),
            _ => Err(HydrateError::Fail),
        }
    }

    pub fn as_map(&mut self) -> Option<&mut Map> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_list(&mut self) -> Option<&mut List> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }
}

impl From<value::Value<'_>> for Value {
    fn from(value: value::Value<'_>) -> Self {
        match value {
            value::Value::Object(ObjType::Map) => Value::Map(Map::default()),
            value::Value::Object(ObjType::List) => Value::List(List::default()),
            value::Value::Object(ObjType::Text) => Value::Text(Text::default()),
            value::Value::Object(ObjType::Table) => Value::Map(Map::default()),
            value::Value::Scalar(s) => Value::Scalar(s.into_owned()),
        }
    }
}

impl From<op_set2::Value<'_>> for Value {
    fn from(value: op_set2::Value<'_>) -> Self {
        match value {
            op_set2::Value::Object(ObjType::Map) => Value::Map(Map::default()),
            op_set2::Value::Object(ObjType::List) => Value::List(List::default()),
            op_set2::Value::Object(ObjType::Text) => Value::Text(Text::default()),
            op_set2::Value::Object(ObjType::Table) => Value::Map(Map::default()),
            op_set2::Value::Scalar(s) => Value::Scalar(s.into_owned()),
        }
    }
}

impl From<Value> for value::Value<'_> {
    fn from(value: Value) -> Self {
        match value {
            Value::Map(_) => value::Value::Object(ObjType::Map),
            Value::List(_) => value::Value::Object(ObjType::List),
            Value::Text(_) => value::Value::Object(ObjType::Text),
            Value::Scalar(s) => value::Value::Scalar(Cow::Owned(s)),
        }
    }
}

impl From<Map> for Value {
    fn from(value: Map) -> Self {
        Value::Map(value)
    }
}

impl From<List> for Value {
    fn from(value: List) -> Self {
        Value::List(value)
    }
}

impl From<Text> for Value {
    fn from(value: Text) -> Self {
        Value::Text(value)
    }
}

impl From<&Value> for value::Value<'_> {
    fn from(value: &Value) -> Self {
        match value {
            Value::Map(_) => value::Value::Object(ObjType::Map),
            Value::List(_) => value::Value::Object(ObjType::List),
            Value::Text(_) => value::Value::Object(ObjType::Text),
            Value::Scalar(s) => value::Value::Scalar(Cow::Owned(s.clone())),
        }
    }
}

impl<T: Into<ScalarValue>> From<T> for Value {
    fn from(value: T) -> Self {
        Value::Scalar(value.into())
    }
}

impl From<HashMap<&str, Value>> for Value {
    fn from(value: HashMap<&str, Value>) -> Self {
        Value::Map(value.into())
    }
}

use crate::Automerge;

impl Automerge {
    pub(crate) fn hydrate_map(&self, obj: &ObjId, clock: Option<&Clock>) -> Value {
        let mut map = Map::new();
        for top in self.ops().top_ops(obj, clock.cloned()) {
            let key = self.ops().to_string(top.elemid_or_key());
            let value = self.hydrate_op(top, clock);
            //let id = top.op.exid();
            let id = self.ops().id_to_exid(top.id);
            let conflict = top.conflict;
            map.insert(key, MapValue::new(value, id, conflict));
        }
        Value::Map(map)
    }

    pub(crate) fn hydrate_list(&self, obj: &ObjId, clock: Option<&Clock>) -> Value {
        let mut list = List::new();
        for top in self.ops().top_ops(obj, clock.cloned()) {
            let value = self.hydrate_op(top, clock);
            //let id = top.exid();
            let id = self.ops().id_to_exid(top.id);
            let conflict = top.conflict;
            list.push(value, id, conflict);
        }
        Value::List(list)
    }

    pub(crate) fn hydrate_text(&self, obj: &ObjId, clock: Option<&Clock>) -> Value {
        let text = self.ops().text(obj, clock.cloned());
        Value::Text(Text::new(text.into()))
    }

    pub(crate) fn hydrate_op(&self, op: Op<'_>, clock: Option<&Clock>) -> Value {
        match op.action() {
            OpType::Make(ObjType::Map) => self.hydrate_map(&op.id.into(), clock),
            OpType::Make(ObjType::Table) => self.hydrate_map(&op.id.into(), clock),
            OpType::Make(ObjType::List) => self.hydrate_list(&op.id.into(), clock),
            OpType::Make(ObjType::Text) => self.hydrate_text(&op.id.into(), clock),
            OpType::Put(scalar) => Value::Scalar(scalar.clone().into()),
            _ => panic!("invalid op to hydrate"),
        }
    }
}

#[macro_export]
macro_rules! hydrate_map {
    {} => {
        $crate::hydrate::Map::default()
    };
    {$($k: expr => $v: expr),* $(,)?} => {
        $crate::hydrate::Map::from(std::collections::HashMap::from([$(($k, $crate::hydrate::Value::from($v)),)*]))
    };
}

#[macro_export]
macro_rules! hydrate_list {
    {$($v: expr),* $(,)?} => {
        $crate::hydrate::List::from(Vec::<$crate::hydrate::Value>::from([$($crate::hydrate::Value::from($v),)*]))
    };
}

#[macro_export]
macro_rules! hydrate_text {
    {$t: expr} => {
        $crate::hydrate::Text::from($t)
    };
}

#[cfg(feature = "wasm")]
impl From<Value> for wasm_bindgen::JsValue {
    fn from(value: Value) -> Self {
        (&value).into()
    }
}

#[cfg(feature = "wasm")]
impl From<&Value> for wasm_bindgen::JsValue {
    fn from(value: &Value) -> Self {
        use js_sys::{Array, Date, Object, Reflect, Uint8Array};
        use wasm_bindgen::JsValue;
        match value {
            Value::Scalar(s) => match s {
                ScalarValue::Bytes(v) => Uint8Array::from(v.as_slice()).into(),
                ScalarValue::Str(v) => v.to_string().into(),
                ScalarValue::Int(v) => (*v as f64).into(),
                ScalarValue::Uint(v) => (*v as f64).into(),
                ScalarValue::F64(v) => (*v).into(),
                ScalarValue::Counter(v) => (f64::from(v)).into(),
                ScalarValue::Timestamp(v) => Date::new(&(*v as f64).into()).into(),
                ScalarValue::Boolean(v) => (*v).into(),
                ScalarValue::Null => JsValue::null(),
                ScalarValue::Unknown {
                    bytes,
                    type_code: _,
                } => Uint8Array::from(bytes.as_slice()).into(),
            },
            Value::Map(m) => {
                let result = Object::new();
                for (key, val) in m.iter() {
                    Reflect::set(&result, &key.into(), &JsValue::from(&val.value)).unwrap();
                }
                result.into()
            }
            Value::List(l) => l
                .iter()
                .map(|v| JsValue::from(&v.value))
                .collect::<Array>()
                .into(),
            Value::Text(t) => String::from(t).into(),
        }
    }
}

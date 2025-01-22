use crate::patches::TextRepresentation;
use crate::types::{Clock, ObjId, Op, OpType};
use crate::{error::HydrateError, value, ObjType, Patch, PatchAction, Prop, ScalarValue};
use std::borrow::Cow;
use std::collections::HashMap;

mod list;
mod map;
mod text;

#[cfg(test)]
mod tests;

use crate::Automerge;

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

impl Value {
    pub fn new<'a, V: Into<crate::Value<'a>>>(value: V, text_rep: TextRepresentation) -> Self {
        match value.into() {
            value::Value::Object(ObjType::Map) => Value::Map(Map::default()),
            value::Value::Object(ObjType::List) => Value::List(List::default()),
            value::Value::Object(ObjType::Text) => Value::Text(Text::new(text_rep, "")),
            value::Value::Object(ObjType::Table) => Value::Map(Map::default()),
            value::Value::Scalar(s) => Value::Scalar(s.into_owned()),
        }
    }

    pub fn is_scalar(&self) -> bool {
        matches!(self, Value::Scalar(_))
    }

    pub fn is_object(&self) -> bool {
        !self.is_scalar()
    }

    pub fn apply_patches<P: IntoIterator<Item = Patch>>(
        &mut self,
        text_rep: TextRepresentation,
        patches: P,
    ) -> Result<(), HydrateError> {
        for p in patches {
            self.apply(p.path.iter().map(|(_, prop)| prop), text_rep, p.action)?;
        }
        Ok(())
    }

    pub(crate) fn apply<'a, P: Iterator<Item = &'a Prop>>(
        &mut self,
        mut path: P,
        text_rep: TextRepresentation,
        patch: PatchAction,
    ) -> Result<(), HydrateError> {
        match (path.next(), self) {
            (Some(Prop::Seq(n)), Value::List(list)) => list
                .get_mut(*n)
                .ok_or_else(|| HydrateError::ApplyInvalidProp(patch.clone()))?
                .apply(path, text_rep, patch),
            (Some(Prop::Map(s)), Value::Map(map)) => map
                .get_mut(s)
                .ok_or_else(|| HydrateError::ApplyInvalidProp(patch.clone()))?
                .apply(path, text_rep, patch),
            (None, Value::Map(map)) => map.apply(text_rep, patch),
            (None, Value::List(list)) => list.apply(text_rep, patch),
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

impl From<HashMap<&str, Value>> for Value {
    fn from(value: HashMap<&str, Value>) -> Self {
        Value::Map(value.into())
    }
}

impl<'a> From<&'a str> for Value {
    fn from(value: &'a str) -> Self {
        Value::Scalar(ScalarValue::Str(value.into()))
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::Scalar(ScalarValue::Uint(value as u64))
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::Scalar(ScalarValue::Uint(value))
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Scalar(ScalarValue::Int(value as i64))
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Scalar(ScalarValue::Int(value))
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Scalar(ScalarValue::F64(value))
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Scalar(ScalarValue::F64(value as f64))
    }
}

impl From<ScalarValue> for Value {
    fn from(value: ScalarValue) -> Self {
        Value::Scalar(value)
    }
}

impl Automerge {
    pub(crate) fn hydrate_map(&self, obj: &ObjId, clock: Option<&Clock>) -> Value {
        let mut map = Map::new();
        for top in self.ops().top_ops(obj, clock.cloned()) {
            let key = self.ops().to_string(top.op.elemid_or_key());
            let value = self.hydrate_op(top.op, clock);
            let id = top.op.exid();
            let conflict = top.conflict;
            map.insert(key, MapValue::new(value, id, conflict));
        }
        Value::Map(map)
    }

    pub(crate) fn hydrate_list(&self, obj: &ObjId, clock: Option<&Clock>) -> Value {
        let mut list = List::new();
        for top in self.ops().top_ops(obj, clock.cloned()) {
            let value = self.hydrate_op(top.op, clock);
            let id = top.op.exid();
            let conflict = top.conflict;
            list.push(value, id, conflict);
        }
        Value::List(list)
    }

    pub(crate) fn hydrate_text(&self, obj: &ObjId, clock: Option<&Clock>) -> Value {
        let text = self.ops().text(obj, clock.cloned());
        Value::Text(Text::new(self.text_encoding().into(), text))
    }

    pub(crate) fn hydrate_op(&self, op: Op<'_>, clock: Option<&Clock>) -> Value {
        match op.action() {
            OpType::Make(ObjType::Map) => self.hydrate_map(&op.id().into(), clock),
            OpType::Make(ObjType::Table) => self.hydrate_map(&op.id().into(), clock),
            OpType::Make(ObjType::List) => self.hydrate_list(&op.id().into(), clock),
            OpType::Make(ObjType::Text) => self.hydrate_text(&op.id().into(), clock),
            OpType::Put(scalar) => Value::Scalar(scalar.clone()),
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
        $crate::hydrate::Text::new($crate::patches::TextRepresentation::String($crate::TextEncoding::default()), $t)
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

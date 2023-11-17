use crate::types::{Clock, ObjId, Op, OpType};
use crate::{error::HydrateError, value, ObjType, Patch, PatchAction, Prop, ScalarValue};
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
        Value::Text(Text::new(text.into()))
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
    {$($k: expr => $v: expr),* $(,)?} => {
        hydrate::Value::from(std::collections::HashMap::from([$(($k, hydrate::Value::from($v)),)*]))
    };
}

#[macro_export]
macro_rules! hydrate_list {
    {$($v: expr),* $(,)?} => {
        hydrate::Value::from(Vec::<hydrate::Value>::from([$(hydrate::Value::from($v),)*]))
    };
}

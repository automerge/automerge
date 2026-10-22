use crate::automerge::view::ReadAt;
use crate::op_set2::{KeyRef, Op, OpSet, OpType, TopOps};
use crate::types::{ObjId, ScalarValue, SequenceType};
use crate::TextEncoding;
use crate::{error::HydrateError, value, ObjType, Patch, PatchAction, Prop};
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
    pub fn new<'a, V: Into<crate::Value<'a>>>(value: V, text_encoding: TextEncoding) -> Self {
        match value.into() {
            value::Value::Object(ObjType::Map) => Value::Map(Map::default()),
            value::Value::Object(ObjType::List) => Value::List(List::default()),
            value::Value::Object(ObjType::Text) => Value::Text(Text::new(text_encoding, "")),
            value::Value::Object(ObjType::Table) => Value::Map(Map::default()),
            value::Value::Scalar(s) => Value::Scalar(s.into_owned()),
        }
    }

    pub fn map() -> Self {
        Value::Map(Map::default())
    }

    pub fn list() -> Self {
        Value::List(List::default())
    }

    pub fn text(text_encoding: TextEncoding, s: &str) -> Self {
        Value::Text(Text::new(text_encoding, s))
    }

    pub fn scalar<V: Into<ScalarValue>>(val: V) -> Self {
        Value::Scalar(val.into())
    }

    pub fn is_scalar(&self) -> bool {
        matches!(self, Value::Scalar(_))
    }

    pub(crate) fn width(&self, seq_type: SequenceType, encoding: TextEncoding) -> usize {
        match seq_type {
            SequenceType::List => 1,
            SequenceType::Text => encoding.width(self.as_str()),
        }
    }

    pub fn is_object(&self) -> bool {
        !self.is_scalar()
    }

    pub(crate) fn as_str(&self) -> &str {
        match &self {
            Self::Scalar(ScalarValue::Str(s)) => s,
            _ => "\u{fffc}",
        }
    }

    pub fn apply_patches<P: IntoIterator<Item = Patch>>(
        &mut self,
        text_encoding: TextEncoding,
        patches: P,
    ) -> Result<(), HydrateError> {
        for p in patches {
            self.apply(p.path.iter().map(|(_, prop)| prop), text_encoding, p.action)?;
        }
        Ok(())
    }

    pub(crate) fn apply<'a, P: Iterator<Item = &'a Prop>>(
        &mut self,
        mut path: P,
        text_encoding: TextEncoding,
        patch: PatchAction,
    ) -> Result<(), HydrateError> {
        match (path.next(), self) {
            (Some(Prop::Seq(n)), Value::List(list)) => list
                .get_mut(*n)
                .ok_or_else(|| HydrateError::ApplyInvalidProp(patch.clone()))?
                .apply(path, text_encoding, patch),
            (Some(Prop::Map(s)), Value::Map(map)) => map
                .get_mut(s)
                .ok_or_else(|| HydrateError::ApplyInvalidProp(patch.clone()))?
                .apply(path, text_encoding, patch),
            // Hydrated text represents embedded block objects only by their
            // replacement characters, so patches to a block's contents do
            // not affect the hydrated text value.
            (Some(Prop::Seq(_)), Value::Text(_)) => Ok(()),
            (None, Value::Map(map)) => map.apply(text_encoding, patch),
            (None, Value::List(list)) => list.apply(text_encoding, patch),
            (None, Value::Text(text)) => text.apply(text_encoding, patch),
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

    pub fn as_i64(&self) -> i64 {
        match self {
            Value::Scalar(s) => s.as_i64_or_default(),
            _ => 0,
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
    pub(crate) fn hydrate_map(&self, obj: &ObjId, read: &ReadAt<'_>) -> Value {
        self.ops().hydrate_map(obj, read, self.text_encoding())
    }
    pub(crate) fn hydrate_list(&self, obj: &ObjId, read: &ReadAt<'_>) -> Value {
        self.ops().hydrate_list(obj, read, self.text_encoding())
    }
    pub(crate) fn hydrate_text(&self, obj: &ObjId, read: &ReadAt<'_>) -> Value {
        self.ops().hydrate_text(obj, read, self.text_encoding())
    }

    /// The object `obj`, of type `typ`, built under `read` from `gathered`.
    pub(crate) fn hydrate_gathered_at(
        &self,
        gathered: &mut Gathered,
        obj: &crate::ObjId,
        read: &ReadAt<'_>,
    ) -> Result<Value, crate::AutomergeError> {
        let obj = self.exid_to_obj(obj)?;
        Ok(gathered.hydrate(self.ops(), &obj.id, obj.typ, read, self.text_encoding()))
    }
}

impl OpSet {
    pub(crate) fn hydrate_map(
        &self,
        obj: &ObjId,
        read: &ReadAt<'_>,
        encoding: TextEncoding,
    ) -> Value {
        let mut map = Map::new();
        for top in self.top_ops(obj, read.borrow()) {
            let key = self.to_string(top.elemid_or_key());
            let id = self.id_to_exid(top.id);
            let conflict = top.conflict;
            let value = self.hydrate_op(top, read, encoding);
            map.insert(key, MapValue::new(value, id, conflict));
        }
        Value::Map(map)
    }

    pub(crate) fn hydrate_list(
        &self,
        obj: &ObjId,
        read: &ReadAt<'_>,
        encoding: TextEncoding,
    ) -> Value {
        let mut list = List::new();
        for top in self.top_ops(obj, read.borrow()) {
            //let id = top.exid();
            let id = self.id_to_exid(top.id);
            let conflict = top.conflict;
            let value = self.hydrate_op(top, read, encoding);
            list.push(value, id, conflict);
        }
        Value::List(list)
    }

    pub(crate) fn hydrate_text(
        &self,
        obj: &ObjId,
        read: &ReadAt<'_>,
        encoding: TextEncoding,
    ) -> Value {
        let text = self.text(obj, read.borrow());
        Value::Text(Text::new(encoding, text))
    }

    /// Every object's top ops under `read`, gathered for [`Gathered::hydrate`]
    /// to build objects from.
    ///
    /// For a current read this is one ordered pass over the whole op set,
    /// with one seek, where [`OpSet::hydrate_map`] seeks every column to each
    /// object it reads: for a document of many small objects the seeks cost
    /// more than reading the ops. A historical read can only decide which ops
    /// are on top within one object, so it is gathered object by object.
    pub(crate) fn gather(&self, read: &ReadAt<'_>) -> Gathered {
        let mut gathered = Gathered::default();
        let mut text_of = HashMap::new();
        if read.historical().is_some() {
            let mut last = None;
            for (obj, _) in self.iter_obj_ids() {
                if last != Some(obj) {
                    last = Some(obj);
                    let tops = self.top_ops(&obj, read.borrow());
                    gathered.gather(self, tops, &mut text_of);
                }
            }
        } else {
            let tops = TopOps::new(self, read, 0..self.len());
            gathered.gather(self, tops, &mut text_of);
        }
        gathered.objects.shrink_to_fit();
        gathered
    }

    pub(crate) fn hydrate_op(
        &self,
        op: Op<'_>,
        read: &ReadAt<'_>,
        encoding: TextEncoding,
    ) -> Value {
        match op.action() {
            OpType::Make(ObjType::Map) => self.hydrate_map(&op.id.into(), read, encoding),
            OpType::Make(ObjType::Table) => self.hydrate_map(&op.id.into(), read, encoding),
            OpType::Make(ObjType::List) => self.hydrate_list(&op.id.into(), read, encoding),
            OpType::Make(ObjType::Text) => self.hydrate_text(&op.id.into(), read, encoding),
            OpType::Put(scalar) => Value::Scalar(scalar.into()),
            _ => panic!("invalid op to hydrate"),
        }
    }
}

/// Every object's visible contents, read from a document by
/// [`Automerge::gather_in_one_pass`] (or [`crate::AutoCommit::gather_in_one_pass`])
/// so that objects can then be built from it without going back to the
/// document's ops for each one.
///
/// Building an object with [`Automerge::hydrate_gathered`] takes its
/// entries, and those of the objects inside it, out of the gathering, so a
/// caller that builds a large document's objects one at a time holds the
/// gathering and one object rather than the whole tree twice. Each object
/// can be built once; built again, it is empty. Text is read from the
/// document when its object is built.
///
/// Objects no longer reachable from the root, such as a deleted map, are
/// gathered too, so that they can be built by id as [`ReadDoc::hydrate`]
/// builds them; they stay in the gathering until they are.
///
/// [`ReadDoc::hydrate`]: crate::ReadDoc::hydrate
#[derive(Debug, Default)]
pub struct Gathered {
    objects: HashMap<ObjId, Vec<Entry>>,
    /// For each text object, the objects inside it (block maps and the
    /// objects in those), which building the text, read whole from the
    /// document, takes out of the gathering.
    in_text: HashMap<ObjId, Vec<ObjId>>,
}

/// One visible top op of an object, as [`Gathered`] holds it: its map key,
/// if the object is a map, and its value or the object it makes.
#[derive(Debug)]
struct Entry {
    key: Option<String>,
    slot: Slot,
    conflict: bool,
}

#[derive(Debug)]
enum Slot {
    Scalar(ScalarValue),
    Object(ObjId, ObjType),
}

impl Gathered {
    /// How many objects are gathered and not yet built. An object with no
    /// visible entries, such as an empty map, is not counted.
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Whether every gathered object has been built.
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Adds the entries of `tops`, which are top ops in op set order. The
    /// ops of a text object are passed over: text is read whole when its
    /// object is built. An object inside a text, such as a block map, is
    /// gathered and recorded in `in_text`; `text_of` maps each object met
    /// inside a text to that text. An object comes after its parent in op
    /// set order, so its parent has been met.
    fn gather<'a>(
        &mut self,
        ops: &OpSet,
        tops: impl Iterator<Item = Op<'a>>,
        text_of: &mut HashMap<ObjId, ObjId>,
    ) {
        let mut current: Option<(ObjId, Option<Vec<Entry>>)> = None;
        for top in tops {
            if current.as_ref().map(|(obj, _)| *obj) != Some(top.obj) {
                if let Some((obj, Some(entries))) = current.take() {
                    self.objects.insert(obj, entries);
                }
                let is_text = ops.object_type(&top.obj) == Some(ObjType::Text);
                if is_text {
                    text_of.insert(top.obj, top.obj);
                } else if let Some(text) = ops
                    .object_parent(&top.obj)
                    .and_then(|parent| text_of.get(&parent).copied())
                {
                    text_of.insert(top.obj, text);
                    self.in_text.entry(text).or_default().push(top.obj);
                }
                current = Some((top.obj, (!is_text).then(Vec::new)));
            }
            let Some((_, Some(entries))) = current.as_mut() else {
                continue;
            };
            let key = match top.elemid_or_key() {
                KeyRef::Map(key) => Some(key.into_owned()),
                KeyRef::Seq(_) => None,
            };
            let slot = match top.action() {
                OpType::Make(typ) => Slot::Object(top.id.into(), typ),
                OpType::Put(scalar) => Slot::Scalar(scalar.into()),
                // Only text has marks; nothing else is a top op.
                _ => continue,
            };
            entries.push(Entry {
                key,
                slot,
                conflict: top.conflict,
            });
        }
        if let Some((obj, Some(entries))) = current {
            self.objects.insert(obj, entries);
        }
    }

    /// Takes the objects inside the text `text` out of the gathering: the
    /// text is read whole from the document.
    fn take_text_objects(&mut self, text: &ObjId) {
        for obj in self.in_text.remove(text).unwrap_or_default() {
            self.objects.remove(&obj);
        }
    }

    /// The value of `obj`, of type `typ`, built from its gathered entries
    /// and, recursively, those of the objects they make, each taken out of
    /// the gathering. Text is read from `ops` under `read`. An object with no
    /// entries, or one already built, is empty.
    pub(crate) fn hydrate(
        &mut self,
        ops: &OpSet,
        obj: &ObjId,
        typ: ObjType,
        read: &ReadAt<'_>,
        encoding: TextEncoding,
    ) -> Value {
        if typ == ObjType::Text {
            self.take_text_objects(obj);
            return ops.hydrate_text(obj, read, encoding);
        }
        let entries = self.objects.remove(obj).unwrap_or_default();
        let mut value = |slot| match slot {
            Slot::Scalar(scalar) => Value::Scalar(scalar),
            Slot::Object(child, typ) => self.hydrate(ops, &child, typ, read, encoding),
        };
        match typ {
            ObjType::Map | ObjType::Table => {
                let mut map = Map::new();
                for Entry {
                    key,
                    slot,
                    conflict,
                } in entries
                {
                    let value = value(slot);
                    map.insert(key.unwrap_or_default(), MapValue { value, conflict });
                }
                Value::Map(map)
            }
            ObjType::List | ObjType::Text => {
                let mut list = List::new();
                for Entry { slot, conflict, .. } in entries {
                    list.push_value(value(slot), conflict);
                }
                Value::List(list)
            }
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
        $crate::hydrate::Text::new($crate::TextEncoding::platform_default(), $t)
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

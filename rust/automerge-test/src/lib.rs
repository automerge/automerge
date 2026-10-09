use std::{
    collections::{BTreeMap, BTreeSet},
    convert::TryInto,
    hash::Hash,
};

use automerge::ReadDoc;

use serde::ser::{SerializeMap, SerializeSeq};

pub fn new_doc() -> automerge::AutoCommit {
    let mut d = automerge::AutoCommit::new();
    d.set_actor(automerge::ActorId::random());
    d
}

pub fn new_doc_with_actor(actor: automerge::ActorId) -> automerge::AutoCommit {
    let mut d = automerge::AutoCommit::new();
    d.set_actor(actor);
    d
}

/// Returns two actor IDs, the first considered to  be ordered before the second
pub fn sorted_actors() -> (automerge::ActorId, automerge::ActorId) {
    let a = automerge::ActorId::random();
    let b = automerge::ActorId::random();
    if a > b {
        (b, a)
    } else {
        (a, b)
    }
}

/// This macro makes it easy to make assertions about a document. It is called with two arguments,
/// the first is a reference to an `automerge::Automerge`, the second is an instance of
/// `RealizedObject<ExportableOpId>`.
///
/// What - I hear you ask - is a `RealizedObject`? It's a fully hydrated version of the contents of
/// an automerge document. You don't need to think about this too much though because you can
/// easily construct one with the `map!` and `list!` macros. Here's an example:
///
/// ## Constructing documents
///
/// ```rust
/// # use automerge::transaction::Transactable;
/// # use automerge_test::{assert_doc, map, list};
/// let mut doc = automerge::AutoCommit::new();
/// let todos = doc.put_object(automerge::ROOT, "todos", automerge::ObjType::List).unwrap();
/// let todo = doc.insert_object(todos, 0, automerge::ObjType::Map).unwrap();
/// let title = doc.put(todo, "title", "water plants").unwrap();
///
/// assert_doc!(
///     &doc,
///     map!{
///         "todos" => {
///             list![
///                 { map!{ "title" => { "water plants" } } }
///             ]
///         }
///     }
/// );
///
/// ```
///
/// This might look more complicated than you were expecting. Why is the first element in the list
/// wrapped in braces? Because every property in an automerge document can have multiple
/// conflicting values we must capture all of these.
///
/// ```rust
/// # use automerge_test::{assert_doc, map};
/// # use automerge::transaction::Transactable;
/// # use automerge::ReadDoc;
///
/// let mut doc1 = automerge::AutoCommit::new();
/// let mut doc2 = automerge::AutoCommit::new();
/// doc1.put(automerge::ROOT, "field", "one").unwrap();
/// doc2.put(automerge::ROOT, "field", "two").unwrap();
/// doc1.merge(&mut doc2);
/// assert_doc!(
///     doc1.document(),
///     map!{
///         "field" => {
///             "one",
///             "two"
///         }
///     }
/// );
/// ```
#[macro_export]
macro_rules! assert_doc {
    ($doc: expr, $expected: expr) => {{
        use $crate::realize;
        let realized = realize($doc);
        let expected_obj = $expected.into();
        if realized != expected_obj {
            $crate::pretty_panic(expected_obj, realized)
        }
    }};
}

/// Like `assert_doc` except that you can specify an object ID and property to select subsections
/// of the document.
#[macro_export]
macro_rules! assert_obj {
    ($doc: expr, $obj_id: expr, $prop: expr, $expected: expr) => {{
        use $crate::realize_prop;
        let realized = realize_prop($doc, $obj_id, $prop);
        let expected_obj = $expected.into();
        if realized != expected_obj {
            $crate::pretty_panic(expected_obj, realized)
        }
    }};
}

/// Construct `RealizedObject::Map`. This macro takes a nested set of curl braces. The outer set is
/// the keys of the map, the inner set is the set of values for that key:
///
/// ```
/// # use automerge_test::map;
/// map!{
///     "key" => {
///         "value1",
///         "value2",
///     }
/// };
/// ```
///
/// The map above would represent a map with a conflict on the "key" property. The values can be
/// anything which implements `Into<RealizedObject>`. Including nested calls to `map!` or `list!`.
#[macro_export]
macro_rules! map {
    (@inner { $($value:expr,)+ }) => { map!(@inner { $($value),+ }) };
    (@inner { $($value:expr),* }) => {
        {
            use std::collections::BTreeSet;
            use $crate::RealizedObject;
            let mut inner: BTreeSet<RealizedObject> = BTreeSet::new();
            $(
                let _ = inner.insert($value.into());
            )*
            inner
        }
    };
    ($($key:expr => $inner:tt,)+) => { map!($($key => $inner),+) };
    ($($key:expr => $inner:tt),*) => {
        {
            use std::collections::{BTreeMap, BTreeSet};
            use $crate::RealizedObject;
            let mut _map: BTreeMap<String, BTreeSet<RealizedObject>> = ::std::collections::BTreeMap::new();
            $(
                let inner = map!(@inner $inner);
                let _ = _map.insert($key.to_string(), inner);
            )*
            RealizedObject::Map(_map)
        }
    }
}

/// Construct `RealizedObject::Sequence`. This macro represents a sequence of values
///
/// ```
/// # use automerge_test::{list, RealizedObject};
/// list![
///     {
///         "value1",
///         "value2",
///     }
/// ];
/// ```
///
/// The list above would represent a list with a conflict on the 0 index. The values can be
/// anything which implements `Into<RealizedObject>` including nested calls to
/// `map!` or `list!`.
#[macro_export]
macro_rules! list {
    (@single $($x:tt)*) => (());
    (@count $($rest:tt),*) => (<[()]>::len(&[$(list!(@single $rest)),*]));

    (@inner { $($value:expr,)+ }) => { list!(@inner { $($value),+ }) };
    (@inner { $($value:expr),* }) => {
        {
            use std::collections::BTreeSet;
            use $crate::RealizedObject;
            let mut inner: BTreeSet<RealizedObject> = BTreeSet::new();
            $(
                let _ = inner.insert($value.into());
            )*
            inner
        }
    };
    ($($inner:tt,)+) => { list!($($inner),+) };
    ($($inner:tt),*) => {
        {
            use std::collections::BTreeSet;
            let _cap = list!(@count $($inner),*);
            let mut _list: Vec<BTreeSet<RealizedObject>> = Vec::new();
            $(
                let inner = list!(@inner $inner);
                let _ = _list.push(inner);
            )*
            RealizedObject::Sequence(_list)
        }
    }
}

pub fn mk_counter(value: i64) -> automerge::ScalarValue {
    automerge::ScalarValue::counter(value)
}

/// A `RealizedObject` is a representation of all the current values in a document - including
/// conflicts.
#[derive(PartialEq, PartialOrd, Ord, Eq, Hash, Debug)]
pub enum RealizedObject {
    Map(BTreeMap<String, BTreeSet<RealizedObject>>),
    Sequence(Vec<BTreeSet<RealizedObject>>),
    Value(OrdScalarValue),
}

// A copy of automerge::ScalarValue which uses decorum::Total for floating point values. This makes the type
// orderable, which is useful when we want to compare conflicting values of a register in an
// automerge document.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum OrdScalarValue {
    Bytes(Vec<u8>),
    Str(smol_str::SmolStr),
    Int(i64),
    Uint(u64),
    F64(decorum::Total<f64>),
    Counter(i64),
    Timestamp(i64),
    Boolean(bool),
    Null,
    Unknown { type_code: u8, bytes: Vec<u8> },
}

impl From<automerge::ScalarValue> for OrdScalarValue {
    fn from(v: automerge::ScalarValue) -> Self {
        match v {
            automerge::ScalarValue::Bytes(v) => OrdScalarValue::Bytes(v),
            automerge::ScalarValue::Str(v) => OrdScalarValue::Str(v),
            automerge::ScalarValue::Int(v) => OrdScalarValue::Int(v),
            automerge::ScalarValue::Uint(v) => OrdScalarValue::Uint(v),
            automerge::ScalarValue::F64(v) => OrdScalarValue::F64(decorum::Total::from(v)),
            automerge::ScalarValue::Counter(c) => OrdScalarValue::Counter(c.into()),
            automerge::ScalarValue::Timestamp(v) => OrdScalarValue::Timestamp(v),
            automerge::ScalarValue::Boolean(v) => OrdScalarValue::Boolean(v),
            automerge::ScalarValue::Null => OrdScalarValue::Null,
            automerge::ScalarValue::Unknown { type_code, bytes } => {
                OrdScalarValue::Unknown { type_code, bytes }
            }
        }
    }
}

impl From<&OrdScalarValue> for automerge::ScalarValue {
    fn from(v: &OrdScalarValue) -> Self {
        match v {
            OrdScalarValue::Bytes(v) => automerge::ScalarValue::Bytes(v.clone()),
            OrdScalarValue::Str(v) => automerge::ScalarValue::Str(v.clone()),
            OrdScalarValue::Int(v) => automerge::ScalarValue::Int(*v),
            OrdScalarValue::Uint(v) => automerge::ScalarValue::Uint(*v),
            OrdScalarValue::F64(v) => automerge::ScalarValue::F64(v.into_inner()),
            OrdScalarValue::Counter(v) => automerge::ScalarValue::counter(*v),
            OrdScalarValue::Timestamp(v) => automerge::ScalarValue::Timestamp(*v),
            OrdScalarValue::Boolean(v) => automerge::ScalarValue::Boolean(*v),
            OrdScalarValue::Null => automerge::ScalarValue::Null,
            OrdScalarValue::Unknown { type_code, bytes } => automerge::ScalarValue::Unknown {
                type_code: *type_code,
                bytes: bytes.to_vec(),
            },
        }
    }
}

impl serde::Serialize for OrdScalarValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            OrdScalarValue::Bytes(v) => serializer.serialize_bytes(v),
            OrdScalarValue::Str(v) => serializer.serialize_str(v.as_str()),
            OrdScalarValue::Int(v) => serializer.serialize_i64(*v),
            OrdScalarValue::Uint(v) => serializer.serialize_u64(*v),
            OrdScalarValue::F64(v) => serializer.serialize_f64(v.into_inner()),
            OrdScalarValue::Counter(v) => {
                serializer.serialize_str(format!("Counter({})", v).as_str())
            }
            OrdScalarValue::Timestamp(v) => {
                serializer.serialize_str(format!("Timestamp({})", v).as_str())
            }
            OrdScalarValue::Boolean(v) => serializer.serialize_bool(*v),
            OrdScalarValue::Null => serializer.serialize_none(),
            OrdScalarValue::Unknown { type_code, .. } => serializer
                .serialize_str(format!("An unknown type with code {}", type_code).as_str()),
        }
    }
}

impl serde::Serialize for RealizedObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Map(kvs) => {
                let mut map_ser = serializer.serialize_map(Some(kvs.len()))?;
                for (k, vs) in kvs {
                    let vs_serded = vs.iter().collect::<Vec<&RealizedObject>>();
                    map_ser.serialize_entry(k, &vs_serded)?;
                }
                map_ser.end()
            }
            Self::Sequence(elems) => {
                let mut list_ser = serializer.serialize_seq(Some(elems.len()))?;
                for elem in elems {
                    let vs_serded = elem.iter().collect::<Vec<&RealizedObject>>();
                    list_ser.serialize_element(&vs_serded)?;
                }
                list_ser.end()
            }
            Self::Value(v) => v.serialize(serializer),
        }
    }
}

pub fn realize<R: ReadDoc>(doc: &R) -> RealizedObject {
    realize_obj(doc, &automerge::ROOT, automerge::ObjType::Map)
}

pub fn realize_prop<R: ReadDoc, P: Into<automerge::Prop>>(
    doc: &R,
    obj_id: &automerge::ObjId,
    prop: P,
) -> RealizedObject {
    let (val, obj_id) = doc.get(obj_id, prop).unwrap().unwrap();
    match val {
        automerge::Value::Object(obj_type) => realize_obj(doc, &obj_id, obj_type),
        automerge::Value::Scalar(v) => RealizedObject::Value(OrdScalarValue::from(v.into_owned())),
    }
}

pub fn realize_obj<R: ReadDoc>(
    doc: &R,
    obj_id: &automerge::ObjId,
    objtype: automerge::ObjType,
) -> RealizedObject {
    match objtype {
        automerge::ObjType::Map | automerge::ObjType::Table => {
            let mut result = BTreeMap::new();
            for key in doc.keys(obj_id) {
                result.insert(key.clone(), realize_values(doc, obj_id, key));
            }
            RealizedObject::Map(result)
        }
        automerge::ObjType::List | automerge::ObjType::Text => {
            let length = doc.length(obj_id);
            let mut result = Vec::with_capacity(length);
            for i in 0..length {
                result.push(realize_values(doc, obj_id, i));
            }
            RealizedObject::Sequence(result)
        }
    }
}

fn realize_values<R: ReadDoc, K: Into<automerge::Prop>>(
    doc: &R,
    obj_id: &automerge::ObjId,
    key: K,
) -> BTreeSet<RealizedObject> {
    let mut values = BTreeSet::new();
    for (value, objid) in doc.get_all(obj_id, key).unwrap() {
        let realized = match value {
            automerge::Value::Object(objtype) => realize_obj(doc, &objid, objtype),
            automerge::Value::Scalar(v) => {
                RealizedObject::Value(OrdScalarValue::from(v.into_owned()))
            }
        };
        values.insert(realized);
    }
    values
}

impl<I: Into<RealizedObject>> From<BTreeMap<&str, BTreeSet<I>>> for RealizedObject {
    fn from(values: BTreeMap<&str, BTreeSet<I>>) -> Self {
        let intoed = values
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.into_iter().map(|v| v.into()).collect()))
            .collect();
        RealizedObject::Map(intoed)
    }
}

impl<I: Into<RealizedObject>> From<Vec<BTreeSet<I>>> for RealizedObject {
    fn from(values: Vec<BTreeSet<I>>) -> Self {
        RealizedObject::Sequence(
            values
                .into_iter()
                .map(|v| v.into_iter().map(|v| v.into()).collect())
                .collect(),
        )
    }
}

impl From<bool> for RealizedObject {
    fn from(b: bool) -> Self {
        RealizedObject::Value(OrdScalarValue::Boolean(b))
    }
}

impl From<usize> for RealizedObject {
    fn from(u: usize) -> Self {
        let v = u.try_into().unwrap();
        RealizedObject::Value(OrdScalarValue::Int(v))
    }
}

impl From<u64> for RealizedObject {
    fn from(v: u64) -> Self {
        RealizedObject::Value(OrdScalarValue::Uint(v))
    }
}

impl From<u32> for RealizedObject {
    fn from(v: u32) -> Self {
        RealizedObject::Value(OrdScalarValue::Uint(v.into()))
    }
}

impl From<i64> for RealizedObject {
    fn from(v: i64) -> Self {
        RealizedObject::Value(OrdScalarValue::Int(v))
    }
}

impl From<i32> for RealizedObject {
    fn from(v: i32) -> Self {
        RealizedObject::Value(OrdScalarValue::Int(v.into()))
    }
}

impl From<automerge::ScalarValue> for RealizedObject {
    fn from(s: automerge::ScalarValue) -> Self {
        RealizedObject::Value(OrdScalarValue::from(s))
    }
}

impl From<&str> for RealizedObject {
    fn from(s: &str) -> Self {
        RealizedObject::Value(OrdScalarValue::Str(smol_str::SmolStr::from(s)))
    }
}

impl From<f64> for RealizedObject {
    fn from(f: f64) -> Self {
        RealizedObject::Value(OrdScalarValue::F64(f.into()))
    }
}

impl From<Vec<u64>> for RealizedObject {
    fn from(vals: Vec<u64>) -> Self {
        RealizedObject::Sequence(
            vals.into_iter()
                .map(|i| {
                    let mut set = BTreeSet::new();
                    set.insert(i.into());
                    set
                })
                .collect(),
        )
    }
}

/// Pretty print the contents of a document
pub fn pretty_print(doc: &automerge::Automerge) {
    println!("{}", serde_json::to_string_pretty(&realize(doc)).unwrap())
}

pub fn pretty_panic(expected_obj: RealizedObject, realized: RealizedObject) {
    let serde_right = serde_json::to_string_pretty(&realized).unwrap();
    let serde_left = serde_json::to_string_pretty(&expected_obj).unwrap();
    panic!(
        "documents didn't match\n expected\n{}\n got\n{}",
        &serde_left, &serde_right
    );
}

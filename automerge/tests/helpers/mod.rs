use automerge::ObjId;

use std::{collections::HashMap, convert::TryInto, hash::Hash};

use serde::ser::{SerializeMap, SerializeSeq};

pub fn new_doc() -> automerge::Automerge {
    automerge::Automerge::new_with_actor_id(automerge::ActorId::random())
}

pub fn new_doc_with_actor(actor: automerge::ActorId) -> automerge::Automerge {
    automerge::Automerge::new_with_actor_id(actor)
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
/// `RealizedObject`.
///
/// What - I hear you ask - is a `RealizedObject`? It's a fully hydrated version of the contents of
/// an automerge document. You don't need to think about this too much though because you can
/// easily construct one with the `map!` and `list!` macros. Here's an example:
///
/// ## Constructing documents
///
/// ```rust
/// let mut doc = automerge::Automerge::new();
/// let todos = doc.set(automerge::ROOT, "todos", automerge::Value::map()).unwrap().unwrap();
/// let todo = doc.insert(todos, 0, automerge::Value::map()).unwrap();
/// let title = doc.set(todo, "title", "water plants").unwrap().unwrap();
///
/// assert_doc!(
///     &doc,
///     map!{
///         "todos" => {
///             todos => list![
///                 { todo => map!{ title = "water plants" } }
///             ]
///         }
///     }
/// );
///
/// ```
///
/// This might look more complicated than you were expecting. Why are there OpIds (`todos`, `todo`,
/// `title`) in there? Well the `RealizedObject` contains all the changes in the document tagged by
/// OpId. This makes it easy to test for conflicts:
///
/// ```rust
/// let mut doc1 = automerge::Automerge::new();
/// let mut doc2 = automerge::Automerge::new();
/// let op1 = doc1.set(automerge::ROOT, "field", "one").unwrap().unwrap();
/// let op2 = doc2.set(automerge::ROOT, "field", "two").unwrap().unwrap();
/// doc1.merge(&mut doc2);
/// assert_doc!(
///     &doc1,
///     map!{
///         "field" => {
///             op1 => "one",
///             op2 => "two"
///         }
///     }
/// );
/// ```
#[macro_export]
macro_rules! assert_doc {
    ($doc: expr, $expected: expr) => {{
        use $crate::helpers::realize;
        let realized = realize($doc);
        let exported: RealizedObject = $expected.into();
        if realized != exported {
            let serde_right = serde_json::to_string_pretty(&realized).unwrap();
            let serde_left = serde_json::to_string_pretty(&exported).unwrap();
            panic!(
                "documents didn't match\n expected\n{}\n got\n{}",
                &serde_left, &serde_right
            );
        }
        pretty_assertions::assert_eq!(realized, exported);
    }};
}

/// Like `assert_doc` except that you can specify an object ID and property to select subsections
/// of the document.
#[macro_export]
macro_rules! assert_obj {
    ($doc: expr, $obj_id: expr, $prop: expr, $expected: expr) => {{
        use $crate::helpers::realize_prop;
        let realized = realize_prop($doc, $obj_id, $prop);
        let exported: RealizedObject = $expected.into();
        if realized != exported {
            let serde_right = serde_json::to_string_pretty(&realized).unwrap();
            let serde_left = serde_json::to_string_pretty(&exported).unwrap();
            panic!(
                "documents didn't match\n expected\n{}\n got\n{}",
                &serde_left, &serde_right
            );
        }
        pretty_assertions::assert_eq!(realized, exported);
    }};
}

/// Construct `RealizedObject::Map`. This macro takes a nested set of curl braces. The outer set is
/// the keys of the map, the inner set is the opid tagged values:
///
/// ```
/// map!{
///     "key" => {
///         opid1 => "value1",
///         opid2 => "value2",
///     }
/// }
/// ```
///
/// The map above would represent a map with a conflict on the "key" property. The values can be
/// anything which implements `Into<RealizedObject<ExportableOpId<'_>>`. Including nested calls to
/// `map!` or `list!`.
#[macro_export]
macro_rules! map {
    (@single $($x:tt)*) => (());
    (@count $($rest:expr),*) => (<[()]>::len(&[$(map!(@single $rest)),*]));

    (@inner { $($opid:expr => $value:expr,)+ }) => { map!(@inner { $($opid => $value),+ }) };
    (@inner { $($opid:expr => $value:expr),* }) => {
        {
            use std::collections::HashMap;
            let mut inner: HashMap<ObjId, RealizedObject> = HashMap::new();
            $(
                let _ = inner.insert(ObjId::from((&$opid)).into_owned(), $value.into());
            )*
            inner
        }
    };
    //(&inner $map:expr, $opid:expr => $value:expr, $($tail:tt),*) => {
        //$map.insert($opid.into(), $value.into());
    //}
    ($($key:expr => $inner:tt,)+) => { map!($($key => $inner),+) };
    ($($key:expr => $inner:tt),*) => {
        {
            use std::collections::HashMap;
            let _cap = map!(@count $($key),*);
            let mut _map: HashMap<String, HashMap<ObjId, RealizedObject>> = ::std::collections::HashMap::with_capacity(_cap);
            $(
                let inner = map!(@inner $inner);
                let _ = _map.insert($key.to_string(), inner);
            )*
            RealizedObject::Map(_map)
        }
    }
}

/// Construct `RealizedObject::Sequence`. This macro represents a sequence of opid tagged values
///
/// ```
/// list![
///     {
///         opid1 => "value1",
///         opid2 => "value2",
///     }
/// ]
/// ```
///
/// The list above would represent a list with a conflict on the 0 index. The values can be
/// anything which implements `Into<RealizedObject<ExportableOpId<'_>>` including nested calls to
/// `map!` or `list!`.
#[macro_export]
macro_rules! list {
    (@single $($x:tt)*) => (());
    (@count $($rest:tt),*) => (<[()]>::len(&[$(list!(@single $rest)),*]));

    (@inner { $($opid:expr => $value:expr,)+ }) => { list!(@inner { $($opid => $value),+ }) };
    (@inner { $($opid:expr => $value:expr),* }) => {
        {
            use std::collections::HashMap;
            let mut inner: HashMap<ObjId, RealizedObject> = HashMap::new();
            $(
                let _ = inner.insert(ObjId::from(&$opid).into_owned(), $value.into());
            )*
            inner
        }
    };
    ($($inner:tt,)+) => { list!($($inner),+) };
    ($($inner:tt),*) => {
        {
            let _cap = list!(@count $($inner),*);
            let mut _list: Vec<HashMap<ObjId, RealizedObject>> = Vec::new();
            $(
                //println!("{}", stringify!($inner));
                let inner = list!(@inner $inner);
                let _ = _list.push(inner);
            )*
            RealizedObject::Sequence(_list)
        }
    }
}

pub fn mk_counter(value: i64) -> automerge::ScalarValue {
    automerge::ScalarValue::Counter(value)
}

#[derive(Eq, Hash, PartialEq, Debug)]
pub struct ExportedOpId(String);

impl std::fmt::Display for ExportedOpId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A `RealizedObject` is a representation of all the current values in a document - including
/// conflicts.
#[derive(PartialEq, Debug)]
pub enum RealizedObject<'a> {
    Map(HashMap<String, HashMap<ObjId<'a>, RealizedObject<'a>>>),
    Sequence(Vec<HashMap<ObjId<'a>, RealizedObject<'a>>>),
    Value(automerge::ScalarValue),
}

impl serde::Serialize for RealizedObject<'static> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Map(kvs) => {
                let mut map_ser = serializer.serialize_map(Some(kvs.len()))?;
                for (k, kvs) in kvs {
                    let kvs_serded = kvs
                        .iter()
                        .map(|(opid, value)| (opid.to_string(), value))
                        .collect::<HashMap<String, &RealizedObject>>();
                    map_ser.serialize_entry(k, &kvs_serded)?;
                }
                map_ser.end()
            }
            Self::Sequence(elems) => {
                let mut list_ser = serializer.serialize_seq(Some(elems.len()))?;
                for elem in elems {
                    let kvs_serded = elem
                        .iter()
                        .map(|(opid, value)| (opid.to_string(), value))
                        .collect::<HashMap<String, &RealizedObject>>();
                    list_ser.serialize_element(&kvs_serded)?;
                }
                list_ser.end()
            }
            Self::Value(v) => v.serialize(serializer),
        }
    }
}

pub fn realize<'a>(doc: &automerge::Automerge) -> RealizedObject<'a> {
    realize_obj(doc, ObjId::Root, automerge::ObjType::Map)
}

pub fn realize_prop<P: Into<automerge::Prop>>(
    doc: &automerge::Automerge,
    obj_id: automerge::ObjId,
    prop: P,
) -> RealizedObject<'static> {
    let (val, obj_id) = doc.value(obj_id, prop).unwrap().unwrap();
    match val {
        automerge::Value::Object(obj_type) => realize_obj(doc, obj_id.into(), obj_type),
        automerge::Value::Scalar(v) => RealizedObject::Value(v),
    }
}

pub fn realize_obj(
    doc: &automerge::Automerge,
    obj_id: automerge::ObjId,
    objtype: automerge::ObjType,
) -> RealizedObject<'static> {
    match objtype {
        automerge::ObjType::Map | automerge::ObjType::Table => {
            let mut result = HashMap::new();
            for key in doc.keys(obj_id.clone()) {
                result.insert(key.clone(), realize_values(doc, obj_id.clone(), key));
            }
            RealizedObject::Map(result)
        }
        automerge::ObjType::List | automerge::ObjType::Text => {
            let length = doc.length(obj_id.clone());
            let mut result = Vec::with_capacity(length);
            for i in 0..length {
                result.push(realize_values(doc, obj_id.clone(), i));
            }
            RealizedObject::Sequence(result)
        }
    }
}

fn realize_values<K: Into<automerge::Prop>>(
    doc: &automerge::Automerge,
    obj_id: automerge::ObjId,
    key: K,
) -> HashMap<ObjId<'static>, RealizedObject<'static>> {
    let mut values_by_objid: HashMap<ObjId, RealizedObject> = HashMap::new();
    for (value, opid) in doc.values(obj_id, key).unwrap() {
        let realized = match value {
            automerge::Value::Object(objtype) => realize_obj(doc, opid.clone().into(), objtype),
            automerge::Value::Scalar(v) => RealizedObject::Value(v),
        };
        values_by_objid.insert(opid.into(), realized);
    }
    values_by_objid
}


impl<'a, I: Into<RealizedObject<'a>>>
    From<HashMap<&str, HashMap<ObjId<'a>, I>>> for RealizedObject<'a>
{
    fn from(values: HashMap<&str, HashMap<ObjId<'a>, I>>) -> Self {
        let intoed = values
            .into_iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    v.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
                )
            })
            .collect();
        RealizedObject::Map(intoed)
    }
}

impl<'a, I: Into<RealizedObject<'a>>>
    From<Vec<HashMap<ObjId<'a>, I>>> for RealizedObject<'a>
{
    fn from(values: Vec<HashMap<ObjId<'a>, I>>) -> Self {
        RealizedObject::Sequence(
            values
                .into_iter()
                .map(|v| v.into_iter().map(|(k, v)| (k, v.into())).collect())
                .collect(),
        )
    }
}

impl From<bool> for RealizedObject<'static> {
    fn from(b: bool) -> Self {
        RealizedObject::Value(b.into())
    }
}

impl From<usize> for RealizedObject<'static> {
    fn from(u: usize) -> Self {
        let v = u.try_into().unwrap();
        RealizedObject::Value(automerge::ScalarValue::Int(v))
    }
}

impl From<automerge::ScalarValue> for RealizedObject<'static> {
    fn from(s: automerge::ScalarValue) -> Self {
        RealizedObject::Value(s)
    }
}

impl From<&str> for RealizedObject<'static> {
    fn from(s: &str) -> Self {
        RealizedObject::Value(automerge::ScalarValue::Str(s.into()))
    }
}

/// Pretty print the contents of a document
#[allow(dead_code)]
pub fn pretty_print(doc: &automerge::Automerge) {
    println!("{}", serde_json::to_string_pretty(&realize(doc)).unwrap())
}

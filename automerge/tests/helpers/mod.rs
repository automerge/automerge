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
/// `RealizedObject<ExportableOpId>`.
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
///             op2.translate(&doc2) => "two"
///         }
///     }
/// );
/// ```
///
/// ## Translating OpIds
///
/// One thing you may have noticed in the example above is the `op2.translate(&doc2)` call. What is
/// that doing there? Well, the problem is that automerge OpIDs (in the current API) are specific
/// to a document. Using an opid from one document in a different document will not work. Therefore
/// this module defines an `OpIdExt` trait with a `translate` method on it. This method takes a
/// document and converts the opid into something which knows how to be compared with opids from
/// another document by using the document you pass to `translate`. Again, all you really need to
/// know is that when constructing a document for comparison you should call `translate(fromdoc)`
/// on opids which come from a document other than the one you pass to `assert_doc`.
#[macro_export]
macro_rules! assert_doc {
    ($doc: expr, $expected: expr) => {{
        use $crate::helpers::{realize, ExportableOpId};
        let realized = realize($doc);
        let to_export: RealizedObject<ExportableOpId<'_>> = $expected.into();
        let exported = to_export.export($doc);
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
        use $crate::helpers::{realize_prop, ExportableOpId};
        let realized = realize_prop($doc, $obj_id, $prop);
        let to_export: RealizedObject<ExportableOpId<'_>> = $expected.into();
        let exported = to_export.export($doc);
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
            let mut inner: HashMap<ExportableOpId<'_>, RealizedObject<ExportableOpId<'_>>> = HashMap::new();
            $(
                let _ = inner.insert($opid.into(), $value.into());
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
            use crate::helpers::ExportableOpId;
            let _cap = map!(@count $($key),*);
            let mut _map: HashMap<String, HashMap<ExportableOpId<'_>, RealizedObject<ExportableOpId<'_>>>> = ::std::collections::HashMap::with_capacity(_cap);
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
            let mut inner: HashMap<ExportableOpId<'_>, RealizedObject<ExportableOpId<'_>>> = HashMap::new();
            $(
                let _ = inner.insert($opid.into(), $value.into());
            )*
            inner
        }
    };
    ($($inner:tt,)+) => { list!($($inner),+) };
    ($($inner:tt),*) => {
        {
            use crate::helpers::ExportableOpId;
            let _cap = list!(@count $($inner),*);
            let mut _list: Vec<HashMap<ExportableOpId<'_>, RealizedObject<ExportableOpId<'_>>>> = Vec::new();
            $(
                //println!("{}", stringify!($inner));
                let inner = list!(@inner $inner);
                let _ = _list.push(inner);
            )*
            RealizedObject::Sequence(_list)
        }
    }
}

/// Translate an op ID produced by one document to an op ID which can be understood by
/// another
///
/// The current API of automerge exposes OpIds of the form (u64, usize) where the first component
/// is the counter of an actors lamport timestamp and the second component is the index into an
/// array of actor IDs stored by the document where the opid was generated. Obviously this is not
/// portable between documents as the index of the actor array is unlikely to match between two
/// documents. This function translates between the two representations.
///
/// At some point we will probably change the API to not be document specific but this function
/// allows us to write tests first.
pub fn translate_obj_id(
    from: &automerge::Automerge,
    to: &automerge::Automerge,
    id: automerge::OpId,
) -> automerge::OpId {
    let exported = from.export(id);
    to.import(&exported).unwrap()
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
pub enum RealizedObject<Oid: PartialEq + Eq + Hash> {
    Map(HashMap<String, HashMap<Oid, RealizedObject<Oid>>>),
    Sequence(Vec<HashMap<Oid, RealizedObject<Oid>>>),
    Value(automerge::ScalarValue),
}

impl serde::Serialize for RealizedObject<ExportedOpId> {
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
                        .collect::<HashMap<String, &RealizedObject<ExportedOpId>>>();
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
                        .collect::<HashMap<String, &RealizedObject<ExportedOpId>>>();
                    list_ser.serialize_element(&kvs_serded)?;
                }
                list_ser.end()
            }
            Self::Value(v) => v.serialize(serializer),
        }
    }
}

pub fn realize(doc: &automerge::Automerge) -> RealizedObject<ExportedOpId> {
    realize_obj(doc, automerge::ROOT, automerge::ObjType::Map)
}

pub fn realize_prop<P: Into<automerge::Prop>>(
    doc: &automerge::Automerge,
    obj_id: automerge::OpId,
    prop: P,
) -> RealizedObject<ExportedOpId> {
    let (val, obj_id) = doc.value(obj_id, prop).unwrap().unwrap();
    match val {
        automerge::Value::Object(obj_type) => realize_obj(doc, obj_id, obj_type),
        automerge::Value::Scalar(v) => RealizedObject::Value(v),
    }
}

pub fn realize_obj(
    doc: &automerge::Automerge,
    obj_id: automerge::OpId,
    objtype: automerge::ObjType,
) -> RealizedObject<ExportedOpId> {
    match objtype {
        automerge::ObjType::Map | automerge::ObjType::Table => {
            let mut result = HashMap::new();
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

fn realize_values<K: Into<automerge::Prop>>(
    doc: &automerge::Automerge,
    obj_id: automerge::OpId,
    key: K,
) -> HashMap<ExportedOpId, RealizedObject<ExportedOpId>> {
    let mut values_by_opid = HashMap::new();
    for (value, opid) in doc.values(obj_id, key).unwrap() {
        let realized = match value {
            automerge::Value::Object(objtype) => realize_obj(doc, opid, objtype),
            automerge::Value::Scalar(v) => RealizedObject::Value(v),
        };
        let exported_opid = ExportedOpId(doc.export(opid));
        values_by_opid.insert(exported_opid, realized);
    }
    values_by_opid
}

impl<'a> RealizedObject<ExportableOpId<'a>> {
    pub fn export(self, doc: &automerge::Automerge) -> RealizedObject<ExportedOpId> {
        match self {
            Self::Map(kvs) => RealizedObject::Map(
                kvs.into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            v.into_iter()
                                .map(|(k, v)| (k.export(doc), v.export(doc)))
                                .collect(),
                        )
                    })
                    .collect(),
            ),
            Self::Sequence(values) => RealizedObject::Sequence(
                values
                    .into_iter()
                    .map(|v| {
                        v.into_iter()
                            .map(|(k, v)| (k.export(doc), v.export(doc)))
                            .collect()
                    })
                    .collect(),
            ),
            Self::Value(v) => RealizedObject::Value(v),
        }
    }
}

impl<'a, O: Into<ExportableOpId<'a>>, I: Into<RealizedObject<ExportableOpId<'a>>>>
    From<HashMap<&str, HashMap<O, I>>> for RealizedObject<ExportableOpId<'a>>
{
    fn from(values: HashMap<&str, HashMap<O, I>>) -> Self {
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

impl<'a, O: Into<ExportableOpId<'a>>, I: Into<RealizedObject<ExportableOpId<'a>>>>
    From<Vec<HashMap<O, I>>> for RealizedObject<ExportableOpId<'a>>
{
    fn from(values: Vec<HashMap<O, I>>) -> Self {
        RealizedObject::Sequence(
            values
                .into_iter()
                .map(|v| v.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
                .collect(),
        )
    }
}

impl From<bool> for RealizedObject<ExportableOpId<'_>> {
    fn from(b: bool) -> Self {
        RealizedObject::Value(b.into())
    }
}

impl From<usize> for RealizedObject<ExportableOpId<'_>> {
    fn from(u: usize) -> Self {
        let v = u.try_into().unwrap();
        RealizedObject::Value(automerge::ScalarValue::Int(v))
    }
}

impl From<automerge::ScalarValue> for RealizedObject<ExportableOpId<'_>> {
    fn from(s: automerge::ScalarValue) -> Self {
        RealizedObject::Value(s)
    }
}

impl From<&str> for RealizedObject<ExportableOpId<'_>> {
    fn from(s: &str) -> Self {
        RealizedObject::Value(automerge::ScalarValue::Str(s.into()))
    }
}

#[derive(Eq, PartialEq, Hash)]
pub enum ExportableOpId<'a> {
    Native(automerge::OpId),
    Translate(Translate<'a>),
}

impl<'a> ExportableOpId<'a> {
    fn export(self, doc: &automerge::Automerge) -> ExportedOpId {
        let oid = match self {
            Self::Native(oid) => oid,
            Self::Translate(Translate { from, opid }) => translate_obj_id(from, doc, opid),
        };
        ExportedOpId(doc.export(oid))
    }
}

pub struct Translate<'a> {
    from: &'a automerge::Automerge,
    opid: automerge::OpId,
}

impl<'a> PartialEq for Translate<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.from.maybe_get_actor().unwrap() == other.from.maybe_get_actor().unwrap()
            && self.opid == other.opid
    }
}

impl<'a> Eq for Translate<'a> {}

impl<'a> Hash for Translate<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.from.maybe_get_actor().unwrap().hash(state);
        self.opid.hash(state);
    }
}

pub trait OpIdExt {
    fn native(self) -> ExportableOpId<'static>;
    fn translate(self, doc: &automerge::Automerge) -> ExportableOpId<'_>;
}

impl OpIdExt for automerge::OpId {
    /// Use this opid directly when exporting
    fn native(self) -> ExportableOpId<'static> {
        ExportableOpId::Native(self)
    }

    /// Translate this OpID from `doc` when exporting
    fn translate(self, doc: &automerge::Automerge) -> ExportableOpId<'_> {
        ExportableOpId::Translate(Translate {
            from: doc,
            opid: self,
        })
    }
}

impl From<automerge::OpId> for ExportableOpId<'_> {
    fn from(oid: automerge::OpId) -> Self {
        ExportableOpId::Native(oid)
    }
}

/// Pretty print the contents of a document
#[allow(dead_code)]
pub fn pretty_print(doc: &automerge::Automerge) {
    println!("{}", serde_json::to_string_pretty(&realize(doc)).unwrap())
}

use serde::ser::{SerializeMap, SerializeSeq};

use crate::{ObjId, ObjType, ReadDoc, Value};

/// A wrapper type which implements [`serde::Serialize`] for a [`ReadDoc`].
///
/// # Example
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use automerge::{AutoCommit, AutomergeError, Value, transaction::Transactable};
/// let mut doc = AutoCommit::new();
/// doc.put(automerge::ROOT, "key", "value")?;
///
/// let serialized = serde_json::to_string(&automerge::AutoSerde::from(&doc)).unwrap();
///
/// assert_eq!(serialized, r#"{"key":"value"}"#);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct AutoSerde<'a, R: crate::ReadDoc>(&'a R);

impl<'a, R: ReadDoc> From<&'a R> for AutoSerde<'a, R> {
    fn from(a: &'a R) -> Self {
        AutoSerde(a)
    }
}

impl<'a, R: crate::ReadDoc> serde::Serialize for AutoSerde<'a, R> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        AutoSerdeMap {
            doc: self.0,
            obj: ObjId::Root,
        }
        .serialize(serializer)
    }
}

struct AutoSerdeMap<'a, R> {
    doc: &'a R,
    obj: ObjId,
}

impl<'a, R: crate::ReadDoc> serde::Serialize for AutoSerdeMap<'a, R> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map_ser = serializer.serialize_map(Some(self.doc.length(&ObjId::Root)))?;
        for key in self.doc.keys(&self.obj) {
            // SAFETY: This only errors if the object ID is unknown, but we construct this type
            // with a known real object ID
            let (val, obj) = self.doc.get(&self.obj, &key).unwrap().unwrap();
            let serdeval = AutoSerdeVal {
                doc: self.doc,
                val,
                obj,
            };
            map_ser.serialize_entry(&key, &serdeval)?;
        }
        map_ser.end()
    }
}

struct AutoSerdeSeq<'a, R> {
    doc: &'a R,
    obj: ObjId,
}

impl<'a, R: crate::ReadDoc> serde::Serialize for AutoSerdeSeq<'a, R> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq_ser = serializer.serialize_seq(None)?;
        for i in 0..self.doc.length(&self.obj) {
            // SAFETY: This only errors if the object ID is unknown, but we construct this type
            // with a known real object ID
            let (val, obj) = self.doc.get(&self.obj, i).unwrap().unwrap();
            let serdeval = AutoSerdeVal {
                doc: self.doc,
                val,
                obj,
            };
            seq_ser.serialize_element(&serdeval)?;
        }
        seq_ser.end()
    }
}

struct AutoSerdeVal<'a, R> {
    doc: &'a R,
    val: Value<'a>,
    obj: ObjId,
}

impl<'a, R: crate::ReadDoc> serde::Serialize for AutoSerdeVal<'a, R> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.val {
            Value::Object(ObjType::Map | ObjType::Table) => {
                let map = AutoSerdeMap {
                    doc: self.doc,
                    obj: self.obj.clone(),
                };
                map.serialize(serializer)
            }
            Value::Object(ObjType::Text) => {
                let text = self.doc.text(&self.obj).unwrap();
                text.serialize(serializer)
            }
            Value::Object(ObjType::List) => {
                let seq = AutoSerdeSeq {
                    doc: self.doc,
                    obj: self.obj.clone(),
                };
                seq.serialize(serializer)
            }
            Value::Scalar(v) => v.serialize(serializer),
        }
    }
}

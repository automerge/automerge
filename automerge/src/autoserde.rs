use serde::ser::{SerializeMap, SerializeSeq};

use crate::{Automerge, ObjId, ObjType, Value};

/// A wrapper type which implements [`serde::Serialize`] for an [`Automerge`].
#[derive(Debug)]
pub struct AutoSerde<'a>(&'a Automerge);

impl<'a> From<&'a Automerge> for AutoSerde<'a> {
    fn from(a: &'a Automerge) -> Self {
        AutoSerde(a)
    }
}

impl<'a> serde::Serialize for AutoSerde<'a> {
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

struct AutoSerdeMap<'a> {
    doc: &'a Automerge,
    obj: ObjId,
}

impl<'a> serde::Serialize for AutoSerdeMap<'a> {
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

struct AutoSerdeSeq<'a> {
    doc: &'a Automerge,
    obj: ObjId,
}

impl<'a> serde::Serialize for AutoSerdeSeq<'a> {
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

struct AutoSerdeVal<'a> {
    doc: &'a Automerge,
    val: Value<'a>,
    obj: ObjId,
}

impl<'a> serde::Serialize for AutoSerdeVal<'a> {
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
            Value::Object(ObjType::List | ObjType::Text) => {
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

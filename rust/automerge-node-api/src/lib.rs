use std::str::FromStr;

use napi::bindgen_prelude::*;
use napi::sys::napi_env__;
use napi::JsString;
use napi_derive::napi;

use am::transaction::Transactable;
use am::ReadDoc;
use automerge as am;

#[napi]
pub type Actor = String;

// TODO: Consider breaking the API and making this "ObjId"
#[napi(js_name = "ObjID")]
pub type ObjId = String;

#[napi]
pub type Value<'env> = Unknown<'env>;

#[napi]
pub type ChangeHash = String;

#[napi]
pub type Heads = Vec<ChangeHash>;

pub struct Prop(am::Prop);

impl FromNapiValue for Prop {
    unsafe fn from_napi_value(
        env: *mut napi_env__,
        napi_val: napi::sys::napi_value,
    ) -> napi::Result<Self> {
        let js_value = Unknown::from_napi_value(env, napi_val)?;
        match js_value.get_type()? {
            napi::ValueType::Number => {
                let v = js_value.coerce_to_number()?;
                v.get_uint32().map(|i| Prop(am::Prop::Seq(i as usize)))
            }
            napi::ValueType::String => {
                let v = js_value.coerce_to_string()?;
                v.into_utf8()
                    .and_then(|utf8| utf8.into_owned())
                    .map(am::Prop::Map)
                    .map(Prop)
            }
            _ => Err(error::InvalidProp.into()),
        }
    }
}

impl Into<am::Prop> for Prop {
    fn into(self) -> am::Prop {
        self.0
    }
}

#[derive(Debug)]
pub enum Datatype {
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
    pub(crate) fn is_seq(&self) -> bool {
        matches!(self, Self::List | Self::Text)
    }

    pub(crate) fn is_scalar(&self) -> bool {
        !matches!(self, Self::Map | Self::Table | Self::List | Self::Text)
    }
}

impl FromNapiValue for Datatype {
    unsafe fn from_napi_value(
        env: *mut napi_env__,
        napi_val: napi::sys::napi_value,
    ) -> napi::Result<Self> {
        let datatype = JsString::from_napi_value(env, napi_val)
            .map_err(|_| error::InvalidDatatype::NotString)?;
        match datatype.into_utf8()?.as_str()? {
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
                    Err(error::InvalidDatatype::UnknownNotImplemented.into())
                } else {
                    Err(error::InvalidDatatype::Unknown(d.to_string()).into())
                }
            }
        }
    }
}

mod error {
    use automerge::AutomergeError;

    #[derive(Debug, thiserror::Error)]
    #[error("could not parse Actor ID as a hex string: {0}")]
    pub struct BadActorId(#[from] hex::FromHexError);

    impl From<BadActorId> for napi::Error {
        fn from(s: BadActorId) -> Self {
            napi::Error::from_reason(&s.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ImportObj {
        #[error("obj id was not a string")]
        NotString,
        #[error("invalid path {0}: {1}")]
        InvalidPath(String, ImportPath),
        #[error("unable to import object id: {0}")]
        BadImport(AutomergeError),
        // TODO: Uncomment when data handlers are implemented
        // #[error("error calling data handler for type {0}: {1:?}")]
        // CallDataHandler(String, JsValue),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ImportPath {
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error("path component {0} ({1}) should be an integer to index a sequence")]
        IndexNotInteger(usize, String),
        #[error("path component {0} ({1}) referenced a nonexistent object")]
        NonExistentObject(usize, String),
        #[error("path did not refer to an object")]
        NotAnObject,
    }

    impl From<ImportObj> for napi::Error {
        fn from(e: ImportObj) -> Self {
            napi::Error::from_reason(format!("invalid object ID: {}", e))
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Insert {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] ImportObj),
        #[error("the value to insert was not a primitive")]
        ValueNotPrimitive,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        InvalidProp(#[from] InvalidProp),
        // #[error(transparent)]
        // InvalidValue(#[from] InvalidValue),
        #[error(transparent)]
        InvalidDatatype(#[from] InvalidDatatype),
    }

    impl From<Insert> for napi::Error {
        fn from(e: Insert) -> Self {
            // TODO: Consider using RangeError like wasm
            napi::Error::from_reason(format!("invalid object ID: {}", e))
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

    impl From<InvalidDatatype> for napi::Error {
        fn from(e: InvalidDatatype) -> Self {
            napi::Error::from_reason(&e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("given property was not a string or integer")]
    pub struct InvalidProp;

    impl From<InvalidProp> for napi::Error {
        fn from(e: InvalidProp) -> Self {
            napi::Error::from_reason(&e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Get {
        #[error("invalid object ID: {0}")]
        ImportObj(#[from] ImportObj),
        #[error("object not visible")]
        NotVisible,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        // TODO: Implement the following error states too
        #[error("bad heads: {0}")]
        BadHeads(#[from] BadChangeHashes),
        // #[error(transparent)]
        // InvalidProp(#[from] InvalidProp),
        // #[error(transparent)]
        // ExportError(#[from] SetProp),
    }

    impl From<Get> for napi::Error {
        fn from(e: Get) -> Self {
            napi::Error::from_reason(&e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadChangeHashes {
        #[error("the change hashes were not an array of strings")]
        NotArray,
        #[error("could not decode hash {0}: {1}")]
        BadElem(usize, BadChangeHash),
    }

    impl From<BadChangeHashes> for napi::Error {
        fn from(e: BadChangeHashes) -> Self {
            napi::Error::from_reason(&e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadChangeHash {
        #[error("change hash was not a string")]
        NotString,
        #[error(transparent)]
        Parse(#[from] automerge::ParseChangeHashError),
    }

    impl From<BadChangeHash> for napi::Error {
        fn from(e: BadChangeHash) -> Self {
            napi::Error::from_reason(&e.to_string())
        }
    }
}

pub(crate) fn get_heads(
    heads: Option<Heads>,
) -> std::result::Result<Option<Vec<am::ChangeHash>>, error::BadChangeHashes> {
    heads
        .map(|h| {
            h.iter()
                .enumerate()
                .map(|(i, v)| {
                    am::ChangeHash::from_str(v).map_err(|e| {
                        error::BadChangeHashes::BadElem(i, error::BadChangeHash::Parse(e))
                    })
                })
                .collect()
        })
        .transpose()
}

pub(crate) fn alloc<'a>(
    env: &'a Env,
    value: &am::Value<'_>,
    text_rep: am::patches::TextRepresentation,
) -> Result<(Datatype, Unknown<'a>)> {
    match value {
        am::Value::Object(o) => match o {
            am::ObjType::Map => Object::new(&env).into_unknown(&env).and_then(|u| Ok((Datatype::Map, u))),
            am::ObjType::Table => Object::new(&env).into_unknown(&env).and_then(|u| Ok((Datatype::Table, u))),
            // TODO: Consider the use of `vec![] as Vec<Unknown>` here
            am::ObjType::List => Array::from_vec(&env, vec![] as Vec<Unknown>).into_unknown(&env).and_then(|u| Ok((Datatype::List, u))),
            am::ObjType::Text => match text_rep {
                am::patches::TextRepresentation::String => "".into_unknown(&env).and_then(|u| Ok((Datatype::Text, u))),
                // TODO: Consider the use of `vec![] as Vec<Unknown>` here
                am::patches::TextRepresentation::Array => Array::from_vec(env, vec![] as Vec<Unknown>).into_unknown(&env).and_then(|u| Ok((Datatype::Text, u))),
            },
        },
        am::Value::Scalar(s) => alloc_scalar(&env, s.as_ref()),
    }
}

pub(crate) fn alloc_scalar<'a>(
    env: &'a Env,
    value: &am::ScalarValue
) -> Result<(Datatype, Unknown<'a>)> {
    match value {
        // TODO: Test to ensure we don't need to explicitly go through Uint8Array::from
        am::ScalarValue::Bytes(v) =>  v.into_unknown(env).and_then(|u| Ok((Datatype::Bytes, u))),
        am::ScalarValue::Str(v) => v.into_unknown(env).and_then(|u| Ok((Datatype::Str, u))),
        am::ScalarValue::Int(v) => (*v as f64).into_unknown(env).and_then(|u| Ok((Datatype::Int, u))),
        am::ScalarValue::Uint(v) => (*v as f64).into_unknown(env).and_then(|u| Ok((Datatype::Uint, u))),
        am::ScalarValue::F64(v) => v.into_unknown(env).and_then(|u| Ok((Datatype::F64, u))),
        am::ScalarValue::Counter(v) => f64::from(v).into_unknown(env).and_then(|u| Ok((Datatype::Counter, u))),
        am::ScalarValue::Timestamp(v) => todo!("Implement alloc_scalar for Timestamp"), // (Datatype::Timestamp, js_sys::Date::new(&(*v as f64).into()).into())
        am::ScalarValue::Boolean(v) => (*v).into_unknown(env).and_then(|u| Ok((Datatype::Boolean, u))),
        am::ScalarValue::Null => Null.into_unknown(env).and_then(|u| Ok((Datatype::Null, u))),
        // TODO: Test to ensure we don't need to explicitly go through Uint8Array::from(bytes.as_slice())
        am::ScalarValue::Unknown { bytes, type_code } => bytes.into_unknown(env).and_then(|u| Ok((Datatype::Unknown(*type_code), u))),
    }
}

#[napi]
pub struct Automerge {
    doc: am::AutoCommit,
    text_rep: am::patches::TextRepresentation,
}

#[napi]
impl Automerge {
    pub(crate) fn new(
        actor: Option<String>,
        text_rep: am::patches::TextRepresentation,
    ) -> std::result::Result<Automerge, error::BadActorId> {
        let mut doc = am::AutoCommit::default().with_text_rep(text_rep.into());
        if let Some(a) = actor {
            let a = automerge::ActorId::from(hex::decode(a)?.to_vec());
            doc.set_actor(a);
        }
        Ok(Automerge {
            doc,
            // freeze: false,
            // external_types: HashMap::default(),
            text_rep,
        })
    }

    pub(crate) fn import(
        &self,
        id: ObjId,
    ) -> std::result::Result<(am::ObjId, am::ObjType), error::ImportObj> {
        let path = id.to_string();
        // valid formats are
        // 123@aabbcc
        // 123@aabccc/prop1/prop2/prop3
        // /prop1/prop2/prop3
        let mut components = path.split('/');
        let obj = components.next();
        let (id, obj_type) = if obj == Some("") {
            (am::ROOT, am::ObjType::Map)
        } else {
            self.doc
                .import(obj.unwrap_or_default())
                .map_err(error::ImportObj::BadImport)?
        };
        self.import_path(id, obj_type, components)
            .map_err(|e| error::ImportObj::InvalidPath(path, e))
    }

    pub(crate) fn import_path<'a, I: Iterator<Item = &'a str>>(
        &self,
        mut obj: am::ObjId,
        mut obj_type: am::ObjType,
        components: I,
    ) -> std::result::Result<(am::ObjId, am::ObjType), error::ImportPath> {
        for (i, prop) in components.enumerate() {
            if prop.is_empty() {
                break;
            }
            let is_map = matches!(obj_type, am::ObjType::Map | am::ObjType::Table);
            let val = if is_map {
                self.doc.get(obj, prop)?
            } else {
                let idx = prop
                    .parse()
                    .map_err(|_| error::ImportPath::IndexNotInteger(i, prop.to_string()))?;
                self.doc.get(obj, am::Prop::Seq(idx))?
            };
            match val {
                Some((am::Value::Object(am::ObjType::Map), id)) => {
                    obj_type = am::ObjType::Map;
                    obj = id;
                }
                Some((am::Value::Object(am::ObjType::Table), id)) => {
                    obj_type = am::ObjType::Table;
                    obj = id;
                }
                Some((am::Value::Object(am::ObjType::List), id)) => {
                    obj_type = am::ObjType::List;
                    obj = id;
                }
                Some((am::Value::Object(am::ObjType::Text), id)) => {
                    obj_type = am::ObjType::Text;
                    obj = id;
                }
                None => return Err(error::ImportPath::NonExistentObject(i, prop.to_string())),
                _ => return Err(error::ImportPath::NotAnObject),
            };
        }
        Ok((obj, obj_type))
    }

    pub(crate) fn import_scalar(
        &self,
        value: &Unknown,
        datatype: Option<Datatype>,
    ) -> Option<am::ScalarValue> {
        match datatype {
            Some(Datatype::Boolean) => value.coerce_to_bool().ok().map(am::ScalarValue::Boolean),
            Some(Datatype::Int) => value
                .coerce_to_number()
                .and_then(|n| n.get_int64())
                .map(am::ScalarValue::Int)
                .ok(),
            Some(Datatype::Uint) => value
                .coerce_to_number()
                .and_then(|n: napi::JsNumber<'_>| n.get_uint32())
                .map(|v| am::ScalarValue::Uint(v as u64))
                .ok(),
            Some(Datatype::Str) => value
                .coerce_to_string()
                .and_then(|s| s.into_utf8())
                .and_then(|utf8| utf8.as_str().map(|v| am::ScalarValue::Str(v.into())))
                .ok(),
            Some(Datatype::F64) => value
                .coerce_to_number()
                .and_then(|n: napi::JsNumber<'_>| n.get_double())
                .map(am::ScalarValue::F64)
                .ok(),
            Some(Datatype::Bytes) => todo!("Implement import_scalar for bytes"), // Some(am::ScalarValue::Bytes()),
            Some(Datatype::Counter) => value
                .coerce_to_number()
                .and_then(|n: napi::JsNumber<'_>| n.get_double())
                .map(|v| am::ScalarValue::counter(v as i64))
                .ok(),
            Some(Datatype::Timestamp) => {
                if let Ok(v) = value.coerce_to_number() {
                    Some(am::ScalarValue::Timestamp(v.get_int64().ok()?))
                // TODO: Handle Date objects
                //} else if let Ok(d) = value.clone().dyn_into::<js_sys::Date>() {
                //    Some(am::ScalarValue::Timestamp(d.get_time() as i64))
                } else {
                    None
                }
            }
            Some(Datatype::Null) => Some(am::ScalarValue::Null),
            Some(_) => None,
            None => match value.get_type().ok()? {
                napi::ValueType::Null => Some(am::ScalarValue::Null),
                napi::ValueType::Boolean => {
                    let v = value.coerce_to_bool().ok()?;
                    Some(am::ScalarValue::Boolean(v))
                }
                napi::ValueType::String => {
                    let v = value.coerce_to_string().and_then(|s| s.into_utf8()).ok()?;
                    Some(am::ScalarValue::Str(v.as_str().ok()?.into()))
                }
                napi::ValueType::Number => {
                    let n = value.coerce_to_number().and_then(|n| n.get_double()).ok()?;
                    if (n.round() - n).abs() < f64::EPSILON {
                        Some(am::ScalarValue::Int(n as i64))
                    } else {
                        Some(am::ScalarValue::F64(n))
                    }
                }
                // TODO: Handle Date objects
                // Some(am::ScalarValue::Timestamp(d.get_time() as i64))
                // TODO: Handle Uint8Array
                // Some(am::ScalarValue::Bytes(o.to_vec()))
                _ => None,
            },
        }
    }

    // put(obj: ObjId, prop: Prop, value: Value, datatype?: Datatype): void;

    #[napi]
    pub fn put(
        &mut self,
        obj: ObjId,
        prop: Prop,
        // TODO: The WASM binding declares taking "object" here as well, but doesn't seem to implement support for it
        #[napi(ts_arg_type = "string | number | boolean | null | Date | Uint8Array")] value: Value,
        datatype: Option<Datatype>,
    ) -> Result<()> {
        let (obj, _) = self.import(obj)?;
        let value = self
            .import_scalar(&value, datatype)
            .ok_or(error::Insert::ValueNotPrimitive)?;
        self.doc
            .put(&obj, prop, value)
            .map_err(error::Insert::Automerge)?;
        Ok(())
    }

    // TODO: Wasm binding was returning undefined on invalid props
    #[napi]
    pub fn get<'a>(
        &mut self,
        env: &'a Env,
        obj: ObjId,
        prop: Prop,
        heads: Option<Heads>,
    ) -> Result<Unknown<'a>> {
        let (obj, _) = self.import(obj)?;
        let heads = get_heads(heads).map_err(error::Get::BadHeads)?;
        let value = if let Some(h) = heads {
            self.doc
                .get_at(&obj, prop, &h)
                .map_err(error::Get::Automerge)?
        } else {
            self.doc.get(&obj, prop).map_err(error::Get::Automerge)?
        };
        if let Some((value, id)) = value {
            match alloc(&env, &value, self.text_rep) {
                Ok((datatype, js_value)) if datatype.is_scalar() => Ok(js_value),
                _ => todo!("Implement conversion of object IDs to Unknown"), // Ok(id.to_string().into_unknown(&env)?)
            }
        } else {
            ().into_unknown(&env)
        }
    }
}

#[derive(Debug, Default)]
#[napi(object)]
pub struct InitOptions {
    pub actor: Option<Actor>,
    #[napi(js_name = "text_v1")]
    pub text_v1: Option<bool>,
}

#[napi(js_name = "create")]
pub fn init(options: Option<InitOptions>) -> napi::Result<Automerge> {
    let options = options.unwrap_or_default();
    let text_v1 = options.text_v1.unwrap_or(false);
    let text_rep = if text_v1 {
        am::patches::TextRepresentation::Array
    } else {
        am::patches::TextRepresentation::String
    };
    Automerge::new(options.actor, text_rep).map_err(|e| e.into())
}

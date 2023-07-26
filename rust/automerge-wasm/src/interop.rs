use crate::error::InsertObject;
use crate::value::Datatype;
use crate::{Automerge, TextRepresentation};
use automerge as am;
use automerge::ReadDoc;
use automerge::ROOT;
use automerge::{Change, ChangeHash, ObjType, Prop};
use js_sys::{Array, Function, JsString, Object, Reflect, Symbol, Uint8Array};
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::fmt::Display;
use std::ops::Deref;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use am::{marks::ExpandMark, ObjId, Patch, PatchAction, Value};

const RAW_DATA_SYMBOL: &str = "_am_raw_value_";
const DATATYPE_SYMBOL: &str = "_am_datatype_";
const RAW_OBJECT_SYMBOL: &str = "_am_objectId";
const META_SYMBOL: &str = "_am_meta";

pub(crate) struct JS(pub(crate) JsValue);
pub(crate) struct AR(pub(crate) Array);

impl Deref for JS {
    type Target = JsValue;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<AR> for JsValue {
    fn from(ar: AR) -> Self {
        ar.0.into()
    }
}

impl From<AR> for Array {
    fn from(ar: AR) -> Self {
        ar.0
    }
}

impl From<JS> for JsValue {
    fn from(js: JS) -> Self {
        js.0
    }
}

impl From<am::sync::State> for JS {
    fn from(state: am::sync::State) -> Self {
        let shared_heads: JS = state.shared_heads.into();
        let last_sent_heads: JS = state.last_sent_heads.into();
        let their_heads: JS = state.their_heads.into();
        let their_need: JS = state.their_need.into();
        let sent_hashes: JS = state.sent_hashes.into();
        let their_have = if let Some(have) = &state.their_have {
            JsValue::from(AR::from(have.as_slice()).0)
        } else {
            JsValue::null()
        };
        let result: JsValue = Object::new().into();
        // we can unwrap here b/c we made the object and know its not frozen
        Reflect::set(&result, &"sharedHeads".into(), &shared_heads.0).unwrap();
        Reflect::set(&result, &"lastSentHeads".into(), &last_sent_heads.0).unwrap();
        Reflect::set(&result, &"theirHeads".into(), &their_heads.0).unwrap();
        Reflect::set(&result, &"theirNeed".into(), &their_need.0).unwrap();
        Reflect::set(&result, &"theirHave".into(), &their_have).unwrap();
        Reflect::set(&result, &"sentHashes".into(), &sent_hashes.0).unwrap();
        Reflect::set(&result, &"inFlight".into(), &state.in_flight.into()).unwrap();
        JS(result)
    }
}

impl From<Vec<ChangeHash>> for JS {
    fn from(heads: Vec<ChangeHash>) -> Self {
        JS(heads
            .iter()
            .map(|h| JsValue::from_str(&h.to_string()))
            .collect::<Array>()
            .into())
    }
}

impl From<HashSet<ChangeHash>> for JS {
    fn from(heads: HashSet<ChangeHash>) -> Self {
        let result: JsValue = Object::new().into();
        for key in &heads {
            Reflect::set(&result, &key.to_string().into(), &true.into()).unwrap();
        }
        JS(result)
    }
}

impl From<BTreeSet<ChangeHash>> for JS {
    fn from(heads: BTreeSet<ChangeHash>) -> Self {
        let result: JsValue = Object::new().into();
        for key in &heads {
            Reflect::set(&result, &key.to_string().into(), &true.into()).unwrap();
        }
        JS(result)
    }
}

impl From<Option<Vec<ChangeHash>>> for JS {
    fn from(heads: Option<Vec<ChangeHash>>) -> Self {
        if let Some(v) = heads {
            let v: Array = v
                .iter()
                .map(|h| JsValue::from_str(&h.to_string()))
                .collect();
            JS(v.into())
        } else {
            JS(JsValue::null())
        }
    }
}

impl TryFrom<JS> for usize {
    type Error = error::BadNumber;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        value.as_f64().map(|n| n as usize).ok_or(error::BadNumber)
    }
}

impl TryFrom<JS> for ExpandMark {
    type Error = error::BadExpand;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.is_undefined() {
            Ok(ExpandMark::default())
        } else {
            value
                .as_string()
                .and_then(|s| match s.as_str() {
                    "before" => Some(ExpandMark::Before),
                    "after" => Some(ExpandMark::After),
                    "both" => Some(ExpandMark::Both),
                    "none" => Some(ExpandMark::None),
                    _ => None,
                })
                .ok_or(error::BadExpand)
        }
    }
}

impl TryFrom<JS> for HashSet<ChangeHash> {
    type Error = error::BadChangeHashSet;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let result = HashSet::new();
        fold_hash_set(result, &value.0, |mut set, hash| {
            set.insert(hash);
            set
        })
    }
}

impl TryFrom<JS> for BTreeSet<ChangeHash> {
    type Error = error::BadChangeHashSet;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let result = BTreeSet::new();
        fold_hash_set(result, &value.0, |mut set, hash| {
            set.insert(hash);
            set
        })
    }
}

fn fold_hash_set<F, O>(init: O, val: &JsValue, f: F) -> Result<O, error::BadChangeHashSet>
where
    F: Fn(O, ChangeHash) -> O,
{
    let mut result = init;
    for key in Reflect::own_keys(val)
        .map_err(|_| error::BadChangeHashSet::ListProp)?
        .iter()
    {
        if let Some(true) = js_get(val, &key)?.0.as_bool() {
            let hash = ChangeHash::try_from(JS(key.clone()))
                .map_err(|e| error::BadChangeHashSet::BadHash(key, e))?;
            result = f(result, hash);
        }
    }
    Ok(result)
}

impl TryFrom<JS> for ChangeHash {
    type Error = error::BadChangeHash;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if let Some(s) = value.0.as_string() {
            Ok(s.parse()?)
        } else {
            Err(error::BadChangeHash::NotString)
        }
    }
}

impl TryFrom<JS> for Option<Vec<ChangeHash>> {
    type Error = error::BadChangeHashes;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.0.is_null() {
            Ok(None)
        } else {
            Vec::<ChangeHash>::try_from(value).map(Some)
        }
    }
}

impl TryFrom<JS> for Vec<ChangeHash> {
    type Error = error::BadChangeHashes;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value
            .0
            .dyn_into::<Array>()
            .map_err(|_| error::BadChangeHashes::NotArray)?;
        let value = value
            .iter()
            .enumerate()
            .map(|(i, v)| {
                ChangeHash::try_from(JS(v)).map_err(|e| error::BadChangeHashes::BadElem(i, e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(value)
    }
}

impl TryFrom<JS> for Vec<Change> {
    type Error = error::BadJSChanges;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value
            .0
            .dyn_into::<Array>()
            .map_err(|_| error::BadJSChanges::ChangesNotArray)?;
        let changes = value
            .iter()
            .enumerate()
            .map(|(i, j)| {
                j.dyn_into().map_err::<error::BadJSChanges, _>(|_| {
                    error::BadJSChanges::ElemNotUint8Array(i)
                })
            })
            .collect::<Result<Vec<Uint8Array>, _>>()?;
        let changes = changes
            .iter()
            .enumerate()
            .map(|(i, arr)| {
                automerge::Change::try_from(arr.to_vec().as_slice())
                    .map_err(|e| error::BadJSChanges::BadChange(i, e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(changes)
    }
}

impl TryFrom<JS> for am::sync::State {
    type Error = error::BadSyncState;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value.0;
        let shared_heads = js_get(&value, "sharedHeads")?
            .try_into()
            .map_err(error::BadSyncState::BadSharedHeads)?;
        let last_sent_heads = js_get(&value, "lastSentHeads")?
            .try_into()
            .map_err(error::BadSyncState::BadLastSentHeads)?;
        let their_heads = js_get(&value, "theirHeads")?
            .try_into()
            .map_err(error::BadSyncState::BadTheirHeads)?;
        let their_need = js_get(&value, "theirNeed")?
            .try_into()
            .map_err(error::BadSyncState::BadTheirNeed)?;
        let their_have = js_get(&value, "theirHave")?
            .try_into()
            .map_err(error::BadSyncState::BadTheirHave)?;
        let sent_hashes = js_get(&value, "sentHashes")?
            .try_into()
            .map_err(error::BadSyncState::BadSentHashes)?;
        let in_flight = js_get(&value, "inFlight")?
            .0
            .as_bool()
            .ok_or(error::BadSyncState::InFlightNotBoolean)?;
        Ok(am::sync::State {
            shared_heads,
            last_sent_heads,
            their_heads,
            their_need,
            their_have,
            sent_hashes,
            in_flight,
        })
    }
}

impl TryFrom<JS> for am::sync::Have {
    type Error = error::BadHave;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let last_sync = js_get(&value.0, "lastSync")?
            .try_into()
            .map_err(error::BadHave::BadLastSync)?;
        let bloom = js_get(&value.0, "bloom")?
            .try_into()
            .map_err(error::BadHave::BadBloom)?;
        Ok(am::sync::Have { last_sync, bloom })
    }
}

impl TryFrom<JS> for Option<Vec<am::sync::Have>> {
    type Error = error::BadHaves;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.0.is_null() {
            Ok(None)
        } else {
            Ok(Some(value.try_into()?))
        }
    }
}

impl TryFrom<JS> for Vec<am::sync::Have> {
    type Error = error::BadHaves;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value
            .0
            .dyn_into::<Array>()
            .map_err(|_| error::BadHaves::NotArray)?;
        let have = value
            .iter()
            .enumerate()
            .map(|(i, s)| JS(s).try_into().map_err(|e| error::BadHaves::BadElem(i, e)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(have)
    }
}

impl TryFrom<JS> for am::sync::BloomFilter {
    type Error = error::BadBloom;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value: Uint8Array = value
            .0
            .dyn_into()
            .map_err(|_| error::BadBloom::NotU8Array)?;
        let value = value.to_vec();
        let value = value.as_slice().try_into()?;
        Ok(value)
    }
}

impl TryFrom<JS> for am::sync::Message {
    type Error = error::BadSyncMessage;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let heads = js_get(&value.0, "heads")?
            .try_into()
            .map_err(error::BadSyncMessage::BadHeads)?;
        let need = js_get(&value.0, "need")?
            .try_into()
            .map_err(error::BadSyncMessage::BadNeed)?;
        let changes = js_get(&value.0, "changes")?.try_into()?;
        let have = js_get(&value.0, "have")?.try_into()?;
        Ok(am::sync::Message {
            heads,
            need,
            have,
            changes,
        })
    }
}

impl From<Vec<ChangeHash>> for AR {
    fn from(values: Vec<ChangeHash>) -> Self {
        AR(values
            .iter()
            .map(|h| JsValue::from_str(&h.to_string()))
            .collect())
    }
}

impl From<&[ChangeHash]> for AR {
    fn from(value: &[ChangeHash]) -> Self {
        AR(value
            .iter()
            .map(|h| JsValue::from_str(&h.to_string()))
            .collect())
    }
}

impl From<&[Change]> for AR {
    fn from(value: &[Change]) -> Self {
        let changes: Array = value
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        AR(changes)
    }
}

impl From<&[am::sync::Have]> for AR {
    fn from(value: &[am::sync::Have]) -> Self {
        AR(value
            .iter()
            .map(|have| {
                let last_sync: Array = have
                    .last_sync
                    .iter()
                    .map(|h| JsValue::from_str(&hex::encode(h.0)))
                    .collect();
                let bloom = Uint8Array::from(have.bloom.to_bytes().as_slice());
                let obj: JsValue = Object::new().into();
                // we can unwrap here b/c we created the object and know its not frozen
                Reflect::set(&obj, &"lastSync".into(), &last_sync.into()).unwrap();
                Reflect::set(&obj, &"bloom".into(), &bloom.into()).unwrap();
                obj
            })
            .collect())
    }
}

pub(crate) fn to_js_err<T: Display>(err: T) -> JsValue {
    js_sys::Error::new(&std::format!("{}", err)).into()
}

pub(crate) fn js_get<J: Into<JsValue>, S: std::fmt::Debug + Into<JsValue>>(
    obj: J,
    prop: S,
) -> Result<JS, error::GetProp> {
    let prop = prop.into();
    Ok(JS(Reflect::get(&obj.into(), &prop).map_err(|e| {
        error::GetProp {
            property: format!("{:?}", prop),
            error: e,
        }
    })?))
}

pub(crate) fn js_set<V: Into<JsValue>, S: std::fmt::Debug + Into<JsValue>>(
    obj: &JsValue,
    prop: S,
    val: V,
) -> Result<bool, error::SetProp> {
    let prop = prop.into();
    Reflect::set(obj, &prop, &val.into()).map_err(|e| error::SetProp {
        property: prop,
        error: e,
    })
}

pub(crate) fn js_get_symbol<J: Into<JsValue>>(obj: J, prop: &Symbol) -> Result<JS, error::GetProp> {
    Ok(JS(Reflect::get(&obj.into(), &prop.into()).map_err(
        |e| error::GetProp {
            property: format!("{}", prop.to_string()),
            error: e,
        },
    )?))
}

pub(crate) fn to_prop(p: JsValue) -> Result<Prop, error::InvalidProp> {
    if let Some(s) = p.as_string() {
        Ok(Prop::Map(s))
    } else if let Some(n) = p.as_f64() {
        Ok(Prop::Seq(n as usize))
    } else {
        Err(error::InvalidProp)
    }
}

pub(crate) enum JsObjType {
    Text(String),
    Map(Vec<(Prop, JsValue)>),
    List(Vec<(Prop, JsValue)>),
}

impl JsObjType {
    pub(crate) fn objtype(&self) -> ObjType {
        match self {
            Self::Text(_) => ObjType::Text,
            Self::Map(_) => ObjType::Map,
            Self::List(_) => ObjType::List,
        }
    }

    pub(crate) fn text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s.as_ref()),
            Self::Map(_) => None,
            Self::List(_) => None,
        }
    }

    pub(crate) fn subvals(&self) -> impl Iterator<Item = (Cow<'_, Prop>, JsValue)> + '_ + Clone {
        match self {
            Self::Text(s) => SubValIter::Str(s.chars().enumerate()),
            Self::Map(sub) => SubValIter::Slice(sub.as_slice().iter()),
            Self::List(sub) => SubValIter::Slice(sub.as_slice().iter()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SubValIter<'a> {
    Slice(std::slice::Iter<'a, (Prop, JsValue)>),
    Str(std::iter::Enumerate<std::str::Chars<'a>>),
}

impl<'a> Iterator for SubValIter<'a> {
    type Item = (std::borrow::Cow<'a, Prop>, JsValue);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Slice(i) => i
                .next()
                .map(|(p, v)| (std::borrow::Cow::Borrowed(p), v.clone())),
            Self::Str(i) => i
                .next()
                .map(|(n, c)| (std::borrow::Cow::Owned(Prop::Seq(n)), c.to_string().into())),
        }
    }
}

pub(crate) fn import_obj(
    value: &JsValue,
    datatype: Option<&str>,
) -> Result<JsObjType, InsertObject> {
    match datatype {
        Some("map") => {
            let map = value
                .clone()
                .dyn_into::<js_sys::Object>()
                .map_err(|_| InsertObject::ValueNotObject)?;
            let map = js_sys::Object::keys(&map)
                .iter()
                .zip(js_sys::Object::values(&map).iter())
                .map(|(key, val)| (key.as_string().unwrap().into(), val))
                .collect();
            Ok(JsObjType::Map(map))
        }
        Some("list") => {
            let list = value
                .clone()
                .dyn_into::<js_sys::Array>()
                .map_err(|_| InsertObject::ValueNotObject)?;
            let list = list
                .iter()
                .enumerate()
                .map(|(i, e)| (i.into(), e))
                .collect();
            Ok(JsObjType::List(list))
        }
        Some("text") => {
            let text = value.as_string().ok_or(InsertObject::ValueNotObject)?;
            Ok(JsObjType::Text(text))
        }
        Some(_) => Err(InsertObject::ValueNotObject),
        None => {
            if let Ok(list) = value.clone().dyn_into::<js_sys::Array>() {
                let list = list
                    .iter()
                    .enumerate()
                    .map(|(i, e)| (i.into(), e))
                    .collect();
                Ok(JsObjType::List(list))
            } else if let Ok(map) = value.clone().dyn_into::<js_sys::Object>() {
                let map = js_sys::Object::keys(&map)
                    .iter()
                    .zip(js_sys::Object::values(&map).iter())
                    .map(|(key, val)| (key.as_string().unwrap().into(), val))
                    .collect();
                Ok(JsObjType::Map(map))
            } else if let Some(s) = value.as_string() {
                Ok(JsObjType::Text(s))
            } else {
                Err(InsertObject::ValueNotObject)
            }
        }
    }
}

pub(crate) fn get_heads(
    heads: Option<Array>,
) -> Result<Option<Vec<ChangeHash>>, error::BadChangeHashes> {
    heads
        .map(|h| {
            h.iter()
                .enumerate()
                .map(|(i, v)| {
                    ChangeHash::try_from(JS(v)).map_err(|e| error::BadChangeHashes::BadElem(i, e))
                })
                .collect()
        })
        .transpose()
}

pub(crate) fn export_object(
    doc: &Automerge,
    obj: &ObjId,
    datatype: Datatype,
    heads: Option<&[ChangeHash]>,
    meta: &JsValue,
) -> Result<JsValue, error::Export> {
    doc.export_object(obj, datatype, heads, meta)
}

pub(crate) fn import(
    doc: &Automerge,
    id: JsValue,
) -> Result<(ObjId, am::ObjType), error::ImportObj> {
    doc.import(id)
}

pub(crate) fn import_prop(doc: &Automerge, prop: JsValue) -> Result<Prop, error::InvalidProp> {
    doc.import_prop(prop)
}

pub(crate) fn import_scalar(
    doc: &Automerge,
    value: &JsValue,
    datatype: &Option<String>,
) -> Option<am::ScalarValue> {
    doc.import_scalar(value, datatype)
}
pub(crate) fn import_value(
    doc: &Automerge,
    value: &JsValue,
    datatype: Option<String>,
) -> Result<(Value<'static>, Vec<(Prop, JsValue)>), error::InvalidValue> {
    doc.import_value(value, datatype)
}
pub(crate) fn unwrap_object(
    doc: &Automerge,
    ext_val: &Object,
) -> Result<(Object, Datatype, ObjId), error::Export> {
    doc.unwrap_object(ext_val)
}

pub(crate) fn wrap_object(
    doc: &Automerge,
    value: Object,
    datatype: Datatype,
    id: &ObjId,
    meta: &JsValue,
) -> Result<Object, error::Export> {
    doc.wrap_object(value, datatype, id, meta)
}

impl Automerge {
    fn export_object(
        &self,
        obj: &ObjId,
        datatype: Datatype,
        heads: Option<&[ChangeHash]>,
        meta: &JsValue,
    ) -> Result<JsValue, error::Export> {
        let result = match datatype {
            Datatype::Text => match self.text_rep {
                TextRepresentation::String => {
                    if let Some(heads) = heads {
                        self.doc.text_at(obj, heads)?.into()
                    } else {
                        self.doc.text(obj)?.into()
                    }
                }
                TextRepresentation::Array => self
                    .wrap_object(self.export_list(obj, heads, meta)?, datatype, obj, meta)?
                    .into(),
            },
            Datatype::List => self
                .wrap_object(self.export_list(obj, heads, meta)?, datatype, obj, meta)?
                .into(),
            _ => self
                .wrap_object(self.export_map(obj, heads, meta)?, datatype, obj, meta)?
                .into(),
        };
        Ok(result)
    }

    fn export_map(
        &self,
        obj: &ObjId,
        heads: Option<&[ChangeHash]>,
        meta: &JsValue,
    ) -> Result<Object, error::Export> {
        let map = Object::new();
        let items = if let Some(heads) = heads {
            self.doc.map_range_at(obj, .., heads)
        } else {
            self.doc.map_range(obj, ..)
        };
        for item in items {
            let subval = match item.value {
                Value::Object(o) => self.export_object(&item.id, o.into(), heads, meta)?,
                Value::Scalar(_) => self.export_value(alloc(&item.value, self.text_rep))?,
            };
            js_set(&map, item.key, &subval)?;
        }
        Ok(map)
    }

    fn export_list(
        &self,
        obj: &ObjId,
        heads: Option<&[ChangeHash]>,
        meta: &JsValue,
    ) -> Result<Object, error::Export> {
        if let Some(heads) = heads {
            self.doc.list_range_at(obj, .., heads)
        } else {
            self.doc.list_range(obj, ..)
        }
        .map(|item| match &item.value {
            Value::Object(o) => self.export_object(&item.id, o.into(), heads, meta),
            Value::Scalar(_) => self.export_value(alloc(&item.value, self.text_rep)),
        })
        .collect::<Result<Array, _>>()
        .map(|array| array.into())
    }

    fn export_value(
        &self,
        (datatype, raw_value): (Datatype, JsValue),
    ) -> Result<JsValue, error::Export> {
        if let Some(function) = self.external_types.get(&datatype) {
            let wrapped_value = function
                .call1(&JsValue::undefined(), &raw_value)
                .map_err(|e| error::Export::CallDataHandler(datatype.to_string(), e))?;
            if let Ok(o) = wrapped_value.dyn_into::<Object>() {
                let key = Symbol::for_(RAW_DATA_SYMBOL);
                set_hidden_value(&o, &key, &raw_value)?;
                let key = Symbol::for_(DATATYPE_SYMBOL);
                set_hidden_value(&o, &key, datatype)?;
                Ok(o.into())
            } else {
                Err(error::Export::InvalidDataHandler(datatype.to_string()))
            }
        } else {
            Ok(raw_value)
        }
    }

    fn unwrap_object(&self, ext_val: &Object) -> Result<(Object, Datatype, ObjId), error::Export> {
        let inner = js_get_symbol(ext_val, &Symbol::for_(RAW_DATA_SYMBOL))?.0;

        let datatype = js_get_symbol(ext_val, &Symbol::for_(DATATYPE_SYMBOL))?
            .0
            .try_into();

        let id_val = js_get_symbol(ext_val, &Symbol::for_(RAW_OBJECT_SYMBOL))?.0;
        let id = if id_val.is_undefined() {
            am::ROOT
        } else {
            self.doc.import(&id_val.as_string().unwrap_or_default())?.0
        };

        let inner = inner
            .dyn_into::<Object>()
            .unwrap_or_else(|_| ext_val.clone());
        let datatype = datatype.unwrap_or_else(|_| {
            if Array::is_array(&inner) {
                Datatype::List
            } else {
                Datatype::Map
            }
        });
        Ok((inner, datatype, id))
    }

    fn unwrap_scalar(&self, ext_val: JsValue) -> Result<JsValue, error::Export> {
        let inner = js_get_symbol(&ext_val, &Symbol::for_(RAW_DATA_SYMBOL))?.0;
        if !inner.is_undefined() {
            Ok(inner)
        } else {
            Ok(ext_val)
        }
    }

    fn maybe_wrap_object(
        &self,
        (datatype, raw_value): (Datatype, JsValue),
        id: &ObjId,
        meta: &JsValue,
    ) -> Result<JsValue, error::Export> {
        if let Ok(obj) = raw_value.clone().dyn_into::<Object>() {
            let result = self.wrap_object(obj, datatype, id, meta)?;
            Ok(result.into())
        } else {
            self.export_value((datatype, raw_value))
        }
    }

    fn wrap_object(
        &self,
        value: Object,
        datatype: Datatype,
        id: &ObjId,
        meta: &JsValue,
    ) -> Result<Object, error::Export> {
        let value = if let Some(function) = self.external_types.get(&datatype) {
            let wrapped_value = function
                .call1(&JsValue::undefined(), &value)
                .map_err(|e| error::Export::CallDataHandler(datatype.to_string(), e))?;
            let wrapped_object = wrapped_value
                .dyn_into::<Object>()
                .map_err(|_| error::Export::InvalidDataHandler(datatype.to_string()))?;
            set_hidden_value(&wrapped_object, &Symbol::for_(RAW_DATA_SYMBOL), value)?;
            wrapped_object
        } else {
            value
        };
        if matches!(datatype, Datatype::Map | Datatype::List)
            || (datatype == Datatype::Text && self.text_rep == TextRepresentation::Array)
        {
            set_hidden_value(
                &value,
                &Symbol::for_(RAW_OBJECT_SYMBOL),
                &JsValue::from(&id.to_string()),
            )?;
        }
        set_hidden_value(&value, &Symbol::for_(DATATYPE_SYMBOL), datatype)?;
        set_hidden_value(&value, &Symbol::for_(META_SYMBOL), meta)?;
        if self.freeze {
            Object::freeze(&value);
        }
        Ok(value)
    }

    fn apply_patch_to_array(
        &self,
        array: &Object,
        patch: &Patch,
        meta: &JsValue,
    ) -> Result<Object, error::ApplyPatch> {
        let result = Array::from(array); // shallow copy
        match &patch.action {
            PatchAction::PutSeq { index, value, .. } => {
                let sub_val =
                    self.maybe_wrap_object(alloc(&value.0, self.text_rep), &value.1, meta)?;
                js_set(&result, *index as f64, &sub_val)?;
                Ok(result.into())
            }
            PatchAction::DeleteSeq { index, length, .. } => {
                Ok(self.sub_splice(result, *index, *length, vec![], meta)?)
            }
            PatchAction::Insert { index, values, .. } => {
                Ok(self.sub_splice(result, *index, 0, values, meta)?)
            }
            PatchAction::Increment { prop, value, .. } => {
                if let Prop::Seq(index) = prop {
                    let index = *index as f64;
                    let old_val = js_get(&result, index)?.0;
                    let old_val = self.unwrap_scalar(old_val)?;
                    if let Some(old) = old_val.as_f64() {
                        let new_value: Value<'_> =
                            am::ScalarValue::counter(old as i64 + *value).into();
                        js_set(
                            &result,
                            index,
                            &self.export_value(alloc(&new_value, self.text_rep))?,
                        )?;
                        Ok(result.into())
                    } else {
                        Err(error::ApplyPatch::IncrementNonNumeric)
                    }
                } else {
                    Err(error::ApplyPatch::IncrementKeyInSeq)
                }
            }
            PatchAction::DeleteMap { .. } => Err(error::ApplyPatch::DeleteKeyFromSeq),
            PatchAction::PutMap { .. } => Err(error::ApplyPatch::PutKeyInSeq),
            PatchAction::SpliceText { index, value, .. } => {
                match self.text_rep {
                    TextRepresentation::String => Err(error::ApplyPatch::SpliceTextInSeq),
                    TextRepresentation::Array => {
                        let val = String::from(value);
                        let elems = val
                            .chars()
                            .map(|c| {
                                (
                                    Value::Scalar(std::borrow::Cow::Owned(am::ScalarValue::Str(
                                        c.to_string().into(),
                                    ))),
                                    ObjId::Root, // Using ROOT is okay because this ID is never used as
                                    // we're producing ScalarValue::Str
                                    false,
                                )
                            })
                            .collect::<Vec<_>>();
                        Ok(self.sub_splice(result, *index, 0, &elems, meta)?)
                    }
                }
            }
            PatchAction::Mark { .. } => Ok(result.into()),
            PatchAction::Conflict { .. } => Ok(result.into()),
            PatchAction::SplitBlock { .. } => Ok(result.into()),
            PatchAction::JoinBlock { .. } => Ok(result.into()),
            PatchAction::UpdateBlock { .. } => Ok(result.into()),
        }
    }

    fn apply_patch_to_map(
        &self,
        map: &Object,
        patch: &Patch,
        meta: &JsValue,
    ) -> Result<Object, error::ApplyPatch> {
        let result = Object::assign(&Object::new(), map); // shallow copy
        match &patch.action {
            PatchAction::PutMap { key, value, .. } => {
                let sub_val =
                    self.maybe_wrap_object(alloc(&value.0, self.text_rep), &value.1, meta)?;
                js_set(&result, key, &sub_val)?;
                Ok(result)
            }
            PatchAction::DeleteMap { key, .. } => {
                Reflect::delete_property(&result, &key.into()).map_err(|e| {
                    error::Export::Delete {
                        prop: key.to_string(),
                        err: e,
                    }
                })?;
                Ok(result)
            }
            PatchAction::Increment { prop, value, .. } => {
                if let Prop::Map(key) = prop {
                    let old_val = js_get(&result, key)?.0;
                    let old_val = self.unwrap_scalar(old_val)?;
                    if let Some(old) = old_val.as_f64() {
                        let new_value: Value<'_> =
                            am::ScalarValue::counter(old as i64 + *value).into();
                        js_set(
                            &result,
                            key,
                            &self.export_value(alloc(&new_value, self.text_rep))?,
                        )?;
                        Ok(result)
                    } else {
                        Err(error::ApplyPatch::IncrementNonNumeric)
                    }
                } else {
                    Err(error::ApplyPatch::IncrementIndexInMap)
                }
            }
            PatchAction::Conflict { .. } => Ok(result),
            PatchAction::Insert { .. } => Err(error::ApplyPatch::InsertInMap),
            PatchAction::DeleteSeq { .. } => Err(error::ApplyPatch::SpliceInMap),
            PatchAction::SpliceText { .. } => Err(error::ApplyPatch::SpliceTextInMap),
            PatchAction::PutSeq { .. } => Err(error::ApplyPatch::PutIdxInMap),
            PatchAction::Mark { .. } => Err(error::ApplyPatch::MarkInMap),
            PatchAction::SplitBlock { .. } => Err(error::ApplyPatch::BlockInMap),
            PatchAction::JoinBlock { .. } => Err(error::ApplyPatch::BlockInMap),
            PatchAction::UpdateBlock { .. } => Err(error::ApplyPatch::BlockInMap),
        }
    }

    pub(crate) fn apply_patch(
        &self,
        obj: Object,
        patch: &Patch,
        depth: usize,
        meta: &JsValue,
    ) -> Result<Object, error::ApplyPatch> {
        let (inner, datatype, id) = self.unwrap_object(&obj)?;
        let prop = patch.path.get(depth).map(|p| prop_to_js(&p.1));
        let result = if let Some(prop) = prop {
            let subval = js_get(&inner, &prop)?.0;
            if subval.is_string() && patch.path.len() - 1 == depth {
                if let Ok(s) = subval.dyn_into::<JsString>() {
                    let new_value = self.apply_patch_to_text(&s, patch)?;
                    let result = shallow_copy(&inner);
                    js_set(&result, &prop, &new_value)?;
                    Ok(result)
                } else {
                    // bad patch - short circuit
                    Ok(obj)
                }
            } else if let Ok(sub_obj) = js_get(&inner, &prop)?.0.dyn_into::<Object>() {
                let new_value = self.apply_patch(sub_obj, patch, depth + 1, meta)?;
                let result = shallow_copy(&inner);
                js_set(&result, &prop, &new_value)?;
                Ok(result)
            } else {
                // if a patch is trying to access a deleted object make no change
                // short circuit the wrap process
                return Ok(obj);
            }
        } else if Array::is_array(&inner) {
            if id == patch.obj {
                self.apply_patch_to_array(&inner, patch, meta)
            } else {
                Ok(Array::from(&inner).into())
            }
        } else if id == patch.obj {
            self.apply_patch_to_map(&inner, patch, meta)
        } else {
            Ok(Object::assign(&Object::new(), &inner))
        }?;

        self.wrap_object(result, datatype, &id, meta)
            .map_err(|e| e.into())
    }

    fn apply_patch_to_text(
        &self,
        string: &JsString,
        patch: &Patch,
    ) -> Result<JsValue, error::ApplyPatch> {
        match &patch.action {
            PatchAction::DeleteSeq { index, length, .. } => {
                let index = *index as u32;
                let before = string.slice(0, index);
                let after = string.slice(index + *length as u32, string.length());
                let result = before.concat(&after);
                Ok(result.into())
            }
            PatchAction::SpliceText { index, value, .. } => {
                let index = *index as u32;
                let length = string.length();
                let before = string.slice(0, index);
                let after = string.slice(index, length);
                let result = before.concat(&String::from(value).into()).concat(&after);
                Ok(result.into())
            }
            _ => Ok(string.into()),
        }
    }

    fn sub_splice<'a, I: IntoIterator<Item = &'a (Value<'a>, ObjId, bool)>>(
        &self,
        o: Array,
        index: usize,
        num_del: usize,
        values: I,
        meta: &JsValue,
    ) -> Result<Object, error::Export> {
        let args: Array = values
            .into_iter()
            .map(|v| self.maybe_wrap_object(alloc(&v.0, self.text_rep), &v.1, meta))
            .collect::<Result<_, _>>()?;
        args.unshift(&(num_del as u32).into());
        args.unshift(&(index as u32).into());
        let method = js_get(&o, "splice")?
            .0
            .dyn_into::<Function>()
            .map_err(error::Export::GetSplice)?;
        Reflect::apply(&method, &o, &args).map_err(error::Export::CallSplice)?;
        Ok(o.into())
    }

    fn import(&self, id: JsValue) -> Result<(ObjId, am::ObjType), error::ImportObj> {
        if let Some(s) = id.as_string() {
            // valid formats are
            // 123@aabbcc
            // 123@aabccc/prop1/prop2/prop3
            // /prop1/prop2/prop3
            let mut components = s.split('/');
            let obj = components.next();
            let (id, obj_type) = if obj == Some("") {
                (ROOT, am::ObjType::Map)
            } else {
                self.doc
                    .import(obj.unwrap_or_default())
                    .map_err(error::ImportObj::BadImport)?
            };
            self.import_path(id, obj_type, components)
                .map_err(|e| error::ImportObj::InvalidPath(s.to_string(), e))
        } else {
            Err(error::ImportObj::NotString)
        }
    }

    fn import_path<'a, I: Iterator<Item = &'a str>>(
        &self,
        mut obj: ObjId,
        mut obj_type: am::ObjType,
        components: I,
    ) -> Result<(ObjId, am::ObjType), error::ImportPath> {
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

    fn import_prop(&self, prop: JsValue) -> Result<Prop, error::InvalidProp> {
        if let Some(s) = prop.as_string() {
            Ok(s.into())
        } else if let Some(n) = prop.as_f64() {
            Ok((n as usize).into())
        } else {
            Err(error::InvalidProp)
        }
    }

    fn import_scalar(&self, value: &JsValue, datatype: &Option<String>) -> Option<am::ScalarValue> {
        match datatype.as_deref() {
            Some("boolean") => value.as_bool().map(am::ScalarValue::Boolean),
            Some("int") => value.as_f64().map(|v| am::ScalarValue::Int(v as i64)),
            Some("uint") => value.as_f64().map(|v| am::ScalarValue::Uint(v as u64)),
            Some("str") => value.as_string().map(|v| am::ScalarValue::Str(v.into())),
            Some("f64") => value.as_f64().map(am::ScalarValue::F64),
            Some("bytes") => Some(am::ScalarValue::Bytes(
                value.clone().dyn_into::<Uint8Array>().unwrap().to_vec(),
            )),
            Some("counter") => value.as_f64().map(|v| am::ScalarValue::counter(v as i64)),
            Some("timestamp") => {
                if let Some(v) = value.as_f64() {
                    Some(am::ScalarValue::Timestamp(v as i64))
                } else if let Ok(d) = value.clone().dyn_into::<js_sys::Date>() {
                    Some(am::ScalarValue::Timestamp(d.get_time() as i64))
                } else {
                    None
                }
            }
            Some("null") => Some(am::ScalarValue::Null),
            Some(_) => None,
            None => {
                if value.is_null() {
                    Some(am::ScalarValue::Null)
                } else if let Some(b) = value.as_bool() {
                    Some(am::ScalarValue::Boolean(b))
                } else if let Some(s) = value.as_string() {
                    Some(am::ScalarValue::Str(s.into()))
                } else if let Some(n) = value.as_f64() {
                    if (n.round() - n).abs() < f64::EPSILON {
                        Some(am::ScalarValue::Int(n as i64))
                    } else {
                        Some(am::ScalarValue::F64(n))
                    }
                } else if let Ok(d) = value.clone().dyn_into::<js_sys::Date>() {
                    Some(am::ScalarValue::Timestamp(d.get_time() as i64))
                } else if let Ok(o) = &value.clone().dyn_into::<Uint8Array>() {
                    Some(am::ScalarValue::Bytes(o.to_vec()))
                } else {
                    None
                }
            }
        }
    }

    fn import_value(
        &self,
        value: &JsValue,
        datatype: Option<String>,
    ) -> Result<(Value<'static>, Vec<(Prop, JsValue)>), error::InvalidValue> {
        match self.import_scalar(value, &datatype) {
            Some(val) => Ok((val.into(), vec![])),
            None => {
                if let Ok(js_obj) = import_obj(value, datatype.as_deref()) {
                    Ok((
                        js_obj.objtype().into(),
                        js_obj
                            .subvals()
                            .map(|(p, v)| (p.into_owned(), v))
                            .collect::<Vec<_>>(),
                    ))
                } else {
                    web_sys::console::log_2(&"Invalid value".into(), value);
                    Err(error::InvalidValue)
                }
            }
        }
    }
}

pub(crate) fn alloc(value: &Value<'_>, text_rep: TextRepresentation) -> (Datatype, JsValue) {
    match value {
        am::Value::Object(o) => match o {
            ObjType::Map => (Datatype::Map, Object::new().into()),
            ObjType::Table => (Datatype::Table, Object::new().into()),
            ObjType::List => (Datatype::List, Array::new().into()),
            ObjType::Text => match text_rep {
                TextRepresentation::String => (Datatype::Text, "".into()),
                TextRepresentation::Array => (Datatype::Text, Array::new().into()),
            },
        },
        am::Value::Scalar(s) => match s.as_ref() {
            am::ScalarValue::Bytes(v) => (Datatype::Bytes, Uint8Array::from(v.as_slice()).into()),
            am::ScalarValue::Str(v) => (Datatype::Str, v.to_string().into()),
            am::ScalarValue::Int(v) => (Datatype::Int, (*v as f64).into()),
            am::ScalarValue::Uint(v) => (Datatype::Uint, (*v as f64).into()),
            am::ScalarValue::F64(v) => (Datatype::F64, (*v).into()),
            am::ScalarValue::Counter(v) => (Datatype::Counter, (f64::from(v)).into()),
            am::ScalarValue::Timestamp(v) => (
                Datatype::Timestamp,
                js_sys::Date::new(&(*v as f64).into()).into(),
            ),
            am::ScalarValue::Boolean(v) => (Datatype::Boolean, (*v).into()),
            am::ScalarValue::Null => (Datatype::Null, JsValue::null()),
            am::ScalarValue::Unknown { bytes, type_code } => (
                Datatype::Unknown(*type_code),
                Uint8Array::from(bytes.as_slice()).into(),
            ),
        },
    }
}

fn set_hidden_value<V: Into<JsValue>>(
    o: &Object,
    key: &Symbol,
    value: V,
) -> Result<(), error::Export> {
    let definition = Object::new();
    js_set(&definition, "value", &value.into()).map_err(|_| error::Export::SetHidden("value"))?;
    js_set(&definition, "writable", false).map_err(|_| error::Export::SetHidden("writable"))?;
    js_set(&definition, "enumerable", false).map_err(|_| error::Export::SetHidden("enumerable"))?;
    js_set(&definition, "configurable", false)
        .map_err(|_| error::Export::SetHidden("configurable"))?;
    Object::define_property(o, &key.into(), &definition);
    Ok(())
}

fn export_path(path: &[(ObjId, Prop)], end: &Prop) -> Array {
    let result = Array::new();
    for p in path {
        result.push(&prop_to_js(&p.1));
    }
    result.push(&prop_to_js(end));
    result
}

fn export_just_path(path: &[(ObjId, Prop)]) -> Array {
    let result = Array::new();
    for p in path {
        result.push(&prop_to_js(&p.1));
    }
    result
}

pub(crate) fn export_patches<I: IntoIterator<Item = Patch>>(
    doc: &Automerge,
    patches: I,
    heads: &[ChangeHash],
) -> Result<Array, error::Export> {
    // this is for performance - each block is the same
    // so i only want to materialize each block once per
    // apply patches
    let mut blocks = HashMap::new();
    patches
        .into_iter()
        // removing update block for now
        .filter(|p| !matches!(p.action, PatchAction::UpdateBlock { .. }))
        .map(|p| export_patch(doc, p, &mut blocks, heads))
        .collect()
}

fn export_patch(
    doc: &Automerge,
    p: Patch,
    blocks: &mut HashMap<ObjId, Object>,
    heads: &[ChangeHash],
) -> Result<JsValue, error::Export> {
    let result = Object::new();
    let path = &p.path.as_slice();
    match p.action {
        PatchAction::PutMap {
            key,
            value,
            conflict,
            ..
        } => {
            js_set(&result, "action", "put")?;
            js_set(&result, "path", export_path(path, &Prop::Map(key)))?;
            js_set(
                &result,
                "value",
                alloc(&value.0, TextRepresentation::String).1,
            )?;
            if conflict {
                js_set(&result, "conflict", true)?;
            }
            Ok(result.into())
        }
        PatchAction::PutSeq {
            index,
            value,
            conflict,
            ..
        } => {
            js_set(&result, "action", "put")?;
            js_set(&result, "path", export_path(path, &Prop::Seq(index)))?;
            js_set(
                &result,
                "value",
                alloc(&value.0, TextRepresentation::String).1,
            )?;
            if conflict {
                js_set(&result, "conflict", true)?;
            }
            Ok(result.into())
        }
        PatchAction::Insert { index, values, .. } => {
            let conflicts = values.iter().map(|v| v.2).collect::<Vec<_>>();
            js_set(&result, "action", "insert")?;
            js_set(&result, "path", export_path(path, &Prop::Seq(index)))?;
            js_set(
                &result,
                "values",
                values
                    .iter()
                    .map(|v| alloc(&v.0, TextRepresentation::String).1)
                    .collect::<Array>(),
            )?;
            if conflicts.iter().any(|c| *c) {
                js_set(
                    &result,
                    "conflicts",
                    conflicts
                        .iter()
                        .map(|c| JsValue::from(*c))
                        .collect::<Array>(),
                )?;
            }
            Ok(result.into())
        }
        PatchAction::SpliceText {
            index,
            value,
            marks,
            ..
        } => {
            js_set(&result, "action", "splice")?;
            js_set(&result, "path", export_path(path, &Prop::Seq(index)))?;
            js_set(&result, "value", String::from(&value))?;
            if let Some(m) = marks {
                if m.num_marks() > 0 {
                    let marks = Object::new();
                    for (name, value) in m.iter_marks() {
                        js_set(
                            &marks,
                            name,
                            alloc(&value.into(), TextRepresentation::String).1,
                        )?;
                    }
                    js_set(&result, "marks", marks)?;
                }
                if let Some(b) = m.block() {
                    let block = export_block(doc, b, blocks, heads)?;
                    //blocks.entry(b).or_insert(export_object(doc,b,ObjType::Map.into(),Some(heads),&JsValue::undefined())?);
                    js_set(&result, "block", block)?;
                }
            }
            Ok(result.into())
        }
        PatchAction::Increment { prop, value, .. } => {
            js_set(&result, "action", "inc")?;
            js_set(&result, "path", export_path(path, &prop))?;
            js_set(&result, "value", &JsValue::from_f64(value as f64))?;
            Ok(result.into())
        }
        PatchAction::DeleteMap { key, .. } => {
            js_set(&result, "action", "del")?;
            js_set(&result, "path", export_path(path, &Prop::Map(key)))?;
            Ok(result.into())
        }
        PatchAction::DeleteSeq { index, length, .. } => {
            js_set(&result, "action", "del")?;
            js_set(&result, "path", export_path(path, &Prop::Seq(index)))?;
            if length > 1 {
                js_set(&result, "length", length)?;
            }
            Ok(result.into())
        }
        PatchAction::Mark { marks, .. } => {
            js_set(&result, "action", "mark")?;
            js_set(&result, "path", export_just_path(path))?;
            let marks_array = Array::new();
            for m in marks.iter() {
                let mark = Object::new();
                js_set(&mark, "name", m.name())?;
                js_set(
                    &mark,
                    "value",
                    &alloc(&m.value().into(), TextRepresentation::String).1,
                )?;
                js_set(&mark, "start", m.start as i32)?;
                js_set(&mark, "end", m.end as i32)?;
                marks_array.push(&mark);
            }
            js_set(&result, "marks", marks_array)?;
            Ok(result.into())
        }
        PatchAction::Conflict { prop } => {
            js_set(&result, "action", "conflict")?;
            js_set(&result, "path", export_path(path, &prop))?;
            Ok(result.into())
        }
        PatchAction::SplitBlock {
            index,
            cursor,
            conflict,
        } => {
            js_set(&result, "action", "splitBlock")?;
            js_set(&result, "path", export_path(path, &index.into()))?;
            js_set(&result, "cursor", cursor.to_string())?;
            if conflict {
                js_set(&result, "conflict", true)?;
            }
            Ok(result.into())
        }
        PatchAction::UpdateBlock { patch } => {
            js_set(&result, "action", "updateBlock")?;
            js_set(&result, "path", export_path(path, &Prop::from(999)))?;
            js_set(&result, "patch", export_patch(doc, *patch, blocks, heads)?)?;
            Ok(result.into())
        }
        PatchAction::JoinBlock { index, cursor } => {
            js_set(&result, "action", "joinBlock")?;
            js_set(&result, "path", export_path(path, &index.into()))?;
            js_set(&result, "cursor", cursor.to_string())?;
            Ok(result.into())
        }
    }
}

fn export_block<'a>(
    doc: &Automerge,
    id: &ObjId,
    blocks: &'a mut HashMap<ObjId, Object>,
    heads: &[ChangeHash],
) -> Result<&'a Object, error::Export> {
    Ok(blocks.entry(id.clone()).or_insert_with(|| {
        doc.export_map(id, Some(heads), &JsValue::undefined())
            // how do I remove this unwrap?
            .unwrap_or_else(|_| Object::new())
    }))
}

fn shallow_copy(obj: &Object) -> Object {
    if Array::is_array(obj) {
        Array::from(obj).into()
    } else {
        Object::assign(&Object::new(), obj)
    }
}

fn prop_to_js(prop: &Prop) -> JsValue {
    match prop {
        Prop::Map(key) => key.into(),
        Prop::Seq(index) => (*index as f64).into(),
    }
}

pub(crate) mod error {
    use automerge::{AutomergeError, LoadChangeError};
    use wasm_bindgen::JsValue;

    #[derive(Debug, thiserror::Error)]
    pub enum BadJSChanges {
        #[error("the changes were not an array of Uint8Array")]
        ChangesNotArray,
        #[error("change {0} was not a Uint8Array")]
        ElemNotUint8Array(usize),
        #[error("error loading change {0}: {1}")]
        BadChange(usize, LoadChangeError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadChangeHashes {
        #[error("the change hashes were not an array of strings")]
        NotArray,
        #[error("could not decode hash {0}: {1}")]
        BadElem(usize, BadChangeHash),
    }

    impl From<BadChangeHashes> for JsValue {
        fn from(e: BadChangeHashes) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadChangeHashSet {
        #[error("not an object")]
        NotObject,
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error("unable to getOwnProperties")]
        ListProp,
        #[error("unable to parse hash from {0:?}: {1}")]
        BadHash(wasm_bindgen::JsValue, BadChangeHash),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadChangeHash {
        #[error("change hash was not a string")]
        NotString,
        #[error(transparent)]
        Parse(#[from] automerge::ParseChangeHashError),
    }

    impl From<BadChangeHash> for JsValue {
        fn from(e: BadChangeHash) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadSyncState {
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error("bad sharedHeads: {0}")]
        BadSharedHeads(BadChangeHashes),
        #[error("bad lastSentHeads: {0}")]
        BadLastSentHeads(BadChangeHashes),
        #[error("bad theirHeads: {0}")]
        BadTheirHeads(BadChangeHashes),
        #[error("bad theirNeed: {0}")]
        BadTheirNeed(BadChangeHashes),
        #[error("bad theirHave: {0}")]
        BadTheirHave(BadHaves),
        #[error("bad sentHashes: {0}")]
        BadSentHashes(BadChangeHashSet),
        #[error("inFlight not a boolean")]
        InFlightNotBoolean,
    }

    impl From<BadSyncState> for JsValue {
        fn from(e: BadSyncState) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("unable to get property {property}: {error:?}")]
    pub struct GetProp {
        pub(super) property: String,
        pub(super) error: wasm_bindgen::JsValue,
    }

    impl From<GetProp> for JsValue {
        fn from(e: GetProp) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("error setting property {property:?} on JS value: {error:?}")]
    pub struct SetProp {
        pub(super) property: JsValue,
        pub(super) error: JsValue,
    }

    impl From<SetProp> for JsValue {
        fn from(e: SetProp) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadHave {
        #[error("bad lastSync: {0}")]
        BadLastSync(BadChangeHashes),
        #[error("bad bloom: {0}")]
        BadBloom(BadBloom),
        #[error(transparent)]
        GetHaveProp(#[from] GetProp),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadHaves {
        #[error("value was not an array")]
        NotArray,
        #[error("error loading have at index {0}: {1}")]
        BadElem(usize, BadHave),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadBloom {
        #[error("the value was not a Uint8Array")]
        NotU8Array,
        #[error("unable to decode: {0}")]
        Decode(#[from] automerge::sync::DecodeBloomError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Export {
        #[error(transparent)]
        Set(#[from] SetProp),
        #[error("unable to delete prop {prop}: {err:?}")]
        Delete { prop: String, err: JsValue },
        #[error("unable to set hidden property {0}")]
        SetHidden(&'static str),
        #[error("data handler for type {0} did not return a valid object")]
        InvalidDataHandler(String),
        #[error("error calling data handler for type {0}: {1:?}")]
        CallDataHandler(String, JsValue),
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error(transparent)]
        InvalidDatatype(#[from] crate::value::InvalidDatatype),
        #[error("unable to get the splice function: {0:?}")]
        GetSplice(JsValue),
        #[error("error calling splice: {0:?}")]
        CallSplice(JsValue),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
    }

    impl From<Export> for JsValue {
        fn from(e: Export) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ApplyPatch {
        #[error(transparent)]
        Export(#[from] Export),
        #[error("cannot delete from a seq")]
        DeleteKeyFromSeq,
        #[error("cannot put key in seq")]
        PutKeyInSeq,
        #[error("cannot increment a non-numeric value")]
        IncrementNonNumeric,
        #[error("cannot increment a key in a seq")]
        IncrementKeyInSeq,
        #[error("cannot increment index in a map")]
        IncrementIndexInMap,
        #[error("cannot insert into a map")]
        InsertInMap,
        #[error("cannot splice into a map")]
        SpliceInMap,
        #[error("cannot splice text into a seq")]
        SpliceTextInSeq,
        #[error("cannot splice text into a map")]
        SpliceTextInMap,
        #[error("cannot put a seq index in a map")]
        PutIdxInMap,
        #[error("cannot mark a span in a map")]
        MarkInMap,
        #[error("cannot have blocks in a map")]
        BlockInMap,
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error(transparent)]
        SetProp(#[from] SetProp),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
    }

    impl From<ApplyPatch> for JsValue {
        fn from(e: ApplyPatch) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadSyncMessage {
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error("unable to read haves: {0}")]
        BadHaves(#[from] BadHaves),
        #[error("could not read changes: {0}")]
        BadJSChanges(#[from] BadJSChanges),
        #[error("could not read heads: {0}")]
        BadHeads(BadChangeHashes),
        #[error("could not read need: {0}")]
        BadNeed(BadChangeHashes),
    }

    impl From<BadSyncMessage> for JsValue {
        fn from(e: BadSyncMessage) -> Self {
            JsValue::from(e.to_string())
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
    }

    impl From<ImportObj> for JsValue {
        fn from(e: ImportObj) -> Self {
            JsValue::from(format!("invalid object ID: {}", e))
        }
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

    #[derive(Debug, thiserror::Error)]
    #[error("expand must be 'left', 'right', 'both', or 'none' - is 'right' by default")]
    pub struct BadExpand;

    #[derive(Debug, thiserror::Error)]
    #[error("argument must be a number")]
    pub struct BadNumber;

    #[derive(Debug, thiserror::Error)]
    #[error("given property was not a string or integer")]
    pub struct InvalidProp;

    #[derive(Debug, thiserror::Error)]
    #[error("given property was not a string or integer")]
    pub struct InvalidValue;
}

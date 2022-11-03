use crate::value::Datatype;
use crate::Automerge;
use automerge as am;
use automerge::transaction::Transactable;
use automerge::{Change, ChangeHash, ObjType, Prop};
use js_sys::{Array, Function, Object, Reflect, Symbol, Uint8Array};
use std::collections::{BTreeSet, HashSet};
use std::fmt::Display;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::{observer::Patch, ObjId, Value};

const RAW_DATA_SYMBOL: &str = "_am_raw_value_";
const DATATYPE_SYMBOL: &str = "_am_datatype_";
const RAW_OBJECT_SYMBOL: &str = "_am_objectId";
const META_SYMBOL: &str = "_am_meta";

pub(crate) struct JS(pub(crate) JsValue);
pub(crate) struct AR(pub(crate) Array);

impl From<AR> for JsValue {
    fn from(ar: AR) -> Self {
        ar.0.into()
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

impl TryFrom<JS> for HashSet<ChangeHash> {
    type Error = JsValue;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let mut result = HashSet::new();
        for key in Reflect::own_keys(&value.0)?.iter() {
            if let Some(true) = Reflect::get(&value.0, &key)?.as_bool() {
                result.insert(serde_wasm_bindgen::from_value(key).map_err(to_js_err)?);
            }
        }
        Ok(result)
    }
}

impl TryFrom<JS> for BTreeSet<ChangeHash> {
    type Error = JsValue;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let mut result = BTreeSet::new();
        for key in Reflect::own_keys(&value.0)?.iter() {
            if let Some(true) = Reflect::get(&value.0, &key)?.as_bool() {
                result.insert(serde_wasm_bindgen::from_value(key).map_err(to_js_err)?);
            }
        }
        Ok(result)
    }
}

impl TryFrom<JS> for Vec<ChangeHash> {
    type Error = JsValue;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value.0.dyn_into::<Array>()?;
        let value: Result<Vec<ChangeHash>, _> =
            value.iter().map(serde_wasm_bindgen::from_value).collect();
        let value = value.map_err(to_js_err)?;
        Ok(value)
    }
}

impl From<JS> for Option<Vec<ChangeHash>> {
    fn from(value: JS) -> Self {
        let value = value.0.dyn_into::<Array>().ok()?;
        let value: Result<Vec<ChangeHash>, _> =
            value.iter().map(serde_wasm_bindgen::from_value).collect();
        let value = value.ok()?;
        Some(value)
    }
}

impl TryFrom<JS> for Vec<Change> {
    type Error = JsValue;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value.0.dyn_into::<Array>()?;
        let changes: Result<Vec<Uint8Array>, _> = value.iter().map(|j| j.dyn_into()).collect();
        let changes = changes?;
        let changes = changes.iter().try_fold(Vec::new(), |mut acc, arr| {
            match automerge::Change::try_from(arr.to_vec().as_slice()) {
                Ok(c) => acc.push(c),
                Err(e) => return Err(to_js_err(e)),
            }
            Ok(acc)
        })?;
        Ok(changes)
    }
}

impl TryFrom<JS> for am::sync::State {
    type Error = JsValue;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value.0;
        let shared_heads = js_get(&value, "sharedHeads")?.try_into()?;
        let last_sent_heads = js_get(&value, "lastSentHeads")?.try_into()?;
        let their_heads = js_get(&value, "theirHeads")?.into();
        let their_need = js_get(&value, "theirNeed")?.into();
        let their_have = js_get(&value, "theirHave")?.try_into()?;
        let sent_hashes = js_get(&value, "sentHashes")?.try_into()?;
        Ok(am::sync::State {
            shared_heads,
            last_sent_heads,
            their_heads,
            their_need,
            their_have,
            sent_hashes,
        })
    }
}

impl TryFrom<JS> for Option<Vec<am::sync::Have>> {
    type Error = JsValue;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.0.is_null() {
            Ok(None)
        } else {
            Ok(Some(value.try_into()?))
        }
    }
}

impl TryFrom<JS> for Vec<am::sync::Have> {
    type Error = JsValue;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value.0.dyn_into::<Array>()?;
        let have: Result<Vec<am::sync::Have>, JsValue> = value
            .iter()
            .map(|s| {
                let last_sync = js_get(&s, "lastSync")?.try_into()?;
                let bloom = js_get(&s, "bloom")?.try_into()?;
                Ok(am::sync::Have { last_sync, bloom })
            })
            .collect();
        let have = have?;
        Ok(have)
    }
}

impl TryFrom<JS> for am::sync::BloomFilter {
    type Error = JsValue;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value: Uint8Array = value.0.dyn_into()?;
        let value = value.to_vec();
        let value = value.as_slice().try_into().map_err(to_js_err)?;
        Ok(value)
    }
}

impl From<&[ChangeHash]> for AR {
    fn from(value: &[ChangeHash]) -> Self {
        AR(value
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(h.0)))
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
                // FIXME - the clone and the unwrap here shouldnt be needed - look at into_bytes()
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

pub(crate) fn js_get<J: Into<JsValue>>(obj: J, prop: &str) -> Result<JS, JsValue> {
    Ok(JS(Reflect::get(&obj.into(), &prop.into())?))
}

pub(crate) fn js_set<V: Into<JsValue>>(obj: &JsValue, prop: &str, val: V) -> Result<bool, JsValue> {
    Reflect::set(obj, &prop.into(), &val.into())
}

pub(crate) fn to_prop(p: JsValue) -> Result<Prop, JsValue> {
    if let Some(s) = p.as_string() {
        Ok(Prop::Map(s))
    } else if let Some(n) = p.as_f64() {
        Ok(Prop::Seq(n as usize))
    } else {
        Err(to_js_err("prop must me a string or number"))
    }
}

pub(crate) fn to_objtype(
    value: &JsValue,
    datatype: &Option<String>,
) -> Option<(ObjType, Vec<(Prop, JsValue)>)> {
    match datatype.as_deref() {
        Some("map") => {
            let map = value.clone().dyn_into::<js_sys::Object>().ok()?;
            let map = js_sys::Object::keys(&map)
                .iter()
                .zip(js_sys::Object::values(&map).iter())
                .map(|(key, val)| (key.as_string().unwrap().into(), val))
                .collect();
            Some((ObjType::Map, map))
        }
        Some("list") => {
            let list = value.clone().dyn_into::<js_sys::Array>().ok()?;
            let list = list
                .iter()
                .enumerate()
                .map(|(i, e)| (i.into(), e))
                .collect();
            Some((ObjType::List, list))
        }
        Some("text") => {
            let text = value.as_string()?;
            let text = text
                .chars()
                .enumerate()
                .map(|(i, ch)| (i.into(), ch.to_string().into()))
                .collect();
            Some((ObjType::Text, text))
        }
        Some(_) => None,
        None => {
            if let Ok(list) = value.clone().dyn_into::<js_sys::Array>() {
                let list = list
                    .iter()
                    .enumerate()
                    .map(|(i, e)| (i.into(), e))
                    .collect();
                Some((ObjType::List, list))
            } else if let Ok(map) = value.clone().dyn_into::<js_sys::Object>() {
                // FIXME unwrap
                let map = js_sys::Object::keys(&map)
                    .iter()
                    .zip(js_sys::Object::values(&map).iter())
                    .map(|(key, val)| (key.as_string().unwrap().into(), val))
                    .collect();
                Some((ObjType::Map, map))
            } else if let Some(text) = value.as_string() {
                let text = text
                    .chars()
                    .enumerate()
                    .map(|(i, ch)| (i.into(), ch.to_string().into()))
                    .collect();
                Some((ObjType::Text, text))
            } else {
                None
            }
        }
    }
}

pub(crate) fn get_heads(heads: Option<Array>) -> Option<Vec<ChangeHash>> {
    let heads = heads?;
    let heads: Result<Vec<ChangeHash>, _> =
        heads.iter().map(serde_wasm_bindgen::from_value).collect();
    heads.ok()
}

impl Automerge {
    pub(crate) fn export_object(
        &self,
        obj: &ObjId,
        datatype: Datatype,
        heads: Option<&Vec<ChangeHash>>,
        meta: &JsValue,
    ) -> Result<JsValue, JsValue> {
        let result = if datatype.is_sequence() {
            self.wrap_object(
                self.export_list(obj, heads, meta)?,
                datatype,
                &obj.to_string().into(),
                meta,
            )?
        } else {
            self.wrap_object(
                self.export_map(obj, heads, meta)?,
                datatype,
                &obj.to_string().into(),
                meta,
            )?
        };
        Ok(result.into())
    }

    pub(crate) fn export_map(
        &self,
        obj: &ObjId,
        heads: Option<&Vec<ChangeHash>>,
        meta: &JsValue,
    ) -> Result<Object, JsValue> {
        let keys = self.doc.keys(obj);
        let map = Object::new();
        for k in keys {
            let val_and_id = if let Some(heads) = heads {
                self.doc.get_at(obj, &k, heads)
            } else {
                self.doc.get(obj, &k)
            };
            if let Ok(Some((val, id))) = val_and_id {
                let subval = match val {
                    Value::Object(o) => self.export_object(&id, o.into(), heads, meta)?,
                    Value::Scalar(_) => self.export_value(alloc(&val))?,
                };
                Reflect::set(&map, &k.into(), &subval)?;
            };
        }

        Ok(map)
    }

    pub(crate) fn export_list(
        &self,
        obj: &ObjId,
        heads: Option<&Vec<ChangeHash>>,
        meta: &JsValue,
    ) -> Result<Object, JsValue> {
        let len = self.doc.length(obj);
        let array = Array::new();
        for i in 0..len {
            let val_and_id = if let Some(heads) = heads {
                self.doc.get_at(obj, i as usize, heads)
            } else {
                self.doc.get(obj, i as usize)
            };
            if let Ok(Some((val, id))) = val_and_id {
                let subval = match val {
                    Value::Object(o) => self.export_object(&id, o.into(), heads, meta)?,
                    Value::Scalar(_) => self.export_value(alloc(&val))?,
                };
                array.push(&subval);
            };
        }

        Ok(array.into())
    }

    pub(crate) fn export_value(
        &self,
        (datatype, raw_value): (Datatype, JsValue),
    ) -> Result<JsValue, JsValue> {
        if let Some(function) = self.external_types.get(&datatype) {
            let wrapped_value = function.call1(&JsValue::undefined(), &raw_value)?;
            if let Ok(o) = wrapped_value.dyn_into::<Object>() {
                let key = Symbol::for_(RAW_DATA_SYMBOL);
                set_hidden_value(&o, &key, &raw_value)?;
                let key = Symbol::for_(DATATYPE_SYMBOL);
                set_hidden_value(&o, &key, datatype)?;
                Ok(o.into())
            } else {
                Err(to_js_err(format!(
                    "data handler for type {} did not return a valid object",
                    datatype
                )))
            }
        } else {
            Ok(raw_value)
        }
    }

    pub(crate) fn unwrap_object(
        &self,
        ext_val: &Object,
    ) -> Result<(Object, Datatype, JsValue), JsValue> {
        let inner = Reflect::get(ext_val, &Symbol::for_(RAW_DATA_SYMBOL))?;

        let datatype = Reflect::get(ext_val, &Symbol::for_(DATATYPE_SYMBOL))?.try_into();

        let mut id = Reflect::get(ext_val, &Symbol::for_(RAW_OBJECT_SYMBOL))?;
        if id.is_undefined() {
            id = "_root".into();
        }

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

    pub(crate) fn unwrap_scalar(&self, ext_val: JsValue) -> Result<JsValue, JsValue> {
        let inner = Reflect::get(&ext_val, &Symbol::for_(RAW_DATA_SYMBOL))?;
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
    ) -> Result<JsValue, JsValue> {
        if let Ok(obj) = raw_value.clone().dyn_into::<Object>() {
            let result = self.wrap_object(obj, datatype, &id.to_string().into(), meta)?;
            Ok(result.into())
        } else {
            self.export_value((datatype, raw_value))
        }
    }

    pub(crate) fn wrap_object(
        &self,
        value: Object,
        datatype: Datatype,
        id: &JsValue,
        meta: &JsValue,
    ) -> Result<Object, JsValue> {
        let value = if let Some(function) = self.external_types.get(&datatype) {
            let wrapped_value = function.call1(&JsValue::undefined(), &value)?;
            let wrapped_object = wrapped_value.dyn_into::<Object>().map_err(|_| {
                to_js_err(format!(
                    "data handler for type {} did not return a valid object",
                    datatype
                ))
            })?;
            set_hidden_value(&wrapped_object, &Symbol::for_(RAW_DATA_SYMBOL), value)?;
            wrapped_object
        } else {
            value
        };
        if matches!(datatype, Datatype::Map | Datatype::List | Datatype::Text) {
            set_hidden_value(&value, &Symbol::for_(RAW_OBJECT_SYMBOL), id)?;
        }
        set_hidden_value(&value, &Symbol::for_(DATATYPE_SYMBOL), datatype)?;
        set_hidden_value(&value, &Symbol::for_(META_SYMBOL), meta)?;
        if self.freeze {
            Object::freeze(&value);
        }
        Ok(value)
    }

    pub(crate) fn apply_patch_to_array(
        &self,
        array: &Object,
        patch: &Patch,
        meta: &JsValue,
    ) -> Result<Object, JsValue> {
        let result = Array::from(array); // shallow copy
        match patch {
            Patch::PutSeq { index, value, .. } => {
                let sub_val = self.maybe_wrap_object(alloc(&value.0), &value.1, meta)?;
                Reflect::set(&result, &(*index as f64).into(), &sub_val)?;
                Ok(result.into())
            }
            Patch::DeleteSeq { index, .. } => self.sub_splice(result, *index, 1, vec![], meta),
            Patch::Insert { index, values, .. } => self.sub_splice(result, *index, 0, values, meta),
            Patch::Increment { prop, value, .. } => {
                if let Prop::Seq(index) = prop {
                    let index = (*index as f64).into();
                    let old_val = Reflect::get(&result, &index)?;
                    let old_val = self.unwrap_scalar(old_val)?;
                    if let Some(old) = old_val.as_f64() {
                        let new_value: Value<'_> =
                            am::ScalarValue::counter(old as i64 + *value).into();
                        Reflect::set(&result, &index, &self.export_value(alloc(&new_value))?)?;
                        Ok(result.into())
                    } else {
                        Err(to_js_err("cant increment a non number value"))
                    }
                } else {
                    Err(to_js_err("cant increment a key on a seq"))
                }
            }
            Patch::DeleteMap { .. } => Err(to_js_err("cannot delete from a seq")),
            Patch::PutMap { .. } => Err(to_js_err("cannot set key in seq")),
        }
    }

    pub(crate) fn apply_patch_to_map(
        &self,
        map: &Object,
        patch: &Patch,
        meta: &JsValue,
    ) -> Result<Object, JsValue> {
        let result = Object::assign(&Object::new(), map); // shallow copy
        match patch {
            Patch::PutMap { key, value, .. } => {
                let sub_val = self.maybe_wrap_object(alloc(&value.0), &value.1, meta)?;
                Reflect::set(&result, &key.into(), &sub_val)?;
                Ok(result)
            }
            Patch::DeleteMap { key, .. } => {
                Reflect::delete_property(&result, &key.into())?;
                Ok(result)
            }
            Patch::Increment { prop, value, .. } => {
                if let Prop::Map(key) = prop {
                    let key = key.into();
                    let old_val = Reflect::get(&result, &key)?;
                    let old_val = self.unwrap_scalar(old_val)?;
                    if let Some(old) = old_val.as_f64() {
                        let new_value: Value<'_> =
                            am::ScalarValue::counter(old as i64 + *value).into();
                        Reflect::set(&result, &key, &self.export_value(alloc(&new_value))?)?;
                        Ok(result)
                    } else {
                        Err(to_js_err("cant increment a non number value"))
                    }
                } else {
                    Err(to_js_err("cant increment an index on a map"))
                }
            }
            Patch::Insert { .. } => Err(to_js_err("cannot insert into map")),
            Patch::DeleteSeq { .. } => Err(to_js_err("cannot splice a map")),
            Patch::PutSeq { .. } => Err(to_js_err("cannot array index a map")),
        }
    }

    pub(crate) fn apply_patch(
        &self,
        obj: Object,
        patch: &Patch,
        depth: usize,
        meta: &JsValue,
    ) -> Result<Object, JsValue> {
        let (inner, datatype, id) = self.unwrap_object(&obj)?;
        let prop = patch.path().get(depth).map(|p| prop_to_js(&p.1));
        let result = if let Some(prop) = prop {
            if let Ok(sub_obj) = Reflect::get(&inner, &prop)?.dyn_into::<Object>() {
                let new_value = self.apply_patch(sub_obj, patch, depth + 1, meta)?;
                let result = shallow_copy(&inner);
                Reflect::set(&result, &prop, &new_value)?;
                Ok(result)
            } else {
                // if a patch is trying to access a deleted object make no change
                // short circuit the wrap process
                return Ok(obj);
            }
        } else if Array::is_array(&inner) {
            self.apply_patch_to_array(&inner, patch, meta)
        } else {
            self.apply_patch_to_map(&inner, patch, meta)
        }?;

        self.wrap_object(result, datatype, &id, meta)
    }

    fn sub_splice<'a, I: IntoIterator<Item = &'a (Value<'a>, ObjId)>>(
        &self,
        o: Array,
        index: usize,
        num_del: usize,
        values: I,
        meta: &JsValue,
    ) -> Result<Object, JsValue> {
        let args: Array = values
            .into_iter()
            .map(|v| self.maybe_wrap_object(alloc(&v.0), &v.1, meta))
            .collect::<Result<_, _>>()?;
        args.unshift(&(num_del as u32).into());
        args.unshift(&(index as u32).into());
        let method = Reflect::get(&o, &"splice".into())?.dyn_into::<Function>()?;
        Reflect::apply(&method, &o, &args)?;
        Ok(o.into())
    }
}

pub(crate) fn alloc(value: &Value<'_>) -> (Datatype, JsValue) {
    match value {
        am::Value::Object(o) => match o {
            ObjType::Map => (Datatype::Map, Object::new().into()),
            ObjType::Table => (Datatype::Table, Object::new().into()),
            ObjType::List => (Datatype::List, Array::new().into()),
            ObjType::Text => (Datatype::Text, Array::new().into()),
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

fn set_hidden_value<V: Into<JsValue>>(o: &Object, key: &Symbol, value: V) -> Result<(), JsValue> {
    let definition = Object::new();
    js_set(&definition, "value", &value.into())?;
    js_set(&definition, "writable", false)?;
    js_set(&definition, "enumerable", false)?;
    js_set(&definition, "configurable", false)?;
    Object::define_property(o, &key.into(), &definition);
    Ok(())
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

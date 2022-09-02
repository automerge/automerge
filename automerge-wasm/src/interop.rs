use crate::AutoCommit;
use automerge as am;
use automerge::transaction::Transactable;
use automerge::{Change, ChangeHash, Prop};
use js_sys::{Array, Function, Object, Reflect, Uint8Array};
use std::collections::{BTreeSet, HashSet};
use std::fmt::Display;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::{observer::Patch, ObjId, ScalarValue, Value};

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
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&h.to_string()))
            .collect();
        JS(heads.into())
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
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
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
                    .map(|h| JsValue::from_str(&hex::encode(&h.0)))
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
) -> Option<(am::ObjType, Vec<(Prop, JsValue)>)> {
    match datatype.as_deref() {
        Some("map") => {
            let map = value.clone().dyn_into::<js_sys::Object>().ok()?;
            // FIXME unwrap
            let map = js_sys::Object::keys(&map)
                .iter()
                .zip(js_sys::Object::values(&map).iter())
                .map(|(key, val)| (key.as_string().unwrap().into(), val))
                .collect();
            Some((am::ObjType::Map, map))
        }
        Some("list") => {
            let list = value.clone().dyn_into::<js_sys::Array>().ok()?;
            let list = list
                .iter()
                .enumerate()
                .map(|(i, e)| (i.into(), e))
                .collect();
            Some((am::ObjType::List, list))
        }
        Some("text") => {
            let text = value.as_string()?;
            let text = text
                .chars()
                .enumerate()
                .map(|(i, ch)| (i.into(), ch.to_string().into()))
                .collect();
            Some((am::ObjType::Text, text))
        }
        Some(_) => None,
        None => {
            if let Ok(list) = value.clone().dyn_into::<js_sys::Array>() {
                let list = list
                    .iter()
                    .enumerate()
                    .map(|(i, e)| (i.into(), e))
                    .collect();
                Some((am::ObjType::List, list))
            } else if let Ok(map) = value.clone().dyn_into::<js_sys::Object>() {
                // FIXME unwrap
                let map = js_sys::Object::keys(&map)
                    .iter()
                    .zip(js_sys::Object::values(&map).iter())
                    .map(|(key, val)| (key.as_string().unwrap().into(), val))
                    .collect();
                Some((am::ObjType::Map, map))
            } else if let Some(text) = value.as_string() {
                let text = text
                    .chars()
                    .enumerate()
                    .map(|(i, ch)| (i.into(), ch.to_string().into()))
                    .collect();
                Some((am::ObjType::Text, text))
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

pub(crate) fn map_to_js(doc: &AutoCommit, obj: &ObjId) -> JsValue {
    let keys = doc.keys(obj);
    let map = Object::new();
    for k in keys {
        let val = doc.get(obj, &k);
        match val {
            Ok(Some((Value::Object(o), exid)))
                if o == am::ObjType::Map || o == am::ObjType::Table =>
            {
                Reflect::set(&map, &k.into(), &map_to_js(doc, &exid)).unwrap();
            }
            Ok(Some((Value::Object(o), exid))) if o == am::ObjType::List => {
                Reflect::set(&map, &k.into(), &list_to_js(doc, &exid)).unwrap();
            }
            Ok(Some((Value::Object(o), exid))) if o == am::ObjType::Text => {
                Reflect::set(&map, &k.into(), &doc.text(&exid).unwrap().into()).unwrap();
            }
            Ok(Some((Value::Scalar(v), _))) => {
                Reflect::set(&map, &k.into(), &ScalarValue(v).into()).unwrap();
            }
            _ => (),
        };
    }
    map.into()
}

pub(crate) fn map_to_js_at(doc: &AutoCommit, obj: &ObjId, heads: &[ChangeHash]) -> JsValue {
    let keys = doc.keys(obj);
    let map = Object::new();
    for k in keys {
        let val = doc.get_at(obj, &k, heads);
        match val {
            Ok(Some((Value::Object(o), exid)))
                if o == am::ObjType::Map || o == am::ObjType::Table =>
            {
                Reflect::set(&map, &k.into(), &map_to_js_at(doc, &exid, heads)).unwrap();
            }
            Ok(Some((Value::Object(o), exid))) if o == am::ObjType::List => {
                Reflect::set(&map, &k.into(), &list_to_js_at(doc, &exid, heads)).unwrap();
            }
            Ok(Some((Value::Object(o), exid))) if o == am::ObjType::Text => {
                Reflect::set(&map, &k.into(), &doc.text_at(&exid, heads).unwrap().into()).unwrap();
            }
            Ok(Some((Value::Scalar(v), _))) => {
                Reflect::set(&map, &k.into(), &ScalarValue(v).into()).unwrap();
            }
            _ => (),
        };
    }
    map.into()
}

pub(crate) fn list_to_js(doc: &AutoCommit, obj: &ObjId) -> JsValue {
    let len = doc.length(obj);
    let array = Array::new();
    for i in 0..len {
        let val = doc.get(obj, i as usize);
        match val {
            Ok(Some((Value::Object(o), exid)))
                if o == am::ObjType::Map || o == am::ObjType::Table =>
            {
                array.push(&map_to_js(doc, &exid));
            }
            Ok(Some((Value::Object(o), exid))) if o == am::ObjType::List => {
                array.push(&list_to_js(doc, &exid));
            }
            Ok(Some((Value::Object(o), exid))) if o == am::ObjType::Text => {
                array.push(&doc.text(&exid).unwrap().into());
            }
            Ok(Some((Value::Scalar(v), _))) => {
                array.push(&ScalarValue(v).into());
            }
            _ => (),
        };
    }
    array.into()
}

pub(crate) fn list_to_js_at(doc: &AutoCommit, obj: &ObjId, heads: &[ChangeHash]) -> JsValue {
    let len = doc.length(obj);
    let array = Array::new();
    for i in 0..len {
        let val = doc.get_at(obj, i as usize, heads);
        match val {
            Ok(Some((Value::Object(o), exid)))
                if o == am::ObjType::Map || o == am::ObjType::Table =>
            {
                array.push(&map_to_js_at(doc, &exid, heads));
            }
            Ok(Some((Value::Object(o), exid))) if o == am::ObjType::List => {
                array.push(&list_to_js_at(doc, &exid, heads));
            }
            Ok(Some((Value::Object(o), exid))) if o == am::ObjType::Text => {
                array.push(&doc.text_at(exid, heads).unwrap().into());
            }
            Ok(Some((Value::Scalar(v), _))) => {
                array.push(&ScalarValue(v).into());
            }
            _ => (),
        };
    }
    array.into()
}

/*
pub(crate) fn export_values<'a, V: Iterator<Item = Value<'a>>>(val: V) -> Array {
  val.map(|v| export_value(&v)).collect()
}
*/

pub(crate) fn export_value(val: &Value<'_>) -> JsValue {
    match val {
        Value::Object(o) if o == &am::ObjType::Map || o == &am::ObjType::Table => {
            Object::new().into()
        }
        Value::Object(_) => Array::new().into(),
        Value::Scalar(v) => ScalarValue(v.clone()).into(),
    }
}

pub(crate) fn apply_patch(obj: JsValue, patch: &Patch) -> Result<JsValue, JsValue> {
    apply_patch2(obj, patch, 0)
}

pub(crate) fn apply_patch2(obj: JsValue, patch: &Patch, depth: usize) -> Result<JsValue, JsValue> {
    match (js_to_map_seq(&obj)?, patch.path().get(depth)) {
        (JsObj::Map(o), Some(Prop::Map(key))) => {
            let sub_obj = Reflect::get(&obj, &key.into())?;
            let new_value = apply_patch2(sub_obj, patch, depth + 1)?;
            let result =
                Reflect::construct(&o.constructor(), &Array::new())?.dyn_into::<Object>()?;
            let result = Object::assign(&result, &o).into();
            Reflect::set(&result, &key.into(), &new_value)?;
            Ok(result)
        }
        (JsObj::Seq(a), Some(Prop::Seq(index))) => {
            let index = JsValue::from_f64(*index as f64);
            let sub_obj = Reflect::get(&obj, &index)?;
            let new_value = apply_patch2(sub_obj, patch, depth + 1)?;
            let result = Reflect::construct(&a.constructor(), &a)?;
            //web_sys::console::log_2(&format!("NEW VAL {}: ", tmpi).into(), &new_value);
            Reflect::set(&result, &index, &new_value)?;
            Ok(result)
        }
        (JsObj::Map(o), None) => {
            let result =
                Reflect::construct(&o.constructor(), &Array::new())?.dyn_into::<Object>()?;
            let result = Object::assign(&result, &o);
            match patch {
                Patch::PutMap { key, value, .. } => {
                    let result = result.into();
                    Reflect::set(&result, &key.into(), &export_value(value))?;
                    Ok(result)
                }
                Patch::DeleteMap { key, .. } => {
                    Reflect::delete_property(&result, &key.into())?;
                    Ok(result.into())
                }
                Patch::Insert { .. } => Err(to_js_err("cannot insert into map")),
                Patch::DeleteSeq { .. } => Err(to_js_err("cannot splice a map")),
                Patch::PutSeq { .. } => Err(to_js_err("cannot array index a map")),
                _ => unimplemented!(),
            }
        }
        (JsObj::Seq(a), None) => {
            match patch {
                Patch::PutSeq { index, value, .. } => {
                    let result = Reflect::construct(&a.constructor(), &a)?;
                    Reflect::set(&result, &(*index as f64).into(), &export_value(value))?;
                    Ok(result)
                }
                Patch::DeleteSeq { index, .. } => {
                    let result = &a.dyn_into::<Array>()?;
                    let mut f = |_, i, _| i != *index as u32;
                    let result = result.filter(&mut f);

                    Ok(result.into())
                }
                Patch::Insert { index, values, .. } => {
                    let from = Reflect::get(&a.constructor().into(), &"from".into())?
                        .dyn_into::<Function>()?;
                    let result = from.call1(&JsValue::undefined(), &a)?.dyn_into::<Array>()?;
                    // FIXME: should be one function call
                    for v in values {
                        result.splice(*index as u32, 0, &export_value(v));
                    }
                    Ok(result.into())
                }
                Patch::DeleteMap { .. } => Err(to_js_err("cannot delete from a seq")),
                Patch::PutMap { .. } => Err(to_js_err("cannot set key in seq")),
                _ => unimplemented!(),
            }
        }
        (_, _) => Err(to_js_err(format!(
            "object/patch missmatch {:?} depth={:?}",
            patch, depth
        ))),
    }
}

#[derive(Debug)]
enum JsObj {
    Map(Object),
    Seq(Array),
}

fn js_to_map_seq(value: &JsValue) -> Result<JsObj, JsValue> {
    if let Ok(array) = value.clone().dyn_into::<Array>() {
        Ok(JsObj::Seq(array))
    } else if let Ok(obj) = value.clone().dyn_into::<Object>() {
        Ok(JsObj::Map(obj))
    } else {
        Err(to_js_err("obj is not Object or Array"))
    }
}

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/automerge/automerge-rs/main/img/brandmark.svg",
    html_favicon_url = "https:///raw.githubusercontent.com/automerge/automerge-rs/main/img/favicon.ico"
)]
#![warn(
    missing_debug_implementations,
    // missing_docs, // TODO: add documentation!
    rust_2021_compatibility,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    const_err,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    private_in_public,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]
#![allow(clippy::unused_unit)]
use am::transaction::CommitOptions;
use am::transaction::{Observed, Transactable, UnObserved};
use automerge as am;
use automerge::{Change, ObjId, ObjType, Prop, Value, ROOT};
use js_sys::{Array, Function, Object, Uint8Array};
use serde::ser::Serialize;
use std::collections::HashMap;
use std::convert::TryInto;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

mod interop;
mod observer;
mod sync;
mod value;

use observer::Observer;

use interop::{alloc, get_heads, js_get, js_set, to_js_err, to_objtype, to_prop, AR, JS};
use sync::SyncState;
use value::Datatype;

#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
          web_sys::console::log_1(&format!( $( $t )* ).into());
    };
}

type AutoCommit = am::AutoCommitWithObs<Observed<Observer>>;

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
#[derive(Debug)]
pub struct Automerge {
    doc: AutoCommit,
    freeze: bool,
    external_types: HashMap<Datatype, Function>,
}

#[wasm_bindgen]
impl Automerge {
    pub fn new(actor: Option<String>) -> Result<Automerge, JsValue> {
        let mut doc = AutoCommit::default();
        if let Some(a) = actor {
            let a = automerge::ActorId::from(hex::decode(a).map_err(to_js_err)?.to_vec());
            doc.set_actor(a);
        }
        Ok(Automerge {
            doc,
            freeze: false,
            external_types: HashMap::default(),
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&mut self, actor: Option<String>) -> Result<Automerge, JsValue> {
        let mut automerge = Automerge {
            doc: self.doc.clone(),
            freeze: self.freeze,
            external_types: self.external_types.clone(),
        };
        if let Some(s) = actor {
            let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
            automerge.doc.set_actor(actor);
        }
        Ok(automerge)
    }

    pub fn fork(&mut self, actor: Option<String>, heads: JsValue) -> Result<Automerge, JsValue> {
        let heads: Result<Vec<am::ChangeHash>, _> = JS(heads).try_into();
        let doc = if let Ok(heads) = heads {
            self.doc.fork_at(&heads)?
        } else {
            self.doc.fork()
        };
        let mut automerge = Automerge {
            doc,
            freeze: self.freeze,
            external_types: self.external_types.clone(),
        };
        if let Some(s) = actor {
            let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
            automerge.doc.set_actor(actor);
        }
        Ok(automerge)
    }

    #[wasm_bindgen(js_name = pendingOps)]
    pub fn pending_ops(&self) -> JsValue {
        (self.doc.pending_ops() as u32).into()
    }

    pub fn commit(&mut self, message: Option<String>, time: Option<f64>) -> JsValue {
        let mut commit_opts = CommitOptions::default();
        if let Some(message) = message {
            commit_opts.set_message(message);
        }
        if let Some(time) = time {
            commit_opts.set_time(time as i64);
        }
        let hash = self.doc.commit_with(commit_opts);
        match hash {
            Some(h) => JsValue::from_str(&hex::encode(h.0)),
            None => JsValue::NULL,
        }
    }

    pub fn merge(&mut self, other: &mut Automerge) -> Result<Array, JsValue> {
        let heads = self.doc.merge(&mut other.doc)?;
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(h.0)))
            .collect();
        Ok(heads)
    }

    pub fn rollback(&mut self) -> f64 {
        self.doc.rollback() as f64
    }

    pub fn keys(&self, obj: JsValue, heads: Option<Array>) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = if let Some(heads) = get_heads(heads) {
            self.doc
                .keys_at(&obj, &heads)
                .map(|s| JsValue::from_str(&s))
                .collect()
        } else {
            self.doc.keys(&obj).map(|s| JsValue::from_str(&s)).collect()
        };
        Ok(result)
    }

    pub fn text(&self, obj: JsValue, heads: Option<Array>) -> Result<String, JsValue> {
        let obj = self.import(obj)?;
        if let Some(heads) = get_heads(heads) {
            Ok(self.doc.text_at(&obj, &heads)?)
        } else {
            Ok(self.doc.text(&obj)?)
        }
    }

    pub fn splice(
        &mut self,
        obj: JsValue,
        start: f64,
        delete_count: f64,
        text: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let start = start as usize;
        let delete_count = delete_count as usize;
        let mut vals = vec![];
        if let Some(t) = text.as_string() {
            self.doc.splice_text(&obj, start, delete_count, &t)?;
        } else {
            if let Ok(array) = text.dyn_into::<Array>() {
                for i in array.iter() {
                    let value = self
                        .import_scalar(&i, &None)
                        .ok_or_else(|| to_js_err("expected scalar"))?;
                    vals.push(value);
                }
            }
            self.doc
                .splice(&obj, start, delete_count, vals.into_iter())?;
        }
        Ok(())
    }

    pub fn push(&mut self, obj: JsValue, value: JsValue, datatype: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("invalid scalar value"))?;
        let index = self.doc.length(&obj);
        self.doc.insert(&obj, index, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = pushObject)]
    pub fn push_object(&mut self, obj: JsValue, value: JsValue) -> Result<Option<String>, JsValue> {
        let obj = self.import(obj)?;
        let (value, subvals) =
            to_objtype(&value, &None).ok_or_else(|| to_js_err("expected object"))?;
        let index = self.doc.length(&obj);
        let opid = self.doc.insert_object(&obj, index, value)?;
        self.subset(&opid, subvals)?;
        Ok(opid.to_string().into())
    }

    pub fn insert(
        &mut self,
        obj: JsValue,
        index: f64,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let index = index as f64;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("expected scalar value"))?;
        self.doc.insert(&obj, index as usize, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = insertObject)]
    pub fn insert_object(
        &mut self,
        obj: JsValue,
        index: f64,
        value: JsValue,
    ) -> Result<Option<String>, JsValue> {
        let obj = self.import(obj)?;
        let index = index as f64;
        let (value, subvals) =
            to_objtype(&value, &None).ok_or_else(|| to_js_err("expected object"))?;
        let opid = self.doc.insert_object(&obj, index as usize, value)?;
        self.subset(&opid, subvals)?;
        Ok(opid.to_string().into())
    }

    pub fn put(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("expected scalar value"))?;
        self.doc.put(&obj, prop, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = putObject)]
    pub fn put_object(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let (value, subvals) =
            to_objtype(&value, &None).ok_or_else(|| to_js_err("expected object"))?;
        let opid = self.doc.put_object(&obj, prop, value)?;
        self.subset(&opid, subvals)?;
        Ok(opid.to_string().into())
    }

    fn subset(&mut self, obj: &am::ObjId, vals: Vec<(am::Prop, JsValue)>) -> Result<(), JsValue> {
        for (p, v) in vals {
            let (value, subvals) = self.import_value(&v, None)?;
            //let opid = self.0.set(id, p, value)?;
            let opid = match (p, value) {
                (Prop::Map(s), Value::Object(objtype)) => {
                    Some(self.doc.put_object(obj, s, objtype)?)
                }
                (Prop::Map(s), Value::Scalar(scalar)) => {
                    self.doc.put(obj, s, scalar.into_owned())?;
                    None
                }
                (Prop::Seq(i), Value::Object(objtype)) => {
                    Some(self.doc.insert_object(obj, i, objtype)?)
                }
                (Prop::Seq(i), Value::Scalar(scalar)) => {
                    self.doc.insert(obj, i, scalar.into_owned())?;
                    None
                }
            };
            if let Some(opid) = opid {
                self.subset(&opid, subvals)?;
            }
        }
        Ok(())
    }

    pub fn increment(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value: f64 = value
            .as_f64()
            .ok_or_else(|| to_js_err("increment needs a numeric value"))?;
        self.doc.increment(&obj, prop, value as i64)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = get)]
    pub fn get(
        &self,
        obj: JsValue,
        prop: JsValue,
        heads: Option<Array>,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let prop = to_prop(prop);
        let heads = get_heads(heads);
        if let Ok(prop) = prop {
            let value = if let Some(h) = heads {
                self.doc.get_at(&obj, prop, &h)?
            } else {
                self.doc.get(&obj, prop)?
            };
            if let Some((value, id)) = value {
                match alloc(&value) {
                    (datatype, js_value) if datatype.is_scalar() => Ok(js_value),
                    _ => Ok(id.to_string().into()),
                }
            } else {
                Ok(JsValue::undefined())
            }
        } else {
            Ok(JsValue::undefined())
        }
    }

    #[wasm_bindgen(js_name = getWithType)]
    pub fn get_with_type(
        &self,
        obj: JsValue,
        prop: JsValue,
        heads: Option<Array>,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let prop = to_prop(prop);
        let heads = get_heads(heads);
        if let Ok(prop) = prop {
            let value = if let Some(h) = heads {
                self.doc.get_at(&obj, prop, &h)?
            } else {
                self.doc.get(&obj, prop)?
            };
            if let Some(value) = value {
                match &value {
                    (Value::Object(obj_type), obj_id) => {
                        let result = Array::new();
                        result.push(&obj_type.to_string().into());
                        result.push(&obj_id.to_string().into());
                        Ok(result.into())
                    }
                    (Value::Scalar(_), _) => {
                        let result = Array::new();
                        let (datatype, value) = alloc(&value.0);
                        result.push(&datatype.into());
                        result.push(&value);
                        Ok(result.into())
                    }
                }
            } else {
                Ok(JsValue::null())
            }
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = getAll)]
    pub fn get_all(
        &self,
        obj: JsValue,
        arg: JsValue,
        heads: Option<Array>,
    ) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(arg);
        if let Ok(prop) = prop {
            let values = if let Some(heads) = get_heads(heads) {
                self.doc.get_all_at(&obj, prop, &heads)
            } else {
                self.doc.get_all(&obj, prop)
            }
            .map_err(to_js_err)?;
            for (value, id) in values {
                let sub = Array::new();
                let (datatype, js_value) = alloc(&value);
                sub.push(&datatype.into());
                if value.is_scalar() {
                    sub.push(&js_value);
                }
                sub.push(&id.to_string().into());
                result.push(&JsValue::from(&sub));
            }
        }
        Ok(result)
    }

    #[wasm_bindgen(js_name = enableFreeze)]
    pub fn enable_freeze(&mut self, enable: JsValue) -> Result<JsValue, JsValue> {
        let enable = enable
            .as_bool()
            .ok_or_else(|| to_js_err("must pass a bool to enableFreeze"))?;
        let old_freeze = self.freeze;
        self.freeze = enable;
        Ok(old_freeze.into())
    }

    #[wasm_bindgen(js_name = enablePatches)]
    pub fn enable_patches(&mut self, enable: JsValue) -> Result<JsValue, JsValue> {
        let enable = enable
            .as_bool()
            .ok_or_else(|| to_js_err("must pass a bool to enablePatches"))?;
        let old_enabled = self.doc.observer().enable(enable);
        Ok(old_enabled.into())
    }

    #[wasm_bindgen(js_name = registerDatatype)]
    pub fn register_datatype(
        &mut self,
        datatype: JsValue,
        function: JsValue,
    ) -> Result<(), JsValue> {
        let datatype = Datatype::try_from(datatype)?;
        if let Ok(function) = function.dyn_into::<Function>() {
            self.external_types.insert(datatype, function);
        } else {
            self.external_types.remove(&datatype);
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = applyPatches)]
    pub fn apply_patches(
        &mut self,
        object: JsValue,
        meta: JsValue,
        callback: JsValue,
    ) -> Result<JsValue, JsValue> {
        let mut object = object.dyn_into::<Object>()?;
        let patches = self.doc.observer().take_patches();
        let callback = callback.dyn_into::<Function>().ok();

        // even if there are no patches we may need to update the meta object
        // which requires that we update the object too
        if patches.is_empty() && !meta.is_undefined() {
            let (obj, datatype, id) = self.unwrap_object(&object)?;
            object = Object::assign(&Object::new(), &obj);
            object = self.wrap_object(object, datatype, &id, &meta)?;
        }

        for p in patches {
            if let Some(c) = &callback {
                let before = object.clone();
                object = self.apply_patch(object, &p, 0, &meta)?;
                c.call3(&JsValue::undefined(), &p.try_into()?, &before, &object)?;
            } else {
                object = self.apply_patch(object, &p, 0, &meta)?;
            }
        }

        Ok(object.into())
    }

    #[wasm_bindgen(js_name = popPatches)]
    pub fn pop_patches(&mut self) -> Result<Array, JsValue> {
        // transactions send out observer updates as they occur, not waiting for them to be
        // committed.
        // If we pop the patches then we won't be able to revert them.

        let patches = self.doc.observer().take_patches();
        let result = Array::new();
        for p in patches {
            result.push(&p.try_into()?);
        }
        Ok(result)
    }

    pub fn length(&self, obj: JsValue, heads: Option<Array>) -> Result<f64, JsValue> {
        let obj = self.import(obj)?;
        if let Some(heads) = get_heads(heads) {
            Ok(self.doc.length_at(&obj, &heads) as f64)
        } else {
            Ok(self.doc.length(&obj) as f64)
        }
    }

    pub fn delete(&mut self, obj: JsValue, prop: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = to_prop(prop)?;
        self.doc.delete(&obj, prop).map_err(to_js_err)?;
        Ok(())
    }

    pub fn save(&mut self) -> Uint8Array {
        Uint8Array::from(self.doc.save().as_slice())
    }

    #[wasm_bindgen(js_name = saveIncremental)]
    pub fn save_incremental(&mut self) -> Uint8Array {
        let bytes = self.doc.save_incremental();
        Uint8Array::from(bytes.as_slice())
    }

    #[wasm_bindgen(js_name = loadIncremental)]
    pub fn load_incremental(&mut self, data: Uint8Array) -> Result<f64, JsValue> {
        let data = data.to_vec();
        let len = self.doc.load_incremental(&data).map_err(to_js_err)?;
        Ok(len as f64)
    }

    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: JsValue) -> Result<(), JsValue> {
        let changes: Vec<_> = JS(changes).try_into()?;
        self.doc.apply_changes(changes).map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&mut self, have_deps: JsValue) -> Result<Array, JsValue> {
        let deps: Vec<_> = JS(have_deps).try_into()?;
        let changes = self.doc.get_changes(&deps)?;
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getChangeByHash)]
    pub fn get_change_by_hash(&mut self, hash: JsValue) -> Result<JsValue, JsValue> {
        let hash = serde_wasm_bindgen::from_value(hash).map_err(to_js_err)?;
        let change = self.doc.get_change_by_hash(&hash);
        if let Some(c) = change {
            Ok(Uint8Array::from(c.raw_bytes()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = getChangesAdded)]
    pub fn get_changes_added(&mut self, other: &mut Automerge) -> Result<Array, JsValue> {
        let changes = self.doc.get_changes_added(&mut other.doc);
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getHeads)]
    pub fn get_heads(&mut self) -> Array {
        let heads = self.doc.get_heads();
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(h.0)))
            .collect();
        heads
    }

    #[wasm_bindgen(js_name = getActorId)]
    pub fn get_actor_id(&self) -> String {
        let actor = self.doc.get_actor();
        actor.to_string()
    }

    #[wasm_bindgen(js_name = getLastLocalChange)]
    pub fn get_last_local_change(&mut self) -> Result<JsValue, JsValue> {
        if let Some(change) = self.doc.get_last_local_change() {
            Ok(Uint8Array::from(change.raw_bytes()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    pub fn dump(&mut self) {
        self.doc.dump()
    }

    #[wasm_bindgen(js_name = getMissingDeps)]
    pub fn get_missing_deps(&mut self, heads: Option<Array>) -> Result<Array, JsValue> {
        let heads = get_heads(heads).unwrap_or_default();
        let deps = self.doc.get_missing_deps(&heads);
        let deps: Array = deps
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(h.0)))
            .collect();
        Ok(deps)
    }

    #[wasm_bindgen(js_name = receiveSyncMessage)]
    pub fn receive_sync_message(
        &mut self,
        state: &mut SyncState,
        message: Uint8Array,
    ) -> Result<(), JsValue> {
        let message = message.to_vec();
        let message = am::sync::Message::decode(message.as_slice()).map_err(to_js_err)?;
        self.doc
            .receive_sync_message(&mut state.0, message)
            .map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = generateSyncMessage)]
    pub fn generate_sync_message(&mut self, state: &mut SyncState) -> Result<JsValue, JsValue> {
        if let Some(message) = self.doc.generate_sync_message(&mut state.0) {
            Ok(Uint8Array::from(message.encode().as_slice()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = toJS)]
    pub fn to_js(&mut self, meta: JsValue) -> Result<JsValue, JsValue> {
        self.export_object(&ROOT, Datatype::Map, None, &meta)
    }

    pub fn materialize(
        &mut self,
        obj: JsValue,
        heads: Option<Array>,
        meta: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj).unwrap_or(ROOT);
        let heads = get_heads(heads);
        let obj_type = self
            .doc
            .object_type(&obj)
            .ok_or_else(|| to_js_err(format!("invalid obj {}", obj)))?;
        let _patches = self.doc.observer().take_patches(); // throw away patches
        self.export_object(&obj, obj_type.into(), heads.as_ref(), &meta)
    }

    fn import(&self, id: JsValue) -> Result<ObjId, JsValue> {
        if let Some(s) = id.as_string() {
            if let Some(post) = s.strip_prefix('/') {
                let mut obj = ROOT;
                let mut is_map = true;
                let parts = post.split('/');
                for prop in parts {
                    if prop.is_empty() {
                        break;
                    }
                    let val = if is_map {
                        self.doc.get(obj, prop)?
                    } else {
                        self.doc.get(obj, am::Prop::Seq(prop.parse().unwrap()))?
                    };
                    match val {
                        Some((am::Value::Object(ObjType::Map), id)) => {
                            is_map = true;
                            obj = id;
                        }
                        Some((am::Value::Object(ObjType::Table), id)) => {
                            is_map = true;
                            obj = id;
                        }
                        Some((am::Value::Object(_), id)) => {
                            is_map = false;
                            obj = id;
                        }
                        None => return Err(to_js_err(format!("invalid path '{}'", s))),
                        _ => return Err(to_js_err(format!("path '{}' is not an object", s))),
                    };
                }
                Ok(obj)
            } else {
                Ok(self.doc.import(&s)?)
            }
        } else {
            Err(to_js_err("invalid objid"))
        }
    }

    fn import_prop(&self, prop: JsValue) -> Result<Prop, JsValue> {
        if let Some(s) = prop.as_string() {
            Ok(s.into())
        } else if let Some(n) = prop.as_f64() {
            Ok((n as usize).into())
        } else {
            Err(to_js_err(format!("invalid prop {:?}", prop)))
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
    ) -> Result<(Value<'static>, Vec<(Prop, JsValue)>), JsValue> {
        match self.import_scalar(value, &datatype) {
            Some(val) => Ok((val.into(), vec![])),
            None => {
                if let Some((o, subvals)) = to_objtype(value, &datatype) {
                    Ok((o.into(), subvals))
                } else {
                    web_sys::console::log_2(&"Invalid value".into(), value);
                    Err(to_js_err("invalid value"))
                }
            }
        }
    }

    #[wasm_bindgen(js_name = emptyChange)]
    pub fn empty_change(&mut self, message: Option<String>, time: Option<f64>) -> JsValue {
        let time = time.map(|f| f as i64);
        let options = CommitOptions { message, time };
        let hash = self.doc.empty_change(options);
        JsValue::from_str(&hex::encode(hash))
    }
}

#[wasm_bindgen(js_name = create)]
pub fn init(actor: Option<String>) -> Result<Automerge, JsValue> {
    console_error_panic_hook::set_once();
    Automerge::new(actor)
}

#[wasm_bindgen(js_name = load)]
pub fn load(data: Uint8Array, actor: Option<String>) -> Result<Automerge, JsValue> {
    let data = data.to_vec();
    let mut doc = am::AutoCommitWithObs::<UnObserved>::load(&data)
        .map_err(to_js_err)?
        .with_observer(Observer::default());
    if let Some(s) = actor {
        let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
        doc.set_actor(actor);
    }
    Ok(Automerge {
        doc,
        freeze: false,
        external_types: HashMap::default(),
    })
}

#[wasm_bindgen(js_name = encodeChange)]
pub fn encode_change(change: JsValue) -> Result<Uint8Array, JsValue> {
    // Alex: Technically we should be using serde_wasm_bindgen::from_value instead of into_serde.
    // Unfortunately serde_wasm_bindgen::from_value fails for some inscrutable reason, so instead
    // we use into_serde (sorry to future me).
    #[allow(deprecated)]
    let change: am::ExpandedChange = change.into_serde().map_err(to_js_err)?;
    let change: Change = change.into();
    Ok(Uint8Array::from(change.raw_bytes()))
}

#[wasm_bindgen(js_name = decodeChange)]
pub fn decode_change(change: Uint8Array) -> Result<JsValue, JsValue> {
    let change = Change::from_bytes(change.to_vec()).map_err(to_js_err)?;
    let change: am::ExpandedChange = change.decode();
    let serializer = serde_wasm_bindgen::Serializer::json_compatible();
    change.serialize(&serializer).map_err(to_js_err)
}

#[wasm_bindgen(js_name = initSyncState)]
pub fn init_sync_state() -> SyncState {
    SyncState(am::sync::State::new())
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = importSyncState)]
pub fn import_sync_state(state: JsValue) -> Result<SyncState, JsValue> {
    Ok(SyncState(JS(state).try_into()?))
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = exportSyncState)]
pub fn export_sync_state(state: &SyncState) -> JsValue {
    JS::from(state.0.clone()).into()
}

#[wasm_bindgen(js_name = encodeSyncMessage)]
pub fn encode_sync_message(message: JsValue) -> Result<Uint8Array, JsValue> {
    let heads = js_get(&message, "heads")?.try_into()?;
    let need = js_get(&message, "need")?.try_into()?;
    let changes = js_get(&message, "changes")?.try_into()?;
    let have = js_get(&message, "have")?.try_into()?;
    Ok(Uint8Array::from(
        am::sync::Message {
            heads,
            need,
            have,
            changes,
        }
        .encode()
        .as_slice(),
    ))
}

#[wasm_bindgen(js_name = decodeSyncMessage)]
pub fn decode_sync_message(msg: Uint8Array) -> Result<JsValue, JsValue> {
    let data = msg.to_vec();
    let msg = am::sync::Message::decode(&data).map_err(to_js_err)?;
    let heads = AR::from(msg.heads.as_slice());
    let need = AR::from(msg.need.as_slice());
    let changes = AR::from(msg.changes.as_slice());
    let have = AR::from(msg.have.as_slice());
    let obj = Object::new().into();
    js_set(&obj, "heads", heads)?;
    js_set(&obj, "need", need)?;
    js_set(&obj, "have", have)?;
    js_set(&obj, "changes", changes)?;
    Ok(obj)
}

#[wasm_bindgen(js_name = encodeSyncState)]
pub fn encode_sync_state(state: &SyncState) -> Result<Uint8Array, JsValue> {
    //let state = state.0.clone();
    Ok(Uint8Array::from(state.0.encode().as_slice()))
}

#[wasm_bindgen(js_name = decodeSyncState)]
pub fn decode_sync_state(data: Uint8Array) -> Result<SyncState, JsValue> {
    SyncState::decode(data)
}

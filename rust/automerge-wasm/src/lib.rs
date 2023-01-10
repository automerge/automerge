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
use automerge::{Change, ObjId, Prop, TextEncoding, Value, ROOT};
use js_sys::{Array, Function, Object, Uint8Array};
use serde::ser::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

mod interop;
mod observer;
mod sync;
mod value;

use observer::Observer;

use interop::{alloc, get_heads, import_obj, js_set, to_js_err, to_prop, AR, JS};
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
    pub fn new(actor: Option<String>) -> Result<Automerge, error::BadActorId> {
        let mut doc = AutoCommit::default().with_encoding(TextEncoding::Utf16);
        if let Some(a) = actor {
            let a = automerge::ActorId::from(hex::decode(a)?.to_vec());
            doc.set_actor(a);
        }
        Ok(Automerge {
            doc,
            freeze: false,
            external_types: HashMap::default(),
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&mut self, actor: Option<String>) -> Result<Automerge, error::BadActorId> {
        let mut automerge = Automerge {
            doc: self.doc.clone(),
            freeze: self.freeze,
            external_types: self.external_types.clone(),
        };
        if let Some(s) = actor {
            let actor = automerge::ActorId::from(hex::decode(s)?.to_vec());
            automerge.doc.set_actor(actor);
        }
        Ok(automerge)
    }

    pub fn fork(
        &mut self,
        actor: Option<String>,
        heads: JsValue,
    ) -> Result<Automerge, error::Fork> {
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
            let actor =
                automerge::ActorId::from(hex::decode(s).map_err(error::BadActorId::from)?.to_vec());
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

    pub fn merge(&mut self, other: &mut Automerge) -> Result<Array, error::Merge> {
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

    pub fn keys(&self, obj: JsValue, heads: Option<Array>) -> Result<Array, error::Get> {
        let (obj, _) = self.import(obj)?;
        let result = if let Some(heads) = get_heads(heads)? {
            self.doc
                .keys_at(&obj, &heads)
                .map(|s| JsValue::from_str(&s))
                .collect()
        } else {
            self.doc.keys(&obj).map(|s| JsValue::from_str(&s)).collect()
        };
        Ok(result)
    }

    pub fn text(&self, obj: JsValue, heads: Option<Array>) -> Result<String, error::Get> {
        let (obj, _) = self.import(obj)?;
        if let Some(heads) = get_heads(heads)? {
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
    ) -> Result<(), error::Splice> {
        let (obj, obj_type) = self.import(obj)?;
        let start = start as usize;
        let delete_count = delete_count as usize;
        if let Some(t) = text.as_string() {
            if obj_type == am::ObjType::Text {
                self.doc.splice_text(&obj, start, delete_count, &t)?;
                return Ok(());
            }
        }
        let mut vals = vec![];
        if let Ok(array) = text.dyn_into::<Array>() {
            for (index, i) in array.iter().enumerate() {
                let value = self
                    .import_scalar(&i, &None)
                    .ok_or(error::Splice::ValueNotPrimitive(index))?;
                vals.push(value);
            }
        }
        if !vals.is_empty() {
            self.doc.splice(&obj, start, delete_count, vals)?;
        } else {
            // no vals given but we still need to call the text vs splice
            // bc utf16
            match obj_type {
                am::ObjType::List => {
                    self.doc.splice(&obj, start, delete_count, vals)?;
                }
                am::ObjType::Text => {
                    self.doc.splice_text(&obj, start, delete_count, "")?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn push(
        &mut self,
        obj: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), error::Insert> {
        let (obj, _) = self.import(obj)?;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or(error::Insert::ValueNotPrimitive)?;
        let index = self.doc.length(&obj);
        self.doc.insert(&obj, index, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = pushObject)]
    pub fn push_object(
        &mut self,
        obj: JsValue,
        value: JsValue,
    ) -> Result<Option<String>, error::InsertObject> {
        let (obj, _) = self.import(obj)?;
        let imported_obj = import_obj(&value, &None)?;
        let index = self.doc.length(&obj);
        let opid = self
            .doc
            .insert_object(&obj, index, imported_obj.objtype())?;
        if let Some(s) = imported_obj.text() {
            self.doc.splice_text(&opid, 0, 0, s)?;
        } else {
            self.subset::<error::InsertObject>(&opid, imported_obj.subvals())?;
        }
        Ok(opid.to_string().into())
    }

    pub fn insert(
        &mut self,
        obj: JsValue,
        index: f64,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), error::Insert> {
        let (obj, _) = self.import(obj)?;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or(error::Insert::ValueNotPrimitive)?;
        self.doc.insert(&obj, index as usize, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = insertObject)]
    pub fn insert_object(
        &mut self,
        obj: JsValue,
        index: f64,
        value: JsValue,
    ) -> Result<Option<String>, error::InsertObject> {
        let (obj, _) = self.import(obj)?;
        let imported_obj = import_obj(&value, &None)?;
        let opid = self
            .doc
            .insert_object(&obj, index as usize, imported_obj.objtype())?;
        if let Some(s) = imported_obj.text() {
            self.doc.splice_text(&opid, 0, 0, s)?;
        } else {
            self.subset::<error::InsertObject>(&opid, imported_obj.subvals())?;
        }
        Ok(opid.to_string().into())
    }

    pub fn put(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), error::Insert> {
        let (obj, _) = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or(error::Insert::ValueNotPrimitive)?;
        self.doc.put(&obj, prop, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = putObject)]
    pub fn put_object(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
    ) -> Result<JsValue, error::InsertObject> {
        let (obj, _) = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let imported_obj = import_obj(&value, &None)?;
        let opid = self.doc.put_object(&obj, prop, imported_obj.objtype())?;
        if let Some(s) = imported_obj.text() {
            self.doc.splice_text(&opid, 0, 0, s)?;
        } else {
            self.subset::<error::InsertObject>(&opid, imported_obj.subvals())?;
        }
        Ok(opid.to_string().into())
    }

    fn subset<E>(&mut self, obj: &am::ObjId, vals: &[(am::Prop, JsValue)]) -> Result<(), E>
    where
        E: From<automerge::AutomergeError>
            + From<interop::error::ImportObj>
            + From<interop::error::InvalidValue>,
    {
        for (p, v) in vals {
            let (value, subvals) = self.import_value(v, None)?;
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
                    Some(self.doc.insert_object(obj, *i, objtype)?)
                }
                (Prop::Seq(i), Value::Scalar(scalar)) => {
                    self.doc.insert(obj, *i, scalar.into_owned())?;
                    None
                }
            };
            if let Some(opid) = opid {
                self.subset::<E>(&opid, &subvals)?;
            }
        }
        Ok(())
    }

    pub fn increment(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
    ) -> Result<(), error::Increment> {
        let (obj, _) = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value: f64 = value.as_f64().ok_or(error::Increment::ValueNotNumeric)?;
        self.doc.increment(&obj, prop, value as i64)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = get)]
    pub fn get(
        &self,
        obj: JsValue,
        prop: JsValue,
        heads: Option<Array>,
    ) -> Result<JsValue, error::Get> {
        let (obj, _) = self.import(obj)?;
        let prop = to_prop(prop);
        let heads = get_heads(heads)?;
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
    ) -> Result<JsValue, error::Get> {
        let (obj, _) = self.import(obj)?;
        let prop = to_prop(prop);
        let heads = get_heads(heads)?;
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
    ) -> Result<Array, error::Get> {
        let (obj, _) = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(arg);
        if let Ok(prop) = prop {
            let values = if let Some(heads) = get_heads(heads)? {
                self.doc.get_all_at(&obj, prop, &heads)
            } else {
                self.doc.get_all(&obj, prop)
            }?;
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
    ) -> Result<(), value::InvalidDatatype> {
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
    ) -> Result<JsValue, error::ApplyPatch> {
        let mut object = object
            .dyn_into::<Object>()
            .map_err(|_| error::ApplyPatch::NotObjectd)?;
        let patches = self.doc.observer().take_patches();
        let callback = callback.dyn_into::<Function>().ok();

        // even if there are no patches we may need to update the meta object
        // which requires that we update the object too
        if patches.is_empty() && !meta.is_undefined() {
            let (obj, datatype, id) = self.unwrap_object(&object)?;
            object = Object::assign(&Object::new(), &obj);
            object = self.wrap_object(object, datatype, &id, &meta)?;
        }

        let mut exposed = HashSet::default();

        let before = object.clone();

        for p in &patches {
            object = self.apply_patch(object, p, 0, &meta, &mut exposed)?;
        }

        if let Some(c) = &callback {
            if !patches.is_empty() {
                let patches: Array = patches
                    .into_iter()
                    .map(JsValue::try_from)
                    .collect::<Result<_, _>>()?;
                c.call3(&JsValue::undefined(), &patches.into(), &before, &object)
                    .map_err(error::ApplyPatch::PatchCallback)?;
            }
        }

        self.finalize_exposed(&object, exposed, &meta)?;

        Ok(object.into())
    }

    #[wasm_bindgen(js_name = popPatches)]
    pub fn pop_patches(&mut self) -> Result<Array, error::PopPatches> {
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

    pub fn length(&self, obj: JsValue, heads: Option<Array>) -> Result<f64, error::Get> {
        let (obj, _) = self.import(obj)?;
        if let Some(heads) = get_heads(heads)? {
            Ok(self.doc.length_at(&obj, &heads) as f64)
        } else {
            Ok(self.doc.length(&obj) as f64)
        }
    }

    pub fn delete(&mut self, obj: JsValue, prop: JsValue) -> Result<(), error::Get> {
        let (obj, _) = self.import(obj)?;
        let prop = to_prop(prop)?;
        self.doc.delete(&obj, prop)?;
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
    pub fn load_incremental(&mut self, data: Uint8Array) -> Result<f64, error::Load> {
        let data = data.to_vec();
        let len = self.doc.load_incremental(&data)?;
        Ok(len as f64)
    }

    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: JsValue) -> Result<(), error::ApplyChangesError> {
        let changes: Vec<_> = JS(changes).try_into()?;
        self.doc.apply_changes(changes)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&mut self, have_deps: JsValue) -> Result<Array, error::Get> {
        let deps: Vec<_> = JS(have_deps).try_into()?;
        let changes = self.doc.get_changes(&deps)?;
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getChangeByHash)]
    pub fn get_change_by_hash(
        &mut self,
        hash: JsValue,
    ) -> Result<JsValue, interop::error::BadChangeHash> {
        let hash = JS(hash).try_into()?;
        let change = self.doc.get_change_by_hash(&hash);
        if let Some(c) = change {
            Ok(Uint8Array::from(c.raw_bytes()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = getChangesAdded)]
    pub fn get_changes_added(&mut self, other: &mut Automerge) -> Array {
        let changes = self.doc.get_changes_added(&mut other.doc);
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        changes
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
    pub fn get_last_local_change(&mut self) -> JsValue {
        if let Some(change) = self.doc.get_last_local_change() {
            Uint8Array::from(change.raw_bytes()).into()
        } else {
            JsValue::null()
        }
    }

    pub fn dump(&mut self) {
        self.doc.dump()
    }

    #[wasm_bindgen(js_name = getMissingDeps)]
    pub fn get_missing_deps(&mut self, heads: Option<Array>) -> Result<Array, error::Get> {
        let heads = get_heads(heads)?.unwrap_or_default();
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
    ) -> Result<(), error::ReceiveSyncMessage> {
        let message = message.to_vec();
        let message = am::sync::Message::decode(message.as_slice())?;
        self.doc.receive_sync_message(&mut state.0, message)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = generateSyncMessage)]
    pub fn generate_sync_message(&mut self, state: &mut SyncState) -> JsValue {
        if let Some(message) = self.doc.generate_sync_message(&mut state.0) {
            Uint8Array::from(message.encode().as_slice()).into()
        } else {
            JsValue::null()
        }
    }

    #[wasm_bindgen(js_name = toJS)]
    pub fn to_js(&mut self, meta: JsValue) -> Result<JsValue, interop::error::Export> {
        self.export_object(&ROOT, Datatype::Map, None, &meta)
    }

    pub fn materialize(
        &mut self,
        obj: JsValue,
        heads: Option<Array>,
        meta: JsValue,
    ) -> Result<JsValue, error::Materialize> {
        let (obj, obj_type) = self.import(obj).unwrap_or((ROOT, am::ObjType::Map));
        let heads = get_heads(heads)?;
        let _patches = self.doc.observer().take_patches(); // throw away patches
        Ok(self.export_object(&obj, obj_type.into(), heads.as_ref(), &meta)?)
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
pub fn init(actor: Option<String>) -> Result<Automerge, error::BadActorId> {
    console_error_panic_hook::set_once();
    Automerge::new(actor)
}

#[wasm_bindgen(js_name = load)]
pub fn load(data: Uint8Array, actor: Option<String>) -> Result<Automerge, error::Load> {
    let data = data.to_vec();
    let mut doc = am::AutoCommitWithObs::<UnObserved>::load(&data)?
        .with_observer(Observer::default())
        .with_encoding(TextEncoding::Utf16);
    if let Some(s) = actor {
        let actor =
            automerge::ActorId::from(hex::decode(s).map_err(error::BadActorId::from)?.to_vec());
        doc.set_actor(actor);
    }
    Ok(Automerge {
        doc,
        freeze: false,
        external_types: HashMap::default(),
    })
}

#[wasm_bindgen(js_name = encodeChange)]
pub fn encode_change(change: JsValue) -> Result<Uint8Array, error::EncodeChange> {
    // Alex: Technically we should be using serde_wasm_bindgen::from_value instead of into_serde.
    // Unfortunately serde_wasm_bindgen::from_value fails for some inscrutable reason, so instead
    // we use into_serde (sorry to future me).
    #[allow(deprecated)]
    let change: am::ExpandedChange = change.into_serde()?;
    let change: Change = change.into();
    Ok(Uint8Array::from(change.raw_bytes()))
}

#[wasm_bindgen(js_name = decodeChange)]
pub fn decode_change(change: Uint8Array) -> Result<JsValue, error::DecodeChange> {
    let change = Change::from_bytes(change.to_vec())?;
    let change: am::ExpandedChange = change.decode();
    let serializer = serde_wasm_bindgen::Serializer::json_compatible();
    Ok(change.serialize(&serializer)?)
}

#[wasm_bindgen(js_name = initSyncState)]
pub fn init_sync_state() -> SyncState {
    SyncState(am::sync::State::new())
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = importSyncState)]
pub fn import_sync_state(state: JsValue) -> Result<SyncState, interop::error::BadSyncState> {
    Ok(SyncState(JS(state).try_into()?))
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = exportSyncState)]
pub fn export_sync_state(state: &SyncState) -> JsValue {
    JS::from(state.0.clone()).into()
}

#[wasm_bindgen(js_name = encodeSyncMessage)]
pub fn encode_sync_message(message: JsValue) -> Result<Uint8Array, interop::error::BadSyncMessage> {
    let message: am::sync::Message = JS(message).try_into()?;
    Ok(Uint8Array::from(message.encode().as_slice()))
}

#[wasm_bindgen(js_name = decodeSyncMessage)]
pub fn decode_sync_message(msg: Uint8Array) -> Result<JsValue, error::BadSyncMessage> {
    let data = msg.to_vec();
    let msg = am::sync::Message::decode(&data)?;
    let heads = AR::from(msg.heads.as_slice());
    let need = AR::from(msg.need.as_slice());
    let changes = AR::from(msg.changes.as_slice());
    let have = AR::from(msg.have.as_slice());
    let obj = Object::new().into();
    // SAFETY: we just created this object
    js_set(&obj, "heads", heads).unwrap();
    js_set(&obj, "need", need).unwrap();
    js_set(&obj, "have", have).unwrap();
    js_set(&obj, "changes", changes).unwrap();
    Ok(obj)
}

#[wasm_bindgen(js_name = encodeSyncState)]
pub fn encode_sync_state(state: &SyncState) -> Uint8Array {
    Uint8Array::from(state.0.encode().as_slice())
}

#[wasm_bindgen(js_name = decodeSyncState)]
pub fn decode_sync_state(data: Uint8Array) -> Result<SyncState, sync::DecodeSyncStateErr> {
    SyncState::decode(data)
}

pub mod error {
    use automerge::AutomergeError;
    use wasm_bindgen::JsValue;

    use crate::interop::{
        self,
        error::{BadChangeHashes, BadJSChanges},
    };

    #[derive(Debug, thiserror::Error)]
    #[error("could not parse Actor ID as a hex string: {0}")]
    pub struct BadActorId(#[from] hex::FromHexError);

    impl From<BadActorId> for JsValue {
        fn from(s: BadActorId) -> Self {
            JsValue::from(s.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ApplyChangesError {
        #[error(transparent)]
        DecodeChanges(#[from] BadJSChanges),
        #[error("error applying changes: {0}")]
        Apply(#[from] AutomergeError),
    }

    impl From<ApplyChangesError> for JsValue {
        fn from(e: ApplyChangesError) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Fork {
        #[error(transparent)]
        BadActor(#[from] BadActorId),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        BadChangeHashes(#[from] BadChangeHashes),
    }

    impl From<Fork> for JsValue {
        fn from(f: Fork) -> Self {
            JsValue::from(f.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error(transparent)]
    pub struct Merge(#[from] AutomergeError);

    impl From<Merge> for JsValue {
        fn from(e: Merge) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Get {
        #[error("invalid object ID: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error("bad heads: {0}")]
        BadHeads(#[from] interop::error::BadChangeHashes),
        #[error(transparent)]
        InvalidProp(#[from] interop::error::InvalidProp),
    }

    impl From<Get> for JsValue {
        fn from(e: Get) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Splice {
        #[error("invalid object ID: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error("value at {0} in values to insert was not a primitive")]
        ValueNotPrimitive(usize),
    }

    impl From<Splice> for JsValue {
        fn from(e: Splice) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Insert {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error("the value to insert was not a primitive")]
        ValueNotPrimitive,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        InvalidProp(#[from] interop::error::InvalidProp),
        #[error(transparent)]
        InvalidValue(#[from] interop::error::InvalidValue),
    }

    impl From<Insert> for JsValue {
        fn from(e: Insert) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum InsertObject {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error("the value to insert must be an object")]
        ValueNotObject,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        InvalidProp(#[from] interop::error::InvalidProp),
        #[error(transparent)]
        InvalidValue(#[from] interop::error::InvalidValue),
    }

    impl From<InsertObject> for JsValue {
        fn from(e: InsertObject) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Increment {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        InvalidProp(#[from] interop::error::InvalidProp),
        #[error("value was not numeric")]
        ValueNotNumeric,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
    }

    impl From<Increment> for JsValue {
        fn from(e: Increment) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadSyncMessage {
        #[error("could not decode sync message: {0}")]
        ReadMessage(#[from] automerge::sync::ReadMessageError),
    }

    impl From<BadSyncMessage> for JsValue {
        fn from(e: BadSyncMessage) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ApplyPatch {
        #[error(transparent)]
        Interop(#[from] interop::error::ApplyPatch),
        #[error(transparent)]
        Export(#[from] interop::error::Export),
        #[error("patch was not an object")]
        NotObjectd,
        #[error("error calling patch callback: {0:?}")]
        PatchCallback(JsValue),
    }

    impl From<ApplyPatch> for JsValue {
        fn from(e: ApplyPatch) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("unable to build patches: {0}")]
    pub struct PopPatches(#[from] interop::error::Export);

    impl From<PopPatches> for JsValue {
        fn from(e: PopPatches) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Materialize {
        #[error(transparent)]
        Export(#[from] interop::error::Export),
        #[error("bad heads: {0}")]
        Heads(#[from] interop::error::BadChangeHashes),
    }

    impl From<Materialize> for JsValue {
        fn from(e: Materialize) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ReceiveSyncMessage {
        #[error(transparent)]
        Decode(#[from] automerge::sync::ReadMessageError),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
    }

    impl From<ReceiveSyncMessage> for JsValue {
        fn from(e: ReceiveSyncMessage) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Load {
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        BadActor(#[from] BadActorId),
    }

    impl From<Load> for JsValue {
        fn from(e: Load) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("Unable to read JS change: {0}")]
    pub struct EncodeChange(#[from] serde_json::Error);

    impl From<EncodeChange> for JsValue {
        fn from(e: EncodeChange) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum DecodeChange {
        #[error(transparent)]
        Load(#[from] automerge::LoadChangeError),
        #[error(transparent)]
        Serialize(#[from] serde_wasm_bindgen::Error),
    }

    impl From<DecodeChange> for JsValue {
        fn from(e: DecodeChange) -> Self {
            JsValue::from(e.to_string())
        }
    }
}

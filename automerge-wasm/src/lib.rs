#![allow(clippy::unused_unit)]
use am::transaction::CommitOptions;
use am::transaction::Transactable;
use automerge as am;
use automerge::{Change, ObjId, Prop, Value, ROOT};
use js_sys::{Array, Object, Uint8Array};
use std::convert::TryInto;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

mod interop;
mod sync;
mod value;

use interop::{get_heads, js_get, js_set, map_to_js, to_js_err, to_objtype, to_prop, AR, JS};
use sync::SyncState;
use value::{datatype, ScalarValue};

#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
          web_sys::console::log_1(&format!( $( $t )* ).into());
    };
}

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
#[derive(Debug)]
pub struct Automerge(automerge::AutoCommit);

#[wasm_bindgen]
impl Automerge {
    pub fn new(actor: Option<String>) -> Result<Automerge, JsValue> {
        let mut automerge = automerge::AutoCommit::new();
        if let Some(a) = actor {
            let a = automerge::ActorId::from(hex::decode(a).map_err(to_js_err)?.to_vec());
            automerge.set_actor(a);
        }
        Ok(Automerge(automerge))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&mut self, actor: Option<String>) -> Result<Automerge, JsValue> {
        if self.0.pending_ops() > 0 {
            self.0.commit();
        }
        let mut automerge = Automerge(self.0.clone());
        if let Some(s) = actor {
            let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
            automerge.0.set_actor(actor);
        }
        Ok(automerge)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn fork(&mut self, actor: Option<String>) -> Result<Automerge, JsValue> {
        let mut automerge = Automerge(self.0.fork());
        if let Some(s) = actor {
            let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
            automerge.0.set_actor(actor);
        }
        Ok(automerge)
    }

    pub fn free(self) {}

    #[wasm_bindgen(js_name = pendingOps)]
    pub fn pending_ops(&self) -> JsValue {
        (self.0.pending_ops() as u32).into()
    }

    pub fn commit(&mut self, message: Option<String>, time: Option<f64>) -> Array {
        let mut commit_opts = CommitOptions::default();
        if let Some(message) = message {
            commit_opts.set_message(message);
        }
        if let Some(time) = time {
            commit_opts.set_time(time as i64);
        }
        let heads = self.0.commit_with(commit_opts);
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        heads
    }

    pub fn merge(&mut self, other: &mut Automerge) -> Result<Array, JsValue> {
        let heads = self.0.merge(&mut other.0)?;
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        Ok(heads)
    }

    pub fn rollback(&mut self) -> f64 {
        self.0.rollback() as f64
    }

    pub fn keys(&mut self, obj: JsValue, heads: Option<Array>) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = if let Some(heads) = get_heads(heads) {
            self.0
                .keys_at(&obj, &heads)
                .map(|s| JsValue::from_str(&s))
                .collect()
        } else {
            self.0.keys(&obj).map(|s| JsValue::from_str(&s)).collect()
        };
        Ok(result)
    }

    pub fn text(&mut self, obj: JsValue, heads: Option<Array>) -> Result<String, JsValue> {
        let obj = self.import(obj)?;
        if let Some(heads) = get_heads(heads) {
            Ok(self.0.text_at(&obj, &heads)?)
        } else {
            Ok(self.0.text(&obj)?)
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
            self.0.splice_text(&obj, start, delete_count, &t)?;
        } else {
            if let Ok(array) = text.dyn_into::<Array>() {
                for i in array.iter() {
                    let value = self
                        .import_scalar(&i, &None)
                        .ok_or_else(|| to_js_err("expected scalar"))?;
                    vals.push(value);
                }
            }
            self.0.splice(&obj, start, delete_count, vals)?;
        }
        Ok(())
    }

    pub fn push(&mut self, obj: JsValue, value: JsValue, datatype: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("invalid scalar value"))?;
        let index = self.0.length(&obj);
        self.0.insert(&obj, index, value)?;
        Ok(())
    }

    pub fn push_object(
        &mut self,
        obj: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<Option<String>, JsValue> {
        let obj = self.import(obj)?;
        let (value, subvals) = to_objtype(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("expected object"))?;
        let index = self.0.length(&obj);
        let opid = self.0.insert_object(&obj, index, value)?;
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
        self.0.insert(&obj, index as usize, value)?;
        Ok(())
    }

    pub fn insert_object(
        &mut self,
        obj: JsValue,
        index: f64,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<Option<String>, JsValue> {
        let obj = self.import(obj)?;
        let index = index as f64;
        let (value, subvals) = to_objtype(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("expected object"))?;
        let opid = self.0.insert_object(&obj, index as usize, value)?;
        self.subset(&opid, subvals)?;
        Ok(opid.to_string().into())
    }

    pub fn set(
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
        self.0.set(&obj, prop, value)?;
        Ok(())
    }

    pub fn set_object(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let (value, subvals) = to_objtype(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("expected object"))?;
        let opid = self.0.set_object(&obj, prop, value)?;
        self.subset(&opid, subvals)?;
        Ok(opid.to_string().into())
    }

    fn subset(&mut self, obj: &am::ObjId, vals: Vec<(am::Prop, JsValue)>) -> Result<(), JsValue> {
        for (p, v) in vals {
            let (value, subvals) = self.import_value(&v, None)?;
            //let opid = self.0.set(id, p, value)?;
            let opid = match (p, value) {
                (Prop::Map(s), Value::Object(objtype)) => Some(self.0.set_object(obj, s, objtype)?),
                (Prop::Map(s), Value::Scalar(scalar)) => {
                    self.0.set(obj, s, scalar)?;
                    None
                }
                (Prop::Seq(i), Value::Object(objtype)) => {
                    Some(self.0.insert_object(obj, i, objtype)?)
                }
                (Prop::Seq(i), Value::Scalar(scalar)) => {
                    self.0.insert(obj, i, scalar)?;
                    None
                }
            };
            if let Some(opid) = opid {
                self.subset(&opid, subvals)?;
            }
        }
        Ok(())
    }

    pub fn inc(&mut self, obj: JsValue, prop: JsValue, value: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value: f64 = value
            .as_f64()
            .ok_or_else(|| to_js_err("inc needs a numberic value"))?;
        self.0.inc(&obj, prop, value as i64)?;
        Ok(())
    }

    pub fn value(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        heads: Option<Array>,
    ) -> Result<Option<Array>, JsValue> {
        let obj = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(prop);
        let heads = get_heads(heads);
        if let Ok(prop) = prop {
            let value = if let Some(h) = heads {
                self.0.value_at(&obj, prop, &h)?
            } else {
                self.0.value(&obj, prop)?
            };
            match value {
                Some((Value::Object(obj_type), obj_id)) => {
                    result.push(&obj_type.to_string().into());
                    result.push(&obj_id.to_string().into());
                    Ok(Some(result))
                }
                Some((Value::Scalar(value), _)) => {
                    result.push(&datatype(&value).into());
                    result.push(&ScalarValue(value).into());
                    Ok(Some(result))
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub fn values(
        &mut self,
        obj: JsValue,
        arg: JsValue,
        heads: Option<Array>,
    ) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(arg);
        if let Ok(prop) = prop {
            let values = if let Some(heads) = get_heads(heads) {
                self.0.values_at(&obj, prop, &heads)
            } else {
                self.0.values(&obj, prop)
            }
            .map_err(to_js_err)?;
            for value in values {
                match value {
                    (Value::Object(obj_type), obj_id) => {
                        let sub = Array::new();
                        sub.push(&obj_type.to_string().into());
                        sub.push(&obj_id.to_string().into());
                        result.push(&sub.into());
                    }
                    (Value::Scalar(value), id) => {
                        let sub = Array::new();
                        sub.push(&datatype(&value).into());
                        sub.push(&ScalarValue(value).into());
                        sub.push(&id.to_string().into());
                        result.push(&sub.into());
                    }
                }
            }
        }
        Ok(result)
    }

    pub fn length(&mut self, obj: JsValue, heads: Option<Array>) -> Result<f64, JsValue> {
        let obj = self.import(obj)?;
        if let Some(heads) = get_heads(heads) {
            Ok(self.0.length_at(&obj, &heads) as f64)
        } else {
            Ok(self.0.length(&obj) as f64)
        }
    }

    pub fn del(&mut self, obj: JsValue, prop: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = to_prop(prop)?;
        self.0.del(&obj, prop).map_err(to_js_err)?;
        Ok(())
    }

    pub fn save(&mut self) -> Result<Uint8Array, JsValue> {
        self.0
            .save()
            .map(|v| Uint8Array::from(v.as_slice()))
            .map_err(to_js_err)
    }

    #[wasm_bindgen(js_name = saveIncremental)]
    pub fn save_incremental(&mut self) -> Uint8Array {
        let bytes = self.0.save_incremental();
        Uint8Array::from(bytes.as_slice())
    }

    #[wasm_bindgen(js_name = loadIncremental)]
    pub fn load_incremental(&mut self, data: Uint8Array) -> Result<f64, JsValue> {
        let data = data.to_vec();
        let len = self.0.load_incremental(&data).map_err(to_js_err)?;
        Ok(len as f64)
    }

    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: JsValue) -> Result<(), JsValue> {
        let changes: Vec<_> = JS(changes).try_into()?;
        self.0.apply_changes(&changes).map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&mut self, have_deps: JsValue) -> Result<Array, JsValue> {
        let deps: Vec<_> = JS(have_deps).try_into()?;
        let changes = self.0.get_changes(&deps);
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getChangesAdded)]
    pub fn get_changes_added(&mut self, other: &mut Automerge) -> Result<Array, JsValue> {
        let changes = self.0.get_changes_added(&mut other.0);
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getHeads)]
    pub fn get_heads(&mut self) -> Array {
        let heads = self.0.get_heads();
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        heads
    }

    #[wasm_bindgen(js_name = getActorId)]
    pub fn get_actor_id(&mut self) -> String {
        let actor = self.0.get_actor();
        actor.to_string()
    }

    #[wasm_bindgen(js_name = getLastLocalChange)]
    pub fn get_last_local_change(&mut self) -> Result<Uint8Array, JsValue> {
        if let Some(change) = self.0.get_last_local_change() {
            Ok(Uint8Array::from(change.raw_bytes()))
        } else {
            Err(to_js_err("no local changes"))
        }
    }

    pub fn dump(&self) {
        self.0.dump()
    }

    #[wasm_bindgen(js_name = getMissingDeps)]
    pub fn get_missing_deps(&mut self, heads: Option<Array>) -> Result<Array, JsValue> {
        let heads = get_heads(heads).unwrap_or_default();
        let deps = self.0.get_missing_deps(&heads);
        let deps: Array = deps
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
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
        let message = am::SyncMessage::decode(message.as_slice()).map_err(to_js_err)?;
        self.0
            .receive_sync_message(&mut state.0, message)
            .map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = generateSyncMessage)]
    pub fn generate_sync_message(&mut self, state: &mut SyncState) -> Result<JsValue, JsValue> {
        if let Some(message) = self.0.generate_sync_message(&mut state.0) {
            Ok(Uint8Array::from(message.encode().map_err(to_js_err)?.as_slice()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = toJS)]
    pub fn to_js(&self) -> JsValue {
        map_to_js(&self.0, &ROOT)
    }

    fn import(&self, id: JsValue) -> Result<ObjId, JsValue> {
        if let Some(s) = id.as_string() {
            Ok(self.0.import(&s)?)
        } else {
            Err(to_js_err("invalid objid"))
        }
    }

    fn import_prop(&mut self, prop: JsValue) -> Result<Prop, JsValue> {
        if let Some(s) = prop.as_string() {
            Ok(s.into())
        } else if let Some(n) = prop.as_f64() {
            Ok((n as usize).into())
        } else {
            Err(to_js_err(format!("invalid prop {:?}", prop)))
        }
    }

    fn import_scalar(
        &mut self,
        value: &JsValue,
        datatype: &Option<String>,
    ) -> Option<am::ScalarValue> {
        match datatype.as_deref() {
            Some("boolean") => value.as_bool().map(am::ScalarValue::Boolean),
            Some("int") => value.as_f64().map(|v| am::ScalarValue::Int(v as i64)),
            Some("uint") => value.as_f64().map(|v| am::ScalarValue::Uint(v as u64)),
            Some("f64") => value.as_f64().map(am::ScalarValue::F64),
            Some("bytes") => Some(am::ScalarValue::Bytes(
                value.clone().dyn_into::<Uint8Array>().unwrap().to_vec(),
            )),
            Some("counter") => value.as_f64().map(|v| am::ScalarValue::counter(v as i64)),
            Some("timestamp") => value.as_f64().map(|v| am::ScalarValue::Timestamp(v as i64)),
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
        &mut self,
        value: &JsValue,
        datatype: Option<String>,
    ) -> Result<(Value, Vec<(Prop, JsValue)>), JsValue> {
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
}

#[wasm_bindgen(js_name = create)]
pub fn init(actor: Option<String>) -> Result<Automerge, JsValue> {
    console_error_panic_hook::set_once();
    Automerge::new(actor)
}

#[wasm_bindgen(js_name = loadDoc)]
pub fn load(data: Uint8Array, actor: Option<String>) -> Result<Automerge, JsValue> {
    let data = data.to_vec();
    let mut automerge = am::AutoCommit::load(&data).map_err(to_js_err)?;
    if let Some(s) = actor {
        let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
        automerge.set_actor(actor);
    }
    Ok(Automerge(automerge))
}

#[wasm_bindgen(js_name = encodeChange)]
pub fn encode_change(change: JsValue) -> Result<Uint8Array, JsValue> {
    let change: am::ExpandedChange = change.into_serde().map_err(to_js_err)?;
    let change: Change = change.into();
    Ok(Uint8Array::from(change.raw_bytes()))
}

#[wasm_bindgen(js_name = decodeChange)]
pub fn decode_change(change: Uint8Array) -> Result<JsValue, JsValue> {
    let change = Change::from_bytes(change.to_vec()).map_err(to_js_err)?;
    let change: am::ExpandedChange = change.decode();
    JsValue::from_serde(&change).map_err(to_js_err)
}

#[wasm_bindgen(js_name = initSyncState)]
pub fn init_sync_state() -> SyncState {
    SyncState(am::SyncState::new())
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = importSyncState)]
pub fn import_sync_state(state: JsValue) -> Result<SyncState, JsValue> {
    Ok(SyncState(JS(state).try_into()?))
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = exportSyncState)]
pub fn export_sync_state(state: SyncState) -> JsValue {
    JS::from(state.0).into()
}

#[wasm_bindgen(js_name = encodeSyncMessage)]
pub fn encode_sync_message(message: JsValue) -> Result<Uint8Array, JsValue> {
    let heads = js_get(&message, "heads")?.try_into()?;
    let need = js_get(&message, "need")?.try_into()?;
    let changes = js_get(&message, "changes")?.try_into()?;
    let have = js_get(&message, "have")?.try_into()?;
    Ok(Uint8Array::from(
        am::SyncMessage {
            heads,
            need,
            have,
            changes,
        }
        .encode()
        .unwrap()
        .as_slice(),
    ))
}

#[wasm_bindgen(js_name = decodeSyncMessage)]
pub fn decode_sync_message(msg: Uint8Array) -> Result<JsValue, JsValue> {
    let data = msg.to_vec();
    let msg = am::SyncMessage::decode(&data).map_err(to_js_err)?;
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
pub fn encode_sync_state(state: SyncState) -> Result<Uint8Array, JsValue> {
    let state = state.0;
    Ok(Uint8Array::from(
        state.encode().map_err(to_js_err)?.as_slice(),
    ))
}

#[wasm_bindgen(js_name = decodeSyncState)]
pub fn decode_sync_state(data: Uint8Array) -> Result<SyncState, JsValue> {
    SyncState::decode(data)
}

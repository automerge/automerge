#![allow(unused_variables)]
use automerge as am;
use automerge::{Prop, Value};
use js_sys::{Array, Uint8Array};
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::Display;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
extern crate web_sys;

#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
          web_sys::console::log_1(&format!( $( $t )* ).into());
    };
}

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

fn datatype(s: &am::ScalarValue) -> String {
    match s {
        am::ScalarValue::Bytes(_) => "bytes".into(),
        am::ScalarValue::Str(_) => "str".into(),
        am::ScalarValue::Int(_) => "int".into(),
        am::ScalarValue::Uint(_) => "uint".into(),
        am::ScalarValue::F64(_) => "f64".into(),
        am::ScalarValue::Counter(_) => "counter".into(),
        am::ScalarValue::Timestamp(_) => "timestamp".into(),
        am::ScalarValue::Boolean(_) => "boolean".into(),
        am::ScalarValue::Null => "null".into(),
    }
}

#[derive(Debug)]
pub struct ScalarValue(am::ScalarValue);

impl From<ScalarValue> for JsValue {
    fn from(val: ScalarValue) -> Self {
        match &val.0 {
            am::ScalarValue::Bytes(v) => js_sys::Uint8Array::from(v.as_slice()).into(),
            am::ScalarValue::Str(v) => v.to_string().into(),
            am::ScalarValue::Int(v) => (*v as f64).into(),
            am::ScalarValue::Uint(v) => (*v as f64).into(),
            am::ScalarValue::F64(v) => (*v).into(),
            am::ScalarValue::Counter(v) => (*v as f64).into(),
            am::ScalarValue::Timestamp(v) => (*v as f64).into(),
            am::ScalarValue::Boolean(v) => (*v).into(),
            am::ScalarValue::Null => JsValue::null(),
        }
    }
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct Automerge(automerge::Automerge);

#[derive(Debug)]
pub struct JsErr(String);

impl From<JsErr> for JsValue {
    fn from(err: JsErr) -> Self {
        js_sys::Error::new(&std::format!("{}", err.0)).into()
    }
}

impl<'a> From<&'a str> for JsErr {
    fn from(s: &'a str) -> Self {
        JsErr(s.to_owned())
    }
}

#[wasm_bindgen]
impl Automerge {
    pub fn new(actor: JsValue) -> Result<Automerge, JsValue> {
        let mut automerge = automerge::Automerge::new();
        if let Some(a) = actor.as_string() {
            let a = automerge::ActorId::from(hex::decode(a).map_err(to_js_err)?.to_vec());
            automerge.set_actor(a);
        }
        Ok(Automerge(automerge))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        Automerge(self.0.clone())
    }

    pub fn free(self) {}

    pub fn pending_ops(&self) -> JsValue {
        (self.0.pending_ops() as u32).into()
    }

    pub fn commit(&mut self, message: JsValue, time: JsValue) -> JsValue {
        let message = message.as_string();
        let time = time.as_f64().map(|v| v as i64);
        self.0.commit(message, time).into()
    }

    pub fn rollback(&mut self) -> JsValue {
        self.0.rollback().into()
    }

    pub fn keys(&mut self, obj: JsValue) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = self
            .0
            .keys(obj)
            .iter()
            .map(|s| JsValue::from_str(s))
            .collect();
        Ok(result)
    }

    pub fn text(&mut self, obj: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        self.0.text(obj).map_err(to_js_err).map(|t| t.into())
    }

    pub fn splice(
        &mut self,
        obj: JsValue,
        start: JsValue,
        delete_count: JsValue,
        text: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let start = to_usize(start, "start")?;
        let delete_count = to_usize(delete_count, "deleteCount")?;
        let mut vals = vec![];
        if let Some(t) = text.as_string() {
            self.0
                .splice_text(obj, start, delete_count, &t)
                .map_err(to_js_err)?;
        } else {
            if let Ok(array) = text.dyn_into::<Array>() {
                for i in array.iter() {
                    if let Some(t) = i.as_string() {
                        vals.push(t.into());
                    } else if let Ok(array) = i.dyn_into::<Array>() {
                        let value = array.get(1);
                        let datatype = array.get(2);
                        let value = self.import_value(value, datatype)?;
                        vals.push(value);
                    }
                }
            }
            self.0
                .splice(obj, start, delete_count, vals)
                .map_err(to_js_err)?;
        }
        Ok(())
    }

    pub fn insert(
        &mut self,
        obj: JsValue,
        index: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        //let key = self.insert_pos_for_index(&obj, prop)?;
        let index: Result<_, JsValue> = index
            .as_f64()
            .ok_or_else(|| "insert index must be a number".into());
        let index = index?;
        let value = self.import_value(value, datatype)?;
        let opid = self
            .0
            .insert(obj, index as usize, value)
            .map_err(to_js_err)?;
        Ok(self.export(opid))
    }

    pub fn set(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value = self.import_value(value, datatype)?;
        let opid = self.0.set(obj, prop, value).map_err(to_js_err)?;
        Ok(self.export(opid))
    }

    pub fn inc(&mut self, obj: JsValue, prop: JsValue, value: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value: f64 = value
            .as_f64()
            .ok_or("inc needs a numberic value")
            .map_err(to_js_err)?;
        self.0.inc(obj, prop, value as i64).map_err(to_js_err)?;
        Ok(())
    }

    pub fn value(&mut self, obj: JsValue, arg: JsValue) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(arg);
        if let Ok(prop) = prop {
            let value = self.0.value(obj, prop).map_err(to_js_err)?;
            match value {
                Some((Value::Object(obj_type), obj_id)) => {
                    result.push(&obj_type.to_string().into());
                    result.push(&self.export(obj_id));
                }
                Some((Value::Scalar(value), _)) => {
                    result.push(&datatype(&value).into());
                    result.push(&ScalarValue(value).into());
                }
                None => {}
            }
        }
        Ok(result)
    }

    pub fn values(&mut self, obj: JsValue, arg: JsValue) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(arg);
        if let Ok(prop) = prop {
            let values = self.0.values(obj, prop).map_err(to_js_err)?;
            for value in values {
                match value {
                    (Value::Object(obj_type), obj_id) => {
                        let sub = Array::new();
                        sub.push(&obj_type.to_string().into());
                        sub.push(&self.export(obj_id));
                        result.push(&sub.into());
                    }
                    (Value::Scalar(value), id) => {
                        let sub = Array::new();
                        sub.push(&datatype(&value).into());
                        sub.push(&ScalarValue(value).into());
                        sub.push(&self.export(id));
                        result.push(&sub.into());
                    }
                }
            }
        }
        Ok(result)
    }

    pub fn length(&mut self, obj: JsValue, arg: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let len = self.0.length(obj) as f64;
        Ok(len.into())
    }

    pub fn del(&mut self, obj: JsValue, prop: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = to_prop(prop)?;
        self.0.del(obj, prop).map_err(to_js_err)?;
        Ok(())
    }

    pub fn save(&mut self) -> Result<Uint8Array, JsValue> {
        self.0
            .save()
            .map(|v| js_sys::Uint8Array::from(v.as_slice()))
            .map_err(to_js_err)
    }

    #[wasm_bindgen(js_name = saveIncremental)]
    pub fn save_incremental(&mut self) -> JsValue {
        let bytes = self.0.save_incremental();
        js_sys::Uint8Array::from(bytes.as_slice()).into()
    }

    #[wasm_bindgen(js_name = loadIncremental)]
    pub fn load_incremental(&mut self, data: Uint8Array) -> Result<JsValue,JsValue> {
        let data = data.to_vec();
        let len = self.0.load_incremental(&data).map_err(to_js_err)?;
        Ok(len.into())
    }

    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: Array) -> Result<(), JsValue> {
        let deps: Result<Vec<js_sys::Uint8Array>, _> =
            changes.iter().map(|j| j.dyn_into()).collect();
        let deps = deps?;
        let deps: Result<Vec<am::Change>, _> =
            deps.iter().map(|a| am::decode_change(a.to_vec())).collect();
        let deps = deps.map_err(to_js_err)?;
        self.0.apply_changes(deps.as_ref()).map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&mut self, have_deps: Array) -> Result<Array, JsValue> {
        let deps: Result<Vec<am::ChangeHash>, _> =
            have_deps.iter().map(|j| j.into_serde()).collect();
        let deps = deps.map_err(to_js_err)?;
        let changes = self.0.get_changes(&deps);
        let changes: Array = changes
            .iter()
            .map(|c| js_sys::Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getChangesAdded)]
    pub fn get_changes_added(&mut self, other: &Automerge) -> Result<Array, JsValue> {
        let changes = self.0.get_changes_added(&other.0);
        let changes: Array = changes
            .iter()
            .map(|c| js_sys::Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getHeads)]
    pub fn get_heads(&mut self) -> Result<Array, JsValue> {
        let heads = self.0.get_heads();
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        Ok(heads)
    }

    #[wasm_bindgen(js_name = getActorId)]
    pub fn get_actor_id(&mut self) -> Result<JsValue, JsValue> {
        let actor = self.0.get_actor();
        Ok(actor.to_string().into())
    }

    #[wasm_bindgen(js_name = getLastLocalChange)]
    pub fn get_last_local_change(&mut self) -> Result<JsValue, JsValue> {
        if let Some(change) = self.0.get_last_local_change() {
            Ok(js_sys::Uint8Array::from(change.raw_bytes()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    pub fn dump(&self) {
        self.0.dump()
    }

    #[wasm_bindgen(js_name = getMissingDeps)]
    pub fn get_missing_deps(&mut self, heads: Array) -> Result<Array, JsValue> {
        let heads: Result<Vec<am::ChangeHash>, _> = heads.iter().map(|j| j.into_serde()).collect();
        let heads = heads.map_err(to_js_err)?;
        let deps = self.0.get_missing_deps(&heads);
        let deps: Array = deps
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        Ok(deps)
    }

    fn export<E: automerge::Exportable>(&self, val: E) -> JsValue {
        self.0.export(val).into()
    }

    fn import<I: automerge::Importable>(&self, id: JsValue) -> Result<I, JsValue> {
        let id_str = id
            .as_string()
            .ok_or("invalid opid/objid/elemid")
            .map_err(to_js_err)?;
        self.0.import(&id_str).map_err(to_js_err)
    }

    fn import_prop(&mut self, prop: JsValue) -> Result<Prop, JsValue> {
        if let Some(s) = prop.as_string() {
            Ok(s.into())
        } else if let Some(n) = prop.as_f64() {
            Ok((n as usize).into())
        } else {
            Err(format!("invalid prop {:?}", prop).into())
        }
    }

    fn import_value(&mut self, value: JsValue, datatype: JsValue) -> Result<Value, JsValue> {
        let datatype = datatype.as_string();
        match datatype.as_deref() {
            Some("boolean") => value
                .as_bool()
                .ok_or_else(|| "value must be a bool".into())
                .map(|v| am::ScalarValue::Boolean(v).into()),
            Some("int") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Int(v as i64).into()),
            Some("uint") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Uint(v as u64).into()),
            Some("f64") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|n| am::ScalarValue::F64(n).into()),
            Some("bytes") => {
                Ok(am::ScalarValue::Bytes(value.dyn_into::<Uint8Array>().unwrap().to_vec()).into())
            }
            Some("counter") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Counter(v as i64).into()),
            Some("timestamp") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Timestamp(v as i64).into()),
            /*
            Some("bytes") => unimplemented!(),
            Some("cursor") => unimplemented!(),
            */
            Some("null") => Ok(am::ScalarValue::Null.into()),
            Some(_) => Err(format!("unknown datatype {:?}", datatype).into()),
            None => {
                if value.is_null() {
                    Ok(am::ScalarValue::Null.into())
                } else if let Some(s) = value.as_string() {
                    // FIXME - we need to detect str vs int vs float vs bool here :/
                    Ok(am::ScalarValue::Str(s.into()).into())
                } else if let Some(n) = value.as_f64() {
                    if (n.round() - n).abs() < f64::EPSILON {
                        Ok(am::ScalarValue::Int(n as i64).into())
                    } else {
                        Ok(am::ScalarValue::F64(n).into())
                    }
                } else if let Some(o) = to_objtype(&value) {
                    Ok(o.into())
                } else if let Ok(o) = &value.dyn_into::<Uint8Array>() {
                    Ok(am::ScalarValue::Bytes(o.to_vec()).into())
                } else {
                    Err("value is invalid".into())
                }
            }
        }
    }
}

pub fn to_usize(val: JsValue, name: &str) -> Result<usize, JsValue> {
    match val.as_f64() {
        Some(n) => Ok(n as usize),
        None => Err(format!("{} must be a number", name).into()),
    }
}

pub fn to_prop(p: JsValue) -> Result<Prop, JsValue> {
    if let Some(s) = p.as_string() {
        Ok(Prop::Map(s))
    } else if let Some(n) = p.as_f64() {
        Ok(Prop::Seq(n as usize))
    } else {
        Err("prop must me a string or number".into())
    }
}

fn to_objtype(a: &JsValue) -> Option<am::ObjType> {
    if !a.is_function() {
        return None;
    }
    let f: js_sys::Function = a.clone().try_into().unwrap();
    let f = f.to_string();
    if f.starts_with("class MAP", 0) {
        Some(am::ObjType::Map)
    } else if f.starts_with("class LIST", 0) {
        Some(am::ObjType::List)
    } else if f.starts_with("class TEXT", 0) {
        Some(am::ObjType::Text)
    } else if f.starts_with("class TABLE", 0) {
        Some(am::ObjType::Table)
    } else {
        None
    }
}

struct ObjType(am::ObjType);

impl TryFrom<JsValue> for ObjType {
    type Error = JsValue;

    fn try_from(val: JsValue) -> Result<Self, Self::Error> {
        match &val.as_string() {
            Some(o) if o == "map" => Ok(ObjType(am::ObjType::Map)),
            Some(o) if o == "list" => Ok(ObjType(am::ObjType::List)),
            Some(o) => Err(format!("unknown obj type {}", o).into()),
            _ => Err("obj type must be a string".into()),
        }
    }
}

#[wasm_bindgen]
pub fn init(actor: JsValue) -> Result<Automerge, JsValue> {
    console_error_panic_hook::set_once();
    Automerge::new(actor)
}

#[wasm_bindgen]
pub fn load(data: Uint8Array, actor: JsValue) -> Result<Automerge, JsValue> {
    let data = data.to_vec();
    let mut automerge = am::Automerge::load(&data).map_err(to_js_err)?;
    if let Some(s) = actor.as_string() {
        let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
        automerge.set_actor(actor)
    }
    Ok(Automerge(automerge))
}

#[wasm_bindgen(js_name = encodeChange)]
pub fn encode_change(change: JsValue) -> Result<Uint8Array, JsValue> {
    let change: am::ExpandedChange = change.into_serde().map_err(to_js_err)?;
    let change: am::Change = change.into();
    Ok(js_sys::Uint8Array::from(change.raw_bytes()))
}

#[wasm_bindgen(js_name = decodeChange)]
pub fn decode_change(change: Uint8Array) -> Result<JsValue, JsValue> {
    let change = am::Change::from_bytes(change.to_vec()).map_err(to_js_err)?;
    let change: am::ExpandedChange = change.decode();
    JsValue::from_serde(&change).map_err(to_js_err)
}

#[wasm_bindgen(js_name = encodeDocument)]
pub fn encode_document(document: JsValue) -> Result<Uint8Array, JsValue> {
    unimplemented!()
}

#[wasm_bindgen(js_name = decodeDocument)]
pub fn decode_document(document: Uint8Array) -> Result<JsValue, JsValue> {
    unimplemented!()
}

#[wasm_bindgen(js_name = encodeSyncMessage)]
pub fn encode_sync_message(message: JsValue) -> Result<Uint8Array, JsValue> {
    unimplemented!()
}

#[wasm_bindgen(js_name = decodeSyncMessage)]
pub fn decode_sync_message(document: Uint8Array) -> Result<JsValue, JsValue> {
    unimplemented!()
}

#[wasm_bindgen(js_name = encodeSyncState)]
pub fn encode_sync_state(document: JsValue) -> Result<Uint8Array, JsValue> {
    unimplemented!()
}

#[wasm_bindgen(js_name = decodeSyncState)]
pub fn decode_sync_state(document: Uint8Array) -> Result<JsValue, JsValue> {
    unimplemented!()
}

#[wasm_bindgen(js_name = MAP)]
pub struct Map {}

#[wasm_bindgen(js_name = LIST)]
pub struct List {}

#[wasm_bindgen(js_name = TEXT)]
pub struct Text {}

#[wasm_bindgen(js_name = TABLE)]
pub struct Table {}

fn to_js_err<T: Display>(err: T) -> JsValue {
    js_sys::Error::new(&std::format!("{}", err)).into()
}

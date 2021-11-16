//#![feature(set_stdio)]

#![allow(unused_variables)]
use automerge as am;
use automerge::{Key, Value};
use js_sys::{Array, Uint8Array};
use wasm_bindgen::JsCast;
//use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Display;
use std::convert::TryFrom;
use std::convert::TryInto;
use wasm_bindgen::prelude::*;
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
        am::ScalarValue::Cursor(_) => "cursor".into(),
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
            am::ScalarValue::Cursor(_) => unimplemented!(),
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
        let actor = match actor.as_string() {
            Some(a) => automerge::ActorId::from(hex::decode(a).map_err(to_js_err)?.to_vec()),
            _ => automerge::ActorId::from(uuid::Uuid::new_v4().as_bytes().to_vec()),
        };
        //let actor = automerge::ActorId::from(uuid::Uuid::new_v4().as_bytes().to_vec());
        let mut automerge = automerge::Automerge::new();
        automerge.set_actor(actor);
        Ok(Automerge(automerge))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        Automerge(self.0.clone())
    }

    pub fn free(self) {}

    pub fn begin(&mut self, message: JsValue, time: JsValue) -> Result<(), JsValue> {
        let message = message.as_string();
        let time = time.as_f64().map(|v| v as i64);
        self.0.begin(message, time).map_err(to_js_err)
    }

    pub fn pending_ops(&self) -> JsValue {
        (self.0.pending_ops() as u32).into()
    }

    pub fn commit(&mut self) -> Result<(), JsValue> {
        self.0.commit().map_err(to_js_err)
    }

    pub fn rollback(&mut self) {
        self.0.rollback();
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

    pub fn make(&mut self, obj: JsValue, prop: JsValue, obj_type: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let key = self.prop_to_key(prop)?;
        let ObjType(obj_type) = &obj_type.try_into()?;
        let obj = self
            .0
            .make(obj, key, *obj_type, false)
            .map_err(to_js_err)?;
        Ok(self.export(obj))
    }

    #[wasm_bindgen(js_name = makeAt)]
    pub fn make_at(&mut self, obj: JsValue, prop: JsValue, obj_type: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let ObjType(obj_type) = &obj_type.try_into()?;
        let len = self.0.list_length(&obj);
        if prop.as_f64().unwrap_or_default() as usize == len {
            let key = self.insert_pos_for_index(&obj, prop)?;
            let id = self.0.make(obj, key, *obj_type, true).map_err(to_js_err)?;
            Ok(self.export(id))
        } else {
            let key = self.set_pos_for_index(&obj, prop)?;
            let id = self.0.make(obj, key, *obj_type, false).map_err(to_js_err)?;
            Ok(self.export(id))
        }
    }

    #[wasm_bindgen(js_name = insertMakeAt)]
    pub fn insert_make_at(&mut self, obj: JsValue, prop: JsValue, obj_type: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let ObjType(obj_type) = &obj_type.try_into()?;
        let len = self.0.list_length(&obj);
        let key = self.insert_pos_for_index(&obj, prop)?;
        let id = self.0.make(obj, key, *obj_type, true).map_err(to_js_err)?;
        Ok(self.export(id))
    }

    pub fn keys(&mut self, obj: JsValue) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = self.0.keys(&obj).iter().map(|k| self.export(*k)).collect();
        Ok(result)
    }

    fn prop_to_key(&mut self, prop: JsValue) -> Result<Key, JsValue> {
        let prop = prop.as_string();
        if prop.is_none() {
            return Err("prop must be a valid string".into());
        }
        let prop = prop.unwrap();
        let key = self.0.prop_to_key(prop).map_err(to_js_err)?;
        Ok(key)
    }

    fn insert_pos_for_index(
        &mut self,
        obj: &am::ObjId,
        index: JsValue,
    ) -> Result<am::Key, JsValue> {
        let index = index.as_f64();
        if index.is_none() {
            return Err("list index must be a number".into());
        }
        let index = index.unwrap() as usize;
        let key = self
            .0
            .insert_pos_for_index(obj, index)
            .ok_or_else(|| JsErr("index is out of bounds".into()))?;
        Ok(key)
    }

    fn set_pos_for_index(&mut self, obj: &am::ObjId, index: JsValue) -> Result<am::Key, JsValue> {
        let index = index.as_f64();
        if index.is_none() {
            return Err("list index must be a number".into());
        }
        let index = index.unwrap() as usize;
        let key = self
            .0
            .set_pos_for_index(obj, index)
            .ok_or_else(|| JsErr("index is out of bounds".into()))?;
        Ok(key)
    }

    pub fn splice(&mut self, obj: JsValue, start: JsValue, delete_count: JsValue, values: Array) -> Result<(),JsValue> {
        let obj = self.import(obj)?;
        let start = to_usize(start,"start")?;
        let delete_count = to_usize(delete_count,"deleteCount")?;
        for i in 0..delete_count {
            log!("DELETE");
            self.0.del(obj,start.into()).map_err(to_js_err)?;
        }
        for i in values.entries() {
            log!("VAL {:?}",i);
        }
        unimplemented!()
        //Ok(())
    }

    pub fn insert(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let key = self.insert_pos_for_index(&obj, prop)?;
        self.do_set(obj, key, value, datatype, true)
    }

    #[wasm_bindgen(js_name = setAt)]
    pub fn set_at(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let len = self.0.list_length(&obj);
        if prop.as_f64().unwrap_or_default() as usize == len {
            let key = self.insert_pos_for_index(&obj, prop)?;
            self.do_set(obj, key, value, datatype, true)
        } else {
            let key = self.set_pos_for_index(&obj, prop)?;
            self.do_set(obj, key, value, datatype, false)
        }
    }

    pub fn set(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let key = self.prop_to_key(prop)?;
        self.do_set(obj, key, value, datatype, false)
    }

    fn do_set(
        &mut self,
        obj: am::ObjId,
        key: Key,
        value: JsValue,
        datatype: JsValue,
        insert: bool,
    ) -> Result<JsValue, JsValue> {
        let datatype = datatype.as_string();
        let value = match datatype.as_deref() {
            Some("boolean") => value
                .as_bool()
                .ok_or_else(|| JsErr("value must be a bool".into()))
                .map(am::ScalarValue::Boolean),
            Some("int") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Int(v as i64)),
            Some("uint") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Uint(v as u64)),
            Some("f64") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(am::ScalarValue::F64),
            Some("bytes") => {
                Ok(am::ScalarValue::Bytes(
                    value.dyn_into::<Uint8Array>().unwrap().to_vec(),
                ))
            }
            Some("counter") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Counter(v as i64)),
            Some("timestamp") => value
                .as_f64()
                .ok_or_else(|| "value must be a number".into())
                .map(|v| am::ScalarValue::Timestamp(v as i64)),
            /*
            Some("bytes") => unimplemented!(),
            Some("cursor") => unimplemented!(),
            */
            Some("null") => Ok(am::ScalarValue::Null),
            Some(_) => Err(JsErr(format!("unknown datatype {:?}", datatype))),
            None => {
                if value.is_null() {
                    Ok(am::ScalarValue::Null)
                } else if let Some(s) = value.as_string() {
                    // FIXME - we need to detect str vs int vs float vs bool here :/
                    Ok(am::ScalarValue::Str(s.into()))
                } else {
                    Err("value is invalid".into())
                }
            }
        }?;
        let opid = self.0.set(obj, key, value, insert).map_err(to_js_err)?;
        Ok(self.export(opid))
    }

    pub fn value(&mut self, obj: JsValue, arg: JsValue) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let prop = arg.as_string();
        let index = arg.as_f64().map(|v| v as usize);
        let result = Array::new();
        let values = match (index, prop) {
            (Some(n), _) => Ok(self.0.list_value(&obj, n)),
            (_, Some(p)) => Ok(self.0.map_value(&obj, &p)),
            _ => Err(JsErr("prop must be a string or number".into())),
        }?;
        //        let value = self.0.map_value(&obj, &prop);

        match values.get(0) {
            Some(Value::Object(obj_type, obj_id)) => {
                result.push(&obj_type.to_string().into());
                result.push(&self.export(*obj_id));
            }
            Some(Value::Scalar(value, _)) => {
                result.push(&datatype(&value).into());
                result.push(&ScalarValue(value.clone()).into());
            }
            None => {}
        }
        Ok(result)
    }

    pub fn conflicts(&mut self, obj: JsValue, arg: JsValue) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let prop = arg.as_string();
        let index = arg.as_f64().map(|v| v as usize);
        //            .ok_or(JsErr("prop must be a string".into()))?;
        let result = Array::new();
        let values = match (index, prop) {
            (Some(n), _) => Ok(self.0.list_value(&obj, n)),
            (_, Some(p)) => Ok(self.0.map_value(&obj, &p)),
            _ => Err(JsErr("prop must be a string or number".into())),
        }?;
        //        let value = self.0.map_value(&obj, &prop);
       
        for value in values {
            match value {
                Value::Object(obj_type, obj_id) => {
                    let sub = Array::new();
                    sub.push(&obj_type.to_string().into());
                    sub.push(&self.export(obj_id));
                    result.push(&sub.into());
                }
                Value::Scalar(value, id) => {
                    let sub = Array::new();
                    sub.push(&datatype(&value).into());
                    sub.push(&ScalarValue(value).into());
                    sub.push(&self.export(id));
                    result.push(&sub.into());
                }
            }
        }
        Ok(result)
    }

    pub fn length(&mut self, obj: JsValue, arg: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let len = self.0.list_length(&obj) as f64;
        Ok(len.into())
    }

    pub fn del(&mut self, obj: JsValue, prop: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        //let key = self.prop_to_key(prop)?;
        if let Some(s) = prop.as_string() {
            self.0.del(obj, s.into()).map_err(to_js_err)
        } else if let Some(n) = prop.as_f64() {
            self.0.del(obj, n.into()).map_err(to_js_err)
        } else {
            return Err(format!("invalid property {:?}",prop).into())
        }
    }

    pub fn save(&self) -> Result<Uint8Array, JsValue> {
        self.0
            .save()
            .map(|v| js_sys::Uint8Array::from(v.as_slice()))
            .map_err(to_js_err)
    }

    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: Array) -> Result<(), JsValue> {
        let deps: Result<Vec<js_sys::Uint8Array>,_> = changes.iter().map(|j| j.dyn_into()).collect();
        let deps = deps?;
        let deps: Result<Vec<am::Change>,_> = deps.iter().map(|a| am::decode_change(a.to_vec())).collect();
        let deps = deps.map_err(to_js_err)?;
        self.0.apply_changes(deps.as_ref()).map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&mut self, have_deps: Array) -> Result<Array, JsValue> {
        let deps: Result<Vec<am::ChangeHash>,_> = have_deps.iter().map(|j| j.into_serde()).collect();
        let deps = deps.map_err(to_js_err)?;
        let changes = self.0.get_changes(&deps);
        let changes : Array = changes.iter().map(|c| js_sys::Uint8Array::from(c.raw_bytes())).collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getChangesAdded)]
    pub fn get_changes_added(&mut self, other: &Automerge) -> Result<Array, JsValue> {
        let changes = self.0.get_changes_added(&other.0);
        let changes : Array = changes.iter().map(|c| js_sys::Uint8Array::from(c.raw_bytes())).collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getHeads)]
    pub fn get_heads(&mut self) -> Result<Array, JsValue> {
        let heads = self.0.get_heads();
        let heads : Array = heads.iter().map(|h| JsValue::from_str(&hex::encode(&h.0))).collect();
        Ok(heads)
    }

    #[wasm_bindgen(js_name = getActorId)]
    pub fn get_actor_id(&self) -> Result<JsValue, JsValue> {
        if let Some(actor) = self.0.get_actor() {
            Ok(actor.to_string().into())
        } else {
            Ok(JsValue::null())
        }
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
}

pub fn to_usize(val: JsValue, name: &str) -> Result<usize,JsValue> {
    match val.as_f64() {
        Some(n) => Ok(n as usize),
        None => Err(format!("{} must be a number",name).into()),
    }
}

struct ObjType(am::ObjType);

impl TryFrom<JsValue> for ObjType {
    type Error = JsValue;

    fn try_from(val: JsValue) -> Result<Self, Self::Error> {
        match &val.as_string() {
            Some(o) if o == "map" => Ok(ObjType(am::ObjType::Map)),
            Some(o) if o == "list" => Ok(ObjType(am::ObjType::List)),
            Some(o) => Err(format!("unknown obj type {}",o).into()),
            _ => Err(format!("obj type must be a string").into()),
        }
    }
}


#[wasm_bindgen]
pub fn init(actor: JsValue) -> Result<Automerge, JsValue> {
    console_error_panic_hook::set_once();
    Automerge::new(actor)
}

#[wasm_bindgen]
pub fn root() -> Result<JsValue, JsValue> {
    Ok("_root".into())
}

fn to_js_err<T: Display>(err: T) -> JsValue {
    js_sys::Error::new(&std::format!("{}", err)).into()
}

//#![feature(set_stdio)]

#![allow(unused_variables)]
use automerge as am;
use automerge::{Key, Value};
use wasm_bindgen::JsCast;
use js_sys::{Array , Uint8Array };
//use serde::{de::DeserializeOwned, Serialize};
use rand::Rng;
use std::fmt::Display;
use wasm_bindgen::prelude::*;
extern crate web_sys;
//extern crate hex;
#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
          web_sys::console::log_1(&format!( $( $t )* ).into());
    };
}

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[derive(Debug)]
pub struct ScalarValue(am::ScalarValue);

impl From<ScalarValue> for JsValue {
    fn from(val: ScalarValue) -> Self {
        match &val.0 {
            am::ScalarValue::Bytes(v) => js_sys::Uint8Array::from(v.as_slice()).into(),
            am::ScalarValue::Str(v) => v.into(),
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
    pub fn new() -> Result<Automerge, JsValue> {
        let actor = automerge::Actor::from("aabbccdd").map_err(to_js_err)?;
        let mut automerge = automerge::Automerge::new();
        automerge.set_actor(actor);
        Ok(Automerge(automerge))

    }

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
        let id_str = id.as_string().ok_or("invalid opid/objid/elemid").map_err(to_js_err)?;
        Ok(self.0.import(&id_str).map_err(to_js_err)?)
    }

    #[wasm_bindgen(js_name = makeMap)]
    pub fn make_map(&mut self, obj: JsValue, prop: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let key = self.prop_to_key(prop)?;
        let obj = self
            .0
            .make(obj, key, am::ObjType::Map, false)
            .map_err(to_js_err)?;
        Ok(self.export(obj))
    }

    #[wasm_bindgen(js_name = makeList)]
    pub fn make_list(&mut self, obj: JsValue, prop: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        //let key = self.prop_to_key(prop)?;
        let key = prop.as_string().unwrap_or_default();
        let obj = self
            .0
            .map_make(obj, &key, am::ObjType::List)
            .map_err(to_js_err)?;
        Ok(self.export(obj))
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
        let key = self.0.prop_to_key(prop);
        Ok(key)
    }

    fn insert_pos_for_index(&mut self, obj: &am::ObjId, index: JsValue) -> Result<am::Key, JsValue> {
        let index = index.as_f64();
        if index.is_none() {
            return Err("index must be a valid number".into());
        }
        let index = index.unwrap() as usize;
        let key = self.0.insert_pos_for_index(obj, index)
          .ok_or(JsErr("index out of bounds".into()))?;
        Ok(key)
    }

    fn set_pos_for_index(&mut self, obj: &am::ObjId, index: JsValue) -> Result<am::Key, JsValue> {
        let index = index.as_f64();
        if index.is_none() {
            return Err("index must be a valid number".into());
        }
        let index = index.unwrap() as usize;
        let key = self.0.set_pos_for_index(obj, index)
          .ok_or(JsErr("index out of bounds".into()))?;
        Ok(key)
    }

    pub fn insert(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<JsValue, JsValue> {
      let obj = self.import(obj)?;
      let key = self.insert_pos_for_index(&obj,prop)?;
      self.do_set(obj,key,value,datatype,true)
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
          let key = self.insert_pos_for_index(&obj,prop)?;
          self.do_set(obj,key,value,datatype,true)
      } else {
          let key = self.set_pos_for_index(&obj,prop)?;
          self.do_set(obj,key,value,datatype,false)
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
      self.do_set(obj,key,value,datatype,false)
    }

    fn do_set(
        &mut self,
        obj: am::ObjId,
        key: Key,
        value: JsValue,
        datatype: JsValue,
        insert: bool
    ) -> Result<JsValue, JsValue> {
        let datatype = datatype.as_string();
        let value = match datatype.as_deref() {
            Some("boolean") => value
                .as_bool()
                .ok_or(JsErr("value must be a bool".into()))
                .map(|v| am::ScalarValue::Boolean(v)),
            Some("int") => value
                .as_f64()
                .ok_or("value must be a number".into())
                .map(|v| am::ScalarValue::Int(v as i64)),
            Some("uint") => value
                .as_f64()
                .ok_or("value must be a number".into())
                .map(|v| am::ScalarValue::Uint(v as u64)),
            Some("f64") => value
                .as_f64()
                .ok_or("value must be a number".into())
                .map(|v| am::ScalarValue::F64(v)),
            Some("bytes") => {
                log!("BYTES {:?}",value);
                Ok( am::ScalarValue::Bytes(value.dyn_into::<Uint8Array>().unwrap().to_vec())) },
            Some("counter") => value
                .as_f64()
                .ok_or("value must be a number".into())
                .map(|v| am::ScalarValue::Counter(v as i64)),
            Some("timestamp") => value
                .as_f64()
                .ok_or("value must be a number".into())
                .map(|v| am::ScalarValue::Timestamp(v as i64)),
            /*
            Some("bytes") => unimplemented!(),
            Some("cursor") => unimplemented!(),
            */
            Some("null") => Ok(am::ScalarValue::Null),
            Some(_) => Err(JsErr(format!("unknown datatype {:?}", datatype).into())),
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

    pub fn value(
        &mut self,
        obj: JsValue,
        arg: JsValue,
    ) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let prop = arg.as_string();
        let index = arg.as_f64().map(|v| v as usize);
//            .ok_or(JsErr("prop must be a string".into()))?;
        let result = Array::new();
        let value = match (index,prop) {
          (Some(n),_) => Ok(self.0.list_value(&obj, n)),
          (_,Some(p)) => Ok(self.0.map_value(&obj, &p)),
          _ => Err(JsErr("prop must be a string or number".into()))
        }?;
//        let value = self.0.map_value(&obj, &prop);
  
        match value {
            Some(Value::Object(obj_type, obj_id)) => {
                result.push(&obj_type.to_string().into());
                result.push(&self.export(obj_id));
            }
            Some(Value::Scalar(value)) => {
                result.push(&value.datatype().into());
                result.push(&ScalarValue(value).into());
            }
            None => {}
        }
        Ok(result)
    }

    pub fn length(
        &mut self,
        obj: JsValue,
        arg: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let len = self.0.list_length(&obj) as f64;
        Ok(len.into())
    }

    pub fn del(&mut self, obj: JsValue, prop: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let key = self.prop_to_key(prop)?;
        self.0.del(obj, key).map_err(to_js_err)
    }

    pub fn save(&self) -> Result<Uint8Array,JsValue> {
        self.0.save().map(|v| js_sys::Uint8Array::from(v.as_slice())).map_err(to_js_err)
    }

    pub fn dump(&self) {
        self.0.dump()
    }
}

/*
impl Default for Automerge {
    fn default() -> Self {
        Self::new()
    }
}
*/

#[wasm_bindgen]
pub fn init() -> Result<Automerge, JsValue> {
    console_error_panic_hook::set_once();
    Automerge::new()
}

#[wasm_bindgen]
pub fn root() -> Result<JsValue, JsValue> {
    Ok("_root".into())
}

fn to_js_err<T: Display>(err: T) -> JsValue {
    js_sys::Error::new(&std::format!("{}", err)).into()
}

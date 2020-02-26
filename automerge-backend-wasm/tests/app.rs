#![cfg(target_arch = "wasm32")]

extern crate automerge_backend_wasm;

use automerge_backend_wasm::log;
use automerge_backend::{ Change, Operation, ObjectID, PrimitiveValue, Key };
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use serde_wasm_bindgen::{from_value, to_value};
use serde_json::{from_str, to_string};
use js_sys::{ JSON };

#[test]
fn test_basic() {
  assert_eq!(1, 1);
}

#[wasm_bindgen_test]
fn test_wasm() {
  let op1 : Operation = Operation::Set {
    object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
    key: Key("somekeyid".to_string()),
    value: PrimitiveValue::Boolean(true),
    datatype: None
  };

  let json_str1 = serde_json::to_string(&op1).unwrap();
  let js_value : JsValue = JsValue::from_serde(&json_str1).unwrap();
  let json_str2 : String = js_value.into_serde().unwrap();
  let op2: Operation = serde_json::from_str(&json_str2).unwrap();

  log(format!("op1 == op2: {:?} {:?}", op1, op2).as_str());
  assert_eq!(op1, op2);

  let js_value : JsValue = serde_wasm_bindgen::to_value(&op1).unwrap();
  let op2 : Operation = serde_wasm_bindgen::from_value(js_value).unwrap();

  log(format!("op1 == op2: {:?} {:?}", op1, op2).as_str());
  assert_eq!(op2, op2);
}


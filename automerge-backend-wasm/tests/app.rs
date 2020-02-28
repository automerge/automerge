#![cfg(target_arch = "wasm32")]

extern crate automerge_backend_wasm;

use automerge_backend::{Key, ObjectID, Operation, PrimitiveValue};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn test_wasm() {
    let op1: Operation = Operation::Set {
        object_id: ObjectID::ID("2ed3ffe8-0ff3-4671-9777-aa16c3e09945".to_string()),
        key: Key("somekeyid".to_string()),
        value: PrimitiveValue::Boolean(true),
        datatype: None,
    };

    let js_value = JsValue::from_serde(&op1).unwrap();
    let op2: Operation = js_value.into_serde().unwrap();

    assert_eq!(op1, op2);
}

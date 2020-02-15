#![cfg(target_arch = "wasm32")]

extern crate automerge_backend_wasm;

use futures::prelude::*;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use automerge_backend_wasm::{applyChanges, init};

wasm_bindgen_test_configure!(run_in_browser);

// This runs a unit test in native Rust, so it can only use Rust APIs.
#[test]
fn rust_test() {
    assert_eq!(1, 1);
    println!("TEST BEGIN");
    let mut s1 = init();
    let s2 = applyChanges(&mut s1, JsValue::from_str("hello"));
    //  println!("s1");
    //  println!("s2 {}", s2);
    println!("TEST END");
}

// This runs a unit test in the browser, so it can use browser APIs.
#[wasm_bindgen_test]
fn web_test() {
    assert_eq!(1, 1);
}

// This runs a unit test in the browser, and in addition it supports asynchronous Future APIs.
#[wasm_bindgen_test(async)]
fn async_test() -> impl Future<Item = (), Error = JsValue> {
    // Creates a JavaScript Promise which will asynchronously resolve with the value 42.
    let promise = js_sys::Promise::resolve(&JsValue::from(42));

    // Converts that Promise into a Future.
    // The unit test will wait for the Future to resolve.
    JsFuture::from(promise).map(|x| {
        assert_eq!(x, 42);
    })
}

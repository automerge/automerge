extern crate automerge_backend;
extern crate serde_json;
//extern crate js_sys;
use wasm_bindgen::prelude::*;
//use js_sys::*;
//use web_sys::console;
use automerge_backend::Backend;
use automerge_backend::Change;

// When the `wee_alloc` feature is enabled, this uses `wee_alloc` as the global
// allocator.
//
// If you don't want to use `wee_alloc`, you can safely delete this.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
  #[wasm_bindgen(js_namespace = console)]
  fn log(s: &str);
}

// We need a wrapper object to attach the wasm-bindgen on
#[wasm_bindgen]
#[derive(PartialEq,Debug, Clone)]
pub struct State { backend: Backend }

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn applyChanges(state: &mut State, changes: JsValue) -> JsValue {
  let c: Vec<Change> = changes.into_serde().unwrap();
  let patch = state.backend.apply_changes(c);
  JsValue::from_serde(&patch).ok().into()
/*
  // attempt to get the [state,patch] working
  // ... -> Array
  let ret = Array::new();
  ret.push(&state.clone().into());
  ret.push(&JsValue::from_serde(&patch).ok().into());
  ret
*/
}

#[wasm_bindgen]
pub fn init() -> State {
  State { backend: automerge_backend::Backend::init() }
}

#[wasm_bindgen(start)]
pub fn main_js() -> Result<(), JsValue> {
  // better error messages in debug mode.
  #[cfg(debug_assertions)]
  console_error_panic_hook::set_once();

  // any startup or boiler plate code goes here

  Ok(())
}

extern crate automerge_backend;
extern crate serde_json;
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use js_sys::Array;
use automerge_backend::{Backend, ActorID, Change, Clock};

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
pub struct State { backend: Rc<RefCell<Backend>> }

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn applyChanges(state: &State, changes: JsValue) -> Array {
  let c: Vec<Change> = changes.into_serde().unwrap();
  let newState = state.clone();
  let patch = newState.backend.borrow_mut().apply_changes(c);
  let ret = Array::new();
  ret.push(&newState.into());
  ret.push(&JsValue::from_serde(&patch).ok().into());
  ret
}

#[wasm_bindgen]
pub struct Bar { }

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn foo() -> Bar {
  Bar {}
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn applyLocalChange(state: &State, change: JsValue) -> Array {
  let c: Change = change.into_serde().unwrap();
  let newState = state.clone();
  let patch = newState.backend.borrow_mut().apply_local_change(c);
  let ret = Array::new();
  ret.push(&newState.into());
  ret.push(&JsValue::from_serde(&patch).ok().into());
  ret
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getPatch(state: &State) -> JsValue {
  let patch = state.backend.borrow().get_patch();
  JsValue::from_serde(&patch).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getChanges(state: &State) -> JsValue {
  let changes = state.backend.borrow().get_changes();
  JsValue::from_serde(&changes).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getChangesForActorId(state: &State, actorId: JsValue) -> JsValue {
  let a: ActorID = actorId.into_serde().unwrap();
  let changes = state.backend.borrow().get_changes_for_actor_id(a);
  JsValue::from_serde(&changes).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getMissingChanges(state: &State, clock: JsValue) -> JsValue {
  let c: Clock = clock.into_serde().unwrap();
  let changes = state.backend.borrow().get_missing_changes(c);
  JsValue::from_serde(&changes).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getMissingDeps(state: &State) -> JsValue {
  let clock = state.backend.borrow().get_missing_deps();
  JsValue::from_serde(&clock).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn merge(state: &mut State, remote: State) -> JsValue {
  let patch = state.backend.borrow_mut().merge(&remote.backend.borrow());
  JsValue::from_serde(&patch).ok().into()
}

#[wasm_bindgen]
pub fn init() -> State {
  State { backend: Rc::new(RefCell::new(Backend::init())) }
}

#[wasm_bindgen(start)]
pub fn main_js() -> Result<(), JsValue> {
  // better error messages in debug mode.
  #[cfg(debug_assertions)]
  console_error_panic_hook::set_once();

  // any startup or boiler plate code goes here

  Ok(())
}

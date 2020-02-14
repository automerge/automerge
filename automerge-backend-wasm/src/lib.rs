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

#[wasm_bindgen(js_name = applyChange)]
pub fn apply_changes(state: &State, changes: JsValue) -> Array {
  let c: Vec<Change> = changes.into_serde().unwrap();
  let new_state = state.clone();
  let patch = new_state.backend.borrow_mut().apply_changes(c);
  let ret = Array::new();
  ret.push(&new_state.into());
  ret.push(&JsValue::from_serde(&patch).ok().into());
  ret
}

#[wasm_bindgen(js_name = applyLocalChange)]
pub fn apply_local_change(state: &State, change: JsValue) -> Array {
  let c: Change = change.into_serde().unwrap();
  let new_state = state.clone();
  let patch = new_state.backend.borrow_mut().apply_local_change(c);
  let ret = Array::new();
  ret.push(&new_state.into());
  ret.push(&JsValue::from_serde(&patch).ok().into());
  ret
}

#[wasm_bindgen(js_name = getPatch)]
pub fn get_patch(state: &State) -> JsValue {
  let patch = state.backend.borrow().get_patch();
  JsValue::from_serde(&patch).ok().into()
}

#[wasm_bindgen(js_name = getChanges)]
pub fn get_changes(state: &State) -> JsValue {
  let changes = state.backend.borrow().get_changes();
  JsValue::from_serde(&changes).ok().into()
}

#[wasm_bindgen(js_name = getChangesForActorId)]
pub fn get_changes_for_actorid(state: &State, actorid: JsValue) -> JsValue {
  let a: ActorID = actorid.into_serde().unwrap();
  let changes = state.backend.borrow().get_changes_for_actor_id(a);
  JsValue::from_serde(&changes).ok().into()
}

#[wasm_bindgen(js_name = getMissingChanges)]
pub fn get_missing_changes(state: &State, clock: JsValue) -> JsValue {
  let c: Clock = clock.into_serde().unwrap();
  let changes = state.backend.borrow().get_missing_changes(c);
  JsValue::from_serde(&changes).ok().into()
}

#[wasm_bindgen(js_name = getMissingDeps)]
pub fn get_missing_deps(state: &State) -> JsValue {
  let clock = state.backend.borrow().get_missing_deps();
  JsValue::from_serde(&clock).ok().into()
}

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

extern crate automerge_backend;
extern crate serde_json;
use wasm_bindgen::prelude::*;
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
pub struct State { backend: Backend }

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn applyChanges(state: &mut State, changes: JsValue) -> JsValue {
  let c: Vec<Change> = changes.into_serde().unwrap();
  let patch = state.backend.apply_changes(c);
  JsValue::from_serde(&patch).ok().into()
/*
  // attempt to get the [state,patch] tuple working
  // return ... -> Array
  let ret = Array::new();
  ret.push(&state.clone().into());
  ret.push(&JsValue::from_serde(&patch).ok().into());
  ret
*/
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn applyLocalChange(state: &mut State, change: JsValue) -> JsValue {
  let c: Change = change.into_serde().unwrap();
  let patch = state.backend.apply_local_change(c);
  JsValue::from_serde(&patch).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getPatch(state: &mut State) -> JsValue {
  let patch = state.backend.get_patch();
  JsValue::from_serde(&patch).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getChanges(state: &mut State) -> JsValue {
  let changes = state.backend.get_changes();
  JsValue::from_serde(&changes).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getChangesForActorId(state: &mut State, actorId: JsValue) -> JsValue {
  let a: ActorID = actorId.into_serde().unwrap();
  let changes = state.backend.get_changes_for_actor_id(a);
  JsValue::from_serde(&changes).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getMissingChanges(state: &mut State, clock: JsValue) -> JsValue {
  let c: Clock = clock.into_serde().unwrap();
  let changes = state.backend.get_missing_changes(c);
  JsValue::from_serde(&changes).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn getMissingDeps(state: &mut State) -> JsValue {
  let clock = state.backend.get_missing_deps();
  JsValue::from_serde(&clock).ok().into()
}

#[allow(non_snake_case)]
#[wasm_bindgen]
pub fn merge(state: &mut State, remote: State) -> JsValue {
  let patch = state.backend.merge(&remote.backend);
  JsValue::from_serde(&patch).ok().into()
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

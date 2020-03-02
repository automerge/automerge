use automerge_backend::{ActorID, AutomergeError, Backend, Change, ChangeRequest, Clock};
use js_sys::Array;
use serde::de::DeserializeOwned;
use serde::Serialize;
use wasm_bindgen::prelude::*;

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    pub fn log(s: &str);
}

fn js_to_rust<T: DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    value.into_serde().map_err(json_error_to_js)
}

fn rust_to_js<T: Serialize>(value: T) -> Result<JsValue, JsValue> {
    JsValue::from_serde(&value).map_err(json_error_to_js)
}

#[wasm_bindgen]
#[derive(PartialEq, Debug, Clone)]
pub struct State {
    backend: Backend,
}

#[wasm_bindgen(js_name = applyChanges)]
pub fn apply_changes(mut state: State, changes: JsValue) -> Result<Array, JsValue> {
    let c: Vec<Change> = js_to_rust(changes)?;
    let patch = state
        .backend
        .apply_changes(c)
        .map_err(automerge_error_to_js)?;
    let ret = Array::new();
    ret.push(&state.into());
    ret.push(&rust_to_js(&patch)?);
    Ok(ret)
}

#[wasm_bindgen(js_name = applyLocalChange)]
pub fn apply_local_change(mut state: State, change: JsValue) -> Result<Array, JsValue> {
    let c: ChangeRequest = js_to_rust(change)?;
    let patch = state
        .backend
        .apply_local_change(c)
        .map_err(automerge_error_to_js)?;
    let ret = Array::new();
    ret.push(&state.into());
    ret.push(&rust_to_js(&patch)?);
    Ok(ret)
}

#[wasm_bindgen(js_name = getPatch)]
pub fn get_patch(state: &State) -> Result<JsValue, JsValue> {
    let patch = state.backend.get_patch();
    rust_to_js(&patch)
}

#[wasm_bindgen(js_name = getChanges)]
pub fn get_changes(old_state: &State, new_state: &State) -> Result<JsValue, JsValue> {
    let changes = old_state.backend.get_changes(&new_state.backend);
    rust_to_js(&changes)
}

#[wasm_bindgen(js_name = getChangesForActor)]
pub fn get_changes_for_actorid(state: &State, actorid: JsValue) -> Result<JsValue, JsValue> {
    let a: ActorID = js_to_rust(actorid)?;
    let changes = state.backend.get_changes_for_actor_id(a);
    rust_to_js(&changes)
}

#[wasm_bindgen(js_name = getMissingChanges)]
pub fn get_missing_changes(state: &State, clock: JsValue) -> Result<JsValue, JsValue> {
    let c: Clock = js_to_rust(clock)?;
    let changes = state.backend.get_missing_changes(c);
    rust_to_js(&changes)
}

#[wasm_bindgen(js_name = getMissingDeps)]
pub fn get_missing_deps(state: &State) -> Result<JsValue, JsValue> {
    let clock = state.backend.get_missing_deps();
    rust_to_js(&clock)
}

#[wasm_bindgen]
pub fn merge(state: &mut State, remote: State) -> Result<JsValue, JsValue> {
    let patch = state
        .backend
        .merge(&remote.backend)
        .map_err(automerge_error_to_js)?;
    rust_to_js(&patch)
}

#[wasm_bindgen]
pub fn init() -> State {
    State {
        backend: Backend::init(),
    }
}

fn automerge_error_to_js(err: AutomergeError) -> JsValue {
    JsValue::from(std::format!("Automerge error: {}", err))
}

fn json_error_to_js(err: serde_json::Error) -> JsValue {
    JsValue::from(std::format!("serde_json error: {}", err))
}

use automerge_backend::{ActorID, AutomergeError, Backend, Change, ChangeRequest, Clock};
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

#[wasm_bindgen]
impl State {
    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: JsValue) -> Result<JsValue, JsValue> {
        let c: Vec<Change> = js_to_rust(changes)?;
        let patch = self
            .backend
            .apply_changes(c)
            .map_err(automerge_error_to_js)?;
        rust_to_js(&patch)
    }

    #[wasm_bindgen(js_name = applyLocalChange)]
    pub fn apply_local_change(&mut self, change: JsValue) -> Result<JsValue, JsValue> {
        let c: ChangeRequest = js_to_rust(change)?;
        let patch = self
            .backend
            .apply_local_change(c)
            .map_err(automerge_error_to_js)?;
        rust_to_js(&patch)
    }

    #[wasm_bindgen(js_name = getPatch)]
    pub fn get_patch(&self) -> Result<JsValue, JsValue> {
        let patch = self.backend.get_patch();
        rust_to_js(&patch)
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&self, state: &State) -> Result<JsValue, JsValue> {
        let changes = self.backend.get_changes(&state.backend);
        rust_to_js(&changes)
    }

    #[wasm_bindgen(js_name = getChangesForActor)]
    pub fn get_changes_for_actorid(&self, actorid: JsValue) -> Result<JsValue, JsValue> {
        let a: ActorID = js_to_rust(actorid)?;
        let changes = self.backend.get_changes_for_actor_id(a);
        rust_to_js(&changes)
    }

    #[wasm_bindgen(js_name = getMissingChanges)]
    pub fn get_missing_changes(&self, clock: JsValue) -> Result<JsValue, JsValue> {
        let c: Clock = js_to_rust(clock)?;
        let changes = self.backend.get_missing_changes(c);
        rust_to_js(&changes)
    }

    #[wasm_bindgen(js_name = getMissingDeps)]
    pub fn get_missing_deps(&self) -> Result<JsValue, JsValue> {
        let clock = self.backend.get_missing_deps();
        rust_to_js(&clock)
    }

    #[wasm_bindgen(js_name = getClock)]
    pub fn get_clock(&self) -> Result<JsValue, JsValue> {
        let clock = self.backend.clock();
        rust_to_js(&clock)
    }

    #[wasm_bindgen(js_name = getHistory)]
    pub fn get_history(&self) -> Result<JsValue, JsValue> {
        let changes = self.backend.history();
        rust_to_js(&changes)
    }

    #[wasm_bindgen]
    pub fn merge(&mut self, remote: &State) -> Result<JsValue, JsValue> {
        let patch = self
            .backend
            .merge(&remote.backend)
            .map_err(automerge_error_to_js)?;
        rust_to_js(&patch)
    }

    #[wasm_bindgen]
    pub fn new() -> State {
        State {
            backend: Backend::init(),
        }
    }
}

fn automerge_error_to_js(err: AutomergeError) -> JsValue {
    JsValue::from(std::format!("Automerge error: {}", err))
}

fn json_error_to_js(err: serde_json::Error) -> JsValue {
    JsValue::from(std::format!("serde_json error: {}", err))
}

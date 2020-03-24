use automerge_backend::{ActorID, AutomergeError, Backend, Change, ChangeRequest, Clock};
use serde::de::DeserializeOwned;
use serde::Serialize;
use wasm_bindgen::prelude::*;

extern crate web_sys;
#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
        //        web_sys::console::log_1(&format!( $( $t )* ).into());
    };
}

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

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

#[allow(clippy::new_without_default)]
#[wasm_bindgen]
impl State {
    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: JsValue) -> Result<JsValue, JsValue> {
        log!("apply_changes {:?}", changes);
        let c: Vec<Change> = js_to_rust(changes)?;
        let patch = self
            .backend
            .apply_changes(c)
            .map_err(automerge_error_to_js)?;
        rust_to_js(&patch)
    }

    #[wasm_bindgen(js_name = loadChanges)]
    pub fn load_changes(&mut self, changes: JsValue) -> Result<(), JsValue> {
        log!("load_changes {:?}", changes);
        let c: Vec<Change> = js_to_rust(changes)?;
        self.backend.load_changes(c).map_err(automerge_error_to_js)
    }

    #[wasm_bindgen(js_name = applyLocalChange)]
    pub fn apply_local_change(&mut self, change: JsValue) -> Result<JsValue, JsValue> {
        log!("apply_local_changes {:?}", change);
        let c: ChangeRequest = js_to_rust(change)?;
        let patch = self
            .backend
            .apply_local_change(c)
            .map_err(automerge_error_to_js)?;
        rust_to_js(&patch)
    }

    #[wasm_bindgen(js_name = getPatch)]
    pub fn get_patch(&self) -> Result<JsValue, JsValue> {
        log!("get_patch");
        let patch = self.backend.get_patch();
        rust_to_js(&patch)
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&self, state: &State) -> Result<JsValue, JsValue> {
        log!("get_changes");
        let changes = self
            .backend
            .get_changes(&state.backend)
            .map_err(automerge_error_to_js)?;
        rust_to_js(&changes)
    }

    #[wasm_bindgen(js_name = getChangesForActor)]
    pub fn get_changes_for_actorid(&self, actorid: JsValue) -> Result<JsValue, JsValue> {
        log!("get_changes_for_actorid");
        let a: ActorID = js_to_rust(actorid)?;
        let changes = self.backend.get_changes_for_actor_id(&a);
        rust_to_js(&changes)
    }

    #[wasm_bindgen(js_name = getMissingChanges)]
    pub fn get_missing_changes(&self, clock: JsValue) -> Result<JsValue, JsValue> {
        log!("get_missing_changes");
        let c: Clock = js_to_rust(clock)?;
        let changes = self.backend.get_missing_changes(c);
        rust_to_js(&changes)
    }

    #[wasm_bindgen(js_name = getMissingDeps)]
    pub fn get_missing_deps(&self) -> Result<JsValue, JsValue> {
        log!("get_missing_deps");
        let clock = self.backend.get_missing_deps();
        rust_to_js(&clock)
    }

    #[wasm_bindgen(js_name = getClock)]
    pub fn get_clock(&self) -> Result<JsValue, JsValue> {
        log!("get_clock");
        let clock = self.backend.clock();
        rust_to_js(&clock)
    }

    #[wasm_bindgen(js_name = getHistory)]
    pub fn get_history(&self) -> Result<JsValue, JsValue> {
        log!("get_history");
        let history = self.backend.history();
        rust_to_js(&history)
    }

    #[wasm_bindgen(js_name = getUndoStack)]
    pub fn get_undo_stack(&self) -> Result<JsValue, JsValue> {
        log!("get_undo_stack");
        let stack = self.backend.undo_stack();
        rust_to_js(&stack)
    }

    #[wasm_bindgen(js_name = getRedoStack)]
    pub fn get_redo_stack(&self) -> Result<JsValue, JsValue> {
        log!("get_redo_stack");
        let stack = self.backend.redo_stack();
        rust_to_js(&stack)
    }

    #[wasm_bindgen]
    pub fn merge(&mut self, remote: &State) -> Result<JsValue, JsValue> {
        log!("merge");
        let patch = self
            .backend
            .merge(&remote.backend)
            .map_err(automerge_error_to_js)?;
        rust_to_js(&patch)
    }

    #[wasm_bindgen]
    pub fn fork(&self) -> State {
        log!("fork");
        self.clone()
    }

    #[wasm_bindgen]
    #[wasm_bindgen(js_name = forkAt)]
    pub fn fork_at(&self, _clock: JsValue) -> Result<State, JsValue> {
        log!("fork_at");
        let clock: Clock = js_to_rust(_clock)?;
        let changes = self
            .backend
            .history()
            .iter()
            .filter(|change| clock.get(&change.actor_id) >= change.seq)
            .map(|&c| c.clone())
            .collect();
        let mut fork = State {
            backend: Backend::init(),
        };
        let _patch = fork
            .backend
            .apply_changes(changes)
            .map_err(automerge_error_to_js)?;
        Ok(fork)
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

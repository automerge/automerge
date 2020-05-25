//#![feature(set_stdio)]

use automerge_backend::{Backend, Change};
use automerge_protocol::{ActorID, ChangeHash, Request};
use js_sys::{Array, Uint8Array};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Display;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

extern crate web_sys;
#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
          web_sys::console::log_1(&format!( $( $t )* ).into());
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
#[derive(PartialEq, Debug)]
pub struct State {
    backend: Backend,
}

#[allow(clippy::new_without_default)]
#[wasm_bindgen]
impl State {
    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: Array) -> Result<JsValue, JsValue> {
        let mut ch = Vec::with_capacity(changes.length() as usize);
        for c in changes.iter() {
            let bytes = c.dyn_into::<Uint8Array>().unwrap().to_vec();
            ch.push(Change::from_bytes(bytes).map_err(to_js_err)?);
        }
        let patch = self.backend.apply_changes(ch).map_err(to_js_err)?;
        rust_to_js(&patch)
    }

    #[wasm_bindgen(js_name = loadChanges)]
    pub fn load_changes(&mut self, changes: Array) -> Result<(), JsValue> {
        let mut ch = Vec::with_capacity(changes.length() as usize);
        for c in changes.iter() {
            let bytes = c.dyn_into::<Uint8Array>().unwrap().to_vec();
            ch.push(Change::from_bytes(bytes).unwrap())
        }
        self.backend.load_changes(ch).map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = applyLocalChange)]
    pub fn apply_local_change(&mut self, change: JsValue) -> Result<JsValue, JsValue> {
        let c: Request = js_to_rust(change)?;
        let patch = self.backend.apply_local_change(c).map_err(to_js_err)?;
        rust_to_js(&patch)
    }

    #[wasm_bindgen(js_name = getPatch)]
    pub fn get_patch(&self) -> Result<JsValue, JsValue> {
        let patch = self.backend.get_patch().map_err(to_js_err)?;
        rust_to_js(&patch)
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&self, have_deps: JsValue) -> Result<Array, JsValue> {
        let deps: Vec<ChangeHash> = js_to_rust(have_deps)?;
        let changes = self.backend.get_changes(&deps);
        let result = Array::new();
        for c in changes {
            let bytes: Uint8Array = c.bytes.as_slice().into();
            result.push(bytes.as_ref());
        }
        Ok(result)
    }

    #[wasm_bindgen(js_name = getChangesForActor)]
    pub fn get_changes_for_actorid(&self, actorid: JsValue) -> Result<Array, JsValue> {
        let a: ActorID = js_to_rust(actorid)?;
        let changes = self
            .backend
            .get_changes_for_actor_id(&a)
            .map_err(to_js_err)?;
        let result = Array::new();
        for c in changes {
            let bytes: Uint8Array = c.bytes.as_slice().into();
            result.push(bytes.as_ref());
        }
        Ok(result)
    }

    #[wasm_bindgen(js_name = getMissingDeps)]
    pub fn get_missing_deps(&self) -> Result<JsValue, JsValue> {
        let hashes = self.backend.get_missing_deps();
        rust_to_js(&hashes)
    }

    #[wasm_bindgen(js_name = getUndoStack)]
    pub fn get_undo_stack(&self) -> Result<JsValue, JsValue> {
        rust_to_js(&self.backend.undo_stack())
    }

    #[wasm_bindgen(js_name = getRedoStack)]
    pub fn get_redo_stack(&self) -> Result<JsValue, JsValue> {
        rust_to_js(&self.backend.redo_stack())
    }

    #[allow(clippy::should_implement_trait)]
    #[wasm_bindgen(js_name = clone)]
    pub fn clone(&self) -> Result<State, JsValue> {
        Ok(State {
            backend: self.backend.clone(),
        })
    }

    #[wasm_bindgen(js_name = save)]
    pub fn save(&self) -> Result<JsValue, JsValue> {
        let data = self.backend.save().map_err(to_js_err)?;
        let js_bytes: Uint8Array = data.as_slice().into();
        Ok(js_bytes.into())
    }

    #[wasm_bindgen(js_name = load)]
    pub fn load(data: JsValue) -> Result<State, JsValue> {
        let data = data.dyn_into::<Uint8Array>().unwrap().to_vec();
        let backend = Backend::load(data).map_err(to_js_err)?;
        Ok(State { backend })
    }

    #[wasm_bindgen]
    pub fn new() -> State {
        State {
            backend: Backend::init(),
        }
    }
}

fn to_js_err<T: Display>(err: T) -> JsValue {
    js_sys::Error::new(&std::format!("Automerge error: {}", err)).into()
}

fn json_error_to_js(err: serde_json::Error) -> JsValue {
    js_sys::Error::new(&std::format!("serde_json error: {}", err)).into()
}

/*
struct WasmSTDIO {}

impl std::io::Write for WasmSTDIO {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let string = String::from_utf8_lossy(&buf).into_owned();
        web_sys::console::log_1(&string.into());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[wasm_bindgen(start)]
pub fn main() {
}
*/

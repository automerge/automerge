//#![feature(set_stdio)]

use automerge_backend::{AutomergeError, Backend, Change};
use automerge_backend::{SyncMessage, SyncState};
use automerge_protocol::{ChangeHash, UncompressedChange};
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

fn array<T: Serialize>(data: &[T]) -> Result<Array, JsValue> {
    let result = Array::new();
    for d in data {
        result.push(&rust_to_js(d)?);
    }
    Ok(result)
}

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

fn js_to_rust<T: DeserializeOwned>(value: &JsValue) -> Result<T, JsValue> {
    value.into_serde().map_err(json_error_to_js)
}

fn rust_to_js<T: Serialize>(value: T) -> Result<JsValue, JsValue> {
    JsValue::from_serde(&value).map_err(json_error_to_js)
}

#[wasm_bindgen]
#[derive(PartialEq, Debug)]
struct State(Backend);

#[wasm_bindgen]
extern "C" {
    pub type Object;

    #[wasm_bindgen(constructor)]
    fn new() -> Object;

    #[wasm_bindgen(method, getter)]
    fn state(this: &Object) -> State;

    #[wasm_bindgen(method, setter)]
    fn set_state(this: &Object, state: State);

    #[wasm_bindgen(method, getter)]
    fn frozen(this: &Object) -> bool;

    #[wasm_bindgen(method, setter)]
    fn set_frozen(this: &Object, frozen: bool);

    #[wasm_bindgen(method, getter)]
    fn heads(this: &Object) -> Array;

    #[wasm_bindgen(method, setter)]
    fn set_heads(this: &Object, heads: Array);
}

#[wasm_bindgen]
pub fn init() -> Result<Object, JsValue> {
    Ok(wrapper(State(Backend::init()), false, Vec::new()))
}

#[wasm_bindgen(js_name = getHeads)]
pub fn get_heads(input: Object) -> Result<Array, JsValue> {
    Ok(input.heads())
}

#[wasm_bindgen(js_name = free)]
pub fn free(input: Object) -> Result<(), JsValue> {
    let state: State = get_state(&input)?;
    std::mem::drop(state);
    input.set_frozen(true);
    input.set_heads(Array::new());
    Ok(())
}

#[wasm_bindgen(js_name = applyLocalChange)]
pub fn apply_local_change(input: Object, change: JsValue) -> Result<JsValue, JsValue> {
    get_mut_input(input, |state| {
        // FIXME unwrap
        let change: UncompressedChange = js_to_rust(&change).unwrap();
        let (patch, change) = state.0.apply_local_change(change)?;
        let result = Array::new();
        let bytes: Uint8Array = change.raw_bytes().into();
        // FIXME unwrap
        let p = rust_to_js(&patch).unwrap();
        result.push(&p);
        result.push(bytes.as_ref());
        Ok(result)
    })
}

#[wasm_bindgen(js_name = applyChanges)]
pub fn apply_changes(input: Object, changes: Array) -> Result<JsValue, JsValue> {
    get_mut_input(input, |state| {
        let ch = import_changes(&changes)?;
        let patch = state.0.apply_changes(ch)?;
        Ok(array(&vec![patch]).unwrap())
    })
}

#[wasm_bindgen(js_name = loadChanges)]
pub fn load_changes(input: Object, changes: Array) -> Result<JsValue, JsValue> {
    get_mut_input(input, |state| {
        let ch = import_changes(&changes)?;
        state.0.load_changes(ch)?;
        Ok(Array::new())
    })
}

#[wasm_bindgen(js_name = load)]
pub fn load(data: JsValue) -> Result<JsValue, JsValue> {
    let data = data.dyn_into::<Uint8Array>().unwrap().to_vec();
    let backend = Backend::load(data).map_err(to_js_err)?;
    let heads = backend.get_heads();
    Ok(wrapper(State(backend), false, heads).into())
}

#[wasm_bindgen(js_name = getPatch)]
pub fn get_patch(input: Object) -> Result<JsValue, JsValue> {
    get_input(input, |state| {
        state.0.get_patch().map_err(to_js_err).and_then(rust_to_js)
    })
}

#[wasm_bindgen(js_name = clone)]
pub fn clone(input: Object) -> Result<Object, JsValue> {
    let old_state = get_state(&input)?;
    let state = State(old_state.0.clone());
    let heads = state.0.get_heads();
    input.set_state(old_state);
    Ok(wrapper(state, false, heads))
}

#[wasm_bindgen(js_name = save)]
pub fn save(input: Object) -> Result<JsValue, JsValue> {
    get_input(input, |state| {
        state
            .0
            .save()
            .map(|data| data.as_slice().into())
            .map(|data: Uint8Array| data.into())
            .map_err(to_js_err)
    })
}

#[wasm_bindgen(js_name = getChanges)]
pub fn get_changes(input: Object, have_deps: JsValue) -> Result<JsValue, JsValue> {
    let deps: Vec<ChangeHash> = js_to_rust(&have_deps)?;
    get_input(input, |state| {
        Ok(export_changes(state.0.get_changes(&deps)).into())
    })
}

#[wasm_bindgen(js_name = getAllChanges)]
pub fn get_all_changes(input: Object) -> Result<JsValue, JsValue> {
    let deps: Vec<ChangeHash> = vec![];
    get_input(input, |state| {
        Ok(export_changes(state.0.get_changes(&deps)).into())
    })
}

#[wasm_bindgen(js_name = getMissingDeps)]
pub fn get_missing_deps(input: Object) -> Result<JsValue, JsValue> {
    get_input(input, |state| {
        rust_to_js(state.0.get_missing_deps(&[], &[]))
    })
}

fn import_changes(changes: &Array) -> Result<Vec<Change>, AutomergeError> {
    let mut ch = Vec::with_capacity(changes.length() as usize);
    for c in changes.iter() {
        let bytes = c.dyn_into::<Uint8Array>().unwrap().to_vec();
        ch.push(Change::from_bytes(bytes)?);
    }
    Ok(ch)
}

fn export_changes(changes: Vec<&Change>) -> Array {
    let result = Array::new();
    for c in changes {
        let bytes: Uint8Array = c.raw_bytes().into();
        result.push(bytes.as_ref());
    }
    result
}

#[wasm_bindgen(js_name = generateSyncMessage)]
pub fn generate_sync_message(sync_state: JsValue, input: Object) -> Result<JsValue, JsValue> {
    get_input(input, |state| {
        let sync_state: SyncState = js_to_rust::<Option<SyncState>>(&sync_state)
            .unwrap_or_default()
            .unwrap_or_default();

        let (sync_state, message) = state.0.generate_sync_message(sync_state);
        let result = Array::new();
        let p = rust_to_js(sync_state).unwrap();
        result.push(&p);
        let message = message
            .map(|m| Uint8Array::from(m.encode().unwrap().as_slice()).into())
            .unwrap_or(JsValue::NULL);
        result.push(&message);
        Ok(result.into())
    })
}

#[wasm_bindgen(js_name = receiveSyncMessage)]
pub fn receive_sync_message(
    sync_state: JsValue,
    input: Object,
    message: JsValue,
) -> Result<JsValue, JsValue> {
    get_mut_input(input, |state| {
        let binary_message = Uint8Array::from(message.clone()).to_vec();
        let message = SyncMessage::decode(binary_message).unwrap();
        let sync_state: SyncState = js_to_rust::<Option<SyncState>>(&sync_state)
            .unwrap_or_default()
            .unwrap_or_default();

        let (sync_state, patch) = state.0.receive_sync_message(message, sync_state)?;

        let result = Array::new();
        result.push(&rust_to_js(&sync_state).unwrap());
        let p = rust_to_js(&patch).unwrap();
        result.push(&p);
        Ok(result)
    })
}

fn get_state(input: &Object) -> Result<State, JsValue> {
    if input.frozen() {
        Err(js_sys::Error::new("Attempting to use an outdated Automerge document that has already been updated. Please use the latest document state, or call Automerge.clone() if you really need to use this old document state.").into())
    } else {
        Ok(input.state())
    }
}

fn wrapper(state: State, frozen: bool, heads: Vec<ChangeHash>) -> Object {
    let heads_array = Array::new();
    for h in heads {
        heads_array.push(&rust_to_js(h).unwrap());
    }

    let wrapper = Object::new();
    wrapper.set_heads(heads_array);
    wrapper.set_frozen(frozen);
    wrapper.set_state(state);
    wrapper
}

fn get_input<F>(input: Object, action: F) -> Result<JsValue, JsValue>
where
    F: Fn(&State) -> Result<JsValue, JsValue>,
{
    let state: State = get_state(&input)?;
    let result = action(&state);
    input.set_state(state);
    result
}

fn get_mut_input<F>(input: Object, action: F) -> Result<JsValue, JsValue>
where
    F: Fn(&mut State) -> Result<Array, AutomergeError>,
{
    let mut state: State = get_state(&input)?;

    match action(&mut state) {
        Ok(result) => {
            let heads = state.0.get_heads();
            let new_state = wrapper(state, false, heads);
            input.set_frozen(true);
            if result.length() == 0 {
                Ok(new_state.into())
            } else {
                result.unshift(&new_state.into());
                Ok(result.into())
            }
        }
        Err(err) => {
            input.set_state(state);
            Err(to_js_err(err))
        }
    }
}

fn to_js_err<T: Display>(err: T) -> JsValue {
    js_sys::Error::new(&std::format!("Automerge error: {}", err)).into()
}

fn json_error_to_js(err: serde_json::Error) -> JsValue {
    js_sys::Error::new(&std::format!("serde_json error: {}", err)).into()
}

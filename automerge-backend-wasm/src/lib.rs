//#![feature(set_stdio)]

use automerge_backend::{AutomergeError, Backend, Change};
use automerge_backend::{BloomFilter, SyncHave, SyncMessage, SyncState};
use automerge_protocol::{ChangeHash, UncompressedChange};
use js_sys::{Array, Uint8Array};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;
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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawSyncState {
    shared_heads: Vec<ChangeHash>,
    last_sent_heads: Option<Vec<ChangeHash>>,
    their_heads: Option<Vec<ChangeHash>>,
    their_need: Option<Vec<ChangeHash>>,
    our_need: Vec<ChangeHash>,
    have: Option<Vec<RawSyncHave>>,
    unapplied_changes: Vec<Vec<u8>>,
    sent_changes: Vec<Vec<u8>>,
}

impl TryFrom<SyncState> for RawSyncState {
    type Error = AutomergeError;

    fn try_from(value: SyncState) -> Result<Self, Self::Error> {
        let have = if let Some(have) = value.have {
            Some(
                have.into_iter()
                    .map(RawSyncHave::try_from)
                    .collect::<Result<_, _>>()?,
            )
        } else {
            None
        };
        let unapplied_changes = value
            .unapplied_changes
            .into_iter()
            .map(|c| c.raw_bytes().to_vec())
            .collect();
        let sent_changes = value
            .sent_changes
            .into_iter()
            .map(|c| c.raw_bytes().to_vec())
            .collect();
        Ok(Self {
            shared_heads: value.shared_heads,
            last_sent_heads: value.last_sent_heads,
            their_heads: value.their_heads,
            their_need: value.their_need,
            our_need: value.our_need,
            have,
            unapplied_changes,
            sent_changes,
        })
    }
}

impl TryFrom<RawSyncState> for SyncState {
    type Error = AutomergeError;

    fn try_from(value: RawSyncState) -> Result<Self, Self::Error> {
        let have = if let Some(have) = value.have {
            Some(
                have.into_iter()
                    .map(SyncHave::try_from)
                    .collect::<Result<_, _>>()?,
            )
        } else {
            None
        };
        let unapplied_changes = value
            .unapplied_changes
            .into_iter()
            .map(Change::from_bytes)
            .collect::<Result<_, _>>()?;
        let sent_changes = value
            .sent_changes
            .into_iter()
            .map(Change::from_bytes)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            shared_heads: value.shared_heads,
            last_sent_heads: value.last_sent_heads,
            their_heads: value.their_heads,
            their_need: value.their_need,
            our_need: value.our_need,
            have,
            unapplied_changes,
            sent_changes,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawSyncHave {
    pub last_sync: Vec<ChangeHash>,
    #[serde(with = "serde_bytes")]
    pub bloom: Vec<u8>,
}

impl TryFrom<SyncHave> for RawSyncHave {
    type Error = AutomergeError;

    fn try_from(value: SyncHave) -> Result<Self, Self::Error> {
        Ok(Self {
            last_sync: value.last_sync,
            bloom: value.bloom.into_bytes()?,
        })
    }
}

impl TryFrom<RawSyncHave> for SyncHave {
    type Error = AutomergeError;

    fn try_from(raw: RawSyncHave) -> Result<Self, Self::Error> {
        Ok(Self {
            last_sync: raw.last_sync,
            bloom: BloomFilter::try_from(raw.bloom.as_slice())?,
        })
    }
}

#[wasm_bindgen(js_name = generateSyncMessage)]
pub fn generate_sync_message(sync_state: JsValue, input: Object) -> Result<JsValue, JsValue> {
    get_input(input, |state| {
        let sync_state: SyncState = if let Some(state) =
            serde_wasm_bindgen::from_value::<Option<RawSyncState>>(sync_state)?
        {
            SyncState::try_from(state).map_err(to_js_err)?
        } else {
            SyncState::default()
        };

        let (sync_state, message) = state.0.generate_sync_message(sync_state);
        let result = Array::new();
        let p =
            serde_wasm_bindgen::to_value(&RawSyncState::try_from(sync_state).map_err(to_js_err)?)?;
        result.push(&p);
        let message = if let Some(message) = message {
            Uint8Array::from(message.encode().map_err(to_js_err)?.as_slice()).into()
        } else {
            JsValue::NULL
        };
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
    let mut state: State = get_state(&input)?;

    let binary_message = Uint8Array::from(message).to_vec();
    let message = SyncMessage::decode(&binary_message).map_err(to_js_err)?;
    let sync_state: SyncState =
        if let Some(state) = serde_wasm_bindgen::from_value::<Option<RawSyncState>>(sync_state)? {
            SyncState::try_from(state).map_err(to_js_err)?
        } else {
            SyncState::default()
        };

    let (sync_state, patch) = match state.0.receive_sync_message(message, sync_state) {
        Ok(r) => r,
        Err(err) => {
            input.set_state(state);
            return Err(to_js_err(err));
        }
    };

    let result = Array::new();
    let sync_state =
        serde_wasm_bindgen::to_value(&RawSyncState::try_from(sync_state).map_err(to_js_err)?)?;
    result.push(&sync_state);

    if patch.is_some() {
        let heads = state.0.get_heads();
        let new_state = wrapper(state, false, heads);
        // the receiveSyncMessage in automerge.js returns the original doc when there is no patch so we should only freeze it when there is a patch
        input.set_frozen(true);
        result.push(&new_state.into());
    } else {
        input.set_state(state);
        result.push(&input);
    }

    let p = rust_to_js(&patch)?;
    result.push(&p);

    Ok(result.into())
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
    F: FnOnce(&State) -> Result<JsValue, JsValue>,
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

use automerge as am;
use automerge::ChangeHash;
use js_sys::Uint8Array;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use wasm_bindgen::prelude::*;

use crate::interop::{to_js_err, AR, JS};

#[wasm_bindgen]
#[derive(Debug)]
pub struct SyncState(pub(crate) am::SyncState);

#[wasm_bindgen]
impl SyncState {
    #[wasm_bindgen(getter, js_name = sharedHeads)]
    pub fn shared_heads(&self) -> JsValue {
        AR::from(self.0.shared_heads.as_slice()).into()
    }

    #[wasm_bindgen(getter, js_name = lastSentHeads)]
    pub fn last_sent_heads(&self) -> JsValue {
        AR::from(self.0.last_sent_heads.as_slice()).into()
    }

    #[wasm_bindgen(setter, js_name = lastSentHeads)]
    pub fn set_last_sent_heads(&mut self, heads: JsValue) -> Result<(), JsValue> {
        let heads: Vec<ChangeHash> = JS(heads).try_into()?;
        self.0.last_sent_heads = heads;
        Ok(())
    }

    #[wasm_bindgen(setter, js_name = sentHashes)]
    pub fn set_sent_hashes(&mut self, hashes: JsValue) -> Result<(), JsValue> {
        let hashes_map: HashMap<ChangeHash, bool> = hashes.into_serde().map_err(to_js_err)?;
        let hashes_set: HashSet<ChangeHash> = hashes_map.keys().cloned().collect();
        self.0.sent_hashes = hashes_set;
        Ok(())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        SyncState(self.0.clone())
    }

    pub(crate) fn decode(data: Uint8Array) -> Result<SyncState, JsValue> {
        let data = data.to_vec();
        let s = am::SyncState::decode(&data);
        let s = s.map_err(to_js_err)?;
        Ok(SyncState(s))
    }
}

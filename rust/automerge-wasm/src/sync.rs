use automerge as am;
use automerge::ChangeHash;
use js_sys::Uint8Array;
use std::collections::{BTreeSet, HashMap};
use std::convert::TryInto;
use wasm_bindgen::prelude::*;

use crate::interop::{self, to_js_err, AR, JS};

#[wasm_bindgen]
#[derive(Debug)]
pub struct SyncState(pub(crate) am::sync::State);

#[wasm_bindgen]
impl SyncState {
    #[wasm_bindgen(getter, js_name = sharedHeads, unchecked_return_type="Heads")]
    pub fn shared_heads(&self) -> JsValue {
        AR::from(self.0.shared_heads.as_slice()).into()
    }

    #[wasm_bindgen(getter, js_name = lastSentHeads, unchecked_return_type="Heads")]
    pub fn last_sent_heads(&self) -> JsValue {
        AR::from(self.0.last_sent_heads.as_slice()).into()
    }

    #[wasm_bindgen(setter, js_name = lastSentHeads)]
    pub fn set_last_sent_heads(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Heads")] heads: JsValue,
    ) -> Result<(), interop::error::BadChangeHashes> {
        let heads: Vec<ChangeHash> = JS(heads).try_into()?;
        self.0.last_sent_heads = heads;
        Ok(())
    }

    #[wasm_bindgen(setter, js_name = sentHashes)]
    pub fn set_sent_hashes(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Heads")] hashes: JsValue,
    ) -> Result<(), JsValue> {
        let hashes_map: HashMap<ChangeHash, bool> =
            serde_wasm_bindgen::from_value(hashes).map_err(to_js_err)?;
        let hashes_set: BTreeSet<ChangeHash> = hashes_map.keys().cloned().collect();
        self.0.sent_hashes = hashes_set;
        Ok(())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        SyncState(self.0.clone())
    }

    pub(crate) fn decode(data: Uint8Array) -> Result<SyncState, DecodeSyncStateErr> {
        let data = data.to_vec();
        let s = am::sync::State::decode(&data)?;
        Ok(SyncState(s))
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct DecodeSyncStateErr(#[from] automerge::sync::DecodeStateError);

impl From<DecodeSyncStateErr> for JsValue {
    fn from(e: DecodeSyncStateErr) -> Self {
        JsValue::from(e.to_string())
    }
}

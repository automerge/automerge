use std::str::FromStr;

use beelay_core::CommitBundle;
use js_sys::{Array, Uint8Array};
use serde::{parse_commit_hash, parse_commit_hashes};
use wasm_bindgen::prelude::*;

mod console_tracing;
mod serde;

#[wasm_bindgen]
pub struct Beelay(beelay_core::Beelay<rand::rngs::OsRng>);

#[wasm_bindgen]
impl Beelay {
    pub fn create(config: JsValue) -> Result<JsValue, JsValue> {
        console_error_panic_hook::set_once();
        let config = config
            .dyn_into::<js_sys::Object>()
            .map_err(|_| JsValue::from_str("config must be an object"))?;
        let peer_id_str = js_sys::Reflect::get(&config, &JsValue::from_str("peerId"))?
            .as_string()
            .ok_or_else(|| JsValue::from_str("peerId must be a string"))?;
        let peer_id = beelay_core::PeerId::from_str(&peer_id_str)
            .map_err(|e| JsValue::from_str(&format!("invalid peer id: {}", e)))?;
        let rng = rand::rngs::OsRng;
        Ok(Beelay(beelay_core::Beelay::new(peer_id, rng)).into())
    }

    #[wasm_bindgen(js_name = "peerId")]
    pub fn peer_id(&self) -> String {
        self.0.peer_id().to_string()
    }

    #[wasm_bindgen(js_name = "createDocument")]
    pub fn create_doc(&mut self) -> Result<JsValue, JsValue> {
        tracing::debug!("begin_create_doc");
        let (story_id, event) = beelay_core::Event::create_doc();
        self.handle_event(event, story_id.serialize().into())
    }

    #[wasm_bindgen(js_name = "loadDocument")]
    pub fn load_document(&mut self, doc_id: String) -> Result<JsValue, JsValue> {
        let doc_id = beelay_core::DocumentId::from_str(&doc_id)
            .map_err(|e| JsValue::from_str(&format!("invalid document id: {}", e)))?;
        let (story_id, event) = beelay_core::Event::load_doc(doc_id);
        self.handle_event(event, story_id.serialize().into())
    }

    #[wasm_bindgen(js_name = "addCommits")]
    pub fn add_commits(&mut self, doc_id: String, commits: JsValue) -> Result<JsValue, JsValue> {
        let commits = serde::parse_commits(commits)?;
        let doc_id = beelay_core::DocumentId::from_str(&doc_id)
            .map_err(|e| JsValue::from_str(&format!("invalid document id: {}", e)))?;
        let (story_id, event) = beelay_core::Event::add_commits(doc_id, commits);
        self.handle_event(event, story_id.serialize().into())
    }

    #[wasm_bindgen(js_name = "addBundle")]
    pub fn add_bundle(
        &mut self,
        doc_id: String,
        start: JsValue,
        end: JsValue,
        checkpoints: JsValue,
        contents: JsValue,
    ) -> Result<JsValue, JsValue> {
        let doc_id = beelay_core::DocumentId::from_str(&doc_id)
            .map_err(|e| JsValue::from_str(&format!("invalid document id: {}", e)))?;
        let start = parse_commit_hash(start)
            .map_err(|e| JsValue::from_str(&format!("invalid start hash: {:?}", e)))?;
        let end = parse_commit_hash(end)
            .map_err(|e| JsValue::from_str(&format!("invalid end hash: {:?}", e)))?;
        let checkpoints = parse_commit_hashes(checkpoints)
            .map_err(|e| JsValue::from_str(&format!("invalid checkpoints: {:?}", e)))?;
        let contents: Uint8Array = contents
            .dyn_into()
            .map_err(|_| JsValue::from_str("contents must be a Uint8Array"))?;
        let bundle = CommitBundle::builder()
            .start(start)
            .end(end)
            .checkpoints(checkpoints)
            .bundled_commits(contents.to_vec())
            .build();
        let (story_id, event) = beelay_core::Event::add_bundle(doc_id, bundle);
        self.handle_event(event, story_id.serialize().into())
    }

    #[wasm_bindgen(js_name = "syncCollection")]
    pub fn sync_collection(&mut self, doc_id: String) -> Result<JsValue, JsValue> {
        let doc_id = beelay_core::DocumentId::from_str(&doc_id)
            .map_err(|e| JsValue::from_str(&format!("invalid document id: {}", e)))?;
        let (story_id, event) = beelay_core::Event::begin_collection_sync(doc_id);
        self.handle_event(event, story_id.serialize().into())
    }

    #[wasm_bindgen(js_name = "addLink")]
    pub fn add_link(&mut self, from_doc_id: String, to_doc_id: String) -> Result<JsValue, JsValue> {
        let from_doc_id = beelay_core::DocumentId::from_str(&from_doc_id)
            .map_err(|e| JsValue::from_str(&format!("invalid from document id: {}", e)))?;
        let to_doc_id = beelay_core::DocumentId::from_str(&to_doc_id)
            .map_err(|e| JsValue::from_str(&format!("invalid to document id: {}", e)))?;
        let (story_id, event) = beelay_core::Event::add_link(beelay_core::AddLink {
            from: from_doc_id,
            to: to_doc_id,
        });
        self.handle_event(event, story_id.serialize().into())
    }

    #[wasm_bindgen(js_name = "peerConnected")]
    pub fn peer_connected(&mut self, peer_id: String) -> Result<JsValue, JsValue> {
        let peer_id = beelay_core::PeerId::from_str(&peer_id)
            .map_err(|e| JsValue::from_str(&format!("invalid peer id: {}", e)))?;
        let event = beelay_core::Event::peer_connected(peer_id);
        self.handle_event(event, JsValue::NULL)
    }

    #[wasm_bindgen(js_name = "peerDisconnected")]
    pub fn peer_disconnected(&mut self, peer_id: String) -> Result<JsValue, JsValue> {
        let peer_id = beelay_core::PeerId::from_str(&peer_id)
            .map_err(|e| JsValue::from_str(&format!("invalid peer id: {}", e)))?;
        let event = beelay_core::Event::peer_disconnected(peer_id);
        self.handle_event(event, JsValue::NULL)
    }

    #[wasm_bindgen(js_name = "receiveMessage")]
    pub fn receive_message(&mut self, message: JsValue) -> Result<JsValue, JsValue> {
        let message_obj = message
            .dyn_into::<js_sys::Object>()
            .map_err(|_| JsValue::from_str("message is not an object"))?;
        let message = serde::parse_envelope(message_obj)?;
        let event = beelay_core::Event::receive(message);
        self.handle_event(event, JsValue::NULL)
    }

    #[wasm_bindgen(js_name = "loadComplete")]
    pub fn load_complete(&mut self, task_id: String, result: JsValue) -> Result<JsValue, JsValue> {
        let task_id = task_id
            .parse()
            .map_err(|_| JsValue::from_str("invalid task id"))?;
        let bytes = if result.is_undefined() {
            None
        } else {
            Some(
                result
                    .dyn_into::<js_sys::Uint8Array>()
                    .map_err(|_| JsValue::from_str("result is not a Uint8Array"))?
                    .to_vec(),
            )
        };
        let event =
            beelay_core::Event::io_complete(beelay_core::io::IoResult::load(task_id, bytes));
        self.handle_event(event, JsValue::NULL)
    }

    #[wasm_bindgen(js_name = "loadRangeComplete")]
    pub fn load_range_complete(
        &mut self,
        task_id: String,
        result: JsValue,
    ) -> Result<JsValue, JsValue> {
        let task_id = task_id
            .parse()
            .map_err(|_| JsValue::from_str("invalid task id"))?;
        let result = serde::parse_load_range_result(result)?;
        let event =
            beelay_core::Event::io_complete(beelay_core::io::IoResult::load_range(task_id, result));
        self.handle_event(event, JsValue::NULL)
    }

    #[wasm_bindgen(js_name = "putComplete")]
    pub fn put_complete(&mut self, task_id: String) -> Result<JsValue, JsValue> {
        let task_id = task_id
            .parse()
            .map_err(|_| JsValue::from_str("invalid task id"))?;
        let event = beelay_core::Event::io_complete(beelay_core::io::IoResult::put(task_id));
        self.handle_event(event, JsValue::NULL)
    }

    #[wasm_bindgen(js_name = "deleteComplete")]
    pub fn delete_complete(&mut self, task_id: String) -> Result<JsValue, JsValue> {
        let task_id = task_id
            .parse()
            .map_err(|_| JsValue::from_str("invalid task id"))?;
        let event = beelay_core::Event::io_complete(beelay_core::io::IoResult::delete(task_id));
        self.handle_event(event, JsValue::NULL)
    }

    #[wasm_bindgen(js_name = "inspectMessage")]
    pub fn inspect_message(payload: JsValue) -> Result<JsValue, JsValue> {
        let payload = payload
            .dyn_into::<js_sys::Uint8Array>()
            .map_err(|_| JsValue::from_str("payload is not a Uint8Array"))?;
        let payload = match beelay_core::Payload::try_from(payload.to_vec().as_slice()) {
            Ok(payload) => payload,
            Err(e) => return Err(JsValue::from_str(&format!("invalid payload: {}", e))),
        };
        serde_wasm_bindgen::to_value(&payload).map_err(|e| JsValue::from_str(&format!("{}", e)))
    }

    fn handle_event(
        &mut self,
        event: beelay_core::Event,
        first_result: JsValue,
    ) -> Result<JsValue, JsValue> {
        let events = self
            .0
            .handle_event(event)
            .map_err(|e| JsValue::from_str(&format!("error running event: {}", e)))?;
        let result = Array::new();
        result.push(&first_result);
        result.push(&serde::serialize_event_results(events));
        Ok(result.into())
    }
}

#[wasm_bindgen]
pub fn init_logging() {
    wasm_tracing::set_as_global_default_with_config(
        wasm_tracing::WASMLayerConfigBuilder::new()
            .set_console_config(wasm_tracing::ConsoleConfig::ReportWithoutConsoleColor)
            .set_max_level(tracing::Level::TRACE)
            .build(),
    );
    console_error_panic_hook::set_once();
    //tracing_subscriber::fmt()
    //.with_writer(console_tracing::MakeConsoleWriter)
    //.with_max_level(tracing::Level::TRACE)
    //.without_time()
    //.init();
}

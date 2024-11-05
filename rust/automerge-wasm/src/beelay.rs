use std::{collections::HashSet, str::FromStr};

use beelay_core::CommitBundle;
use js_sys::{Array, Uint8Array};
use serde::{parse_commit_hash, parse_commit_hashes};
use tracing_subscriber::{filter::FilterFn, layer::SubscriberExt, util::SubscriberInitExt};
use wasm_bindgen::prelude::*;
use web_sys::console;

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
        let start = if start.is_null() {
            None
        } else {
            Some(
                parse_commit_hash(start)
                    .map_err(|e| JsValue::from_str(&format!("invalid start hash: {:?}", e)))?,
            )
        };
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

    #[wasm_bindgen(js_name = "syncDoc")]
    pub fn sync_doc(&mut self, doc_id: String, peer: String) -> Result<JsValue, JsValue> {
        let doc_id = beelay_core::DocumentId::from_str(&doc_id)
            .map_err(|e| JsValue::from_str(&format!("invalid document id: {}", e)))?;
        let peer_id = beelay_core::PeerId::from_str(&peer)
            .map_err(|e| JsValue::from_str(&format!("invalid peer id: {}", e)))?;
        let (story_id, event) = beelay_core::Event::sync_doc(doc_id, peer_id);
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

    pub fn listen(&mut self, to_peer: JsValue, starting_from: JsValue) -> Result<JsValue, JsValue> {
        let to_peer = to_peer
            .as_string()
            .ok_or_else(|| JsValue::from_str("to_peer must be a string"))?;
        let to_peer = beelay_core::PeerId::from_str(&to_peer)
            .map_err(|e| JsValue::from_str(&format!("bad peer id: {:?}", e)))?;
        let starting_from = starting_from
            .as_string()
            .ok_or_else(|| JsValue::from_str("starting_from must be a string"))?;
        let starting_from = beelay_core::SnapshotId::from_str(&starting_from)
            .map_err(|e| JsValue::from_str(&format!("invalid snapshot id: {:?}", e)))?;
        let (story_id, event) = beelay_core::Event::listen(to_peer, starting_from);
        self.handle_event(event, story_id.serialize().into()).into()
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

    #[wasm_bindgen(js_name = "askComplete")]
    pub fn ask_complete(&mut self, task_id: String, peers: JsValue) -> Result<JsValue, JsValue> {
        let task_id = task_id
            .parse()
            .map_err(|_| JsValue::from_str("invalid task id"))?;
        let peers = peers
            .dyn_into::<js_sys::Array>()
            .map_err(|_| JsValue::from_str("peers is not an array"))?;
        let peers = peers
            .iter()
            .map(|peer| {
                peer.as_string()
                    .ok_or_else(|| JsValue::from_str("peer is not a string"))
                    .and_then(|peer| {
                        beelay_core::PeerId::from_str(&peer)
                            .map_err(|e| JsValue::from_str(&format!("bad peer id: {:?}", e)))
                    })
            })
            .collect::<Result<HashSet<_>, _>>()?;
        let event = beelay_core::Event::io_complete(beelay_core::io::IoResult::ask(task_id, peers));
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

#[allow(unreachable_pub)]
#[wasm_bindgen]
pub fn init_logging(level: JsValue) {
    console_error_panic_hook::set_once();
    let level = level
        .as_string()
        .unwrap_or("trace".to_string())
        .parse()
        .unwrap_or(tracing::Level::TRACE);
    let module_filter = FilterFn::new(|metadata| {
        metadata
            .module_path()
            .map(|p| p.starts_with("beelay"))
            .unwrap_or(false)
    });
    let subscriber = tracing_subscriber::fmt::fmt()
        .with_writer(console_tracing::MakeConsoleWriter)
        .with_max_level(level)
        .without_time()
        .finish();
    let subscriber = subscriber.with(module_filter);
    if let Err(e) = subscriber.try_init() {
        console::warn_1(&JsValue::from(
            format!("unable to set global logger: {:?}", e).as_str(),
        ));
    }
}

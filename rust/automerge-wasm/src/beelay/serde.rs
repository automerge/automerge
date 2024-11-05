mod handle_event_result;
use std::collections::HashMap;

use beelay_core::{Commit, CommitHash, Envelope, PeerId, StorageKey};
pub(crate) use handle_event_result::serialize_event_results;
use js_sys::{Array, Object};
use wasm_bindgen::{JsCast as _, JsValue};

pub(crate) fn parse_envelope(obj: Object) -> Result<Envelope, JsValue> {
    let sender = parse_peer_id(&obj, "sender")?;
    let recipient = parse_peer_id(&obj, "recipient")?;
    let payload_val =
        get_field(&obj, "message")?.ok_or_else(|| JsValue::from_str("Missing 'message' field"))?;
    let payload_uint8array = payload_val
        .dyn_into::<js_sys::Uint8Array>()
        .map_err(|_| JsValue::from_str("'message' is not a Uint8Array"))?;
    let payload = beelay_core::Payload::try_from(payload_uint8array.to_vec().as_ref())
        .map_err(|e| JsValue::from_str(&format!("invalid payload: {}", e)))?;
    Ok(Envelope::new(sender, recipient, payload))
}

pub(crate) fn parse_load_range_result(
    obj: JsValue,
) -> Result<HashMap<StorageKey, Vec<u8>>, JsValue> {
    let result_array: Array = obj
        .dyn_into()
        .map_err(|_| JsValue::from_str("object is not an array"))?;
    let mut result = HashMap::new();
    for i in 0..result_array.length() {
        let obj = &result_array
            .get(i)
            .dyn_into()
            .map_err(|_| JsValue::from_str("not an object"))?;
        let key_array: Array = get_field(obj, "key")?
            .ok_or_else(|| JsValue::from_str("Missing 'key' field"))?
            .dyn_into()
            .map_err(|_| JsValue::from_str("not an array"))?;
        let key = key_array
            .iter()
            .map(|v| {
                v.as_string()
                    .ok_or_else(|| JsValue::from_str("not a string"))
            })
            .collect::<Result<Vec<String>, JsValue>>()?;
        let storage_key = StorageKey::try_from(key)
            .map_err(|e| JsValue::from_str(&format!("invalid storage key: {}", e)))?;
        let value: Vec<u8> = get_field(obj, "data")?
            .ok_or_else(|| JsValue::from_str("Missing 'data' field"))?
            .dyn_into::<js_sys::Uint8Array>()
            .map_err(|_| JsValue::from_str("not a Uint8Array"))?
            .to_vec();
        result.insert(storage_key, value);
    }
    Ok(result)
}

pub(crate) fn parse_commit(obj: &Object) -> Result<Commit, JsValue> {
    let hash_str: String = get_field(obj, "hash")?
        .ok_or_else(|| JsValue::from_str("Missing 'hash' field"))?
        .as_string()
        .ok_or_else(|| JsValue::from_str("'hash' field is not a string"))?;
    let hash = hash_str
        .parse()
        .map_err(|_| JsValue::from_str(&format!("'{}' is not a valid CommitHash", hash_str)))?;
    let parents: Array = get_field(obj, "parents")?
        .ok_or_else(|| JsValue::from_str("Missing 'parents' field"))?
        .dyn_into()
        .map_err(|_| JsValue::from_str("'parents' is not an array"))?;
    let parents = parents
        .iter()
        .map(|v| {
            v.as_string()
                .ok_or_else(|| JsValue::from_str("not a string"))?
                .parse::<CommitHash>()
                .map_err(|_| JsValue::from_str("not a valid CommitHash"))
        })
        .collect::<Result<Vec<_>, JsValue>>()?;
    let contents: Vec<u8> = get_field(obj, "contents")?
        .ok_or_else(|| JsValue::from_str("Missing 'contents' field"))?
        .dyn_into::<js_sys::Uint8Array>()
        .map_err(|_| JsValue::from_str("'contents' is not a Uint8Array"))?
        .to_vec();
    Ok(Commit::new(parents, contents, hash))
}

pub(crate) fn parse_commits(v: JsValue) -> Result<Vec<Commit>, JsValue> {
    let commits_array: Array = v
        .dyn_into()
        .map_err(|_| JsValue::from_str("not an array"))?;
    let mut commits = Vec::with_capacity(commits_array.length() as usize);
    for i in 0..commits_array.length() {
        let commit: Commit = parse_commit(
            &commits_array
                .get(i)
                .dyn_into()
                .map_err(|_| JsValue::from_str("not an object"))?,
        )?;
        commits.push(commit);
    }
    Ok(commits)
}

fn parse_peer_id(obj: &Object, field: &str) -> Result<PeerId, JsValue> {
    get_field(obj, field)?
        .ok_or_else(|| JsValue::from_str(&format!("Missing '{}' field", field)))?
        .as_string()
        .ok_or_else(|| JsValue::from_str(&format!("'{}' is not a string", field)))?
        .parse()
        .map_err(|_| JsValue::from_str(&format!("'{}' is not a valid PeerId", field)))
}

fn get_field(obj: &Object, field: &str) -> Result<Option<JsValue>, JsValue> {
    let result = js_sys::Reflect::get(obj, &JsValue::from_str(field))
        .map_err(|_| format!("error getting field '{}'", field))?;
    if result.is_undefined() {
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

pub(crate) fn parse_commit_hash(v: JsValue) -> Result<CommitHash, JsValue> {
    let hash_str = v
        .as_string()
        .ok_or_else(|| JsValue::from_str("not a string"))?;
    hash_str
        .parse()
        .map_err(|_| JsValue::from_str(&format!("'{}' is not a valid CommitHash", hash_str)))
}

pub(crate) fn parse_commit_hashes(v: JsValue) -> Result<Vec<CommitHash>, JsValue> {
    let hashes_array: Array = v
        .dyn_into()
        .map_err(|_| JsValue::from_str("not an array"))?;
    let mut hashes = Vec::with_capacity(hashes_array.length() as usize);
    for i in 0..hashes_array.length() {
        let hash: CommitHash = parse_commit_hash(hashes_array.get(i))
            .map_err(|e| format!("error parsing CommitHash at index {}: {:?}", i, e))?;
        hashes.push(hash);
    }
    Ok(hashes)
}

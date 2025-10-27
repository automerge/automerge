use crate::error::InsertObject;
use crate::export_cache::CachedObject;
use crate::value::Datatype;
use crate::{Automerge, UpdateSpansArgs};
use am::sync::{Capability, ChunkList, MessageVersion};
use automerge as am;
use automerge::iter::{Span, Spans};
use automerge::marks::{MarkSet, UpdateSpansConfig};
use automerge::ReadDoc;
use automerge::ROOT;
use automerge::{Change, ChangeHash, ObjType, Prop};
use js_sys::{Array, BigInt, Function, JsString, Number, Object, Reflect, Uint8Array};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Display;
use std::ops::Deref;
use std::ops::Range;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use am::{marks::ExpandMark, CursorPosition, MoveCursor, ObjId, Patch, PatchAction, Value};

pub(crate) use crate::export_cache::ExportCache;

pub(crate) struct JS(pub(crate) JsValue);
pub(crate) struct AR(pub(crate) Array);

impl Deref for JS {
    type Target = JsValue;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<AR> for JsValue {
    fn from(ar: AR) -> Self {
        ar.0.into()
    }
}

impl From<AR> for Array {
    fn from(ar: AR) -> Self {
        ar.0
    }
}

impl From<JS> for JsValue {
    fn from(js: JS) -> Self {
        js.0
    }
}

impl AsRef<JsValue> for JS {
    fn as_ref(&self) -> &JsValue {
        &self.0
    }
}

impl<'a> From<&am::ChangeMetadata<'a>> for JS {
    fn from(c: &am::ChangeMetadata<'a>) -> Self {
        let change = Object::new();
        let message = c
            .message
            .as_deref()
            .map(JsValue::from)
            .unwrap_or(JsValue::NULL);
        js_set(&change, "actor", c.actor.to_string()).unwrap();
        js_set(&change, "seq", c.seq as f64).unwrap();
        js_set(&change, "startOp", c.start_op as f64).unwrap();
        js_set(&change, "maxOp", c.max_op as f64).unwrap();
        js_set(&change, "time", c.timestamp as f64).unwrap();
        js_set(&change, "message", message).unwrap();
        js_set(&change, "deps", AR::from(c.deps.as_slice())).unwrap();
        js_set(&change, "hash", c.hash.to_string()).unwrap();
        JS(change.into())
    }
}

impl From<am::sync::State> for JS {
    fn from(state: am::sync::State) -> Self {
        let shared_heads: JS = state.shared_heads.into();
        let last_sent_heads: JS = state.last_sent_heads.into();
        let their_heads: JS = state.their_heads.into();
        let their_need: JS = state.their_need.into();
        let sent_hashes: JS = state.sent_hashes.into();
        let their_have = if let Some(have) = &state.their_have {
            JsValue::from(AR::from(have.as_slice()).0)
        } else {
            JsValue::null()
        };
        let have_responded = state.have_responded.into();
        let result: JsValue = Object::new().into();
        // we can unwrap here b/c we made the object and know its not frozen
        Reflect::set(&result, &"sharedHeads".into(), &shared_heads.0).unwrap();
        Reflect::set(&result, &"lastSentHeads".into(), &last_sent_heads.0).unwrap();
        Reflect::set(&result, &"theirHeads".into(), &their_heads.0).unwrap();
        Reflect::set(&result, &"theirNeed".into(), &their_need.0).unwrap();
        Reflect::set(&result, &"theirHave".into(), &their_have).unwrap();
        Reflect::set(&result, &"sentHashes".into(), &sent_hashes.0).unwrap();
        Reflect::set(&result, &"inFlight".into(), &state.in_flight.into()).unwrap();
        Reflect::set(&result, &"haveResponded".into(), &have_responded).unwrap();
        if let Some(caps) = state.their_capabilities {
            Reflect::set(
                &result,
                &"theirCapabilities".into(),
                &AR::from(&caps[..]).into(),
            )
            .unwrap();
        }
        JS(result)
    }
}

impl From<Vec<ChangeHash>> for JS {
    fn from(heads: Vec<ChangeHash>) -> Self {
        JS(heads
            .iter()
            .map(|h| JsValue::from_str(&h.to_string()))
            .collect::<Array>()
            .into())
    }
}

impl From<HashSet<ChangeHash>> for JS {
    fn from(heads: HashSet<ChangeHash>) -> Self {
        let result: JsValue = Object::new().into();
        for key in &heads {
            Reflect::set(&result, &key.to_string().into(), &true.into()).unwrap();
        }
        JS(result)
    }
}

impl From<BTreeSet<ChangeHash>> for JS {
    fn from(heads: BTreeSet<ChangeHash>) -> Self {
        let result: JsValue = Object::new().into();
        for key in &heads {
            Reflect::set(&result, &key.to_string().into(), &true.into()).unwrap();
        }
        JS(result)
    }
}

impl From<Option<Vec<ChangeHash>>> for JS {
    fn from(heads: Option<Vec<ChangeHash>>) -> Self {
        if let Some(v) = heads {
            let v: Array = v
                .iter()
                .map(|h| JsValue::from_str(&h.to_string()))
                .collect();
            JS(v.into())
        } else {
            JS(JsValue::null())
        }
    }
}

impl TryFrom<JS> for usize {
    type Error = error::BadNumber;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        value.as_f64().map(|n| n as usize).ok_or(error::BadNumber)
    }
}

impl TryFrom<JS> for CursorPosition {
    type Error = error::BadCursorPosition;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        match value.as_f64() {
            Some(idx) => {
                if idx < 0f64 {
                    Ok(CursorPosition::Start)
                } else {
                    Ok(CursorPosition::Index(idx as usize))
                }
            }
            None => value
                .as_string()
                .and_then(|s| match s.as_str() {
                    "start" => Some(CursorPosition::Start),
                    "end" => Some(CursorPosition::End),
                    _ => None,
                })
                .ok_or(error::BadCursorPosition),
        }
    }
}

impl TryFrom<JS> for MoveCursor {
    type Error = error::BadMoveCursor;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.is_undefined() {
            Ok(MoveCursor::default())
        } else {
            value
                .as_string()
                .and_then(|s| match s.as_str() {
                    "before" => Some(MoveCursor::Before),
                    "after" => Some(MoveCursor::After),
                    _ => None,
                })
                .ok_or(error::BadMoveCursor)
        }
    }
}

impl TryFrom<JS> for ExpandMark {
    type Error = error::BadExpand;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.is_undefined() {
            Ok(ExpandMark::default())
        } else {
            value
                .as_string()
                .and_then(|s| match s.as_str() {
                    "before" => Some(ExpandMark::Before),
                    "after" => Some(ExpandMark::After),
                    "both" => Some(ExpandMark::Both),
                    "none" => Some(ExpandMark::None),
                    _ => None,
                })
                .ok_or(error::BadExpand)
        }
    }
}

impl TryFrom<JS> for HashSet<ChangeHash> {
    type Error = error::BadChangeHashSet;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let result = HashSet::new();
        fold_hash_set(result, &value.0, |mut set, hash| {
            set.insert(hash);
            set
        })
    }
}

impl TryFrom<JS> for BTreeSet<ChangeHash> {
    type Error = error::BadChangeHashSet;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let result = BTreeSet::new();
        fold_hash_set(result, &value.0, |mut set, hash| {
            set.insert(hash);
            set
        })
    }
}

fn fold_hash_set<F, O>(init: O, val: &JsValue, f: F) -> Result<O, error::BadChangeHashSet>
where
    F: Fn(O, ChangeHash) -> O,
{
    let mut result = init;
    for key in Reflect::own_keys(val)
        .map_err(|_| error::BadChangeHashSet::ListProp)?
        .iter()
    {
        if let Some(true) = js_get(val, &key)?.0.as_bool() {
            let hash = ChangeHash::try_from(JS(key.clone()))
                .map_err(|e| error::BadChangeHashSet::BadHash(key, e))?;
            result = f(result, hash);
        }
    }
    Ok(result)
}

impl TryFrom<JS> for ChangeHash {
    type Error = error::BadChangeHash;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if let Some(s) = value.0.as_string() {
            Ok(s.parse()?)
        } else {
            Err(error::BadChangeHash::NotString)
        }
    }
}

impl TryFrom<JS> for Option<Vec<ChangeHash>> {
    type Error = error::BadChangeHashes;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.0.is_null() {
            Ok(None)
        } else {
            Vec::<ChangeHash>::try_from(value).map(Some)
        }
    }
}

impl TryFrom<JS> for Vec<ChangeHash> {
    type Error = error::BadChangeHashes;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value
            .0
            .dyn_into::<Array>()
            .map_err(|_| error::BadChangeHashes::NotArray)?;
        let value = value
            .iter()
            .enumerate()
            .map(|(i, v)| {
                ChangeHash::try_from(JS(v)).map_err(|e| error::BadChangeHashes::BadElem(i, e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(value)
    }
}

impl TryFrom<JS> for Vec<Change> {
    type Error = error::BadJSChanges;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value
            .0
            .dyn_into::<Array>()
            .map_err(|_| error::BadJSChanges::ChangesNotArray)?;
        let changes = value
            .iter()
            .enumerate()
            .map(|(i, j)| {
                j.dyn_into().map_err::<error::BadJSChanges, _>(|_| {
                    error::BadJSChanges::ElemNotUint8Array(i)
                })
            })
            .collect::<Result<Vec<Uint8Array>, _>>()?;
        let changes = changes
            .iter()
            .enumerate()
            .map(|(i, arr)| {
                automerge::Change::try_from(arr.to_vec().as_slice())
                    .map_err(|e| error::BadJSChanges::BadChange(i, e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(changes)
    }
}

impl TryFrom<JS> for Vec<u8> {
    type Error = error::BadUint8Array;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value
            .0
            .dyn_into::<Uint8Array>()
            .map_err(|_| error::BadUint8Array)?;
        Ok(value.to_vec())
    }
}

impl TryFrom<JS> for am::sync::State {
    type Error = error::BadSyncState;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value.0;
        let shared_heads = js_get(&value, "sharedHeads")?
            .try_into()
            .map_err(error::BadSyncState::BadSharedHeads)?;
        let last_sent_heads = js_get(&value, "lastSentHeads")?
            .try_into()
            .map_err(error::BadSyncState::BadLastSentHeads)?;
        let their_heads = js_get(&value, "theirHeads")?
            .try_into()
            .map_err(error::BadSyncState::BadTheirHeads)?;
        let their_need = js_get(&value, "theirNeed")?
            .try_into()
            .map_err(error::BadSyncState::BadTheirNeed)?;
        let their_have = js_get(&value, "theirHave")?
            .try_into()
            .map_err(error::BadSyncState::BadTheirHave)?;
        let sent_hashes = js_get(&value, "sentHashes")?
            .try_into()
            .map_err(error::BadSyncState::BadSentHashes)?;
        let in_flight = js_get(&value, "inFlight")?
            .0
            .as_bool()
            .ok_or(error::BadSyncState::InFlightNotBoolean)?;
        let have_responded = js_get(&value, "haveResponded")?
            .0
            .as_bool()
            .unwrap_or(false);
        let their_capabilities = {
            let caps_obj = js_get(&value, "theirCapabilities")?;
            if !caps_obj.is_undefined() {
                caps_obj
                    .try_into()
                    .map_err(error::BadSyncState::BadTheirCapabilities)?
            } else {
                None
            }
        };
        Ok(am::sync::State {
            shared_heads,
            last_sent_heads,
            their_heads,
            their_need,
            their_have,
            sent_hashes,
            in_flight,
            have_responded,
            their_capabilities,
        })
    }
}

impl TryFrom<JS> for am::sync::Have {
    type Error = error::BadHave;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let last_sync = js_get(&value.0, "lastSync")?
            .try_into()
            .map_err(error::BadHave::BadLastSync)?;
        let bloom = js_get(&value.0, "bloom")?
            .try_into()
            .map_err(error::BadHave::BadBloom)?;
        Ok(am::sync::Have { last_sync, bloom })
    }
}

impl TryFrom<JS> for Option<Vec<am::sync::Have>> {
    type Error = error::BadHaves;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.0.is_null() {
            Ok(None)
        } else {
            Ok(Some(value.try_into()?))
        }
    }
}

impl TryFrom<JS> for Vec<am::sync::Have> {
    type Error = error::BadHaves;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value
            .0
            .dyn_into::<Array>()
            .map_err(|_| error::BadHaves::NotArray)?;
        let have = value
            .iter()
            .enumerate()
            .map(|(i, s)| JS(s).try_into().map_err(|e| error::BadHaves::BadElem(i, e)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(have)
    }
}

impl TryFrom<JS> for am::sync::BloomFilter {
    type Error = error::BadBloom;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value: Uint8Array = value
            .0
            .dyn_into()
            .map_err(|_| error::BadBloom::NotU8Array)?;
        let value = value.to_vec();
        let value = value.as_slice().try_into()?;
        Ok(value)
    }
}

impl TryFrom<JS> for am::sync::Message {
    type Error = error::BadSyncMessage;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let heads = js_get(&value.0, "heads")?
            .try_into()
            .map_err(error::BadSyncMessage::BadHeads)?;
        let need = js_get(&value.0, "need")?
            .try_into()
            .map_err(error::BadSyncMessage::BadNeed)?;
        let have = js_get(&value.0, "have")?.try_into()?;

        let supported_capabilities = {
            let caps_obj = js_get(&value.0, "supportedCapabilities")?;
            if !caps_obj.is_undefined() {
                caps_obj
                    .try_into()
                    .map_err(error::BadSyncMessage::BadSupportedCapabilities)?
            } else {
                None
            }
        };

        let version = match js_get(&value.0, "type")?.as_string() {
            Some(s) => match s.as_str() {
                "v1" => MessageVersion::V1,
                "v2" => MessageVersion::V2,
                _ => MessageVersion::V1,
            },
            None => MessageVersion::V1,
        };

        let changes_obj = js_get(&value.0, "changes")?;
        if changes_obj.is_undefined() {
            return Err(error::BadSyncMessage::MissingChanges);
        }

        let changes = changes_obj
            .try_into()
            .map_err(error::BadSyncMessage::BadJSChanges)?;

        Ok(am::sync::Message {
            heads,
            need,
            have,
            changes,
            supported_capabilities,
            version,
        })
    }
}

impl TryFrom<JS> for Option<Datatype> {
    type Error = crate::value::InvalidDatatype;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.0.is_null() || value.0.is_undefined() {
            return Ok(None);
        }
        Datatype::try_from(value.0).map(Some)
    }
}

impl From<Vec<ChangeHash>> for AR {
    fn from(values: Vec<ChangeHash>) -> Self {
        AR(values
            .iter()
            .map(|h| JsValue::from_str(&h.to_string()))
            .collect())
    }
}

impl From<&[String]> for AR {
    fn from(value: &[String]) -> Self {
        AR(value.iter().map(JsValue::from).collect())
    }
}

impl From<&[ChangeHash]> for AR {
    fn from(value: &[ChangeHash]) -> Self {
        AR(value
            .iter()
            .map(|h| JsValue::from_str(&h.to_string()))
            .collect())
    }
}

impl From<&[Change]> for AR {
    fn from(value: &[Change]) -> Self {
        let changes: Array = value
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        AR(changes)
    }
}

impl From<&ChunkList> for AR {
    fn from(value: &ChunkList) -> Self {
        let chunks: Array = value.iter().map(Uint8Array::from).collect();
        AR(chunks)
    }
}

impl From<&[am::sync::Have]> for AR {
    fn from(value: &[am::sync::Have]) -> Self {
        AR(value
            .iter()
            .map(|have| {
                let last_sync: Array = have
                    .last_sync
                    .iter()
                    .map(|h| JsValue::from_str(&hex::encode(h.0)))
                    .collect();
                let bloom = Uint8Array::from(have.bloom.to_bytes().as_slice());
                let obj: JsValue = Object::new().into();
                // we can unwrap here b/c we created the object and know its not frozen
                Reflect::set(&obj, &"lastSync".into(), &last_sync.into()).unwrap();
                Reflect::set(&obj, &"bloom".into(), &bloom.into()).unwrap();
                obj
            })
            .collect())
    }
}

impl From<&[am::sync::Capability]> for AR {
    fn from(value: &[am::sync::Capability]) -> Self {
        AR(value
            .iter()
            .filter_map(|c| match c {
                am::sync::Capability::MessageV1 => Some(JsValue::from_str("message-v1")),
                am::sync::Capability::MessageV2 => Some(JsValue::from_str("message-v2")),
                am::sync::Capability::Unknown(_) => None,
            })
            .collect())
    }
}

impl TryFrom<JS> for ChunkList {
    type Error = error::BadChunkList;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value
            .0
            .dyn_into::<Array>()
            .map_err(|_| error::BadChunkList::NotArray)?;
        let value = value
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let chunk = v
                    .dyn_into::<Uint8Array>()
                    .map_err(|_e| error::BadChunkList::ElemNotUint8Array(i))?;
                Ok(chunk.to_vec())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(value.into())
    }
}

impl TryFrom<JS> for Option<Vec<Capability>> {
    type Error = error::BadCapabilities;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        if value.0.is_null() {
            Ok(None)
        } else {
            Vec::<Capability>::try_from(value).map(Some)
        }
    }
}

impl TryFrom<JS> for Vec<Capability> {
    type Error = error::BadCapabilities;

    fn try_from(value: JS) -> Result<Self, Self::Error> {
        let value = value
            .0
            .dyn_into::<Array>()
            .map_err(|_| error::BadCapabilities::NotArray)?;
        let value = value
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let as_str = v
                    .as_string()
                    .ok_or(error::BadCapabilities::ElemNotString(i))?;
                match as_str.as_str() {
                    "message-v1" => Ok(Capability::MessageV1),
                    "message-v2" => Ok(Capability::MessageV2),
                    other => Err(error::BadCapabilities::ElemNotValid(i, other.to_string())),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(value)
    }
}

pub(crate) fn import_span(doc: &Automerge, value: JsValue) -> Result<am::Span, error::InvalidSpan> {
    let Ok(obj) = value.dyn_into::<Object>() else {
        return Err(error::InvalidSpan::NotObjectOrString);
    };
    if let Some(str) = obj.as_string() {
        return Ok(am::Span::Text {
            text: str,
            marks: None,
        });
    }
    let type_str = js_get(&obj, "type")?;
    let type_str = type_str
        .as_string()
        .ok_or(error::InvalidSpan::TypeNotString)?;
    match type_str.as_str() {
        "text" => {
            let text_value = js_get(&obj, "value")?
                .as_string()
                .ok_or(error::InvalidSpan::TextNotString)?;

            let marks = js_get(&obj, "marks")?;
            let markset = import_marks(marks.0).map_err(error::InvalidSpan::InvalidMarks)?;
            Ok(am::Span::Text {
                text: text_value,
                marks: markset,
            })
        }
        "block" => {
            let value = js_get(&obj, "value")?;
            let hydrate_val = js_val_to_hydrate(doc, value.0);
            let Ok(am::hydrate::Value::Map(map)) = hydrate_val else {
                return Err(error::InvalidSpan::BlockNotObject);
            };
            Ok(am::Span::Block(map))
        }
        other => Err(error::InvalidSpan::InvalidType(other.to_string())),
    }
}

fn import_marks(value: JsValue) -> Result<Option<Arc<MarkSet>>, error::ImportMark> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    let value = value
        .dyn_into::<js_sys::Object>()
        .map_err(|_| error::ImportMark::NotObject)?;
    let kvs = js_sys::Object::entries(&value);

    let mark_pairs = kvs
        .iter()
        .map(|kv| {
            let kv = kv
                .dyn_into::<Array>()
                .expect("entries returns an iterator of arrays");
            let key = kv.get(0).as_string().expect("keys are strings");
            let value = kv.get(1);
            let value = import_scalar(&value, None)
                .map_err(|_| error::ImportMark::InvalidValue(key.clone()))?;

            Ok::<_, error::ImportMark>((key, value))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let marks = MarkSet::from_iter(mark_pairs);
    if marks.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(marks)))
    }
}

pub(crate) fn import_update_spans_args(
    doc: &Automerge,
    value: JS,
) -> Result<UpdateSpansArgs, error::InvalidUpdateSpansArgs> {
    let value = value
        .0
        .dyn_into::<Array>()
        .map_err(|_| error::InvalidUpdateSpansArgs::NotArray)?;
    let mut values = Vec::new();
    for (i, v) in value.into_iter().enumerate() {
        let span =
            import_span(doc, v).map_err(|e| error::InvalidUpdateSpansArgs::InvalidElement(i, e))?;
        values.push(span);
    }
    Ok(UpdateSpansArgs(values))
}

pub(crate) fn import_update_spans_config(
    value: JsValue,
) -> Result<UpdateSpansConfig, error::ImportUpdateSpansConfig> {
    if value.is_undefined() || value.is_null() {
        return Ok(UpdateSpansConfig::default());
    }
    let value = value
        .dyn_into::<Object>()
        .map_err(|_| error::ImportUpdateSpansConfig::NotObject)?;
    let default_expand = js_get(&value, "defaultExpand")?
        .try_into()
        .map_err(error::ImportUpdateSpansConfig::BadDefaultExpand)?;

    let mut config = UpdateSpansConfig::default().with_default_expand(default_expand);

    let per_mark_expands = js_get(&value, "perMarkExpand")?;
    if per_mark_expands.0.is_undefined() || per_mark_expands.0.is_null() {
        return Ok(config);
    }

    if let Ok(obj) = per_mark_expands.0.dyn_into::<Object>() {
        let kvs = js_sys::Object::entries(&obj);
        for kv in kvs.iter() {
            let kv = kv
                .dyn_into::<Array>()
                .expect("entries returns an iterator of arrays");
            let key = kv.get(0).as_string().expect("keys are strings");
            let value = kv.get(1);
            let expand: ExpandMark = JS(value).try_into().map_err(|e| {
                error::ImportUpdateSpansConfig::BadPerMarkExpand {
                    key: key.clone(),
                    error: e,
                }
            })?;
            config = config.with_mark_expand(key, expand);
        }
    }

    Ok(config)
}

pub(crate) fn to_js_err<T: Display>(err: T) -> JsValue {
    js_sys::Error::new(&std::format!("{}", err)).into()
}

pub(crate) fn js_get<J: Into<JsValue>, S: std::fmt::Debug + Into<JsValue>>(
    obj: J,
    prop: S,
) -> Result<JS, error::GetProp> {
    let prop = prop.into();
    Ok(JS(Reflect::get(&obj.into(), &prop).map_err(|e| {
        error::GetProp {
            property: format!("{:?}", prop),
            error: e,
        }
    })?))
}

pub(crate) fn js_set<V: Into<JsValue>, S: std::fmt::Debug + Into<JsValue>>(
    obj: &JsValue,
    property: S,
    val: V,
) -> Result<bool, error::SetProp> {
    let property = property.into();
    let val = val.into();
    Reflect::set(obj, &property, &val).map_err(|error| error::SetProp { property, error })
}

pub(crate) fn to_prop(p: JsValue) -> Result<Prop, error::InvalidProp> {
    if let Some(s) = p.as_string() {
        Ok(Prop::Map(s))
    } else if let Some(n) = p.as_f64() {
        Ok(Prop::Seq(n as usize))
    } else {
        Err(error::InvalidProp)
    }
}

#[derive(Debug)]
pub(crate) enum JsObjType {
    Text(String),
    Map(Vec<(Prop, JsValue)>),
    List(Vec<(Prop, JsValue)>),
}

impl JsObjType {
    pub(crate) fn objtype(&self) -> ObjType {
        match self {
            Self::Text(_) => ObjType::Text,
            Self::Map(_) => ObjType::Map,
            Self::List(_) => ObjType::List,
        }
    }

    pub(crate) fn text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s.as_ref()),
            Self::Map(_) => None,
            Self::List(_) => None,
        }
    }

    pub(crate) fn subvals(&self) -> impl Iterator<Item = (Cow<'_, Prop>, JsValue)> + '_ + Clone {
        match self {
            Self::Text(s) => SubValIter::Str(s.chars().enumerate()),
            Self::Map(sub) => SubValIter::Slice(sub.as_slice().iter()),
            Self::List(sub) => SubValIter::Slice(sub.as_slice().iter()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SubValIter<'a> {
    Slice(std::slice::Iter<'a, (Prop, JsValue)>),
    Str(std::iter::Enumerate<std::str::Chars<'a>>),
}

impl<'a> Iterator for SubValIter<'a> {
    type Item = (std::borrow::Cow<'a, Prop>, JsValue);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Slice(i) => i
                .next()
                .map(|(p, v)| (std::borrow::Cow::Borrowed(p), v.clone())),
            Self::Str(i) => i
                .next()
                .map(|(n, c)| (std::borrow::Cow::Owned(Prop::Seq(n)), c.to_string().into())),
        }
    }
}

pub(crate) fn import_obj(
    value: &JsValue,
    datatype: Option<Datatype>,
) -> Result<JsObjType, InsertObject> {
    match datatype {
        Some(Datatype::Map) => {
            let map = value
                .clone()
                .dyn_into::<js_sys::Object>()
                .map_err(|_| InsertObject::ValueNotObject)?;
            let map = js_sys::Object::keys(&map)
                .iter()
                .zip(js_sys::Object::values(&map).iter())
                .map(|(key, val)| (key.as_string().unwrap().into(), val))
                .collect();
            Ok(JsObjType::Map(map))
        }
        Some(Datatype::List) => {
            let list = value
                .clone()
                .dyn_into::<js_sys::Array>()
                .map_err(|_| InsertObject::ValueNotObject)?;
            let list = list
                .iter()
                .enumerate()
                .map(|(i, e)| (i.into(), e))
                .collect();
            Ok(JsObjType::List(list))
        }
        Some(Datatype::Text) => {
            let text = value.as_string().ok_or(InsertObject::ValueNotObject)?;
            Ok(JsObjType::Text(text))
        }
        Some(_) => Err(InsertObject::ValueNotObject),
        None => {
            if let Ok(list) = value.clone().dyn_into::<js_sys::Array>() {
                let list = list
                    .iter()
                    .enumerate()
                    .map(|(i, e)| (i.into(), e))
                    .collect();
                Ok(JsObjType::List(list))
            } else if let Ok(map) = value.clone().dyn_into::<js_sys::Object>() {
                let map = js_sys::Object::keys(&map)
                    .iter()
                    .zip(js_sys::Object::values(&map).iter())
                    .map(|(key, val)| (key.as_string().unwrap().into(), val))
                    .collect();
                Ok(JsObjType::Map(map))
            } else if let Some(s) = value.as_string() {
                Ok(JsObjType::Text(s))
            } else {
                Err(InsertObject::ValueNotObject)
            }
        }
    }
}

pub(crate) fn get_heads(heads: JsValue) -> Result<Option<Vec<ChangeHash>>, error::BadChangeHashes> {
    if heads.is_undefined() || heads.is_null() {
        return Ok(None);
    }
    let Ok(heads) = heads.dyn_into::<js_sys::Array>() else {
        return Err(error::BadChangeHashes::NotArray);
    };
    heads
        .iter()
        .enumerate()
        .map(|(i, v)| {
            ChangeHash::try_from(JS(v)).map_err(|e| error::BadChangeHashes::BadElem(i, e))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

#[derive(Clone, Debug)]
pub(crate) struct ExternalTypeConstructor {
    construct: Function,
    deconstruct: Function,
}

impl ExternalTypeConstructor {
    pub(crate) fn new(construct: Function, deconstruct: Function) -> Self {
        Self {
            construct,
            deconstruct,
        }
    }

    pub(crate) fn construct(
        &self,
        inner_value: &JsValue,
        datatype: Datatype,
    ) -> Result<JsValue, error::Export> {
        self.construct
            .call1(&JsValue::undefined(), inner_value)
            .map_err(|e| error::Export::CallDataHandler(datatype.to_string(), e))
    }

    pub(crate) fn deconstruct(
        &self,
        value: &JsValue,
    ) -> Result<Option<JsValue>, error::ImportValue> {
        let decon_result = self
            .deconstruct
            .call1(&JsValue::undefined(), value)
            .map_err(error::ImportValue::CallDataHandler)?;
        if decon_result.is_undefined() {
            return Ok(None);
        }
        Ok(Some(decon_result))
    }
}

fn bigint_to_scalar(value: BigInt) -> Result<am::ScalarValue, error::ImportValue> {
    let max = std::sync::LazyLock::new(|| BigInt::from(i64::MAX));
    if value > *max {
        Ok(am::ScalarValue::Uint(bigint_to_u64(value)?))
    } else {
        Ok(am::ScalarValue::Int(bigint_to_i64(value)?))
    }
}

fn bigint_to_i64(value: BigInt) -> Result<i64, error::ImportValue> {
    // going bigint -> string -> parse is inefficient but
    // much much simpler b/c othrewise we'd need to bounce it through
    // Number and deal with lossy bits, or mod/bit shift in 32 bit chunks

    let max = std::sync::LazyLock::new(|| BigInt::from(i64::MAX));
    let min = std::sync::LazyLock::new(|| BigInt::from(i64::MIN));

    if value > *max {
        return Err(error::ImportValue::BigIntTooLarge(value, max.clone()));
    }
    if value < *min {
        return Err(error::ImportValue::BigIntTooSmall(value, min.clone()));
    }

    let value = value.to_string(10)?;
    let value = String::from(value).parse::<i64>()?;
    Ok(value)
}

fn jsvalue_to_u64(value: &JsValue) -> Result<u64, error::ImportValue> {
    if BigInt::is_type_of(value) {
        bigint_to_u64(BigInt::from(value.clone()))
    } else if let Some(v) = value.as_f64() {
        Ok(v as u64)
    } else {
        Err(error::ImportValue::Invalid(value.clone()))
    }
}

fn jsvalue_to_i64(value: &JsValue) -> Result<i64, error::ImportValue> {
    if BigInt::is_type_of(value) {
        bigint_to_i64(BigInt::from(value.clone()))
    } else if let Some(v) = value.as_f64() {
        Ok(v as i64)
    } else if let Ok(d) = value.clone().dyn_into::<js_sys::Date>() {
        Ok(d.get_time() as i64)
    } else {
        Err(error::ImportValue::Invalid(value.clone()))
    }
}

fn jsvalue_to_bytes(value: &JsValue) -> Result<Vec<u8>, error::ImportValue> {
    if let Ok(v) = value.clone().dyn_into::<Uint8Array>() {
        Ok(v.to_vec())
    } else {
        Err(error::ImportValue::Invalid(value.clone()))
    }
}

fn bigint_to_u64(value: BigInt) -> Result<u64, error::ImportValue> {
    let max = std::sync::LazyLock::new(|| BigInt::from(u64::MAX));
    let min = std::sync::LazyLock::new(|| BigInt::from(u64::MIN));
    if value > *max {
        return Err(error::ImportValue::BigIntTooLarge(value, max.clone()));
    }
    if value < *min {
        return Err(error::ImportValue::BigIntTooSmall(value, min.clone()));
    }
    let value = value.to_string(10)?;
    let value = String::from(value).parse::<u64>()?;
    Ok(value)
}

pub(crate) fn import_scalar(
    value: &JsValue,
    datatype: Option<Datatype>,
) -> Result<am::ScalarValue, error::ImportValue> {
    match datatype {
        Some(Datatype::Boolean) => value.as_bool().map(am::ScalarValue::Boolean),
        Some(Datatype::Int) => Some(am::ScalarValue::Int(jsvalue_to_i64(value)?)),
        Some(Datatype::Uint) => Some(am::ScalarValue::Uint(jsvalue_to_u64(value)?)),
        Some(Datatype::Str) => value.as_string().map(|v| am::ScalarValue::Str(v.into())),
        Some(Datatype::F64) => value.as_f64().map(am::ScalarValue::F64),
        Some(Datatype::Bytes) => Some(am::ScalarValue::Bytes(jsvalue_to_bytes(value)?)),
        Some(Datatype::Counter) => Some(am::ScalarValue::counter(jsvalue_to_i64(value)?)),
        Some(Datatype::Timestamp) => Some(am::ScalarValue::Timestamp(jsvalue_to_i64(value)?)),
        Some(Datatype::Null) => Some(am::ScalarValue::Null),
        Some(_) => return Err(error::ImportValue::ValueNotPrimitive), // Map, Text, List ...
        None => {
            if value.is_null() {
                Some(am::ScalarValue::Null)
            } else if let Some(b) = value.as_bool() {
                Some(am::ScalarValue::Boolean(b))
            } else if let Some(s) = value.as_string() {
                Some(am::ScalarValue::Str(s.into()))
            } else if BigInt::is_type_of(value) {
                Some(bigint_to_scalar(BigInt::from(value.clone()))?)
            } else if let Some(n) = value.as_f64() {
                if (n.round() >= 1.0) && (n.round() - n).abs() < f64::EPSILON {
                    Some(am::ScalarValue::Int(n as i64))
                } else {
                    Some(am::ScalarValue::F64(n))
                }
            } else if let Ok(d) = value.clone().dyn_into::<js_sys::Date>() {
                Some(am::ScalarValue::Timestamp(d.get_time() as i64))
            } else if let Ok(o) = value.clone().dyn_into::<Uint8Array>() {
                Some(am::ScalarValue::Bytes(o.to_vec()))
            } else {
                None
            }
        }
    }
    .ok_or_else(|| error::ImportValue::Invalid(value.clone()))
}

impl Automerge {
    pub(crate) fn export_value(
        &self,
        (datatype, raw_value): (Datatype, JsValue),
        cache: &ExportCache<'_>,
    ) -> Result<JsValue, error::Export> {
        if let Some(function) = self.external_types.get(&datatype) {
            let wrapped_value = function.construct(&raw_value, datatype)?;
            //web_sys::console::log_1(&format!("wrapped_value: {:?}", wrapped_value).into());
            match wrapped_value.dyn_into::<Object>() {
                Ok(o) => {
                    cache.set_raw_data(&o, &raw_value)?;
                    cache.set_datatype(&o, &datatype.into())?;
                    Ok(o.into())
                }
                Err(val) => match val.dyn_into::<JsString>() {
                    Ok(s) => Ok(s.into()),
                    Err(_) => Err(error::Export::InvalidDataHandler(datatype.to_string())),
                },
            }
        } else {
            Ok(raw_value)
        }
    }

    pub(crate) fn unwrap_object(
        &self,
        ext_val: &Object,
        cache: &mut ExportCache<'_>,
        meta: &JsValue,
    ) -> Result<(bool, CachedObject), error::Export> {
        let id_val = cache.get_raw_object(ext_val)?;
        let id = if id_val.is_undefined() {
            am::ROOT
        } else {
            self.doc.import(&id_val.as_string().unwrap_or_default())?.0
        };

        if let Some(result) = cache.objs.get(&id) {
            return Ok((true, result.clone()));
        }

        let inner = cache.get_raw_data(ext_val)?;

        let datatype_raw = cache.get_datatype(ext_val)?;
        let datatype = datatype_raw.clone().try_into();

        let inner = inner
            .dyn_into::<Object>()
            .unwrap_or_else(|_| ext_val.clone());
        let datatype = datatype.unwrap_or_else(|_| {
            if Array::is_array(&inner) {
                Datatype::List
            } else {
                Datatype::Map
            }
        });

        let inner = shallow_copy(&inner);

        let outer = self.wrap_object_mini(&inner, datatype, cache)?;

        cache.set_hidden(&outer, &id_val, &datatype_raw, meta)?;

        let cached_object = CachedObject {
            inner,
            outer,
            id: id.clone(),
        };

        cache.objs.insert(id, cached_object.clone());

        Ok((false, cached_object))
    }

    pub(crate) fn unwrap_scalar(
        &self,
        ext_val: JsValue,
        cache: &ExportCache<'_>,
    ) -> Result<JsValue, error::Export> {
        let inner = cache.get_raw_data(&ext_val)?;
        if !inner.is_undefined() {
            Ok(inner)
        } else {
            Ok(ext_val)
        }
    }

    fn maybe_wrap_object(
        &self,
        (datatype, raw_value): (Datatype, JsValue),
        id: &ObjId,
        meta: &JsValue,
        cache: &ExportCache<'_>,
    ) -> Result<JsValue, error::Export> {
        if let Ok(obj) = raw_value.clone().dyn_into::<Object>() {
            let result = self.wrap_object(&obj, datatype, id, meta, cache)?;
            Ok(result.into())
        } else {
            self.export_value((datatype, raw_value), cache)
        }
    }

    pub(crate) fn wrap_object_mini(
        &self,
        value: &Object,
        datatype: Datatype,
        cache: &ExportCache<'_>,
    ) -> Result<Object, error::Export> {
        if let Some(constructor) = self.external_types.get(&datatype) {
            let wrapped_value = constructor.construct(value, datatype)?;
            let wrapped_object = wrapped_value
                .dyn_into::<Object>()
                .map_err(|_| error::Export::InvalidDataHandler(datatype.to_string()))?;
            cache.set_raw_data(&wrapped_object, value)?;
            Ok(wrapped_object)
        } else {
            Ok(value.clone())
        }
    }

    pub(crate) fn wrap_object(
        &self,
        value: &Object,
        datatype: Datatype,
        id: &ObjId,
        meta: &JsValue,
        cache: &ExportCache<'_>,
    ) -> Result<Object, error::Export> {
        let value = if let Some(constructor) = self.external_types.get(&datatype) {
            let wrapped_value = constructor.construct(value, datatype)?;
            let wrapped_object = wrapped_value
                .dyn_into::<Object>()
                .map_err(|_| error::Export::InvalidDataHandler(datatype.to_string()))?;
            cache.set_raw_data(&wrapped_object, value)?;
            wrapped_object
        } else {
            value.clone()
        };
        if matches!(datatype, Datatype::Map | Datatype::List) {
            cache.set_raw_object(&value, &JsValue::from(&id.to_string()))?;
        }
        cache.set_datatype(&value, &datatype.into())?;
        cache.set_meta(&value, meta)?;
        if self.freeze {
            Object::freeze(&value);
        }
        Ok(value)
    }

    pub(crate) fn apply_patch_to_array(
        &self,
        array: &Array,
        patch: &Patch,
        meta: &JsValue,
        cache: &ExportCache<'_>,
    ) -> Result<(), error::ApplyPatch> {
        match &patch.action {
            PatchAction::PutSeq { index, value, .. } => {
                let sub_val = self.maybe_wrap_object(alloc(&value.0), &value.1, meta, cache)?;
                js_set(array, *index as f64, &sub_val)?;
                Ok(())
            }
            PatchAction::DeleteSeq { index, length, .. } => {
                self.sub_splice(array, *index, *length, vec![], meta, cache)
            }
            PatchAction::Insert { index, values, .. } => {
                self.sub_splice(array, *index, 0, values, meta, cache)
            }
            PatchAction::Increment { prop, value, .. } => {
                if let Prop::Seq(index) = prop {
                    let index = *index as f64;
                    let old_val = js_get(array, index)?.0;
                    let old_val = self.unwrap_scalar(old_val, cache)?;
                    if let Some(old) = old_val.as_f64() {
                        let new_value: Value<'_> =
                            am::ScalarValue::counter(old as i64 + *value).into();
                        js_set(array, index, &self.export_value(alloc(&new_value), cache)?)?;
                        Ok(())
                    } else {
                        Err(error::ApplyPatch::IncrementNonNumeric)
                    }
                } else {
                    Err(error::ApplyPatch::IncrementKeyInSeq)
                }
            }
            PatchAction::DeleteMap { .. } => Err(error::ApplyPatch::DeleteKeyFromSeq),
            PatchAction::PutMap { .. } => Err(error::ApplyPatch::PutKeyInSeq),
            PatchAction::SpliceText { .. } => Err(error::ApplyPatch::SpliceTextInSeq),
            PatchAction::Mark { .. } => Ok(()),
            PatchAction::Conflict { .. } => Ok(()),
        }
    }

    pub(crate) fn apply_patch_to_map(
        &self,
        map: &Object,
        patch: &Patch,
        meta: &JsValue,
        cache: &ExportCache<'_>,
    ) -> Result<(), error::ApplyPatch> {
        match &patch.action {
            PatchAction::PutMap { key, value, .. } => {
                let sub_val = self.maybe_wrap_object(alloc(&value.0), &value.1, meta, cache)?;
                js_set(map, key, &sub_val)?;
                Ok(())
            }
            PatchAction::DeleteMap { key, .. } => {
                Reflect::delete_property(map, &key.into()).map_err(|e| error::Export::Delete {
                    prop: key.to_string(),
                    err: e,
                })?;
                Ok(())
            }
            PatchAction::Increment { prop, value, .. } => {
                if let Prop::Map(key) = prop {
                    let old_val = js_get(map, key)?.0;
                    let old_val = self.unwrap_scalar(old_val, cache)?;
                    if let Some(old) = old_val.as_f64() {
                        let new_value: Value<'_> =
                            am::ScalarValue::counter(old as i64 + *value).into();
                        js_set(map, key, &self.export_value(alloc(&new_value), cache)?)?;
                        Ok(())
                    } else {
                        Err(error::ApplyPatch::IncrementNonNumeric)
                    }
                } else {
                    Err(error::ApplyPatch::IncrementIndexInMap)
                }
            }
            PatchAction::Conflict { .. } => Ok(()),
            PatchAction::Insert { .. } => Err(error::ApplyPatch::InsertInMap),
            PatchAction::DeleteSeq { .. } => Err(error::ApplyPatch::SpliceInMap),
            PatchAction::SpliceText { .. } => Err(error::ApplyPatch::SpliceTextInMap),
            PatchAction::PutSeq { .. } => Err(error::ApplyPatch::PutIdxInMap),
            PatchAction::Mark { .. } => Err(error::ApplyPatch::MarkInMap),
        }
    }

    pub(crate) fn apply_patch(
        &self,
        root: Object,
        patch: &Patch,
        meta: &JsValue,
        cache: &mut ExportCache<'_>,
    ) -> Result<Object, error::ApplyPatch> {
        let (_, root_cache) = self.unwrap_object(&root, cache, meta)?;
        let mut current = root_cache.clone();
        for (i, p) in patch.path.iter().enumerate() {
            let prop = prop_to_js(&p.1);
            let subval = js_get(&current.inner, &prop)?.0;
            if subval.is_string() && patch.path.len() - 1 == i {
                let s = subval.dyn_into::<JsString>().unwrap();
                let new_text = self.apply_patch_to_text(&s, patch)?;
                js_set(&current.inner, &prop, &new_text)?;
                return Ok(root_cache.outer);
            }
            if subval.is_object() {
                let subval = subval.dyn_into::<Object>().unwrap();
                let (cache_hit, cached_obj) = self.unwrap_object(&subval, cache, meta)?;
                if !cache_hit {
                    js_set(&current.inner, &prop, &cached_obj.outer)?;
                }
                current = cached_obj;
            } else {
                return Ok(root); // invalid patch
            }
        }
        if current.id != patch.obj {
            return Ok(root);
        }
        if current.inner.is_array() {
            let inner_array = current
                .inner
                .dyn_into::<Array>()
                .map_err(|_| error::ApplyPatch::NotArray)?;
            self.apply_patch_to_array(&inner_array, patch, meta, cache)?;
        } else {
            self.apply_patch_to_map(&current.inner, patch, meta, cache)?;
        }
        Ok(root_cache.outer)
    }

    fn apply_patch_to_text(
        &self,
        string: &JsString,
        patch: &Patch,
    ) -> Result<JsValue, error::ApplyPatch> {
        match &patch.action {
            PatchAction::DeleteSeq { index, length, .. } => {
                let index = *index as u32;
                let before = string.slice(0, index);
                let after = string.slice(index + *length as u32, string.length());
                let result = before.concat(&after);
                Ok(result.into())
            }
            PatchAction::SpliceText { index, value, .. } => {
                let index = *index as u32;
                let length = string.length();
                let before = string.slice(0, index);
                let after = string.slice(index, length);
                let result = before.concat(&value.make_string().into()).concat(&after);
                Ok(result.into())
            }
            _ => Ok(string.into()),
        }
    }

    fn sub_splice<'a, I: IntoIterator<Item = &'a (Value<'a>, ObjId, bool)>>(
        &self,
        o: &Array,
        index: usize,
        num_del: usize,
        values: I,
        meta: &JsValue,
        cache: &ExportCache<'_>,
    ) -> Result<(), error::ApplyPatch> {
        let args: Array = values
            .into_iter()
            .map(|v| self.maybe_wrap_object(alloc(&v.0), &v.1, meta, cache))
            .collect::<Result<_, _>>()?;
        args.unshift(&(num_del as u32).into());
        args.unshift(&(index as u32).into());
        let method = js_get(o, "splice")?
            .0
            .dyn_into::<Function>()
            .map_err(error::Export::GetSplice)?;
        Reflect::apply(&method, o, &args).map_err(error::Export::CallSplice)?;
        Ok(())
    }

    pub(crate) fn import(&self, id: JsValue) -> Result<(ObjId, am::ObjType), error::ImportObj> {
        if let Some(s) = id.as_string() {
            // valid formats are
            // 123@aabbcc
            // 123@aabccc/prop1/prop2/prop3
            // /prop1/prop2/prop3
            let mut components = s.split('/');
            let obj = components.next();
            let (id, obj_type) = if obj == Some("") {
                (ROOT, am::ObjType::Map)
            } else {
                self.doc
                    .import(obj.unwrap_or_default())
                    .map_err(error::ImportObj::BadImport)?
            };
            self.import_path(id, obj_type, components)
                .map_err(|e| error::ImportObj::InvalidPath(s.to_string(), Box::new(e)))
        } else {
            Err(error::ImportObj::NotString)
        }
    }

    pub(crate) fn import_path<'a, I: Iterator<Item = &'a str>>(
        &self,
        mut obj: ObjId,
        mut obj_type: am::ObjType,
        components: I,
    ) -> Result<(ObjId, am::ObjType), error::ImportPath> {
        for (i, prop) in components.enumerate() {
            if prop.is_empty() {
                break;
            }
            let is_map = matches!(obj_type, am::ObjType::Map | am::ObjType::Table);
            let val = if is_map {
                self.doc.get(obj, prop)?
            } else {
                let idx = prop
                    .parse()
                    .map_err(|_| error::ImportPath::IndexNotInteger(i, prop.to_string()))?;
                self.doc.get(obj, am::Prop::Seq(idx))?
            };
            match val {
                Some((am::Value::Object(am::ObjType::Map), id)) => {
                    obj_type = am::ObjType::Map;
                    obj = id;
                }
                Some((am::Value::Object(am::ObjType::Table), id)) => {
                    obj_type = am::ObjType::Table;
                    obj = id;
                }
                Some((am::Value::Object(am::ObjType::List), id)) => {
                    obj_type = am::ObjType::List;
                    obj = id;
                }
                Some((am::Value::Object(am::ObjType::Text), id)) => {
                    obj_type = am::ObjType::Text;
                    obj = id;
                }
                None => return Err(error::ImportPath::NonExistentObject(i, prop.to_string())),
                _ => return Err(error::ImportPath::NotAnObject),
            };
        }
        Ok((obj, obj_type))
    }

    pub(crate) fn import_prop(&self, prop: JsValue) -> Result<Prop, error::InvalidProp> {
        if let Some(s) = prop.as_string() {
            Ok(s.into())
        } else if let Some(n) = prop.as_f64() {
            Ok((n as usize).into())
        } else {
            Err(error::InvalidProp)
        }
    }

    pub(crate) fn import_value(
        &self,
        value: &JsValue,
        datatype: Option<Datatype>,
    ) -> Result<(Value<'static>, Vec<(Prop, JsValue)>), error::InvalidValue> {
        match import_scalar(value, datatype).ok() {
            Some(val) => Ok((val.into(), vec![])),
            None => {
                if let Ok(js_obj) = import_obj(value, datatype) {
                    Ok((
                        js_obj.objtype().into(),
                        js_obj
                            .subvals()
                            .map(|(p, v)| (p.into_owned(), v))
                            .collect::<Vec<_>>(),
                    ))
                } else {
                    web_sys::console::log_2(&"Invalid value".into(), value);
                    Err(error::InvalidValue)
                }
            }
        }
    }
}

pub(crate) fn alloc(value: &Value<'_>) -> (Datatype, JsValue) {
    match value {
        am::Value::Object(o) => match o {
            ObjType::Map => (Datatype::Map, Object::new().into()),
            ObjType::Table => (Datatype::Table, Object::new().into()),
            ObjType::List => (Datatype::List, Array::new().into()),
            ObjType::Text => (Datatype::Text, "".into()),
        },
        am::Value::Scalar(s) => alloc_scalar(s.as_ref()),
    }
}

pub(crate) const SAFE_INT: Range<i64> =
    (Number::MIN_SAFE_INTEGER as i64)..(Number::MAX_SAFE_INTEGER as i64);
pub(crate) const SAFE_UINT: Range<u64> = 0..(Number::MIN_SAFE_INTEGER as u64);

pub(crate) fn alloc_scalar(value: &am::ScalarValue) -> (Datatype, JsValue) {
    match value {
        am::ScalarValue::Bytes(v) => (Datatype::Bytes, Uint8Array::from(v.as_slice()).into()),
        am::ScalarValue::Str(v) => (Datatype::Str, v.to_string().into()),
        am::ScalarValue::Int(v) if SAFE_INT.contains(v) => (Datatype::Int, (*v as f64).into()),
        am::ScalarValue::Int(v) => (Datatype::Int, BigInt::from(*v).into()),
        am::ScalarValue::Uint(v) if SAFE_UINT.contains(v) => (Datatype::Uint, (*v as f64).into()),
        am::ScalarValue::Uint(v) => (Datatype::Uint, BigInt::from(*v).into()),
        am::ScalarValue::F64(v) => (Datatype::F64, (*v).into()),
        am::ScalarValue::Counter(v) => {
            let v = i64::from(v);
            if SAFE_INT.contains(&v) {
                (Datatype::Counter, (v as f64).into())
            } else {
                (Datatype::Counter, BigInt::from(v).into())
            }
        }
        am::ScalarValue::Timestamp(v) => (
            Datatype::Timestamp,
            js_sys::Date::new(&(*v as f64).into()).into(),
        ),
        am::ScalarValue::Boolean(v) => (Datatype::Boolean, (*v).into()),
        am::ScalarValue::Null => (Datatype::Null, JsValue::null()),
        am::ScalarValue::Unknown { bytes, type_code } => (
            Datatype::Unknown(*type_code),
            Uint8Array::from(bytes.as_slice()).into(),
        ),
    }
}

fn export_path(path: &[(ObjId, Prop)], end: &Prop) -> Array {
    let result = Array::new();
    for p in path {
        result.push(&prop_to_js(&p.1));
    }
    result.push(&prop_to_js(end));
    result
}

pub(crate) fn export_just_path(path: &[(ObjId, Prop)]) -> Array {
    let result = Array::new();
    for p in path {
        result.push(&prop_to_js(&p.1));
    }
    result
}

pub(crate) fn export_spans(
    doc: &Automerge,
    cache: ExportCache<'_>,
    spans: Spans<'_>,
) -> Result<Array, error::SetProp> {
    spans.map(|span| export_span(doc, &cache, span)).collect()
}

pub(crate) fn export_span(
    doc: &Automerge,
    cache: &ExportCache<'_>,
    span: Span,
) -> Result<Object, error::SetProp> {
    match span {
        Span::Text { text: t, marks: m } => {
            let result = Object::new();
            js_set(&result, "type", "text")?;
            js_set(&result, "value", t)?;
            if let Some(m) = &m {
                // copy paste - export marks
                if m.num_marks() > 0 {
                    let marks = Object::new();
                    for (name, value) in m.iter() {
                        js_set(&marks, name, alloc(&value.into()).1)?;
                    }
                    js_set(&result, "marks", marks)?;
                }
            }
            Ok(result)
        }
        Span::Block(b) => {
            let result = Object::new();
            js_set(&result, "type", "block")?;
            js_set(&result, "value", export_hydrate(doc, cache, b.into()))?;
            Ok(result)
        }
    }
}

pub(super) fn export_hydrate(
    doc: &Automerge,
    cache: &ExportCache<'_>,
    value: am::hydrate::Value,
) -> JsValue {
    match value {
        am::hydrate::Value::Scalar(s) => {
            let (datatype, val) = alloc_scalar(&s);
            doc.export_value((datatype, val), cache).unwrap()
        }
        am::hydrate::Value::Map(h_map) => {
            let map = Object::new();
            for (k, v) in h_map.iter() {
                let val = export_hydrate(doc, cache, v.value.clone());
                Reflect::set(&map, &k.into(), &val).unwrap();
            }
            map.into()
        }
        am::hydrate::Value::List(h_list) => {
            let list = Array::new();
            for v in h_list.iter() {
                let val = export_hydrate(doc, cache, v.value.clone());
                list.push(&val);
            }
            list.into()
        }
        am::hydrate::Value::Text(text) => text.to_string().into(),
    }
}

pub(crate) fn export_patches<I: IntoIterator<Item = Patch>>(
    externals: &HashMap<Datatype, ExternalTypeConstructor>,
    patches: I,
) -> Result<Array, error::Export> {
    // this is for performance - each block is the same
    // so i only want to materialize each block once per
    // apply patches
    patches
        .into_iter()
        // removing update block for now
        .map(|p| export_patch(externals, p))
        .collect()
}

fn export_patch(
    externals: &HashMap<Datatype, ExternalTypeConstructor>,
    p: Patch,
) -> Result<JsValue, error::Export> {
    let result = Object::new();
    let path = &p.path.as_slice();
    match p.action {
        PatchAction::PutMap {
            key,
            value,
            conflict,
            ..
        } => {
            js_set(&result, "action", "put")?;
            js_set(&result, "path", export_path(path, &Prop::Map(key)))?;

            let (datatype, value) = alloc(&value.0);
            let exported_val = if let Some(external_type) = externals.get(&datatype) {
                external_type.construct(&value, datatype)?
            } else {
                value
            };
            js_set(&result, "value", exported_val)?;
            if conflict {
                js_set(&result, "conflict", true)?;
            }
            Ok(result.into())
        }
        PatchAction::PutSeq {
            index,
            value,
            conflict,
            ..
        } => {
            js_set(&result, "action", "put")?;
            js_set(&result, "path", export_path(path, &Prop::Seq(index)))?;

            let (datatype, value) = alloc(&value.0);
            let exported_val = if let Some(external_type) = externals.get(&datatype) {
                external_type.construct(&value, datatype)?
            } else {
                value
            };
            js_set(&result, "value", exported_val)?;
            if conflict {
                js_set(&result, "conflict", true)?;
            }
            Ok(result.into())
        }
        PatchAction::Insert { index, values, .. } => {
            let conflicts = values.iter().map(|v| v.2).collect::<Vec<_>>();
            let values = values
                .iter()
                .map(|v| {
                    let (datatype, js_val) = alloc(&v.0);
                    let exported_val = if let Some(external_type) = externals.get(&datatype) {
                        external_type.construct(&js_val, datatype)
                    } else {
                        Ok(js_val)
                    };
                    exported_val
                })
                .collect::<Result<Array, _>>()?;
            js_set(&result, "action", "insert")?;
            js_set(&result, "path", export_path(path, &Prop::Seq(index)))?;
            js_set(&result, "values", values)?;
            if conflicts.iter().any(|c| *c) {
                js_set(
                    &result,
                    "conflicts",
                    conflicts
                        .iter()
                        .map(|c| JsValue::from(*c))
                        .collect::<Array>(),
                )?;
            }
            Ok(result.into())
        }
        PatchAction::SpliceText {
            index,
            value,
            marks,
            ..
        } => {
            js_set(&result, "action", "splice")?;
            js_set(&result, "path", export_path(path, &Prop::Seq(index)))?;
            js_set(&result, "value", value.make_string())?;
            if let Some(m) = marks {
                if m.num_marks() > 0 {
                    let marks = Object::new();
                    for (name, value) in m.iter() {
                        js_set(&marks, name, alloc(&value.into()).1)?;
                    }
                    js_set(&result, "marks", marks)?;
                }
            }
            Ok(result.into())
        }
        PatchAction::Increment { prop, value, .. } => {
            js_set(&result, "action", "inc")?;
            js_set(&result, "path", export_path(path, &prop))?;
            js_set(&result, "value", JsValue::from_f64(value as f64))?;
            Ok(result.into())
        }
        PatchAction::DeleteMap { key, .. } => {
            js_set(&result, "action", "del")?;
            js_set(&result, "path", export_path(path, &Prop::Map(key)))?;
            Ok(result.into())
        }
        PatchAction::DeleteSeq { index, length, .. } => {
            js_set(&result, "action", "del")?;
            js_set(&result, "path", export_path(path, &Prop::Seq(index)))?;
            if length > 1 {
                js_set(&result, "length", length)?;
            }
            Ok(result.into())
        }
        PatchAction::Mark { marks, .. } => {
            js_set(&result, "action", "mark")?;
            js_set(&result, "path", export_just_path(path))?;
            let marks_array = Array::new();
            for m in marks.iter() {
                let mark = Object::new();
                js_set(&mark, "name", m.name())?;
                js_set(&mark, "value", &alloc(&m.value().into()).1)?;
                js_set(&mark, "start", m.start as i32)?;
                js_set(&mark, "end", m.end as i32)?;
                marks_array.push(&mark);
            }
            js_set(&result, "marks", marks_array)?;
            Ok(result.into())
        }
        PatchAction::Conflict { prop } => {
            js_set(&result, "action", "conflict")?;
            js_set(&result, "path", export_path(path, &prop))?;
            Ok(result.into())
        }
    }
}

fn shallow_copy(obj: &Object) -> Object {
    if Array::is_array(obj) {
        Array::from(obj).into()
    } else {
        Object::assign(&Object::new(), obj)
    }
}

fn prop_to_js(prop: &Prop) -> JsValue {
    match prop {
        Prop::Map(key) => key.into(),
        Prop::Seq(index) => (*index as f64).into(),
    }
}

pub(super) fn js_val_to_hydrate(
    doc: &Automerge,
    js_val: JsValue,
) -> Result<am::hydrate::Value, error::JsValToHydrate> {
    let (datatype, value) = match doc.external_types.iter().find_map(|(dt, et)| {
        et.deconstruct(&js_val)
            .map(|r| r.map(|v| (*dt, v)))
            .transpose()
    }) {
        Some(Ok((dt, v))) => (Some(dt), v),
        Some(Err(e)) => return Err(e.into()),
        None => (None, js_val.clone()),
    };
    if let Ok(js_obj) = import_obj(&value, datatype) {
        match js_obj.objtype() {
            am::ObjType::Map | am::ObjType::Table => {
                let obj: HashMap<String, am::hydrate::Value> = js_obj
                    .subvals()
                    .filter_map(|(p, v)| match p.as_ref() {
                        Prop::Map(key) => Some((key.to_string(), v)),
                        _ => None,
                    })
                    .map(|(k, v)| js_val_to_hydrate(doc, v).map(|v| (k, v)))
                    .collect::<Result<_, _>>()?;
                Ok(am::hydrate::Value::Map(obj.into()))
            }
            am::ObjType::List => {
                let obj: Vec<am::hydrate::Value> = js_obj
                    .subvals()
                    .map(|(_, v)| js_val_to_hydrate(doc, v))
                    .collect::<Result<_, _>>()?;
                Ok(am::hydrate::Value::List(obj.into()))
            }
            am::ObjType::Text => {
                let Some(obj) = js_obj.text() else {
                    return Err(error::JsValToHydrate::InvalidText);
                };
                // This code path is only used in `next`, which uses a string representation
                // and we're targeting JS, which uses utf16 strings
                Ok(am::hydrate::Value::Text(am::hydrate::Text::new(
                    am::TextEncoding::Utf16CodeUnit,
                    obj,
                )))
            }
        }
    } else if let Ok(val) = import_scalar(&value, datatype) {
        Ok(am::hydrate::Value::Scalar(val))
    } else {
        Err(error::JsValToHydrate::UnknownType)
    }
}

pub(crate) mod error {
    use automerge::{AutomergeError, LoadChangeError};
    use js_sys::BigInt;
    use wasm_bindgen::JsValue;

    #[derive(Debug, thiserror::Error)]
    pub enum BadJSChanges {
        #[error("the changes were not an array of Uint8Array")]
        ChangesNotArray,
        #[error("change {0} was not a Uint8Array")]
        ElemNotUint8Array(usize),
        #[error("error loading change {0}: {1}")]
        BadChange(usize, LoadChangeError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadChangeHashes {
        #[error("the change hashes were not an array of strings")]
        NotArray,
        #[error("could not decode hash {0}: {1}")]
        BadElem(usize, BadChangeHash),
    }

    impl From<BadChangeHashes> for JsValue {
        fn from(e: BadChangeHashes) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadChangeHashSet {
        #[error("not an object")]
        NotObject,
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error("unable to getOwnProperties")]
        ListProp,
        #[error("unable to parse hash from {0:?}: {1}")]
        BadHash(wasm_bindgen::JsValue, BadChangeHash),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadChangeHash {
        #[error("change hash was not a string")]
        NotString,
        #[error(transparent)]
        Parse(#[from] automerge::ParseChangeHashError),
    }

    impl From<BadChangeHash> for JsValue {
        fn from(e: BadChangeHash) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadSyncState {
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error("bad sharedHeads: {0}")]
        BadSharedHeads(BadChangeHashes),
        #[error("bad lastSentHeads: {0}")]
        BadLastSentHeads(BadChangeHashes),
        #[error("bad theirHeads: {0}")]
        BadTheirHeads(BadChangeHashes),
        #[error("bad theirNeed: {0}")]
        BadTheirNeed(BadChangeHashes),
        #[error("bad theirHave: {0}")]
        BadTheirHave(BadHaves),
        #[error("bad sentHashes: {0}")]
        BadSentHashes(BadChangeHashSet),
        #[error("inFlight not a boolean")]
        InFlightNotBoolean,
        #[error("bad theirCapabilities: {0}")]
        BadTheirCapabilities(BadCapabilities),
    }

    impl From<BadSyncState> for JsValue {
        fn from(e: BadSyncState) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("unable to get property {property}: {error:?}")]
    pub struct GetProp {
        pub(crate) property: String,
        pub(crate) error: wasm_bindgen::JsValue,
    }

    impl From<GetProp> for JsValue {
        fn from(e: GetProp) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("error setting property {property:?} on JS value: {error:?}")]
    pub struct SetProp {
        pub(crate) property: JsValue,
        pub(crate) error: JsValue,
    }

    impl From<SetProp> for JsValue {
        fn from(e: SetProp) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadHave {
        #[error("bad lastSync: {0}")]
        BadLastSync(BadChangeHashes),
        #[error("bad bloom: {0}")]
        BadBloom(BadBloom),
        #[error(transparent)]
        GetHaveProp(#[from] GetProp),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadHaves {
        #[error("value was not an array")]
        NotArray,
        #[error("error loading have at index {0}: {1}")]
        BadElem(usize, BadHave),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadBloom {
        #[error("the value was not a Uint8Array")]
        NotU8Array,
        #[error("unable to decode: {0}")]
        Decode(#[from] automerge::sync::DecodeBloomError),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Export {
        #[error(transparent)]
        Set(#[from] SetProp),
        #[error("unable to delete prop {prop}: {err:?}")]
        Delete { prop: String, err: JsValue },
        #[error("unable to set hidden property {0}")]
        SetHidden(&'static str),
        #[error("data handler for type {0} did not return a valid object")]
        InvalidDataHandler(String),
        #[error("error calling data handler for type {0}: {1:?}")]
        CallDataHandler(String, JsValue),
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error(transparent)]
        InvalidDatatype(#[from] crate::value::InvalidDatatype),
        #[error("unable to get the splice function: {0:?}")]
        GetSplice(JsValue),
        #[error("error calling splice: {0:?}")]
        CallSplice(JsValue),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error("reflect set failed")]
        ReflectSet(JsValue),
        #[error("invalid root processed")]
        InvalidRoot,
        #[error("missing child in export")]
        MissingChild,
    }

    impl From<Export> for JsValue {
        fn from(e: Export) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ApplyPatch {
        #[error(transparent)]
        Export(#[from] Export),
        #[error("cannot delete from a seq")]
        DeleteKeyFromSeq,
        #[error("cannot put key in seq")]
        PutKeyInSeq,
        #[error("cannot increment a non-numeric value")]
        IncrementNonNumeric,
        #[error("cannot increment a key in a seq")]
        IncrementKeyInSeq,
        #[error("cannot increment index in a map")]
        IncrementIndexInMap,
        #[error("cannot insert into a map")]
        InsertInMap,
        #[error("cannot splice into a map")]
        SpliceInMap,
        #[error("cannot splice text into a seq")]
        SpliceTextInSeq,
        #[error("cannot splice text into a map")]
        SpliceTextInMap,
        #[error("cannot put a seq index in a map")]
        PutIdxInMap,
        #[error("cannot mark a span in a map")]
        MarkInMap,
        #[error("cannot have blocks in a map")]
        BlockInMap,
        #[error("array patch applied to non array")]
        NotArray,
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error(transparent)]
        SetProp(#[from] SetProp),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
    }

    impl From<ApplyPatch> for JsValue {
        fn from(e: ApplyPatch) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadSyncMessage {
        #[error(transparent)]
        GetProp(#[from] GetProp),
        #[error("unable to read haves: {0}")]
        BadHaves(#[from] BadHaves),
        #[error("could not read changes: {0}")]
        BadJSChanges(#[from] BadChunkList),
        #[error("could not read 'changes' as Uint8Array: {0}")]
        BadRawChanges(BadUint8Array),
        #[error("could not read heads: {0}")]
        BadHeads(BadChangeHashes),
        #[error("could not read need: {0}")]
        BadNeed(BadChangeHashes),
        #[error("no 'changes' property")]
        MissingChanges,
        #[error("bad supported_capabilities: {0}")]
        BadSupportedCapabilities(BadCapabilities),
        #[error("wholeDoc cannot be used in a type: v1 message")]
        WholeDocInV1,
    }

    impl From<BadSyncMessage> for JsValue {
        fn from(e: BadSyncMessage) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ImportObj {
        #[error("obj id was not a string")]
        NotString,
        #[error("invalid path {0}: {1}")]
        InvalidPath(String, Box<ImportPath>),
        #[error("unable to import object id: {0}")]
        BadImport(AutomergeError),
        #[error("error calling data handler for type {0}: {1:?}")]
        CallDataHandler(String, JsValue),
    }

    impl From<ImportObj> for JsValue {
        fn from(e: ImportObj) -> Self {
            JsValue::from(format!("invalid object ID: {}", e))
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ImportPath {
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error("path component {0} ({1}) should be an integer to index a sequence")]
        IndexNotInteger(usize, String),
        #[error("path component {0} ({1}) referenced a nonexistent object")]
        NonExistentObject(usize, String),
        #[error("path did not refer to an object")]
        NotAnObject,
    }
    #[derive(Debug, thiserror::Error)]
    #[error("cursor position must be an index (number), 'start' or 'end'")]
    pub struct BadCursorPosition;

    #[derive(Debug, thiserror::Error)]
    #[error("move must be 'before' or 'after' - is 'after' by default")]
    pub struct BadMoveCursor;

    #[derive(Debug, thiserror::Error)]
    #[error("expand must be 'left', 'right', 'both', or 'none' - is 'right' by default")]
    pub struct BadExpand;

    #[derive(Debug, thiserror::Error)]
    #[error("argument must be a number")]
    pub struct BadNumber;

    #[derive(Debug, thiserror::Error)]
    #[error("given property was not a string or integer")]
    pub struct InvalidProp;

    #[derive(Debug, thiserror::Error)]
    #[error("given property was not a string or integer")]
    pub struct InvalidValue;

    #[derive(thiserror::Error, Debug)]
    #[error("not a Uint8Array")]
    pub struct BadUint8Array;

    #[derive(thiserror::Error, Debug)]
    pub enum BadCapabilities {
        #[error("capabilities was not an array")]
        NotArray,
        #[error("element {0} was not a string")]
        ElemNotString(usize),
        #[error("element {0} was not a valid capability: {1}")]
        ElemNotValid(usize, String),
    }

    #[derive(thiserror::Error, Debug)]
    pub enum BadChunkList {
        #[error("chunk list was not an array")]
        NotArray,
        #[error("element {0} was not a Uint8Array")]
        ElemNotUint8Array(usize),
    }

    #[derive(thiserror::Error, Debug)]
    pub enum InvalidSpan {
        #[error("must be a block object or a text span")]
        NotObjectOrString,
        #[error("block must be an object")]
        BlockNotObject,
        #[error(transparent)]
        ReflectGet(#[from] GetProp),
        #[error("'type' property must be a string")]
        TypeNotString,
        #[error("invalid 'type' property: {0}")]
        InvalidType(String),
        #[error("'text' property must be a string")]
        TextNotString,
        #[error("invalid marks: {0}")]
        InvalidMarks(ImportMark),
        #[error("marks were not an object")]
        MarksNotObject,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum InvalidUpdateSpansArgs {
        #[error("updateSpans args must be an array")]
        NotArray,
        #[error("block {0} not a valid block: {1}")]
        InvalidElement(usize, InvalidSpan),
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ImportValue {
        #[error("value not primitive")]
        ValueNotPrimitive,
        #[error("invalid import: {0:?}")]
        Invalid(JsValue),
        #[error("bignum {0:?} larger than max {1:?}")]
        BigIntTooLarge(BigInt, BigInt),
        #[error("bignum {0:?} smaller than min {1:?}")]
        BigIntTooSmall(BigInt, BigInt),
        #[error("bignum invalid")]
        BigIntInvalid,
        #[error("error calling deconstructor: {0:?}")]
        CallDataHandler(JsValue),
        #[error("deconstructor did not return an array of [datatype, value]")]
        BadDeconstructor,
        #[error("deconstructor returned a bad datatype: {0}")]
        BadDataType(#[from] crate::value::InvalidDatatype),
    }

    impl From<js_sys::RangeError> for ImportValue {
        fn from(_: js_sys::RangeError) -> Self {
            ImportValue::BigIntInvalid
        }
    }
    impl From<std::num::ParseIntError> for ImportValue {
        fn from(_: std::num::ParseIntError) -> Self {
            ImportValue::BigIntInvalid
        }
    }

    #[derive(thiserror::Error, Debug)]
    pub enum JsValToHydrate {
        #[error(transparent)]
        ImportValue(#[from] ImportValue),
        #[error(transparent)]
        InvalidValue(#[from] InvalidValue),
        #[error("text object had no text")]
        InvalidText,
        #[error("bigint too large: {0}")]
        BigIntTooLarge(js_sys::BigInt),
        #[error("bigint too small: {0}")]
        BigIntTooSmall(js_sys::BigInt),
        #[error("unable to determine type of value")]
        UnknownType,
    }

    #[derive(thiserror::Error, Debug)]
    pub enum ImportMark {
        #[error("key at index {0} was not a string")]
        KeyNotString(usize),
        #[error("value for key {0} could not be converted to a scalar value")]
        InvalidValue(String),
        #[error("marks was not an object")]
        NotObject,
    }

    #[derive(thiserror::Error, Debug)]
    pub enum ImportUpdateSpansConfig {
        #[error("config was not an object")]
        NotObject,
        #[error("failed to get property {0}")]
        GetProp(#[from] GetProp),
        #[error("invalid defaultExpand: {0}")]
        BadDefaultExpand(BadExpand),
        #[error("invalid perMarkExpand{key}: {error}")]
        BadPerMarkExpand { key: String, error: BadExpand },
        #[error("perMarkExpands was not null but also not an object")]
        PerMarkNotObject,
    }
}

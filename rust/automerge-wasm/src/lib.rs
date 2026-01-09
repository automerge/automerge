#![doc(
    html_logo_url = "https://raw.githubusercontent.com/automerge/automerge/main/img/brandmark.svg",
    html_favicon_url = "https:///raw.githubusercontent.com/automerge/automerge/main/img/favicon.ico"
)]
#![warn(
    missing_debug_implementations,
    // missing_docs, // TODO: add documentation!
    rust_2021_compatibility,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]
use am::marks::Mark;
use am::transaction::CommitOptions;
use am::transaction::Transactable;
use am::CursorPosition;
use am::OnPartialLoad;
use am::ScalarValue;
use am::StringMigration;
use am::VerificationMode;
use automerge as am;
use automerge::TextEncoding;
use automerge::{sync::SyncDoc, AutoCommit, Change, Prop, ReadDoc, Value, ROOT};
use interop::import_scalar;
use js_sys::Reflect;
use js_sys::{Array, Function, Object, Uint8Array};
use serde::ser::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

mod export_cache;
mod interop;
mod sync;
mod value;

use interop::{alloc, get_heads, import_obj, js_get, js_set, to_js_err, to_prop, AR, JS};
use sync::SyncState;
use value::Datatype;

use crate::interop::SubValIter;

#[wasm_bindgen(typescript_custom_section)]
const TS: &'static str = r#"
export type Actor = string;
export type ObjID = string;
export type Change = Uint8Array;
export type SyncMessage = Uint8Array;
export type Prop = string | number;
export type Hash = string;
export type Heads = Hash[];
export type ScalarValue = string | number | boolean | null | Date | Uint8Array;
export type Value = ScalarValue | object;
export type MaterializeValue =
  | { [key: string]: MaterializeValue }
  | Array<MaterializeValue>
  | Value;
export type MapObjType = { [key: string]: ObjType | Value };
export type ObjInfo = { id: ObjID; type: ObjTypeName; path?: Prop[] };
export type Span =
  | { type: "text"; value: string; marks?: MarkSet }
  | { type: "block"; value: { [key: string]: MaterializeValue } };
export type ListObjType = Array<ObjType | Value>;
export type ObjType = string | ListObjType | MapObjType;
export type FullValue =
  | ["str", string]
  | ["int", number]
  | ["uint", number]
  | ["f64", number]
  | ["boolean", boolean]
  | ["timestamp", Date]
  | ["counter", number]
  | ["bytes", Uint8Array]
  | ["null", null]
  | ["map", ObjID]
  | ["list", ObjID]
  | ["text", ObjID]
  | ["table", ObjID];

export type Cursor = string;
export type CursorPosition = number | "start" | "end";
export type MoveCursor = "before" | "after";

export type FullValueWithId =
  | ["str", string, ObjID]
  | ["int", number, ObjID]
  | ["uint", number, ObjID]
  | ["f64", number, ObjID]
  | ["boolean", boolean, ObjID]
  | ["timestamp", Date, ObjID]
  | ["counter", number, ObjID]
  | ["bytes", Uint8Array, ObjID]
  | ["null", null, ObjID]
  | ["map", ObjID]
  | ["list", ObjID]
  | ["text", ObjID]
  | ["table", ObjID];

export enum ObjTypeName {
  list = "list",
  map = "map",
  table = "table",
  text = "text",
}

export type Datatype =
  | "boolean"
  | "str"
  | "int"
  | "uint"
  | "f64"
  | "null"
  | "timestamp"
  | "counter"
  | "bytes"
  | "map"
  | "text"
  | "list";

export type SyncHave = {
  lastSync: Heads;
  bloom: Uint8Array;
};

export type DecodedSyncMessage = {
  heads: Heads;
  need: Heads;
  have: SyncHave[];
  changes: Change[];
};

export type DecodedChange = {
  actor: Actor;
  seq: number;
  startOp: number;
  time: number;
  message: string | null;
  deps: Heads;
  hash: Hash;
  ops: Op[];
};

export type ChangeMetadata = {
  actor: Actor;
  seq: number;
  startOp: number;
  maxOp: number;
  time: number;
  message: string | null;
  deps: Heads;
  hash: Hash;
};

type PartialBy<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>;
export type ChangeToEncode = PartialBy<DecodedChange, "hash">;

export type Op = {
  action: string;
  obj: ObjID;
  key: string;
  value?: string | number | boolean;
  datatype?: string;
  pred: string[];
};

export type PatchValue =
  | string
  | number
  | boolean
  | null
  | Date
  | Uint8Array
  | {}
  | [];
export type Patch =
  | PutPatch
  | DelPatch
  | SpliceTextPatch
  | IncPatch
  | InsertPatch
  | MarkPatch
  | UnmarkPatch
  | ConflictPatch;

export type PutPatch = {
  action: "put";
  path: Prop[];
  value: PatchValue;
  conflict?: boolean;
};

export interface MarkSet {
  [name: string]: ScalarValue;
}

export type MarkPatch = {
  action: "mark";
  path: Prop[];
  marks: Mark[];
};

export type MarkRange = {
  expand?: "before" | "after" | "both" | "none";
  start: number;
  end: number;
};

export type UnmarkPatch = {
  action: "unmark";
  path: Prop[];
  name: string;
  start: number;
  end: number;
};

export type IncPatch = {
  action: "inc";
  path: Prop[];
  value: number;
};

export type DelPatch = {
  action: "del";
  path: Prop[];
  length?: number;
};

export type SpliceTextPatch = {
  action: "splice";
  path: Prop[];
  value: string;
  marks?: MarkSet;
};

export type InsertPatch = {
  action: "insert";
  path: Prop[];
  values: PatchValue[];
  marks?: MarkSet;
  conflicts?: boolean[];
};

export type ConflictPatch = {
  action: "conflict";
  path: Prop[];
};

export type Mark = {
  name: string;
  value: ScalarValue;
  start: number;
  end: number;
};

// Some definitions can't be typed using the wasm_bindgen annotations
// (specifically optional function parameters) so we do that work here
// and merge this definition with the `class Automerge` definition
// which follows
interface Automerge {

    fork(actor?: string, heads?: Heads): Automerge;

    put(obj: ObjID, prop: Prop, value: Value, datatype?: Datatype): void;
    get(obj: ObjID, prop: Prop, heads?: Heads): Value | undefined;
    getWithType(obj: ObjID, prop: Prop, heads?: Heads): FullValue | null;
    getAll(obj: ObjID, arg: Prop, heads?: Heads): FullValueWithId[];

    keys(obj: ObjID, heads?: Heads): string[];
    text(obj: ObjID, heads?: Heads): string;
    spans(obj: ObjID, heads?: Heads): Span[];
    marks(obj: ObjID, heads?: Heads): Mark[];
    marksAt(obj: ObjID, index: number, heads?: Heads): MarkSet;
    length(obj: ObjID, heads?: Heads): number;

    objInfo(obj: ObjID, heads?: Heads): ObjInfo;

    materialize(obj?: ObjID, heads?: Heads, metadata?: unknown): MaterializeValue;

    push(obj: ObjID, value: Value, datatype?: Datatype): void;

    insert(obj: ObjID, index: number, value: Value, datatype?: Datatype): void;

    splice(
      obj: ObjID,
      start: number,
      delete_count: number,
      text?: string | Array<Value>,
    ): void;

    mark(
      obj: ObjID,
      range: MarkRange,
      name: string,
      value: Value,
      datatype?: Datatype,
    ): void;

    getCursor(
      obj: ObjID,
      position: CursorPosition,
      heads?: Heads,
      move?: MoveCursor,
    ): Cursor;

    applyPatches<Doc>(obj: Doc, meta?: unknown): Doc;

    applyAndReturnPatches<Doc>(
      obj: Doc,
      meta?: unknown,
    ): { value: Doc; patches: Patch[] };

    getBlock(obj: ObjID, index: number, heads?: Heads): { [key: string]: MaterializeValue } | null;

    getMissingDeps(heads?: Heads): Heads;

    getCursorPosition(obj: ObjID, cursor: Cursor, heads?: Heads): number;

    diffPath(path: Prop[] | string, before: Heads, after: Heads, options?: DiffOptions): Patch[];
}


export type LoadOptions = {
  actor?: Actor;
  unchecked?: boolean;
  allowMissingDeps?: boolean;
  convertImmutableStringsToText?: boolean;
};

// if recursive is false do not diff child objects
export type DiffOptions = {
  recursive?: boolean;
};

export type InitOptions = {
  actor?: Actor;
};

export function create(options?: InitOptions): Automerge;
export function load(data: Uint8Array, options?: LoadOptions): Automerge;

export interface JsSyncState {
  sharedHeads: Heads;
  lastSentHeads: Heads;
  theirHeads: Heads | undefined;
  theirHeed: Heads | undefined;
  theirHave: SyncHave[] | undefined;
  sentHashes: Heads;
}

export interface DecodedBundle {
  changes: DecodedChange[];
  deps: Heads;
}

export interface API {
  create(options?: InitOptions): Automerge;
  load(data: Uint8Array, options?: LoadOptions): Automerge;
  encodeChange(change: ChangeToEncode): Change;
  decodeChange(change: Change): DecodedChange;
  initSyncState(): SyncState;
  encodeSyncMessage(message: DecodedSyncMessage): SyncMessage;
  decodeSyncMessage(msg: SyncMessage): DecodedSyncMessage;
  encodeSyncState(state: SyncState): Uint8Array;
  decodeSyncState(data: Uint8Array): SyncState;
  exportSyncState(state: SyncState): JsSyncState;
  importSyncState(state: JsSyncState): SyncState;
  readBundle(data: Uint8Array): DecodedBundle;
  wasmReleaseInfo(): WasmReleaseInfo;
}

export interface Stats {
  numChanges: number;
  numOps: number;
  numActors: number;
}

export interface WasmReleaseInfo {
  gitHead: string;
  cargoPackageName: string;
  cargoPackageVersion: string;
  rustcVersion: string;
}

export type UpdateSpansConfig = {
    defaultExpand?: "before" | "after" | "both" | "none";
    perMarkExpand?: {[key: string]: "before" | "after" | "both" | "none" }
}
"#;

#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
          web_sys::console::log_1(&format!( $( $t )* ).into());
    };
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct Automerge {
    doc: AutoCommit,
    freeze: bool,
    external_types: HashMap<Datatype, interop::ExternalTypeConstructor>,
}

#[wasm_bindgen]
impl Automerge {
    pub fn new(actor: Option<String>) -> Result<Automerge, error::BadActorId> {
        let mut doc = AutoCommit::new_with_encoding(TextEncoding::Utf16CodeUnit);
        if let Some(a) = actor {
            let a = automerge::ActorId::from(hex::decode(a)?.to_vec());
            doc.set_actor(a);
        }
        Ok(Automerge {
            doc,
            freeze: false,
            external_types: HashMap::default(),
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&mut self, actor: Option<String>) -> Result<Automerge, error::BadActorId> {
        let mut automerge = Automerge {
            doc: self.doc.clone(),
            freeze: self.freeze,
            external_types: self.external_types.clone(),
        };
        if let Some(s) = actor {
            let actor = automerge::ActorId::from(hex::decode(s)?.to_vec());
            automerge.doc.set_actor(actor);
        }
        Ok(automerge)
    }

    // We skip typescript here because the function is defined in the `interface Automerge`
    // definition at the top of this file
    #[wasm_bindgen(skip_typescript)]
    pub fn fork(
        &mut self,
        actor: Option<String>,
        heads: JsValue,
    ) -> Result<Automerge, error::Fork> {
        let heads: Result<Vec<am::ChangeHash>, _> = JS(heads).try_into();
        let doc = if let Ok(heads) = heads {
            self.doc.fork_at(&heads)?
        } else {
            self.doc.fork()
        };
        let mut automerge = Automerge {
            doc,
            freeze: self.freeze,
            external_types: self.external_types.clone(),
        };
        if let Some(s) = actor {
            let actor =
                automerge::ActorId::from(hex::decode(s).map_err(error::BadActorId::from)?.to_vec());
            automerge.doc.set_actor(actor);
        }
        Ok(automerge)
    }

    #[wasm_bindgen(js_name = pendingOps, unchecked_return_type="number")]
    pub fn pending_ops(&self) -> JsValue {
        (self.doc.pending_ops() as u32).into()
    }

    #[wasm_bindgen(unchecked_return_type = "Hash | null")]
    pub fn commit(&mut self, message: Option<String>, time: Option<f64>) -> JsValue {
        let mut commit_opts = CommitOptions::default();
        if let Some(message) = message {
            commit_opts.set_message(message);
        }
        if let Some(time) = time {
            commit_opts.set_time(time as i64);
        }
        let hash = self.doc.commit_with(commit_opts);
        match hash {
            Some(h) => JsValue::from_str(&hex::encode(h.0)),
            None => JsValue::NULL,
        }
    }

    #[wasm_bindgen(unchecked_return_type = "Heads")]
    pub fn merge(&mut self, other: &mut Automerge) -> Result<Array, error::Merge> {
        let heads = self.doc.merge(&mut other.doc)?;
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(h.0)))
            .collect();
        Ok(heads)
    }

    pub fn rollback(&mut self) -> f64 {
        self.doc.rollback() as f64
    }

    // skip_typescript as the optional heads parameter can't be typed
    #[wasm_bindgen(skip_typescript)]
    pub fn keys(&self, obj: JsValue, heads: JsValue) -> Result<Array, error::Get> {
        let (obj, _) = self.import(obj)?;
        let result = if let Some(heads) = get_heads(heads)? {
            self.doc
                .keys_at(&obj, &heads)
                .map(|s| JsValue::from_str(&s))
                .collect()
        } else {
            self.doc.keys(&obj).map(|s| JsValue::from_str(&s)).collect()
        };
        Ok(result)
    }

    // skip_typescript as the optional heads parameter can't be typed
    #[wasm_bindgen(skip_typescript)]
    pub fn text(&self, obj: JsValue, heads: JsValue) -> Result<String, error::Get> {
        let (obj, _) = self.import(obj)?;
        if let Some(heads) = get_heads(heads)? {
            Ok(self.doc.text_at(&obj, &heads)?)
        } else {
            Ok(self.doc.text(&obj)?)
        }
    }

    // skip_typescript as the optional heads parameter can't be typed
    #[wasm_bindgen(skip_typescript)]
    pub fn spans(&self, obj: JsValue, heads: JsValue) -> Result<Array, error::GetSpans> {
        let (obj, _) = self.import(obj)?;
        let spans = if let Some(heads) = get_heads(heads)? {
            self.doc.spans_at(&obj, &heads)?
        } else {
            self.doc.spans(&obj)?
        };
        let cache = interop::ExportCache::new(self)?;
        Ok(interop::export_spans(self, cache, spans)?)
    }

    // skip_typescript as the text argument is optional which can only be typed
    // in the typescript custom section
    #[wasm_bindgen(skip_typescript)]
    pub fn splice(
        &mut self,
        obj: JsValue,
        start: f64,
        delete_count: f64,
        text: JsValue,
    ) -> Result<(), error::Splice> {
        let (obj, obj_type) = self.import(obj)?;
        let start = start as usize;
        let delete_count = delete_count as isize;
        let vals = if let Some(t) = text.as_string() {
            if obj_type == am::ObjType::Text {
                self.doc.splice_text(&obj, start, delete_count, &t)?;
                return Ok(());
            } else {
                t.chars()
                    .map(|c| ScalarValue::Str(c.to_string().into()))
                    .collect::<Vec<_>>()
            }
        } else {
            let mut vals = vec![];
            if let Ok(array) = text.dyn_into::<Array>() {
                for i in array.iter() {
                    let value = import_scalar(&i, None)?;
                    vals.push(value);
                }
            }
            vals
        };
        if !vals.is_empty() {
            self.doc.splice(&obj, start, delete_count, vals)?;
        } else {
            // no vals given but we still need to call the text vs splice
            // bc utf16
            match obj_type {
                am::ObjType::List => {
                    self.doc.splice(&obj, start, delete_count, vals)?;
                }
                am::ObjType::Text => {
                    self.doc.splice_text(&obj, start, delete_count, "")?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = updateText)]
    pub fn update_text(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        #[wasm_bindgen(unchecked_param_type = "string")] new_text: JsValue,
    ) -> Result<(), error::UpdateText> {
        let (obj, obj_type) = self.import(obj)?;
        if !matches!(obj_type, am::ObjType::Text) {
            return Err(error::UpdateText::ObjectNotText);
        }
        if let Some(t) = new_text.as_string() {
            self.doc.update_text(&obj, t)?;
            Ok(())
        } else {
            Err(error::UpdateText::ValueNotString)
        }
    }

    #[wasm_bindgen(js_name = updateSpans)]
    pub fn update_spans(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Span[]")] args: JsValue,
        #[wasm_bindgen(unchecked_param_type = "UpdateSpansConfig | undefined | null")]
        config: JsValue,
    ) -> Result<(), error::UpdateSpans> {
        let (obj, obj_type) = self.import(obj)?;
        if !matches!(obj_type, am::ObjType::Text) {
            return Err(error::UpdateSpans::ObjectNotText);
        }
        let args = interop::import_update_spans_args(self, JS(args))?;
        let config = interop::import_update_spans_config(config)?;
        self.doc.update_spans(&obj, config, args.0)?;
        Ok(())
    }

    // skip_typescript as the datatype argument is optional which can only be
    // typed in the typescript custom section
    #[wasm_bindgen(skip_typescript)]
    pub fn push(
        &mut self,
        obj: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), error::Insert> {
        let (obj, _) = self.import(obj)?;
        let datatype = JS(datatype).try_into()?;
        let value = import_scalar(&value, datatype)?;
        let index = self.doc.length(&obj);
        self.doc.insert(&obj, index, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = pushObject, unchecked_return_type="ObjID")]
    pub fn push_object(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ObjType")] value: JsValue,
    ) -> Result<String, error::InsertObject> {
        let (obj, _) = self.import(obj)?;
        let imported_obj = import_obj(&value, None)?;
        let index = self.doc.length(&obj);
        let opid = self
            .doc
            .insert_object(&obj, index, imported_obj.objtype())?;
        if let Some(s) = imported_obj.text() {
            self.doc.splice_text(&opid, 0, 0, s)?;
        } else {
            self.subset::<error::InsertObject, _>(&opid, imported_obj.subvals())?;
        }
        Ok(opid.to_string())
    }

    // skip_typescript as the datatype argument is optional which can only be
    // typed in the typescript custom section
    #[wasm_bindgen(skip_typescript)]
    pub fn insert(
        &mut self,
        obj: JsValue,
        index: f64,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), error::Insert> {
        let (obj, _) = self.import(obj)?;
        let datatype = JS(datatype).try_into()?;
        let value = import_scalar(&value, datatype)?;
        self.doc.insert(&obj, index as usize, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = splitBlock)]
    pub fn split_block(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        index: f64,
        #[wasm_bindgen(unchecked_param_type = "{[key: string]: MaterializeValue}")] block: JsValue,
    ) -> Result<(), error::SplitBlock> {
        let (obj, _) = self.import(obj)?;
        let block_id = self.doc.split_block(&obj, index as usize)?;
        let hydrate = match interop::js_val_to_hydrate(self, block) {
            Ok(val @ am::hydrate::Value::Map(_)) => val,
            _ => return Err(error::SplitBlock::InvalidArgs),
        };

        self.doc.update_object(&block_id, &hydrate)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = joinBlock)]
    pub fn join_block(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        index: usize,
    ) -> Result<(), error::Block> {
        let (text, _) = self.import(obj)?;
        self.doc.join_block(&text, index)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = updateBlock)]
    pub fn update_block(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        index: usize,
        #[wasm_bindgen(unchecked_param_type = "{[key: string]: MaterializeValue}")] block: JsValue,
    ) -> Result<(), error::UpdateBlock> {
        let (text, _) = self.import(obj)?;
        let new_block = self.doc.replace_block(&text, index)?;
        let new_value = interop::js_val_to_hydrate(self, block)?;
        if !matches!(new_value, am::hydrate::Value::Map(_)) {
            return Err(error::UpdateBlock::InvalidArgs);
        }
        self.doc.update_object(&new_block, &new_value)?;
        Ok(())
    }

    // skip_typescript as the optional heads parameter can't be typed here
    #[wasm_bindgen(js_name = getBlock, skip_typescript)]
    pub fn get_block(
        &mut self,
        text: JsValue,
        index: usize,
        heads: JsValue,
    ) -> Result<JsValue, error::GetBlock> {
        let (text, _) = self.import(text)?;
        let Some((Value::Object(am::ObjType::Map), id)) = self.doc.get(&text, index)? else {
            return Ok(JsValue::null());
        };

        let heads = get_heads(heads)?;
        let hydrated = if let Some(h) = heads {
            self.doc.hydrate(&id, Some(&h))?
        } else {
            self.doc.hydrate(&id, None)?
        };

        Ok(interop::export_hydrate(
            self,
            &interop::ExportCache::new(self).unwrap(),
            hydrated,
        ))
    }

    #[wasm_bindgen(js_name = insertObject, unchecked_return_type="ObjID")]
    pub fn insert_object(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        index: f64,
        #[wasm_bindgen(unchecked_param_type = "ObjType")] value: JsValue,
    ) -> Result<String, error::InsertObject> {
        let (obj, _) = self.import(obj)?;
        let imported_obj = import_obj(&value, None)?;
        let opid = self
            .doc
            .insert_object(&obj, index as usize, imported_obj.objtype())?;
        if let Some(s) = imported_obj.text() {
            self.doc.splice_text(&opid, 0, 0, s)?;
        } else {
            self.subset::<error::InsertObject, _>(&opid, imported_obj.subvals())?;
        }
        Ok(opid.to_string())
    }

    // skip_typescript as the datatype argument is optional which can only be
    // typed in the custom section
    #[wasm_bindgen(skip_typescript)]
    pub fn put(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), error::Insert> {
        let (obj, _) = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let datatype = JS(datatype).try_into()?;
        let value = import_scalar(&value, datatype)?;
        self.doc.put(&obj, prop, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = putObject, unchecked_return_type="ObjID")]
    pub fn put_object(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Prop")] prop: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ObjType")] value: JsValue,
    ) -> Result<JsValue, error::InsertObject> {
        let (obj, _) = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let imported_obj = import_obj(&value, None)?;
        let opid = self.doc.put_object(&obj, prop, imported_obj.objtype())?;
        if let Some(s) = imported_obj.text() {
            self.doc.splice_text(&opid, 0, 0, s)?;
        } else {
            self.subset::<error::InsertObject, _>(&opid, imported_obj.subvals())?;
        }
        Ok(opid.to_string().into())
    }

    fn subset<'a, E, I>(&mut self, obj: &am::ObjId, vals: I) -> Result<(), E>
    where
        I: IntoIterator<Item = (Cow<'a, am::Prop>, JsValue)>,
        E: From<automerge::AutomergeError>
            + From<interop::error::ImportObj>
            + From<interop::error::InvalidValue>,
    {
        for (p, v) in vals {
            let (value, subvals) = self.import_value(&v, None)?;
            //let opid = self.0.set(id, p, value)?;
            let opid = match (p.as_ref(), value) {
                (Prop::Map(s), Value::Object(objtype)) => {
                    Some(self.doc.put_object(obj, s, objtype)?)
                }
                (Prop::Map(s), Value::Scalar(scalar)) => {
                    self.doc.put(obj, s, scalar.into_owned())?;
                    None
                }
                (Prop::Seq(i), Value::Object(objtype)) => {
                    Some(self.doc.insert_object(obj, *i, objtype)?)
                }
                (Prop::Seq(i), Value::Scalar(scalar)) => {
                    self.doc.insert(obj, *i, scalar.into_owned())?;
                    None
                }
            };
            if let Some(opid) = opid {
                self.subset::<E, _>(&opid, SubValIter::Slice(subvals.as_slice().iter()))?;
            }
        }
        Ok(())
    }

    pub fn increment(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Prop")] prop: JsValue,
        #[wasm_bindgen(unchecked_param_type = "number")] value: JsValue,
    ) -> Result<(), error::Increment> {
        let (obj, _) = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value: f64 = value.as_f64().ok_or(error::Increment::ValueNotNumeric)?;
        self.doc.increment(&obj, prop, value as i64)?;
        Ok(())
    }

    // skip_typescript as the optional heads parameter can't be typed here
    #[wasm_bindgen(js_name = get, skip_typescript)]
    pub fn get(&self, obj: JsValue, prop: JsValue, heads: JsValue) -> Result<JsValue, error::Get> {
        let (obj, _) = self.import(obj)?;
        let prop = to_prop(prop);
        let heads = get_heads(heads)?;
        if let Ok(prop) = prop {
            let value = if let Some(h) = heads {
                self.doc.get_at(&obj, prop, &h)?
            } else {
                self.doc.get(&obj, prop)?
            };
            if let Some((value, id)) = value {
                match alloc(&value) {
                    (datatype, js_value) if datatype.is_scalar() => Ok(js_value),
                    _ => Ok(id.to_string().into()),
                }
            } else {
                Ok(JsValue::undefined())
            }
        } else {
            Ok(JsValue::undefined())
        }
    }

    // skip_typescript as the optional heads parameter can't be typed
    #[wasm_bindgen(js_name = getWithType, skip_typescript)]
    pub fn get_with_type(
        &self,
        obj: JsValue,
        prop: JsValue,
        heads: JsValue,
    ) -> Result<JsValue, error::Get> {
        let (obj, _) = self.import(obj)?;
        let prop = to_prop(prop);
        let heads = get_heads(heads)?;
        if let Ok(prop) = prop {
            let value = if let Some(h) = heads {
                self.doc.get_at(&obj, prop, &h)?
            } else {
                self.doc.get(&obj, prop)?
            };
            if let Some(value) = value {
                match &value {
                    (Value::Object(obj_type), obj_id) => {
                        let result = Array::new();
                        result.push(&obj_type.to_string().into());
                        result.push(&obj_id.to_string().into());
                        Ok(result.into())
                    }
                    (Value::Scalar(_), _) => {
                        let result = Array::new();
                        let (datatype, value) = alloc(&value.0);
                        result.push(&datatype.into());
                        result.push(&value);
                        Ok(result.into())
                    }
                }
            } else {
                Ok(JsValue::null())
            }
        } else {
            Ok(JsValue::null())
        }
    }

    // skip_typescript as we can't type the optional heads parameter
    #[wasm_bindgen(js_name = objInfo, skip_typescript)]
    pub fn obj_info(&self, obj: JsValue, heads: JsValue) -> Result<Object, error::Get> {
        // fixme - import takes a path - needs heads to be accurate
        let (obj, _) = self.import(obj)?;
        let typ = self.doc.object_type(&obj)?;
        let result = Object::new();
        let parents = if let Some(heads) = get_heads(heads)? {
            self.doc.parents_at(&obj, &heads)
        } else {
            self.doc.parents(&obj)
        }?;
        js_set(&result, "id", obj.to_string())?;
        js_set(&result, "type", typ.to_string())?;
        if let Some(path) = parents.visible_path() {
            let path = interop::export_just_path(&path);
            js_set(&result, "path", &path)?;
        }
        Ok(result)
    }

    // skip_typescript as the optional heads parameter can't be typed here
    #[wasm_bindgen(js_name = getAll, skip_typescript)]
    pub fn get_all(&self, obj: JsValue, arg: JsValue, heads: JsValue) -> Result<Array, error::Get> {
        let (obj, _) = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(arg);
        if let Ok(prop) = prop {
            let values = if let Some(heads) = get_heads(heads)? {
                self.doc.get_all_at(&obj, prop, &heads)
            } else {
                self.doc.get_all(&obj, prop)
            }?;
            for (value, id) in values {
                let sub = Array::new();
                let (datatype, js_value) = alloc(&value);
                sub.push(&datatype.into());
                if value.is_scalar() {
                    sub.push(&js_value);
                }
                sub.push(&id.to_string().into());
                result.push(&JsValue::from(&sub));
            }
        }
        Ok(result)
    }

    #[wasm_bindgen(js_name = enableFreeze)]
    pub fn enable_freeze(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "boolean")] enable: JsValue,
    ) -> Result<bool, JsValue> {
        let enable = enable
            .as_bool()
            .ok_or_else(|| to_js_err("must pass a bool to enableFreeze"))?;
        let old_freeze = self.freeze;
        self.freeze = enable;
        Ok(old_freeze)
    }

    #[wasm_bindgen(js_name=registerDatatype)]
    pub fn register_datatype(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "string")] datatype: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Function", js_name = "construct")]
        export_function: JsValue,
        #[wasm_bindgen(
            unchecked_param_type = "(arg: any) => any | undefined",
            js_name = "deconstruct"
        )]
        import_function: JsValue,
    ) -> Result<(), value::InvalidDatatype> {
        let datatype = Datatype::try_from(datatype)?;
        let Ok(export_function) = export_function.dyn_into::<Function>() else {
            self.external_types.remove(&datatype);
            return Ok(());
        };
        let Ok(import_function) = import_function.dyn_into::<Function>() else {
            self.external_types.remove(&datatype);
            return Ok(());
        };
        self.external_types.insert(
            datatype,
            interop::ExternalTypeConstructor::new(export_function, import_function),
        );
        Ok(())
    }

    // skip_typescript as the function can only by typed in the custom section
    #[wasm_bindgen(js_name = applyPatches, skip_typescript)]
    pub fn apply_patches(&mut self, object: JsValue, meta: JsValue) -> Result<JsValue, JsValue> {
        let (value, _patches) = self.apply_patches_impl(object, meta)?;
        Ok(value)
    }

    // skip_typescript as the function can only by typed in the custom section
    #[wasm_bindgen(js_name = applyAndReturnPatches, skip_typescript)]
    pub fn apply_and_return_patches(
        &mut self,
        object: JsValue,
        meta: JsValue,
    ) -> Result<JsValue, JsValue> {
        let (value, patches) = self.apply_patches_impl(object, meta)?;

        let patches = interop::export_patches(&self.external_types, patches)?;

        let result = Object::new();
        js_set(&result, "value", value)?;
        js_set(&result, "patches", patches)?;
        Ok(result.into())
    }

    fn apply_patches_impl(
        &mut self,
        object: JsValue,
        meta: JsValue,
    ) -> Result<(JsValue, Vec<automerge::Patch>), JsValue> {
        let mut object = object
            .dyn_into::<Object>()
            .map_err(|_| error::ApplyPatch::NotObjectd)?;

        let shortcut = self.doc.diff_cursor().is_empty();
        let patches = self.doc.diff_incremental();

        let mut cache = interop::ExportCache::new(self)?;

        if shortcut {
            let value = cache.materialize(ROOT, Datatype::Map, None, &meta)?;
            return Ok((value, patches));
        }

        // even if there are no patches we may need to update the meta object
        // which requires that we update the object too
        if !meta.is_undefined() {
            let (_, cached_obj) = self.unwrap_object(&object, &mut cache, &meta)?;
            object = cached_obj.inner;
        }

        for p in &patches {
            object = self.apply_patch(object, p, &meta, &mut cache)?;
        }

        if self.freeze {
            Object::freeze(&object);
        }

        Ok((object.into(), patches))
    }

    #[wasm_bindgen(js_name = diffIncremental, unchecked_return_type="Patch[]")]
    pub fn diff_incremental(&mut self) -> Result<Array, error::PopPatches> {
        // transactions send out observer updates as they occur, not waiting for them to be
        // committed.
        // If we pop the patches then we won't be able to revert them.

        let patches = self.doc.diff_incremental();
        let result = interop::export_patches(&self.external_types, patches)?;
        Ok(result)
    }

    #[wasm_bindgen(js_name = updateDiffCursor)]
    pub fn update_diff_cursor(&mut self) {
        self.doc.update_diff_cursor();
    }

    #[wasm_bindgen(js_name = resetDiffCursor)]
    pub fn reset_diff_cursor(&mut self) {
        self.doc.reset_diff_cursor();
    }

    #[wasm_bindgen(unchecked_return_type = "Patch[]")]
    pub fn diff(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Heads")] before: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Heads")] after: JsValue,
    ) -> Result<Array, error::Diff> {
        let before = get_heads(before)
            .map_err(error::Diff::InvalidBeforeHeads)?
            .ok_or_else(|| error::Diff::MissingBeforeHeads)?;
        let after = get_heads(after)
            .map_err(error::Diff::InvalidAfterHeads)?
            .ok_or_else(|| error::Diff::MissingAfterHeads)?;

        let patches = self.doc.diff(&before, &after);

        Ok(interop::export_patches(&self.external_types, patches)?)
    }

    #[wasm_bindgen(js_name = diffPath, skip_typescript)]
    pub fn diff_path(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Prop[] | string")] path: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Heads")] before: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Heads")] after: JsValue,
        options: JsValue,
    ) -> Result<Array, error::Diff> {
        let obj = self.import(path)?.0;
        let recursive = js_get(&options, "recursive")
            .ok()
            .and_then(|a| a.as_bool())
            .unwrap_or(true);

        let before = get_heads(before)
            .map_err(error::Diff::InvalidBeforeHeads)?
            .ok_or_else(|| error::Diff::MissingBeforeHeads)?;
        let after = get_heads(after)
            .map_err(error::Diff::InvalidBeforeHeads)?
            .ok_or_else(|| error::Diff::MissingBeforeHeads)?;

        let patches = self.doc.diff_obj(&obj, &before, &after, recursive)?;

        Ok(interop::export_patches(&self.external_types, patches)?)
    }

    pub fn isolate(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Heads")] heads: JsValue,
    ) -> Result<(), error::Isolate> {
        let Some(heads) = get_heads(heads)? else {
            return Err(error::Isolate::NoHeads);
        };

        self.doc.isolate(&heads);
        Ok(())
    }

    pub fn integrate(&mut self) {
        self.doc.integrate()
    }

    // skip_typescript as the optional heads parameter can't be typed
    #[wasm_bindgen(skip_typescript)]
    pub fn length(&self, obj: JsValue, heads: JsValue) -> Result<f64, error::Get> {
        let (obj, _) = self.import(obj)?;
        if let Some(heads) = get_heads(heads)? {
            Ok(self.doc.length_at(&obj, &heads) as f64)
        } else {
            Ok(self.doc.length(&obj) as f64)
        }
    }

    pub fn delete(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        #[wasm_bindgen(unchecked_param_type = "Prop")] prop: JsValue,
    ) -> Result<(), error::Get> {
        let (obj, _) = self.import(obj)?;
        let prop = to_prop(prop)?;
        self.doc.delete(&obj, prop)?;
        Ok(())
    }

    pub fn save(&mut self) -> Uint8Array {
        Uint8Array::from(self.doc.save().as_slice())
    }

    #[wasm_bindgen(js_name = saveIncremental)]
    pub fn save_incremental(&mut self) -> Uint8Array {
        let bytes = self.doc.save_incremental();
        Uint8Array::from(bytes.as_slice())
    }

    #[wasm_bindgen(js_name=saveSince)]
    pub fn save_since(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Heads")] heads: JsValue,
    ) -> Result<Uint8Array, interop::error::BadChangeHashes> {
        let heads = get_heads(heads)?.unwrap_or(Vec::new());
        let bytes = self.doc.save_after(&heads);
        Ok(Uint8Array::from(bytes.as_slice()))
    }

    #[wasm_bindgen(js_name = saveNoCompress)]
    pub fn save_nocompress(&mut self) -> Uint8Array {
        let bytes = self.doc.save_nocompress();
        Uint8Array::from(bytes.as_slice())
    }

    #[wasm_bindgen(js_name = saveAndVerify)]
    pub fn save_and_verify(&mut self) -> Result<Uint8Array, error::Load> {
        let bytes = self.doc.save_and_verify()?;
        Ok(Uint8Array::from(bytes.as_slice()))
    }

    #[wasm_bindgen(js_name = loadIncremental)]
    pub fn load_incremental(&mut self, data: Uint8Array) -> Result<f64, error::Load> {
        let data = data.to_vec();
        let len = self.doc.load_incremental(&data)?;
        Ok(len as f64)
    }

    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Change[]")] changes: JsValue,
    ) -> Result<(), error::ApplyChangesError> {
        let changes: Vec<Change> = JS(changes).try_into()?;

        //for c in &changes {
        //am::log!("apply change: {:?}", c.raw_bytes());
        //}
        self.doc.apply_changes(changes)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = getChanges, unchecked_return_type="Change[]")]
    pub fn get_changes(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Heads")] have_deps: JsValue,
    ) -> Result<Array, error::Get> {
        let deps: Vec<_> = JS(have_deps).try_into()?;
        let changes = self.doc.get_changes(&deps);
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getChangesMeta, unchecked_return_type="ChangeMetadata[]")]
    pub fn get_changes_meta(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Heads")] have_deps: JsValue,
    ) -> Result<Array, error::Get> {
        let deps: Vec<_> = JS(have_deps).try_into()?;
        let changes = self.doc.get_changes_meta(&deps);
        let changes: Array = changes.iter().map(JS::from).collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getChangeByHash, unchecked_return_type="Change | null")]
    pub fn get_change_by_hash(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Hash")] hash: JsValue,
    ) -> Result<JsValue, interop::error::BadChangeHash> {
        let hash = JS(hash).try_into()?;
        let change = self.doc.get_change_by_hash(&hash);
        if let Some(c) = change {
            Ok(Uint8Array::from(c.raw_bytes()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = getChangeMetaByHash, unchecked_return_type="ChangeMetadata | null")]
    pub fn get_change_meta_by_hash(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Hash")] hash: JsValue,
    ) -> Result<JsValue, interop::error::BadChangeHash> {
        let hash = JS(hash).try_into()?;
        let change_meta = self.doc.get_change_meta_by_hash(&hash);
        if let Some(c) = change_meta {
            Ok(JS::from(&c).0)
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = getDecodedChangeByHash, unchecked_return_type="DecodedChange | null")]
    pub fn get_decoded_change_by_hash(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Hash")] hash: JsValue,
    ) -> Result<JsValue, error::GetDecodedChangeByHash> {
        let hash = JS(hash).try_into()?;
        let change = self.doc.get_change_by_hash(&hash);
        if let Some(c) = change {
            let change: am::ExpandedChange = c.decode();
            let serializer = serde_wasm_bindgen::Serializer::json_compatible();
            Ok(change.serialize(&serializer)?)
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = getChangesAdded, unchecked_return_type="Change[]")]
    pub fn get_changes_added(&mut self, other: &mut Automerge) -> Array {
        let changes = self.doc.get_changes_added(&mut other.doc);
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        changes
    }

    #[wasm_bindgen(js_name = getHeads, unchecked_return_type="Heads")]
    pub fn get_heads(&mut self) -> Array {
        let heads = self.doc.get_heads();
        AR::from(heads).into()
    }

    #[wasm_bindgen(js_name = getActorId, unchecked_return_type="Actor")]
    pub fn get_actor_id(&self) -> String {
        self.doc.get_actor().to_string()
    }

    #[wasm_bindgen(js_name = getLastLocalChange, unchecked_return_type="Change | null")]
    pub fn get_last_local_change(&mut self) -> JsValue {
        if let Some(change) = self.doc.get_last_local_change() {
            Uint8Array::from(change.raw_bytes()).into()
        } else {
            JsValue::null()
        }
    }

    pub fn dump(&mut self) {
        self.doc.dump()
    }

    // skip_typescript as the optional heads parameter can't be typed
    #[wasm_bindgen(js_name = getMissingDeps, skip_typescript)]
    pub fn get_missing_deps(&mut self, heads: JsValue) -> Result<Array, error::Get> {
        let heads = get_heads(heads)?.unwrap_or_default();
        let deps = self.doc.get_missing_deps(&heads);
        let deps: Array = deps
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(h.0)))
            .collect();
        Ok(deps)
    }

    #[wasm_bindgen(js_name = receiveSyncMessage)]
    pub fn receive_sync_message(
        &mut self,
        state: &mut SyncState,
        #[wasm_bindgen(unchecked_param_type = "SyncMessage")] message: Uint8Array,
    ) -> Result<(), error::ReceiveSyncMessage> {
        let message = message.to_vec();
        //am::log!("receive sync message: {:?}", message.as_slice());
        let message = am::sync::Message::decode(message.as_slice())?;
        self.doc
            .sync()
            .receive_sync_message(&mut state.0, message)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = generateSyncMessage, unchecked_return_type = "SyncMessage | null")]
    pub fn generate_sync_message(&mut self, state: &mut SyncState) -> JsValue {
        if let Some(message) = self.doc.sync().generate_sync_message(&mut state.0) {
            let message = message.encode();
            //am::log!("generate sync message: {:?}", message.as_slice());
            Uint8Array::from(message.as_slice()).into()
        } else {
            JsValue::null()
        }
    }

    #[wasm_bindgen(js_name = toJS, unchecked_return_type="MaterializeValue")]
    pub fn to_js(&mut self, meta: JsValue) -> Result<JsValue, interop::error::Export> {
        let mut cache = interop::ExportCache::new(self)?;
        cache.materialize(ROOT, Datatype::Map, None, &meta)
    }

    // Skip typescript as the arguments are all optional which can only be typed
    // in the typescript custom section
    #[wasm_bindgen(skip_typescript)]
    pub fn materialize(
        &mut self,
        obj: JsValue,
        heads: JsValue,
        meta: JsValue,
    ) -> Result<JsValue, error::Materialize> {
        let (obj, obj_type) = self.import(obj).unwrap_or((ROOT, am::ObjType::Map));
        let heads = get_heads(heads)?;
        self.doc.update_diff_cursor();
        let mut cache = interop::ExportCache::new(self)?;
        Ok(cache.materialize(obj, obj_type.into(), heads.as_deref(), &meta)?)
    }

    // skip_typescript as the heads and move_cursor arguments are optional which
    // can only be typed in the typescript custom section
    #[wasm_bindgen(js_name = getCursor, skip_typescript)]
    pub fn get_cursor(
        &mut self,
        obj: JsValue,
        position: JsValue,
        heads: JsValue,
        move_cursor: JsValue,
    ) -> Result<String, error::Cursor> {
        let (obj, obj_type) = self.import(obj).unwrap_or((ROOT, am::ObjType::Map));
        if obj_type != am::ObjType::Text {
            return Err(error::Cursor::InvalidObjType(obj_type));
        }

        let heads = get_heads(heads)?;

        let position: CursorPosition = JS(position)
            .try_into()
            .map_err(|_| error::Cursor::InvalidCursorPosition)?;

        // TODO do we want this?

        // convert positions >= string.length into `CursorPosition::End`
        // note: negative indices are converted to `CursorPosition::Start` in
        // `impl TryFrom<JS> for CursorPosition`
        let len = match heads {
            Some(ref heads) => self.doc.length_at(&obj, heads),
            None => self.doc.length(&obj),
        };

        let position = match position {
            CursorPosition::Index(i) if i >= len => CursorPosition::End,
            _ => position,
        };

        let cursor = if move_cursor.is_undefined() {
            self.doc.get_cursor(obj, position, heads.as_deref())?
        } else {
            let move_cursor = JS(move_cursor)
                .try_into()
                .map_err(|_| error::Cursor::InvalidMoveCursor)?;

            self.doc
                .get_cursor_moving(obj, position, heads.as_deref(), move_cursor)?
        };

        Ok(cursor.to_string())
    }

    // skip_typescript as the optional heads parameter can't be typed
    #[wasm_bindgen(js_name = getCursorPosition, skip_typescript)]
    pub fn get_cursor_position(
        &mut self,
        obj: JsValue,
        cursor: JsValue,
        heads: JsValue,
    ) -> Result<f64, error::Cursor> {
        let (obj, obj_type) = self.import(obj).unwrap_or((ROOT, am::ObjType::Map));
        if obj_type != am::ObjType::Text {
            return Err(error::Cursor::InvalidObjType(obj_type));
        }
        let cursor = cursor.as_string().ok_or(error::Cursor::InvalidCursor)?;
        let cursor = am::Cursor::try_from(cursor)?;
        let heads = get_heads(heads)?;
        let position = self
            .doc
            .get_cursor_position(obj, &cursor, heads.as_deref())?;
        Ok(position as f64)
    }

    #[wasm_bindgen(js_name = emptyChange, unchecked_return_type="Hash")]
    pub fn empty_change(&mut self, message: Option<String>, time: Option<f64>) -> JsValue {
        let time = time.map(|f| f as i64);
        let options = CommitOptions { message, time };
        let hash = self.doc.empty_change(options);
        JsValue::from_str(&hex::encode(hash))
    }

    // skip_typescript because the datatype argument is optional which can only
    // be typed in the typescript custom section
    #[wasm_bindgen(skip_typescript)]
    pub fn mark(
        &mut self,
        obj: JsValue,
        range: JsValue,
        name: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), error::Mark> {
        let (obj, _) = self.import(obj)?;

        let range = range
            .dyn_into::<Object>()
            .map_err(|_| error::Mark::InvalidRange)?;

        let start = js_get(&range, "start").map_err(|_| error::Mark::InvalidStart)?;
        let start = start.try_into().map_err(|_| error::Mark::InvalidStart)?;

        let end = js_get(&range, "end").map_err(|_| error::Mark::InvalidEnd)?;
        let end = end.try_into().map_err(|_| error::Mark::InvalidEnd)?;

        let expand = js_get(&range, "expand").ok();
        let expand = expand.map(|s| s.try_into()).transpose()?;
        let expand = expand.unwrap_or_default();

        let name = name.as_string().ok_or(error::Mark::InvalidName)?;

        let datatype = JS(datatype).try_into()?;
        let value = import_scalar(&value, datatype)?;

        self.doc
            .mark(&obj, Mark::new(name, value, start, end), expand)?;
        Ok(())
    }

    pub fn unmark(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ObjID")] obj: JsValue,
        #[wasm_bindgen(unchecked_param_type = "MarkRange")] range: JsValue,
        #[wasm_bindgen(unchecked_param_type = "string")] name: JsValue,
    ) -> Result<(), error::Mark> {
        self.mark(obj, range, name, JsValue::NULL, JsValue::from_str("null"))
    }

    // skip_typescript as we can't type the optional heads paramater
    #[wasm_bindgen(skip_typescript)]
    pub fn marks(&mut self, obj: JsValue, heads: JsValue) -> Result<JsValue, JsValue> {
        let (obj, _) = self.import(obj)?;
        let heads = get_heads(heads)?;
        let marks = if let Some(heads) = heads {
            self.doc.marks_at(obj, &heads).map_err(to_js_err)?
        } else {
            self.doc.marks(obj).map_err(to_js_err)?
        };
        let result = Array::new();
        for m in marks {
            let mark = Object::new();
            let (_datatype, value) = alloc(&m.value().clone().into());
            js_set(&mark, "name", m.name())?;
            js_set(&mark, "value", value)?;
            js_set(&mark, "start", m.start as i32)?;
            js_set(&mark, "end", m.end as i32)?;
            result.push(&mark.into());
        }
        Ok(result.into())
    }

    // skip_typescript as we can't type the optional heads paramater
    #[wasm_bindgen(js_name = marksAt, skip_typescript)]
    pub fn marks_at(
        &mut self,
        obj: JsValue,
        index: f64,
        heads: JsValue,
    ) -> Result<Object, JsValue> {
        let (obj, _) = self.import(obj)?;
        let heads = get_heads(heads)?;
        let marks = self
            .doc
            .get_marks(obj, index as usize, heads.as_deref())
            .map_err(to_js_err)?;
        let result = Object::new();
        for (mark, value) in marks.iter() {
            let (_datatype, value) = alloc(&value.into());
            js_set(&result, mark, value)?;
        }
        Ok(result)
    }

    pub(crate) fn text_at(
        &self,
        obj: &am::ObjId,
        heads: Option<&[am::ChangeHash]>,
    ) -> Result<String, am::AutomergeError> {
        if let Some(heads) = heads {
            Ok(self.doc.text_at(obj, heads)?)
        } else {
            Ok(self.doc.text(obj)?)
        }
    }

    #[wasm_bindgen(js_name = hasOurChanges)]
    pub fn has_our_changes(&mut self, state: &mut SyncState) -> bool {
        self.doc.has_our_changes(&state.0)
    }

    #[wasm_bindgen(js_name = topoHistoryTraversal, unchecked_return_type="Hash[]")]
    pub fn topo_history_traversal(&mut self) -> JsValue {
        let hashes = self
            .doc
            .get_changes(&[])
            .into_iter()
            .map(|c| c.hash())
            .collect::<Vec<_>>();
        AR::from(hashes).into()
    }

    #[wasm_bindgen(js_name = stats, unchecked_return_type="Stats")]
    pub fn stats(&mut self) -> JsValue {
        let stats = self.doc.stats();
        let result = Object::new();
        js_set(
            &result,
            "numChanges",
            JsValue::from(stats.num_changes as usize),
        )
        .unwrap();
        js_set(&result, "numOps", JsValue::from(stats.num_ops as usize)).unwrap();
        js_set(
            &result,
            "numActors",
            JsValue::from(stats.num_actors as usize),
        )
        .unwrap();
        result.into()
    }

    #[wasm_bindgen(js_name = "saveBundle")]
    pub fn save_bundle(&mut self, hashes: JsValue) -> Result<Uint8Array, error::SaveBundle> {
        let hashes: Vec<automerge::ChangeHash> = JS(hashes).try_into()?;
        let bundle = self
            .doc
            .bundle(hashes.into_iter())
            .map_err(error::SaveBundle::DoBundle)?;
        Ok(Uint8Array::from(bundle.bytes()))
    }
}

// skip_typescript as the definition requires an optional argument so we define
// the function in the typescript custom section at the top of the file
#[wasm_bindgen(js_name = create, skip_typescript)]
pub fn init(options: JsValue) -> Result<Automerge, error::BadActorId> {
    console_error_panic_hook::set_once();
    let actor = js_get(&options, "actor").ok().and_then(|a| a.as_string());
    Automerge::new(actor)
}

// skip_typescript as the options argument is optional which can only be typed
// in the typescript custom section
#[wasm_bindgen(js_name = load, skip_typescript)]
pub fn load(data: Uint8Array, options: JsValue) -> Result<Automerge, error::Load> {
    let data = data.to_vec();
    let actor = js_get(&options, "actor").ok().and_then(|a| a.as_string());
    let unchecked = js_get(&options, "unchecked")
        .ok()
        .and_then(|v1| v1.as_bool())
        .unwrap_or(false);
    let verification_mode = if unchecked {
        VerificationMode::DontCheck
    } else {
        VerificationMode::Check
    };
    let allow_missing_deps = js_get(&options, "allowMissingDeps")
        .ok()
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let on_partial_load = if allow_missing_deps {
        OnPartialLoad::Ignore
    } else {
        OnPartialLoad::Error
    };
    let string_migration = if js_get(&options, "convertImmutableStringsToText")
        .ok()
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        StringMigration::ConvertToText
    } else {
        StringMigration::NoMigration
    };
    let mut doc = am::AutoCommit::load_with_options(
        &data,
        am::LoadOptions::new()
            .on_partial_load(on_partial_load)
            .verification_mode(verification_mode)
            .migrate_strings(string_migration)
            .text_encoding(TextEncoding::Utf16CodeUnit),
    )?;
    if let Some(s) = actor {
        let actor =
            automerge::ActorId::from(hex::decode(s).map_err(error::BadActorId::from)?.to_vec());
        doc.set_actor(actor);
    }
    Ok(Automerge {
        doc,
        freeze: false,
        external_types: HashMap::default(),
    })
}

#[wasm_bindgen(js_name = wasmReleaseInfo, unchecked_return_type = "WasmReleaseInfo")]
pub fn wasm_release_info() -> JsValue {
    let result = Object::new();
    js_set(
        &result,
        "gitHead",
        JsValue::from_str(option_env!("GIT_HEAD").unwrap_or("unknown")),
    )
    .unwrap();
    js_set(
        &result,
        "cargoPackageName",
        JsValue::from_str(env!("CARGO_PKG_NAME")),
    )
    .unwrap();
    js_set(
        &result,
        "cargoPackageVersion",
        JsValue::from_str(env!("CARGO_PKG_VERSION")),
    )
    .unwrap();
    js_set(
        &result,
        "rustcVersion",
        JsValue::from_str(env!("CARGO_PKG_RUST_VERSION")),
    )
    .unwrap();
    result.into()
}

#[wasm_bindgen(js_name = encodeChange)]
pub fn encode_change(change: JsValue) -> Result<Uint8Array, error::EncodeChange> {
    // Alex: Technically we should be using serde_wasm_bindgen::from_value instead of into_serde.
    // Unfortunately serde_wasm_bindgen::from_value fails for some inscrutable reason, so instead
    // we use into_serde (sorry to future me).
    #[allow(deprecated)]
    let change: am::ExpandedChange = change.into_serde()?;
    let change: Change = change.into();
    Ok(Uint8Array::from(change.raw_bytes()))
}

#[wasm_bindgen(js_name = decodeChange, unchecked_return_type="DecodedChange")]
pub fn decode_change(change: Uint8Array) -> Result<JsValue, error::DecodeChange> {
    let change = Change::from_bytes(change.to_vec())?;
    let change: am::ExpandedChange = change.decode();
    let serializer = serde_wasm_bindgen::Serializer::json_compatible();
    Ok(change.serialize(&serializer)?)
}

#[wasm_bindgen(js_name = initSyncState, unchecked_return_type="SyncState")]
pub fn init_sync_state() -> SyncState {
    SyncState(am::sync::State::new())
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = importSyncState)]
pub fn import_sync_state(state: JsValue) -> Result<SyncState, interop::error::BadSyncState> {
    Ok(SyncState(JS(state).try_into()?))
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = exportSyncState, unchecked_return_type="JsSyncState")]
pub fn export_sync_state(state: &SyncState) -> JsValue {
    JS::from(state.0.clone()).into()
}

#[wasm_bindgen(js_name = encodeSyncMessage, unchecked_return_type="SyncMessage")]
pub fn encode_sync_message(message: JsValue) -> Result<Uint8Array, interop::error::BadSyncMessage> {
    let message: am::sync::Message = JS(message).try_into()?;
    Ok(Uint8Array::from(message.encode().as_slice()))
}

#[wasm_bindgen(js_name = decodeSyncMessage, unchecked_return_type="DecodedSyncMessage")]
pub fn decode_sync_message(msg: Uint8Array) -> Result<JsValue, error::BadSyncMessage> {
    let data = msg.to_vec();
    let msg = am::sync::Message::decode(&data)?;
    let heads = AR::from(msg.heads.as_slice());
    let need = AR::from(msg.need.as_slice());
    let changes = AR::from(&msg.changes);
    let have = AR::from(msg.have.as_slice());
    let obj = Object::new().into();
    // SAFETY: we just created this object
    js_set(&obj, "heads", heads).unwrap();
    js_set(&obj, "need", need).unwrap();
    js_set(&obj, "have", have).unwrap();
    js_set(&obj, "changes", changes).unwrap();

    match msg.version {
        am::sync::MessageVersion::V1 => {
            js_set(&obj, "type", JsValue::from_str("v1")).unwrap();
        }
        am::sync::MessageVersion::V2 => {
            js_set(&obj, "type", JsValue::from_str("v2")).unwrap();
        }
    };

    if let Some(caps) = msg.supported_capabilities {
        let caps = AR::from(caps.as_slice());
        js_set(&obj, "supportedCapabilities", caps).unwrap();
    }

    Ok(obj)
}

#[wasm_bindgen(js_name = encodeSyncState)]
pub fn encode_sync_state(state: &SyncState) -> Uint8Array {
    Uint8Array::from(state.0.encode().as_slice())
}

#[wasm_bindgen(js_name = decodeSyncState, unchecked_return_type="SyncState")]
pub fn decode_sync_state(data: Uint8Array) -> Result<SyncState, sync::DecodeSyncStateErr> {
    SyncState::decode(data)
}

struct UpdateSpansArgs(Vec<am::iter::Span>);

#[wasm_bindgen(js_name = "readBundle")]
pub fn read_bundle(bundle: Uint8Array) -> Result<JsValue, error::ReadBundle> {
    let bundle_bytes = bundle.to_vec();
    let bundle = automerge::Bundle::try_from(bundle_bytes.as_slice())
        .map_err(|e| error::ReadBundle(e.to_string()))?;
    let changes = bundle
        .to_changes()
        .map_err(|e| error::ReadBundle(e.to_string()))?;
    let js_changes = changes
        .iter()
        .map(|c| {
            let legacy_change = automerge::ExpandedChange::from(c);
            let serializer = serde_wasm_bindgen::Serializer::json_compatible();
            legacy_change
                .serialize(&serializer)
                .map_err(|e| error::ReadBundle(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let result = js_sys::Object::new();
    Reflect::set(
        &result,
        &("changes").into(),
        &js_sys::Array::from_iter(js_changes.iter()).into(),
    )
    .unwrap();

    let deps = bundle.deps().to_vec();
    Reflect::set(&result, &("deps").into(), &JS::from(deps).0).unwrap();
    Ok(result.into())
}

pub mod error {
    use automerge::{AutomergeError, ObjType};
    use js_sys::RangeError;
    use wasm_bindgen::JsValue;

    use crate::interop::{
        self,
        error::{BadChangeHashes, BadJSChanges},
    };

    #[derive(Debug, thiserror::Error)]
    #[error("could not parse Actor ID as a hex string: {0}")]
    pub struct BadActorId(#[from] hex::FromHexError);

    impl From<BadActorId> for JsValue {
        fn from(s: BadActorId) -> Self {
            RangeError::new(&s.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ApplyChangesError {
        #[error(transparent)]
        DecodeChanges(#[from] BadJSChanges),
        #[error("error applying changes: {0}")]
        Apply(#[from] AutomergeError),
    }

    impl From<ApplyChangesError> for JsValue {
        fn from(e: ApplyChangesError) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Fork {
        #[error(transparent)]
        BadActor(#[from] BadActorId),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        BadChangeHashes(#[from] BadChangeHashes),
    }

    impl From<Fork> for JsValue {
        fn from(f: Fork) -> Self {
            RangeError::new(&f.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error(transparent)]
    pub struct Merge(#[from] AutomergeError);

    impl From<Merge> for JsValue {
        fn from(e: Merge) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Get {
        #[error("invalid object ID: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error("object not visible")]
        NotVisible,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error("bad heads: {0}")]
        BadHeads(#[from] interop::error::BadChangeHashes),
        #[error(transparent)]
        InvalidProp(#[from] interop::error::InvalidProp),
        #[error(transparent)]
        ExportError(#[from] interop::error::SetProp),
    }

    impl From<Get> for JsValue {
        fn from(e: Get) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Splice {
        #[error("invalid object ID: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        InvalidImport(#[from] interop::error::ImportValue),
        #[error("value at {0} in values to insert was not a primitive")]
        ValueNotPrimitive(usize),
    }

    impl From<Splice> for JsValue {
        fn from(e: Splice) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum UpdateText {
        #[error("invalid object ID: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error("object was not a text object")]
        ObjectNotText,
        #[error("update_text is only availalbe for the string representation of text objects")]
        TextRepNotString,
        #[error("value passed to update_text was not a string")]
        ValueNotString,
    }

    impl From<UpdateText> for JsValue {
        fn from(e: UpdateText) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum UpdateSpans {
        #[error("invalid object ID: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error("object was not a text object")]
        ObjectNotText,
        #[error(transparent)]
        InvalidArgs(#[from] interop::error::InvalidUpdateSpansArgs),
        #[error("update_text is only availalbe for the string representation of text objects")]
        TextRepNotString,
        #[error("invalid config: {0}")]
        BadConfig(#[from] interop::error::ImportUpdateSpansConfig),
    }

    impl From<UpdateSpans> for JsValue {
        fn from(e: UpdateSpans) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Insert {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        InvalidProp(#[from] interop::error::InvalidProp),
        #[error(transparent)]
        InvalidImport(#[from] interop::error::ImportValue),
        #[error(transparent)]
        InvalidValue(#[from] interop::error::InvalidValue),
        #[error(transparent)]
        InvalidDatatype(#[from] crate::value::InvalidDatatype),
    }

    impl From<Insert> for JsValue {
        fn from(e: Insert) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Block {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error("block name must be a string")]
        InvalidName,
        #[error("block parents must be an array of strings")]
        InvalidParents,
        #[error("invalid cursor")]
        InvalidCursor,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
    }

    impl From<Block> for JsValue {
        fn from(e: Block) -> Self {
            JsValue::from(e.to_string())
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum InsertObject {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error("the value to insert must be an object")]
        ValueNotObject,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        InvalidProp(#[from] interop::error::InvalidProp),
        #[error(transparent)]
        InvalidValue(#[from] interop::error::InvalidValue),
    }

    impl From<InsertObject> for JsValue {
        fn from(e: InsertObject) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Increment {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        InvalidProp(#[from] interop::error::InvalidProp),
        #[error("value was not numeric")]
        ValueNotNumeric,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
    }

    impl From<Increment> for JsValue {
        fn from(e: Increment) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum BadSyncMessage {
        #[error("could not decode sync message: {0}")]
        ReadMessage(#[from] automerge::sync::ReadMessageError),
    }

    impl From<BadSyncMessage> for JsValue {
        fn from(e: BadSyncMessage) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ApplyPatch {
        #[error(transparent)]
        Interop(#[from] interop::error::ApplyPatch),
        #[error(transparent)]
        Export(#[from] interop::error::Export),
        #[error("patch was not an object")]
        NotObjectd,
        #[error("error calling patch callback: {0:?}")]
        PatchCallback(JsValue),
    }

    impl From<ApplyPatch> for JsValue {
        fn from(e: ApplyPatch) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("unable to build patches: {0}")]
    pub struct PopPatches(#[from] interop::error::Export);

    impl From<PopPatches> for JsValue {
        fn from(e: PopPatches) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Diff {
        #[error(transparent)]
        Import(#[from] interop::error::ImportObj),
        #[error(transparent)]
        Export(#[from] interop::error::Export),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error("invalid before heads: {0}")]
        InvalidBeforeHeads(interop::error::BadChangeHashes),
        #[error("before heads were null or undefined")]
        MissingBeforeHeads,
        #[error("invalid after heads: {0}")]
        InvalidAfterHeads(interop::error::BadChangeHashes),
        #[error("after heads were null or undefined")]
        MissingAfterHeads,
    }

    impl From<Diff> for JsValue {
        fn from(e: Diff) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Isolate {
        #[error("bad heads: {0}")]
        Heads(#[from] interop::error::BadChangeHashes),
        #[error("no heads specified")]
        NoHeads,
    }

    impl From<Isolate> for JsValue {
        fn from(e: Isolate) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Materialize {
        #[error(transparent)]
        Export(#[from] interop::error::Export),
        #[error("bad heads: {0}")]
        Heads(#[from] interop::error::BadChangeHashes),
    }

    impl From<Materialize> for JsValue {
        fn from(e: Materialize) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Cursor {
        //#[error(transparent)]
        //Export(#[from] interop::error::Export),
        #[error("invalid cursor")]
        InvalidCursor,
        #[error("invalid position - must be an index, 'start' or 'end'")]
        InvalidCursorPosition,
        #[error("invalid move - must be 'before' or 'after'")]
        InvalidMoveCursor,
        #[error("cursors only valid on text - obj type: {0}")]
        InvalidObjType(ObjType),
        #[error("bad heads: {0}")]
        Heads(#[from] interop::error::BadChangeHashes),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
    }

    impl From<Cursor> for JsValue {
        fn from(e: Cursor) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ReceiveSyncMessage {
        #[error(transparent)]
        Decode(#[from] automerge::sync::ReadMessageError),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
    }

    impl From<ReceiveSyncMessage> for JsValue {
        fn from(e: ReceiveSyncMessage) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Load {
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        BadActor(#[from] BadActorId),
    }

    impl From<Load> for JsValue {
        fn from(e: Load) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("Unable to read JS change: {0}")]
    pub struct EncodeChange(#[from] serde_json::Error);

    impl From<EncodeChange> for JsValue {
        fn from(e: EncodeChange) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum DecodeChange {
        #[error(transparent)]
        Load(#[from] automerge::LoadChangeError),
        #[error(transparent)]
        Serialize(#[from] serde_wasm_bindgen::Error),
    }

    impl From<DecodeChange> for JsValue {
        fn from(e: DecodeChange) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum Mark {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        Expand(#[from] interop::error::BadExpand),
        #[error("Invalid mark name")]
        InvalidName,
        #[error("Invalid mark value: {0}")]
        ImportValue(#[from] interop::error::ImportValue),
        #[error("start must be a number")]
        InvalidStart,
        #[error("end must be a number")]
        InvalidEnd,
        #[error("range must be an object")]
        InvalidRange,
        #[error(transparent)]
        InvalidDatatype(#[from] crate::value::InvalidDatatype),
    }

    impl From<Mark> for JsValue {
        fn from(e: Mark) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum SplitBlock {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error("the block value must be a map")]
        InvalidArgs,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        UpdateObject(#[from] automerge::error::UpdateObjectError),
    }

    impl From<SplitBlock> for JsValue {
        fn from(e: SplitBlock) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum UpdateBlock {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error("the updated block args must be a map")]
        InvalidArgs,
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        Update(#[from] automerge::error::UpdateObjectError),
        #[error("invalid value")]
        InvalidValue(#[from] interop::error::JsValToHydrate),
    }

    impl From<UpdateBlock> for JsValue {
        fn from(e: UpdateBlock) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum GetBlock {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        Set(#[from] interop::error::SetProp),
        #[error(transparent)]
        Export(#[from] interop::error::Export),
        #[error(transparent)]
        BadHeads(#[from] interop::error::BadChangeHashes),
    }

    impl From<GetBlock> for JsValue {
        fn from(e: GetBlock) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum GetSpans {
        #[error("invalid object id: {0}")]
        ImportObj(#[from] interop::error::ImportObj),
        #[error(transparent)]
        BadHeads(#[from] interop::error::BadChangeHashes),
        #[error(transparent)]
        Export(#[from] interop::error::Export),
        #[error(transparent)]
        Automerge(#[from] AutomergeError),
        #[error(transparent)]
        Set(#[from] interop::error::SetProp),
    }

    impl From<GetSpans> for JsValue {
        fn from(e: GetSpans) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum GetDecodedChangeByHash {
        #[error(transparent)]
        BadChangeHash(#[from] super::interop::error::BadChangeHash),
        #[error(transparent)]
        SerdeWasm(#[from] serde_wasm_bindgen::Error),
    }

    impl From<GetDecodedChangeByHash> for JsValue {
        fn from(e: GetDecodedChangeByHash) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    pub enum SaveBundle {
        #[error(transparent)]
        BadChangeHashes(#[from] interop::error::BadChangeHashes),
        #[error("error creating bundle: {0}")]
        DoBundle(automerge::AutomergeError),
    }

    impl From<SaveBundle> for JsValue {
        fn from(e: SaveBundle) -> Self {
            RangeError::new(&e.to_string()).into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("error parsing bundle: {}", 0)]
    pub struct ReadBundle(pub(super) String);

    impl From<ReadBundle> for JsValue {
        fn from(e: ReadBundle) -> Self {
            RangeError::new(&e.0).into()
        }
    }
}

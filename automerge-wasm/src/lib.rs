#![doc(
    html_logo_url = "https://raw.githubusercontent.com/automerge/automerge-rs/main/img/brandmark.svg",
    html_favicon_url = "https:///raw.githubusercontent.com/automerge/automerge-rs/main/img/favicon.ico"
)]
#![warn(
    missing_debug_implementations,
    // missing_docs, // TODO: add documentation!
    rust_2021_compatibility,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    const_err,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    private_in_public,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]
#![allow(clippy::unused_unit)]
use am::transaction::CommitOptions;
use am::transaction::Transactable;
use am::ApplyOptions;
use automerge as am;
use automerge::Patch;
use automerge::VecOpObserver;
use automerge::{Change, ObjId, Prop, Value, ROOT};
use js_sys::{Array, Object, Uint8Array};
use regex::Regex;
use std::convert::TryInto;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

mod interop;
mod sync;
mod value;

use interop::{
    get_heads, get_js_heads, js_get, js_set, list_to_js, list_to_js_at, map_to_js, map_to_js_at,
    to_js_err, to_objtype, to_prop, AR, JS,
};
use sync::SyncState;
use value::{datatype, ScalarValue};

#[allow(unused_macros)]
macro_rules! log {
    ( $( $t:tt )* ) => {
          web_sys::console::log_1(&format!( $( $t )* ).into());
    };
}

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
#[derive(Debug)]
pub struct Automerge {
    doc: automerge::AutoCommit,
    observer: Option<VecOpObserver>,
}

#[wasm_bindgen]
impl Automerge {
    pub fn new(actor: Option<String>) -> Result<Automerge, JsValue> {
        let mut automerge = automerge::AutoCommit::new();
        if let Some(a) = actor {
            let a = automerge::ActorId::from(hex::decode(a).map_err(to_js_err)?.to_vec());
            automerge.set_actor(a);
        }
        Ok(Automerge {
            doc: automerge,
            observer: None,
        })
    }

    fn ensure_transaction_closed(&mut self) {
        if self.doc.pending_ops() > 0 {
            let mut opts = CommitOptions::default();
            if let Some(observer) = self.observer.as_mut() {
                opts.set_op_observer(observer);
            }
            self.doc.commit_with(opts);
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&mut self, actor: Option<String>) -> Result<Automerge, JsValue> {
        self.ensure_transaction_closed();
        let mut automerge = Automerge {
            doc: self.doc.clone(),
            observer: None,
        };
        if let Some(s) = actor {
            let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
            automerge.doc.set_actor(actor);
        }
        Ok(automerge)
    }

    pub fn fork(&mut self, actor: Option<String>) -> Result<Automerge, JsValue> {
        self.ensure_transaction_closed();
        let mut automerge = Automerge {
            doc: self.doc.fork(),
            observer: None,
        };
        if let Some(s) = actor {
            let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
            automerge.doc.set_actor(actor);
        }
        Ok(automerge)
    }

    #[wasm_bindgen(js_name = forkAt)]
    pub fn fork_at(&mut self, heads: JsValue, actor: Option<String>) -> Result<Automerge, JsValue> {
        let deps: Vec<_> = JS(heads).try_into()?;
        let mut automerge = Automerge {
            doc: self.doc.fork_at(&deps)?,
            observer: None,
        };
        if let Some(s) = actor {
            let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
            automerge.doc.set_actor(actor);
        }
        Ok(automerge)
    }

    pub fn free(self) {}

    #[wasm_bindgen(js_name = pendingOps)]
    pub fn pending_ops(&self) -> JsValue {
        (self.doc.pending_ops() as u32).into()
    }

    pub fn commit(&mut self, message: Option<String>, time: Option<f64>) -> JsValue {
        let mut commit_opts = CommitOptions::default();
        if let Some(message) = message {
            commit_opts.set_message(message);
        }
        if let Some(time) = time {
            commit_opts.set_time(time as i64);
        }
        if let Some(observer) = self.observer.as_mut() {
            commit_opts.set_op_observer(observer);
        }
        let hash = self.doc.commit_with(commit_opts);
        JsValue::from_str(&hex::encode(&hash.0))
    }

    pub fn merge(&mut self, other: &mut Automerge) -> Result<Array, JsValue> {
        self.ensure_transaction_closed();
        let options = if let Some(observer) = self.observer.as_mut() {
            ApplyOptions::default().with_op_observer(observer)
        } else {
            ApplyOptions::default()
        };
        let objs = self.doc.merge_with(&mut other.doc, options)?;
        let objs: Array = objs.iter().map(|o| JsValue::from(o.to_string())).collect();
        Ok(objs)
    }

    pub fn rollback(&mut self) -> f64 {
        self.doc.rollback() as f64
    }

    pub fn keys(&self, obj: JsValue, heads: Option<Array>) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = if let Some(heads) = get_heads(heads) {
            self.doc
                .keys_at(&obj, &heads)
                .map(|s| JsValue::from_str(&s))
                .collect()
        } else {
            self.doc.keys(&obj).map(|s| JsValue::from_str(&s)).collect()
        };
        Ok(result)
    }

    pub fn text(&self, obj: JsValue, heads: Option<Array>) -> Result<String, JsValue> {
        let obj = self.import(obj)?;
        if let Some(heads) = get_heads(heads) {
            Ok(self.doc.text_at(&obj, &heads)?)
        } else {
            Ok(self.doc.text(&obj)?)
        }
    }

    pub fn splice(
        &mut self,
        obj: JsValue,
        start: f64,
        delete_count: f64,
        text: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let start = start as usize;
        let delete_count = delete_count as usize;
        let mut vals = vec![];
        if let Some(t) = text.as_string() {
            self.doc.splice_text(&obj, start, delete_count, &t)?;
        } else {
            if let Ok(array) = text.dyn_into::<Array>() {
                for i in array.iter() {
                    let value = self
                        .import_scalar(&i, &None)
                        .ok_or_else(|| to_js_err("expected scalar"))?;
                    vals.push(value);
                }
            }
            self.doc
                .splice(&obj, start, delete_count, vals.into_iter())?;
        }
        Ok(())
    }

    pub fn push(&mut self, obj: JsValue, value: JsValue, datatype: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("invalid scalar value"))?;
        let index = self.doc.length(&obj);
        self.doc.insert(&obj, index, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = pushObject)]
    pub fn push_object(&mut self, obj: JsValue, value: JsValue) -> Result<Option<String>, JsValue> {
        let obj = self.import(obj)?;
        let (value, subvals) =
            to_objtype(&value, &None).ok_or_else(|| to_js_err("expected object"))?;
        let index = self.doc.length(&obj);
        let opid = self.doc.insert_object(&obj, index, value)?;
        self.subset(&opid, subvals)?;
        Ok(opid.to_string().into())
    }

    pub fn insert(
        &mut self,
        obj: JsValue,
        index: f64,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let index = index as f64;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("expected scalar value"))?;
        self.doc.insert(&obj, index as usize, value)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = insertObject)]
    pub fn insert_object(
        &mut self,
        obj: JsValue,
        index: f64,
        value: JsValue,
    ) -> Result<Option<String>, JsValue> {
        let obj = self.import(obj)?;
        let index = index as f64;
        let (value, subvals) =
            to_objtype(&value, &None).ok_or_else(|| to_js_err("expected object"))?;
        let opid = self.doc.insert_object(&obj, index as usize, value)?;
        self.subset(&opid, subvals)?;
        Ok(opid.to_string().into())
    }

    pub fn put(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("expected scalar value"))?;
        self.doc.put(&obj, prop, value)?;
        Ok(())
    }

    pub fn make(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
        _datatype: JsValue,
    ) -> Result<JsValue, JsValue> {
        // remove this
        am::log!("doc.make() is depricated - please use doc.set_object() or doc.insert_object()");
        self.put_object(obj, prop, value)
    }

    #[wasm_bindgen(js_name = putObject)]
    pub fn put_object(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
    ) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let (value, subvals) =
            to_objtype(&value, &None).ok_or_else(|| to_js_err("expected object"))?;
        let opid = self.doc.put_object(&obj, prop, value)?;
        self.subset(&opid, subvals)?;
        Ok(opid.to_string().into())
    }

    fn subset(&mut self, obj: &am::ObjId, vals: Vec<(am::Prop, JsValue)>) -> Result<(), JsValue> {
        for (p, v) in vals {
            let (value, subvals) = self.import_value(&v, None)?;
            //let opid = self.doc.set(id, p, value)?;
            let opid = match (p, value) {
                (Prop::Map(s), Value::Object(objtype)) => {
                    Some(self.doc.put_object(obj, s, objtype)?)
                }
                (Prop::Map(s), Value::Scalar(scalar)) => {
                    self.doc.put(obj, s, scalar.into_owned())?;
                    None
                }
                (Prop::Seq(i), Value::Object(objtype)) => {
                    Some(self.doc.insert_object(obj, i, objtype)?)
                }
                (Prop::Seq(i), Value::Scalar(scalar)) => {
                    self.doc.insert(obj, i, scalar.into_owned())?;
                    None
                }
            };
            if let Some(opid) = opid {
                self.subset(&opid, subvals)?;
            }
        }
        Ok(())
    }

    pub fn increment(
        &mut self,
        obj: JsValue,
        prop: JsValue,
        value: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = self.import_prop(prop)?;
        let value: f64 = value
            .as_f64()
            .ok_or_else(|| to_js_err("increment needs a numeric value"))?;
        self.doc.increment(&obj, prop, value as i64)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = get)]
    pub fn get(
        &self,
        obj: JsValue,
        prop: JsValue,
        heads: Option<Array>,
    ) -> Result<Option<Array>, JsValue> {
        let obj = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(prop);
        let heads = get_heads(heads);
        if let Ok(prop) = prop {
            let value = if let Some(h) = heads {
                self.doc.get_at(&obj, prop, &h)?
            } else {
                self.doc.get(&obj, prop)?
            };
            match value {
                Some((Value::Object(obj_type), obj_id)) => {
                    result.push(&obj_type.to_string().into());
                    result.push(&obj_id.to_string().into());
                    Ok(Some(result))
                }
                Some((Value::Scalar(value), _)) => {
                    result.push(&datatype(&value).into());
                    result.push(&ScalarValue(value).into());
                    Ok(Some(result))
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    #[wasm_bindgen(js_name = getAll)]
    pub fn get_all(
        &self,
        obj: JsValue,
        arg: JsValue,
        heads: Option<Array>,
    ) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let result = Array::new();
        let prop = to_prop(arg);
        if let Ok(prop) = prop {
            let values = if let Some(heads) = get_heads(heads) {
                self.doc.get_all_at(&obj, prop, &heads)
            } else {
                self.doc.get_all(&obj, prop)
            }
            .map_err(to_js_err)?;
            for value in values {
                match value {
                    (Value::Object(obj_type), obj_id) => {
                        let sub = Array::new();
                        sub.push(&obj_type.to_string().into());
                        sub.push(&obj_id.to_string().into());
                        result.push(&sub.into());
                    }
                    (Value::Scalar(value), id) => {
                        let sub = Array::new();
                        sub.push(&datatype(&value).into());
                        sub.push(&ScalarValue(value).into());
                        sub.push(&id.to_string().into());
                        result.push(&sub.into());
                    }
                }
            }
        }
        Ok(result)
    }

    #[wasm_bindgen(js_name = enablePatches)]
    pub fn enable_patches(&mut self, enable: JsValue) -> Result<(), JsValue> {
        let enable = enable
            .as_bool()
            .ok_or_else(|| to_js_err("expected boolean"))?;
        if enable {
            if self.observer.is_none() {
                self.observer = Some(VecOpObserver::default());
            }
        } else {
            self.observer = None;
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = popPatches)]
    pub fn pop_patches(&mut self) -> Result<Array, JsValue> {
        // transactions send out observer updates as they occur, not waiting for them to be
        // committed.
        // If we pop the patches then we won't be able to revert them.
        self.ensure_transaction_closed();

        let patches = self
            .observer
            .as_mut()
            .map_or_else(Vec::new, |o| o.take_patches());
        let result = Array::new();
        for p in patches {
            let patch = Object::new();
            match p {
                Patch::Put {
                    obj,
                    key,
                    value,
                    conflict,
                } => {
                    js_set(&patch, "action", "put")?;
                    js_set(&patch, "obj", obj.to_string())?;
                    js_set(&patch, "key", key)?;
                    match value {
                        (Value::Object(obj_type), obj_id) => {
                            js_set(&patch, "datatype", obj_type.to_string())?;
                            js_set(&patch, "value", obj_id.to_string())?;
                        }
                        (Value::Scalar(value), _) => {
                            js_set(&patch, "datatype", datatype(&value))?;
                            js_set(&patch, "value", ScalarValue(value))?;
                        }
                    };
                    js_set(&patch, "conflict", conflict)?;
                }

                Patch::Insert { obj, index, value } => {
                    js_set(&patch, "action", "insert")?;
                    js_set(&patch, "obj", obj.to_string())?;
                    js_set(&patch, "key", index as f64)?;
                    match value {
                        (Value::Object(obj_type), obj_id) => {
                            js_set(&patch, "datatype", obj_type.to_string())?;
                            js_set(&patch, "value", obj_id.to_string())?;
                        }
                        (Value::Scalar(value), _) => {
                            js_set(&patch, "datatype", datatype(&value))?;
                            js_set(&patch, "value", ScalarValue(value))?;
                        }
                    };
                }

                Patch::Increment { obj, key, value } => {
                    js_set(&patch, "action", "increment")?;
                    js_set(&patch, "obj", obj.to_string())?;
                    js_set(&patch, "key", key)?;
                    js_set(&patch, "value", value.0)?;
                }

                Patch::Delete { obj, key } => {
                    js_set(&patch, "action", "delete")?;
                    js_set(&patch, "obj", obj.to_string())?;
                    js_set(&patch, "key", key)?;
                }
            }
            result.push(&patch);
        }
        Ok(result)
    }

    pub fn length(&self, obj: JsValue, heads: Option<Array>) -> Result<f64, JsValue> {
        let obj = self.import(obj)?;
        if let Some(heads) = get_heads(heads) {
            Ok(self.doc.length_at(&obj, &heads) as f64)
        } else {
            Ok(self.doc.length(&obj) as f64)
        }
    }

    pub fn delete(&mut self, obj: JsValue, prop: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let prop = to_prop(prop)?;
        self.doc.delete(&obj, prop).map_err(to_js_err)?;
        Ok(())
    }

    pub fn mark(
        &mut self,
        obj: JsValue,
        range: JsValue,
        name: JsValue,
        value: JsValue,
        datatype: JsValue,
    ) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let re = Regex::new(r"([\[\(])(\d+)\.\.(\d+)([\)\]])").unwrap();
        let range = range.as_string().ok_or("range must be a string")?;
        let cap = re.captures_iter(&range).next().ok_or("range must be in the form of (start..end] or [start..end) etc... () for sticky, [] for normal")?;
        let start: usize = cap[2].parse().map_err(|_| to_js_err("invalid start"))?;
        let end: usize = cap[3].parse().map_err(|_| to_js_err("invalid end"))?;
        let start_sticky = &cap[1] == "(";
        let end_sticky = &cap[4] == ")";
        let name = name
            .as_string()
            .ok_or("invalid mark name")
            .map_err(to_js_err)?;
        let value = self
            .import_scalar(&value, &datatype.as_string())
            .ok_or_else(|| to_js_err("invalid value"))?;
        self.doc
            .mark(&obj, start, start_sticky, end, end_sticky, &name, value)
            .map_err(to_js_err)?;
        Ok(())
    }

    pub fn unmark(&mut self, obj: JsValue, mark: JsValue) -> Result<(), JsValue> {
        let obj = self.import(obj)?;
        let mark = self.import(mark)?;
        self.doc.unmark(&obj, &mark).map_err(to_js_err)?;
        Ok(())
    }

    pub fn spans(&mut self, obj: JsValue) -> Result<JsValue, JsValue> {
        let obj = self.import(obj)?;
        let text: Vec<_> = self.doc.list_range(&obj, ..).collect();
        let spans = self.doc.spans(&obj).map_err(to_js_err)?;
        let mut last_pos = 0;
        let result = Array::new();
        for s in spans {
            let marks = Array::new();
            for m in s.marks {
                let mark = Array::new();
                mark.push(&m.0.into());
                mark.push(&datatype(&m.1).into());
                mark.push(&ScalarValue(m.1).into());
                marks.push(&mark.into());
            }
            let text_span = &text[last_pos..s.pos]; //.slice(last_pos, s.pos);
            if !text_span.is_empty() {
                let t: String = text_span
                    .iter()
                    .filter_map(|(_, v, _)| v.as_string())
                    .collect();
                result.push(&t.into());
            }
            result.push(&marks);
            last_pos = s.pos;
            //let obj = Object::new().into();
            //js_set(&obj, "pos", s.pos as i32)?;
            //js_set(&obj, "marks", marks)?;
            //result.push(&obj.into());
        }
        let text_span = &text[last_pos..];
        if !text_span.is_empty() {
            let t: String = text_span
                .iter()
                .filter_map(|(_, v, _)| v.as_string())
                .collect();
            result.push(&t.into());
        }
        Ok(result.into())
    }

    pub fn raw_spans(&mut self, obj: JsValue) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let spans = self.doc.raw_spans(&obj).map_err(to_js_err)?;
        let result = Array::new();
        for s in spans {
            result.push(&JsValue::from_serde(&s).map_err(to_js_err)?);
        }
        Ok(result)
    }

    pub fn blame(
        &mut self,
        obj: JsValue,
        baseline: JsValue,
        change_sets: JsValue,
    ) -> Result<Array, JsValue> {
        am::log!("doc.blame() is depricated - please use doc.attribute()");
        self.attribute(obj, baseline, change_sets)
    }

    pub fn attribute(
        &mut self,
        obj: JsValue,
        baseline: JsValue,
        change_sets: JsValue,
    ) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let baseline = get_js_heads(baseline)?;
        let change_sets = change_sets.dyn_into::<Array>()?;
        let change_sets = change_sets
            .iter()
            .map(get_js_heads)
            .collect::<Result<Vec<_>, _>>()?;
        let result = self.doc.attribute(&obj, &baseline, &change_sets)?;
        let result = result
            .into_iter()
            .map(|cs| {
                let add = cs
                    .add
                    .iter()
                    .map::<Result<JsValue, JsValue>, _>(|range| {
                        let r = Object::new();
                        js_set(&r, "start", range.start as f64)?;
                        js_set(&r, "end", range.end as f64)?;
                        Ok(JsValue::from(&r))
                    })
                    .collect::<Result<Vec<JsValue>, JsValue>>()?
                    .iter()
                    .collect::<Array>();
                let del = cs
                    .del
                    .iter()
                    .map::<Result<JsValue, JsValue>, _>(|d| {
                        let r = Object::new();
                        js_set(&r, "pos", d.0 as f64)?;
                        js_set(&r, "val", &d.1)?;
                        Ok(JsValue::from(&r))
                    })
                    .collect::<Result<Vec<JsValue>, JsValue>>()?
                    .iter()
                    .collect::<Array>();
                let obj = Object::new();
                js_set(&obj, "add", add)?;
                js_set(&obj, "del", del)?;
                Ok(obj.into())
            })
            .collect::<Result<Vec<JsValue>, JsValue>>()?
            .iter()
            .collect::<Array>();
        Ok(result)
    }

    pub fn attribute2(
        &mut self,
        obj: JsValue,
        baseline: JsValue,
        change_sets: JsValue,
    ) -> Result<Array, JsValue> {
        let obj = self.import(obj)?;
        let baseline = get_js_heads(baseline)?;
        let change_sets = change_sets.dyn_into::<Array>()?;
        let change_sets = change_sets
            .iter()
            .map(get_js_heads)
            .collect::<Result<Vec<_>, _>>()?;
        let result = self.doc.attribute2(&obj, &baseline, &change_sets)?;
        let result = result
            .into_iter()
            .map(|cs| {
                let add = cs
                    .add
                    .iter()
                    .map::<Result<JsValue, JsValue>, _>(|a| {
                        let r = Object::new();
                        js_set(&r, "actor", &self.doc.actor_to_str(a.actor))?;
                        js_set(&r, "start", a.range.start as f64)?;
                        js_set(&r, "end", a.range.end as f64)?;
                        Ok(JsValue::from(&r))
                    })
                    .collect::<Result<Vec<JsValue>, JsValue>>()?
                    .iter()
                    .collect::<Array>();
                let del = cs
                    .del
                    .iter()
                    .map::<Result<JsValue, JsValue>, _>(|d| {
                        let r = Object::new();
                        js_set(&r, "actor", &self.doc.actor_to_str(d.actor))?;
                        js_set(&r, "pos", d.pos as f64)?;
                        js_set(&r, "val", &d.span)?;
                        Ok(JsValue::from(&r))
                    })
                    .collect::<Result<Vec<JsValue>, JsValue>>()?
                    .iter()
                    .collect::<Array>();
                let obj = Object::new();
                js_set(&obj, "add", add)?;
                js_set(&obj, "del", del)?;
                Ok(obj.into())
            })
            .collect::<Result<Vec<JsValue>, JsValue>>()?
            .iter()
            .collect::<Array>();
        Ok(result)
    }

    pub fn save(&mut self) -> Uint8Array {
        self.ensure_transaction_closed();
        Uint8Array::from(self.doc.save().as_slice())
    }

    #[wasm_bindgen(js_name = saveIncremental)]
    pub fn save_incremental(&mut self) -> Uint8Array {
        self.ensure_transaction_closed();
        let bytes = self.doc.save_incremental();
        Uint8Array::from(bytes.as_slice())
    }

    #[wasm_bindgen(js_name = loadIncremental)]
    pub fn load_incremental(&mut self, data: Uint8Array) -> Result<f64, JsValue> {
        self.ensure_transaction_closed();
        let data = data.to_vec();
        let options = if let Some(observer) = self.observer.as_mut() {
            ApplyOptions::default().with_op_observer(observer)
        } else {
            ApplyOptions::default()
        };
        let len = self
            .doc
            .load_incremental_with(&data, options)
            .map_err(to_js_err)?;
        Ok(len as f64)
    }

    #[wasm_bindgen(js_name = applyChanges)]
    pub fn apply_changes(&mut self, changes: JsValue) -> Result<(), JsValue> {
        self.ensure_transaction_closed();
        let changes: Vec<_> = JS(changes).try_into()?;
        let options = if let Some(observer) = self.observer.as_mut() {
            ApplyOptions::default().with_op_observer(observer)
        } else {
            ApplyOptions::default()
        };
        self
            .doc
            .apply_changes_with(changes, options)
            .map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = getChanges)]
    pub fn get_changes(&mut self, have_deps: JsValue) -> Result<Array, JsValue> {
        self.ensure_transaction_closed();
        let deps: Vec<_> = JS(have_deps).try_into()?;
        let changes = self.doc.get_changes(&deps)?;
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getChangeByHash)]
    pub fn get_change_by_hash(&mut self, hash: JsValue) -> Result<JsValue, JsValue> {
        self.ensure_transaction_closed();
        let hash = hash.into_serde().map_err(to_js_err)?;
        let change = self.doc.get_change_by_hash(&hash);
        if let Some(c) = change {
            Ok(Uint8Array::from(c.raw_bytes()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = getChangesAdded)]
    pub fn get_changes_added(&mut self, other: &mut Automerge) -> Result<Array, JsValue> {
        self.ensure_transaction_closed();
        let changes = self.doc.get_changes_added(&mut other.doc);
        let changes: Array = changes
            .iter()
            .map(|c| Uint8Array::from(c.raw_bytes()))
            .collect();
        Ok(changes)
    }

    #[wasm_bindgen(js_name = getHeads)]
    pub fn get_heads(&mut self) -> Array {
        self.ensure_transaction_closed();
        let heads = self.doc.get_heads();
        let heads: Array = heads
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        heads
    }

    #[wasm_bindgen(js_name = getActorId)]
    pub fn get_actor_id(&self) -> String {
        let actor = self.doc.get_actor();
        actor.to_string()
    }

    #[wasm_bindgen(js_name = getLastLocalChange)]
    pub fn get_last_local_change(&mut self) -> Result<Uint8Array, JsValue> {
        self.ensure_transaction_closed();
        if let Some(change) = self.doc.get_last_local_change() {
            Ok(Uint8Array::from(change.raw_bytes()))
        } else {
            Err(to_js_err("no local changes"))
        }
    }

    pub fn dump(&mut self) {
        self.ensure_transaction_closed();
        self.doc.dump()
    }

    #[wasm_bindgen(js_name = getMissingDeps)]
    pub fn get_missing_deps(&mut self, heads: Option<Array>) -> Result<Array, JsValue> {
        self.ensure_transaction_closed();
        let heads = get_heads(heads).unwrap_or_default();
        let deps = self.doc.get_missing_deps(&heads);
        let deps: Array = deps
            .iter()
            .map(|h| JsValue::from_str(&hex::encode(&h.0)))
            .collect();
        Ok(deps)
    }

    #[wasm_bindgen(js_name = receiveSyncMessage)]
    pub fn receive_sync_message(
        &mut self,
        state: &mut SyncState,
        message: Uint8Array,
    ) -> Result<(), JsValue> {
        self.ensure_transaction_closed();
        let message = message.to_vec();
        let message = am::sync::Message::decode(message.as_slice()).map_err(to_js_err)?;
        let options = if let Some(observer) = self.observer.as_mut() {
            ApplyOptions::default().with_op_observer(observer)
        } else {
            ApplyOptions::default()
        };
        self
            .doc
            .receive_sync_message_with(&mut state.0, message, options)
            .map_err(to_js_err)?;
        Ok(())
    }

    #[wasm_bindgen(js_name = generateSyncMessage)]
    pub fn generate_sync_message(&mut self, state: &mut SyncState) -> Result<JsValue, JsValue> {
        self.ensure_transaction_closed();
        if let Some(message) = self.doc.generate_sync_message(&mut state.0) {
            Ok(Uint8Array::from(message.encode().as_slice()).into())
        } else {
            Ok(JsValue::null())
        }
    }

    #[wasm_bindgen(js_name = toJS)]
    pub fn to_js(&self) -> JsValue {
        map_to_js(&self.doc, &ROOT)
    }

    pub fn materialize(&self, obj: JsValue, heads: Option<Array>) -> Result<JsValue, JsValue> {
        let obj = self.import(obj).unwrap_or(ROOT);
        let heads = get_heads(heads);
        if let Some(heads) = heads {
            match self.doc.object_type(&obj) {
                Some(am::ObjType::Map) => Ok(map_to_js_at(&self.doc, &obj, heads.as_slice())),
                Some(am::ObjType::List) => Ok(list_to_js_at(&self.doc, &obj, heads.as_slice())),
                Some(am::ObjType::Text) => Ok(self.doc.text_at(&obj, heads.as_slice())?.into()),
                Some(am::ObjType::Table) => Ok(map_to_js_at(&self.doc, &obj, heads.as_slice())),
                None => Err(to_js_err(format!("invalid obj {}", obj))),
            }
        } else {
            match self.doc.object_type(&obj) {
                Some(am::ObjType::Map) => Ok(map_to_js(&self.doc, &obj)),
                Some(am::ObjType::List) => Ok(list_to_js(&self.doc, &obj)),
                Some(am::ObjType::Text) => Ok(self.doc.text(&obj)?.into()),
                Some(am::ObjType::Table) => Ok(map_to_js(&self.doc, &obj)),
                None => Err(to_js_err(format!("invalid obj {}", obj))),
            }
        }
    }

    fn import(&self, id: JsValue) -> Result<ObjId, JsValue> {
        if let Some(s) = id.as_string() {
            if let Some(post) = s.strip_prefix('/') {
                let mut obj = ROOT;
                let mut is_map = true;
                let parts = post.split('/');
                for prop in parts {
                    if prop.is_empty() {
                        break;
                    }
                    let val = if is_map {
                        self.doc.get(obj, prop)?
                    } else {
                        self.doc.get(obj, am::Prop::Seq(prop.parse().unwrap()))?
                    };
                    match val {
                        Some((am::Value::Object(am::ObjType::Map), id)) => {
                            is_map = true;
                            obj = id;
                        }
                        Some((am::Value::Object(am::ObjType::Table), id)) => {
                            is_map = true;
                            obj = id;
                        }
                        Some((am::Value::Object(_), id)) => {
                            is_map = false;
                            obj = id;
                        }
                        None => return Err(to_js_err(format!("invalid path '{}'", s))),
                        _ => return Err(to_js_err(format!("path '{}' is not an object", s))),
                    };
                }
                Ok(obj)
            } else {
                Ok(self.doc.import(&s)?)
            }
        } else {
            Err(to_js_err("invalid objid"))
        }
    }

    fn import_prop(&self, prop: JsValue) -> Result<Prop, JsValue> {
        if let Some(s) = prop.as_string() {
            Ok(s.into())
        } else if let Some(n) = prop.as_f64() {
            Ok((n as usize).into())
        } else {
            Err(to_js_err(format!("invalid prop {:?}", prop)))
        }
    }

    fn import_scalar(&self, value: &JsValue, datatype: &Option<String>) -> Option<am::ScalarValue> {
        match datatype.as_deref() {
            Some("boolean") => value.as_bool().map(am::ScalarValue::Boolean),
            Some("int") => value.as_f64().map(|v| am::ScalarValue::Int(v as i64)),
            Some("uint") => value.as_f64().map(|v| am::ScalarValue::Uint(v as u64)),
            Some("str") => value.as_string().map(|v| am::ScalarValue::Str(v.into())),
            Some("f64") => value.as_f64().map(am::ScalarValue::F64),
            Some("bytes") => Some(am::ScalarValue::Bytes(
                value.clone().dyn_into::<Uint8Array>().unwrap().to_vec(),
            )),
            Some("counter") => value.as_f64().map(|v| am::ScalarValue::counter(v as i64)),
            Some("timestamp") => {
                if let Some(v) = value.as_f64() {
                    Some(am::ScalarValue::Timestamp(v as i64))
                } else if let Ok(d) = value.clone().dyn_into::<js_sys::Date>() {
                    Some(am::ScalarValue::Timestamp(d.get_time() as i64))
                } else {
                    None
                }
            }
            Some("null") => Some(am::ScalarValue::Null),
            Some(_) => None,
            None => {
                if value.is_null() {
                    Some(am::ScalarValue::Null)
                } else if let Some(b) = value.as_bool() {
                    Some(am::ScalarValue::Boolean(b))
                } else if let Some(s) = value.as_string() {
                    Some(am::ScalarValue::Str(s.into()))
                } else if let Some(n) = value.as_f64() {
                    if (n.round() - n).abs() < f64::EPSILON {
                        Some(am::ScalarValue::Int(n as i64))
                    } else {
                        Some(am::ScalarValue::F64(n))
                    }
                } else if let Ok(d) = value.clone().dyn_into::<js_sys::Date>() {
                    Some(am::ScalarValue::Timestamp(d.get_time() as i64))
                } else if let Ok(o) = &value.clone().dyn_into::<Uint8Array>() {
                    Some(am::ScalarValue::Bytes(o.to_vec()))
                } else {
                    None
                }
            }
        }
    }

    fn import_value(
        &self,
        value: &JsValue,
        datatype: Option<String>,
    ) -> Result<(Value<'static>, Vec<(Prop, JsValue)>), JsValue> {
        match self.import_scalar(value, &datatype) {
            Some(val) => Ok((val.into(), vec![])),
            None => {
                if let Some((o, subvals)) = to_objtype(value, &datatype) {
                    Ok((o.into(), subvals))
                } else {
                    web_sys::console::log_2(&"Invalid value".into(), value);
                    Err(to_js_err("invalid value"))
                }
            }
        }
    }
}

#[wasm_bindgen(js_name = create)]
pub fn init(actor: Option<String>) -> Result<Automerge, JsValue> {
    console_error_panic_hook::set_once();
    Automerge::new(actor)
}

#[wasm_bindgen(js_name = loadDoc)]
pub fn load(data: Uint8Array, actor: Option<String>) -> Result<Automerge, JsValue> {
    let data = data.to_vec();
    let observer = None;
    let options = ApplyOptions::<()>::default();
    let mut automerge = am::AutoCommit::load_with(&data, options).map_err(to_js_err)?;
    if let Some(s) = actor {
        let actor = automerge::ActorId::from(hex::decode(s).map_err(to_js_err)?.to_vec());
        automerge.set_actor(actor);
    }
    Ok(Automerge {
        doc: automerge,
        observer,
    })
}

#[wasm_bindgen(js_name = encodeChange)]
pub fn encode_change(change: JsValue) -> Result<Uint8Array, JsValue> {
    let change: am::ExpandedChange = change.into_serde().map_err(to_js_err)?;
    let change: Change = change.into();
    Ok(Uint8Array::from(change.raw_bytes()))
}

#[wasm_bindgen(js_name = decodeChange)]
pub fn decode_change(change: Uint8Array) -> Result<JsValue, JsValue> {
    let change = Change::from_bytes(change.to_vec()).map_err(to_js_err)?;
    let change: am::ExpandedChange = change.decode();
    JsValue::from_serde(&change).map_err(to_js_err)
}

#[wasm_bindgen(js_name = initSyncState)]
pub fn init_sync_state() -> SyncState {
    SyncState(am::sync::State::new())
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = importSyncState)]
pub fn import_sync_state(state: JsValue) -> Result<SyncState, JsValue> {
    Ok(SyncState(JS(state).try_into()?))
}

// this is needed to be compatible with the automerge-js api
#[wasm_bindgen(js_name = exportSyncState)]
pub fn export_sync_state(state: SyncState) -> JsValue {
    JS::from(state.0).into()
}

#[wasm_bindgen(js_name = encodeSyncMessage)]
pub fn encode_sync_message(message: JsValue) -> Result<Uint8Array, JsValue> {
    let heads = js_get(&message, "heads")?.try_into()?;
    let need = js_get(&message, "need")?.try_into()?;
    let changes = js_get(&message, "changes")?.try_into()?;
    let have = js_get(&message, "have")?.try_into()?;
    Ok(Uint8Array::from(
        am::sync::Message {
            heads,
            need,
            have,
            changes,
        }
        .encode()
        .as_slice(),
    ))
}

#[wasm_bindgen(js_name = decodeSyncMessage)]
pub fn decode_sync_message(msg: Uint8Array) -> Result<JsValue, JsValue> {
    let data = msg.to_vec();
    let msg = am::sync::Message::decode(&data).map_err(to_js_err)?;
    let heads = AR::from(msg.heads.as_slice());
    let need = AR::from(msg.need.as_slice());
    let changes = AR::from(msg.changes.as_slice());
    let have = AR::from(msg.have.as_slice());
    let obj = Object::new().into();
    js_set(&obj, "heads", heads)?;
    js_set(&obj, "need", need)?;
    js_set(&obj, "have", have)?;
    js_set(&obj, "changes", changes)?;
    Ok(obj)
}

#[wasm_bindgen(js_name = encodeSyncState)]
pub fn encode_sync_state(state: SyncState) -> Result<Uint8Array, JsValue> {
    let state = state.0;
    Ok(Uint8Array::from(state.encode().as_slice()))
}

#[wasm_bindgen(js_name = decodeSyncState)]
pub fn decode_sync_state(data: Uint8Array) -> Result<SyncState, JsValue> {
    SyncState::decode(data)
}

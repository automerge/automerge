#![allow(dead_code)]

use crate::interop::{self, alloc, js_set};
use automerge::{Automerge, ObjId, OpObserver, Prop, SequenceTree, Value};
use js_sys::{Array, Object};
use wasm_bindgen::prelude::*;

#[derive(Debug, Clone, Default)]
pub(crate) struct Observer {
    enabled: bool,
    patches: Vec<Patch>,
}

impl Observer {
    pub(crate) fn take_patches(&mut self) -> Vec<Patch> {
        std::mem::take(&mut self.patches)
    }
    pub(crate) fn enable(&mut self, enable: bool) -> bool {
        if self.enabled && !enable {
            self.patches.truncate(0)
        }
        let old_enabled = self.enabled;
        self.enabled = enable;
        old_enabled
    }

    fn get_path(&mut self, doc: &Automerge, obj: &ObjId) -> Option<Vec<(ObjId, Prop)>> {
        match doc.parents(obj) {
            Ok(mut parents) => parents.visible_path(),
            Err(e) => {
                automerge::log!("error generating patch : {:?}", e);
                None
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Patch {
    PutMap {
        obj: ObjId,
        path: Vec<(ObjId, Prop)>,
        key: String,
        value: (Value<'static>, ObjId),
        expose: bool,
    },
    PutSeq {
        obj: ObjId,
        path: Vec<(ObjId, Prop)>,
        index: usize,
        value: (Value<'static>, ObjId),
        expose: bool,
    },
    Insert {
        obj: ObjId,
        path: Vec<(ObjId, Prop)>,
        index: usize,
        values: SequenceTree<(Value<'static>, ObjId)>,
    },
    SpliceText {
        obj: ObjId,
        path: Vec<(ObjId, Prop)>,
        index: usize,
        value: SequenceTree<u16>,
    },
    Increment {
        obj: ObjId,
        path: Vec<(ObjId, Prop)>,
        prop: Prop,
        value: i64,
    },
    DeleteMap {
        obj: ObjId,
        path: Vec<(ObjId, Prop)>,
        key: String,
    },
    DeleteSeq {
        obj: ObjId,
        path: Vec<(ObjId, Prop)>,
        index: usize,
        length: usize,
    },
}

impl OpObserver for Observer {
    fn insert(
        &mut self,
        doc: &Automerge,
        obj: ObjId,
        index: usize,
        tagged_value: (Value<'_>, ObjId),
    ) {
        if self.enabled {
            let value = (tagged_value.0.to_owned(), tagged_value.1);
            if let Some(Patch::Insert {
                obj: tail_obj,
                index: tail_index,
                values,
                ..
            }) = self.patches.last_mut()
            {
                let range = *tail_index..=*tail_index + values.len();
                if tail_obj == &obj && range.contains(&index) {
                    values.insert(index - *tail_index, value);
                    return;
                }
            }
            if let Some(path) = self.get_path(doc, &obj) {
                let mut values = SequenceTree::new();
                values.push(value);
                let patch = Patch::Insert {
                    path,
                    obj,
                    index,
                    values,
                };
                self.patches.push(patch);
            }
        }
    }

    fn splice_text(&mut self, doc: &Automerge, obj: ObjId, index: usize, value: &str) {
        if self.enabled {
            if let Some(Patch::SpliceText {
                obj: tail_obj,
                index: tail_index,
                value: prev_value,
                ..
            }) = self.patches.last_mut()
            {
                let range = *tail_index..=*tail_index + prev_value.len();
                if tail_obj == &obj && range.contains(&index) {
                    let i = index - *tail_index;
                    for (n, ch) in value.encode_utf16().enumerate() {
                        prev_value.insert(i + n, ch)
                    }
                    return;
                }
            }
            if let Some(path) = self.get_path(doc, &obj) {
                let mut v = SequenceTree::new();
                for ch in value.encode_utf16() {
                    v.push(ch)
                }
                let patch = Patch::SpliceText {
                    path,
                    obj,
                    index,
                    value: v,
                };
                self.patches.push(patch);
            }
        }
    }

    fn delete_seq(&mut self, doc: &Automerge, obj: ObjId, index: usize, length: usize) {
        if self.enabled {
            match self.patches.last_mut() {
                Some(Patch::SpliceText {
                    obj: tail_obj,
                    index: tail_index,
                    value,
                    ..
                }) => {
                    let range = *tail_index..*tail_index + value.len();
                    if tail_obj == &obj
                        && range.contains(&index)
                        && range.contains(&(index + length - 1))
                    {
                        for _ in 0..length {
                            value.remove(index - *tail_index);
                        }
                        return;
                    }
                }
                Some(Patch::Insert {
                    obj: tail_obj,
                    index: tail_index,
                    values,
                    ..
                }) => {
                    let range = *tail_index..*tail_index + values.len();
                    if tail_obj == &obj
                        && range.contains(&index)
                        && range.contains(&(index + length - 1))
                    {
                        for _ in 0..length {
                            values.remove(index - *tail_index);
                        }
                        return;
                    }
                }
                Some(Patch::DeleteSeq {
                    obj: tail_obj,
                    index: tail_index,
                    length: tail_length,
                    ..
                }) => {
                    if tail_obj == &obj && index == *tail_index {
                        *tail_length += length;
                        return;
                    }
                }
                _ => {}
            }
            if let Some(path) = self.get_path(doc, &obj) {
                let patch = Patch::DeleteSeq {
                    path,
                    obj,
                    index,
                    length,
                };
                self.patches.push(patch)
            }
        }
    }

    fn delete_map(&mut self, doc: &Automerge, obj: ObjId, key: &str) {
        if self.enabled {
            if let Some(path) = self.get_path(doc, &obj) {
                let patch = Patch::DeleteMap {
                    path,
                    obj,
                    key: key.to_owned(),
                };
                self.patches.push(patch)
            }
        }
    }

    fn put(
        &mut self,
        doc: &Automerge,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        _conflict: bool,
    ) {
        if self.enabled {
            let expose = false;
            if let Some(path) = self.get_path(doc, &obj) {
                let value = (tagged_value.0.to_owned(), tagged_value.1);
                let patch = match prop {
                    Prop::Map(key) => Patch::PutMap {
                        path,
                        obj,
                        key,
                        value,
                        expose,
                    },
                    Prop::Seq(index) => Patch::PutSeq {
                        path,
                        obj,
                        index,
                        value,
                        expose,
                    },
                };
                self.patches.push(patch);
            }
        }
    }

    fn expose(
        &mut self,
        doc: &Automerge,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        _conflict: bool,
    ) {
        if self.enabled {
            let expose = true;
            if let Some(path) = self.get_path(doc, &obj) {
                let value = (tagged_value.0.to_owned(), tagged_value.1);
                let patch = match prop {
                    Prop::Map(key) => Patch::PutMap {
                        path,
                        obj,
                        key,
                        value,
                        expose,
                    },
                    Prop::Seq(index) => Patch::PutSeq {
                        path,
                        obj,
                        index,
                        value,
                        expose,
                    },
                };
                self.patches.push(patch);
            }
        }
    }

    fn increment(&mut self, doc: &Automerge, obj: ObjId, prop: Prop, tagged_value: (i64, ObjId)) {
        if self.enabled {
            if let Some(path) = self.get_path(doc, &obj) {
                let value = tagged_value.0;
                self.patches.push(Patch::Increment {
                    path,
                    obj,
                    prop,
                    value,
                })
            }
        }
    }

    fn merge(&mut self, other: &Self) {
        self.patches.extend_from_slice(other.patches.as_slice())
    }

    fn branch(&self) -> Self {
        Observer {
            patches: vec![],
            enabled: self.enabled,
        }
    }
}

fn prop_to_js(p: &Prop) -> JsValue {
    match p {
        Prop::Map(key) => JsValue::from_str(key),
        Prop::Seq(index) => JsValue::from_f64(*index as f64),
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

impl Patch {
    pub(crate) fn path(&self) -> &[(ObjId, Prop)] {
        match &self {
            Self::PutMap { path, .. } => path.as_slice(),
            Self::PutSeq { path, .. } => path.as_slice(),
            Self::Increment { path, .. } => path.as_slice(),
            Self::Insert { path, .. } => path.as_slice(),
            Self::SpliceText { path, .. } => path.as_slice(),
            Self::DeleteMap { path, .. } => path.as_slice(),
            Self::DeleteSeq { path, .. } => path.as_slice(),
        }
    }

    pub(crate) fn obj(&self) -> &ObjId {
        match &self {
            Self::PutMap { obj, .. } => obj,
            Self::PutSeq { obj, .. } => obj,
            Self::Increment { obj, .. } => obj,
            Self::Insert { obj, .. } => obj,
            Self::SpliceText { obj, .. } => obj,
            Self::DeleteMap { obj, .. } => obj,
            Self::DeleteSeq { obj, .. } => obj,
        }
    }
}

impl TryFrom<Patch> for JsValue {
    type Error = interop::error::Export;

    fn try_from(p: Patch) -> Result<Self, Self::Error> {
        let result = Object::new();
        match p {
            Patch::PutMap {
                path, key, value, ..
            } => {
                js_set(&result, "action", "put")?;
                js_set(
                    &result,
                    "path",
                    export_path(path.as_slice(), &Prop::Map(key)),
                )?;
                js_set(&result, "value", alloc(&value.0).1)?;
                Ok(result.into())
            }
            Patch::PutSeq {
                path, index, value, ..
            } => {
                js_set(&result, "action", "put")?;
                js_set(
                    &result,
                    "path",
                    export_path(path.as_slice(), &Prop::Seq(index)),
                )?;
                js_set(&result, "value", alloc(&value.0).1)?;
                Ok(result.into())
            }
            Patch::Insert {
                path,
                index,
                values,
                ..
            } => {
                js_set(&result, "action", "insert")?;
                js_set(
                    &result,
                    "path",
                    export_path(path.as_slice(), &Prop::Seq(index)),
                )?;
                js_set(
                    &result,
                    "values",
                    values.iter().map(|v| alloc(&v.0).1).collect::<Array>(),
                )?;
                Ok(result.into())
            }
            Patch::SpliceText {
                path, index, value, ..
            } => {
                js_set(&result, "action", "splice")?;
                js_set(
                    &result,
                    "path",
                    export_path(path.as_slice(), &Prop::Seq(index)),
                )?;
                let bytes: Vec<u16> = value.iter().cloned().collect();
                js_set(&result, "value", String::from_utf16_lossy(bytes.as_slice()))?;
                Ok(result.into())
            }
            Patch::Increment {
                path, prop, value, ..
            } => {
                js_set(&result, "action", "inc")?;
                js_set(&result, "path", export_path(path.as_slice(), &prop))?;
                js_set(&result, "value", &JsValue::from_f64(value as f64))?;
                Ok(result.into())
            }
            Patch::DeleteMap { path, key, .. } => {
                js_set(&result, "action", "del")?;
                js_set(
                    &result,
                    "path",
                    export_path(path.as_slice(), &Prop::Map(key)),
                )?;
                Ok(result.into())
            }
            Patch::DeleteSeq {
                path,
                index,
                length,
                ..
            } => {
                js_set(&result, "action", "del")?;
                js_set(
                    &result,
                    "path",
                    export_path(path.as_slice(), &Prop::Seq(index)),
                )?;
                if length > 1 {
                    js_set(&result, "length", length)?;
                }
                Ok(result.into())
            }
        }
    }
}

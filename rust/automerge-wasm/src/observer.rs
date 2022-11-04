#![allow(dead_code)]

use crate::interop::{alloc, js_set};
use automerge::{ObjId, OpObserver, Parents, Prop, SequenceTree, Value};
use js_sys::{Array, Object};
use ropey::Rope;
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
        value: Rope,
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
        mut parents: Parents<'_>,
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
                //if tail_obj == &obj && *tail_index + values.len() == index {
                if tail_obj == &obj && range.contains(&index) {
                    values.insert(index - *tail_index, value);
                    return;
                }
            }
            if let Some(path) = parents.visible_path() {
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

    fn splice_text(&mut self, mut parents: Parents<'_>, obj: ObjId, index: usize, value: &str) {
        if self.enabled {
            if let Some(Patch::SpliceText {
                obj: tail_obj,
                index: tail_index,
                value: prev_value,
                ..
            }) = self.patches.last_mut()
            {
                let range = *tail_index..=*tail_index + prev_value.len_chars();
                if tail_obj == &obj && range.contains(&index) {
                    prev_value.insert(index - *tail_index, value);
                    return;
                }
            }
            if let Some(path) = parents.visible_path() {
                let patch = Patch::SpliceText {
                    path,
                    obj,
                    index,
                    value: Rope::from_str(value),
                };
                self.patches.push(patch);
            }
        }
    }

    fn delete(&mut self, mut parents: Parents<'_>, obj: ObjId, prop: Prop) {
        if self.enabled {
            match self.patches.last_mut() {
                Some(Patch::Insert {
                    obj: tail_obj,
                    index: tail_index,
                    values,
                    ..
                }) => {
                    if let Prop::Seq(index) = prop {
                        let range = *tail_index..*tail_index + values.len();
                        if tail_obj == &obj && range.contains(&index) {
                            values.remove(index - *tail_index);
                            return;
                        }
                    }
                }
                Some(Patch::SpliceText {
                    obj: tail_obj,
                    index: tail_index,
                    value,
                    ..
                }) => {
                    if let Prop::Seq(index) = prop {
                        let range = *tail_index..*tail_index + value.len_chars();
                        if tail_obj == &obj && range.contains(&index) {
                            let start = index - *tail_index;
                            let end = start + 1;
                            value.remove(start..end);
                            return;
                        }
                    }
                }
                _ => {}
            }
            if let Some(path) = parents.visible_path() {
                let patch = match prop {
                    Prop::Map(key) => Patch::DeleteMap { path, obj, key },
                    Prop::Seq(index) => Patch::DeleteSeq {
                        path,
                        obj,
                        index,
                        length: 1,
                    },
                };
                self.patches.push(patch)
            }
        }
    }

    fn put(
        &mut self,
        mut parents: Parents<'_>,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        _conflict: bool,
    ) {
        if self.enabled {
            let expose = false;
            if let Some(path) = parents.visible_path() {
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
        mut parents: Parents<'_>,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        _conflict: bool,
    ) {
        if self.enabled {
            let expose = true;
            if let Some(path) = parents.visible_path() {
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

    fn flag_conflict(&mut self, mut _parents: Parents<'_>, _obj: ObjId, _prop: Prop) {}

    fn increment(
        &mut self,
        mut parents: Parents<'_>,
        obj: ObjId,
        prop: Prop,
        tagged_value: (i64, ObjId),
    ) {
        if self.enabled {
            if let Some(path) = parents.visible_path() {
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
    type Error = JsValue;

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
                js_set(&result, "value", value.to_string())?;
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

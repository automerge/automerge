#![allow(dead_code)]

use crate::interop::{export_value, js_set};
use automerge::{ObjId, OpObserver, Parents, Prop, Value};
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
    pub(crate) fn enable(&mut self, enable: bool) {
        if self.enabled && !enable {
            self.patches.truncate(0)
        }
        self.enabled = enable;
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Patch {
    Put {
        obj: ObjId,
        path: Vec<Prop>,
        prop: Prop,
        value: Value<'static>,
        conflict: bool,
    },
    Insert {
        obj: ObjId,
        path: Vec<Prop>,
        index: usize,
        value: Value<'static>,
    },
    Increment {
        obj: ObjId,
        path: Vec<Prop>,
        prop: Prop,
        value: i64,
    },
    Delete {
        obj: ObjId,
        path: Vec<Prop>,
        prop: Prop,
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
            let path = parents.path().into_iter().map(|p| p.1).collect();
            let value = tagged_value.0.to_owned();
            self.patches.push(Patch::Insert {
                path,
                obj,
                index,
                value,
            })
        }
    }

    fn put(
        &mut self,
        mut parents: Parents<'_>,
        obj: ObjId,
        prop: Prop,
        tagged_value: (Value<'_>, ObjId),
        conflict: bool,
    ) {
        if self.enabled {
            let path = parents.path().into_iter().map(|p| p.1).collect();
            let value = tagged_value.0.to_owned();
            self.patches.push(Patch::Put {
                path,
                obj,
                prop,
                value,
                conflict,
            })
        }
    }

    fn increment(
        &mut self,
        mut parents: Parents<'_>,
        obj: ObjId,
        prop: Prop,
        tagged_value: (i64, ObjId),
    ) {
        if self.enabled {
            let path = parents.path().into_iter().map(|p| p.1).collect();
            let value = tagged_value.0;
            self.patches.push(Patch::Increment {
                path,
                obj,
                prop,
                value,
            })
        }
    }

    fn delete(&mut self, mut parents: Parents<'_>, obj: ObjId, prop: Prop) {
        if self.enabled {
            let path = parents.path().into_iter().map(|p| p.1).collect();
            self.patches.push(Patch::Delete { path, obj, prop })
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

fn export_path(path: &[Prop], end: &Prop) -> Array {
    let result = Array::new();
    for p in path {
        result.push(&prop_to_js(p));
    }
    result.push(&prop_to_js(end));
    result
}

impl TryFrom<Patch> for JsValue {
    type Error = JsValue;

    fn try_from(p: Patch) -> Result<Self, Self::Error> {
        let result = Object::new();
        match p {
            Patch::Put {
                path,
                prop,
                value,
                conflict,
                ..
            } => {
                js_set(&result, "action", "put")?;
                js_set(&result, "path", export_path(path.as_slice(), &prop))?;
                js_set(&result, "value", export_value(&value))?;
                js_set(&result, "conflict", &JsValue::from_bool(conflict))?;
                Ok(result.into())
            }
            Patch::Insert {
                path, index, value, ..
            } => {
                js_set(&result, "action", "ins")?;
                js_set(
                    &result,
                    "path",
                    export_path(path.as_slice(), &Prop::Seq(index)),
                )?;
                js_set(&result, "value", export_value(&value))?;
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
            Patch::Delete { path, prop, .. } => {
                js_set(&result, "action", "del")?;
                js_set(&result, "path", export_path(path.as_slice(), &prop))?;
                Ok(result.into())
            }
        }
    }
}

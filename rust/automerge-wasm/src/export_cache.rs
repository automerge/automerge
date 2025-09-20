use crate::interop::error;
use crate::interop::{ExternalTypeConstructor, SAFE_INT, SAFE_UINT};
use crate::value::Datatype;
use crate::Automerge;
use automerge as am;
use automerge::ChangeHash;
use js_sys::{Array, BigInt, JsString, Object, Reflect, Symbol, Uint8Array};
use rustc_hash::FxBuildHasher;
use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use am::{ObjId, ObjType, ReadDoc};

use am::iter::{DocItem, DocObjItem};

const RAW_DATA_SYMBOL: &str = "_am_raw_value_";
const DATATYPE_SYMBOL: &str = "_am_datatype_";
const RAW_OBJECT_SYMBOL: &str = "_am_objectId";
const META_SYMBOL: &str = "_am_meta";

#[derive(Debug, Clone)]
pub(crate) struct ExportCache<'a> {
    pub(crate) objs: HashMap<ObjId, CachedObject, FxBuildHasher>,
    obj_cache: HashMap<ObjId, (Object, JsValue)>,
    to_freeze: Vec<Object>,
    datatypes: HashMap<Datatype, JsString, FxBuildHasher>,
    keys: HashMap<Cow<'a, str>, JsString>,
    definition: Object,
    value_key: JsValue,
    meta_sym: Symbol,
    datatype_sym: Symbol,
    raw_obj_sym: Symbol,
    raw_data_sym: Symbol,
    doc: &'a Automerge,
}

impl<'a> ExportCache<'a> {
    pub(crate) fn new(doc: &'a Automerge) -> Result<Self, error::Export> {
        let definition = Object::new();
        let f = false.into();
        Reflect::set(&definition, &"writable".into(), &f)
            .map_err(|_| error::Export::SetHidden("writable"))?;
        Reflect::set(&definition, &"enumerable".into(), &f)
            .map_err(|_| error::Export::SetHidden("enumerable"))?;
        Reflect::set(&definition, &"configurable".into(), &f)
            .map_err(|_| error::Export::SetHidden("configurable"))?;
        let raw_obj_sym = Symbol::for_(RAW_OBJECT_SYMBOL);
        let datatype_sym = Symbol::for_(DATATYPE_SYMBOL);
        let meta_sym = Symbol::for_(META_SYMBOL);
        let raw_data_sym = Symbol::for_(RAW_DATA_SYMBOL);
        let value_key = "value".into();
        Ok(Self {
            objs: HashMap::default(),
            obj_cache: HashMap::default(),
            to_freeze: Vec::new(),
            datatypes: HashMap::default(),
            keys: HashMap::default(),
            definition,
            raw_obj_sym,
            datatype_sym,
            meta_sym,
            value_key,
            raw_data_sym,
            doc,
        })
    }

    fn make_value_ref(
        &mut self,
        parent: &Object,
        prop: &JsValue,
        obj: ObjId,
        value: am::ValueRef<'a>,
        meta: &JsValue,
    ) -> Result<JsValue, error::Export> {
        Ok(match value {
            am::ValueRef::Object(ObjType::Map) => self.make_map(obj, meta)?.into(),
            am::ValueRef::Object(ObjType::Text) => {
                self.obj_cache
                    .insert(obj.clone(), (parent.clone(), prop.clone()));
                JsValue::from_str("")
            }
            am::ValueRef::Scalar(s) => self.export_scalar_ref(&s)?,
            am::ValueRef::Object(obj_type) => self.make_list(obj, obj_type.into(), meta)?.into(),
        })
    }

    fn make_object(
        &mut self,
        obj: &ObjId,
        d: Datatype,
        meta: &JsValue,
    ) -> Result<Option<Object>, error::Export> {
        match d {
            Datatype::Map => Ok(Some(self.make_map(obj.clone(), meta)?)),
            Datatype::List => Ok(Some(self.make_list(obj.clone(), d, meta)?)),
            _ => Ok(None),
        }
    }

    fn make_list(
        &mut self,
        obj: ObjId,
        datatype: Datatype,
        meta: &JsValue,
    ) -> Result<Object, error::Export> {
        let child = Array::new();
        self.obj_cache
            .insert(obj.clone(), (child.clone().into(), JsValue::null()));
        self.wrap_object(child.into(), &obj, datatype, meta)
    }

    fn make_map(&mut self, obj: ObjId, meta: &JsValue) -> Result<Object, error::Export> {
        let child = Object::new();
        self.obj_cache
            .insert(obj.clone(), (child.clone(), JsValue::null()));
        self.wrap_object(child, &obj, Datatype::Map, meta)
    }

    #[inline(never)]
    pub(crate) fn materialize(
        &mut self,
        obj: ObjId,
        datatype: Datatype,
        heads: Option<&[ChangeHash]>,
        meta: &JsValue,
    ) -> Result<JsValue, error::Export> {
        if datatype == Datatype::Text {
            return Ok(self.doc.text_at(&obj, heads)?.into());
        }
        let mut current_obj_id = Arc::new(obj.clone());
        let mut o = self
            .make_object(&current_obj_id, datatype, meta)?
            .ok_or(error::Export::InvalidRoot)?;
        let mut index = 0;
        let mut buffer = String::new();
        let mut parent_prop = JsValue::null();
        let result = JsValue::from(&o);
        let iter = self.doc.doc.iter_at(obj, heads);
        for DocObjItem { obj, item } in iter {
            if obj != current_obj_id {
                if !buffer.is_empty() {
                    _set(&o, &parent_prop, &JsValue::from_str(&buffer))?;
                    buffer.truncate(0);
                }
                if let Some((new_o, new_p)) = self.obj_cache.get(&obj) {
                    o = new_o.clone();
                    parent_prop = new_p.clone();
                }
                current_obj_id = obj;
                index = 0;
            }
            match item {
                DocItem::Text(span) => {
                    buffer.push_str(span.as_str());
                }
                DocItem::Map(map) => {
                    let prop = self.ensure_key(map.key.clone());
                    let value = self.make_value_ref(&o, &prop, map.id(), map.value, meta)?;
                    _set(&o, &prop, &value)?;
                }
                DocItem::List(list) => {
                    let prop = JsValue::from_f64(index as f64);
                    let value = self.make_value_ref(&o, &prop, list.id(), list.value, meta)?;
                    _set(&o, &prop, &value)?;
                    index += 1;
                }
            }
        }
        if !buffer.is_empty() {
            _set(&o, &parent_prop, &JsValue::from_str(&buffer))?;
        }
        for o in &self.to_freeze {
            Object::freeze(o);
        }
        Ok(result)
    }

    #[inline(never)]
    fn export_scalar_ref(&self, value: &am::ScalarValueRef<'_>) -> Result<JsValue, error::Export> {
        let (datatype, js_value) = match value {
            am::ScalarValueRef::Bytes(v) => (Datatype::Bytes, Uint8Array::from(v.deref()).into()),
            am::ScalarValueRef::Str(v) => (Datatype::Str, v.to_string().into()),
            am::ScalarValueRef::Int(v) if SAFE_INT.contains(v) => {
                (Datatype::Int, (*v as f64).into())
            }
            am::ScalarValueRef::Int(v) => (Datatype::Int, BigInt::from(*v).into()),
            am::ScalarValueRef::Uint(v) if SAFE_UINT.contains(v) => {
                (Datatype::Uint, (*v as f64).into())
            }
            am::ScalarValueRef::Uint(v) => (Datatype::Uint, BigInt::from(*v).into()),
            am::ScalarValueRef::F64(v) => (Datatype::F64, (*v).into()),
            am::ScalarValueRef::Counter(v) if SAFE_INT.contains(v) => {
                (Datatype::Counter, (*v as f64).into())
            }
            am::ScalarValueRef::Counter(v) => (Datatype::Counter, BigInt::from(*v).into()),
            am::ScalarValueRef::Timestamp(v) => (
                Datatype::Timestamp,
                js_sys::Date::new(&(*v as f64).into()).into(),
            ),
            am::ScalarValueRef::Boolean(v) => (Datatype::Boolean, (*v).into()),
            am::ScalarValueRef::Null => (Datatype::Null, JsValue::null()),
            am::ScalarValueRef::Unknown { bytes, type_code } => (
                Datatype::Unknown(*type_code),
                Uint8Array::from(bytes.deref()).into(),
            ),
        };
        self.wrap_scalar(js_value, datatype)
    }

    #[inline(never)]
    fn wrap_scalar(&self, value: JsValue, datatype: Datatype) -> Result<JsValue, error::Export> {
        if let Some(constructor) = self.doc.external_types.get(&datatype) {
            let wrapped_value = constructor.construct(&value, datatype)?;
            let o = wrapped_value
                .dyn_into::<Object>()
                .map_err(|_| error::Export::InvalidDataHandler(datatype.to_string()))?;
            self.set_raw_data(&o, &value)?;
            self.set_datatype(&o, &datatype.into())?;
            Ok(o.into())
        } else {
            Ok(value)
        }
    }

    #[inline(never)]
    fn ensure_datatype(&mut self, datatype: Datatype) {
        self.datatypes
            .entry(datatype)
            .or_insert_with(|| JsString::from(datatype.to_string()));
    }

    #[inline(never)]
    fn ensure_key(&mut self, key: Cow<'a, str>) -> JsValue {
        if let Some(v) = self.keys.get(&key) {
            v.into()
        } else {
            let v = JsString::from(key.borrow());
            self.keys.insert(key, v.clone());
            v.into()
        }
    }

    #[inline(never)]
    fn wrap_object(
        &mut self,
        value: Object,
        obj: &ObjId,
        datatype: Datatype,
        meta: &JsValue,
    ) -> Result<Object, error::Export> {
        let value = if let Some(constructor) = self.doc.external_types.get(&datatype) {
            self.wrap_custom_object(&value, datatype, constructor)?
        } else {
            value
        };

        // I have to do this dance to make the borrow checker happy
        self.ensure_datatype(datatype);
        let js_datatype = self.datatypes.get(&datatype).unwrap(); // save - ensure above

        let js_objid = JsString::from(obj.to_string());

        self.set_hidden(&value, &js_objid, js_datatype, meta)?;

        if self.doc.freeze {
            self.to_freeze.push(value.clone());
        }

        Ok(value)
    }

    #[inline(never)]
    fn wrap_custom_object(
        &self,
        value: &Object,
        datatype: Datatype,
        constructor: &ExternalTypeConstructor,
    ) -> Result<Object, error::Export> {
        let wrapped_value = constructor.construct(value, datatype)?;
        let wrapped_object = wrapped_value
            .dyn_into::<Object>()
            .map_err(|_| error::Export::InvalidDataHandler(datatype.to_string()))?;
        self.set_raw_data(&wrapped_object, value)?;
        Ok(wrapped_object)
    }

    pub(crate) fn set_meta(&self, obj: &Object, value: &JsValue) -> Result<(), error::Export> {
        self.set_value(obj, &self.meta_sym, value)
    }

    pub(crate) fn get_raw_data(&self, obj: &JsValue) -> Result<JsValue, error::GetProp> {
        self.get_value(obj, &self.raw_data_sym)
    }

    pub(crate) fn set_raw_data(&self, obj: &Object, value: &JsValue) -> Result<(), error::Export> {
        self.set_value(obj, &self.raw_data_sym, value)
    }

    pub(crate) fn get_raw_object(&self, obj: &JsValue) -> Result<JsValue, error::GetProp> {
        self.get_value(obj, &self.raw_obj_sym)
    }

    pub(crate) fn set_raw_object(
        &self,
        obj: &Object,
        value: &JsValue,
    ) -> Result<(), error::Export> {
        self.set_value(obj, &self.raw_obj_sym, value)
    }

    pub(crate) fn get_datatype(&self, obj: &JsValue) -> Result<JsValue, error::GetProp> {
        self.get_value(obj, &self.datatype_sym)
    }

    pub(crate) fn set_datatype(&self, obj: &Object, value: &JsValue) -> Result<(), error::Export> {
        self.set_value(obj, &self.datatype_sym, value)
    }

    pub(crate) fn get_value(&self, obj: &JsValue, key: &Symbol) -> Result<JsValue, error::GetProp> {
        Reflect::get(obj, key).map_err(|error| error::GetProp {
            property: key.to_string().into(),
            error,
        })
    }

    pub(crate) fn set_value(
        &self,
        obj: &Object,
        key: &JsValue,
        value: &JsValue,
    ) -> Result<(), error::Export> {
        Reflect::set(&self.definition, &self.value_key, value)
            .map_err(|_| error::Export::SetHidden("value"))?;
        Object::define_property(obj, key, &self.definition);
        Ok(())
    }

    pub(crate) fn set_hidden(
        &self,
        obj: &Object,
        raw_obj: &JsValue,
        datatype: &JsValue,
        meta: &JsValue,
    ) -> Result<(), error::Export> {
        self.set_value(obj, &self.raw_obj_sym, raw_obj)?;
        self.set_value(obj, &self.datatype_sym, datatype)?;
        self.set_value(obj, &self.meta_sym, meta)
    }
}

fn _set(obj: &JsValue, property: &JsValue, value: &JsValue) -> Result<bool, error::Export> {
    Reflect::set(obj, property, value).map_err(error::Export::ReflectSet)
}

#[derive(Debug, Clone)]
pub(crate) struct CachedObject {
    pub(crate) id: ObjId,
    pub(crate) inner: Object,
    pub(crate) outer: Object,
}

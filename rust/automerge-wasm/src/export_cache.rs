use crate::interop::error;
use crate::interop::ExternalTypeConstructor;
use crate::value::Datatype;
use crate::Automerge;
use automerge as am;
use automerge::ChangeHash;
use fxhash::FxBuildHasher;
use js_sys::{Array, JsString, Object, Reflect, Symbol, Uint8Array};
use std::borrow::{Borrow, Cow};
use std::collections::BTreeSet;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use am::ObjId;

use am::iter::{ListRangeItem, MapRangeItem};

const RAW_DATA_SYMBOL: &str = "_am_raw_value_";
const DATATYPE_SYMBOL: &str = "_am_datatype_";
const RAW_OBJECT_SYMBOL: &str = "_am_objectId";
const META_SYMBOL: &str = "_am_meta";

#[derive(Debug, Clone)]
pub(crate) struct ExportCache<'a> {
    pub(crate) objs: HashMap<ObjId, CachedObject, FxBuildHasher>,
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

    #[inline(never)]
    fn zip_objects(
        &mut self,
        obj: ObjId,
        meta: &JsValue,
        stack: Vec<StackItem<'a>>,
        mut objects: HashMap<ObjId, JsValue>,
    ) -> Result<JsValue, error::Export> {
        for item in stack.into_iter().rev() {
            match item {
                StackItem::Map { obj, values } => {
                    let js_obj = Object::new();
                    for v in values {
                        let x = match &v.value {
                            am::ValueRef::Object(_) => {
                                objects.remove(&v.id).ok_or(error::Export::MissingChild)?
                            }
                            am::ValueRef::Scalar(s) => self.export_scalar_ref(s)?,
                        };
                        self.set_prop(&js_obj, v.key, &x)?;
                    }
                    let wrapped = self.wrap_object(js_obj, &obj, Datatype::Map, meta)?;
                    objects.insert(obj, JsValue::from(wrapped));
                }
                StackItem::Seq {
                    obj,
                    values,
                    datatype,
                } => {
                    let js_array: Result<Array, _> = values
                        .iter()
                        .map(|v| match &v.value {
                            am::Value::Object(_) => {
                                objects.remove(&v.id).ok_or(error::Export::MissingChild)
                            }
                            am::Value::Scalar(s) => self.export_scalar(s),
                        })
                        .collect();
                    let wrapped =
                        self.wrap_object(Object::from(js_array?), &obj, datatype, meta)?;
                    objects.insert(obj, JsValue::from(wrapped));
                }
            }
        }
        objects.remove(&obj).ok_or(error::Export::InvalidRoot)
    }

    #[inline(never)]
    pub(crate) fn materialize(
        &mut self,
        obj: ObjId,
        datatype: Datatype,
        heads: Option<&Vec<ChangeHash>>,
        meta: &JsValue,
    ) -> Result<JsValue, error::Export> {
        let mut to_do = BTreeSet::from([(obj.clone(), datatype)]);
        let mut stack = vec![];
        let mut objects = HashMap::new();
        while let Some((obj, datatype)) = to_do.pop_first() {
            match datatype {
                Datatype::Map => {
                    let mut values = vec![];
                    for v in self.doc.map_range_at(&obj, heads) {
                        if let am::ValueRef::Object(obj_type) = v.value {
                            to_do.insert((v.id.clone(), obj_type.into()));
                        }
                        values.push(v);
                    }
                    stack.push(StackItem::Map { obj, values });
                }
                Datatype::Text if self.doc.text_rep.is_string() => {
                    let text = self.doc.text_at(&obj, heads)?;
                    objects.insert(obj, JsValue::from(JsString::from(text)));
                }
                datatype if datatype.is_seq() => {
                    let mut values = vec![];
                    for v in self.doc.list_range_at(&obj, heads) {
                        if let am::Value::Object(obj_type) = v.value {
                            to_do.insert((v.id.clone(), obj_type.into()));
                        }
                        values.push(v);
                    }
                    stack.push(StackItem::Seq {
                        obj,
                        values,
                        datatype,
                    });
                }
                _ => {}
            }
        }
        self.zip_objects(obj, meta, stack, objects)
    }

    #[inline(never)]
    pub(crate) fn set_prop(
        &mut self,
        obj: &Object,
        key: Cow<'a, str>,
        value: &JsValue,
    ) -> Result<(), error::Export> {
        self.ensure_key(key.clone());
        let key = self.keys.get(&key).unwrap(); // save - ensure above
        Reflect::set(obj, key, value).map_err(|error| error::SetProp {
            property: JsValue::from(key),
            error,
        })?;
        Ok(())
    }

    fn export_scalar_ref(&self, value: &am::ScalarValueRef<'_>) -> Result<JsValue, error::Export> {
        let (datatype, js_value) = match value {
            am::ScalarValueRef::Bytes(v) => (Datatype::Bytes, Uint8Array::from(v.as_ref()).into()),
            am::ScalarValueRef::Str(v) => (Datatype::Str, v.to_string().into()),
            am::ScalarValueRef::Int(v) => (Datatype::Int, (*v as f64).into()),
            am::ScalarValueRef::Uint(v) => (Datatype::Uint, (*v as f64).into()),
            am::ScalarValueRef::F64(v) => (Datatype::F64, (*v).into()),
            am::ScalarValueRef::Counter(v) => (Datatype::Counter, (*v as f64).into()),
            am::ScalarValueRef::Timestamp(v) => (
                Datatype::Timestamp,
                js_sys::Date::new(&(*v as f64).into()).into(),
            ),
            am::ScalarValueRef::Boolean(v) => (Datatype::Boolean, (*v).into()),
            am::ScalarValueRef::Null => (Datatype::Null, JsValue::null()),
            am::ScalarValueRef::Unknown { bytes, type_code } => (
                Datatype::Unknown(*type_code),
                Uint8Array::from(bytes.as_ref()).into(),
            ),
        };
        self.wrap_scalar(js_value, datatype)
    }

    #[inline(never)]
    fn export_scalar(&self, value: &am::ScalarValue) -> Result<JsValue, error::Export> {
        let (datatype, js_value) = match value {
            am::ScalarValue::Bytes(v) => (Datatype::Bytes, Uint8Array::from(v.as_slice()).into()),
            am::ScalarValue::Str(v) => (Datatype::Str, v.to_string().into()),
            am::ScalarValue::Int(v) => (Datatype::Int, (*v as f64).into()),
            am::ScalarValue::Uint(v) => (Datatype::Uint, (*v as f64).into()),
            am::ScalarValue::F64(v) => (Datatype::F64, (*v).into()),
            am::ScalarValue::Counter(v) => (Datatype::Counter, (f64::from(v)).into()),
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
    fn ensure_key(&mut self, key: Cow<'a, str>) {
        self.keys
            .entry(key.clone())
            .or_insert_with(|| JsString::from(key.borrow()));
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
            Object::freeze(&value);
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

enum StackItem<'a> {
    Map {
        obj: ObjId,
        values: Vec<MapRangeItem<'a>>,
    },
    Seq {
        obj: ObjId,
        values: Vec<ListRangeItem<'a>>,
        datatype: Datatype,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct CachedObject {
    pub(crate) id: ObjId,
    pub(crate) inner: Object,
    pub(crate) outer: Object,
}

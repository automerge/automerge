use crate::interop::error;
use crate::value::Datatype;
use crate::Automerge;
use automerge as am;
use automerge::ChangeHash;
use fxhash::FxBuildHasher;
use js_sys::{Array, Function, JsString, Object, Reflect, Symbol, Uint8Array};
use std::collections::HashMap;
use std::ops::RangeFull;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use am::ObjId;

use am::iter::{ListRange, ListRangeItem, MapRange, MapRangeItem};

const RAW_DATA_SYMBOL: &str = "_am_raw_value_";
const DATATYPE_SYMBOL: &str = "_am_datatype_";
const RAW_OBJECT_SYMBOL: &str = "_am_objectId";
const META_SYMBOL: &str = "_am_meta";

#[derive(Debug)]
enum Pending<'a> {
    Map(
        Object,
        MapRangeItem<'a>,
        MapRange<'a, RangeFull>,
        ObjId,
        Datatype,
    ),
    List(
        Array,
        ListRangeItem<'a>,
        ListRange<'a, RangeFull>,
        ObjId,
        Datatype,
    ),
}

impl<'a> Pending<'a> {
    fn next_task(&self) -> Task<'a> {
        match self {
            Self::Map(_, item, ..) => Task::New(item.id.clone(), Datatype::from(&item.value)),
            Self::List(_, item, ..) => Task::New(item.id.clone(), Datatype::from(&item.value)),
        }
    }

    fn complete(
        self,
        value: JsValue,
        export: &mut ExportCache<'a>,
    ) -> Result<Task<'a>, error::Export> {
        match self {
            Self::Map(js_obj, map_item, iter, obj, datatype) => {
                export.set_prop(&js_obj, map_item.key, &value)?;
                Ok(Task::Map(js_obj, iter, obj, datatype))
            }
            Self::List(js_array, _list_item, iter, obj, datatype) => {
                js_array.push(&value);
                Ok(Task::List(js_array, iter, obj, datatype))
            }
        }
    }
}

#[derive(Debug)]
enum Task<'a> {
    New(ObjId, Datatype),
    Map(Object, MapRange<'a, RangeFull>, ObjId, Datatype),
    List(Array, ListRange<'a, RangeFull>, ObjId, Datatype),
}

impl<'a> Task<'a> {
    #[inline(never)]
    fn progress(
        self,
        export: &mut ExportCache<'a>,
        heads: Option<&Vec<ChangeHash>>,
        meta: &JsValue,
    ) -> Result<Progress<'a>, error::Export> {
        match self {
            Self::New(obj, Datatype::Text) if export.doc.text_rep.is_string() => {
                let text = export.doc.text_at(&obj, heads)?;
                Ok(Progress::Done(JsValue::from(text)))
            }
            Self::New(obj, datatype) if datatype.is_seq() => {
                let iter = export.doc.list_range_at(&obj, heads);
                let array = Array::new();
                Ok(Progress::Task(Task::List(array, iter, obj, datatype)))
            }
            Self::New(obj, datatype) => {
                let iter = export.doc.map_range_at(&obj, heads);
                let js_obj = Object::new();
                Ok(Progress::Task(Task::Map(js_obj, iter, obj, datatype)))
            }
            Self::Map(js_obj, iter, obj, datatype) => {
                Self::progress_map(js_obj, iter, obj, datatype, meta, export)
            }
            Self::List(js_array, iter, obj, datatype) => {
                Self::progress_list(js_array, iter, obj, datatype, meta, export)
            }
        }
    }

    #[inline(never)]
    fn progress_map(
        js_obj: Object,
        mut iter: MapRange<'a, RangeFull>,
        obj: ObjId,
        datatype: Datatype,
        meta: &JsValue,
        export: &mut ExportCache<'a>,
    ) -> Result<Progress<'a>, error::Export> {
        while let Some(map_item) = iter.next() {
            match map_item.value {
                am::Value::Object(_) => {
                    let pending = Pending::Map(js_obj, map_item, iter, obj, datatype);
                    return Ok(Progress::Pending(pending));
                }
                am::Value::Scalar(s) => {
                    export.set_prop(&js_obj, map_item.key, &export.export_scalar(&s)?)?
                }
            }
        }
        let wrapped = export.wrap_object(js_obj, &obj, datatype, meta)?;
        Ok(Progress::Done(JsValue::from(wrapped)))
    }

    #[inline(never)]
    fn progress_list(
        js_array: Array,
        mut iter: ListRange<'a, RangeFull>,
        obj: ObjId,
        datatype: Datatype,
        meta: &JsValue,
        export: &mut ExportCache<'a>,
    ) -> Result<Progress<'a>, error::Export> {
        while let Some(list_item) = iter.next() {
            match list_item.value {
                am::Value::Object(_) => {
                    let pending = Pending::List(js_array, list_item, iter, obj, datatype);
                    return Ok(Progress::Pending(pending));
                }
                am::Value::Scalar(s) => {
                    js_array.push(&export.export_scalar(&s)?);
                }
            }
        }
        let wrapped = export.wrap_object(Object::from(js_array), &obj, datatype, meta)?;
        Ok(Progress::Done(JsValue::from(wrapped)))
    }
}

#[derive(Debug)]
enum Progress<'a> {
    Done(JsValue),
    Task(Task<'a>),
    Pending(Pending<'a>),
}

#[derive(Debug, Clone)]
pub(crate) struct ExportCache<'a> {
    pub(crate) objs: HashMap<ObjId, CachedObject, FxBuildHasher>,
    datatypes: HashMap<Datatype, JsString, FxBuildHasher>,
    keys: HashMap<&'a str, JsString>,
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
    pub(crate) fn materialize(
        &mut self,
        obj: ObjId,
        datatype: Datatype,
        heads: Option<&Vec<ChangeHash>>,
        meta: &JsValue,
    ) -> Result<JsValue, error::Export> {
        let mut task = Task::New(obj, datatype);
        let mut stack: Vec<Pending<'_>> = Vec::new();
        loop {
            match task.progress(self, heads, meta)? {
                Progress::Task(t) => {
                    task = t;
                }
                Progress::Done(value) => match stack.pop() {
                    Some(pending) => {
                        task = pending.complete(value, self)?;
                    }
                    None => return Ok(value),
                },
                Progress::Pending(p) => {
                    task = p.next_task();
                    stack.push(p);
                }
            }
        }
    }

    #[inline(never)]
    pub(crate) fn set_prop(
        &mut self,
        obj: &Object,
        key: &'a str,
        value: &JsValue,
    ) -> Result<(), error::Export> {
        self.ensure_key(key);
        let key = self.keys.get(&key).unwrap(); // save - ensure above
        Reflect::set(obj, key, value).map_err(|error| error::SetProp {
            property: JsValue::from(key),
            error,
        })?;
        Ok(())
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
        if let Some(function) = self.doc.external_types.get(&datatype) {
            let wrapped_value = function
                .call1(&JsValue::undefined(), &value)
                .map_err(|e| error::Export::CallDataHandler(datatype.to_string(), e))?;
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
    fn ensure_key(&mut self, key: &'a str) {
        self.keys.entry(key).or_insert_with(|| JsString::from(key));
    }

    #[inline(never)]
    fn wrap_object(
        &mut self,
        value: Object,
        obj: &ObjId,
        datatype: Datatype,
        meta: &JsValue,
    ) -> Result<Object, error::Export> {
        let value = if let Some(function) = self.doc.external_types.get(&datatype) {
            self.wrap_custom_object(&value, datatype, function)?
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
        function: &Function,
    ) -> Result<Object, error::Export> {
        let wrapped_value = function
            .call1(&JsValue::undefined(), value)
            .map_err(|e| error::Export::CallDataHandler(datatype.to_string(), e))?;
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

#[derive(Debug, Clone)]
pub(crate) struct CachedObject {
    pub(crate) id: ObjId,
    pub(crate) inner: Object,
    pub(crate) outer: Object,
}

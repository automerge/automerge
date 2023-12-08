use crate::interop::error;
use crate::value::Datatype;
use crate::Automerge;
use automerge as am;
use automerge::ChangeHash;
use fxhash::FxBuildHasher;
use js_sys::{Array, Function, JsString, Object, Reflect, Uint8Array, WeakMap};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::RangeFull;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use am::ObjId;

use am::iter::{ListRange, ListRangeItem, MapRange, MapRangeItem};

#[derive(Debug, Clone)]
pub(crate) struct ObjMetadata {
    pub(crate) obj: Option<ObjId>,
    pub(crate) datatype: Datatype,
    pub(crate) user_data: JsValue,
    pub(crate) raw: ObjFormat,
}

#[derive(Debug, Clone)]
pub(crate) enum ObjFormat {
    Normal,
    CustomObject(Object),
    CustomScalar(JsValue),
}

impl ObjFormat {
    pub(crate) fn as_obj(&self) -> Option<&Object> {
        match self {
            Self::CustomObject(o) => Some(o),
            _ => None,
        }
    }

    pub(crate) fn as_js(&self) -> Option<&JsValue> {
        match self {
            Self::CustomObject(o) => Some(o),
            Self::CustomScalar(s) => Some(s),
            _ => None,
        }
    }
}

impl TryFrom<ObjMetadata> for JsValue {
    type Error = JsValue;

    fn try_from(data: ObjMetadata) -> Result<JsValue, JsValue> {
        let o = Object::new();
        if let Some(id) = data.obj.as_ref() {
            Reflect::set(&o, &"obj".into(), &id.to_string().into())?;
        }
        Reflect::set(&o, &"datatype".into(), &data.datatype.to_string().into())?;
        if !data.user_data.is_undefined() {
            Reflect::set(&o, &"user_data".into(), &data.user_data)?;
        }
        if let Some(raw) = data.raw.as_js() {
            Reflect::set(&o, &"raw".into(), raw)?;
        }
        Ok(o.into())
    }
}

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
                Self::progress_map(js_obj, iter, obj, datatype, export)
            }
            Self::List(js_array, iter, obj, datatype) => {
                Self::progress_list(js_array, iter, obj, datatype, export)
            }
        }
    }

    #[inline(never)]
    fn progress_map(
        js_obj: Object,
        mut iter: MapRange<'a, RangeFull>,
        obj: ObjId,
        datatype: Datatype,
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
        let wrapped = export.wrap_object(js_obj, &obj, datatype)?;
        Ok(Progress::Done(JsValue::from(wrapped)))
    }

    #[inline(never)]
    fn progress_list(
        js_array: Array,
        mut iter: ListRange<'a, RangeFull>,
        obj: ObjId,
        datatype: Datatype,
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
        let wrapped = export.wrap_object(Object::from(js_array), &obj, datatype)?;
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
    objs: HashMap<ObjId, COWJsObj, FxBuildHasher>,
    metadata: WeakMap,
    user_data: JsValue,
    keys: HashMap<&'a str, JsString>,
    doc: &'a Automerge,
}

impl<'a> ExportCache<'a> {
    pub(crate) fn user_data(&self) -> &JsValue {
        &self.user_data
    }

    pub(crate) fn new(
        doc: &'a Automerge,
        metadata: WeakMap,
        user_data: JsValue,
    ) -> Result<Self, error::Export> {
        Ok(Self {
            objs: HashMap::default(),
            keys: HashMap::default(),
            doc,
            metadata,
            user_data,
        })
    }

    #[inline(never)]
    pub(crate) fn materialize(
        &mut self,
        obj: ObjId,
        datatype: Datatype,
        heads: Option<&Vec<ChangeHash>>,
    ) -> Result<JsValue, error::Export> {
        let mut task = Task::New(obj, datatype);
        let mut stack: Vec<Pending<'_>> = Vec::new();
        loop {
            match task.progress(self, heads)? {
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
            am::ScalarValue::Unknown { bytes, type_code } => {
                let mut buff = bytes.clone();
                buff.push(*type_code);
                (Datatype::Unknown, Uint8Array::from(buff.as_slice()).into())
            }
        };
        self.wrap_scalar(js_value, datatype)
    }

    #[inline(never)]
    pub(crate) fn wrap_scalar(
        &self,
        value: JsValue,
        datatype: Datatype,
    ) -> Result<JsValue, error::Export> {
        if let Some(function) = self.doc.external_types.get(&datatype) {
            let wrapped_value = function
                .call1(&JsValue::undefined(), &value)
                .map_err(|e| error::Export::CallDataHandler(datatype.to_string(), e))?;
            let o = wrapped_value
                .dyn_into::<Object>()
                .map_err(|_| error::Export::InvalidDataHandler(datatype.to_string()))?;
            let encoded = Self::encode_obj_metadata(
                None,
                datatype,
                &self.user_data,
                ObjFormat::CustomScalar(value),
            );
            self.metadata.set(&o, &encoded);
            Ok(JsValue::from(o))
        } else {
            Ok(value)
        }
    }

    #[inline(never)]
    fn ensure_key(&mut self, key: &'a str) {
        self.keys.entry(key).or_insert_with(|| JsString::from(key));
    }

    #[inline(never)]
    pub(crate) fn wrap_object(
        &mut self,
        mut value: Object,
        obj: &ObjId,
        datatype: Datatype,
    ) -> Result<Object, error::Export> {
        let mut raw = ObjFormat::Normal;

        if let Some(function) = self.doc.external_types.get(&datatype) {
            let wrapped = Self::wrap_custom_object(&value, datatype, function)?;
            raw = ObjFormat::CustomObject(value);
            value = wrapped;
        }

        self.set_obj_metadata(&value, Some(obj), datatype, raw);

        if self.doc.freeze {
            Object::freeze(&value);
        }

        Ok(value)
    }

    // todo - figure out a way to return a reference here instead of cloning
    pub(crate) fn copy_on_write(
        &mut self,
        outer: &Object,
    ) -> Result<(bool, COWJsObj), error::Export> {
        let metadata = self.get_obj_metadata(outer).unwrap_or(ObjMetadata {
            obj: Some(am::ROOT),
            datatype: Datatype::Map,
            user_data: self.user_data().clone(),
            raw: ObjFormat::Normal,
        });

        let obj = metadata
            .obj
            .as_ref()
            .ok_or(error::Export::InvalidObjMetadata)?
            .clone();
        let datatype = metadata.datatype;

        match self.objs.entry(obj.clone()) {
            Entry::Occupied(entry) => Ok((true, entry.get().clone())),
            Entry::Vacant(entry) => {
                let mut cow_js = COWJsObj {
                    metadata,
                    outer: outer.clone(),
                };

                let shallow_copy = shallow_copy(cow_js.inner());

                if let Some(function) = self.doc.external_types.get(&datatype) {
                    let wrapped = Self::wrap_custom_object(&shallow_copy, datatype, function)?;
                    cow_js.set_wrapped_object(shallow_copy, wrapped);
                } else {
                    cow_js.set_normal_object(shallow_copy);
                }

                // in theory - we could reuse the underlying Array and just update entry 2 & 3
                let encoded = Self::encode_obj_metadata(
                    Some(&obj),
                    datatype,
                    &self.user_data,
                    cow_js.metadata.raw.clone(),
                );

                self.metadata.set(&cow_js.outer, &encoded);

                entry.insert(cow_js.clone());

                Ok((false, cow_js))
            }
        }
    }

    #[inline(never)]
    pub(crate) fn wrap_custom_object(
        value: &JsValue,
        datatype: Datatype,
        function: &Function,
    ) -> Result<Object, error::Export> {
        let wrapped_value = function
            .call1(&JsValue::undefined(), value)
            .map_err(|e| error::Export::CallDataHandler(datatype.to_string(), e))?;
        let wrapped_object = wrapped_value
            .dyn_into::<Object>()
            .map_err(|_| error::Export::InvalidDataHandler(datatype.to_string()))?;
        Ok(wrapped_object)
    }

    pub(crate) fn encode_obj_metadata(
        obj: Option<&ObjId>,
        datatype: Datatype,
        user_data: &JsValue,
        raw: ObjFormat,
    ) -> JsValue {
        let mut bytes = obj.map(|o| o.to_bytes()).unwrap_or_default();
        bytes.push(datatype as u8);
        let state = Array::new();
        state.push(&Uint8Array::from(bytes.as_slice()));
        state.push(user_data);
        match &raw {
            ObjFormat::CustomObject(o) => {
                state.push(o);
            }
            ObjFormat::CustomScalar(s) => {
                state.push(s);
            }
            _ => {}
        }
        JsValue::from(state)
    }

    pub(crate) fn get_obj_metadata(&self, obj: &Object) -> Option<ObjMetadata> {
        let meta = self.metadata.get(obj);
        Self::decode_obj_metadata(&meta)
    }

    pub(crate) fn decode_obj_metadata(state: &JsValue) -> Option<ObjMetadata> {
        let array = state.clone().dyn_into::<Array>().ok()?;
        let mut raw = ObjFormat::Normal;
        if array.length() == 3 {
            let inner = array.get(2);
            if inner.is_object() {
                raw = ObjFormat::CustomObject(inner.dyn_into::<Object>().ok()?);
            } else {
                raw = ObjFormat::CustomScalar(inner);
            }
        }
        let user_data = array.get(1);
        //let mut bytes = array.pop().dyn_into::<Uint8Array>().ok()?.to_vec();
        let bytes = array.get(0).dyn_into::<Uint8Array>();
        let mut bytes = bytes.ok()?.to_vec();
        let datatype = bytes.pop()?;
        let datatype = Datatype::try_from(datatype).ok()?;
        let obj = ObjId::try_from(bytes.as_slice()).ok();
        Some(ObjMetadata {
            obj,
            datatype,
            user_data,
            raw,
        })
    }

    pub(crate) fn set_obj_metadata(
        &self,
        js_obj: &Object,
        id: Option<&ObjId>,
        datatype: Datatype,
        raw: ObjFormat,
    ) {
        let encoded = Self::encode_obj_metadata(id, datatype, &self.user_data, raw);
        self.metadata.set(js_obj, &encoded);
    }

    pub(crate) fn freeze_objects(&self) {
        for cow in self.objs.values() {
            Object::freeze(&cow.outer);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct COWJsObj {
    pub(crate) metadata: ObjMetadata,
    pub(crate) outer: Object,
}

impl COWJsObj {
    pub(crate) fn id(&self) -> Option<&ObjId> {
        self.metadata.obj.as_ref()
    }

    pub(crate) fn inner(&self) -> &Object {
        self.metadata.raw.as_obj().unwrap_or(&self.outer)
    }

    fn set_wrapped_object(&mut self, copy: Object, wrapped: Object) {
        self.metadata.raw = ObjFormat::CustomObject(copy);
        self.outer = wrapped;
    }

    fn set_normal_object(&mut self, copy: Object) {
        self.outer = copy;
        self.metadata.raw = ObjFormat::Normal;
    }
}

fn shallow_copy(obj: &Object) -> Object {
    if Array::is_array(obj) {
        Array::from(obj).into()
    } else {
        Object::assign(&Object::new(), obj)
    }
}

use crate::{AMobj, AMresult, AmDataType};
use automerge as am;
use libc::{c_double};
use std::{
    ffi::{c_void, CStr},
    ops::Deref,
    os::raw::c_char,
};

impl Deref for AMobj {
    type Target = am::ObjId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
impl From<*const AMobj> for AMobj {
    fn from(obj: *const AMobj) -> Self {
        unsafe { obj.as_ref().cloned().unwrap_or(AMobj(am::ROOT)) }
    }
}

impl From<AMresult> for *mut AMresult {
    fn from(b: AMresult) -> Self {
        Box::into_raw(Box::new(b))
    }
}

impl From<&am::Value> for AmDataType {
    fn from(v: &am::Value) -> Self {
        match v {
            am::Value::Scalar(am::ScalarValue::Str(_)) => AmDataType::Str,
            am::Value::Scalar(am::ScalarValue::Int(_)) => AmDataType::Int,
            am::Value::Scalar(am::ScalarValue::Uint(_)) => AmDataType::Uint,
            am::Value::Scalar(am::ScalarValue::F64(_)) => AmDataType::F64,
            am::Value::Scalar(am::ScalarValue::Boolean(_)) => AmDataType::Boolean,
            am::Value::Scalar(am::ScalarValue::Bytes(_)) => AmDataType::Bytes,
            am::Value::Scalar(am::ScalarValue::Counter(_)) => AmDataType::Counter,
            am::Value::Scalar(am::ScalarValue::Timestamp(_)) => AmDataType::Timestamp,
            am::Value::Scalar(am::ScalarValue::Null) => AmDataType::Null,
            am::Value::Object(am::ObjType::Map) => AmDataType::Map,
            am::Value::Object(am::ObjType::List) => AmDataType::List,
            am::Value::Object(am::ObjType::Table) => AmDataType::Table,
            am::Value::Object(am::ObjType::Text) => AmDataType::Text,
        }
    }
}

pub(crate) fn import_value(
    value: *const c_void,
    data_type: AmDataType,
) -> Result<am::Value, AMresult> {
    unsafe {
        match data_type {
            AmDataType::Str => {
                let value: *const c_char = value.cast();
                if !value.is_null() {
                    Some(CStr::from_ptr(value).to_string_lossy().to_string().into())
                } else {
                    None
                }
            }
            AmDataType::Boolean => value
                .cast::<*const c_char>()
                .as_ref()
                .map(|v| am::Value::boolean(**v != 0)),
            AmDataType::Int => value
                .cast::<*const i64>()
                .as_ref()
                .map(|v| am::Value::int(**v)),
            AmDataType::Uint => value
                .cast::<*const u64>()
                .as_ref()
                .map(|v| am::Value::uint(**v)),
            AmDataType::F64 => value
                .cast::<*const c_double>()
                .as_ref()
                .map(|v| am::Value::f64(**v)),
            AmDataType::Timestamp => value
                .cast::<*const i64>()
                .as_ref()
                .map(|v| am::Value::timestamp(**v)),
            AmDataType::Counter => value
                .cast::<*const i64>()
                .as_ref()
                .map(|v| am::Value::counter(**v)),
            AmDataType::Null => Some(am::Value::null()),
            AmDataType::Map => Some(am::Value::map()),
            AmDataType::List => Some(am::Value::list()),
            AmDataType::Text => Some(am::Value::text()),
            AmDataType::Table => Some(am::Value::table()),
            _ => return Err(AMresult::err("Invalid data type")),
        }
        .ok_or_else(|| AMresult::err("Null value"))
    }
}

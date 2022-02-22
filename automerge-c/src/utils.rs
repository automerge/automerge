use crate::{AMobj, AMresult, AmDatatype};
use automerge as am;
use libc::{c_double, c_long, c_ulong};
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

impl From<&am::Value> for AmDatatype {
    fn from(v: &am::Value) -> Self {
        match v {
            am::Value::Scalar(am::ScalarValue::Str(_)) => AmDatatype::Str,
            am::Value::Scalar(am::ScalarValue::Int(_)) => AmDatatype::Int,
            am::Value::Scalar(am::ScalarValue::Uint(_)) => AmDatatype::Uint,
            am::Value::Scalar(am::ScalarValue::F64(_)) => AmDatatype::F64,
            am::Value::Scalar(am::ScalarValue::Boolean(_)) => AmDatatype::Boolean,
            am::Value::Scalar(am::ScalarValue::Bytes(_)) => AmDatatype::Bytes,
            am::Value::Scalar(am::ScalarValue::Counter(_)) => AmDatatype::Counter,
            am::Value::Scalar(am::ScalarValue::Timestamp(_)) => AmDatatype::Timestamp,
            am::Value::Scalar(am::ScalarValue::Null) => AmDatatype::Null,
            am::Value::Object(am::ObjType::Map) => AmDatatype::Map,
            am::Value::Object(am::ObjType::List) => AmDatatype::List,
            am::Value::Object(am::ObjType::Table) => AmDatatype::Table,
            am::Value::Object(am::ObjType::Text) => AmDatatype::Text,
        }
    }
}

pub(crate) fn import_value(
    value: *const c_void,
    datatype: AmDatatype,
) -> Result<am::Value, AMresult> {
    unsafe {
        match datatype {
            AmDatatype::Str => {
                let value: *const c_char = value.cast();
                if !value.is_null() {
                    Some(CStr::from_ptr(value).to_string_lossy().to_string().into())
                } else {
                    None
                }
            }
            AmDatatype::Boolean => value
                .cast::<*const c_char>()
                .as_ref()
                .map(|v| am::Value::boolean(**v != 0)),
            AmDatatype::Int => value
                .cast::<*const c_long>()
                .as_ref()
                .map(|v| am::Value::int(**v)),
            AmDatatype::Uint => value
                .cast::<*const c_ulong>()
                .as_ref()
                .map(|v| am::Value::uint(**v)),
            AmDatatype::F64 => value
                .cast::<*const c_double>()
                .as_ref()
                .map(|v| am::Value::f64(**v)),
            AmDatatype::Timestamp => value
                .cast::<*const c_long>()
                .as_ref()
                .map(|v| am::Value::timestamp(**v)),
            AmDatatype::Counter => value
                .cast::<*const c_long>()
                .as_ref()
                .map(|v| am::Value::counter(**v)),
            AmDatatype::Null => Some(am::Value::null()),
            AmDatatype::Map => Some(am::Value::map()),
            AmDatatype::List => Some(am::Value::list()),
            AmDatatype::Text => Some(am::Value::text()),
            AmDatatype::Table => Some(am::Value::table()),
            _ => return Err(AMresult::err("Invalid datatype")),
        }
        .ok_or_else(|| AMresult::err("Null value"))
    }
}

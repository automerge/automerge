use automerge as am;
use libc::{c_long, c_ulong, c_double};
use std::{
    ffi::{c_void, CStr},
    ops::{Deref, DerefMut},
    os::raw::c_char,
};
use crate::{ ObjId, Automerge, Prop, CError , Datatype };

impl Deref for Automerge {
    type Target = am::Automerge;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl DerefMut for Automerge {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.handle
    }
}

impl Deref for ObjId {
    type Target = am::ObjId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<*const ObjId> for ObjId {
    fn from(obj: *const ObjId) -> Self {
        unsafe { obj.as_ref().cloned().unwrap_or(ObjId(am::ROOT)) }
    }
}

impl From<Automerge> for *mut Automerge {
    fn from(b: Automerge) -> Self {
        Box::into_raw(Box::new(b))
    }
}

impl From<*const c_char> for Prop {
    fn from(c: *const c_char) -> Self {
        unsafe { Prop(CStr::from_ptr(c).to_string_lossy().to_string().into()) }
    }
}

impl From<&am::Value> for Datatype {
    fn from(v: &am::Value) -> Self {
        match v {
            am::Value::Scalar(am::ScalarValue::Str(_)) => Datatype::Str,
            am::Value::Scalar(am::ScalarValue::Int(_)) => Datatype::Int,
            am::Value::Scalar(am::ScalarValue::Uint(_)) => Datatype::Uint,
            am::Value::Scalar(am::ScalarValue::F64(_)) => Datatype::F64,
            am::Value::Scalar(am::ScalarValue::Boolean(_)) => Datatype::Boolean,
            am::Value::Scalar(am::ScalarValue::Bytes(_)) => Datatype::Bytes,
            am::Value::Scalar(am::ScalarValue::Counter(_)) => Datatype::Counter,
            am::Value::Scalar(am::ScalarValue::Timestamp(_)) => Datatype::Timestamp,
            am::Value::Scalar(am::ScalarValue::Null) => Datatype::Null,
            am::Value::Object(am::ObjType::Map) => Datatype::Map,
            am::Value::Object(am::ObjType::List) => Datatype::List,
            am::Value::Object(am::ObjType::Table) => Datatype::Table,
            am::Value::Object(am::ObjType::Text) => Datatype::Text,
        }
    }
}

impl From<Prop> for am::Prop {
    fn from(p: Prop) -> Self {
        p.0
    }
}

impl From<am::AutomergeError> for CError {
    fn from(e: am::AutomergeError) -> Self {
      CError::AutomergeError(e)
    }
}

pub (crate) fn import_value(value: *const c_void, datatype: Datatype) -> Result<am::Value, CError> {
  unsafe {
    match datatype {
      Datatype::Str => {
        let value : *const c_char = value.cast();
        if !value.is_null() {
          Some(CStr::from_ptr(value).to_string_lossy().to_string().into())
        } else {
          None
        }
      },
      Datatype::Boolean => {
        value.cast::<*const c_char>().as_ref()
          .map(|v| am::Value::boolean(**v != 0))
      },
      Datatype::Int => {
        value.cast::<*const c_long>().as_ref()
          .map(|v| am::Value::int(**v))
      },
      Datatype::Uint => {
        value.cast::<*const c_ulong>().as_ref()
          .map(|v| am::Value::uint(**v))
      },
      Datatype::F64 => {
        value.cast::<*const c_double>().as_ref()
          .map(|v| am::Value::f64(**v))
      },
      Datatype::Timestamp => {
        value.cast::<*const c_long>().as_ref()
          .map(|v| am::Value::timestamp(**v))
      },
      Datatype::Counter => {
        value.cast::<*const c_long>().as_ref()
          .map(|v| am::Value::counter(**v))
      },
      Datatype::Null =>Some(am::Value::null()),
      Datatype::Map => Some(am::Value::map()),
      Datatype::List => Some(am::Value::list()),
      Datatype::Text => Some(am::Value::text()),
      Datatype::Table => Some(am::Value::table()),
      _ => {
        return Err(CError::InvalidDatatype(datatype))
      }
    }.ok_or(CError::NullValue(datatype))
  }
}

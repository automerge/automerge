use automerge as am;
use libc::{c_int, c_long, c_ulong, c_double};
use std::{
    ffi::{c_void, CStr, CString},
    ops::{Deref, DerefMut},
    os::raw::c_char,
};

#[no_mangle]
pub static ROOT: am::ObjId = am::ROOT;

pub const AM_TYPE_STR : c_int = 1;
pub const AM_TYPE_INT : c_int = 2;
pub const AM_TYPE_UINT : c_int = 3;
pub const AM_TYPE_F64 : c_int = 4;
pub const AM_TYPE_BOOL : c_int = 5;
pub const AM_TYPE_COUNTER : c_int = 6;
pub const AM_TYPE_TIMESTAMP : c_int = 7;
pub const AM_TYPE_BYTES : c_int = 8;
pub const AM_TYPE_NULL : c_int = 9;
pub const AM_TYPE_MAP : c_int = 10;
pub const AM_TYPE_LIST : c_int = 11;
pub const AM_TYPE_TEXT : c_int = 12;
pub const AM_TYPE_TABLE : c_int = 13;

/// Try to turn a `*mut Automerge` into a &mut Automerge,
/// return an error code if failure
macro_rules! get_handle_mut {
    ($handle:expr) => {{
        let handle = $handle.as_mut();
        match handle {
            Some(b) => b,
            // Don't call `record_error` b/c there is no valid handle!
            None => return CError::NullAutomerge.error_code(),
        }
    }};
}

pub struct Automerge {
    handle: am::Automerge,
    results: Vec<am::ObjId>,
    error: Option<CString>,
}

#[derive(Clone)]
pub struct ObjId(am::ObjId);

pub struct Prop(am::Prop);

/// All possible errors that a C caller could face
#[derive(thiserror::Error, Debug)]
pub enum CError {
    #[error("Automerge pointer was null")]
    NullAutomerge,
    #[error("ObjId pointer was null")]
    NullObjId,
    #[error("Value pointer was null with datatype {0}")]
    NullValue(c_int),
    #[error("Invalid datatype: {0}")]
    InvalidDatatype(c_int),
    #[error("AutomergeError: '{0}'")]
    AutomergeError(am::AutomergeError),
}

impl CError {
    fn error_code(self) -> isize {
        // 0 is reserved for "success"
        const BASE: isize = -1;
        match self {
            CError::NullAutomerge => BASE,
            CError::NullObjId => BASE - 1,
            CError::NullValue(_) => BASE - 2,
            CError::InvalidDatatype(_) => BASE - 3,
            CError::AutomergeError(_) => BASE - 4,
        }
    }
}

impl Clone for Automerge {
    fn clone(&self) -> Self {
        Automerge {
            handle: self.handle.clone(),
            results: Vec::new(),
            error: None,
        }
    }
}

impl Automerge {
    fn create(handle: am::Automerge) -> Automerge {
        Automerge {
            handle,
            results: Vec::new(),
            error: None,
        }
    }

    unsafe fn set_map(
        &mut self,
        obj: *const ObjId,
        prop: *const c_char,
        datatype: c_int,
        value: *const c_void, // i64, u64, boolean, char*, u8*+len,
    ) -> Result<Vec<am::ObjId>, CError> {
        let obj: &ObjId = obj.try_into()?;
        let prop = Prop::from(prop);
        let value = self.get_value(value, datatype)?;
        Ok(self.set(&obj, prop, value).map(|id| id.into_iter().collect())?)
    }

    fn get_value(
        &self,
        value: *const c_void,
        datatype: c_int,
    ) -> Result<am::Value, CError> {
      unsafe {
        match datatype {
          AM_TYPE_STR => {
            let value : *const c_char = value.cast();
            if !value.is_null() {
              Some(CStr::from_ptr(value).to_string_lossy().to_string().into())
            } else {
              None
            }
          },
          AM_TYPE_BOOL => {
            value.cast::<*const c_char>().as_ref()
              .map(|v| am::Value::boolean(**v != 0))
          },
          AM_TYPE_INT => {
            value.cast::<*const c_long>().as_ref()
              .map(|v| am::Value::int(**v))
          },
          AM_TYPE_UINT => {
            value.cast::<*const c_ulong>().as_ref()
              .map(|v| am::Value::uint(**v))
          },
          AM_TYPE_F64 => {
            value.cast::<*const c_double>().as_ref()
              .map(|v| am::Value::f64(**v))
          },
          AM_TYPE_TIMESTAMP => {
            value.cast::<*const c_long>().as_ref()
              .map(|v| am::Value::timestamp(**v))
          },
          AM_TYPE_COUNTER => {
            value.cast::<*const c_long>().as_ref()
              .map(|v| am::Value::counter(**v))
          },
          AM_TYPE_NULL =>Some(am::Value::null()),
          AM_TYPE_MAP => Some(am::Value::map()),
          AM_TYPE_LIST => Some(am::Value::list()),
          AM_TYPE_TEXT => Some(am::Value::text()),
          AM_TYPE_TABLE => Some(am::Value::table()),
          _ => {
            return Err(CError::InvalidDatatype(datatype))
          }
        }.ok_or(CError::NullValue(datatype))
      }
    }

    fn resolve<E:Into<CError>>(&mut self, result: Result<Vec<am::ObjId>, E>) -> isize {
        match result {
            Ok(r) => {
              self.results = r;
              self.results.len() as isize
            },
            Err(err) => {
                let err = err.into();
                let c_error = match CString::new(format!("{}", err)) {
                    Ok(e) => e,
                    Err(_) => {
                        return -1;
                    }
                };
                self.error = Some(c_error);
                err.error_code()
            }
        }
    }
}

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

impl TryFrom<*const ObjId> for &ObjId {
    type Error = CError;

    fn try_from(obj: *const ObjId) -> Result<Self, Self::Error> {
        unsafe { obj.as_ref().ok_or(CError::NullObjId) }
    }
}

/*
impl From<Result<Option<am::ObjId>,AutomergeError>> for Result<Vec<am::ObjId>,CError> {
    fn from(r: Result<Option<am::ObjId>,am::AutomergeError>) -> Self {
      r.map_err(|e| CError::AutomergeError(e)).map(|id| id.iter().collect())
    }
}
*/

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

#[no_mangle]
pub extern "C" fn automerge_create() -> *mut Automerge {
    Automerge::create(am::Automerge::new()).into()
}

/// # Safety
/// This must be called with a valid handle pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_free(handle: *mut Automerge) {
    let handle: Automerge = *Box::from_raw(handle);
    drop(handle)
}

/// # Safety
/// This must be called with a valid handle pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_clone(handle: *mut Automerge) -> *mut Automerge {
    let handle: Automerge = *Box::from_raw(handle);
    handle.clone().into()
}

/// # Safety
/// This should be called with a valid pointer to a `Automerge` and `ObjId`. value pointer type and `datatype` must match.
#[no_mangle]
pub unsafe extern "C" fn automerge_set_map(
    handle: *mut Automerge,
    obj: *const ObjId,
    prop: *const c_char,
    datatype: c_int,
    value: *const c_void, // i64, u64, boolean, char*, u8*+len,
) -> isize {
    let handle = get_handle_mut!(handle);
    let result = handle.set_map(obj, prop, datatype, value);
    handle.resolve(result)
}

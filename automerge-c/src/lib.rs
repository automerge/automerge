use automerge as am;
use std::{
    fmt,
    ffi::{c_void, CString, CStr},
    os::raw::c_char,
};

mod utils;
mod api_result;

use utils::{ import_value };
use api_result::{ ApiResult };

#[derive(Debug)]
#[repr(u32)]
pub enum Datatype {
    Str,
    Int,
    Uint,
    F64,
    Boolean,
    Counter,
    Timestamp,
    Bytes,
    Null,
    Map,
    List,
    Table,
    Text,
}

impl fmt::Display for Datatype {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

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
    results: Vec<ApiResult>,
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
    #[error("argument was null")]
    InvalidPointer,
    #[error("actor was invalid")]
    InvalidActor,
    #[error("value pointer was null with datatype {0}")]
    NullValue(Datatype),
    #[error("Invalid datatype: {0}")]
    InvalidDatatype(Datatype),
    #[error("AutomergeError: '{0}'")]
    AutomergeError(am::AutomergeError),
}

impl CError {
    fn error_code(self) -> isize {
        // 0 is reserved for "success"
        const BASE: isize = -1;
        match self {
            CError::NullAutomerge => BASE,
            CError::InvalidPointer => BASE - 1,
            CError::NullValue(_) => BASE - 2,
            CError::InvalidDatatype(_) => BASE - 3,
            CError::InvalidActor => BASE - 4,
            CError::AutomergeError(_) => BASE - 5,
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

    unsafe fn map_set(
        &mut self,
        obj: *const ObjId,
        prop: *const c_char,
        datatype: Datatype,
        value: *const c_void, // i64, u64, boolean, char*, u8*+len,
    ) -> Result<Vec<am::ObjId>, CError> {
        let obj = ObjId::from(obj);
        let prop = Prop::from(prop);
        let value = import_value(value, datatype)?;
        Ok(self.set(&obj, prop, value).map(|id| id.into_iter().collect())?)
    }

    unsafe fn map_values(
        &mut self,
        obj: *const ObjId,
        prop: *const c_char,
    ) -> Result<Vec<(am::Value,am::ObjId)>, CError> {
        let obj = ObjId::from(obj);
        let prop = Prop::from(prop);
        Ok(self.values(&obj, prop)?)
    }


    fn resolve<E:Into<CError>,V:Into<ApiResult>>(&mut self, result: Result<Vec<V>, E>) -> isize {
        match result {
            Ok(r) => {
              self.results = r.into_iter().map(|v| v.into()).collect();
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

#[no_mangle]
pub extern "C" fn am_create() -> *mut Automerge {
    Automerge::create(am::Automerge::new()).into()
}

/// # Safety
/// This must be called with a valid handle pointer
#[no_mangle]
pub unsafe extern "C" fn am_free(handle: *mut Automerge) {
    let handle: Automerge = *Box::from_raw(handle);
    drop(handle)
}

/// # Safety
/// This must be called with a valid handle pointer
#[no_mangle]
pub unsafe extern "C" fn am_clone(handle: *mut Automerge) -> *mut Automerge {
    let handle: Automerge = *Box::from_raw(handle);
    handle.clone().into()
}

/// # Safety
/// This should be called with a valid pointer to a `Automerge` and `ObjId`. value pointer type and `datatype` must match.
#[no_mangle]
pub unsafe extern "C" fn am_set_actor_hex(
    handle: *mut Automerge,
    actor: *const c_char,
) -> isize {
    let handle = get_handle_mut!(handle);
    if actor.is_null() {
        CError::InvalidPointer.error_code()
    } else {
        let actor = CStr::from_ptr(actor).to_string_lossy().to_string();
        if let Ok(actor) = actor.try_into() {
            handle.set_actor(actor);
            0
        } else {
            CError::InvalidActor.error_code()
        }
    }
}

/// # Safety
/// This should be called with a valid pointer to a `Automerge` and `ObjId`. value pointer type and `datatype` must match.
#[no_mangle]
pub unsafe extern "C" fn am_map_set(
    handle: *mut Automerge,
    obj: *const ObjId,
    prop: *const c_char,
    datatype: Datatype,
    value: *const c_void, // i64, u64, boolean, char*, u8*+len,
) -> isize {
    let handle = get_handle_mut!(handle);
    let result = handle.map_set(obj, prop, datatype, value);
    handle.resolve(result)
}

/// # Safety
/// This should be called with a valid pointer to a `Automerge` and `ObjId`. value pointer type and `datatype` must match.
#[no_mangle]
pub unsafe extern "C" fn am_map_values(
    handle: *mut Automerge,
    obj: *const ObjId,
    prop: *const c_char,
) -> isize {
    let handle = get_handle_mut!(handle);
    let result = handle.map_values(obj, prop);
    handle.resolve(result)
}

/// # Safety
/// This should be called with a valid pointer to a `Automerge` and `ObjId`. value pointer type and `datatype` must match.
#[no_mangle]
pub unsafe extern "C" fn am_pop_value(
    handle: *mut Automerge,
    datatype: *mut Datatype,
    value: *mut u8,
    len: usize
) -> isize {
    let handle = get_handle_mut!(handle);
    let r = handle.results.pop().unwrap(); // handle no results - FIXME
    if let Some(d) = r.datatype() {
        *datatype = d;
        let buff = r.to_bytes();
        if buff.len() > len {
            (buff.len() as isize) * - 1
        } else {
            value.copy_from(buff.as_ptr(), buff.len());
            buff.len() as isize
        }
    } else {
        -1 // not a value - FIXME
    }
}


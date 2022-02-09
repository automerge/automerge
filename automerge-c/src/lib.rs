use automerge as am;
use std::{
    ffi::{c_void, CStr},
    fmt,
    os::raw::c_char,
};

mod doc;
mod result;
mod utils;

use doc::AMdoc;
use result::AMresult;
use utils::import_value;

#[derive(Debug)]
#[repr(u8)]
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

#[derive(Debug)]
#[repr(u8)]
pub enum ResultType {
    Ok,
    ObjId,
}

impl fmt::Display for Datatype {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

macro_rules! to_str {
    ($s:expr) => {{
        CStr::from_ptr($s).to_string_lossy().to_string()
    }};
}

macro_rules! to_doc {
    ($handle:expr) => {{
        let handle = $handle.as_mut();
        match handle {
            Some(b) => b,
            None => return AMresult::Error("Invalid AMdoc pointer".into()).into(),
        }
    }};
}

macro_rules! to_value {
    ($a:expr,$b:expr) => {{
        match import_value($a, $b) {
            Ok(v) => v,
            Err(r) => return r.into(),
        }
    }};
}

macro_rules! to_prop {
    ($key:expr) => {{
        // TODO - check null pointer
        am::Prop::Map(std::ffi::CStr::from_ptr($key).to_string_lossy().to_string())
    }};
}

macro_rules! to_obj {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::Error("Invalid ObjID pointer".into()).into(),
        }
    }};
}

macro_rules! to_result {
    ($val:expr) => {{
        Box::into_raw(Box::new(($val).into()))
    }};
}

#[derive(Clone)]
pub struct AMobj(am::ObjId);

#[no_mangle]
pub extern "C" fn AMcreate() -> *mut AMdoc {
    AMdoc::create(am::Automerge::new()).into()
}

/// # Safety
/// This must be called with a valid doc pointer
#[no_mangle]
pub unsafe extern "C" fn AMfree(doc: *mut AMdoc) {
    if !doc.is_null() {
        let doc: AMdoc = *Box::from_raw(doc);
        drop(doc)
    }
}

/// # Safety
/// This must be called with a valid doc pointer
#[no_mangle]
pub unsafe extern "C" fn AMclone(doc: *mut AMdoc) -> *mut AMdoc {
    let doc = *Box::from_raw(doc);
    let copy = doc.clone();
    std::mem::forget(doc);
    copy.into()
}

/// # Safety
/// This should be called with a valid pointer to a `AMdoc`.
/// key="actor" value=(actor id in hex format)
#[no_mangle]
pub unsafe extern "C" fn AMconfig(
    doc: *mut AMdoc,
    key: *const c_char,
    value: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let key = to_str!(key);
    match key.as_str() {
        "actor" => {
            let actor = to_str!(value);
            if let Ok(actor) = actor.try_into() {
                doc.set_actor(actor);
                AMresult::Ok.into()
            } else {
                AMresult::Error(format!("Invalid actor '{}'", to_str!(value))).into()
            }
        }
        k => AMresult::Error(format!("Invalid config key '{}'", k)).into(),
    }
}

/// # Safety
/// This should be called with a valid pointer to a `AMdoc`.
/// key="actor" value=(actor id in hex format)
#[no_mangle]
pub unsafe extern "C" fn AMgetActor(_doc: *mut AMdoc) -> *mut AMresult {
    //let doc = to_doc!(doc);
    unimplemented!()
}

/// # Safety
/// This should be called with a valid pointer to a `AMresult` or NULL
#[no_mangle]
pub unsafe extern "C" fn AMresultStatus(_result: *const AMresult) -> isize {
    unimplemented!()
}

/// # Safety
/// This should be called with a valid pointer to a `AMresult` or NULL
#[no_mangle]
pub unsafe extern "C" fn AMmapSet(
    doc: *mut AMdoc,
    obj: *mut am::ObjId,
    key: *const c_char,
    datatype: Datatype,
    value: *const c_void,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result!(doc.set(to_obj!(obj), to_prop!(key), to_value!(value, datatype)))
}

/// # Safety
/// This should be called with a valid pointer to a `AMresult` or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistSet(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    datatype: Datatype,
    value: *const c_void,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result!(doc.set(to_obj!(obj), index, to_value!(value,datatype)))
}

/// # Safety
/// This should be called with a valid pointer to a `AMresult` or NULL
#[no_mangle]
pub unsafe extern "C" fn AMgetObj(_result: *mut AMresult) -> *mut am::ObjId {
    unimplemented!()
}

/// # Safety
/// This should be called with a valid pointer to a `AMresult` or NULL
#[no_mangle]
pub unsafe extern "C" fn AMclear(result: *mut AMresult) {
    if !result.is_null() {
        let result: AMresult = *Box::from_raw(result);
        drop(result)
    }
}

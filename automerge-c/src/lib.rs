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
pub enum AmDatatype {
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
pub enum AmStatus {
    CommandOk,
    ObjOk,
    ValuesOk,
    ChangesOk,
    InvalidResult,
    Error,
}

impl fmt::Display for AmDatatype {
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
            None => return AMresult::err("Invalid AMdoc pointer").into(),
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
            None => &AMobj(am::ObjId::Root),
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
                AMresult::err(&format!("Invalid actor '{}'", to_str!(value))).into()
            }
        }
        k => AMresult::err(&format!("Invalid config key '{}'", k)).into(),
    }
}

/// # Safety
/// This should be called with a valid pointer to a `AMdoc`.
/// key="actor" value=(actor id in hex format)
#[no_mangle]
pub unsafe extern "C" fn AMgetActor(_doc: *mut AMdoc) -> *mut AMresult {
    unimplemented!()
}

/// # Safety
/// This should be called with a valid pointer to a `AMresult` or NULL
#[no_mangle]
pub unsafe extern "C" fn AMresultStatus(result: *mut AMresult) -> AmStatus {
  match result.as_mut() {
      Some(AMresult::Ok) => AmStatus::CommandOk,
      Some(AMresult::Error(_)) => AmStatus::Error,
      Some(AMresult::ObjId(_)) => AmStatus::ObjOk,
      Some(AMresult::Values(_)) => AmStatus::ValuesOk,
      Some(AMresult::Changes(_)) => AmStatus::ChangesOk,
      None => AmStatus::InvalidResult,
  }
}

/// # Safety
/// This should be called with a valid pointer to a `AMresult` or NULL
#[no_mangle]
pub unsafe extern "C" fn AMmapSet(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    datatype: AmDatatype,
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
    datatype: AmDatatype,
    value: *const c_void,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result!(doc.set(to_obj!(obj), index, to_value!(value,datatype)))
}

/// # Safety
/// This should be called with a valid pointer to a `AMresult` or NULL
#[no_mangle]
pub unsafe extern "C" fn AMgetObj(_result: *mut AMresult) -> *mut AMobj {
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

/// # Safety
/// This should be called with a valid pointer to a `AMresult` or NULL
#[no_mangle]
pub unsafe extern "C" fn AMerrorMessage(result: *mut AMresult) -> *const c_char {
  match result.as_mut() {
      Some(AMresult::Error(s)) => s.as_ptr(),
      _ => 0 as *const c_char,
  }
}

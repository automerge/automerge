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

/// \ingroup enumerations
/// \brief All data types that a value can be set to.
#[derive(Debug)]
#[repr(u8)]
pub enum AmDataType {
    /// A null value.
    Null,
    /// A boolean value.
    Boolean,
    /// An ordered collection of byte values.
    Bytes,
    /// A CRDT counter value.
    Counter,
    /// A 64-bit floating-point value.
    F64,
    /// A signed integer value.
    Int,
    /// An ordered collection of (index, value) pairs.
    List,
    /// A unordered collection of (key, value) pairs.
    Map,
    /// A UTF-8 string value.
    Str,
    /// An unordered collection of records like in an RDBMS.
    Table,
    /// An ordered collection of (index, value) pairs optimized for characters.
    Text,
    /// A Lamport timestamp value.
    Timestamp,
    /// An unsigned integer value.
    Uint,
}

/// \ingroup enumerations
/// \brief The status of an API call.
#[derive(Debug)]
#[repr(u8)]
pub enum AmStatus {
    /// The command was successful.
    CommandOk,
    /// The result is an object ID.
    ObjOk,
    /// The result is one or more values.
    ValuesOk,
    /// The result is one or more changes.
    ChangesOk,
    /// The result is invalid.
    InvalidResult,
    /// The result was an error.
    Error,
}

impl fmt::Display for AmDataType {
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

/// \class AMobj
/// \brief An object's unique identifier.
#[derive(Clone)]
pub struct AMobj(am::ObjId);

/// \memberof AMdoc
/// \brief Allocates a new `AMdoc` struct and initializes it with defaults.
///
/// \return A pointer to an `AMdoc` struct.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMdestroy()`.
#[no_mangle]
pub extern "C" fn AMcreate() -> *mut AMdoc {
    AMdoc::create(am::Automerge::new()).into()
}

/// \memberof AMdoc
/// \brief Deallocates the storage for an `AMdoc` struct previously
///        allocated by `AMcreate()` or `AMdup()`.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \pre \p doc must be a valid address.
#[no_mangle]
pub unsafe extern "C" fn AMdestroy(doc: *mut AMdoc) {
    if !doc.is_null() {
        let doc: AMdoc = *Box::from_raw(doc);
        drop(doc)
    }
}

/// \memberof AMdoc
/// \brief Allocates storage for an `AMdoc` struct and initializes it by
///        duplicating the `AMdoc` struct pointed to by \p doc.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMdoc` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMdestroy()`.
#[no_mangle]
pub unsafe extern "C" fn AMdup(doc: *mut AMdoc) -> *mut AMdoc {
    let doc = *Box::from_raw(doc);
    let copy = doc.clone();
    std::mem::forget(doc);
    copy.into()
}

/// \memberof AMdoc
/// \brief Sets a configuration property of an `AMdoc` struct.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] key A configuration property's key string.
/// \param[in] value A configuration property's string value or `NULL`.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
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

/// \memberof AMdoc
/// \brief Get an `AMdoc` struct's actor ID value as a hexadecimal string.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing a string value.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
#[no_mangle]
pub unsafe extern "C" fn AMgetActor(_doc: *mut AMdoc) -> *mut AMresult {
    unimplemented!()
}

/// \memberof AMresult
/// \brief Get the status code of an `AMresult` struct.
///
/// \param[in] result A pointer to an `AMresult` struct or `NULL`.
/// \return An `AmStatus` enum tag.
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

/// \memberof AMdoc
/// \brief Set a map object's value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct.
/// \param[in] key A map object's key string.
/// \param[in] data_type An `AmDataType` enum tag matching the actual type that
///            \p value points to.
/// \param[in] value A pointer to the value at \p key or `NULL`.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p obj must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
#[no_mangle]
pub unsafe extern "C" fn AMmapSet(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    data_type: AmDataType,
    value: *const c_void,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result!(doc.set(to_obj!(obj), to_prop!(key), to_value!(value, data_type)))
}

/// \memberof AMdoc
/// \brief Set a list object's value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct.
/// \param[in] index A list object's index number.
/// \param[in] data_type An `AmDataType` enum tag matching the actual type that
///            \p value points to.
/// \param[in] value A pointer to the value at \p index or `NULL`.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p obj must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
#[no_mangle]
pub unsafe extern "C" fn AMlistSet(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    data_type: AmDataType,
    value: *const c_void,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result!(doc.set(to_obj!(obj), index, to_value!(value,data_type)))
}

/// \memberof AMresult
/// \brief Get an `AMresult` struct's `AMobj` struct value.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return A pointer to an `AMobj` struct.
/// \pre \p result must be a valid address.
#[no_mangle]
pub unsafe extern "C" fn AMgetObj(_result: *mut AMresult) -> *mut AMobj {
    unimplemented!()
}

/// \memberof AMresult
/// \brief Deallocates the storage for an `AMresult` struct.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \pre \p result must be a valid address.
#[no_mangle]
pub unsafe extern "C" fn AMclear(result: *mut AMresult) {
    if !result.is_null() {
        let result: AMresult = *Box::from_raw(result);
        drop(result)
    }
}

/// \memberof AMresult
/// \brief Get an `AMresult` struct's error message string.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return A string value or `NULL`.
/// \pre \p result must be a valid address.
#[no_mangle]
pub unsafe extern "C" fn AMerrorMessage(result: *mut AMresult) -> *const c_char {
  match result.as_mut() {
      Some(AMresult::Error(s)) => s.as_ptr(),
      _ => 0 as *const c_char,
  }
}

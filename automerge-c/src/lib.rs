use automerge as am;
use std::{ffi::CStr, os::raw::c_char};

mod doc;
mod result;
mod utils;

use automerge::transaction::Transactable;
use doc::AMdoc;
use result::AMresult;

/// \ingroup enumerations
/// \enum AmObjType
#[repr(u8)]
pub enum AmObjType {
    /// A key-value map.
    Map,
    /// A list.
    List,
    /// A list of Unicode graphemes.
    Text,
}

impl From<AmObjType> for am::ObjType {
    fn from(o: AmObjType) -> Self {
        match o {
            AmObjType::Map => am::ObjType::Map,
            AmObjType::List => am::ObjType::List,
            AmObjType::Text => am::ObjType::Text,
        }
    }
}

/// \enum AmStatus
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

unsafe fn to_str(c: *const c_char) -> String {
    CStr::from_ptr(c).to_string_lossy().to_string()
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

macro_rules! to_obj {
    ($handle:expr) => {{
        match $handle.as_ref() {
            Some(b) => b,
            None => &AMobj(am::ObjId::Root),
        }
    }};
}

fn to_result<R: Into<AMresult>>(r: R) -> *mut AMresult {
    (r.into()).into()
}

/// \struct AMobj
/// \brief An object's unique identifier.
#[derive(Clone)]
pub struct AMobj(am::ObjId);

impl AsRef<am::ObjId> for AMobj {
    fn as_ref(&self) -> &am::ObjId {
        &self.0
    }
}

/// \memberof AMdoc
/// \brief Allocates a new `AMdoc` struct and initializes it with defaults.
///
/// \return A pointer to an `AMdoc` struct.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMdestroy()`.
#[no_mangle]
pub extern "C" fn AMcreate() -> *mut AMdoc {
    AMdoc::create(am::AutoCommit::new()).into()
}

/// \memberof AMdoc
/// \brief Deallocates the storage for an `AMdoc` struct previously
///        allocated by `AMcreate()` or `AMdup()`.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \pre \p doc must be a valid address.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
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
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMdup(doc: *mut AMdoc) -> *mut AMdoc {
    let doc = *Box::from_raw(doc);
    let copy = doc.clone();
    std::mem::forget(doc);
    copy.into()
}

/// \memberof AMdoc
/// \brief Set a configuration property of an `AMdoc` struct.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] key A configuration property's string key.
/// \param[in] value A configuration property's string value or `NULL`.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// key and value must be valid c strings
#[no_mangle]
pub unsafe extern "C" fn AMconfig(
    doc: *mut AMdoc,
    key: *const c_char,
    value: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let key = to_str(key);
    match key.as_str() {
        "actor" => {
            let actor = to_str(value);
            if let Ok(actor) = actor.try_into() {
                doc.set_actor(actor);
                AMresult::Ok.into()
            } else {
                AMresult::err(&format!("Invalid actor '{}'", to_str(value))).into()
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
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetActor(_doc: *mut AMdoc) -> *mut AMresult {
    unimplemented!()
}

/// \memberof AMresult
/// \brief Get the status code of an `AMresult` struct.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return An `AmStatus` enum tag.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
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
/// \brief Set a map object's key to a signed integer value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A string key for the map object identified by \p obj.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapSetInt(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.set(to_obj!(obj), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Set a map object's key to an unsigned integer value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A string key for the map object identified by \p obj.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapSetUint(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    value: u64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.set(to_obj!(obj), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a string value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A string key for the map object identified by \p obj.
/// \param[in] value A string.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapSetStr(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    value: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.set(to_obj!(obj), to_str(key), to_str(value)))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a byte array value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A string key for the map object identified by \p obj.
/// \param[in] value A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p value.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// key must be a c string of the map key to be used
/// value must be a byte array of lenth `count`
#[no_mangle]
pub unsafe extern "C" fn AMmapSetBytes(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    value: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let slice = std::slice::from_raw_parts(value, count);
    let mut vec = Vec::new();
    vec.extend_from_slice(slice);
    to_result(doc.set(to_obj!(obj), to_str(key), vec))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a float value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A string key for the map object identified by \p obj.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapSetF64(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    value: f64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.set(to_obj!(obj), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a CRDT counter value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A string key for the map object identified by \p obj.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapSetCounter(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.set(
        to_obj!(obj),
        to_str(key),
        am::ScalarValue::Counter(value.into()),
    ))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a Lamport timestamp value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A string key for the map object identified by \p obj.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapSetTimestamp(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.set(to_obj!(obj), to_str(key), am::ScalarValue::Timestamp(value)))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a null value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A string key for the map object identified by \p obj.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapSetNull(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.set(to_obj!(obj), to_str(key), ()))
}

/// \memberof AMdoc
/// \brief Set a map object's key to an empty object value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A string key for the map object identified by \p obj.
/// \param[in] objtype An `AmObjType` enum tag.
/// \return A pointer to an `AMresult` struct containing a pointer to an `AMobj` struct.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapSetObject(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
    objtype: AmObjType,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.set_object(to_obj!(obj), to_str(key), objtype.into()))
}

/// \memberof AMdoc
/// \brief Set a list object's index to a signed integer value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing no value.
/// \pre \p doc must be a valid address.
/// \pre \p obj must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// value must be a pointer to data of the type specified in data_type
#[no_mangle]
pub unsafe extern "C" fn AMlistSetInt(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj = to_obj!(obj);
    if insert {
        to_result(doc.insert(obj, index, value))
    } else {
        to_result(doc.set(obj, index, value))
    }
}

/// \memberof AMresult
/// \brief Get an `AMresult` struct's `AMobj` struct value.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return A pointer to an `AMobj` struct.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
#[no_mangle]
pub unsafe extern "C" fn AMgetObj(_result: *mut AMresult) -> *mut AMobj {
    unimplemented!()
}

/// \memberof AMresult
/// \brief Deallocates the storage for an `AMresult` struct.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
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
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
#[no_mangle]
pub unsafe extern "C" fn AMerrorMessage(result: *mut AMresult) -> *const c_char {
    match result.as_mut() {
        Some(AMresult::Error(s)) => s.as_ptr(),
        _ => std::ptr::null::<c_char>(),
    }
}

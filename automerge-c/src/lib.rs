use automerge as am;
use std::{ffi::CStr, ffi::CString, os::raw::c_char};

mod doc;
mod result;
mod utils;

use automerge::transaction::Transactable;
use doc::AMdoc;
use result::{AMobj, AMresult, AMvalue};

/// \ingroup enumerations
/// \enum AMobjType
/// \brief The type of an object value.
#[repr(u8)]
pub enum AMobjType {
    /// A list.
    List = 1,
    /// A key-value map.
    Map,
    /// A list of Unicode graphemes.
    Text,
}

impl From<AMobjType> for am::ObjType {
    fn from(o: AMobjType) -> Self {
        match o {
            AMobjType::Map => am::ObjType::Map,
            AMobjType::List => am::ObjType::List,
            AMobjType::Text => am::ObjType::Text,
        }
    }
}

/// \ingroup enumerations
/// \enum AMstatus
/// \brief The status of an API call.
#[derive(Debug)]
#[repr(u8)]
pub enum AMstatus {
    /// Success.
    /// \note This tag is unalphabetized so that `0` indicates success.
    Ok,
    /// Failure due to an error.
    Error,
    /// Failure due to an invalid result.
    InvalidResult,
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

macro_rules! to_obj_id {
    ($handle:expr) => {{
        match $handle.as_ref() {
            Some(am_obj) => am::ObjId::from(am_obj),
            None => am::ROOT,
        }
    }};
}

fn to_result<R: Into<AMresult>>(r: R) -> *mut AMresult {
    (r.into()).into()
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
/// \param[in] key A configuration property's UTF-8 string key.
/// \param[in] value A configuration property's UTF-8 string value or `NULL`.
/// \return A pointer to an `AMresult` struct containing nothing.
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
                AMresult::Nothing.into()
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
/// \return A pointer to an `AMresult` struct containing a UTF-8 string value.
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
/// \return An `AMstatus` enum tag.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultStatus(result: *mut AMresult) -> AMstatus {
    match result.as_mut() {
        Some(AMresult::Error(_)) => AMstatus::Error,
        None => AMstatus::InvalidResult,
        _ => AMstatus::Ok,
    }
}

/// \memberof AMresult
/// \brief Get the size of an `AMresult` struct.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return The count of values in \p result.
/// \pre \p result must be a valid address.
#[no_mangle]
pub unsafe extern "C" fn AMresultSize(result: *mut AMresult) -> usize {
    if let Some(result) = result.as_mut() {
        match result {
            AMresult::ActorId(_) | AMresult::ObjId(_) => 1,
            AMresult::Changes(changes) => changes.len(),
            AMresult::Error(_) | AMresult::Nothing => 0,
            AMresult::Scalars(vec, _) => vec.len(),
        }
    } else {
        0
    }
}

/// \memberof AMresult
/// \brief Get a value from an `AMresult` struct.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \param[in] index The index of a value.
/// \return An `AMvalue` struct.
/// \pre \p result must be a valid address.
/// \pre `0 <=` \p index `<=` AMresultSize() for \p result.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultValue(result: *mut AMresult, index: usize) -> AMvalue {
    let mut value = AMvalue::Nothing;
    if let Some(result) = result.as_mut() {
        match result {
            AMresult::ActorId(actor_id) => {
                if index == 0 {
                    value = AMvalue::ActorId(actor_id.into());
                }
            }
            AMresult::Changes(_) => {}
            AMresult::Error(_) => {}
            AMresult::ObjId(obj_id) => {
                if index == 0 {
                    value = AMvalue::Obj((&*obj_id).into());
                }
            }
            AMresult::Nothing => (),
            AMresult::Scalars(vec, hosted_str) => {
                if let Some(element) = vec.get(index) {
                    match element {
                        am::Value::Scalar(scalar) => match scalar {
                            am::ScalarValue::Boolean(flag) => {
                                value = AMvalue::Boolean(*flag as i8);
                            }
                            am::ScalarValue::Bytes(bytes) => {
                                value = AMvalue::Bytes(bytes.into());
                            }
                            am::ScalarValue::Counter(counter) => {
                                value = AMvalue::Counter(counter.into());
                            }
                            am::ScalarValue::F64(float) => {
                                value = AMvalue::F64(*float);
                            }
                            am::ScalarValue::Int(int) => {
                                value = AMvalue::Int(*int);
                            }
                            am::ScalarValue::Null => {
                                value = AMvalue::Null;
                            }
                            am::ScalarValue::Str(smol_str) => {
                                *hosted_str = CString::new(smol_str.to_string()).ok();
                                if let Some(c_str) = hosted_str {
                                    value = AMvalue::Str(c_str.as_ptr());
                                }
                            }
                            am::ScalarValue::Timestamp(timestamp) => {
                                value = AMvalue::Timestamp(*timestamp);
                            }
                            am::ScalarValue::Uint(uint) => {
                                value = AMvalue::Uint(*uint);
                            }
                        },
                        // \todo Confirm that an object value should be ignored
                        //       when there's no object ID variant.
                        am::Value::Object(_) => (),
                    }
                }
            }
        }
    };
    value
}

/// \memberof AMdoc
/// \brief Set a map object's key to a signed integer value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
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
    to_result(doc.set(to_obj_id!(obj), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Set a map object's key to an unsigned integer value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing nothing.
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
    to_result(doc.set(to_obj_id!(obj), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a UTF-8 string value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \pre \p value must be a valid address.
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
    to_result(doc.set(to_obj_id!(obj), to_str(key), to_str(value)))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a byte array value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p value.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \pre \p value must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p value.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// value must be a byte array of length `count`
/// key must be a c string of the map key to be used
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
    to_result(doc.set(to_obj_id!(obj), to_str(key), vec))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a float value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing nothing.
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
    to_result(doc.set(to_obj_id!(obj), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a CRDT counter value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
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
        to_obj_id!(obj),
        to_str(key),
        am::ScalarValue::Counter(value.into()),
    ))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a Lamport timestamp value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
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
    to_result(doc.set(
        to_obj_id!(obj),
        to_str(key),
        am::ScalarValue::Timestamp(value),
    ))
}

/// \memberof AMdoc
/// \brief Set a map object's key to a null value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \return A pointer to an `AMresult` struct containing nothing.
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
    to_result(doc.set(to_obj_id!(obj), to_str(key), ()))
}

/// \memberof AMdoc
/// \brief Set a map object's key to an empty object value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] obj_type An `AMobjType` enum tag.
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
    obj_type: AMobjType,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.set_object(to_obj_id!(obj), to_str(key), obj_type.into()))
}

/// \memberof AMdoc
/// \brief Get the value at a list object's index.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \return A pointer to an `AMresult` struct.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistGet(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.value(to_obj_id!(obj), index))
}

/// \memberof AMdoc
/// \brief Get the value at a map object's key.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \return A pointer to an `AMresult` struct.
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
pub unsafe extern "C" fn AMmapGet(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    key: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.value(to_obj_id!(obj), to_str(key)))
}

/// \memberof AMdoc
/// \brief Set a list object's index to a byte array value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p value.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \pre \p value must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p value.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// value must be a byte array of length `count`
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMlistSetBytes(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
    value: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj);
    let slice = std::slice::from_raw_parts(value, count);
    let mut vec = Vec::new();
    vec.extend_from_slice(slice);
    to_result(if insert {
        doc.insert(obj_id, index, vec)
    } else {
        doc.set(obj_id, index, vec)
    })
}

/// \memberof AMdoc
/// \brief Set a list object's index to a CRDT counter value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistSetCounter(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj);
    let value = am::ScalarValue::Counter(value.into());
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.set(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Set a list object's index to a float value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistSetF64(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
    value: f64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.set(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Set a list object's index to a signed integer value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistSetInt(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.set(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Set a list object's index to a null value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistSetNull(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj);
    let value = ();
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.set(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Set a list object's index to an empty object value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] obj_type An `AMobjType` enum tag.
/// \return A pointer to an `AMresult` struct containing a pointer to an `AMobj` struct.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistSetObject(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
    obj_type: AMobjType,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj);
    let value = obj_type.into();
    to_result(if insert {
        doc.insert_object(&obj_id, index, value)
    } else {
        doc.set_object(&obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Set a list object's index to a UTF-8 string value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \pre \p value must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
/// value must be a pointer to a valid address.
#[no_mangle]
pub unsafe extern "C" fn AMlistSetStr(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
    value: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj);
    let value = to_str(value);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.set(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Set a list object's index to a Lamport timestamp value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistSetTimestamp(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj);
    let value = am::ScalarValue::Timestamp(value);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.set(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Set a list object's index to an unsigned integer value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMclear()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj must be a pointer to a valid AMobj or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistSetUint(
    doc: *mut AMdoc,
    obj: *mut AMobj,
    index: usize,
    insert: bool,
    value: u64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.set(obj_id, index, value)
    })
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
/// \return A UTF-8 string value or `NULL`.
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

/// \memberof AMdoc
/// \brief Get the size of an `AMobj` struct.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj A pointer to an `AMobj` struct or `NULL`.
/// \return The count of values in \p obj.
/// \pre \p doc must be a valid address.
#[no_mangle]
pub unsafe extern "C" fn AMobjSize(doc: *const AMdoc, obj: *const AMobj) -> usize {
    if let Some(doc) = doc.as_ref() {
        doc.length(to_obj_id!(obj))
    } else {
        0
    }
}

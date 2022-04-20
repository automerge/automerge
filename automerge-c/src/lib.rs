use automerge as am;
use smol_str::SmolStr;
use std::{borrow::Cow, ffi::CStr, ffi::CString, os::raw::c_char};

mod doc;
mod result;
mod utils;

use automerge::transaction::Transactable;
use doc::AMdoc;
use result::{AMobjId, AMresult, AMvalue};

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
            Some(obj_id) => obj_id,
            None => &am::ROOT,
        }
    }};
}

fn to_result<'a, R: Into<AMresult<'a>>>(r: R) -> *mut AMresult<'a> {
    (r.into()).into()
}

/// \memberof AMdoc
/// \brief Allocates a new `AMdoc` struct and initializes it with defaults.
///
/// \return A pointer to an `AMdoc` struct.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeDoc()`.
#[no_mangle]
pub extern "C" fn AMallocDoc() -> *mut AMdoc {
    AMdoc::new(am::AutoCommit::new()).into()
}

/// \memberof AMdoc
/// \brief Deallocates the storage for an `AMdoc` struct previously
///        allocated by `AMallocDoc()` or `AMdup()`.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \pre \p doc must be a valid address.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMfreeDoc(doc: *mut AMdoc) {
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
///          with `AMfreeDoc()`.
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
/// \brief Gets an `AMdoc` struct's actor ID value as an array of bytes.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an `AMbyteSpan`.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetActor<'a>(doc: *mut AMdoc) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(Ok(doc.get_actor().clone()))
}

/// \memberof AMdoc
/// \brief Gets an `AMdoc` struct's actor ID value as a hexadecimal string.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing a `char const*`.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetActorHex<'a>(doc: *mut AMdoc) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let hex_str = doc.get_actor().to_hex_string();
    let value = am::Value::Scalar(Cow::Owned(am::ScalarValue::Str(SmolStr::new(hex_str))));
    to_result(Ok(value))
}

/// \memberof AMdoc
/// \brief Puts an array of bytes as the actor ID value of an `AMdoc` struct.  .
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] value A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p value.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p value must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p value.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// value must be a byte array of length `count`
#[no_mangle]
pub unsafe extern "C" fn AMsetActor<'a>(
    doc: *mut AMdoc,
    value: *const u8,
    count: usize,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let slice = std::slice::from_raw_parts(value, count);
    doc.set_actor(am::ActorId::from(slice));
    to_result(Ok(()))
}

/// \memberof AMdoc
/// \brief Puts a hexadecimal string as the actor ID value of an `AMdoc` struct.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] hex_str A string of hexadecimal characters.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p hex_str must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// hex_str must be a null-terminated array of `c_char`
#[no_mangle]
pub unsafe extern "C" fn AMsetActorHex<'a>(
    doc: *mut AMdoc,
    hex_str: *const c_char,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let slice = std::slice::from_raw_parts(hex_str as *const u8, libc::strlen(hex_str));
    to_result(match hex::decode(slice) {
        Ok(vec) => {
            doc.set_actor(vec.into());
            Ok(())
        }
        Err(error) => Err(am::AutomergeError::HexDecode(error)),
    })
}

/// \memberof AMresult
/// \brief Gets the status code of an `AMresult` struct.
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
/// \brief Gets the size of an `AMresult` struct.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return The count of values in \p result.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
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
/// \brief Gets a value from an `AMresult` struct.
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
                    value = AMvalue::ObjId(obj_id);
                }
            }
            AMresult::Nothing => (),
            AMresult::Scalars(vec, hosted_str) => {
                if let Some(element) = vec.get(index) {
                    match element {
                        am::Value::Scalar(scalar) => match scalar.as_ref() {
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
/// \brief Puts a signed integer as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapPutInt<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
    value: i64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Puts an unsigned integer as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapPutUint<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
    value: u64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Puts a UTF-8 string as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \pre \p value must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
/// value must be a null-terminated array of `c_char`
#[no_mangle]
pub unsafe extern "C" fn AMmapPutStr<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
    value: *const c_char,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), to_str(value)))
}

/// \memberof AMdoc
/// \brief Puts an array of bytes as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p value.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \pre \p value must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p value.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
/// value must be a byte array of length `count`
#[no_mangle]
pub unsafe extern "C" fn AMmapPutBytes<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
    value: *const u8,
    count: usize,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let slice = std::slice::from_raw_parts(value, count);
    let mut vec = Vec::new();
    vec.extend_from_slice(slice);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), vec))
}

/// \memberof AMdoc
/// \brief Puts a float as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapPutF64<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
    value: f64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Puts a CRDT counter as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapPutCounter<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
    value: i64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.put(
        to_obj_id!(obj_id),
        to_str(key),
        am::ScalarValue::Counter(value.into()),
    ))
}

/// \memberof AMdoc
/// \brief Puts a Lamport timestamp as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapPutTimestamp<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
    value: i64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.put(
        to_obj_id!(obj_id),
        to_str(key),
        am::ScalarValue::Timestamp(value),
    ))
}

/// \memberof AMdoc
/// \brief Puts null as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned p ointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapPutNull<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), ()))
}

/// \memberof AMdoc
/// \brief Puts an empty object as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \param[in] obj_type An `AMobjIdType` enum tag.
/// \return A pointer to an `AMresult` struct containing a pointer to an `AMobjId` struct.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapPutObject<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
    obj_type: AMobjType,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.put_object(to_obj_id!(obj_id), to_str(key), obj_type.into()))
}

/// \memberof AMdoc
/// \brief Gets the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj.
/// \return A pointer to an `AMresult` struct.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistGet<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.get(to_obj_id!(obj_id), index))
}

/// \memberof AMdoc
/// \brief Gets the value for a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj.
/// \return A pointer to an `AMresult` struct.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapGet<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    key: *const c_char,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    to_result(doc.get(to_obj_id!(obj_id), to_str(key)))
}

/// \memberof AMdoc
/// \brief Puts an array of bytes as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p value.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \pre \p value must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p value.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// value must be a byte array of length `count`
#[no_mangle]
pub unsafe extern "C" fn AMlistPutBytes<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
    insert: bool,
    value: *const u8,
    count: usize,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let slice = std::slice::from_raw_parts(value, count);
    let mut vec = Vec::new();
    vec.extend_from_slice(slice);
    to_result(if insert {
        doc.insert(obj_id, index, vec)
    } else {
        doc.put(obj_id, index, vec)
    })
}

/// \memberof AMdoc
/// \brief Puts a CRDT counter as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutCounter<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let value = am::ScalarValue::Counter(value.into());
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a float as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutF64<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
    insert: bool,
    value: f64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a signed integer as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutInt<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts null as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutNull<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
    insert: bool,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let value = ();
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts an empty object as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] obj_type An `AMobjIdType` enum tag.
/// \return A pointer to an `AMresult` struct containing a pointer to an `AMobjId` struct.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutObject<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
    insert: bool,
    obj_type: AMobjType,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let value = obj_type.into();
    to_result(if insert {
        doc.insert_object(obj_id, index, value)
    } else {
        doc.put_object(&obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a UTF-8 string as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \pre \p value must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// value must be a null-terminated array of `c_char`
#[no_mangle]
pub unsafe extern "C" fn AMlistPutStr<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
    insert: bool,
    value: *const c_char,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let value = to_str(value);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a Lamport timestamp as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutTimestamp<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let value = am::ScalarValue::Timestamp(value);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts an unsigned integer as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj.
/// \param[in] insert A flag to insert \p value before \p index instead of writing \p value over \p index.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing nothing.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutUint<'a>(
    doc: *mut AMdoc,
    obj_id: *mut AMobjId,
    index: usize,
    insert: bool,
    value: u64,
) -> *mut AMresult<'a> {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
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
pub unsafe extern "C" fn AMfreeResult(result: *mut AMresult) {
    if !result.is_null() {
        let result: AMresult = *Box::from_raw(result);
        drop(result)
    }
}

/// \memberof AMresult
/// \brief Gets an `AMresult` struct's error message string.
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
/// \brief Gets the size of an `AMobjId` struct.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \return The count of values in \p obj.
/// \pre \p doc must be a valid address.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMobjSize(doc: *const AMdoc, obj_id: *const AMobjId) -> usize {
    if let Some(doc) = doc.as_ref() {
        doc.length(to_obj_id!(obj_id))
    } else {
        0
    }
}

/// \memberof AMdoc
/// \brief Deallocates the storage for an `AMobjId` struct.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \pre \p doc must be a valid address.
/// \pre \p obj_id must be a valid address.
/// \note An `AMobjId` struct is automatically deallocated along with its owning
///       `AMdoc` struct, this function just enables an `AMobjId` struct to be
///       deallocated sooner than that.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMfreeObjId(doc: *mut AMdoc, obj_id: *const AMobjId) {
    if let Some(doc) = doc.as_mut() {
        if let Some(obj_id) = obj_id.as_ref() {
            doc.drop_obj_id(obj_id);
        };
    };
}

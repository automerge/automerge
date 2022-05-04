use automerge as am;
use smol_str::SmolStr;
use std::{borrow::Cow, ffi::CStr, ffi::CString, os::raw::c_char};

mod byte_span;
mod change_hashes;
mod changes;
mod doc;
mod result;

use automerge::transaction::{CommitOptions, Transactable};

use byte_span::AMbyteSpan;
use change_hashes::AMchangeHashes;
use changes::{AMchange, AMchanges};
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

fn to_result<R: Into<AMresult>>(r: R) -> *mut AMresult {
    (r.into()).into()
}

/// \memberof AMdoc
/// \brief Allocates a new `AMdoc` struct and initializes it with defaults.
///
/// \return A pointer to an `AMdoc` struct.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeDoc()`.
#[no_mangle]
pub extern "C" fn AMalloc() -> *mut AMdoc {
    AMdoc::new(am::AutoCommit::new()).into()
}

/// \memberof AMdoc
/// \brief Commits the current operations on \p doc with an optional message
///        and/or time override as seconds since the epoch.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] message A UTF-8 string or `NULL`.
/// \param[in] time A pointer to a `time_t` value or `NULL`.
/// \return A pointer to an `AMresult` struct containing a change hash as an
///         `AMbyteSpan` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMcommit(
    doc: *mut AMdoc,
    message: *const c_char,
    time: *const libc::time_t,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let mut options = CommitOptions::default();
    if !message.is_null() {
        options.set_message(to_str(message));
    }
    if let Some(time) = time.as_ref() {
        options.set_time(*time);
    }
    to_result(doc.commit_with::<()>(options))
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
/// \brief Deallocates the storage for an `AMdoc` struct previously
///        allocated by `AMalloc()` or `AMdup()`.
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
/// \brief Loads the compact form of an incremental save of an `AMdoc` struct
///        into \p doc.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes in \p src to load.
/// \return A pointer to an `AMresult` struct containing the number of
///         operations loaded from \p src.
/// \pre \p doc must be a valid address.
/// \pre \p src must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p src.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// src must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMload(doc: *mut AMdoc, src: *const u8, count: usize) -> *mut AMresult {
    let doc = to_doc!(doc);
    let mut data = Vec::new();
    data.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(doc.load_incremental(&data))
}

/// \memberof AMdoc
/// \brief Applies all of the changes in \p src which are not in \p dest to
///        \p dest.
///
/// \param[in] dest A pointer to an `AMdoc` struct.
/// \param[in] src A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an `AMchangeHashes`
///         struct.
/// \pre \p dest must be a valid address.
/// \pre \p src must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// dest must be a pointer to a valid AMdoc
/// src must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMmerge(dest: *mut AMdoc, src: *mut AMdoc) -> *mut AMresult {
    let dest = to_doc!(dest);
    to_result(dest.merge(to_doc!(src)))
}

/// \memberof AMdoc
/// \brief Saves the entirety of \p doc into a compact form.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an array of bytes as
///         an `AMbyteSpan` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMsave(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(Ok(doc.save()))
}
/// \memberof AMdoc
/// \brief Gets an `AMdoc` struct's actor ID value as an array of bytes.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an actor ID as an
///         `AMbyteSpan` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetActor(doc: *mut AMdoc) -> *mut AMresult {
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
pub unsafe extern "C" fn AMgetActorHex(doc: *mut AMdoc) -> *mut AMresult {
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
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p value must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p value.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// value must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMsetActor(
    doc: *mut AMdoc,
    value: *const u8,
    count: usize,
) -> *mut AMresult {
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
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMsetActorHex(doc: *mut AMdoc, hex_str: *const c_char) -> *mut AMresult {
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
            AMresult::ChangeHashes(change_hashes) => change_hashes.len(),
            AMresult::Changes(changes) => changes.len(),
            AMresult::Error(_) | AMresult::Void => 0,
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
pub unsafe extern "C" fn AMresultValue<'a>(result: *mut AMresult, index: usize) -> AMvalue<'a> {
    let mut value = AMvalue::Void;
    if let Some(result) = result.as_mut() {
        match result {
            AMresult::ActorId(actor_id) => {
                if index == 0 {
                    value = AMvalue::ActorId(actor_id.into());
                }
            }
            AMresult::ChangeHashes(change_hashes) => {
                value = AMvalue::ChangeHashes(AMchangeHashes::new(change_hashes));
            }
            AMresult::Changes(changes) => {
                value = AMvalue::Changes(AMchanges::new(changes));
            }
            AMresult::Error(_) => {}
            AMresult::ObjId(obj_id) => {
                if index == 0 {
                    value = AMvalue::ObjId(obj_id);
                }
            }
            AMresult::Scalars(vec, hosted_str) => {
                if let Some(element) = vec.get(index) {
                    match element {
                        am::Value::Scalar(scalar) => match scalar.as_ref() {
                            am::ScalarValue::Boolean(flag) => {
                                value = AMvalue::Boolean(*flag);
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
            AMresult::Void => (),
        }
    };
    value
}

/// \memberof AMdoc
/// \brief Deletes a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMmapDelete(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.delete(to_obj_id!(obj_id), to_str(key)))
}

/// \memberof AMdoc
/// \brief Puts a boolean as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A boolean.
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMmapPutBool(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    value: bool,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Puts a signed integer as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMmapPutInt(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Puts an unsigned integer as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMmapPutUint(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    value: u64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Puts a UTF-8 string as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMmapPutStr(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    value: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), to_str(value)))
}

/// \memberof AMdoc
/// \brief Puts an array of bytes as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p value.
/// \return A pointer to an `AMresult` struct containing a void.
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
/// value must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMmapPutBytes(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    value: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let mut vec = Vec::new();
    vec.extend_from_slice(std::slice::from_raw_parts(value, count));
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), vec))
}

/// \memberof AMdoc
/// \brief Puts a float as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMmapPutF64(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    value: f64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Puts a CRDT counter as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMmapPutCounter(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    value: i64,
) -> *mut AMresult {
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
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMmapPutTimestamp(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    value: i64,
) -> *mut AMresult {
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
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \return A pointer to an `AMresult` struct containing a void.
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
pub unsafe extern "C" fn AMmapPutNull(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), ()))
}

/// \memberof AMdoc
/// \brief Puts an empty object as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
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
pub unsafe extern "C" fn AMmapPutObject(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    obj_type: AMobjType,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.put_object(to_obj_id!(obj_id), to_str(key), obj_type.into()))
}

/// \memberof AMdoc
/// \brief Gets the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index within the list object identified by \p obj_id.
/// \return A pointer to an `AMresult` struct.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistGet(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.get(to_obj_id!(obj_id), index))
}

/// \memberof AMdoc
/// \brief Gets the value for a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
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
pub unsafe extern "C" fn AMmapGet(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.get(to_obj_id!(obj_id), to_str(key)))
}

/// \memberof AMdoc
/// \brief Deletes an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistDelete(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.delete(to_obj_id!(obj_id), index))
}

/// \memberof AMdoc
/// \brief Puts a boolean as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A boolean.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutBool(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: bool,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let value = am::ScalarValue::Boolean(value);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts an array of bytes as the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p value.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \pre \p value must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p value.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// value must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMlistPutBytes(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let mut vec = Vec::new();
    vec.extend_from_slice(std::slice::from_raw_parts(value, count));
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
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutCounter(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
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
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutF64(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: f64,
) -> *mut AMresult {
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
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutInt(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
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
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutNull(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
) -> *mut AMresult {
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
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] obj_type An `AMobjIdType` enum tag.
/// \return A pointer to an `AMresult` struct containing a pointer to an `AMobjId` struct.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutObject(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    obj_type: AMobjType,
) -> *mut AMresult {
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
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
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
pub unsafe extern "C" fn AMlistPutStr(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: *const c_char,
) -> *mut AMresult {
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
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutTimestamp(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
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
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistPutUint(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: u64,
) -> *mut AMresult {
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
/// \brief Gets the size of an object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \return The count of values in the object identified by \p obj_id.
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
/// \brief Gets the historical size of an object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] change A pointer to an `AMchange` struct or `NULL`.
/// \return The count of values in the object identified by \p obj_id at
///         \p change.
/// \pre \p doc must be a valid address.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// change must be a pointer to a valid AMchange or NULL
#[no_mangle]
pub unsafe extern "C" fn AMobjSizeAt(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    change: *const AMchange,
) -> usize {
    if let Some(doc) = doc.as_ref() {
        if let Some(change) = change.as_ref() {
            let change: &am::Change = change.as_ref();
            let change_hashes = vec![change.hash];
            return doc.length_at(to_obj_id!(obj_id), &change_hashes);
        }
    };
    0
}

/// \memberof AMchange
/// \brief Gets the change hash within an `AMchange` struct.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A change hash as an `AMbyteSpan` struct.
/// \pre \p change must be a valid address.
/// \internal
///
/// # Safety
/// change must be a pointer to a valid AMchange
#[no_mangle]
pub unsafe extern "C" fn AMgetChangeHash(change: *const AMchange) -> AMbyteSpan {
    match change.as_ref() {
        Some(change) => change.into(),
        None => AMbyteSpan::default(),
    }
}

/// \memberof AMdoc
/// \brief Gets the changes added to \p doc by their respective hashes.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] have_deps A pointer to an `AMchangeHashes` struct or `NULL`.
/// \return A pointer to an `AMresult` struct containing an `AMchanges` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned pointer must be deallocated
///          with `AMfreeResult()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetChanges(
    doc: *mut AMdoc,
    have_deps: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let empty_deps = Vec::<am::ChangeHash>::new();
    let have_deps = match have_deps.as_ref() {
        Some(have_deps) => have_deps.as_ref(),
        None => &empty_deps,
    };
    to_result(Ok(doc.get_changes(have_deps)))
}

/// \memberof AMchange
/// \brief Gets the message within an `AMchange` struct.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A UTF-8 string or `NULL`.
/// \pre \p change must be a valid address.
/// \internal
///
/// # Safety
/// change must be a pointer to a valid AMchange
#[no_mangle]
pub unsafe extern "C" fn AMgetMessage(change: *const AMchange) -> *const c_char {
    if let Some(change) = change.as_ref() {
        if let Some(c_message) = change.c_message() {
            return c_message.as_ptr();
        }
    }
    std::ptr::null::<c_char>()
}

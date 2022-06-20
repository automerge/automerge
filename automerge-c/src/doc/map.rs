use automerge as am;
use automerge::transaction::Transactable;
use std::os::raw::c_char;

use crate::doc::utils::to_str;
use crate::doc::{to_doc, to_obj_id, AMdoc};
use crate::obj::{AMobjId, AMobjType};
use crate::result::{to_result, AMresult};

/// \memberof AMdoc
/// \brief Deletes a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Gets the value for a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \return A pointer to an `AMresult` struct.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Increments a counter for a key in a map object by the given value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
#[no_mangle]
pub unsafe extern "C" fn AMmapIncrement(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.increment(to_obj_id!(obj_id), to_str(key), value))
}

/// \memberof AMdoc
/// \brief Puts a boolean as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A boolean.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Puts a sequence of bytes as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p src.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \pre \p src must be a valid address.
/// \pre `0 <=` \p count `<=` size of \p src.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// key must be a c string of the map key to be used
/// src must be a byte array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMmapPutBytes(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: *const c_char,
    src: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let mut vec = Vec::new();
    vec.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(doc.put(to_obj_id!(obj_id), to_str(key), vec))
}

/// \memberof AMdoc
/// \brief Puts a CRDT counter as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Puts null as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] obj_type An `AMobjIdType` enum tag.
/// \return A pointer to an `AMresult` struct containing a pointer to an `AMobjId` struct.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Puts a float as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Puts a signed integer as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Puts a UTF-8 string as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \pre \p value must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Puts a Lamport timestamp as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Puts an unsigned integer as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string key for the map object identified by \p obj_id.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p key must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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

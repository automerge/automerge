use automerge as am;
use automerge::transaction::Transactable;
use std::os::raw::c_char;

use crate::doc::{to_doc, to_doc_const, to_obj_id, to_str, AMdoc};
use crate::obj::{AMobjId, AMobjType};
use crate::result::{to_result, AMresult};

/// \memberof AMdoc
/// \brief Deletes an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Gets the value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index within the list object identified by \p obj_id.
/// \return A pointer to an `AMresult` struct.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistGet(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    index: usize,
) -> *mut AMresult {
    let doc = to_doc_const!(doc);
    to_result(doc.get(to_obj_id!(obj_id), index))
}

/// \memberof AMdoc
/// \brief Increments a counter at an index in a list object by the given
///        value.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMlistIncrement(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.increment(to_obj_id!(obj_id), index, value))
}

/// \memberof AMdoc
/// \brief Puts a boolean as the value at an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A boolean.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \brief Puts a sequence of bytes as the value at an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p src before \p index instead of
///            writing \p src over \p index.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p src.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \pre \p src must be a valid address.
/// \pre `0 <=` \p count `<=` size of \p src.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// src must be a byte array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMlistPutBytes(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    src: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let mut vec = Vec::new();
    vec.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(if insert {
        doc.insert(obj_id, index, vec)
    } else {
        doc.put(obj_id, index, vec)
    })
}

/// \memberof AMdoc
/// \brief Puts a CRDT counter as the value at an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] obj_type An `AMobjIdType` enum tag.
/// \return A pointer to an `AMresult` struct containing a pointer to an `AMobjId` struct.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \pre \p value must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre `0 <=` \p index `<=` length of the list object identified by \p obj_id.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
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

use automerge as am;
use automerge::transaction::Transactable;
use automerge::ReadDoc;

use crate::byte_span::{to_str, AMbyteSpan};
use crate::change_hashes::AMchangeHashes;
use crate::doc::{to_doc, to_doc_mut, to_obj_id, AMdoc};
use crate::obj::{to_obj_type, AMobjId, AMobjType};
use crate::result::{to_result, AMresult};

pub mod item;
pub mod items;

/// \memberof AMdoc
/// \brief Deletes a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapDelete(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.delete(to_obj_id!(obj_id), key))
}

/// \memberof AMdoc
/// \brief Gets the current or historical value for a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for a historical
///                  value or `NULL` for the current value.
/// \return A pointer to an `AMresult` struct that doesn't contain a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapGet(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let key = to_str!(key);
    match heads.as_ref() {
        None => to_result(doc.get(obj_id, key)),
        Some(heads) => to_result(doc.get_at(obj_id, key, heads.as_ref())),
    }
}

/// \memberof AMdoc
/// \brief Gets all of the historical values for a key in a map object until
///        its current one or a specific one.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for a historical
///                  last value or `NULL` for the current last value.
/// \return A pointer to an `AMresult` struct containing an `AMobjItems` struct.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapGetAll(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let key = to_str!(key);
    match heads.as_ref() {
        None => to_result(doc.get_all(obj_id, key)),
        Some(heads) => to_result(doc.get_all_at(obj_id, key, heads.as_ref())),
    }
}

/// \memberof AMdoc
/// \brief Increments a counter for a key in a map object by the given value.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapIncrement(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.increment(to_obj_id!(obj_id), key, value))
}

/// \memberof AMdoc
/// \brief Puts a boolean as the value of a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A boolean.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapPutBool(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    value: bool,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.put(to_obj_id!(obj_id), key, value))
}

/// \memberof AMdoc
/// \brief Puts a sequence of bytes as the value of a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p src.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \pre \p src `!= NULL`.
/// \pre `0 <` \p count `<= sizeof(`\p src`)`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// src must be a byte array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMmapPutBytes(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    val: AMbyteSpan,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    let mut vec = Vec::new();
    vec.extend_from_slice(std::slice::from_raw_parts(val.src, val.count));
    to_result(doc.put(to_obj_id!(obj_id), key, vec))
}

/// \memberof AMdoc
/// \brief Puts a CRDT counter as the value of a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapPutCounter(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.put(
        to_obj_id!(obj_id),
        key,
        am::ScalarValue::Counter(value.into()),
    ))
}

/// \memberof AMdoc
/// \brief Puts null as the value of a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapPutNull(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.put(to_obj_id!(obj_id), key, ()))
}

/// \memberof AMdoc
/// \brief Puts an empty object as the value of a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] obj_type An `AMobjIdType` enum tag.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMobjId` struct.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \pre \p obj_type != `AM_OBJ_TYPE_VOID`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapPutObject(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    obj_type: AMobjType,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.put_object(to_obj_id!(obj_id), key, to_obj_type!(obj_type)))
}

/// \memberof AMdoc
/// \brief Puts a float as the value of a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapPutF64(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    value: f64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.put(to_obj_id!(obj_id), key, value))
}

/// \memberof AMdoc
/// \brief Puts a signed integer as the value of a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapPutInt(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.put(to_obj_id!(obj_id), key, value))
}

/// \memberof AMdoc
/// \brief Puts a UTF-8 string as the value of a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A UTF-8 string view as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapPutStr(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    value: AMbyteSpan,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    to_result(doc.put(to_obj_id!(obj_id), to_str!(key), to_str!(value)))
}

/// \memberof AMdoc
/// \brief Puts a *nix timestamp (milliseconds) as the value of a key in a map
///        object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapPutTimestamp(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.put(to_obj_id!(obj_id), key, am::ScalarValue::Timestamp(value)))
}

/// \memberof AMdoc
/// \brief Puts an unsigned integer as the value of a key in a map object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p key `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapPutUint(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    value: u64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.put(to_obj_id!(obj_id), key, value))
}

/// \memberof AMdoc
/// \brief Gets the current or historical keys and values of the map object
///        within the given range.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] begin The first key in a subrange or `AMstr(NULL)` to indicate the
///                  absolute first key.
/// \param[in] end The key one past the last key in a subrange or `AMstr(NULL)` to
///                indicate one past the absolute last key.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for historical
///                  keys and values or `NULL` for current keys and values.
/// \return A pointer to an `AMresult` struct containing an `AMmapItems`
///         struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapRange(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    begin: AMbyteSpan,
    end: AMbyteSpan,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    match (begin.is_null(), end.is_null()) {
        (false, false) => {
            let (begin, end) = (to_str!(begin).to_string(), to_str!(end).to_string());
            if begin > end {
                return AMresult::err(&format!("Invalid range [{}-{})", begin, end)).into();
            };
            let bounds = begin..end;
            if let Some(heads) = heads.as_ref() {
                to_result(doc.map_range_at(obj_id, bounds, heads.as_ref()))
            } else {
                to_result(doc.map_range(obj_id, bounds))
            }
        }
        (false, true) => {
            let bounds = to_str!(begin).to_string()..;
            if let Some(heads) = heads.as_ref() {
                to_result(doc.map_range_at(obj_id, bounds, heads.as_ref()))
            } else {
                to_result(doc.map_range(obj_id, bounds))
            }
        }
        (true, false) => {
            let bounds = ..to_str!(end).to_string();
            if let Some(heads) = heads.as_ref() {
                to_result(doc.map_range_at(obj_id, bounds, heads.as_ref()))
            } else {
                to_result(doc.map_range(obj_id, bounds))
            }
        }
        (true, true) => {
            let bounds = ..;
            if let Some(heads) = heads.as_ref() {
                to_result(doc.map_range_at(obj_id, bounds, heads.as_ref()))
            } else {
                to_result(doc.map_range(obj_id, bounds))
            }
        }
    }
}

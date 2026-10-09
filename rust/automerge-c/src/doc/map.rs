use automerge as am;
use automerge::transaction::Transactable;
use automerge::ReadDoc;

use crate::byte_span::{to_str, AMbyteSpan};
use crate::doc::{to_doc, to_doc_mut, AMdoc};
use crate::items::AMitems;
use crate::obj::{to_obj_id, to_obj_type, AMobjId, AMobjType};
use crate::result::{to_result, AMresult};

/// \memberof AMdoc
/// \brief Deletes an item from a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key The UTF-8 string view key of an item within the map object
///                identified by \p obj_id as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \brief Gets a current or historical item within a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key The UTF-8 string view key of an item within the map object
///                identified by \p obj_id as an `AMbyteSpan` struct.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select a historical item at \p key or `NULL`
///                  to select the current item at \p key.
/// \return A pointer to an `AMresult` struct with an `AMitem` struct.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapGet(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let key = to_str!(key);
    match heads.as_ref() {
        None => to_result((doc.get(obj_id, key), key)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result((doc.get_at(obj_id, key, &heads), key)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Gets all of the historical items at a key within a map object until
///        its current one or a specific one.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key The UTF-8 string view key of an item within the map object
///                identified by \p obj_id as an `AMbyteSpan` struct.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select a historical last item or `NULL` to
///                  select the current last item.
/// \return A pointer to an `AMresult` struct with an `AMItems` struct.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapGetAll(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let key = to_str!(key);
    match heads.as_ref() {
        None => to_result(doc.get_all(obj_id, key)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result(doc.get_all_at(obj_id, key, &heads)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Increments a counter at a key in a map object by the given value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key The UTF-8 string view key of an item within the map object
///                identified by \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key The UTF-8 string view key of an item within the map object
///                identified by \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A boolean.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \brief Puts an array of bytes value at a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key The UTF-8 string view key of an item within the map object
///                identified by \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A view onto an array of bytes as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \pre \p value.src `!= NULL`
/// \pre `0 <` \p value.count `<= sizeof(`\p value.src `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
/// value.src must be a byte array of length >= value.count
#[no_mangle]
pub unsafe extern "C" fn AMmapPutBytes(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    value: AMbyteSpan,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    to_result(doc.put(to_obj_id!(obj_id), key, Vec::<u8>::from(&value)))
}

/// \memberof AMdoc
/// \brief Puts a CRDT counter as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] obj_type An `AMobjIdType` enum tag.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_OBJ_TYPE` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
#[no_mangle]
pub unsafe extern "C" fn AMmapPutObject(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    key: AMbyteSpan,
    obj_type: AMobjType,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let key = to_str!(key);
    let obj_type = to_obj_type!(obj_type);
    to_result((
        doc.put_object(to_obj_id!(obj_id), key, obj_type),
        key,
        obj_type,
    ))
}

/// \memberof AMdoc
/// \brief Puts a float as the value of a key in a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A UTF-8 string view as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] key A UTF-8 string view key for the map object identified by
///                \p obj_id as an `AMbyteSpan` struct.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p key.src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// key.src must be a byte array of length >= key.count
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
/// \brief Gets the current or historical items of the map object within the
///        given range.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] begin The first key in a subrange or `AMstr(NULL)` to indicate the
///                  absolute first key.
/// \param[in] end The key one past the last key in a subrange or `AMstr(NULL)`
///                to indicate one past the absolute last key.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select historical items or `NULL` to select
///                  current items.
/// \return A pointer to an `AMresult` struct with an `AMitems` struct.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// begin.src must be a byte array of length >= begin.count or std::ptr::null()
/// end.src must be a byte array of length >= end.count or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMmapRange(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    begin: AMbyteSpan,
    end: AMbyteSpan,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let heads = match heads.as_ref() {
        None => None,
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => Some(heads),
            Err(e) => {
                return AMresult::error(&e.to_string()).into();
            }
        },
    };
    match (begin.is_null(), end.is_null()) {
        (false, false) => {
            let (begin, end) = (to_str!(begin).to_string(), to_str!(end).to_string());
            if begin > end {
                return AMresult::error(&format!("Invalid range [{}-{})", begin, end)).into();
            };
            let bounds = begin..end;
            if let Some(heads) = heads {
                to_result(doc.map_range_at(obj_id, bounds, &heads))
            } else {
                to_result(doc.map_range(obj_id, bounds))
            }
        }
        (false, true) => {
            let bounds = to_str!(begin).to_string()..;
            if let Some(heads) = heads {
                to_result(doc.map_range_at(obj_id, bounds, &heads))
            } else {
                to_result(doc.map_range(obj_id, bounds))
            }
        }
        (true, false) => {
            let bounds = ..to_str!(end).to_string();
            if let Some(heads) = heads {
                to_result(doc.map_range_at(obj_id, bounds, &heads))
            } else {
                to_result(doc.map_range(obj_id, bounds))
            }
        }
        (true, true) => {
            let bounds = ..;
            if let Some(heads) = heads {
                to_result(doc.map_range_at(obj_id, bounds, &heads))
            } else {
                to_result(doc.map_range(obj_id, bounds))
            }
        }
    }
}

use automerge as am;
use automerge::transaction::Transactable;
use automerge::ReadDoc;

use crate::byte_span::{to_str, AMbyteSpan};
use crate::doc::{to_doc, to_doc_mut, AMdoc};
use crate::items::AMitems;
use crate::obj::{to_obj_id, to_obj_type, AMobjId, AMobjType};
use crate::result::{to_result, AMresult};

macro_rules! adjust {
    ($pos:expr, $insert:expr, $len:expr) => {{
        // An empty object can only be inserted into.
        let insert = $insert || $len == 0;
        let end = if insert { $len } else { $len - 1 };
        if $pos > end && $pos != usize::MAX {
            return AMresult::error(&format!("Invalid pos {}", $pos)).into();
        }
        (std::cmp::min($pos, end), insert)
    }};
}

macro_rules! to_range {
    ($begin:expr, $end:expr) => {{
        if $begin > $end {
            return AMresult::error(&format!("Invalid range [{}-{})", $begin, $end)).into();
        };
        ($begin..$end)
    }};
}

/// \memberof AMdoc
/// \brief Deletes an item from a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistDelete(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, _) = adjust!(pos, false, doc.length(obj_id));
    to_result(doc.delete(obj_id, pos))
}

/// \memberof AMdoc
/// \brief Gets a current or historical item within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select a historical item at \p pos or `NULL`
///                  to select the current item at \p pos.
/// \return A pointer to an `AMresult` struct with an `AMitem` struct.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistGet(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, _) = adjust!(pos, false, doc.length(obj_id));
    match heads.as_ref() {
        None => to_result((doc.get(obj_id, pos), pos)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result((doc.get_at(obj_id, pos, &heads), pos)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Gets all of the historical items at a position within a list object
///        until its current one or a specific one.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select a historical last item or `NULL` to select
///                  the current last item.
/// \return A pointer to an `AMresult` struct with an `AMitems` struct.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistGetAll(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, _) = adjust!(pos, false, doc.length(obj_id));
    match heads.as_ref() {
        None => to_result(doc.get_all(obj_id, pos)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result(doc.get_all_at(obj_id, pos, &heads)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Increments a counter value in an item within a list object by the
///        given value.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistIncrement(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, _) = adjust!(pos, false, doc.length(obj_id));
    to_result(doc.increment(obj_id, pos, value))
}

/// \memberof AMdoc
/// \brief Puts a boolean value into an item within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \param[in] value A boolean.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutBool(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
    value: bool,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    let value = am::ScalarValue::Boolean(value);
    to_result(if insert {
        doc.insert(obj_id, pos, value)
    } else {
        doc.put(obj_id, pos, value)
    })
}

/// \memberof AMdoc
/// \brief Puts an array of bytes value at a position within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \param[in] value A view onto the array of bytes to copy from as an
///                  `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \pre \p value.src `!= NULL`
/// \pre `0 <` \p value.count `<= sizeof(`\p value.src `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// value.src must be a byte array of length >= value.count
#[no_mangle]
pub unsafe extern "C" fn AMlistPutBytes(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
    value: AMbyteSpan,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    let value: Vec<u8> = (&value).into();
    to_result(if insert {
        doc.insert(obj_id, pos, value)
    } else {
        doc.put(obj_id, pos, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a CRDT counter value into an item within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutCounter(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    let value = am::ScalarValue::Counter(value.into());
    to_result(if insert {
        doc.insert(obj_id, pos, value)
    } else {
        doc.put(obj_id, pos, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a float value into an item within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutF64(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
    value: f64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    to_result(if insert {
        doc.insert(obj_id, pos, value)
    } else {
        doc.put(obj_id, pos, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a signed integer value into an item within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutInt(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    to_result(if insert {
        doc.insert(obj_id, pos, value)
    } else {
        doc.put(obj_id, pos, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a null value into an item within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutNull(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    to_result(if insert {
        doc.insert(obj_id, pos, ())
    } else {
        doc.put(obj_id, pos, ())
    })
}

/// \memberof AMdoc
/// \brief Puts an empty object value into an item within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \param[in] obj_type An `AMobjIdType` enum tag.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_OBJ_TYPE` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutObject(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
    obj_type: AMobjType,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    let obj_type = to_obj_type!(obj_type);
    to_result(if insert {
        (doc.insert_object(obj_id, pos, obj_type), pos, obj_type)
    } else {
        (doc.put_object(obj_id, pos, obj_type), pos, obj_type)
    })
}

/// \memberof AMdoc
/// \brief Puts a UTF-8 string value into an item within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \param[in] value A UTF-8 string view as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \pre \p value.src `!= NULL`
/// \pre `0 <` \p value.count `<= sizeof(`\p value.src `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// value.src must be a byte array of length >= value.count
#[no_mangle]
pub unsafe extern "C" fn AMlistPutStr(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
    value: AMbyteSpan,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    let value = to_str!(value);
    to_result(if insert {
        doc.insert(obj_id, pos, value)
    } else {
        doc.put(obj_id, pos, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a *nix timestamp (milliseconds) value into an item within a
///        list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutTimestamp(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    let value = am::ScalarValue::Timestamp(value);
    to_result(if insert {
        doc.insert(obj_id, pos, value)
    } else {
        doc.put(obj_id, pos, value)
    })
}

/// \memberof AMdoc
/// \brief Puts an unsigned integer value into an item within a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos The position of an item within the list object identified by
///                \p obj_id or `SIZE_MAX` to indicate its last item if
///                \p insert `== false` or one past its last item if
///                \p insert `== true`.
/// \param[in] insert A flag for inserting a new item for \p value before
///                   \p pos instead of putting \p value into the item at
///                   \p pos.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutUint(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    insert: bool,
    value: u64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (pos, insert) = adjust!(pos, insert, doc.length(obj_id));
    to_result(if insert {
        doc.insert(obj_id, pos, value)
    } else {
        doc.put(obj_id, pos, value)
    })
}

/// \memberof AMdoc
/// \brief Gets the current or historical items in the list object within the
///        given range.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] begin The first pos in a range of indices.
/// \param[in] end At least one past the last pos in a range of indices.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select historical items or `NULL` to select
///                  current items.
/// \return A pointer to an `AMresult` struct with an `AMitems` struct.
/// \pre \p doc `!= NULL`
/// \pre \p begin `<=` \p end `<= SIZE_MAX`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistRange(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    begin: usize,
    end: usize,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let range = to_range!(begin, end);
    match heads.as_ref() {
        None => to_result(doc.list_range(obj_id, range)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result(doc.list_range_at(obj_id, range, &heads)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

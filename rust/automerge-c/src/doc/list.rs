use automerge as am;
use automerge::transaction::Transactable;
use std::os::raw::c_char;

use crate::change_hashes::AMchangeHashes;
use crate::doc::{to_doc, to_doc_mut, to_obj_id, to_str, AMdoc};
use crate::obj::{AMobjId, AMobjType};
use crate::result::{to_result, AMresult};

pub mod item;
pub mod items;

macro_rules! adjust {
    ($index:expr, $insert:expr, $len:expr) => {{
        // An empty object can only be inserted into.
        let insert = $insert || $len == 0;
        let end = if insert { $len } else { $len - 1 };
        if $index > end && $index != usize::MAX {
            return AMresult::err(&format!("Invalid index {}", $index)).into();
        }
        (std::cmp::min($index, end), insert)
    }};
}

macro_rules! to_range {
    ($begin:expr, $end:expr) => {{
        if $begin > $end {
            return AMresult::err(&format!("Invalid range [{}-{})", $begin, $end)).into();
        };
        ($begin..$end)
    }};
}

/// \memberof AMdoc
/// \brief Deletes an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistDelete(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, _) = adjust!(index, false, doc.length(obj_id));
    to_result(doc.delete(obj_id, index))
}

/// \memberof AMdoc
/// \brief Gets the current or historical value at an index in a list object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for a historical
///                  value or `NULL` for the current value.
/// \return A pointer to an `AMresult` struct that doesn't contain a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistGet(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, _) = adjust!(index, false, doc.length(obj_id));
    match heads.as_ref() {
        None => to_result(doc.get(obj_id, index)),
        Some(heads) => to_result(doc.get_at(obj_id, index, heads.as_ref())),
    }
}

/// \memberof AMdoc
/// \brief Gets all of the historical values at an index in a list object until
///        its current one or a specific one.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for a historical
///                  last value or `NULL` for the current last value.
/// \return A pointer to an `AMresult` struct containing an `AMobjItems` struct.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistGetAll(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, _) = adjust!(index, false, doc.length(obj_id));
    match heads.as_ref() {
        None => to_result(doc.get_all(obj_id, index)),
        Some(heads) => to_result(doc.get_all_at(obj_id, index, heads.as_ref())),
    }
}

/// \memberof AMdoc
/// \brief Increments a counter at an index in a list object by the given
///        value.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistIncrement(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, _) = adjust!(index, false, doc.length(obj_id));
    to_result(doc.increment(obj_id, index, value))
}

/// \memberof AMdoc
/// \brief Puts a boolean as the value at an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A boolean.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutBool(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: bool,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
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
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p src before \p index instead of
///            writing \p src over \p index.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes to copy from \p src.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \pre \p src `!= NULL`.
/// \pre `0 <` \p count `<= sizeof(`\p src`)`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
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
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
    let mut value = Vec::new();
    value.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts a CRDT counter as the value at an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutCounter(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
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
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit float.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutF64(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: f64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
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
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutInt(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
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
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutNull(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
    to_result(if insert {
        doc.insert(obj_id, index, ())
    } else {
        doc.put(obj_id, index, ())
    })
}

/// \memberof AMdoc
/// \brief Puts an empty object as the value at an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p value before \p index instead of
///                   writing \p value over \p index.
/// \param[in] obj_type An `AMobjIdType` enum tag.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMobjId` struct.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutObject(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    obj_type: AMobjType,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
    let object = obj_type.into();
    to_result(if insert {
        doc.insert_object(obj_id, index, object)
    } else {
        doc.put_object(obj_id, index, object)
    })
}

/// \memberof AMdoc
/// \brief Puts a UTF-8 string as the value at an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \pre \p value `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// value must be a null-terminated array of `c_char`
#[no_mangle]
pub unsafe extern "C" fn AMlistPutStr(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: *const c_char,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
    let value = to_str(value);
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Puts an epoch timestamp as the value at an index in a list object.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit signed integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutTimestamp(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
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
/// \param[in] index An index in the list object identified by \p obj_id or
///                  `SIZE_MAX` to indicate its last index if \p insert
///                  `== false` or one past its last index if \p insert
///                  `== true`.
/// \param[in] insert A flag to insert \p value before \p index instead of
///            writing \p value over \p index.
/// \param[in] value A 64-bit unsigned integer.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p index `<= AMobjSize(`\p obj_id`)` or \p index `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistPutUint(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    index: usize,
    insert: bool,
    value: u64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let (index, insert) = adjust!(index, insert, doc.length(obj_id));
    to_result(if insert {
        doc.insert(obj_id, index, value)
    } else {
        doc.put(obj_id, index, value)
    })
}

/// \memberof AMdoc
/// \brief Gets the current or historical indices and values of the list object
///        within the given range.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] begin The first index in a range of indices.
/// \param[in] end At least one past the last index in a range of indices.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for historical
///                  indices and values or `NULL` for current indices and
///                  values.
/// \return A pointer to an `AMresult` struct containing an `AMlistItems`
///         struct.
/// \pre \p doc `!= NULL`.
/// \pre \p begin `<=` \p end `<= SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMlistRange(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    begin: usize,
    end: usize,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let range = to_range!(begin, end);
    match heads.as_ref() {
        None => to_result(doc.list_range(obj_id, range)),
        Some(heads) => to_result(doc.list_range_at(obj_id, range, heads.as_ref())),
    }
}

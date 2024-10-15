use automerge as am;
use std::cell::RefCell;

use crate::byte_span::{to_str, AMbyteSpan};
use crate::result::{to_result, AMresult};

macro_rules! to_cursor {
    ($handle:expr) => {{
        match $handle.as_ref() {
            Some(b) => b,
            None => return AMresult::error("Invalid `AMcursor*`").into(),
        }
    }};
}

pub(crate) use to_cursor;

/// \struct AMcursor
/// \installed_headerfile
/// \brief An identifier of a position within a list object or text object.
///
/// Example use cases:
/// 1. Maintaining the contextual position of a user's cursor while merging
///    remote changes.
/// 2. Indexing a sentence within a text field.
#[derive(PartialEq)]
pub struct AMcursor {
    body: am::Cursor,
    bytes: RefCell<Option<Vec<u8>>>,
    fmt_str: RefCell<Option<Box<str>>>,
}

impl AMcursor {
    pub fn new(cursor: am::Cursor) -> Self {
        Self {
            body: cursor,
            bytes: Default::default(),
            fmt_str: Default::default(),
        }
    }

    fn to_bytes(&self) -> AMbyteSpan {
        let mut bytes = self.bytes.borrow_mut();
        let vec = self.body.to_bytes();
        let ptr = bytes.insert(vec);
        AMbyteSpan {
            src: ptr.as_ptr(),
            count: ptr.len(),
        }
    }

    fn to_str(&self) -> AMbyteSpan {
        let mut fmt_str = self.fmt_str.borrow_mut();
        let fmt_string = self.body.to_string();
        fmt_str
            .insert(fmt_string.into_boxed_str())
            .as_bytes()
            .into()
    }
}

impl AsRef<am::Cursor> for AMcursor {
    fn as_ref(&self) -> &am::Cursor {
        &self.body
    }
}

/// \memberof AMcursor
/// \brief Gets the value of a cursor as an array of bytes.
///
/// \param[in] cursor A pointer to an `AMcursor` struct.
/// \return An `AMbyteSpan` struct for an array of bytes.
/// \pre \p cursor `!= NULL`
/// \internal
///
/// # Safety
/// cursor must be a valid pointer to an AMcursor
#[no_mangle]
pub unsafe extern "C" fn AMcursorBytes(cursor: *const AMcursor) -> AMbyteSpan {
    match cursor.as_ref() {
        Some(cursor) => cursor.to_bytes(),
        None => Default::default(),
    }
}

/// \memberof AMcursor
/// \brief Tests the equality of two cursors.
///
/// \param[in] cursor1 A pointer to an `AMcursor` struct.
/// \param[in] cursor2 A pointer to an `AMcursor` struct.
/// \return `true` if \p cursor1 `==` \p cursor2 and `false` otherwise.
/// \pre \p cursor1 `!= NULL`
/// \pre \p cursor2 `!= NULL`
/// \post `!(`\p cursor1 `&&` \p cursor2 `) -> false`
/// \internal
///
/// #Safety
/// cursor1 must be a valid pointer to an AMcursor
/// cursor2 must be a valid pointer to an AMcursor
#[no_mangle]
pub unsafe extern "C" fn AMcursorEqual(cursor1: *const AMcursor, cursor2: *const AMcursor) -> bool {
    match (cursor1.as_ref(), cursor2.as_ref()) {
        (Some(cursor1), Some(cursor2)) => cursor1.as_ref() == cursor2.as_ref(),
        (None, None) | (None, Some(_)) | (Some(_), None) => false,
    }
}

/// \memberof AMcursor
/// \brief Allocates a new cursor and initializes it from an array of
///        bytes value.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The count of bytes to copy from the array pointed to by
///                  \p src.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_CURSOR` item.
/// \pre \p src `!= NULL`
/// \pre `sizeof(`\p src `) > 0`
/// \pre \p count `<= sizeof(`\p src `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// src must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMcursorFromBytes(src: *const u8, count: usize) -> *mut AMresult {
    if !src.is_null() {
        let value = std::slice::from_raw_parts(src, count);
        to_result(am::Cursor::try_from(value))
    } else {
        AMresult::error("Invalid uint8_t*").into()
    }
}

/// \memberof AMcursor
/// \brief Allocates a new cursor and initializes it from a UTF-8 string view
///        value.
///
/// \param[in] value A UTF-8 string view as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_CURSOR` item.
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// value.src must be a byte array of length >= value.count
#[no_mangle]
pub unsafe extern "C" fn AMcursorFromStr(value: AMbyteSpan) -> *mut AMresult {
    to_result(am::Cursor::try_from(to_str!(value)))
}

/// \memberof AMcursor
/// \brief Gets the value of a cursor as a UTF-8 string view.
///
/// \param[in] cursor A pointer to an `AMcursor` struct.
/// \return A UTF-8 string view as an `AMbyteSpan` struct.
/// \pre \p cursor `!= NULL`
/// \internal
///
/// # Safety
/// cursor must be a valid pointer to an AMcursor
#[no_mangle]
pub unsafe extern "C" fn AMcursorStr(cursor: *const AMcursor) -> AMbyteSpan {
    match cursor.as_ref() {
        Some(cursor) => cursor.to_str(),
        None => Default::default(),
    }
}

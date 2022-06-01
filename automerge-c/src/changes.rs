use automerge as am;
use std::collections::BTreeMap;
use std::ffi::c_void;

use crate::change::AMchange;

/// \struct AMchanges
/// \brief A bidirectional iterator over a sequence of changes.
#[repr(C)]
pub struct AMchanges {
    /// The length of the sequence.
    len: usize,
    /// The offset from \p ptr, \p +offset -> forward direction,
    /// \p -offset -> reverse direction.
    offset: isize,
    /// A pointer to the first change or `NULL`.
    ptr: *const c_void,
    /// Reserved.
    storage: *mut c_void,
}

impl AMchanges {
    pub fn new(changes: &[am::Change], storage: &mut BTreeMap<usize, AMchange>) -> Self {
        let storage: *mut BTreeMap<usize, AMchange> = storage;
        Self {
            len: changes.len(),
            offset: 0,
            ptr: changes.as_ptr() as *const c_void,
            storage: storage as *mut c_void,
        }
    }

    pub fn advance(&mut self, n: isize) {
        let len = self.len as isize;
        if n != 0 && self.offset >= -len && self.offset < len {
            // It's being advanced and it hasn't stopped.
            self.offset = std::cmp::max(-(len + 1), std::cmp::min(self.offset + n, len));
        };
    }

    pub fn next(&mut self, n: isize) -> Option<*const AMchange> {
        let len = self.len as isize;
        if self.offset < -len || self.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &mut [am::Change] =
                unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut am::Change, self.len) };
            let index = (self.offset + if self.offset < 0 { len } else { 0 }) as usize;
            let storage = unsafe { &mut *(self.storage as *mut BTreeMap<usize, AMchange>) };
            let value = match storage.get_mut(&index) {
                Some(value) => value,
                None => {
                    storage.insert(index, AMchange::new(&mut slice[index]));
                    storage.get_mut(&index).unwrap()
                }
            };
            self.advance(n);
            Some(value)
        }
    }

    pub fn prev(&mut self, n: isize) -> Option<*const AMchange> {
        self.advance(n);
        let len = self.len as isize;
        if self.offset < -len || self.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &mut [am::Change] =
                unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut am::Change, self.len) };
            let index = (self.offset + if self.offset < 0 { len } else { 0 }) as usize;
            let storage = unsafe { &mut *(self.storage as *mut BTreeMap<usize, AMchange>) };
            Some(match storage.get_mut(&index) {
                Some(value) => value,
                None => {
                    storage.insert(index, AMchange::new(&mut slice[index]));
                    storage.get_mut(&index).unwrap()
                }
            })
        }
    }
}

impl AsRef<[am::Change]> for AMchanges {
    fn as_ref(&self) -> &[am::Change] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const am::Change, self.len) }
    }
}

impl Default for AMchanges {
    fn default() -> Self {
        Self {
            len: 0,
            offset: 0,
            ptr: std::ptr::null(),
            storage: std::ptr::null_mut(),
        }
    }
}

/// \memberof AMchanges
/// \brief Advances/rewinds an `AMchanges` struct by at most \p |n|
/// positions.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \pre \p changes must be a valid address.
/// \internal
///
/// #Safety
/// changes must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesAdvance(changes: *mut AMchanges, n: isize) {
    if let Some(changes) = changes.as_mut() {
        changes.advance(n);
    };
}

/// \memberof AMchanges
/// \brief Compares two change sequences for equality.
///
/// \param[in] changes1 A pointer to an `AMchanges` struct.
/// \param[in] changes2 A pointer to an `AMchanges` struct.
/// \return `true` if \p changes1 `==` \p changes2 and `false` otherwise.
/// \pre \p changes1 must be a valid address.
/// \pre \p changes2 must be a valid address.
/// \internal
///
/// #Safety
/// changes1 must be a pointer to a valid AMchanges
/// changes2 must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesEqual(
    changes1: *const AMchanges,
    changes2: *const AMchanges,
) -> bool {
    match (changes1.as_ref(), changes2.as_ref()) {
        (Some(changes1), Some(changes2)) => changes1.as_ref() == changes2.as_ref(),
        (None, Some(_)) | (Some(_), None) | (None, None) => false,
    }
}

/// \memberof AMchanges
/// \brief Gets a pointer to the `AMchange` struct at the current position of
///        an `AMchanges`struct and then advances/rewinds it by at most \p |n|
///        positions.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \return A pointer to an `AMchange` struct that's `NULL` when \p changes was
///         previously advanced/rewound past its forward/backward limit.
/// \pre \p changes must be a valid address.
/// \internal
///
/// #Safety
/// changes must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesNext(changes: *mut AMchanges, n: isize) -> *const AMchange {
    if let Some(changes) = changes.as_mut() {
        if let Some(change) = changes.next(n) {
            return change;
        }
    }
    std::ptr::null()
}

/// \memberof AMchanges
/// \brief Advances/rewinds an `AMchanges` struct by at most \p |n|
///        positions and then gets a pointer to the `AMchange` struct at its
///        current position.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \return A pointer to an `AMchange` struct that's `NULL` when \p changes is
///         presently advanced/rewound past its forward/backward limit.
/// \pre \p changes must be a valid address.
/// \internal
///
/// #Safety
/// changes must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesPrev(changes: *mut AMchanges, n: isize) -> *const AMchange {
    if let Some(changes) = changes.as_mut() {
        if let Some(change) = changes.prev(n) {
            return change;
        }
    }
    std::ptr::null()
}

/// \memberof AMchanges
/// \brief Gets the size of an `AMchanges` struct.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \return The count of values in \p changes.
/// \pre \p changes must be a valid address.
/// \internal
///
/// #Safety
/// changes must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesSize(changes: *const AMchanges) -> usize {
    if let Some(changes) = changes.as_ref() {
        changes.len
    } else {
        0
    }
}

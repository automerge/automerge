use automerge as am;
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::mem::size_of;

use crate::change::AMchange;

#[repr(C)]
struct Detail {
    len: usize,
    offset: isize,
    storage: *mut c_void,
}

/// \note cbindgen won't propagate the value of a `std::mem::size_of<T>()` call
///       (https://github.com/eqrion/cbindgen/issues/252) but it will
///       propagate the name of a constant initialized from it so if the
///       constant's name is a symbolic representation of the value it can be
///       converted into a number by post-processing the header it generated.
pub const USIZE_USIZE_USIZE_: usize = size_of::<Detail>();

impl Detail {
    fn new(len: usize, offset: isize, storage: &mut BTreeMap<usize, AMchange>) -> Self {
        let storage: *mut BTreeMap<usize, AMchange> = storage;
        Self {
            len,
            offset,
            storage: storage as *mut c_void,
        }
    }
}

impl From<Detail> for [u8; USIZE_USIZE_USIZE_] {
    fn from(detail: Detail) -> Self {
        unsafe {
            std::slice::from_raw_parts((&detail as *const Detail) as *const u8, USIZE_USIZE_USIZE_)
                .try_into()
                .unwrap()
        }
    }
}

/// \struct AMchanges
/// \brief A bidirectional iterator over a sequence of changes.
#[repr(C)]
pub struct AMchanges {
    /// A pointer to the first change or `NULL`.
    ptr: *const c_void,
    /// Reserved.
    detail: [u8; USIZE_USIZE_USIZE_],
}

impl AMchanges {
    pub fn new(changes: &[am::Change], storage: &mut BTreeMap<usize, AMchange>) -> Self {
        Self {
            ptr: changes.as_ptr() as *const c_void,
            detail: Detail::new(changes.len(), 0, storage).into(),
        }
    }

    pub fn advance(&mut self, n: isize) {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        let len = detail.len as isize;
        if n != 0 && detail.offset >= -len && detail.offset < len {
            // It's being advanced and it hasn't stopped.
            detail.offset = std::cmp::max(-(len + 1), std::cmp::min(detail.offset + n, len));
        };
    }

    pub fn len(&self) -> usize {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        detail.len
    }

    pub fn next(&mut self, n: isize) -> Option<*const AMchange> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        let len = detail.len as isize;
        if detail.offset < -len || detail.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &mut [am::Change] =
                unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut am::Change, detail.len) };
            let index = (detail.offset + if detail.offset < 0 { len } else { 0 }) as usize;
            let storage = unsafe { &mut *(detail.storage as *mut BTreeMap<usize, AMchange>) };
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
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        let len = detail.len as isize;
        if detail.offset < -len || detail.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &mut [am::Change] =
                unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut am::Change, detail.len) };
            let index = (detail.offset + if detail.offset < 0 { len } else { 0 }) as usize;
            let storage = unsafe { &mut *(detail.storage as *mut BTreeMap<usize, AMchange>) };
            Some(match storage.get_mut(&index) {
                Some(value) => value,
                None => {
                    storage.insert(index, AMchange::new(&mut slice[index]));
                    storage.get_mut(&index).unwrap()
                }
            })
        }
    }

    pub fn reverse(&self) -> Self {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        let storage = unsafe { &mut *(detail.storage as *mut BTreeMap<usize, AMchange>) };
        Self {
            ptr: self.ptr,
            detail: Detail::new(detail.len, -(detail.offset + 1), storage).into(),
        }
    }
}

impl AsRef<[am::Change]> for AMchanges {
    fn as_ref(&self) -> &[am::Change] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(self.ptr as *const am::Change, detail.len) }
    }
}

impl Default for AMchanges {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null(),
            detail: [0; USIZE_USIZE_USIZE_],
        }
    }
}

/// \memberof AMchanges
/// \brief Advances/rewinds an iterator over a sequence of changes by at most
///        \p |n| positions.
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
/// \brief Tests the equality of two sequences of changes underlying a pair
///        of iterators.
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
/// \brief Gets the change at the current position of an iterator over a
///        sequence of changes and then advances/rewinds it by at most \p |n|
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
/// \brief Advances/rewinds an iterator over a sequence of changes by at most
///        \p |n| positions and then gets the change at its current position.
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
/// \brief Gets the size of the sequence of changes underlying an iterator.
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
        changes.len()
    } else {
        0
    }
}

/// \memberof AMchanges
/// \brief Creates a reversed copy of a changes iterator.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \return An `AMchanges` struct
/// \pre \p changes must be a valid address.
/// \internal
///
/// #Safety
/// changes must be a pointer to a valid AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesReverse(changes: *const AMchanges) -> AMchanges {
    if let Some(changes) = changes.as_ref() {
        changes.reverse()
    } else {
        AMchanges::default()
    }
}

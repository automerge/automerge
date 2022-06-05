use automerge as am;
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::mem::size_of;

use crate::sync::have::AMsyncHave;

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
    fn new(len: usize, offset: isize, storage: &mut BTreeMap<usize, AMsyncHave>) -> Self {
        let storage: *mut BTreeMap<usize, AMsyncHave> = storage;
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

/// \struct AMsyncHaves
/// \brief A bidirectional iterator over a sequence of synchronization haves.
#[repr(C)]
pub struct AMsyncHaves {
    /// A pointer to the first synchronization have or `NULL`.
    ptr: *const c_void,
    /// Reserved.
    detail: [u8; USIZE_USIZE_USIZE_],
}

impl AMsyncHaves {
    pub fn new(haves: &[am::sync::Have], storage: &mut BTreeMap<usize, AMsyncHave>) -> Self {
        Self {
            ptr: haves.as_ptr() as *const c_void,
            detail: Detail::new(haves.len(), 0, storage).into(),
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

    pub fn next(&mut self, n: isize) -> Option<*const AMsyncHave> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        let len = detail.len as isize;
        if detail.offset < -len || detail.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &[am::sync::Have] = unsafe {
                std::slice::from_raw_parts(self.ptr as *const am::sync::Have, detail.len)
            };
            let index = (detail.offset + if detail.offset < 0 { len } else { 0 }) as usize;
            let storage = unsafe { &mut *(detail.storage as *mut BTreeMap<usize, AMsyncHave>) };
            let value = match storage.get_mut(&index) {
                Some(value) => value,
                None => {
                    storage.insert(index, AMsyncHave::new(&slice[index]));
                    storage.get_mut(&index).unwrap()
                }
            };
            self.advance(n);
            Some(value)
        }
    }

    pub fn prev(&mut self, n: isize) -> Option<*const AMsyncHave> {
        self.advance(n);
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        let len = detail.len as isize;
        if detail.offset < -len || detail.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &[am::sync::Have] = unsafe {
                std::slice::from_raw_parts(self.ptr as *const am::sync::Have, detail.len)
            };
            let index = (detail.offset + if detail.offset < 0 { len } else { 0 }) as usize;
            let storage = unsafe { &mut *(detail.storage as *mut BTreeMap<usize, AMsyncHave>) };
            Some(match storage.get_mut(&index) {
                Some(value) => value,
                None => {
                    storage.insert(index, AMsyncHave::new(&slice[index]));
                    storage.get_mut(&index).unwrap()
                }
            })
        }
    }

    pub fn reverse(&self) -> Self {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        let storage = unsafe { &mut *(detail.storage as *mut BTreeMap<usize, AMsyncHave>) };
        Self {
            ptr: self.ptr,
            detail: Detail::new(detail.len, -(detail.offset + 1), storage).into(),
        }
    }
}

impl AsRef<[am::sync::Have]> for AMsyncHaves {
    fn as_ref(&self) -> &[am::sync::Have] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(self.ptr as *const am::sync::Have, detail.len) }
    }
}

impl Default for AMsyncHaves {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null(),
            detail: [0; USIZE_USIZE_USIZE_],
        }
    }
}

/// \memberof AMsyncHaves
/// \brief Advances/rewinds an iterator over a sequence of synchronization
///        haves by at most \p |n| positions.
///
/// \param[in] sync_haves A pointer to an `AMsyncHaves` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \pre \p sync_haves must be a valid address.
/// \internal
///
/// #Safety
/// sync_haves must be a pointer to a valid AMsyncHaves
#[no_mangle]
pub unsafe extern "C" fn AMsyncHavesAdvance(sync_haves: *mut AMsyncHaves, n: isize) {
    if let Some(sync_haves) = sync_haves.as_mut() {
        sync_haves.advance(n);
    };
}

/// \memberof AMsyncHaves
/// \brief Tests the equality of two sequences of synchronization haves
///        underlying a pair of iterators.
///
/// \param[in] sync_haves1 A pointer to an `AMsyncHaves` struct.
/// \param[in] sync_haves2 A pointer to an `AMsyncHaves` struct.
/// \return `true` if \p sync_haves1 `==` \p sync_haves2 and `false` otherwise.
/// \pre \p sync_haves1 must be a valid address.
/// \pre \p sync_haves2 must be a valid address.
/// \internal
///
/// #Safety
/// sync_haves1 must be a pointer to a valid AMsyncHaves
/// sync_haves2 must be a pointer to a valid AMsyncHaves
#[no_mangle]
pub unsafe extern "C" fn AMsyncHavesEqual(
    sync_haves1: *const AMsyncHaves,
    sync_haves2: *const AMsyncHaves,
) -> bool {
    match (sync_haves1.as_ref(), sync_haves2.as_ref()) {
        (Some(sync_haves1), Some(sync_haves2)) => sync_haves1.as_ref() == sync_haves2.as_ref(),
        (None, Some(_)) | (Some(_), None) | (None, None) => false,
    }
}

/// \memberof AMsyncHaves
/// \brief Gets the synchronization have at the current position of an iterator
///        over a sequence of synchronization haves and then advances/rewinds
///        it by at most \p |n| positions.
///
/// \param[in] sync_haves A pointer to an `AMsyncHaves` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \return A pointer to an `AMsyncHave` struct that's `NULL` when
///         \p sync_haves was previously advanced/rewound past its
///         forward/backward limit.
/// \pre \p sync_haves must be a valid address.
/// \internal
///
/// #Safety
/// sync_haves must be a pointer to a valid AMsyncHaves
#[no_mangle]
pub unsafe extern "C" fn AMsyncHavesNext(
    sync_haves: *mut AMsyncHaves,
    n: isize,
) -> *const AMsyncHave {
    if let Some(sync_haves) = sync_haves.as_mut() {
        if let Some(sync_have) = sync_haves.next(n) {
            return sync_have;
        }
    }
    std::ptr::null()
}

/// \memberof AMsyncHaves
/// \brief Advances/rewinds an iterator over a sequence of synchronization
///        haves by at most \p |n| positions and then gets the synchronization
///        have at its current position.
///
/// \param[in] sync_haves A pointer to an `AMsyncHaves` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \return A pointer to an `AMsyncHave` struct that's `NULL` when \p sync_haves
///         is presently advanced/rewound past its
///         forward/backward limit.
/// \pre \p sync_haves must be a valid address.
/// \internal
///
/// #Safety
/// sync_haves must be a pointer to a valid AMsyncHaves
#[no_mangle]
pub unsafe extern "C" fn AMsyncHavesPrev(
    sync_haves: *mut AMsyncHaves,
    n: isize,
) -> *const AMsyncHave {
    if let Some(sync_haves) = sync_haves.as_mut() {
        if let Some(sync_have) = sync_haves.prev(n) {
            return sync_have;
        }
    }
    std::ptr::null()
}

/// \memberof AMsyncHaves
/// \brief Gets the size of the sequence of synchronization haves underlying an
///        iterator.
///
/// \param[in] sync_haves A pointer to an `AMsyncHaves` struct.
/// \return The count of values in \p sync_haves.
/// \pre \p sync_haves must be a valid address.
/// \internal
///
/// #Safety
/// sync_haves must be a pointer to a valid AMsyncHaves
#[no_mangle]
pub unsafe extern "C" fn AMsyncHavesSize(sync_haves: *const AMsyncHaves) -> usize {
    if let Some(sync_haves) = sync_haves.as_ref() {
        sync_haves.len()
    } else {
        0
    }
}

/// \memberof AMsyncHaves
/// \brief Creates a reversed copy of a synchronization haves iterator.
///
/// \param[in] sync_haves A pointer to an `AMsyncHaves` struct.
/// \return An `AMsyncHaves` struct
/// \pre \p sync_haves must be a valid address.
/// \internal
///
/// #Safety
/// sync_haves must be a pointer to a valid AMsyncHaves
#[no_mangle]
pub unsafe extern "C" fn AMsyncHavesReverse(sync_haves: *const AMsyncHaves) -> AMsyncHaves {
    if let Some(sync_haves) = sync_haves.as_ref() {
        sync_haves.reverse()
    } else {
        AMsyncHaves::default()
    }
}

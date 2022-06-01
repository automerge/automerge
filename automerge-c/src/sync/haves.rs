use automerge as am;
use std::collections::BTreeMap;
use std::ffi::c_void;

use crate::sync::have::AMsyncHave;

/// \struct AMsyncHaves
/// \brief A bidirectional iterator over a sequence of synchronization haves.
#[repr(C)]
pub struct AMsyncHaves {
    /// The length of the sequence.
    len: usize,
    /// The offset from \p ptr, \p +offset -> forward direction,
    /// \p -offset -> reverse direction.
    offset: isize,
    /// A pointer to the first synchronization have or `NULL`.
    ptr: *const c_void,
    /// Reserved.
    storage: *mut c_void,
}

impl AMsyncHaves {
    pub fn new(sync_haves: &[am::sync::Have], storage: &mut BTreeMap<usize, AMsyncHave>) -> Self {
        let storage: *mut BTreeMap<usize, AMsyncHave> = storage;
        Self {
            len: sync_haves.len(),
            offset: 0,
            ptr: sync_haves.as_ptr() as *const c_void,
            storage: storage as *mut c_void,
        }
    }

    pub fn advance(&mut self, n: isize) {
        let len = self.len as isize;
        if n != 0 && self.offset >= -len && self.offset < len {
            // It's being advanced and it's hasn't stopped.
            self.offset = std::cmp::max(-(len + 1), std::cmp::min(self.offset + n, len));
        };
    }

    pub fn next(&mut self, n: isize) -> Option<*const AMsyncHave> {
        let len = self.len as isize;
        if self.offset < -len || self.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &[am::sync::Have] =
                unsafe { std::slice::from_raw_parts(self.ptr as *const am::sync::Have, self.len) };
            let index = (self.offset + if self.offset < 0 { len } else { 0 }) as usize;
            let storage = unsafe { &mut *(self.storage as *mut BTreeMap<usize, AMsyncHave>) };
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
        let len = self.len as isize;
        if self.offset < -len || self.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &[am::sync::Have] =
                unsafe { std::slice::from_raw_parts(self.ptr as *const am::sync::Have, self.len) };
            let index = (self.offset + if self.offset < 0 { len } else { 0 }) as usize;
            let storage = unsafe { &mut *(self.storage as *mut BTreeMap<usize, AMsyncHave>) };
            Some(match storage.get_mut(&index) {
                Some(value) => value,
                None => {
                    storage.insert(index, AMsyncHave::new(&slice[index]));
                    storage.get_mut(&index).unwrap()
                }
            })
        }
    }
}

impl AsRef<[am::sync::Have]> for AMsyncHaves {
    fn as_ref(&self) -> &[am::sync::Have] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const am::sync::Have, self.len) }
    }
}

impl Default for AMsyncHaves {
    fn default() -> Self {
        Self {
            len: 0,
            offset: 0,
            ptr: std::ptr::null(),
            storage: std::ptr::null_mut(),
        }
    }
}

/// \memberof AMsyncHaves
/// \brief Advances/rewinds an `AMsyncHaves` struct by at most \p |n|
/// positions.
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
/// \brief Gets a pointer to the `AMsyncHave` struct at the current position of
///        an `AMsyncHaves`struct and then advances/rewinds it by at most \p |n|
///        positions.
///
/// \param[in] sync_haves A pointer to an `AMsyncHaves` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \return A pointer to an `AMsyncHave` struct that's `NULL` when \p sync_haves
///         was previously advanced/rewound past its
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
/// \brief Advances/rewinds an `AMsyncHaves` struct by at most \p |n|
///        positions and then gets a pointer to the `AMsyncHave` struct at its
///        current position.
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
/// \brief Gets the size of an `AMsyncHaves` struct.
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
        sync_haves.len
    } else {
        0
    }
}

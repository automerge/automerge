use automerge as am;
use std::ffi::c_void;

use crate::AMbyteSpan;

/// \struct AMchangeHashes
/// \brief A bidirectional iterator over a sequence of `AMbyteSpan` structs.
#[repr(C)]
pub struct AMchangeHashes {
    len: usize,
    offset: isize,
    ptr: *const c_void,
}

impl AsRef<[am::ChangeHash]> for AMchangeHashes {
    fn as_ref(&self) -> &[am::ChangeHash] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const am::ChangeHash, self.len) }
    }
}

impl AMchangeHashes {
    pub fn new(change_hashes: &[am::ChangeHash]) -> Self {
        Self {
            len: change_hashes.len(),
            offset: 0,
            ptr: change_hashes.as_ptr() as *const c_void,
        }
    }

    pub fn advance(&mut self, n: isize) {
        let len = self.len as isize;
        if n != 0 && self.offset >= -len && self.offset < len {
            // It's being advanced and it's hasn't stopped.
            self.offset = std::cmp::max(-(len + 1), std::cmp::min(self.offset + n, len));
        };
    }

    pub fn next(&mut self, n: isize) -> Option<&am::ChangeHash> {
        let len = self.len as isize;
        if self.offset < -len || self.offset == len {
            // It's stopped.
            None
        } else {
            let slice =
                unsafe { std::slice::from_raw_parts(self.ptr as *const am::ChangeHash, self.len) };
            let index = (self.offset + if self.offset < 0 { len } else { 0 }) as usize;
            let element = Some(&slice[index]);
            self.advance(n);
            element
        }
    }

    pub fn prev(&mut self, n: isize) -> Option<&am::ChangeHash> {
        self.advance(n);
        let len = self.len as isize;
        if self.offset < -len || self.offset == len {
            // It's stopped.
            None
        } else {
            let slice =
                unsafe { std::slice::from_raw_parts(self.ptr as *const am::ChangeHash, self.len) };
            let index = (self.offset + if self.offset < 0 { len } else { 0 }) as usize;
            Some(&slice[index])
        }
    }
}

/// \memberof AMchangeHashes
/// \brief Advances/rewinds an `AMchangeHashes` struct by at most \p |n|
/// positions.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
/// number of positions to advance/rewind.
/// \pre \p change_hashes must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMadvanceChangeHashes(change_hashes: *mut AMchangeHashes, n: isize) {
    if let Some(change_hashes) = change_hashes.as_mut() {
        change_hashes.advance(n);
    };
}

/// \memberof AMchangeHashes
/// \brief Gets the size of an `AMchangeHashes` struct.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \return The count of values in \p change_hashes.
/// \pre \p change_hashes must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMchangeHashesSize(change_hashes: *const AMchangeHashes) -> usize {
    if let Some(change_hashes) = change_hashes.as_ref() {
        change_hashes.len
    } else {
        0
    }
}

/// \memberof AMchangeHashes
/// \brief Gets the `AMbyteSpan` struct at the current position of an
/// `AMchangeHashes`struct and then advances/rewinds it by at most \p |n|
/// positions.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
/// number of positions to advance/rewind.
/// \return An `AMbyteSpan` struct that's invalid when \p change_hashes was
/// previously advanced/rewound past its forward/backward limit.
/// \pre \p change_hashes must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMnextChangeHash(
    change_hashes: *mut AMchangeHashes,
    n: isize,
) -> AMbyteSpan {
    if let Some(change_hashes) = change_hashes.as_mut() {
        if let Some(change_hash) = change_hashes.next(n) {
            return change_hash.into();
        }
    }
    AMbyteSpan::default()
}

/// \memberof AMchangeHashes
/// \brief Advances/rewinds an `AMchangeHashes` struct by at most \p |n|
/// positions and then gets the `AMbyteSpan` struct at its current position.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
/// number of positions to advance/rewind.
/// \return An `AMbyteSpan` struct that's invalid when \p change_hashes is
/// presently advanced/rewound past its forward/backward limit.
/// \pre \p change_hashes must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMprevChangeHash(
    change_hashes: *mut AMchangeHashes,
    n: isize,
) -> AMbyteSpan {
    if let Some(change_hashes) = change_hashes.as_mut() {
        if let Some(change_hash) = change_hashes.prev(n) {
            return change_hash.into();
        }
    }
    AMbyteSpan::default()
}

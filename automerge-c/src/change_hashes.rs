use automerge as am;
use std::cmp::Ordering;
use std::ffi::c_void;

use crate::byte_span::AMbyteSpan;

/// \struct AMchangeHashes
/// \brief A bidirectional iterator over a sequence of change hashes.
#[repr(C)]
pub struct AMchangeHashes {
    /// The length of the sequence.
    len: usize,
    /// The offset from \p ptr, \p +offset -> forward direction,
    /// \p -offset -> reverse direction.
    offset: isize,
    /// A pointer to the first change hash or `NULL`.
    ptr: *const c_void,
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

impl AsRef<[am::ChangeHash]> for AMchangeHashes {
    fn as_ref(&self) -> &[am::ChangeHash] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const am::ChangeHash, self.len) }
    }
}

impl Default for AMchangeHashes {
    fn default() -> Self {
        Self {
            len: 0,
            offset: 0,
            ptr: std::ptr::null(),
        }
    }
}

/// \memberof AMchangeHashes
/// \brief Advances/rewinds an iterator over a sequence of change hashes by at
///        most \p |n| positions.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \pre \p change_hashes must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMchangeHashesAdvance(change_hashes: *mut AMchangeHashes, n: isize) {
    if let Some(change_hashes) = change_hashes.as_mut() {
        change_hashes.advance(n);
    };
}

/// \memberof AMchangeHashes
/// \brief Compares the sequences of change hashes underlying a pair of
///        iterators.
///
/// \param[in] change_hashes1 A pointer to an `AMchangeHashes` struct.
/// \param[in] change_hashes2 A pointer to an `AMchangeHashes` struct.
/// \return `-1` if \p change_hashes1 `<` \p change_hashes2, `0` if
///         \p change_hashes1 `==` \p change_hashes2 and `1` if
///         \p change_hashes1 `>` \p change_hashes2.
/// \pre \p change_hashes1 must be a valid address.
/// \pre \p change_hashes2 must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes1 must be a pointer to a valid AMchangeHashes
/// change_hashes2 must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMchangeHashesCmp(
    change_hashes1: *const AMchangeHashes,
    change_hashes2: *const AMchangeHashes,
) -> isize {
    match (change_hashes1.as_ref(), change_hashes2.as_ref()) {
        (Some(change_hashes1), Some(change_hashes2)) => {
            match change_hashes1.as_ref().cmp(change_hashes2.as_ref()) {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }
        }
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (None, None) => 0,
    }
}

/// \memberof AMchangeHashes
/// \brief Gets the change hash at the current position of an iterator over
///        a sequence of change hashes and then advances/rewinds it by at most
///        \p |n| positions.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \return An `AMbyteSpan` struct with `.src == NULL` when \p change_hashes
///         was previously advanced/rewound past its forward/backward limit.
/// \pre \p change_hashes must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMchangeHashesNext(
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
/// \brief Advances/rewinds an iterator over a sequence of change hashes by at
///        most \p |n| positions and then gets the change hash at its current
///        position.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \param[in] n The direction (\p -n -> backward, \p +n -> forward) and maximum
///              number of positions to advance/rewind.
/// \return An `AMbyteSpan` struct that's null when \p change_hashes is
///         presently advanced/rewound past its forward/backward limit.
/// \pre \p change_hashes must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMchangeHashesPrev(
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

/// \memberof AMchangeHashes
/// \brief Gets the size of the sequence of change hashes underlying an
///        iterator.
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

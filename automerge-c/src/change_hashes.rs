use automerge as am;
use std::cmp::Ordering;
use std::ffi::c_void;
use std::mem::size_of;

use crate::byte_span::AMbyteSpan;

#[repr(C)]
struct Detail {
    len: usize,
    offset: isize,
}

/// \note cbindgen won't propagate the value of a `std::mem::size_of<T>()` call
///       (https://github.com/eqrion/cbindgen/issues/252) but it will
///       propagate the name of a constant initialized from it so if the
///       constant's name is a symbolic representation of the value it can be
///       converted into a number by post-processing the header it generated.
pub const USIZE_USIZE_: usize = size_of::<Detail>();

impl Detail {
    fn new(len: usize, offset: isize) -> Self {
        Self { len, offset }
    }
}

impl From<Detail> for [u8; USIZE_USIZE_] {
    fn from(detail: Detail) -> Self {
        unsafe {
            std::slice::from_raw_parts((&detail as *const Detail) as *const u8, USIZE_USIZE_)
                .try_into()
                .unwrap()
        }
    }
}

/// \struct AMchangeHashes
/// \brief A bidirectional iterator over a sequence of change hashes.
#[repr(C)]
pub struct AMchangeHashes {
    /// A pointer to the first change hash or `NULL`.
    ptr: *const c_void,
    /// Reserved.
    detail: [u8; USIZE_USIZE_],
}

impl AMchangeHashes {
    pub fn new(change_hashes: &[am::ChangeHash]) -> Self {
        Self {
            ptr: change_hashes.as_ptr() as *const c_void,
            detail: Detail::new(change_hashes.len(), 0).into(),
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

    pub fn next(&mut self, n: isize) -> Option<&am::ChangeHash> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        let len = detail.len as isize;
        if detail.offset < -len || detail.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &[am::ChangeHash] = unsafe {
                std::slice::from_raw_parts(self.ptr as *const am::ChangeHash, detail.len)
            };
            let index = (detail.offset + if detail.offset < 0 { len } else { 0 }) as usize;
            let value = &slice[index];
            self.advance(n);
            Some(value)
        }
    }

    pub fn prev(&mut self, n: isize) -> Option<&am::ChangeHash> {
        self.advance(n);
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        let len = detail.len as isize;
        if detail.offset < -len || detail.offset == len {
            // It's stopped.
            None
        } else {
            let slice: &[am::ChangeHash] = unsafe {
                std::slice::from_raw_parts(self.ptr as *const am::ChangeHash, detail.len)
            };
            let index = (detail.offset + if detail.offset < 0 { len } else { 0 }) as usize;
            Some(&slice[index])
        }
    }

    pub fn reverse(&self) -> Self {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        Self {
            ptr: self.ptr,
            detail: Detail::new(detail.len, -(detail.offset + 1)).into(),
        }
    }
}

impl AsRef<[am::ChangeHash]> for AMchangeHashes {
    fn as_ref(&self) -> &[am::ChangeHash] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(self.ptr as *const am::ChangeHash, detail.len) }
    }
}

impl Default for AMchangeHashes {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null(),
            detail: [0; USIZE_USIZE_],
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
/// \return An `AMbyteSpan` struct with `.src == NULL` when \p change_hashes
///         is presently advanced/rewound past its forward/backward limit.
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
        change_hashes.len()
    } else {
        0
    }
}

/// \memberof AMchangeHashes
/// \brief Creates a reversed copy of a change hashes iterator.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \return An `AMchangeHashes` struct
/// \pre \p change_hashes must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMchangeHashesReverse(
    change_hashes: *const AMchangeHashes,
) -> AMchangeHashes {
    if let Some(change_hashes) = change_hashes.as_ref() {
        change_hashes.reverse()
    } else {
        AMchangeHashes::default()
    }
}

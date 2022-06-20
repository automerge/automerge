use automerge as am;
use std::cmp::Ordering;
use std::ffi::c_void;
use std::mem::size_of;

use crate::byte_span::AMbyteSpan;
use crate::result::{to_result, AMresult};

#[repr(C)]
struct Detail {
    len: usize,
    offset: isize,
    ptr: *const c_void,
}

/// \note cbindgen won't propagate the value of a `std::mem::size_of<T>()` call
///       (https://github.com/eqrion/cbindgen/issues/252) but it will
///       propagate the name of a constant initialized from it so if the
///       constant's name is a symbolic representation of the value it can be
///       converted into a number by post-processing the header it generated.
pub const USIZE_USIZE_USIZE_: usize = size_of::<Detail>();

impl Detail {
    fn new(change_hashes: &[am::ChangeHash], offset: isize) -> Self {
        Self {
            len: change_hashes.len(),
            offset,
            ptr: change_hashes.as_ptr() as *const c_void,
        }
    }

    pub fn advance(&mut self, n: isize) {
        if n == 0 {
            return;
        }
        let len = self.len as isize;
        self.offset = if self.offset < 0 {
            /* It's reversed. */
            std::cmp::max(-(len + 1), std::cmp::min(self.offset - n, -1))
        } else {
            std::cmp::max(0, std::cmp::min(self.offset + n, len))
        }
    }

    pub fn get_index(&self) -> usize {
        (self.offset
            + if self.offset < 0 {
                self.len as isize
            } else {
                0
            }) as usize
    }

    pub fn next(&mut self, n: isize) -> Option<&am::ChangeHash> {
        if self.is_stopped() {
            return None;
        }
        let slice: &[am::ChangeHash] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const am::ChangeHash, self.len) };
        let value = &slice[self.get_index()];
        self.advance(n);
        Some(value)
    }

    pub fn is_stopped(&self) -> bool {
        let len = self.len as isize;
        self.offset < -len || self.offset == len
    }

    pub fn prev(&mut self, n: isize) -> Option<&am::ChangeHash> {
        /* Check for rewinding. */
        let prior_offset = self.offset;
        self.advance(-n);
        if (self.offset == prior_offset) || self.is_stopped() {
            return None;
        }
        let slice: &[am::ChangeHash] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const am::ChangeHash, self.len) };
        Some(&slice[self.get_index()])
    }

    pub fn reversed(&self) -> Self {
        Self {
            len: self.len,
            offset: -(self.offset + 1),
            ptr: self.ptr,
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

/// \struct AMchangeHashes
/// \brief A random-access iterator over a sequence of change hashes.
#[repr(C)]
pub struct AMchangeHashes {
    /// An implementation detail that is intentionally opaque.
    /// \warning Modifying \p detail will cause undefined behavior.
    /// \note The actual size of \p detail will vary by platform, this is just
    ///       the one for the platform this documentation was built on.
    detail: [u8; USIZE_USIZE_USIZE_],
}

impl AMchangeHashes {
    pub fn new(change_hashes: &[am::ChangeHash]) -> Self {
        Self {
            detail: Detail::new(change_hashes, 0).into(),
        }
    }

    pub fn advance(&mut self, n: isize) {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.advance(n);
    }

    pub fn len(&self) -> usize {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        detail.len
    }

    pub fn next(&mut self, n: isize) -> Option<&am::ChangeHash> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.next(n)
    }

    pub fn prev(&mut self, n: isize) -> Option<&am::ChangeHash> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.prev(n)
    }

    pub fn reversed(&self) -> Self {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        Self {
            detail: detail.reversed().into(),
        }
    }
}

impl AsRef<[am::ChangeHash]> for AMchangeHashes {
    fn as_ref(&self) -> &[am::ChangeHash] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(detail.ptr as *const am::ChangeHash, detail.len) }
    }
}

impl Default for AMchangeHashes {
    fn default() -> Self {
        Self {
            detail: [0; USIZE_USIZE_USIZE_],
        }
    }
}

/// \memberof AMchangeHashes
/// \brief Advances an iterator over a sequence of change hashes by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
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

/// \memberof AMchangeHashesInit
/// \brief Allocates an iterator over a sequence of change hashes and
///        initializes it from a sequence of byte spans.
///
/// \param[in] src A pointer to an array of `AMbyteSpan` structs.
/// \param[in] count The number of `AMbyteSpan` structs to copy from \p src.
/// \return A pointer to an `AMresult` struct containing an `AMchangeHashes`
///         struct.
/// \pre \p src must be a valid address.
/// \pre `0 <=` \p count `<=` size of \p src.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// src must be an AMbyteSpan array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMchangeHashesInit(src: *const AMbyteSpan, count: usize) -> *mut AMresult {
    let mut change_hashes = Vec::<am::ChangeHash>::new();
    for n in 0..count {
        let byte_span = &*src.add(n);
        let slice = std::slice::from_raw_parts(byte_span.src, byte_span.count);
        match am::ChangeHash::try_from(slice) {
            Ok(change_hash) => {
                change_hashes.push(change_hash);
            }
            Err(e) => {
                return to_result(Err(e));
            }
        }
    }
    to_result(Ok::<Vec<am::ChangeHash>, am::InvalidChangeHashSlice>(
        change_hashes,
    ))
}

/// \memberof AMchangeHashes
/// \brief Gets the change hash at the current position of an iterator over a
///        sequence of change hashes and then advances it by at most \p |n|
///        positions where the sign of \p n is relative to the iterator's
///        direction.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return An `AMbyteSpan` struct with `.src == NULL` when \p change_hashes
///         was previously advanced past its forward/reverse limit.
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
/// \brief Advances an iterator over a sequence of change hashes by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction and then gets the change hash at its new
///        position.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return An `AMbyteSpan` struct with `.src == NULL` when \p change_hashes is
///         presently advanced past its forward/reverse limit.
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
/// \brief Creates an iterator over the same sequence of change hashes as the
///        given one but with the opposite position and direction.
///
/// \param[in] change_hashes A pointer to an `AMchangeHashes` struct.
/// \return An `AMchangeHashes` struct
/// \pre \p change_hashes must be a valid address.
/// \internal
///
/// #Safety
/// change_hashes must be a pointer to a valid AMchangeHashes
#[no_mangle]
pub unsafe extern "C" fn AMchangeHashesReversed(
    change_hashes: *const AMchangeHashes,
) -> AMchangeHashes {
    if let Some(change_hashes) = change_hashes.as_ref() {
        change_hashes.reversed()
    } else {
        AMchangeHashes::default()
    }
}

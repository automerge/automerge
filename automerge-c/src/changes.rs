use automerge as am;
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::mem::size_of;

use crate::byte_span::AMbyteSpan;
use crate::change::AMchange;
use crate::result::{to_result, AMresult};

#[repr(C)]
struct Detail {
    len: usize,
    offset: isize,
    ptr: *const c_void,
    storage: *mut c_void,
}

/// \note cbindgen won't propagate the value of a `std::mem::size_of<T>()` call
///       (https://github.com/eqrion/cbindgen/issues/252) but it will
///       propagate the name of a constant initialized from it so if the
///       constant's name is a symbolic representation of the value it can be
///       converted into a number by post-processing the header it generated.
pub const USIZE_USIZE_USIZE_USIZE_: usize = size_of::<Detail>();

impl Detail {
    fn new(changes: &[am::Change], offset: isize, storage: &mut BTreeMap<usize, AMchange>) -> Self {
        let storage: *mut BTreeMap<usize, AMchange> = storage;
        Self {
            len: changes.len(),
            offset,
            ptr: changes.as_ptr() as *const c_void,
            storage: storage as *mut c_void,
        }
    }

    pub fn advance(&mut self, n: isize) {
        if n == 0 {
            return;
        }
        let len = self.len as isize;
        self.offset = if self.offset < 0 {
            // It's reversed.
            let unclipped = self.offset.checked_sub(n).unwrap_or(isize::MIN);
            if unclipped >= 0 {
                // Clip it to the forward stop.
                len
            } else {
                std::cmp::min(std::cmp::max(-(len + 1), unclipped), -1)
            }
        } else {
            let unclipped = self.offset.checked_add(n).unwrap_or(isize::MAX);
            if unclipped < 0 {
                // Clip it to the reverse stop.
                -(len + 1)
            } else {
                std::cmp::max(0, std::cmp::min(unclipped, len))
            }
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

    pub fn next(&mut self, n: isize) -> Option<*const AMchange> {
        if self.is_stopped() {
            return None;
        }
        let slice: &mut [am::Change] =
            unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut am::Change, self.len) };
        let storage = unsafe { &mut *(self.storage as *mut BTreeMap<usize, AMchange>) };
        let index = self.get_index();
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

    pub fn is_stopped(&self) -> bool {
        let len = self.len as isize;
        self.offset < -len || self.offset == len
    }

    pub fn prev(&mut self, n: isize) -> Option<*const AMchange> {
        self.advance(-n);
        if self.is_stopped() {
            return None;
        }
        let slice: &mut [am::Change] =
            unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut am::Change, self.len) };
        let storage = unsafe { &mut *(self.storage as *mut BTreeMap<usize, AMchange>) };
        let index = self.get_index();
        Some(match storage.get_mut(&index) {
            Some(value) => value,
            None => {
                storage.insert(index, AMchange::new(&mut slice[index]));
                storage.get_mut(&index).unwrap()
            }
        })
    }

    pub fn reversed(&self) -> Self {
        Self {
            len: self.len,
            offset: -(self.offset + 1),
            ptr: self.ptr,
            storage: self.storage,
        }
    }

    pub fn rewound(&self) -> Self {
        Self {
            len: self.len,
            offset: if self.offset < 0 { -1 } else { 0 },
            ptr: self.ptr,
            storage: self.storage,
        }
    }
}

impl From<Detail> for [u8; USIZE_USIZE_USIZE_USIZE_] {
    fn from(detail: Detail) -> Self {
        unsafe {
            std::slice::from_raw_parts(
                (&detail as *const Detail) as *const u8,
                USIZE_USIZE_USIZE_USIZE_,
            )
            .try_into()
            .unwrap()
        }
    }
}

/// \struct AMchanges
/// \installed_headerfile
/// \brief A random-access iterator over a sequence of changes.
#[repr(C)]
#[derive(Eq, PartialEq)]
pub struct AMchanges {
    /// An implementation detail that is intentionally opaque.
    /// \warning Modifying \p detail will cause undefined behavior.
    /// \note The actual size of \p detail will vary by platform, this is just
    ///       the one for the platform this documentation was built on.
    detail: [u8; USIZE_USIZE_USIZE_USIZE_],
}

impl AMchanges {
    pub fn new(changes: &[am::Change], storage: &mut BTreeMap<usize, AMchange>) -> Self {
        Self {
            detail: Detail::new(changes, 0, &mut *storage).into(),
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

    pub fn next(&mut self, n: isize) -> Option<*const AMchange> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.next(n)
    }

    pub fn prev(&mut self, n: isize) -> Option<*const AMchange> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.prev(n)
    }

    pub fn reversed(&self) -> Self {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        Self {
            detail: detail.reversed().into(),
        }
    }

    pub fn rewound(&self) -> Self {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        Self {
            detail: detail.rewound().into(),
        }
    }
}

impl AsRef<[am::Change]> for AMchanges {
    fn as_ref(&self) -> &[am::Change] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(detail.ptr as *const am::Change, detail.len) }
    }
}

impl Default for AMchanges {
    fn default() -> Self {
        Self {
            detail: [0; USIZE_USIZE_USIZE_USIZE_],
        }
    }
}

/// \memberof AMchanges
/// \brief Advances an iterator over a sequence of changes by at most \p |n|
///        positions where the sign of \p n is relative to the iterator's
///        direction.
///
/// \param[in,out] changes A pointer to an `AMchanges` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \pre \p changes `!= NULL`.
/// \internal
///
/// #Safety
/// changes must be a valid pointer to an AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesAdvance(changes: *mut AMchanges, n: isize) {
    if let Some(changes) = changes.as_mut() {
        changes.advance(n);
    };
}

/// \memberof AMchanges
/// \brief Tests the equality of two sequences of changes underlying a pair of
///        iterators.
///
/// \param[in] changes1 A pointer to an `AMchanges` struct.
/// \param[in] changes2 A pointer to an `AMchanges` struct.
/// \return `true` if \p changes1 `==` \p changes2 and `false` otherwise.
/// \pre \p changes1 `!= NULL`.
/// \pre \p changes2 `!= NULL`.
/// \internal
///
/// #Safety
/// changes1 must be a valid pointer to an AMchanges
/// changes2 must be a valid pointer to an AMchanges
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
/// \brief Allocates an iterator over a sequence of changes and initializes it
///        from a sequence of byte spans.
///
/// \param[in] src A pointer to an array of `AMbyteSpan` structs.
/// \param[in] count The number of `AMbyteSpan` structs to copy from \p src.
/// \return A pointer to an `AMresult` struct containing an `AMchanges` struct.
/// \pre \p src `!= NULL`.
/// \pre `0 <` \p count `<= sizeof(`\p src`) / sizeof(AMbyteSpan)`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// src must be an AMbyteSpan array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMchangesInit(src: *const AMbyteSpan, count: usize) -> *mut AMresult {
    let mut changes = Vec::<am::Change>::new();
    for n in 0..count {
        let byte_span = &*src.add(n);
        let slice = std::slice::from_raw_parts(byte_span.src, byte_span.count);
        match slice.try_into() {
            Ok(change) => {
                changes.push(change);
            }
            Err(e) => {
                return to_result(Err::<Vec<am::Change>, am::LoadChangeError>(e));
            }
        }
    }
    to_result(Ok::<Vec<am::Change>, am::LoadChangeError>(changes))
}

/// \memberof AMchanges
/// \brief Gets the change at the current position of an iterator over a
///        sequence of changes and then advances it by at most \p |n| positions
///        where the sign of \p n is relative to the iterator's direction.
///
/// \param[in,out] changes A pointer to an `AMchanges` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMchange` struct that's `NULL` when \p changes was
///         previously advanced past its forward/reverse limit.
/// \pre \p changes `!= NULL`.
/// \internal
///
/// #Safety
/// changes must be a valid pointer to an AMchanges
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
/// \brief Advances an iterator over a sequence of changes by at most \p |n|
///        positions where the sign of \p n is relative to the iterator's
///        direction and then gets the change at its new position.
///
/// \param[in,out] changes A pointer to an `AMchanges` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMchange` struct that's `NULL` when \p changes is
///         presently advanced past its forward/reverse limit.
/// \pre \p changes `!= NULL`.
/// \internal
///
/// #Safety
/// changes must be a valid pointer to an AMchanges
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
/// \pre \p changes `!= NULL`.
/// \internal
///
/// #Safety
/// changes must be a valid pointer to an AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesSize(changes: *const AMchanges) -> usize {
    if let Some(changes) = changes.as_ref() {
        changes.len()
    } else {
        0
    }
}

/// \memberof AMchanges
/// \brief Creates an iterator over the same sequence of changes as the given
///        one but with the opposite position and direction.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \return An `AMchanges` struct.
/// \pre \p changes `!= NULL`.
/// \internal
///
/// #Safety
/// changes must be a valid pointer to an AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesReversed(changes: *const AMchanges) -> AMchanges {
    if let Some(changes) = changes.as_ref() {
        changes.reversed()
    } else {
        AMchanges::default()
    }
}

/// \memberof AMchanges
/// \brief Creates an iterator at the starting position over the same sequence
///        of changes as the given one.
///
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \return An `AMchanges` struct
/// \pre \p changes `!= NULL`.
/// \internal
///
/// #Safety
/// changes must be a valid pointer to an AMchanges
#[no_mangle]
pub unsafe extern "C" fn AMchangesRewound(changes: *const AMchanges) -> AMchanges {
    if let Some(changes) = changes.as_ref() {
        changes.rewound()
    } else {
        AMchanges::default()
    }
}

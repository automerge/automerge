use std::cmp::Ordering;
use std::ffi::c_void;
use std::mem::size_of;
use std::os::raw::c_char;

use crate::byte_span::AMbyteSpan;

/// \brief Creates a string view from a C string.
///
/// \param[in] c_str A UTF-8 C string.
/// \return A UTF-8 string view as an `AMbyteSpan` struct.
/// \internal
///
/// #Safety
/// c_str must be a null-terminated array of `c_char`
#[no_mangle]
pub unsafe extern "C" fn AMstr(c_str: *const c_char) -> AMbyteSpan {
    c_str.into()
}

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
    fn new(strings: &[String], offset: isize) -> Self {
        Self {
            len: strings.len(),
            offset,
            ptr: strings.as_ptr() as *const c_void,
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

    pub fn next(&mut self, n: isize) -> Option<AMbyteSpan> {
        if self.is_stopped() {
            return None;
        }
        let slice: &[String] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const String, self.len) };
        let value = slice[self.get_index()].as_bytes().into();
        self.advance(n);
        Some(value)
    }

    pub fn is_stopped(&self) -> bool {
        let len = self.len as isize;
        self.offset < -len || self.offset == len
    }

    pub fn prev(&mut self, n: isize) -> Option<AMbyteSpan> {
        self.advance(-n);
        if self.is_stopped() {
            return None;
        }
        let slice: &[String] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const String, self.len) };
        Some(slice[self.get_index()].as_bytes().into())
    }

    pub fn reversed(&self) -> Self {
        Self {
            len: self.len,
            offset: -(self.offset + 1),
            ptr: self.ptr,
        }
    }

    pub fn rewound(&self) -> Self {
        Self {
            len: self.len,
            offset: if self.offset < 0 { -1 } else { 0 },
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

/// \struct AMstrs
/// \installed_headerfile
/// \brief A random-access iterator over a sequence of UTF-8 strings.
#[repr(C)]
#[derive(Eq, PartialEq)]
pub struct AMstrs {
    /// An implementation detail that is intentionally opaque.
    /// \warning Modifying \p detail will cause undefined behavior.
    /// \note The actual size of \p detail will vary by platform, this is just
    ///       the one for the platform this documentation was built on.
    detail: [u8; USIZE_USIZE_USIZE_],
}

impl AMstrs {
    pub fn new(strings: &[String]) -> Self {
        Self {
            detail: Detail::new(strings, 0).into(),
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

    pub fn next(&mut self, n: isize) -> Option<AMbyteSpan> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.next(n)
    }

    pub fn prev(&mut self, n: isize) -> Option<AMbyteSpan> {
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

impl AsRef<[String]> for AMstrs {
    fn as_ref(&self) -> &[String] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(detail.ptr as *const String, detail.len) }
    }
}

impl Default for AMstrs {
    fn default() -> Self {
        Self {
            detail: [0; USIZE_USIZE_USIZE_],
        }
    }
}

/// \memberof AMstrs
/// \brief Advances an iterator over a sequence of UTF-8 strings by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction.
///
/// \param[in,out] strs A pointer to an `AMstrs` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \pre \p strs `!= NULL`.
/// \internal
///
/// #Safety
/// strs must be a valid pointer to an AMstrs
#[no_mangle]
pub unsafe extern "C" fn AMstrsAdvance(strs: *mut AMstrs, n: isize) {
    if let Some(strs) = strs.as_mut() {
        strs.advance(n);
    };
}

/// \memberof AMstrs
/// \brief Compares the sequences of UTF-8 strings underlying a pair of
///        iterators.
///
/// \param[in] strs1 A pointer to an `AMstrs` struct.
/// \param[in] strs2 A pointer to an `AMstrs` struct.
/// \return `-1` if \p strs1 `<` \p strs2, `0` if
///         \p strs1 `==` \p strs2 and `1` if
///         \p strs1 `>` \p strs2.
/// \pre \p strs1 `!= NULL`.
/// \pre \p strs2 `!= NULL`.
/// \internal
///
/// #Safety
/// strs1 must be a valid pointer to an AMstrs
/// strs2 must be a valid pointer to an AMstrs
#[no_mangle]
pub unsafe extern "C" fn AMstrsCmp(strs1: *const AMstrs, strs2: *const AMstrs) -> isize {
    match (strs1.as_ref(), strs2.as_ref()) {
        (Some(strs1), Some(strs2)) => match strs1.as_ref().cmp(strs2.as_ref()) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        },
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (None, None) => 0,
    }
}

/// \memberof AMstrs
/// \brief Gets the key at the current position of an iterator over a sequence
///        of UTF-8 strings and then advances it by at most \p |n| positions
///        where the sign of \p n is relative to the iterator's direction.
///
/// \param[in,out] strs A pointer to an `AMstrs` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A UTF-8 string view as an `AMbyteSpan` struct that's `AMstr(NULL)`
///         when \p strs was previously advanced past its forward/reverse limit.
/// \pre \p strs `!= NULL`.
/// \internal
///
/// #Safety
/// strs must be a valid pointer to an AMstrs
#[no_mangle]
pub unsafe extern "C" fn AMstrsNext(strs: *mut AMstrs, n: isize) -> AMbyteSpan {
    if let Some(strs) = strs.as_mut() {
        if let Some(key) = strs.next(n) {
            return key
        }
    }
    Default::default()
}

/// \memberof AMstrs
/// \brief Advances an iterator over a sequence of UTF-8 strings by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction and then gets the key at its new position.
///
/// \param[in,out] strs A pointer to an `AMstrs` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A UTF-8 string view as an `AMbyteSpan` struct that's `AMstr(NULL)`
///         when \p strs is presently advanced past its forward/reverse limit.
/// \pre \p strs `!= NULL`.
/// \internal
///
/// #Safety
/// strs must be a valid pointer to an AMstrs
#[no_mangle]
pub unsafe extern "C" fn AMstrsPrev(strs: *mut AMstrs, n: isize) -> AMbyteSpan {
    if let Some(strs) = strs.as_mut() {
        if let Some(key) = strs.prev(n) {
            return key;
        }
    }
    Default::default()
}

/// \memberof AMstrs
/// \brief Gets the size of the sequence of UTF-8 strings underlying an
///        iterator.
///
/// \param[in] strs A pointer to an `AMstrs` struct.
/// \return The count of values in \p strs.
/// \pre \p strs `!= NULL`.
/// \internal
///
/// #Safety
/// strs must be a valid pointer to an AMstrs
#[no_mangle]
pub unsafe extern "C" fn AMstrsSize(strs: *const AMstrs) -> usize {
    if let Some(strs) = strs.as_ref() {
        strs.len()
    } else {
        0
    }
}

/// \memberof AMstrs
/// \brief Creates an iterator over the same sequence of UTF-8 strings as the
///        given one but with the opposite position and direction.
///
/// \param[in] strs A pointer to an `AMstrs` struct.
/// \return An `AMstrs` struct.
/// \pre \p strs `!= NULL`.
/// \internal
///
/// #Safety
/// strs must be a valid pointer to an AMstrs
#[no_mangle]
pub unsafe extern "C" fn AMstrsReversed(strs: *const AMstrs) -> AMstrs {
    if let Some(strs) = strs.as_ref() {
        strs.reversed()
    } else {
        AMstrs::default()
    }
}

/// \memberof AMstrs
/// \brief Creates an iterator at the starting position over the same sequence
///        of UTF-8 strings as the given one.
///
/// \param[in] strs A pointer to an `AMstrs` struct.
/// \return An `AMstrs` struct
/// \pre \p strs `!= NULL`.
/// \internal
///
/// #Safety
/// strs must be a valid pointer to an AMstrs
#[no_mangle]
pub unsafe extern "C" fn AMstrsRewound(strs: *const AMstrs) -> AMstrs {
    if let Some(strs) = strs.as_ref() {
        strs.rewound()
    } else {
        Default::default()
    }
}

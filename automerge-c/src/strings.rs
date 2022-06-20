use std::cmp::Ordering;
use std::ffi::{c_void, CString};
use std::mem::size_of;
use std::os::raw::c_char;

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
    fn new(cstrings: &[CString], offset: isize) -> Self {
        Self {
            len: cstrings.len(),
            offset,
            ptr: cstrings.as_ptr() as *const c_void,
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

    pub fn next(&mut self, n: isize) -> Option<*const c_char> {
        if self.is_stopped() {
            return None;
        }
        let slice: &[CString] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const CString, self.len) };
        let value = slice[self.get_index()].as_ptr();
        self.advance(n);
        Some(value)
    }

    pub fn is_stopped(&self) -> bool {
        let len = self.len as isize;
        self.offset < -len || self.offset == len
    }

    pub fn prev(&mut self, n: isize) -> Option<*const c_char> {
        /* Check for rewinding. */
        let prior_offset = self.offset;
        self.advance(-n);
        if (self.offset == prior_offset) || self.is_stopped() {
            return None;
        }
        let slice: &[CString] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const CString, self.len) };
        Some(slice[self.get_index()].as_ptr())
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

/// \struct AMstrings
/// \brief A random-access iterator over a sequence of UTF-8 strings.
#[repr(C)]
pub struct AMstrings {
    /// An implementation detail that is intentionally opaque.
    /// \warning Modifying \p detail will cause undefined behavior.
    /// \note The actual size of \p detail will vary by platform, this is just
    ///       the one for the platform this documentation was built on.
    detail: [u8; USIZE_USIZE_USIZE_],
}

impl AMstrings {
    pub fn new(cstrings: &[CString]) -> Self {
        Self {
            detail: Detail::new(cstrings, 0).into(),
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

    pub fn next(&mut self, n: isize) -> Option<*const c_char> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.next(n)
    }

    pub fn prev(&mut self, n: isize) -> Option<*const c_char> {
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

impl AsRef<[String]> for AMstrings {
    fn as_ref(&self) -> &[String] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(detail.ptr as *const String, detail.len) }
    }
}

impl Default for AMstrings {
    fn default() -> Self {
        Self {
            detail: [0; USIZE_USIZE_USIZE_],
        }
    }
}

/// \memberof AMstrings
/// \brief Advances an iterator over a sequence of UTF-8 strings by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction.
///
/// \param[in] strings A pointer to an `AMstrings` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \pre \p strings must be a valid address.
/// \internal
///
/// #Safety
/// strings must be a pointer to a valid AMstrings
#[no_mangle]
pub unsafe extern "C" fn AMstringsAdvance(strings: *mut AMstrings, n: isize) {
    if let Some(strings) = strings.as_mut() {
        strings.advance(n);
    };
}

/// \memberof AMstrings
/// \brief Compares the sequences of UTF-8 strings underlying a pair of
///        iterators.
///
/// \param[in] strings1 A pointer to an `AMstrings` struct.
/// \param[in] strings2 A pointer to an `AMstrings` struct.
/// \return `-1` if \p strings1 `<` \p strings2, `0` if
///         \p strings1 `==` \p strings2 and `1` if
///         \p strings1 `>` \p strings2.
/// \pre \p strings1 must be a valid address.
/// \pre \p strings2 must be a valid address.
/// \internal
///
/// #Safety
/// strings1 must be a pointer to a valid AMstrings
/// strings2 must be a pointer to a valid AMstrings
#[no_mangle]
pub unsafe extern "C" fn AMstringsCmp(
    strings1: *const AMstrings,
    strings2: *const AMstrings,
) -> isize {
    match (strings1.as_ref(), strings2.as_ref()) {
        (Some(strings1), Some(strings2)) => match strings1.as_ref().cmp(strings2.as_ref()) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        },
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (None, None) => 0,
    }
}

/// \memberof AMstrings
/// \brief Gets the key at the current position of an iterator over a
///        sequence of UTF-8 strings and then advances it by at most \p |n|
///        positions where the sign of \p n is relative to the iterator's direction.
///
/// \param[in] strings A pointer to an `AMstrings` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A UTF-8 string that's `NULL` when \p strings was previously
///         advanced past its forward/reverse limit.
/// \pre \p strings must be a valid address.
/// \internal
///
/// #Safety
/// strings must be a pointer to a valid AMstrings
#[no_mangle]
pub unsafe extern "C" fn AMstringsNext(strings: *mut AMstrings, n: isize) -> *const c_char {
    if let Some(strings) = strings.as_mut() {
        if let Some(key) = strings.next(n) {
            return key;
        }
    }
    std::ptr::null()
}

/// \memberof AMstrings
/// \brief Advances an iterator over a sequence of UTF-8 strings by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction and then gets the key at its new position.
///
/// \param[in] strings A pointer to an `AMstrings` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A UTF-8 string that's `NULL` when \p strings is presently advanced
///         past its forward/reverse limit.
/// \pre \p strings must be a valid address.
/// \internal
///
/// #Safety
/// strings must be a pointer to a valid AMstrings
#[no_mangle]
pub unsafe extern "C" fn AMstringsPrev(strings: *mut AMstrings, n: isize) -> *const c_char {
    if let Some(strings) = strings.as_mut() {
        if let Some(key) = strings.prev(n) {
            return key;
        }
    }
    std::ptr::null()
}

/// \memberof AMstrings
/// \brief Gets the size of the sequence of UTF-8 strings underlying an
///        iterator.
///
/// \param[in] strings A pointer to an `AMstrings` struct.
/// \return The count of values in \p strings.
/// \pre \p strings must be a valid address.
/// \internal
///
/// #Safety
/// strings must be a pointer to a valid AMstrings
#[no_mangle]
pub unsafe extern "C" fn AMstringsSize(strings: *const AMstrings) -> usize {
    if let Some(strings) = strings.as_ref() {
        strings.len()
    } else {
        0
    }
}

/// \memberof AMstrings
/// \brief Creates an iterator over the same sequence of UTF-8 strings as the
///        given one but with the opposite position and direction.
///
/// \param[in] strings A pointer to an `AMstrings` struct.
/// \return An `AMstrings` struct.
/// \pre \p strings must be a valid address.
/// \internal
///
/// #Safety
/// strings must be a pointer to a valid AMstrings
#[no_mangle]
pub unsafe extern "C" fn AMstringsReversed(strings: *const AMstrings) -> AMstrings {
    if let Some(strings) = strings.as_ref() {
        strings.reversed()
    } else {
        AMstrings::default()
    }
}

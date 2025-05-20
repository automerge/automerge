use automerge as am;

use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::size_of;

use crate::item::AMitem;
use crate::result::AMresult;

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
    fn new(items: &[AMitem], offset: isize) -> Self {
        Self {
            len: items.len(),
            offset,
            ptr: items.as_ptr() as *mut c_void,
        }
    }

    fn advance(&mut self, n: isize) {
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

    fn get_index(&self) -> usize {
        (self.offset
            + if self.offset < 0 {
                self.len as isize
            } else {
                0
            }) as usize
    }

    fn next(&mut self, n: isize) -> Option<&mut AMitem> {
        if self.is_stopped() {
            return None;
        }
        let slice: &mut [AMitem] =
            unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut AMitem, self.len) };
        let value = &mut slice[self.get_index()];
        self.advance(n);
        Some(value)
    }

    fn is_stopped(&self) -> bool {
        let len = self.len as isize;
        self.offset < -len || self.offset == len
    }

    fn prev(&mut self, n: isize) -> Option<&mut AMitem> {
        self.advance(-n);
        if self.is_stopped() {
            return None;
        }
        let slice: &mut [AMitem] =
            unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut AMitem, self.len) };
        Some(&mut slice[self.get_index()])
    }

    fn reversed(&self) -> Self {
        Self {
            len: self.len,
            offset: -(self.offset + 1),
            ptr: self.ptr,
        }
    }

    fn rewound(&self) -> Self {
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

/// \struct AMitems
/// \installed_headerfile
/// \brief A random-access iterator over a sequence of `AMitem` structs.
#[repr(C)]
#[derive(Eq, PartialEq)]
pub struct AMitems<'a> {
    /// An implementation detail that is intentionally opaque.
    /// \warning Modifying \p detail will cause undefined behavior.
    /// \note The actual size of \p detail will vary by platform, this is just
    ///       the one for the platform this documentation was built on.
    detail: [u8; USIZE_USIZE_USIZE_],
    phantom: PhantomData<&'a mut AMresult>,
}

impl AMitems<'_> {
    pub fn new(items: &[AMitem]) -> Self {
        Self {
            detail: Detail::new(items, 0).into(),
            phantom: PhantomData,
        }
    }

    fn advance(&mut self, n: isize) {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.advance(n);
    }

    fn len(&self) -> usize {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        detail.len
    }

    fn next(&mut self, n: isize) -> Option<&mut AMitem> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.next(n)
    }

    fn prev(&mut self, n: isize) -> Option<&mut AMitem> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.prev(n)
    }

    fn reversed(&self) -> Self {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        Self {
            detail: detail.reversed().into(),
            phantom: PhantomData,
        }
    }

    fn rewound(&self) -> Self {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        Self {
            detail: detail.rewound().into(),
            phantom: PhantomData,
        }
    }
}

impl AsRef<[AMitem]> for AMitems<'_> {
    fn as_ref(&self) -> &[AMitem] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(detail.ptr as *const AMitem, detail.len) }
    }
}

impl Default for AMitems<'_> {
    fn default() -> Self {
        Self {
            detail: [0; USIZE_USIZE_USIZE_],
            phantom: PhantomData,
        }
    }
}

impl TryFrom<&AMitems<'_>> for Vec<am::Change> {
    type Error = am::AutomergeError;

    fn try_from(items: &AMitems<'_>) -> Result<Self, Self::Error> {
        let mut changes = Vec::<am::Change>::with_capacity(items.len());
        for item in items.as_ref().iter() {
            match <&am::Change>::try_from(item.as_ref()) {
                Ok(change) => {
                    changes.push(change.clone());
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(changes)
    }
}

impl TryFrom<&AMitems<'_>> for Vec<am::ChangeHash> {
    type Error = am::AutomergeError;

    fn try_from(items: &AMitems<'_>) -> Result<Self, Self::Error> {
        let mut change_hashes = Vec::<am::ChangeHash>::with_capacity(items.len());
        for item in items.as_ref().iter() {
            match <&am::ChangeHash>::try_from(item.as_ref()) {
                Ok(change_hash) => {
                    change_hashes.push(*change_hash);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(change_hashes)
    }
}

impl TryFrom<&AMitems<'_>> for Vec<am::ScalarValue> {
    type Error = am::AutomergeError;

    fn try_from(items: &AMitems<'_>) -> Result<Self, Self::Error> {
        let mut scalars = Vec::<am::ScalarValue>::with_capacity(items.len());
        for item in items.as_ref().iter() {
            match <&am::ScalarValue>::try_from(item.as_ref()) {
                Ok(scalar) => {
                    scalars.push(scalar.clone());
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(scalars)
    }
}

/// \memberof AMitems
/// \brief Advances an iterator over a sequence of object items by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction.
///
/// \param[in] items A pointer to an `AMitems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \pre \p items `!= NULL`
/// \internal
///
/// #Safety
/// items must be a valid pointer to an AMitems
#[no_mangle]
pub unsafe extern "C" fn AMitemsAdvance(items: *mut AMitems, n: isize) {
    if let Some(items) = items.as_mut() {
        items.advance(n);
    };
}

/// \memberof AMitems
/// \brief Tests the equality of two sequences of object items underlying a
///        pair of iterators.
///
/// \param[in] items1 A pointer to an `AMitems` struct.
/// \param[in] items2 A pointer to an `AMitems` struct.
/// \return `true` if \p items1 `==` \p items2 and `false` otherwise.
/// \pre \p items1 `!= NULL`
/// \pre \p items1 `!= NULL`
/// \post `!(`\p items1 `&&` \p items2 `) -> false`
/// \internal
///
/// #Safety
/// items1 must be a valid pointer to an AMitems
/// items2 must be a valid pointer to an AMitems
#[no_mangle]
pub unsafe extern "C" fn AMitemsEqual(items1: *const AMitems, items2: *const AMitems) -> bool {
    match (items1.as_ref(), items2.as_ref()) {
        (Some(items1), Some(items2)) => items1.as_ref() == items2.as_ref(),
        (None, None) | (None, Some(_)) | (Some(_), None) => false,
    }
}

/// \memberof AMitems
/// \brief Gets the object item at the current position of an iterator over a
///        sequence of object items and then advances it by at most \p |n|
///        positions where the sign of \p n is relative to the iterator's
///        direction.
///
/// \param[in] items A pointer to an `AMitems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMitem` struct that's `NULL` when \p items
///         was previously advanced past its forward/reverse limit.
/// \pre \p items `!= NULL`
/// \internal
///
/// #Safety
/// items must be a valid pointer to an AMitems
#[no_mangle]
pub unsafe extern "C" fn AMitemsNext(items: *mut AMitems, n: isize) -> *mut AMitem {
    if let Some(items) = items.as_mut() {
        if let Some(item) = items.next(n) {
            return item;
        }
    }
    std::ptr::null_mut()
}

/// \memberof AMitems
/// \brief Advances an iterator over a sequence of object items by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction and then gets the object item at its new
///        position.
///
/// \param[in] items A pointer to an `AMitems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMitem` struct that's `NULL` when \p items
///         is presently advanced past its forward/reverse limit.
/// \pre \p items `!= NULL`
/// \internal
///
/// #Safety
/// items must be a valid pointer to an AMitems
#[no_mangle]
pub unsafe extern "C" fn AMitemsPrev(items: *mut AMitems, n: isize) -> *mut AMitem {
    if let Some(items) = items.as_mut() {
        if let Some(obj_item) = items.prev(n) {
            return obj_item;
        }
    }
    std::ptr::null_mut()
}

/// \memberof AMitems
/// \brief Gets the size of the sequence underlying an iterator.
///
/// \param[in] items A pointer to an `AMitems` struct.
/// \return The count of items in \p items.
/// \pre \p items `!= NULL`
/// \internal
///
/// #Safety
/// items must be a valid pointer to an AMitems
#[no_mangle]
pub unsafe extern "C" fn AMitemsSize(items: *const AMitems) -> usize {
    if let Some(items) = items.as_ref() {
        return items.len();
    }
    0
}

/// \memberof AMitems
/// \brief Creates an iterator over the same sequence of items as the
///        given one but with the opposite position and direction.
///
/// \param[in] items A pointer to an `AMitems` struct.
/// \return An `AMitems` struct
/// \pre \p items `!= NULL`
/// \internal
///
/// #Safety
/// items must be a valid pointer to an AMitems
#[no_mangle]
pub unsafe extern "C" fn AMitemsReversed(items: *const AMitems) -> AMitems {
    if let Some(items) = items.as_ref() {
        return items.reversed();
    }
    Default::default()
}

/// \memberof AMitems
/// \brief Creates an iterator at the starting position over the same sequence
///        of items as the given one.
///
/// \param[in] items A pointer to an `AMitems` struct.
/// \return An `AMitems` struct
/// \pre \p items `!= NULL`
/// \internal
///
/// #Safety
/// items must be a valid pointer to an AMitems
#[no_mangle]
pub unsafe extern "C" fn AMitemsRewound(items: *const AMitems) -> AMitems {
    if let Some(items) = items.as_ref() {
        return items.rewound();
    }
    Default::default()
}

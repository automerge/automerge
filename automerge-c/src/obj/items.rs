use std::ffi::c_void;
use std::mem::size_of;

use crate::obj::item::AMobjItem;

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
    fn new(obj_items: &[AMobjItem], offset: isize) -> Self {
        Self {
            len: obj_items.len(),
            offset,
            ptr: obj_items.as_ptr() as *const c_void,
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

    pub fn next(&mut self, n: isize) -> Option<&AMobjItem> {
        if self.is_stopped() {
            return None;
        }
        let slice: &[AMobjItem] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const AMobjItem, self.len) };
        let value = &slice[self.get_index()];
        self.advance(n);
        Some(value)
    }

    pub fn is_stopped(&self) -> bool {
        let len = self.len as isize;
        self.offset < -len || self.offset == len
    }

    pub fn prev(&mut self, n: isize) -> Option<&AMobjItem> {
        self.advance(-n);
        if self.is_stopped() {
            return None;
        }
        let slice: &[AMobjItem] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const AMobjItem, self.len) };
        Some(&slice[self.get_index()])
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

/// \struct AMobjItems
/// \brief A random-access iterator over a sequence of object items.
#[repr(C)]
#[derive(PartialEq)]
pub struct AMobjItems {
    /// An implementation detail that is intentionally opaque.
    /// \warning Modifying \p detail will cause undefined behavior.
    /// \note The actual size of \p detail will vary by platform, this is just
    ///       the one for the platform this documentation was built on.
    detail: [u8; USIZE_USIZE_USIZE_],
}

impl AMobjItems {
    pub fn new(obj_items: &[AMobjItem]) -> Self {
        Self {
            detail: Detail::new(obj_items, 0).into(),
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

    pub fn next(&mut self, n: isize) -> Option<&AMobjItem> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.next(n)
    }

    pub fn prev(&mut self, n: isize) -> Option<&AMobjItem> {
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

impl AsRef<[AMobjItem]> for AMobjItems {
    fn as_ref(&self) -> &[AMobjItem] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(detail.ptr as *const AMobjItem, detail.len) }
    }
}

impl Default for AMobjItems {
    fn default() -> Self {
        Self {
            detail: [0; USIZE_USIZE_USIZE_],
        }
    }
}

/// \memberof AMobjItems
/// \brief Advances an iterator over a sequence of object items by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction.
///
/// \param[in,out] obj_items A pointer to an `AMobjItems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \pre \p obj_items `!= NULL`.
/// \internal
///
/// #Safety
/// obj_items must be a valid pointer to an AMobjItems
#[no_mangle]
pub unsafe extern "C" fn AMobjItemsAdvance(obj_items: *mut AMobjItems, n: isize) {
    if let Some(obj_items) = obj_items.as_mut() {
        obj_items.advance(n);
    };
}

/// \memberof AMobjItems
/// \brief Tests the equality of two sequences of object items underlying a
///        pair of iterators.
///
/// \param[in] obj_items1 A pointer to an `AMobjItems` struct.
/// \param[in] obj_items2 A pointer to an `AMobjItems` struct.
/// \return `true` if \p obj_items1 `==` \p obj_items2 and `false` otherwise.
/// \pre \p obj_items1 `!= NULL`.
/// \pre \p obj_items2 `!= NULL`.
/// \internal
///
/// #Safety
/// obj_items1 must be a valid pointer to an AMobjItems
/// obj_items2 must be a valid pointer to an AMobjItems
#[no_mangle]
pub unsafe extern "C" fn AMobjItemsEqual(
    obj_items1: *const AMobjItems,
    obj_items2: *const AMobjItems,
) -> bool {
    match (obj_items1.as_ref(), obj_items2.as_ref()) {
        (Some(obj_items1), Some(obj_items2)) => obj_items1.as_ref() == obj_items2.as_ref(),
        (None, Some(_)) | (Some(_), None) | (None, None) => false,
    }
}

/// \memberof AMobjItems
/// \brief Gets the object item at the current position of an iterator over a
///        sequence of object items and then advances it by at most \p |n|
///        positions where the sign of \p n is relative to the iterator's
///        direction.
///
/// \param[in,out] obj_items A pointer to an `AMobjItems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMobjItem` struct that's `NULL` when \p obj_items
///         was previously advanced past its forward/reverse limit.
/// \pre \p obj_items `!= NULL`.
/// \internal
///
/// #Safety
/// obj_items must be a valid pointer to an AMobjItems
#[no_mangle]
pub unsafe extern "C" fn AMobjItemsNext(obj_items: *mut AMobjItems, n: isize) -> *const AMobjItem {
    if let Some(obj_items) = obj_items.as_mut() {
        if let Some(obj_item) = obj_items.next(n) {
            return obj_item;
        }
    }
    std::ptr::null()
}

/// \memberof AMobjItems
/// \brief Advances an iterator over a sequence of object items by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction and then gets the object item at its new
///        position.
///
/// \param[in,out] obj_items A pointer to an `AMobjItems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMobjItem` struct that's `NULL` when \p obj_items
///         is presently advanced past its forward/reverse limit.
/// \pre \p obj_items `!= NULL`.
/// \internal
///
/// #Safety
/// obj_items must be a valid pointer to an AMobjItems
#[no_mangle]
pub unsafe extern "C" fn AMobjItemsPrev(obj_items: *mut AMobjItems, n: isize) -> *const AMobjItem {
    if let Some(obj_items) = obj_items.as_mut() {
        if let Some(obj_item) = obj_items.prev(n) {
            return obj_item;
        }
    }
    std::ptr::null()
}

/// \memberof AMobjItems
/// \brief Gets the size of the sequence of object items underlying an
///        iterator.
///
/// \param[in] obj_items A pointer to an `AMobjItems` struct.
/// \return The count of values in \p obj_items.
/// \pre \p obj_items `!= NULL`.
/// \internal
///
/// #Safety
/// obj_items must be a valid pointer to an AMobjItems
#[no_mangle]
pub unsafe extern "C" fn AMobjItemsSize(obj_items: *const AMobjItems) -> usize {
    if let Some(obj_items) = obj_items.as_ref() {
        obj_items.len()
    } else {
        0
    }
}

/// \memberof AMobjItems
/// \brief Creates an iterator over the same sequence of object items as the
///        given one but with the opposite position and direction.
///
/// \param[in] obj_items A pointer to an `AMobjItems` struct.
/// \return An `AMobjItems` struct
/// \pre \p obj_items `!= NULL`.
/// \internal
///
/// #Safety
/// obj_items must be a valid pointer to an AMobjItems
#[no_mangle]
pub unsafe extern "C" fn AMobjItemsReversed(obj_items: *const AMobjItems) -> AMobjItems {
    if let Some(obj_items) = obj_items.as_ref() {
        obj_items.reversed()
    } else {
        AMobjItems::default()
    }
}

/// \memberof AMobjItems
/// \brief Creates an iterator at the starting position over the same sequence
///        of object items as the given one.
///
/// \param[in] obj_items A pointer to an `AMobjItems` struct.
/// \return An `AMobjItems` struct
/// \pre \p obj_items `!= NULL`.
/// \internal
///
/// #Safety
/// obj_items must be a valid pointer to an AMobjItems
#[no_mangle]
pub unsafe extern "C" fn AMobjItemsRewound(obj_items: *const AMobjItems) -> AMobjItems {
    if let Some(obj_items) = obj_items.as_ref() {
        obj_items.rewound()
    } else {
        AMobjItems::default()
    }
}

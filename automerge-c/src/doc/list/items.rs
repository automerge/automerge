use std::ffi::c_void;
use std::mem::size_of;

use crate::doc::list::item::AMlistItem;

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
    fn new(list_items: &[AMlistItem], offset: isize) -> Self {
        Self {
            len: list_items.len(),
            offset,
            ptr: list_items.as_ptr() as *const c_void,
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

    pub fn next(&mut self, n: isize) -> Option<&AMlistItem> {
        if self.is_stopped() {
            return None;
        }
        let slice: &[AMlistItem] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const AMlistItem, self.len) };
        let value = &slice[self.get_index()];
        self.advance(n);
        Some(value)
    }

    pub fn is_stopped(&self) -> bool {
        let len = self.len as isize;
        self.offset < -len || self.offset == len
    }

    pub fn prev(&mut self, n: isize) -> Option<&AMlistItem> {
        self.advance(-n);
        if self.is_stopped() {
            return None;
        }
        let slice: &[AMlistItem] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const AMlistItem, self.len) };
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

/// \struct AMlistItems
/// \brief A random-access iterator over a sequence of list object items.
#[repr(C)]
#[derive(PartialEq)]
pub struct AMlistItems {
    /// An implementation detail that is intentionally opaque.
    /// \warning Modifying \p detail will cause undefined behavior.
    /// \note The actual size of \p detail will vary by platform, this is just
    ///       the one for the platform this documentation was built on.
    detail: [u8; USIZE_USIZE_USIZE_],
}

impl AMlistItems {
    pub fn new(list_items: &[AMlistItem]) -> Self {
        Self {
            detail: Detail::new(list_items, 0).into(),
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

    pub fn next(&mut self, n: isize) -> Option<&AMlistItem> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.next(n)
    }

    pub fn prev(&mut self, n: isize) -> Option<&AMlistItem> {
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

impl AsRef<[AMlistItem]> for AMlistItems {
    fn as_ref(&self) -> &[AMlistItem] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(detail.ptr as *const AMlistItem, detail.len) }
    }
}

impl Default for AMlistItems {
    fn default() -> Self {
        Self {
            detail: [0; USIZE_USIZE_USIZE_],
        }
    }
}

/// \memberof AMlistItems
/// \brief Advances an iterator over a sequence of list object items by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction.
///
/// \param[in,out] list_items A pointer to an `AMlistItems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \pre \p list_items `!= NULL`.
/// \internal
///
/// #Safety
/// list_items must be a valid pointer to an AMlistItems
#[no_mangle]
pub unsafe extern "C" fn AMlistItemsAdvance(list_items: *mut AMlistItems, n: isize) {
    if let Some(list_items) = list_items.as_mut() {
        list_items.advance(n);
    };
}

/// \memberof AMlistItems
/// \brief Tests the equality of two sequences of list object items underlying
///        a pair of iterators.
///
/// \param[in] list_items1 A pointer to an `AMlistItems` struct.
/// \param[in] list_items2 A pointer to an `AMlistItems` struct.
/// \return `true` if \p list_items1 `==` \p list_items2 and `false` otherwise.
/// \pre \p list_items1 `!= NULL`.
/// \pre \p list_items2 `!= NULL`.
/// \internal
///
/// #Safety
/// list_items1 must be a valid pointer to an AMlistItems
/// list_items2 must be a valid pointer to an AMlistItems
#[no_mangle]
pub unsafe extern "C" fn AMlistItemsEqual(
    list_items1: *const AMlistItems,
    list_items2: *const AMlistItems,
) -> bool {
    match (list_items1.as_ref(), list_items2.as_ref()) {
        (Some(list_items1), Some(list_items2)) => list_items1.as_ref() == list_items2.as_ref(),
        (None, Some(_)) | (Some(_), None) | (None, None) => false,
    }
}

/// \memberof AMlistItems
/// \brief Gets the list object item at the current position of an iterator
///        over a sequence of list object items and then advances it by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction.
///
/// \param[in,out] list_items A pointer to an `AMlistItems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMlistItem` struct that's `NULL` when
///         \p list_items was previously advanced past its forward/reverse
///         limit.
/// \pre \p list_items `!= NULL`.
/// \internal
///
/// #Safety
/// list_items must be a valid pointer to an AMlistItems
#[no_mangle]
pub unsafe extern "C" fn AMlistItemsNext(
    list_items: *mut AMlistItems,
    n: isize,
) -> *const AMlistItem {
    if let Some(list_items) = list_items.as_mut() {
        if let Some(list_item) = list_items.next(n) {
            return list_item;
        }
    }
    std::ptr::null()
}

/// \memberof AMlistItems
/// \brief Advances an iterator over a sequence of list object items by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction and then gets the list object item at its new
///        position.
///
/// \param[in,out] list_items A pointer to an `AMlistItems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMlistItem` struct that's `NULL` when
///         \p list_items is presently advanced past its forward/reverse limit.
/// \pre \p list_items `!= NULL`.
/// \internal
///
/// #Safety
/// list_items must be a valid pointer to an AMlistItems
#[no_mangle]
pub unsafe extern "C" fn AMlistItemsPrev(
    list_items: *mut AMlistItems,
    n: isize,
) -> *const AMlistItem {
    if let Some(list_items) = list_items.as_mut() {
        if let Some(list_item) = list_items.prev(n) {
            return list_item;
        }
    }
    std::ptr::null()
}

/// \memberof AMlistItems
/// \brief Gets the size of the sequence of list object items underlying an
///        iterator.
///
/// \param[in] list_items A pointer to an `AMlistItems` struct.
/// \return The count of values in \p list_items.
/// \pre \p list_items `!= NULL`.
/// \internal
///
/// #Safety
/// list_items must be a valid pointer to an AMlistItems
#[no_mangle]
pub unsafe extern "C" fn AMlistItemsSize(list_items: *const AMlistItems) -> usize {
    if let Some(list_items) = list_items.as_ref() {
        list_items.len()
    } else {
        0
    }
}

/// \memberof AMlistItems
/// \brief Creates an iterator over the same sequence of list object items as
///        the given one but with the opposite position and direction.
///
/// \param[in] list_items A pointer to an `AMlistItems` struct.
/// \return An `AMlistItems` struct
/// \pre \p list_items `!= NULL`.
/// \internal
///
/// #Safety
/// list_items must be a valid pointer to an AMlistItems
#[no_mangle]
pub unsafe extern "C" fn AMlistItemsReversed(list_items: *const AMlistItems) -> AMlistItems {
    if let Some(list_items) = list_items.as_ref() {
        list_items.reversed()
    } else {
        AMlistItems::default()
    }
}

/// \memberof AMlistItems
/// \brief Creates an iterator at the starting position over the same sequence
///        of list object items as the given one.
///
/// \param[in] list_items A pointer to an `AMlistItems` struct.
/// \return An `AMlistItems` struct
/// \pre \p list_items `!= NULL`.
/// \internal
///
/// #Safety
/// list_items must be a valid pointer to an AMlistItems
#[no_mangle]
pub unsafe extern "C" fn AMlistItemsRewound(list_items: *const AMlistItems) -> AMlistItems {
    if let Some(list_items) = list_items.as_ref() {
        list_items.rewound()
    } else {
        AMlistItems::default()
    }
}

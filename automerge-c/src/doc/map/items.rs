use std::ffi::c_void;
use std::mem::size_of;

use crate::doc::map::item::AMmapItem;

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
    fn new(map_items: &[AMmapItem], offset: isize) -> Self {
        Self {
            len: map_items.len(),
            offset,
            ptr: map_items.as_ptr() as *const c_void,
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

    pub fn next(&mut self, n: isize) -> Option<&AMmapItem> {
        if self.is_stopped() {
            return None;
        }
        let slice: &[AMmapItem] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const AMmapItem, self.len) };
        let value = &slice[self.get_index()];
        self.advance(n);
        Some(value)
    }

    pub fn is_stopped(&self) -> bool {
        let len = self.len as isize;
        self.offset < -len || self.offset == len
    }

    pub fn prev(&mut self, n: isize) -> Option<&AMmapItem> {
        self.advance(-n);
        if self.is_stopped() {
            return None;
        }
        let slice: &[AMmapItem] =
            unsafe { std::slice::from_raw_parts(self.ptr as *const AMmapItem, self.len) };
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

/// \struct AMmapItems
/// \installed_headerfile
/// \brief A random-access iterator over a sequence of map object items.
#[repr(C)]
#[derive(Eq, PartialEq)]
pub struct AMmapItems {
    /// An implementation detail that is intentionally opaque.
    /// \warning Modifying \p detail will cause undefined behavior.
    /// \note The actual size of \p detail will vary by platform, this is just
    ///       the one for the platform this documentation was built on.
    detail: [u8; USIZE_USIZE_USIZE_],
}

impl AMmapItems {
    pub fn new(map_items: &[AMmapItem]) -> Self {
        Self {
            detail: Detail::new(map_items, 0).into(),
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

    pub fn next(&mut self, n: isize) -> Option<&AMmapItem> {
        let detail = unsafe { &mut *(self.detail.as_mut_ptr() as *mut Detail) };
        detail.next(n)
    }

    pub fn prev(&mut self, n: isize) -> Option<&AMmapItem> {
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

impl AsRef<[AMmapItem]> for AMmapItems {
    fn as_ref(&self) -> &[AMmapItem] {
        let detail = unsafe { &*(self.detail.as_ptr() as *const Detail) };
        unsafe { std::slice::from_raw_parts(detail.ptr as *const AMmapItem, detail.len) }
    }
}

impl Default for AMmapItems {
    fn default() -> Self {
        Self {
            detail: [0; USIZE_USIZE_USIZE_],
        }
    }
}

/// \memberof AMmapItems
/// \brief Advances an iterator over a sequence of map object items by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction.
///
/// \param[in,out] map_items A pointer to an `AMmapItems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \pre \p map_items `!= NULL`.
/// \internal
///
/// #Safety
/// map_items must be a valid pointer to an AMmapItems
#[no_mangle]
pub unsafe extern "C" fn AMmapItemsAdvance(map_items: *mut AMmapItems, n: isize) {
    if let Some(map_items) = map_items.as_mut() {
        map_items.advance(n);
    };
}

/// \memberof AMmapItems
/// \brief Tests the equality of two sequences of map object items underlying
///        a pair of iterators.
///
/// \param[in] map_items1 A pointer to an `AMmapItems` struct.
/// \param[in] map_items2 A pointer to an `AMmapItems` struct.
/// \return `true` if \p map_items1 `==` \p map_items2 and `false` otherwise.
/// \pre \p map_items1 `!= NULL`.
/// \pre \p map_items2 `!= NULL`.
/// \internal
///
/// #Safety
/// map_items1 must be a valid pointer to an AMmapItems
/// map_items2 must be a valid pointer to an AMmapItems
#[no_mangle]
pub unsafe extern "C" fn AMmapItemsEqual(
    map_items1: *const AMmapItems,
    map_items2: *const AMmapItems,
) -> bool {
    match (map_items1.as_ref(), map_items2.as_ref()) {
        (Some(map_items1), Some(map_items2)) => map_items1.as_ref() == map_items2.as_ref(),
        (None, Some(_)) | (Some(_), None) | (None, None) => false,
    }
}

/// \memberof AMmapItems
/// \brief Gets the map object item at the current position of an iterator
///        over a sequence of map object items and then advances it by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction.
///
/// \param[in,out] map_items A pointer to an `AMmapItems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMmapItem` struct that's `NULL` when \p map_items
///         was previously advanced past its forward/reverse limit.
/// \pre \p map_items `!= NULL`.
/// \internal
///
/// #Safety
/// map_items must be a valid pointer to an AMmapItems
#[no_mangle]
pub unsafe extern "C" fn AMmapItemsNext(map_items: *mut AMmapItems, n: isize) -> *const AMmapItem {
    if let Some(map_items) = map_items.as_mut() {
        if let Some(map_item) = map_items.next(n) {
            return map_item;
        }
    }
    std::ptr::null()
}

/// \memberof AMmapItems
/// \brief Advances an iterator over a sequence of map object items by at most
///        \p |n| positions where the sign of \p n is relative to the
///        iterator's direction and then gets the map object item at its new
///        position.
///
/// \param[in,out] map_items A pointer to an `AMmapItems` struct.
/// \param[in] n The direction (\p -n -> opposite, \p n -> same) and maximum
///              number of positions to advance.
/// \return A pointer to an `AMmapItem` struct that's `NULL` when \p map_items
///         is presently advanced past its forward/reverse limit.
/// \pre \p map_items `!= NULL`.
/// \internal
///
/// #Safety
/// map_items must be a valid pointer to an AMmapItems
#[no_mangle]
pub unsafe extern "C" fn AMmapItemsPrev(map_items: *mut AMmapItems, n: isize) -> *const AMmapItem {
    if let Some(map_items) = map_items.as_mut() {
        if let Some(map_item) = map_items.prev(n) {
            return map_item;
        }
    }
    std::ptr::null()
}

/// \memberof AMmapItems
/// \brief Gets the size of the sequence of map object items underlying an
///        iterator.
///
/// \param[in] map_items A pointer to an `AMmapItems` struct.
/// \return The count of values in \p map_items.
/// \pre \p map_items `!= NULL`.
/// \internal
///
/// #Safety
/// map_items must be a valid pointer to an AMmapItems
#[no_mangle]
pub unsafe extern "C" fn AMmapItemsSize(map_items: *const AMmapItems) -> usize {
    if let Some(map_items) = map_items.as_ref() {
        map_items.len()
    } else {
        0
    }
}

/// \memberof AMmapItems
/// \brief Creates an iterator over the same sequence of map object items as
///        the given one but with the opposite position and direction.
///
/// \param[in] map_items A pointer to an `AMmapItems` struct.
/// \return An `AMmapItems` struct
/// \pre \p map_items `!= NULL`.
/// \internal
///
/// #Safety
/// map_items must be a valid pointer to an AMmapItems
#[no_mangle]
pub unsafe extern "C" fn AMmapItemsReversed(map_items: *const AMmapItems) -> AMmapItems {
    if let Some(map_items) = map_items.as_ref() {
        map_items.reversed()
    } else {
        AMmapItems::default()
    }
}

/// \memberof AMmapItems
/// \brief Creates an iterator at the starting position over the same sequence of map object items as the given one.
///
/// \param[in] map_items A pointer to an `AMmapItems` struct.
/// \return An `AMmapItems` struct
/// \pre \p map_items `!= NULL`.
/// \internal
///
/// #Safety
/// map_items must be a valid pointer to an AMmapItems
#[no_mangle]
pub unsafe extern "C" fn AMmapItemsRewound(map_items: *const AMmapItems) -> AMmapItems {
    if let Some(map_items) = map_items.as_ref() {
        map_items.rewound()
    } else {
        AMmapItems::default()
    }
}

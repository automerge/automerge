use automerge as am;
use std::cell::RefCell;
use std::ffi::CString;

use crate::obj::AMobjId;
use crate::result::AMvalue;

/// \struct AMlistItem
/// \installed_headerfile
/// \brief An item in a list object.
#[repr(C)]
pub struct AMlistItem {
    /// The index of an item in a list object.
    index: usize,
    /// The object identifier of an item in a list object.
    obj_id: AMobjId,
    /// The value of an item in a list object.
    value: (am::Value<'static>, RefCell<Option<CString>>),
}

impl AMlistItem {
    pub fn new(index: usize, value: am::Value<'static>, obj_id: am::ObjId) -> Self {
        Self {
            index,
            obj_id: AMobjId::new(obj_id),
            value: (value, Default::default()),
        }
    }
}

impl PartialEq for AMlistItem {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.obj_id == other.obj_id && self.value.0 == other.value.0
    }
}

/*
impl From<&AMlistItem> for (usize, am::Value<'static>, am::ObjId) {
    fn from(list_item: &AMlistItem) -> Self {
        (list_item.index, list_item.value.0.clone(), list_item.obj_id.as_ref().clone())
    }
}
*/

/// \memberof AMlistItem
/// \brief Gets the index of an item in a list object.
///
/// \param[in] list_item A pointer to an `AMlistItem` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p list_item `!= NULL`.
/// \internal
///
/// # Safety
/// list_item must be a valid pointer to an AMlistItem
#[no_mangle]
pub unsafe extern "C" fn AMlistItemIndex(list_item: *const AMlistItem) -> usize {
    if let Some(list_item) = list_item.as_ref() {
        list_item.index
    } else {
        usize::MAX
    }
}

/// \memberof AMlistItem
/// \brief Gets the object identifier of an item in a list object.
///
/// \param[in] list_item A pointer to an `AMlistItem` struct.
/// \return A pointer to an `AMobjId` struct.
/// \pre \p list_item `!= NULL`.
/// \internal
///
/// # Safety
/// list_item must be a valid pointer to an AMlistItem
#[no_mangle]
pub unsafe extern "C" fn AMlistItemObjId(list_item: *const AMlistItem) -> *const AMobjId {
    if let Some(list_item) = list_item.as_ref() {
        &list_item.obj_id
    } else {
        std::ptr::null()
    }
}

/// \memberof AMlistItem
/// \brief Gets the value of an item in a list object.
///
/// \param[in] list_item A pointer to an `AMlistItem` struct.
/// \return An `AMvalue` struct.
/// \pre \p list_item `!= NULL`.
/// \internal
///
/// # Safety
/// list_item must be a valid pointer to an AMlistItem
#[no_mangle]
pub unsafe extern "C" fn AMlistItemValue<'a>(list_item: *const AMlistItem) -> AMvalue<'a> {
    if let Some(list_item) = list_item.as_ref() {
        (&list_item.value.0, &list_item.value.1).into()
    } else {
        AMvalue::Void
    }
}

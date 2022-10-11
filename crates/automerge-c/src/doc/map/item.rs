use automerge as am;
use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;

use crate::obj::AMobjId;
use crate::result::AMvalue;

/// \struct AMmapItem
/// \installed_headerfile
/// \brief An item in a map object.
#[repr(C)]
pub struct AMmapItem {
    /// The key of an item in a map object.
    key: CString,
    /// The object identifier of an item in a map object.
    obj_id: AMobjId,
    /// The value of an item in a map object.
    value: (am::Value<'static>, RefCell<Option<CString>>),
}

impl AMmapItem {
    pub fn new(key: &'static str, value: am::Value<'static>, obj_id: am::ObjId) -> Self {
        Self {
            key: CString::new(key).unwrap(),
            obj_id: AMobjId::new(obj_id),
            value: (value, Default::default()),
        }
    }
}

impl PartialEq for AMmapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.obj_id == other.obj_id && self.value.0 == other.value.0
    }
}

/*
impl From<&AMmapItem> for (String, am::Value<'static>, am::ObjId) {
    fn from(map_item: &AMmapItem) -> Self {
        (map_item.key.into_string().unwrap(), map_item.value.0.clone(), map_item.obj_id.as_ref().clone())
    }
}
*/

/// \memberof AMmapItem
/// \brief Gets the key of an item in a map object.
///
/// \param[in] map_item A pointer to an `AMmapItem` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p map_item `!= NULL`.
/// \internal
///
/// # Safety
/// map_item must be a valid pointer to an AMmapItem
#[no_mangle]
pub unsafe extern "C" fn AMmapItemKey(map_item: *const AMmapItem) -> *const c_char {
    if let Some(map_item) = map_item.as_ref() {
        map_item.key.as_ptr()
    } else {
        std::ptr::null()
    }
}

/// \memberof AMmapItem
/// \brief Gets the object identifier of an item in a map object.
///
/// \param[in] map_item A pointer to an `AMmapItem` struct.
/// \return A pointer to an `AMobjId` struct.
/// \pre \p map_item `!= NULL`.
/// \internal
///
/// # Safety
/// map_item must be a valid pointer to an AMmapItem
#[no_mangle]
pub unsafe extern "C" fn AMmapItemObjId(map_item: *const AMmapItem) -> *const AMobjId {
    if let Some(map_item) = map_item.as_ref() {
        &map_item.obj_id
    } else {
        std::ptr::null()
    }
}

/// \memberof AMmapItem
/// \brief Gets the value of an item in a map object.
///
/// \param[in] map_item A pointer to an `AMmapItem` struct.
/// \return An `AMvalue` struct.
/// \pre \p map_item `!= NULL`.
/// \internal
///
/// # Safety
/// map_item must be a valid pointer to an AMmapItem
#[no_mangle]
pub unsafe extern "C" fn AMmapItemValue<'a>(map_item: *const AMmapItem) -> AMvalue<'a> {
    if let Some(map_item) = map_item.as_ref() {
        (&map_item.value.0, &map_item.value.1).into()
    } else {
        AMvalue::Void
    }
}

use automerge as am;

use crate::byte_span::AMbyteSpan;
use crate::obj::AMobjId;
use crate::result::AMvalue;

/// \struct AMmapItem
/// \installed_headerfile
/// \brief An item in a map object.
pub struct AMmapItem {
    /// The key of an item in a map object.
    key: String,
    /// The object identifier of an item in a map object.
    obj_id: AMobjId,
    /// The value of an item in a map object.
    value: am::Value<'static>,
}

impl AMmapItem {
    pub fn new(key: &'static str, value: am::Value<'static>, obj_id: am::ObjId) -> Self {
        Self {
            key: key.to_string(),
            obj_id: AMobjId::new(obj_id),
            value,
        }
    }
}

impl PartialEq for AMmapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.obj_id == other.obj_id && self.value == other.value
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
/// \return An `AMbyteSpan` view of a UTF-8 string.
/// \pre \p map_item `!= NULL`.
/// \internal
///
/// # Safety
/// map_item must be a valid pointer to an AMmapItem
#[no_mangle]
pub unsafe extern "C" fn AMmapItemKey(map_item: *const AMmapItem) -> AMbyteSpan {
    if let Some(map_item) = map_item.as_ref() {
        map_item.key.as_bytes().into()
    } else {
        Default::default()
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
        (&map_item.value).into()
    } else {
        AMvalue::Void
    }
}

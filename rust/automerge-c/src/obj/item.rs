use automerge as am;

use crate::obj::AMobjId;
use crate::result::AMvalue;

/// \struct AMobjItem
/// \installed_headerfile
/// \brief An item in an object.
pub struct AMobjItem {
    /// The object identifier of an item in an object.
    obj_id: AMobjId,
    /// The value of an item in an object.
    value: am::Value<'static>,
}

impl AMobjItem {
    pub fn new(value: am::Value<'static>, obj_id: am::ObjId) -> Self {
        Self {
            obj_id: AMobjId::new(obj_id),
            value,
        }
    }
}

impl PartialEq for AMobjItem {
    fn eq(&self, other: &Self) -> bool {
        self.obj_id == other.obj_id && self.value == other.value
    }
}

impl From<&AMobjItem> for (am::Value<'static>, am::ObjId) {
    fn from(obj_item: &AMobjItem) -> Self {
        (obj_item.value.clone(), obj_item.obj_id.as_ref().clone())
    }
}

/// \memberof AMobjItem
/// \brief Gets the object identifier of an item in an object.
///
/// \param[in] obj_item A pointer to an `AMobjItem` struct.
/// \return A pointer to an `AMobjId` struct.
/// \pre \p obj_item `!= NULL`.
/// \internal
///
/// # Safety
/// obj_item must be a valid pointer to an AMobjItem
#[no_mangle]
pub unsafe extern "C" fn AMobjItemObjId(obj_item: *const AMobjItem) -> *const AMobjId {
    if let Some(obj_item) = obj_item.as_ref() {
        &obj_item.obj_id
    } else {
        std::ptr::null()
    }
}

/// \memberof AMobjItem
/// \brief Gets the value of an item in an object.
///
/// \param[in] obj_item A pointer to an `AMobjItem` struct.
/// \return An `AMvalue` struct.
/// \pre \p obj_item `!= NULL`.
/// \internal
///
/// # Safety
/// obj_item must be a valid pointer to an AMobjItem
#[no_mangle]
pub unsafe extern "C" fn AMobjItemValue<'a>(obj_item: *const AMobjItem) -> AMvalue<'a> {
    if let Some(obj_item) = obj_item.as_ref() {
        (&obj_item.value).into()
    } else {
        AMvalue::Void
    }
}

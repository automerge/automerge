use automerge as am;
use std::cell::RefCell;
use std::ops::Deref;

use crate::actor_id::AMactorId;

pub mod item;
pub mod items;

/// \struct AMobjId
/// \installed_headerfile
/// \brief An object's unique identifier.
#[derive(PartialEq)]
pub struct AMobjId {
    body: am::ObjId,
    c_actor_id: RefCell<Option<AMactorId>>,
}

impl AMobjId {
    pub fn new(body: am::ObjId) -> Self {
        Self {
            body,
            c_actor_id: RefCell::<Option<AMactorId>>::default(),
        }
    }

    pub fn actor_id(&self) -> *const AMactorId {
        let mut c_actor_id = self.c_actor_id.borrow_mut();
        match c_actor_id.as_mut() {
            None => {
                if let am::ObjId::Id(_, actor_id, _) = &self.body {
                    return c_actor_id.insert(AMactorId::new(actor_id));
                }
            }
            Some(value) => {
                return value;
            }
        }
        std::ptr::null()
    }
}

impl AsRef<am::ObjId> for AMobjId {
    fn as_ref(&self) -> &am::ObjId {
        &self.body
    }
}

impl Deref for AMobjId {
    type Target = am::ObjId;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

/// \memberof AMobjId
/// \brief Gets the actor identifier of an object identifier.
///
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \return A pointer to an `AMactorId` struct or `NULL`.
/// \pre \p obj_id `!= NULL`.
/// \internal
///
/// # Safety
/// obj_id must be a valid pointer to an AMobjId
#[no_mangle]
pub unsafe extern "C" fn AMobjIdActorId(obj_id: *const AMobjId) -> *const AMactorId {
    if let Some(obj_id) = obj_id.as_ref() {
        return obj_id.actor_id();
    };
    std::ptr::null()
}

/// \memberof AMobjId
/// \brief Gets the counter of an object identifier.
///
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p obj_id `!= NULL`.
/// \internal
///
/// # Safety
/// obj_id must be a valid pointer to an AMobjId
#[no_mangle]
pub unsafe extern "C" fn AMobjIdCounter(obj_id: *const AMobjId) -> u64 {
    if let Some(obj_id) = obj_id.as_ref() {
        match obj_id.as_ref() {
            am::ObjId::Id(counter, _, _) => *counter,
            am::ObjId::Root => 0,
        }
    } else {
        u64::MAX
    }
}

/// \memberof AMobjId
/// \brief Tests the equality of two object identifiers.
///
/// \param[in] obj_id1 A pointer to an `AMobjId` struct.
/// \param[in] obj_id2 A pointer to an `AMobjId` struct.
/// \return `true` if \p obj_id1 `==` \p obj_id2 and `false` otherwise.
/// \pre \p obj_id1 `!= NULL`.
/// \pre \p obj_id2 `!= NULL`.
/// \internal
///
/// #Safety
/// obj_id1 must be a valid AMobjId pointer
/// obj_id2 must be a valid AMobjId pointer
#[no_mangle]
pub unsafe extern "C" fn AMobjIdEqual(obj_id1: *const AMobjId, obj_id2: *const AMobjId) -> bool {
    match (obj_id1.as_ref(), obj_id2.as_ref()) {
        (Some(obj_id1), Some(obj_id2)) => obj_id1 == obj_id2,
        (None, Some(_)) | (Some(_), None) | (None, None) => false,
    }
}

/// \memberof AMobjId
/// \brief Gets the index of an object identifier.
///
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p obj_id `!= NULL`.
/// \internal
///
/// # Safety
/// obj_id must be a valid pointer to an AMobjId
#[no_mangle]
pub unsafe extern "C" fn AMobjIdIndex(obj_id: *const AMobjId) -> usize {
    if let Some(obj_id) = obj_id.as_ref() {
        match obj_id.as_ref() {
            am::ObjId::Id(_, _, index) => *index,
            am::ObjId::Root => 0,
        }
    } else {
        usize::MAX
    }
}

/// \ingroup enumerations
/// \enum AMobjType
/// \brief The type of an object value.
#[repr(u8)]
pub enum AMobjType {
    /// A list.
    List = 1,
    /// A key-value map.
    Map,
    /// A list of Unicode graphemes.
    Text,
}

impl From<AMobjType> for am::ObjType {
    fn from(o: AMobjType) -> Self {
        match o {
            AMobjType::Map => am::ObjType::Map,
            AMobjType::List => am::ObjType::List,
            AMobjType::Text => am::ObjType::Text,
        }
    }
}

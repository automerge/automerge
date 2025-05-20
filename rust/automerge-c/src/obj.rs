use automerge as am;
use std::any::type_name;
use std::cell::RefCell;
use std::ops::Deref;

use crate::actor_id::AMactorId;

macro_rules! to_obj_id {
    ($handle:expr) => {{
        match $handle.as_ref() {
            Some(obj_id) => obj_id,
            None => &automerge::ROOT,
        }
    }};
}

pub(crate) use to_obj_id;

macro_rules! to_obj_type {
    ($c_obj_type:expr) => {{
        let result: Result<am::ObjType, am::AutomergeError> = (&$c_obj_type).try_into();
        match result {
            Ok(obj_type) => obj_type,
            Err(e) => return AMresult::error(&e.to_string()).into(),
        }
    }};
}

pub(crate) use to_obj_type;

/// \struct AMobjId
/// \installed_headerfile
/// \brief An object's unique identifier.
#[derive(Eq, PartialEq)]
pub struct AMobjId {
    body: am::ObjId,
    c_actor_id: RefCell<Option<AMactorId>>,
}

impl AMobjId {
    pub fn new(obj_id: am::ObjId) -> Self {
        Self {
            body: obj_id,
            c_actor_id: Default::default(),
        }
    }

    fn actor_id(&self) -> *const AMactorId {
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
/// \brief Gets the actor identifier component of an object identifier.
///
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \return A pointer to an `AMactorId` struct or `NULL`.
/// \pre \p obj_id `!= NULL`
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
/// \brief Gets the counter component of an object identifier.
///
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p obj_id `!= NULL`
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
/// \pre \p obj_id1 `!= NULL`
/// \pre \p obj_id1 `!= NULL`
/// \post `!(`\p obj_id1 `&&` \p obj_id2 `) -> false`
/// \internal
///
/// #Safety
/// obj_id1 must be a valid AMobjId pointer
/// obj_id2 must be a valid AMobjId pointer
#[no_mangle]
pub unsafe extern "C" fn AMobjIdEqual(obj_id1: *const AMobjId, obj_id2: *const AMobjId) -> bool {
    match (obj_id1.as_ref(), obj_id2.as_ref()) {
        (Some(obj_id1), Some(obj_id2)) => obj_id1 == obj_id2,
        (None, None) | (None, Some(_)) | (Some(_), None) => false,
    }
}

/// \memberof AMobjId
/// \brief Gets the index component of an object identifier.
///
/// \param[in] obj_id A pointer to an `AMobjId` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p obj_id `!= NULL`
/// \internal
///
/// # Safety
/// obj_id must be a valid pointer to an AMobjId
#[no_mangle]
pub unsafe extern "C" fn AMobjIdIndex(obj_id: *const AMobjId) -> usize {
    use am::ObjId::*;

    if let Some(obj_id) = obj_id.as_ref() {
        match obj_id.as_ref() {
            Id(_, _, index) => *index,
            Root => 0,
        }
    } else {
        usize::MAX
    }
}

/// \ingroup enumerations
/// \enum AMobjType
/// \installed_headerfile
/// \brief The type of an object.
#[derive(Eq, PartialEq)]
#[repr(C)]
pub enum AMobjType {
    /// The default tag, not a type signifier.
    Default = 0,
    /// A list.
    List = 1,
    /// A key-value map.
    Map,
    /// A list of Unicode graphemes.
    Text,
}

impl Default for AMobjType {
    fn default() -> Self {
        Self::Default
    }
}

impl From<&am::ObjType> for AMobjType {
    fn from(o: &am::ObjType) -> Self {
        use am::ObjType::*;

        match o {
            List => Self::List,
            Map | Table => Self::Map,
            Text => Self::Text,
        }
    }
}

impl TryFrom<&AMobjType> for am::ObjType {
    type Error = am::AutomergeError;

    fn try_from(c_obj_type: &AMobjType) -> Result<Self, Self::Error> {
        use am::AutomergeError::InvalidValueType;
        use AMobjType::*;

        match c_obj_type {
            List => Ok(Self::List),
            Map => Ok(Self::Map),
            Text => Ok(Self::Text),
            _ => Err(InvalidValueType {
                expected: type_name::<Self>().to_string(),
                unexpected: type_name::<u8>().to_string(),
            }),
        }
    }
}

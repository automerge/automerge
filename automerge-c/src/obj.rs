use automerge as am;
use std::ops::Deref;

/// \struct AMobjId
/// \brief An object's unique identifier.
pub struct AMobjId(am::ObjId);

impl AMobjId {
    pub fn new(obj_id: am::ObjId) -> Self {
        Self(obj_id)
    }
}

impl AsRef<am::ObjId> for AMobjId {
    fn as_ref(&self) -> &am::ObjId {
        &self.0
    }
}

impl Deref for AMobjId {
    type Target = am::ObjId;

    fn deref(&self) -> &Self::Target {
        &self.0
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

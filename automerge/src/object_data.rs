use crate::types::ObjId;
use crate::ObjType;

use crate::op_tree::OpTreeInternal;

/// Stores the data for an object.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObjectData {
    /// The type of this object.
    pub(crate) typ: ObjType,
    /// The operations pertaining to this object.
    pub(crate) ops: OpTreeInternal,
    /// The id of the parent object, root has no parent.
    pub parent: Option<ObjId>,
}

impl ObjectData {
    pub fn root() -> Self {
        ObjectData {
            typ: ObjType::Map,
            ops: Default::default(),
            parent: None,
        }
    }

    pub fn new(typ: ObjType, parent: Option<ObjId>) -> Self {
        ObjectData {
            typ,
            ops: Default::default(),
            parent,
        }
    }
}

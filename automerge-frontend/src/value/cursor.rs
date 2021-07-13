use automerge_protocol as amp;
use serde::Serialize;

#[derive(Serialize, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "derive-arbitrary", derive(arbitrary::Arbitrary))]
pub struct Cursor {
    pub index: u32,
    pub(crate) object: amp::ObjectId,
    pub(crate) elem_opid: amp::OpId,
}

impl Cursor {
    pub fn new(index: u32, obj: amp::ObjectId, op: amp::OpId) -> Cursor {
        Cursor {
            index,
            object: obj,
            elem_opid: op,
        }
    }
}

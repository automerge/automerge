use serde::{Serialize, Serializer};

use super::op::RawOpType;
use crate::{legacy::OpType, ObjType};

impl Serialize for OpType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // We convert to a `RawOpType` b/c here we only want to serialize the type of the `OpType`
        // and not its associated data, since in JSON the associated data is under a different key.
        let raw_type = match self {
            OpType::Make(ObjType::Map) => RawOpType::MakeMap,
            OpType::Make(ObjType::Table) => RawOpType::MakeTable,
            OpType::Make(ObjType::List) => RawOpType::MakeList,
            OpType::Make(ObjType::Text) => RawOpType::MakeText,
            OpType::Delete => RawOpType::Del,
            OpType::Increment(_) => RawOpType::Inc,
            OpType::Put(_) => RawOpType::Set,
            OpType::MarkBegin(_) => RawOpType::MarkBegin,
            OpType::MarkEnd(_) => RawOpType::MarkEnd,
        };
        raw_type.serialize(serializer)
    }
}

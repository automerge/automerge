use serde::{Serialize, Serializer};

use super::op::RawOpType;
use crate::{ObjType, OpType};

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
            OpType::MarkBegin(_) => RawOpType::MarkBegin,
            OpType::MarkEnd(_) => RawOpType::MarkEnd,
            OpType::Del => RawOpType::Del,
            OpType::Inc(_) => RawOpType::Inc,
            OpType::Set(_) => RawOpType::Set,
        };
        raw_type.serialize(serializer)
    }
}

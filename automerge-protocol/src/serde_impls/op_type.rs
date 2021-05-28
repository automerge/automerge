use serde::{Serialize, Serializer};

use super::op::RawOpType;
use crate::{MapType, ObjType, OpType, SequenceType};

impl Serialize for OpType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // We convert to a `RawOpType` b/c here we only want to serialize the type of the `OpType`
        // and not its associated data, since in JSON the associated data is under a different key.
        let raw_type = match self {
            OpType::Make(ObjType::Map(MapType::Map)) => RawOpType::MakeMap,
            OpType::Make(ObjType::Map(MapType::Table)) => RawOpType::MakeTable,
            OpType::Make(ObjType::Sequence(SequenceType::List)) => RawOpType::MakeList,
            OpType::Make(ObjType::Sequence(SequenceType::Text)) => RawOpType::MakeText,
            OpType::Del(..) => RawOpType::Del,
            OpType::Inc(_) => RawOpType::Inc,
            OpType::Set(_) => RawOpType::Set,
            OpType::MultiSet(..) => RawOpType::Set,
        };
        raw_type.serialize(serializer)
    }
}

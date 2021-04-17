use serde::{Serialize, Serializer};

use crate::{MapType, ObjType, OpType, SequenceType};

impl Serialize for OpType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match self {
            OpType::Make(ObjType::Map(MapType::Map)) => "makeMap",
            OpType::Make(ObjType::Map(MapType::Table)) => "makeTable",
            OpType::Make(ObjType::Sequence(SequenceType::List)) => "makeList",
            OpType::Make(ObjType::Sequence(SequenceType::Text)) => "makeText",
            OpType::Del => "del",
            OpType::Inc(_) => "inc",
            OpType::Set(_) => "set",
        };
        serializer.serialize_str(s)
    }
}

use serde::{Serialize, Serializer};
use crate::{OpType, ObjType};

impl Serialize for OpType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match self {
            OpType::Make(ObjType::Map) => "makeMap",
            OpType::Make(ObjType::Table) => "makeTable",
            OpType::Make(ObjType::List) => "makeList",
            OpType::Make(ObjType::Text) => "makeText",
            OpType::Del => "del",
            OpType::Link(_) => "link",
            OpType::Inc(_) => "inc",
            OpType::Set(_) => "set",
        };
        serializer.serialize_str(s)
    }
}


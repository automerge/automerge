use automerge_protocol::{MapType, ObjType, ObjectID, SequenceType, Value};
use serde::{Serialize, Serializer};

#[derive(PartialEq, Debug, Clone)]
pub enum OpType {
    Make(ObjType),
    Del,
    Link(ObjectID),
    Inc(i64),
    Set(Value),
}

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
            OpType::Link(_) => "link",
            OpType::Inc(_) => "inc",
            OpType::Set(_) => "set",
        };
        serializer.serialize_str(s)
    }
}

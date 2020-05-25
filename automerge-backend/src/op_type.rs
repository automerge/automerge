use automerge_protocol as amp;
use serde::{Serialize, Serializer};

#[derive(PartialEq, Debug, Clone)]
pub enum OpType {
    Make(amp::ObjType),
    Del,
    Link(amp::ObjectID),
    Inc(i64),
    Set(amp::Value),
}

impl Serialize for OpType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match self {
            OpType::Make(amp::ObjType::Map) => "makeMap",
            OpType::Make(amp::ObjType::Table) => "makeTable",
            OpType::Make(amp::ObjType::List) => "makeList",
            OpType::Make(amp::ObjType::Text) => "makeText",
            OpType::Del => "del",
            OpType::Link(_) => "link",
            OpType::Inc(_) => "inc",
            OpType::Set(_) => "set",
        };
        serializer.serialize_str(s)
    }
}

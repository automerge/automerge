use crate::error;
use automerge_protocol as amp;
use serde::{Serialize, Serializer};
use std::convert::TryFrom;

#[derive(PartialEq, Debug, Clone)]
pub enum OpType {
    Make(amp::ObjType),
    Del,
    Inc(i64),
    Set(amp::ScalarValue),
}

impl Serialize for OpType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match self {
            OpType::Make(amp::ObjType::Map(amp::MapType::Map)) => "makeMap",
            OpType::Make(amp::ObjType::Map(amp::MapType::Table)) => "makeTable",
            OpType::Make(amp::ObjType::Sequence(amp::SequenceType::List)) => "makeList",
            OpType::Make(amp::ObjType::Sequence(amp::SequenceType::Text)) => "makeText",
            OpType::Del => "del",
            OpType::Inc(_) => "inc",
            OpType::Set(_) => "set",
        };
        serializer.serialize_str(s)
    }
}

impl TryFrom<&amp::Op> for OpType {
    type Error = error::InvalidChangeError;
    fn try_from(op: &amp::Op) -> Result<Self, Self::Error> {
        match op.action {
            amp::OpType::MakeMap => Ok(OpType::Make(amp::ObjType::Map(amp::MapType::Map))),
            amp::OpType::MakeTable => Ok(OpType::Make(amp::ObjType::Map(amp::MapType::Table))),
            amp::OpType::MakeList => Ok(OpType::Make(amp::ObjType::Sequence(
                amp::SequenceType::List,
            ))),
            amp::OpType::MakeText => Ok(OpType::Make(amp::ObjType::Sequence(
                amp::SequenceType::Text,
            ))),
            amp::OpType::Del => Ok(OpType::Del),
            amp::OpType::Set => op
                .value
                .as_ref()
                .map(|v| OpType::Set(v.clone()))
                .ok_or(error::InvalidChangeError::SetOpWithoutValue),
            amp::OpType::Inc => match &op.value {
                Some(amp::ScalarValue::Int(i)) => Ok(OpType::Inc(*i)),
                Some(amp::ScalarValue::Uint(u)) => Ok(OpType::Inc(*u as i64)),
                Some(amp::ScalarValue::Counter(i)) => Ok(OpType::Inc(*i)),
                val => Err(error::InvalidChangeError::IncOperationWithInvalidValue {
                    op_value: val.clone(),
                }),
            },
        }
    }
}

use crate::op_type::OpType;
use crate::undo_operation::UndoOperation;
use automerge_protocol as amp;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

impl Serialize for UndoOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;

        match &self.action {
            OpType::Set(amp::ScalarValue::Counter(_)) => fields += 2,
            OpType::Set(amp::ScalarValue::Timestamp(_)) => fields += 2,
            OpType::Link(_) | OpType::Inc(_) | OpType::Set(_) => fields += 1,
            _ => {}
        }

        let mut op = serializer.serialize_struct("UndoOperation", fields)?;
        op.serialize_field("action", &self.action)?;
        op.serialize_field("obj", &self.obj)?;
        op.serialize_field("key", &self.key)?;
        op.serialize_field("insert", &false)?;
        match &self.action {
            OpType::Link(child) => op.serialize_field("child", &child)?,
            OpType::Inc(n) => op.serialize_field("value", &n)?,
            OpType::Set(amp::ScalarValue::Timestamp(value)) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &amp::DataType::Timestamp)?;
            }
            OpType::Set(amp::ScalarValue::Counter(value)) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &amp::DataType::Counter)?;
            }
            OpType::Set(value) => op.serialize_field("value", &value)?,
            _ => {}
        }
        op.end()
    }
}

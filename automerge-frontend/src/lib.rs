use automerge_backend::{PrimitiveValue, DataType, Patch};
use std::collections::HashMap;

mod change_context;

enum Value {
    Sequence(Vec<Value>),
    Text(String),
    Primitive(PrimitiveValue, Option<DataType>),
    Object(HashMap<String, Value>),
    Table(HashMap<String, Value>),
}

struct Frontend {
}

impl Frontend {
    fn state(&self) -> Value {
        Value::Primitive(PrimitiveValue::Null, None)
    }

    fn apply_patch(&mut self, patch: Patch) -> () {
    }
}

enum PathElement {
    Key(String),
    Index(usize),
}

struct LocalChange {
    path: Vec<PathElement>
}

#[cfg(test)]
mod tests {

}

use automerge_protocol as amp;

use crate::op_handle::OpHandle;

#[derive(Eq, PartialEq, Hash, Debug, Clone, Copy)]
pub(crate) struct ActorID(pub usize);

#[derive(Eq, PartialEq, Debug, Hash, Clone, Copy)]
pub(crate) struct OpID(pub u64, pub ActorID);

#[derive(Eq, PartialEq, Debug, Hash, Clone, Copy)]
pub(crate) enum ObjectID {
    ID(OpID),
    Root,
}

#[derive(PartialEq, Eq, Debug, Hash, Clone, Copy)]
pub(crate) enum ElementID {
    Head,
    ID(OpID),
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub(crate) enum Key {
    Map(String),
    Seq(ElementID),
}

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct InternalOp {
    pub action: InternalOpType,
    pub obj: ObjectID,
    pub key: Key,
    pub pred: Vec<OpID>,
    pub insert: bool,
}

impl InternalOp {
    pub fn obj_type(&self) -> Option<amp::ObjType> {
        match self.action {
            InternalOpType::Make(objtype) => Some(objtype),
            _ => None,
        }
    }

    pub fn is_inc(&self) -> bool {
        matches!(self.action, InternalOpType::Inc(_))
    }

    pub fn is_make(&self) -> bool {
        self.obj_type().is_some()
    }

    pub fn generate_redos(&self, overwritten: &[OpHandle]) -> Vec<InternalUndoOperation> {
        let key = self.key.clone();

        if let InternalOpType::Inc(value) = self.action {
            vec![InternalUndoOperation {
                action: InternalOpType::Inc(-value),
                obj: self.obj,
                key,
            }]
        } else if overwritten.is_empty() {
            vec![InternalUndoOperation {
                action: InternalOpType::Del,
                obj: self.obj,
                key,
            }]
        } else {
            overwritten.iter().map(|o| o.invert(&key)).collect()
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct InternalUndoOperation {
    pub action: InternalOpType,
    pub obj: ObjectID,
    pub key: Key,
}

impl InternalUndoOperation {
    pub fn into_operation(self, pred: Vec<OpID>) -> InternalOp {
        InternalOp {
            action: self.action,
            obj: self.obj,
            key: self.key,
            insert: false,
            pred,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub(crate) enum InternalOpType {
    Make(amp::ObjType),
    Del,
    Link(ObjectID),
    Inc(i64),
    Set(amp::ScalarValue),
}

impl Key {
    pub fn as_element_id(&self) -> Option<ElementID> {
        match self {
            Key::Map(_) => None,
            Key::Seq(eid) => Some(*eid),
        }
    }

    pub fn to_opid(&self) -> Option<OpID> {
        match self.as_element_id()? {
            ElementID::ID(id) => Some(id),
            ElementID::Head => None,
        }
    }

    pub fn head() -> Self {
        Key::Seq(ElementID::Head)
    }
}

impl From<OpID> for ObjectID {
    fn from(id: OpID) -> ObjectID {
        ObjectID::ID(id)
    }
}

impl From<OpID> for ElementID {
    fn from(id: OpID) -> ElementID {
        ElementID::ID(id)
    }
}

impl From<OpID> for Key {
    fn from(id: OpID) -> Key {
        Key::Seq(ElementID::ID(id))
    }
}

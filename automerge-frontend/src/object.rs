use crate::{Value, MapType, SequenceType};
use std::{cell::RefCell, collections::HashMap, rc::Rc};
use automerge_backend as amb;

/// Represents the set of conflicting values for a register in an automerge
/// document.
#[derive(Clone, Debug)]
pub struct Values(pub(crate) HashMap<amb::OpID, Rc<RefCell<Object>>>);

impl Values {
    fn to_value(&self) -> Value {
        self.default_value().borrow().value()
    }

    pub(crate) fn default_value(&self) -> Rc<RefCell<Object>> {
        let mut op_ids: Vec<&amb::OpID> = self.0.keys().collect();
        op_ids.sort();
        let default_op_id = op_ids.first().unwrap();
        self.0.get(default_op_id).map(|o| o.clone()).unwrap()
    }

    pub(crate) fn update_for_opid(&mut self, opid: amb::OpID, value: Rc<RefCell<Object>>) {
        self.0.insert(opid, value);
    }
}

/// Internal data type used to represent the values of an automerge document
#[derive(Clone, Debug)]
pub enum Object {
    Sequence(amb::ObjectID, Vec<Option<Values>>, SequenceType),
    Map(amb::ObjectID, HashMap<String, Values>, MapType),
    Primitive(amb::Value),
}

impl Object {
    pub(crate) fn value(&self) -> Value {
        match self {
            Object::Sequence(_, vals, seq_type) => Value::Sequence(
                vals.iter()
                    .filter_map(|v| v.clone().map(|v2| v2.to_value()))
                    .collect(),
                seq_type.clone(),
            ),
            Object::Map(_, vals, map_type) => Value::Map(
                vals.iter()
                    .map(|(k, v)| (k.to_string(), v.to_value()))
                    .collect(),
                map_type.clone(),
            ),
            Object::Primitive(v) => Value::Primitive(v.clone()),
        }
    }

    pub(crate) fn id(&self) -> Option<amb::ObjectID> {
        match self {
            Object::Sequence(oid, _, _) => Some(oid.clone()),
            Object::Map(oid, _, _) => Some(oid.clone()),
            Object::Primitive(..) => None,
        }
    }

    pub(crate) fn backend_type(&self) -> Option<amb::ObjType> {
        match self {
            Object::Sequence(oid, _, seq_type) => Some(match seq_type {
                SequenceType::List => amb::ObjType::List,
                SequenceType::Text => amb::ObjType::Text,
            }),
            Object::Map(oid, _, map_type) => Some(match map_type {
                MapType::Map => amb::ObjType::Map,
                MapType::Table => amb::ObjType::Table,
            }),
            Object::Primitive(..) => None,
        }
    }
}

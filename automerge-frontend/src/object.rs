use crate::{MapType, SequenceType, Value};
use automerge_protocol as amp;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

/// Represents the set of conflicting values for a register in an automerge
/// document.
#[derive(Clone, Debug)]
pub struct Values(pub(crate) HashMap<amp::OpID, Rc<RefCell<Object>>>);

impl Values {
    fn to_value(&self) -> Value {
        self.default_value().borrow().value()
    }

    pub(crate) fn default_value(&self) -> Rc<RefCell<Object>> {
        let mut op_ids: Vec<&amp::OpID> = self.0.keys().collect();
        op_ids.sort();
        let default_op_id = op_ids.first().unwrap();
        self.0.get(default_op_id).cloned().unwrap()
    }

    pub(crate) fn update_for_opid(&mut self, opid: amp::OpID, value: Rc<RefCell<Object>>) {
        self.0.insert(opid, value);
    }

    pub(crate) fn default_op_id(&self) -> Option<amp::OpID> {
        let mut op_ids: Vec<&amp::OpID> = self.0.keys().collect();
        op_ids.sort();
        #[allow(clippy::map_clone)]
        op_ids.first().map(|oid| *oid).cloned()
    }
}

/// Internal data type used to represent the values of an automerge document
#[derive(Clone, Debug)]
pub enum Object {
    Sequence(amp::ObjectID, Vec<Option<Values>>, SequenceType),
    Map(amp::ObjectID, HashMap<String, Values>, MapType),
    Primitive(amp::Value),
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

    // TODO RequestKey is probably out of place here, but it was convenient
    pub(crate) fn default_op_id_for_key(&self, key: amp::RequestKey) -> Option<amp::OpID> {
        match (key, self) {
            // TODO this whole function feels off but this clone especially upsets me
            (amp::RequestKey::Num(i), Object::Sequence(_, vals, _)) => vals
                .get(i as usize)
                .and_then(|v| v.clone().and_then(|inner| inner.default_op_id())),
            (amp::RequestKey::Str(ref s), Object::Map(_, vals, _)) => {
                vals.get(s.as_str()).and_then(|v| v.default_op_id())
            }
            _ => None,
        }
    }

    pub(crate) fn id(&self) -> Option<amp::ObjectID> {
        match self {
            Object::Sequence(oid, _, _) => Some(oid.clone()),
            Object::Map(oid, _, _) => Some(oid.clone()),
            Object::Primitive(..) => None,
        }
    }
}

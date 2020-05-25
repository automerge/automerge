use crate::internal::{
    ActorID, ElementID, InternalOpType, InternalOperation, InternalUndoOperation, Key, ObjectID,
    OpID,
};
use crate::op_type::OpType;
use crate::undo_operation::UndoOperation;
use crate::Operation;
use automerge_protocol as amp;
use std::cmp::Ordering;

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct ActorMap(Vec<amp::ActorID>);

impl ActorMap {
    pub fn new() -> ActorMap {
        ActorMap(Vec::new())
    }

    pub fn import_key(&mut self, key: amp::Key) -> Key {
        match key {
            amp::Key::Map(string) => Key::Map(string),
            amp::Key::Seq(eid) => Key::Seq(self.import_element_id(eid)),
        }
    }

    pub fn import_actor(&mut self, actor: amp::ActorID) -> ActorID {
        if let Some(idx) = self.0.iter().position(|a| a == &actor) {
            ActorID(idx)
        } else {
            self.0.push(actor);
            ActorID(self.0.len() - 1)
        }
    }

    pub fn import_opid(&mut self, opid: amp::OpID) -> OpID {
        OpID(opid.0, self.import_actor(opid.1))
    }

    pub fn import_obj(&mut self, obj: amp::ObjectID) -> ObjectID {
        match obj {
            amp::ObjectID::Root => ObjectID::Root,
            amp::ObjectID::ID(opid) => ObjectID::ID(self.import_opid(opid)),
        }
    }

    pub fn import_element_id(&mut self, eid: amp::ElementID) -> ElementID {
        match eid {
            amp::ElementID::Head => ElementID::Head,
            amp::ElementID::ID(opid) => ElementID::ID(self.import_opid(opid)),
        }
    }

    pub fn import_op(&mut self, op: Operation) -> InternalOperation {
        InternalOperation {
            action: self.import_optype(op.action),
            obj: self.import_obj(op.obj),
            key: self.import_key(op.key),
            pred: op.pred.into_iter().map(|id| self.import_opid(id)).collect(),
            insert: op.insert,
        }
    }

    pub fn import_optype(&mut self, optype: OpType) -> InternalOpType {
        match optype {
            OpType::Make(val) => InternalOpType::Make(val),
            OpType::Del => InternalOpType::Del,
            OpType::Link(obj) => InternalOpType::Link(self.import_obj(obj)),
            OpType::Inc(val) => InternalOpType::Inc(val),
            OpType::Set(val) => InternalOpType::Set(val),
        }
    }

    pub fn export_key(&self, key: &Key) -> amp::Key {
        match key {
            Key::Map(string) => amp::Key::Map(string.clone()),
            Key::Seq(eid) => amp::Key::Seq(self.export_element_id(eid)),
        }
    }

    pub fn export_actor(&self, actor: &ActorID) -> amp::ActorID {
        self.0[actor.0].clone()
    }

    pub fn export_opid(&self, opid: &OpID) -> amp::OpID {
        amp::OpID(opid.0, self.export_actor(&opid.1))
    }

    pub fn export_obj(&self, obj: &ObjectID) -> amp::ObjectID {
        match obj {
            ObjectID::Root => amp::ObjectID::Root,
            ObjectID::ID(opid) => amp::ObjectID::ID(self.export_opid(opid)),
        }
    }

    pub fn export_element_id(&self, eid: &ElementID) -> amp::ElementID {
        match eid {
            ElementID::Head => amp::ElementID::Head,
            ElementID::ID(opid) => amp::ElementID::ID(self.export_opid(opid)),
        }
    }

    pub fn export_op(&self, op: &InternalOperation) -> Operation {
        Operation {
            action: self.export_optype(&op.action),
            obj: self.export_obj(&op.obj),
            key: self.export_key(&op.key),
            pred: op.pred.iter().map(|id| self.export_opid(id)).collect(),
            insert: op.insert,
        }
    }

    pub fn export_undo(&self, op: &InternalUndoOperation) -> UndoOperation {
        UndoOperation {
            action: self.export_optype(&op.action),
            obj: self.export_obj(&op.obj),
            key: self.export_key(&op.key),
        }
    }

    pub fn export_optype(&self, optype: &InternalOpType) -> OpType {
        match optype {
            InternalOpType::Make(val) => OpType::Make(*val),
            InternalOpType::Del => OpType::Del,
            InternalOpType::Link(obj) => OpType::Link(self.export_obj(obj)),
            InternalOpType::Inc(val) => OpType::Inc(*val),
            InternalOpType::Set(val) => OpType::Set(val.clone()),
        }
    }

    #[allow(dead_code)]
    pub fn index_of(&mut self, actor: &amp::ActorID) -> usize {
        if let Some(index) = self.0.iter().position(|a| a == actor) {
            return index;
        }
        self.0.push(actor.clone());
        self.0.len() - 1
    }

    #[allow(dead_code)]
    pub fn actor_for(&self, index: usize) -> Option<&amp::ActorID> {
        self.0.get(index)
    }

    pub fn cmp(&self, eid1: &ElementID, eid2: &ElementID) -> Ordering {
        match (eid1, eid2) {
            (ElementID::Head, ElementID::Head) => Ordering::Equal,
            (ElementID::Head, _) => Ordering::Less,
            (_, ElementID::Head) => Ordering::Greater,
            (ElementID::ID(opid1), ElementID::ID(opid2)) => self.cmp_opid(opid1, opid2),
        }
    }

    pub fn opid_to_string(&self, id: &OpID) -> String {
        format!("{}@{}", id.0, self.export_actor(&id.1).to_hex_string())
    }

    pub fn elementid_to_string(&self, eid: &ElementID) -> String {
        match eid {
            ElementID::Head => "_head".into(),
            ElementID::ID(id) => self.opid_to_string(id),
        }
    }

    pub fn key_to_string(&self, key: &Key) -> String {
        match &key {
            Key::Map(s) => s.clone(),
            Key::Seq(eid) => self.elementid_to_string(eid),
        }
    }

    fn cmp_opid(&self, op1: &OpID, op2: &OpID) -> Ordering {
        if op1.0 != op2.0 {
            op1.0.cmp(&op2.0)
        } else {
            let actor1 = &self.0[(op1.1).0];
            let actor2 = &self.0[(op2.1).0];
            actor1.cmp(&actor2)
            //op1.1.cmp(&op2.1)
        }
    }
}

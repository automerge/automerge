use std::cmp::Ordering;

use automerge_protocol as amp;

use crate::internal::{ActorId, ElementId, InternalOp, InternalOpType, Key, ObjectId, OpId};

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct ActorMap(Vec<amp::ActorId>);

impl ActorMap {
    pub fn new() -> ActorMap {
        ActorMap(Vec::new())
    }

    pub fn import_key(&mut self, key: &amp::Key) -> Key {
        match key {
            amp::Key::Map(string) => Key::Map(string.to_string()),
            amp::Key::Seq(eid) => Key::Seq(self.import_element_id(eid)),
        }
    }

    pub fn import_actor(&mut self, actor: &amp::ActorId) -> ActorId {
        if let Some(idx) = self.0.iter().position(|a| a == actor) {
            ActorId(idx)
        } else {
            self.0.push(actor.clone());
            ActorId(self.0.len() - 1)
        }
    }

    pub fn import_opid(&mut self, opid: &amp::OpId) -> OpId {
        OpId(opid.0, self.import_actor(&opid.1))
    }

    pub fn import_obj(&mut self, obj: &amp::ObjectId) -> ObjectId {
        match obj {
            amp::ObjectId::Root => ObjectId::Root,
            amp::ObjectId::Id(ref opid) => ObjectId::Id(self.import_opid(opid)),
        }
    }

    pub fn import_element_id(&mut self, eid: &amp::ElementId) -> ElementId {
        match eid {
            amp::ElementId::Head => ElementId::Head,
            amp::ElementId::Id(ref opid) => ElementId::Id(self.import_opid(opid)),
        }
    }

    pub fn import_op(&mut self, op: amp::Op) -> InternalOp {
        InternalOp {
            action: Self::import_optype(&op.action),
            obj: self.import_obj(&op.obj),
            key: self.import_key(&op.key),
            pred: op
                .pred
                .into_iter()
                .map(|ref id| self.import_opid(id))
                .collect(),
            insert: op.insert,
        }
    }

    pub fn import_optype(optype: &amp::OpType) -> InternalOpType {
        match optype {
            amp::OpType::Make(val) => InternalOpType::Make(*val),
            amp::OpType::Del => InternalOpType::Del,
            amp::OpType::Inc(val) => InternalOpType::Inc(*val),
            amp::OpType::Set(val) => InternalOpType::Set(val.clone()),
        }
    }

    pub fn export_actor(&self, actor: ActorId) -> amp::ActorId {
        self.0[actor.0].clone()
    }

    pub fn export_opid(&self, opid: &OpId) -> amp::OpId {
        amp::OpId(opid.0, self.export_actor(opid.1))
    }

    pub fn export_obj(&self, obj: &ObjectId) -> amp::ObjectId {
        match obj {
            ObjectId::Root => amp::ObjectId::Root,
            ObjectId::Id(opid) => amp::ObjectId::Id(self.export_opid(opid)),
        }
    }

    #[allow(dead_code)]
    pub fn index_of(&mut self, actor: &amp::ActorId) -> usize {
        if let Some(index) = self.0.iter().position(|a| a == actor) {
            return index;
        }
        self.0.push(actor.clone());
        self.0.len() - 1
    }

    #[allow(dead_code)]
    pub fn actor_for(&self, index: usize) -> Option<&amp::ActorId> {
        self.0.get(index)
    }

    pub fn cmp(&self, eid1: &ElementId, eid2: &ElementId) -> Ordering {
        match (eid1, eid2) {
            (ElementId::Head, ElementId::Head) => Ordering::Equal,
            (ElementId::Head, _) => Ordering::Less,
            (_, ElementId::Head) => Ordering::Greater,
            (ElementId::Id(opid1), ElementId::Id(opid2)) => self.cmp_opid(opid1, opid2),
        }
    }

    pub fn opid_to_string(&self, id: &OpId) -> String {
        format!("{}@{}", id.0, self.export_actor(id.1).to_hex_string())
    }

    pub fn elementid_to_string(&self, eid: &ElementId) -> String {
        match eid {
            ElementId::Head => "_head".into(),
            ElementId::Id(id) => self.opid_to_string(id),
        }
    }

    pub fn key_to_string(&self, key: &Key) -> String {
        match &key {
            Key::Map(s) => s.clone(),
            Key::Seq(eid) => self.elementid_to_string(eid),
        }
    }

    fn cmp_opid(&self, op1: &OpId, op2: &OpId) -> Ordering {
        if op1.0 == op2.0 {
            let actor1 = &self.0[(op1.1).0];
            let actor2 = &self.0[(op2.1).0];
            actor1.cmp(actor2)
            //op1.1.cmp(&op2.1)
        } else {
            op1.0.cmp(&op2.0)
        }
    }
}

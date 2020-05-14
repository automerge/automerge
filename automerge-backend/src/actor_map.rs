use automerge_protocol::{ActorID, ElementID, Key, ObjectID, OpID};
use std::cmp::Ordering;

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct ActorMap(Vec<ActorID>);

impl ActorMap {
    pub fn new() -> ActorMap {
        ActorMap(Vec::new())
    }

    #[allow(dead_code)]
    pub fn index_of(&mut self, actor: &ActorID) -> usize {
        if let Some(index) = self.0.iter().position(|a| a == actor) {
            return index;
        }
        self.0.push(actor.clone());
        self.0.len() - 1
    }

    #[allow(dead_code)]
    pub fn actor_for(&self, index: usize) -> Option<&ActorID> {
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
        format!("{}@{}", id.0, id.1)
    }

    pub fn elementid_to_string(&self, eid: &ElementID) -> String {
        match eid {
            ElementID::Head => "_head".into(),
            ElementID::ID(id) => self.opid_to_string(id),
        }
    }

    pub fn key_to_string(&self, key: &Key) -> String {
        match key {
            Key::Map(s) => s.clone(),
            Key::Seq(eid) => self.elementid_to_string(eid),
        }
    }

    pub fn object_to_string(&self, obj: &ObjectID) -> String {
        match obj {
            ObjectID::ID(opid) => self.opid_to_string(opid),
            ObjectID::Root => "00000000-0000-0000-0000-000000000000".into(),
        }
    }

    fn cmp_opid(&self, op1: &OpID, op2: &OpID) -> Ordering {
        if op1.0 != op2.0 {
            op1.0.cmp(&op2.0)
        } else {
            //            let actor1 = self.actor_for(op1).unwrap();
            //            let actor2 = self.actor_for(op2).unwrap();
            //            actor1.cmp(&actor2)
            op1.1.cmp(&op2.1)
        }
    }
}

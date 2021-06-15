#![allow(dead_code)]

use std::cmp::Ordering;

use automerge_protocol as amp;

use crate::{error::AutomergeError, internal::InternalOpType, patches::IncrementalPatch};

#[derive(Debug, PartialEq, Clone, Default)]
pub struct OpSet {
    actors: Vec<amp::ActorId>,
    changes: Vec<Change>,
    ops: Vec<Op>,
}
/*

#[derive(PartialEq, Debug, Clone)]
pub struct Change {
    bytes: ChangeBytes,
    pub hash: amp::ChangeHash,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    message: Range<usize>,
    actors: Vec<amp::ActorId>,
    pub deps: Vec<amp::ChangeHash>,
    ops: HashMap<u32, Range<usize>>,
    extra_bytes: Range<usize>,
}

 */

impl OpSet {
    fn index_for_actor(&self, actor: &amp::ActorId) -> Option<usize> {
        self.actors.iter().position(|n| n == actor)
    }

    fn import_key(&self, key: &amp::Key) -> Key {
        match key {
            amp::Key::Map(string) => Key::Map(string.clone()),
            amp::Key::Seq(amp::ElementId::Head) => Key::Seq(OpId(0, 0)),
            amp::Key::Seq(amp::ElementId::Id(id)) => Key::Seq(self.import_opid(id)),
        }
    }

    fn import_objectid(&self, obj: &amp::ObjectId) -> OpId {
        match obj {
            amp::ObjectId::Root => OpId(0, 0),
            amp::ObjectId::Id(id) => self.import_opid(id),
        }
    }

    fn import_opid(&self, opid: &amp::OpId) -> OpId {
        OpId(opid.0, self.index_for_actor(&opid.1).unwrap())
    }

    fn lamport_compare(&self, op1: &OpId, op2: &OpId) -> Ordering {
        match (op1, op2) {
            (OpId(0, 0), OpId(0, 0)) => Ordering::Equal,
            (OpId(0, 0), OpId(_, _)) => Ordering::Less,
            (OpId(_, _), OpId(0, 0)) => Ordering::Greater,
            (OpId(ctr1, actor1), OpId(ctr2, actor2)) => {
                if ctr1 == ctr2 {
                    let actor1 = &self.actors[*actor1];
                    let actor2 = &self.actors[*actor2];
                    actor1.cmp(actor2)
                } else {
                    op1.0.cmp(&op2.0)
                }
            }
        }
    }

    fn seek_to_obj(&self, obj: &OpId) -> usize {
        if self.ops.is_empty() {
            return 0;
        }
        let mut current_obj = None;
        for (i, next) in self.ops.iter().enumerate() {
            if current_obj == Some(&next.obj) {
                continue;
            }
            if &next.obj == obj || self.lamport_compare(&next.obj, obj) == Ordering::Greater {
                return i;
            }
            current_obj = Some(&next.obj);
        }
        self.ops.len()
    }

    fn seek(&self, op: &Op) -> (usize, usize) {
        let obj_start = self.seek_to_obj(&op.obj);

        match &op.key {
            Key::Map(_) => {
                for (i, next) in self.ops[obj_start..].iter().enumerate() {
                    if next.key >= op.key || next.obj != op.obj {
                        return (obj_start + i, 0);
                    }
                }
                (self.ops.len(), 0)
            }
            Key::Seq(_) => {
                if op.insert {
                    //for o in self.ops[obj_start..].iter().enumerate() {
                    //}
                    unimplemented!()
                } else {
                    let mut elem_visible = false;
                    let mut visible = 0;
                    for (i, next) in self.ops[obj_start..].iter().enumerate() {
                        if next.insert && next.key == op.key || next.obj != op.obj {
                            return (obj_start + i, visible);
                        }
                        if next.insert {
                            elem_visible = false
                        }
                        if next.succ.is_empty() && !elem_visible {
                            visible += 1;
                            elem_visible = true
                        }
                    }
                    panic!() // error - cant find place to insert
                }
            }
        }
    }

    pub(crate) fn apply_change(
        &mut self,
        change: crate::Change,
        _diffs: &mut IncrementalPatch,
    ) -> Result<(), AutomergeError> {
        for actor in &change.actors {
            if self.index_for_actor(actor).is_none() {
                self.actors.push(actor.clone());
            }
        }

        let actor = self.index_for_actor(change.actor_id()).unwrap(); // can unwrap b/c we added it above
        let extra_bytes = change.extra_bytes().to_vec();

        let change_id = self.changes.len();
        let ops: Vec<Op> = change
            .iter_ops()
            .enumerate()
            .map(|(i, expanded_op)| Op {
                change: change_id,
                id: OpId(change.start_op + i as u64, actor),
                action: expanded_op.action,
                insert: expanded_op.insert,
                key: self.import_key(&expanded_op.key),
                obj: self.import_objectid(&expanded_op.obj),
                pred: expanded_op
                    .pred
                    .iter()
                    .map(|id| self.import_opid(id))
                    .collect(),
                succ: vec![],
            })
            .collect();

        self.changes.push(Change {
            actor,
            hash: change.hash,
            seq: change.seq,
            max_op: change.max_op(),
            time: change.time,
            message: change.message(),
            deps: change.deps,
            extra_bytes,
        });

        for op in ops {
            // slow as balls
            // *** put them in the right place
            let (pos, _visible_count) = self.seek(&op);
            self.ops.insert(pos, op);
        }

        // update pred/succ properly
        // handle inc/del - they are special
        // generate diffs as we do it
        //
        // look at old code below and see what we might also need to do

        unimplemented!()
        /*
        if self.history_index.contains_key(&change.hash) {
            return Ok(());
        }

        self.event_handlers.before_apply_change(&change);

        let change_index = self.update_history(change);

        // SAFETY: change_index is the index for the change we've just added so this can't (and
        // shouldn't) panic. This is to get around the borrow checker.
        let change = &self.history[change_index];

        let op_set = &mut self.op_set;

        let start_op = change.start_op;

        op_set.update_deps(change);

        let ops = OpHandle::extract(change, &mut self.actors);

        op_set.max_op = max(
            op_set.max_op,
            (start_op + (ops.len() as u64)).saturating_sub(1),
        );

        op_set.apply_ops(ops, diffs, &mut self.actors)?;

        self.event_handlers.after_apply_change(change);

        Ok(())
        */
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct OpId(u64, usize);

#[derive(PartialEq, Debug, Clone)]
pub struct Change {
    pub actor: usize,
    pub hash: amp::ChangeHash,
    pub seq: u64,
    pub max_op: u64,
    pub time: i64,
    pub message: Option<String>,
    pub deps: Vec<amp::ChangeHash>,
    pub extra_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
struct Op {
    pub change: usize,
    pub id: OpId,
    pub action: InternalOpType,
    pub obj: OpId,
    pub key: Key,
    pub succ: Vec<OpId>,
    pub pred: Vec<OpId>,
    pub insert: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Key {
    Map(String),
    Seq(OpId),
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Key::Map(p1), Key::Map(p2)) => p1.partial_cmp(p2),
            _ => None,
        }
    }
}

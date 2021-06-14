
use automerge_protocol as amp;

use crate::{
    //actor_map::ActorMap,
    error::AutomergeError,
    internal::{InternalOpType},
    //object_store::ObjState,
    op_handle::OpHandle,
    //ordered_set::OrderedSet,
    patches::{IncrementalPatch},
};

#[derive(Debug, PartialEq, Clone, Default)]
pub (crate) struct OpSet {
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
            amp::Key::Seq(amp::ElementId::Head) => Key::Seq(OpId(0,0)),
            amp::Key::Seq(amp::ElementId::Id(id)) => Key::Seq(self.import_opid(id)),
        }
    }

    fn import_objectid(&self, obj: &amp::ObjectId) -> OpId {
        match obj {
            amp::ObjectId::Root => OpId(0,0),
            amp::ObjectId::Id(id) => self.import_opid(id),
        }
    }

    fn import_opid(&self, opid: &amp::OpId) -> OpId {
        OpId(opid.0, self.index_for_actor(&opid.1).unwrap())
    }

    fn seek(&self, op: &Op) -> usize {
        0
    }

    fn apply_change(
        &mut self,
        change: crate::Change,
        diffs: &mut IncrementalPatch,
    ) -> Result<(), AutomergeError> {

        for actor in change.actors.iter() {
            if self.index_for_actor(actor).is_none() {
                self.actors.push(actor.clone());
            }
        }

        let actor = self.index_for_actor(&change.actor_id()).unwrap(); // can unwrap b/c we added it above
        let extra_bytes = change.extra_bytes().to_vec();

        let change_id = self.changes.len();
        let ops : Vec<Op> = change.iter_ops().enumerate().map(|(i, expanded_op)| {
            Op {
                change: change_id,
                id: OpId(change.start_op + i as u64, actor),
                action: expanded_op.action,
                insert: expanded_op.insert,
                key: self.import_key(&expanded_op.key),
                obj: self.import_objectid(&expanded_op.obj),
                pred: expanded_op.pred.iter().map( |id| self.import_opid(&id)).collect(),
                succ: vec![],
            }
        }).collect();

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
            let pos = self.seek(&op);
            self.ops.insert_at(pos,op);
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
pub struct OpId(u64,usize);

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct Change {
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
pub(crate) struct Op {
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


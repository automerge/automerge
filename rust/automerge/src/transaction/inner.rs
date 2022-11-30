use std::num::NonZeroU64;

use crate::automerge::Actor;
use crate::exid::ExId;
use crate::query::{self, OpIdSearch};
use crate::storage::Change as StoredChange;
use crate::types::{Key, ObjId, OpId};
use crate::{op_tree::OpSetMetadata, types::Op, Automerge, Change, ChangeHash, OpObserver, Prop};
use crate::{AutomergeError, ObjType, OpType, ScalarValue};

#[derive(Debug, Clone)]
pub(crate) struct TransactionInner {
    actor: usize,
    seq: u64,
    start_op: NonZeroU64,
    time: i64,
    message: Option<String>,
    deps: Vec<ChangeHash>,
    operations: Vec<(ObjId, Prop, Op)>,
}

/// Arguments required to create a new transaction
pub(crate) struct TransactionArgs {
    /// The index of the actor ID this transaction will create ops for in the
    /// [`OpSetMetadata::actors`]
    pub(crate) actor_index: usize,
    /// The sequence number of the change this transaction will create
    pub(crate) seq: u64,
    /// The start op of the change this transaction will create
    pub(crate) start_op: NonZeroU64,
    /// The dependencies of the change this transaction will create
    pub(crate) deps: Vec<ChangeHash>,
}

impl TransactionInner {
    pub(crate) fn new(
        TransactionArgs {
            actor_index: actor,
            seq,
            start_op,
            deps,
        }: TransactionArgs,
    ) -> Self {
        TransactionInner {
            actor,
            seq,
            start_op,
            time: 0,
            message: None,
            operations: vec![],
            deps,
        }
    }

    /// Create an empty change
    pub(crate) fn empty(
        doc: &mut Automerge,
        args: TransactionArgs,
        message: Option<String>,
        time: Option<i64>,
    ) -> ChangeHash {
        Self::new(args).commit_impl(doc, message, time)
    }

    pub(crate) fn pending_ops(&self) -> usize {
        self.operations.len()
    }

    /// Commit the operations performed in this transaction, returning the hashes corresponding to
    /// the new heads.
    ///
    /// Returns `None` if there were no operations to commit
    #[tracing::instrument(skip(self, doc))]
    pub(crate) fn commit(
        self,
        doc: &mut Automerge,
        message: Option<String>,
        time: Option<i64>,
    ) -> Option<ChangeHash> {
        if self.pending_ops() == 0 {
            return None;
        }
        Some(self.commit_impl(doc, message, time))
    }

    pub(crate) fn commit_impl(
        mut self,
        doc: &mut Automerge,
        message: Option<String>,
        time: Option<i64>,
    ) -> ChangeHash {
        if message.is_some() {
            self.message = message;
        }

        if let Some(t) = time {
            self.time = t;
        }

        let num_ops = self.pending_ops();
        let change = self.export(&doc.ops.m);
        let hash = change.hash();
        #[cfg(not(debug_assertions))]
        tracing::trace!(commit=?hash, deps=?change.deps(), "committing transaction");
        #[cfg(debug_assertions)]
        {
            let ops = change.iter_ops().collect::<Vec<_>>();
            tracing::trace!(commit=?hash, ?ops, deps=?change.deps(), "committing transaction");
        }
        doc.update_history(change, num_ops);
        debug_assert_eq!(doc.get_heads(), vec![hash]);
        hash
    }

    #[tracing::instrument(skip(self, metadata))]
    pub(crate) fn export(self, metadata: &OpSetMetadata) -> Change {
        use crate::storage::{change::PredOutOfOrder, convert::op_as_actor_id};

        let actor = metadata.actors.get(self.actor).clone();
        let ops = self.operations.iter().map(|o| (&o.0, &o.2));
        //let (ops, other_actors) = encode_change_ops(ops, actor.clone(), actors, props);
        let deps = self.deps.clone();
        let stored = match StoredChange::builder()
            .with_actor(actor)
            .with_seq(self.seq)
            .with_start_op(self.start_op)
            .with_message(self.message.clone())
            .with_dependencies(deps)
            .with_timestamp(self.time)
            .build(
                ops.into_iter()
                    .map(|(obj, op)| op_as_actor_id(obj, op, metadata)),
            ) {
            Ok(s) => s,
            Err(PredOutOfOrder) => {
                // SAFETY: types::Op::preds is `types::OpIds` which ensures ops are always sorted
                panic!("preds out of order");
            }
        };
        #[cfg(debug_assertions)]
        {
            let realized_ops = self.operations.iter().collect::<Vec<_>>();
            tracing::trace!(?stored, ops=?realized_ops, "committing change");
        }
        #[cfg(not(debug_assertions))]
        tracing::trace!(?stored, "committing change");
        Change::new(stored)
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub(crate) fn rollback(self, doc: &mut Automerge) -> usize {
        let num = self.pending_ops();
        // remove in reverse order so sets are removed before makes etc...
        for (obj, _prop, op) in self.operations.into_iter().rev() {
            for pred_id in &op.pred {
                if let Some(p) = doc.ops.search(&obj, OpIdSearch::new(*pred_id)).index() {
                    doc.ops.replace(&obj, p, |o| o.remove_succ(&op));
                }
            }
            if let Some(pos) = doc.ops.search(&obj, OpIdSearch::new(op.id)).index() {
                doc.ops.remove(&obj, pos);
            }
        }

        // remove the actor from the cache so that it doesn't end up in the saved document
        if doc.states.get(&self.actor).is_none() && doc.ops.m.actors.len() > 0 {
            let actor = doc.ops.m.actors.remove_last();
            doc.actor = Actor::Unused(actor);
        }

        num
    }

    /// Set the value of property `P` to value `V` in object `obj`.
    ///
    /// # Returns
    ///
    /// The opid of the operation which was created, or None if this operation doesn't change the
    /// document
    ///
    /// # Errors
    ///
    /// This will return an error if
    /// - The object does not exist
    /// - The key is the wrong type for the object
    /// - The key does not exist in the object
    pub(crate) fn put<P: Into<Prop>, V: Into<ScalarValue>, Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        ex_obj: &ExId,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        let value = value.into();
        let prop = prop.into();
        self.local_op(doc, op_observer, obj, prop, value.into())?;
        Ok(())
    }

    /// Set the value of property `P` to value `V` in object `obj`.
    ///
    /// # Returns
    ///
    /// The opid of the operation which was created, or None if this operation doesn't change the
    /// document
    ///
    /// # Errors
    ///
    /// This will return an error if
    /// - The object does not exist
    /// - The key is the wrong type for the object
    /// - The key does not exist in the object
    pub(crate) fn put_object<P: Into<Prop>, Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        ex_obj: &ExId,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        let prop = prop.into();
        let id = self
            .local_op(doc, op_observer, obj, prop, value.into())?
            .unwrap();
        let id = doc.id_to_exid(id);
        Ok(id)
    }

    fn next_id(&mut self) -> OpId {
        OpId(self.start_op.get() + self.pending_ops() as u64, self.actor)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_local_op<Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        prop: Prop,
        op: Op,
        pos: usize,
        obj: ObjId,
        succ_pos: &[usize],
    ) {
        doc.ops.add_succ(&obj, succ_pos.iter().copied(), &op);

        if !op.is_delete() {
            doc.ops.insert(pos, &obj, op.clone());
        }

        self.finalize_op(doc, op_observer, obj, prop, op);
    }

    pub(crate) fn insert<V: Into<ScalarValue>, Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        ex_obj: &ExId,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        let value = value.into();
        tracing::trace!(obj=?obj, value=?value, "inserting value");
        self.do_insert(doc, op_observer, obj, index, value.into())?;
        Ok(())
    }

    pub(crate) fn insert_object<Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        ex_obj: &ExId,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        let id = self.do_insert(doc, op_observer, obj, index, value.into())?;
        let id = doc.id_to_exid(id);
        Ok(id)
    }

    fn do_insert<Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        obj: ObjId,
        index: usize,
        action: OpType,
    ) -> Result<OpId, AutomergeError> {
        let id = self.next_id();

        let query = doc.ops.search(&obj, query::InsertNth::new(index));

        let key = query.key()?;

        let op = Op {
            id,
            action,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        };

        doc.ops.insert(query.pos(), &obj, op.clone());

        self.finalize_op(doc, op_observer, obj, Prop::Seq(index), op);

        Ok(id)
    }

    pub(crate) fn local_op<Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        obj: ObjId,
        prop: Prop,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        match prop {
            Prop::Map(s) => self.local_map_op(doc, op_observer, obj, s, action),
            Prop::Seq(n) => self.local_list_op(doc, op_observer, obj, n, action),
        }
    }

    fn local_map_op<Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        obj: ObjId,
        prop: String,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        if prop.is_empty() {
            return Err(AutomergeError::EmptyStringKey);
        }

        let id = self.next_id();
        let prop_index = doc.ops.m.props.cache(prop.clone());
        let query = doc.ops.search(&obj, query::Prop::new(prop_index));

        // no key present to delete
        if query.ops.is_empty() && action == OpType::Delete {
            return Ok(None);
        }

        if query.ops.len() == 1 && query.ops[0].is_noop(&action) {
            return Ok(None);
        }

        // increment operations are only valid against counter values.
        // if there are multiple values (from conflicts) then we just need one of them to be a counter.
        if matches!(action, OpType::Increment(_)) && query.ops.iter().all(|op| !op.is_counter()) {
            return Err(AutomergeError::MissingCounter);
        }

        let pred = doc.ops.m.sorted_opids(query.ops.iter().map(|o| o.id));

        let op = Op {
            id,
            action,
            key: Key::Map(prop_index),
            succ: Default::default(),
            pred,
            insert: false,
        };

        let pos = query.pos;
        let ops_pos = query.ops_pos;
        self.insert_local_op(doc, op_observer, Prop::Map(prop), op, pos, obj, &ops_pos);

        Ok(Some(id))
    }

    fn local_list_op<Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        obj: ObjId,
        index: usize,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        let query = doc.ops.search(&obj, query::Nth::new(index));

        let id = self.next_id();
        let pred = doc.ops.m.sorted_opids(query.ops.iter().map(|o| o.id));
        let key = query.key()?;

        if query.ops.len() == 1 && query.ops[0].is_noop(&action) {
            return Ok(None);
        }

        // increment operations are only valid against counter values.
        // if there are multiple values (from conflicts) then we just need one of them to be a counter.
        if matches!(action, OpType::Increment(_)) && query.ops.iter().all(|op| !op.is_counter()) {
            return Err(AutomergeError::MissingCounter);
        }

        let op = Op {
            id,
            action,
            key,
            succ: Default::default(),
            pred,
            insert: false,
        };

        let pos = query.pos;
        let ops_pos = query.ops_pos;
        self.insert_local_op(doc, op_observer, Prop::Seq(index), op, pos, obj, &ops_pos);

        Ok(Some(id))
    }

    pub(crate) fn increment<P: Into<Prop>, Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        obj: &ExId,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        self.local_op(doc, op_observer, obj, prop.into(), OpType::Increment(value))?;
        Ok(())
    }

    pub(crate) fn delete<P: Into<Prop>, Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        ex_obj: &ExId,
        prop: P,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        let prop = prop.into();
        self.local_op(doc, op_observer, obj, prop, OpType::Delete)?;
        Ok(())
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements
    pub(crate) fn splice<Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        mut op_observer: Option<&mut Obs>,
        ex_obj: &ExId,
        mut pos: usize,
        del: usize,
        vals: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        for _ in 0..del {
            // This unwrap and rewrap of the option is necessary to appeas the borrow checker :(
            if let Some(obs) = op_observer.as_mut() {
                self.local_op(doc, Some(*obs), obj, pos.into(), OpType::Delete)?;
            } else {
                self.local_op::<Obs>(doc, None, obj, pos.into(), OpType::Delete)?;
            }
        }
        for v in vals {
            // As above this unwrap and rewrap of the option is necessary to appeas the borrow checker :(
            if let Some(obs) = op_observer.as_mut() {
                self.do_insert(doc, Some(*obs), obj, pos, v.clone().into())?;
            } else {
                self.do_insert::<Obs>(doc, None, obj, pos, v.clone().into())?;
            }
            pos += 1;
        }
        Ok(())
    }

    fn finalize_op<Obs: OpObserver>(
        &mut self,
        doc: &mut Automerge,
        op_observer: Option<&mut Obs>,
        obj: ObjId,
        prop: Prop,
        op: Op,
    ) {
        // TODO - id_to_exid should be a noop if not used - change type to Into<ExId>?
        if let Some(op_observer) = op_observer {
            let ex_obj = doc.ops.id_to_exid(obj.0);
            let parents = doc.ops.parents(obj);
            if op.insert {
                let value = (op.value(), doc.ops.id_to_exid(op.id));
                match prop {
                    Prop::Map(_) => panic!("insert into a map"),
                    Prop::Seq(index) => op_observer.insert(parents, ex_obj, index, value),
                }
            } else if op.is_delete() {
                op_observer.delete(parents, ex_obj, prop.clone());
            } else if let Some(value) = op.get_increment_value() {
                op_observer.increment(
                    parents,
                    ex_obj,
                    prop.clone(),
                    (value, doc.ops.id_to_exid(op.id)),
                );
            } else {
                let value = (op.value(), doc.ops.id_to_exid(op.id));
                op_observer.put(parents, ex_obj, prop.clone(), value, false);
            }
        }
        self.operations.push((obj, prop, op));
    }
}

#[cfg(test)]
mod tests {
    use crate::{transaction::Transactable, ROOT};

    use super::*;

    #[test]
    fn map_rollback_doesnt_panic() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();

        let a = tx.put_object(ROOT, "a", ObjType::Map).unwrap();
        tx.put(&a, "b", 1).unwrap();
        assert!(tx.get(&a, "b").unwrap().is_some());
    }
}

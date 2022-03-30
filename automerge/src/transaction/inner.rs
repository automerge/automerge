use std::collections::HashMap;
use std::num::NonZeroU64;

use crate::automerge::Actor;
use crate::exid::ExId;
use crate::query::{self, InsertNth, OpIdSearch};
use crate::types::{ElemId, Key, ObjId, OpId};
use crate::{change::export_change, types::Op, Automerge, ChangeHash, Prop};
use crate::{AutomergeError, ObjType, OpType, ScalarValue};

#[derive(Debug, Clone)]
pub(crate) struct InsertBuffer {
    /// index that the first op in this buffer wanted to insert at
    target_index: usize,
    /// number of actions this buffer has seen
    num_actions: usize,
    /// index in the optree that the first op in this buffer was inserted in
    tree_index: usize,
    /// OpId of the last insert operation, used as the key for the next.
    last_id: OpId,
}

impl InsertBuffer {
    pub fn new(id: OpId, index: usize, tree_index: usize) -> Self {
        Self {
            target_index: index,
            num_actions: 1,
            tree_index,
            last_id: id,
        }
    }

    pub fn push(
        &mut self,
        doc: &Automerge,
        obj: &ObjId,
        id: OpId,
        index: usize,
    ) -> Result<Key, AutomergeError> {
        if self.target_index + self.num_actions == index {
            // this is an insert into the same object and at the next index so we can group it
            self.num_actions += 1;
            // key is the id of the last valid insert, since these are sequential inserts we
            // can just use the last insert's id
            let key = Ok(Key::Seq(ElemId(self.last_id)));
            self.last_id = id;
            key
        } else {
            // this buffer is not valid for this insert so start a new one
            let query = doc.ops.search(obj, InsertNth::new(index));
            *self = InsertBuffer {
                last_id: id,
                target_index: index,
                num_actions: 1,
                tree_index: query.pos(),
            };
            query.key()
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionInner {
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) start_op: NonZeroU64,
    pub(crate) time: i64,
    pub(crate) message: Option<String>,
    pub(crate) extra_bytes: Vec<u8>,
    pub(crate) hash: Option<ChangeHash>,
    pub(crate) deps: Vec<ChangeHash>,
    pub(crate) operations: Vec<(ObjId, Op)>,
    /// Buffers to capture runs of inserts efficiently, one per object.
    pub(crate) insert_buffers: HashMap<ObjId, InsertBuffer>,
}

impl TransactionInner {
    pub fn pending_ops(&self) -> usize {
        self.operations.len()
    }

    /// Commit the operations performed in this transaction, returning the hashes corresponding to
    /// the new heads.
    pub fn commit(
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

        let num_ops = self.operations.len();
        let change = export_change(self, &doc.ops.m.actors, &doc.ops.m.props);
        let hash = change.hash;
        doc.update_history(change, num_ops);
        debug_assert_eq!(doc.get_heads(), vec![hash]);
        hash
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub fn rollback(self, doc: &mut Automerge) -> usize {
        // remove the actor from the cache so that it doesn't end up in the saved document
        if doc.states.get(&self.actor).is_none() {
            let actor = doc.ops.m.actors.remove_last();
            doc.actor = Actor::Unused(actor);
        }

        let num = self.operations.len();
        // remove in reverse order so sets are removed before makes etc...
        for (obj, op) in self.operations.iter().rev() {
            for pred_id in &op.pred {
                if let Some(p) = doc.ops.search(obj, OpIdSearch::new(*pred_id)).index() {
                    doc.ops.replace(obj, p, |o| o.remove_succ(op));
                }
            }
            if let Some(pos) = doc.ops.search(obj, OpIdSearch::new(op.id)).index() {
                doc.ops.remove(obj, pos);
            }
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
    pub fn set<P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        doc: &mut Automerge,
        obj: &ExId,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        let value = value.into();
        self.local_op(doc, obj, prop.into(), value.into())?;
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
    pub fn set_object<P: Into<Prop>>(
        &mut self,
        doc: &mut Automerge,
        obj: &ExId,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        let id = self.local_op(doc, obj, prop.into(), value.into())?.unwrap();
        Ok(doc.id_to_exid(id))
    }

    fn next_id(&self) -> OpId {
        OpId(
            self.start_op.get() + self.operations.len() as u64,
            self.actor,
        )
    }

    fn insert_local_op(
        &mut self,
        doc: &mut Automerge,
        op: Op,
        pos: usize,
        obj: ObjId,
        succ_pos: &[usize],
    ) {
        for succ in succ_pos {
            doc.ops.replace(&obj, *succ, |old_op| {
                old_op.add_succ(&op);
            });
        }

        if !op.is_del() {
            doc.ops.insert(pos, &obj, op.clone());
        }

        self.operations.push((obj, op));
    }

    pub fn insert<V: Into<ScalarValue>>(
        &mut self,
        doc: &mut Automerge,
        obj: &ExId,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        let value = value.into();
        self.do_insert(doc, obj, index, value.into())?;
        Ok(())
    }

    pub fn insert_object(
        &mut self,
        doc: &mut Automerge,
        obj: &ExId,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        let id = self.do_insert(doc, obj, index, value.into())?.unwrap();
        Ok(doc.id_to_exid(id))
    }

    fn do_insert(
        &mut self,
        doc: &mut Automerge,
        obj: ObjId,
        index: usize,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        let id = self.next_id();
        let mut key = None;
        let buffer = self
            .insert_buffers
            .entry(obj)
            .and_modify(|buffer| key = Some(buffer.push(doc, &obj, id, index)))
            .or_insert_with(|| {
                let query = doc.ops.search(&obj, InsertNth::new(index));
                key = Some(query.key());
                InsertBuffer::new(id, index, query.pos())
            });

        // SAFETY: we set this key in all branches above.
        let key = key.unwrap()?;

        let is_make = matches!(&action, OpType::Make(_));

        let op = Op {
            id,
            action,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        };

        doc.ops
            .insert(buffer.tree_index + buffer.num_actions - 1, &obj, op.clone());

        self.operations.push((obj, op));

        if is_make {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn local_op(
        &mut self,
        doc: &mut Automerge,
        obj: ObjId,
        prop: Prop,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        // invalidate the insert buffer as we may clobber things it is targetting
        self.insert_buffers.remove(&obj);
        match prop {
            Prop::Map(s) => self.local_map_op(doc, obj, s, action),
            Prop::Seq(n) => self.local_list_op(doc, obj, n, action),
        }
    }

    fn local_map_op(
        &mut self,
        doc: &mut Automerge,
        obj: ObjId,
        prop: String,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        if prop.is_empty() {
            return Err(AutomergeError::EmptyStringKey);
        }

        let id = self.next_id();
        let prop = doc.ops.m.props.cache(prop);
        let query = doc.ops.search(&obj, query::Prop::new(prop));

        // no key present to delete
        if query.ops.is_empty() && action == OpType::Del {
            return Ok(None);
        }

        if query.ops.len() == 1 && query.ops[0].is_noop(&action) {
            return Ok(None);
        }

        let is_make = matches!(&action, OpType::Make(_));

        let pred = query.ops.iter().map(|op| op.id).collect();

        let op = Op {
            id,
            action,
            key: Key::Map(prop),
            succ: Default::default(),
            pred,
            insert: false,
        };

        self.insert_local_op(doc, op, query.pos, obj, &query.ops_pos);

        if is_make {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    fn local_list_op(
        &mut self,
        doc: &mut Automerge,
        obj: ObjId,
        index: usize,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        let query = doc.ops.search(&obj, query::Nth::new(index));

        let id = self.next_id();
        let pred = query.ops.iter().map(|op| op.id).collect();
        let key = query.key()?;

        if query.ops.len() == 1 && query.ops[0].is_noop(&action) {
            return Ok(None);
        }

        let is_make = matches!(&action, OpType::Make(_));

        let op = Op {
            id,
            action,
            key,
            succ: Default::default(),
            pred,
            insert: false,
        };

        self.insert_local_op(doc, op, query.pos, obj, &query.ops_pos);

        if is_make {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    pub fn inc<P: Into<Prop>>(
        &mut self,
        doc: &mut Automerge,
        obj: &ExId,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        self.local_op(doc, obj, prop.into(), OpType::Inc(value))?;
        Ok(())
    }

    pub fn del<P: Into<Prop>>(
        &mut self,
        doc: &mut Automerge,
        obj: &ExId,
        prop: P,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        self.local_op(doc, obj, prop.into(), OpType::Del)?;
        Ok(())
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements
    pub fn splice(
        &mut self,
        doc: &mut Automerge,
        obj: &ExId,
        mut pos: usize,
        del: usize,
        vals: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        for _ in 0..del {
            // del()
            self.local_op(doc, obj, pos.into(), OpType::Del)?;
        }
        for v in vals {
            // insert()
            self.do_insert(doc, obj, pos, v.into())?;
            pos += 1;
        }
        Ok(())
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

        let a = tx.set_object(ROOT, "a", ObjType::Map).unwrap();
        tx.set(&a, "b", 1).unwrap();
        assert!(tx.value(&a, "b").unwrap().is_some());
    }
}

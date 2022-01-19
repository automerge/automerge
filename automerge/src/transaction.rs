use std::ops::Deref;

use crate::exid::ExId;
use crate::query;
use crate::types::{Key, ObjId, OpId};
use crate::{change::export_change, types::Op, Automerge, ChangeHash, Prop, Value};
use crate::{AutomergeError, OpType};
use unicode_segmentation::UnicodeSegmentation;

#[derive(Debug)]
pub struct Transaction<'a> {
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) start_op: u64,
    pub(crate) time: i64,
    pub(crate) message: Option<String>,
    pub(crate) extra_bytes: Vec<u8>,
    pub(crate) hash: Option<ChangeHash>,
    pub(crate) deps: Vec<ChangeHash>,
    pub(crate) operations: Vec<Op>,
    pub(crate) doc: &'a mut Automerge,
}

impl<'a> Transaction<'a> {
    pub fn pending_ops(&self) -> usize {
        self.operations.len()
    }

    /// Commit the operations performed in this transaction, returning the hashes corresponding to
    /// the new heads.
    pub fn commit(mut self, message: Option<String>, time: Option<i64>) -> Vec<ChangeHash> {
        if message.is_some() {
            self.message = message;
        }

        if let Some(t) = time {
            self.time = t;
        }

        self.operations.len();

        self.doc.update_history(export_change(
            &self,
            &self.doc.ops.m.actors,
            &self.doc.ops.m.props,
        ));

        self.doc.get_heads()
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub fn rollback(self) -> usize {
        let num = self.operations.len();
        for op in &self.operations {
            for pred_id in &op.pred {
                // FIXME - use query to make this fast
                if let Some(p) = self.doc.ops.iter().position(|o| o.id == *pred_id) {
                    self.doc.ops.replace(op.obj, p, |o| o.remove_succ(op));
                }
            }
            if let Some(pos) = self.doc.ops.iter().position(|o| o.id == op.id) {
                self.doc.ops.remove(op.obj, pos);
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
    pub fn set<P: Into<Prop>, V: Into<Value>>(
        &mut self,
        obj: &ExId,
        prop: P,
        value: V,
    ) -> Result<Option<ExId>, AutomergeError> {
        let obj = self.doc.exid_to_obj(obj)?;
        let value = value.into();
        if let Some(id) = self.local_op(obj, prop.into(), value.into())? {
            Ok(Some(self.doc.id_to_exid(id)))
        } else {
            Ok(None)
        }
    }

    fn next_id(&mut self) -> OpId {
        OpId(self.start_op + self.operations.len() as u64, self.actor)
    }

    fn insert_local_op(&mut self, op: Op, pos: usize, succ_pos: &[usize]) {
        for succ in succ_pos {
            self.doc.ops.replace(op.obj, *succ, |old_op| {
                old_op.add_succ(&op);
            });
        }

        if !op.is_del() {
            self.doc.ops.insert(pos, op.clone());
        }

        self.operations.push(op);
    }

    pub fn insert<V: Into<Value>>(
        &mut self,
        obj: &ExId,
        index: usize,
        value: V,
    ) -> Result<Option<ExId>, AutomergeError> {
        let obj = self.doc.exid_to_obj(obj)?;
        if let Some(id) = self.do_insert(obj, index, value)? {
            Ok(Some(self.doc.id_to_exid(id)))
        } else {
            Ok(None)
        }
    }

    fn do_insert<V: Into<Value>>(
        &mut self,
        obj: ObjId,
        index: usize,
        value: V,
    ) -> Result<Option<OpId>, AutomergeError> {
        let id = self.next_id();

        let query = self.doc.ops.search(obj, query::InsertNth::new(index));

        let key = query.key()?;
        let value = value.into();
        let action = value.into();
        let is_make = matches!(&action, OpType::Make(_));

        let op = Op {
            change: self.doc.history.len(),
            id,
            action,
            obj,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        };

        self.doc.ops.insert(query.pos(), op.clone());
        self.operations.push(op);

        if is_make {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn local_op(
        &mut self,
        obj: ObjId,
        prop: Prop,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        match prop {
            Prop::Map(s) => self.local_map_op(obj, s, action),
            Prop::Seq(n) => self.local_list_op(obj, n, action),
        }
    }

    fn local_map_op(
        &mut self,
        obj: ObjId,
        prop: String,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        if prop.is_empty() {
            return Err(AutomergeError::EmptyStringKey);
        }

        let id = self.next_id();
        let prop = self.doc.ops.m.props.cache(prop);
        let query = self.doc.ops.search(obj, query::Prop::new(prop));

        if query.ops.len() == 1 && query.ops[0].is_noop(&action) {
            return Ok(None);
        }

        let is_make = matches!(&action, OpType::Make(_));

        let pred = query.ops.iter().map(|op| op.id).collect();

        let op = Op {
            change: self.doc.history.len(),
            id,
            action,
            obj,
            key: Key::Map(prop),
            succ: Default::default(),
            pred,
            insert: false,
        };

        self.insert_local_op(op, query.pos, &query.ops_pos);

        if is_make {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    fn local_list_op(
        &mut self,
        obj: ObjId,
        index: usize,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        let query = self.doc.ops.search(obj, query::Nth::new(index));

        let id = self.next_id();
        let pred = query.ops.iter().map(|op| op.id).collect();
        let key = query.key()?;

        if query.ops.len() == 1 && query.ops[0].is_noop(&action) {
            return Ok(None);
        }

        let is_make = matches!(&action, OpType::Make(_));

        let op = Op {
            change: self.doc.history.len(),
            id,
            action,
            obj,
            key,
            succ: Default::default(),
            pred,
            insert: false,
        };

        self.insert_local_op(op, query.pos, &query.ops_pos);

        if is_make {
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    pub fn inc<P: Into<Prop>>(
        &mut self,
        obj: &ExId,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        let obj = self.doc.exid_to_obj(obj)?;
        self.local_op(obj, prop.into(), OpType::Inc(value))?;
        Ok(())
    }

    pub fn del<P: Into<Prop>>(&mut self, obj: &ExId, prop: P) -> Result<(), AutomergeError> {
        let obj = self.doc.exid_to_obj(obj)?;
        self.local_op(obj, prop.into(), OpType::Del)?;
        Ok(())
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements
    pub fn splice(
        &mut self,
        obj: &ExId,
        mut pos: usize,
        del: usize,
        vals: Vec<Value>,
    ) -> Result<Vec<ExId>, AutomergeError> {
        let obj = self.doc.exid_to_obj(obj)?;
        for _ in 0..del {
            // del()
            self.local_op(obj, pos.into(), OpType::Del)?;
        }
        let mut results = Vec::new();
        for v in vals {
            // insert()
            let id = self.do_insert(obj, pos, v.clone())?;
            if let Some(id) = id {
                results.push(self.doc.id_to_exid(id));
            }
            pos += 1;
        }
        Ok(results)
    }

    pub fn splice_text(
        &mut self,
        obj: &ExId,
        pos: usize,
        del: usize,
        text: &str,
    ) -> Result<Vec<ExId>, AutomergeError> {
        let mut vals = vec![];
        for c in text.to_owned().graphemes(true) {
            vals.push(c.into());
        }
        self.splice(obj, pos, del, vals)
    }
}

impl<'a> Deref for Transaction<'a> {
    type Target = Automerge;

    fn deref(&self) -> &Self::Target {
        self.doc
    }
}

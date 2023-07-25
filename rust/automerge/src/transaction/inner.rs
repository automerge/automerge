use std::num::NonZeroU64;
use std::rc::Rc;

use crate::exid::ExId;
use crate::hydrate::{Map, Value};
use crate::marks::{ExpandMark, Mark, RichText};
use crate::patches::{PatchLog, TextRepresentation};
use crate::query::{self, OpIdSearch};
use crate::storage::Change as StoredChange;
use crate::types::{Clock, Key, ListEncoding, ObjId, ObjMeta, OpId, OpIds};
use crate::{op_tree::OpSetMetadata, types::Op, Automerge, Change, ChangeHash, Prop};
use crate::{AutomergeError, ObjType, OpType, ScalarValue};

#[derive(Debug, Clone)]
pub(crate) struct TransactionInner {
    actor: usize,
    seq: u64,
    start_op: NonZeroU64,
    time: i64,
    message: Option<String>,
    deps: Vec<ChangeHash>,
    scope: Option<Clock>,
    operations: Vec<(ObjId, Op)>,
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
    /// The scope that should be visible to the transaction
    pub(crate) scope: Option<Clock>,
}

impl TransactionInner {
    pub(crate) fn new(
        TransactionArgs {
            actor_index: actor,
            seq,
            start_op,
            deps,
            scope,
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
            scope,
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
        let change = self.export(&doc.ops().m);
        let hash = change.hash();
        #[cfg(not(debug_assertions))]
        tracing::trace!(commit=?hash, deps=?change.deps(), "committing transaction");
        #[cfg(debug_assertions)]
        {
            let ops = change.iter_ops().collect::<Vec<_>>();
            tracing::trace!(commit=?hash, ?ops, deps=?change.deps(), "committing transaction");
        }
        doc.update_history(change, num_ops);
        //debug_assert_eq!(doc.get_heads(), vec![hash]);
        hash
    }

    #[tracing::instrument(skip(self, metadata))]
    pub(crate) fn export(self, metadata: &OpSetMetadata) -> Change {
        use crate::storage::{change::PredOutOfOrder, convert::op_as_actor_id};

        let actor = metadata.actors.get(self.actor).clone();
        let deps = self.deps.clone();
        let stored = match StoredChange::builder()
            .with_actor(actor)
            .with_seq(self.seq)
            .with_start_op(self.start_op)
            .with_message(self.message.clone())
            .with_dependencies(deps)
            .with_timestamp(self.time)
            .build(
                self.operations
                    .iter()
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
        let encoding = ListEncoding::List; // encoding doesnt matter here - we dont care what the index is
        for (obj, op) in self.operations.into_iter().rev() {
            for pred_id in &op.pred {
                if let Some(p) = doc
                    .ops()
                    .search(&obj, OpIdSearch::opid(*pred_id, encoding, None))
                    .found()
                {
                    doc.ops_mut().change_vis(&obj, p, |o| o.remove_succ(&op));
                }
            }
            if let Some(pos) = doc
                .ops()
                .search(&obj, OpIdSearch::opid(op.id, encoding, None))
                .found()
            {
                doc.ops_mut().remove(&obj, pos);
            }
        }

        doc.rollback_last_actor();

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
    pub(crate) fn put<P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        prop: P,
        value: V,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj, patch_log.text_rep())?;
        let value = value.into();
        let prop = prop.into();
        match (&prop, obj.typ) {
            (Prop::Map(_), ObjType::Map) => Ok(()),
            (Prop::Seq(_), ObjType::List) => Ok(()),
            (Prop::Seq(_), ObjType::Text) => Ok(()),
            _ => Err(AutomergeError::InvalidOp(obj.typ)),
        }?;
        self.local_op(doc, patch_log, &obj, prop, value.into())?;
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
    pub(crate) fn put_object<P: Into<Prop>>(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        prop: P,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj, patch_log.text_rep())?;
        let prop = prop.into();
        match (&prop, obj.typ) {
            (Prop::Map(_), ObjType::Map) => Ok(()),
            (Prop::Seq(_), ObjType::List) => Ok(()),
            _ => Err(AutomergeError::InvalidOp(obj.typ)),
        }?;
        let id = self
            .local_op(doc, patch_log, &obj, prop, value.into())?
            .unwrap();
        let id = doc.id_to_exid(id);
        Ok(id)
    }

    fn next_id(&mut self) -> OpId {
        OpId::new(self.start_op.get() + self.pending_ops() as u64, self.actor)
    }

    fn next_insert(&mut self, key: Key, value: ScalarValue) -> Op {
        Op {
            id: self.next_id(),
            action: OpType::Put(value),
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        }
    }

    fn next_delete(&mut self, key: Key, pred: OpIds) -> Op {
        Op {
            id: self.next_id(),
            action: OpType::Delete,
            key,
            succ: Default::default(),
            pred,
            insert: false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_local_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        prop: Prop,
        op: Op,
        pos: usize,
        obj: &ObjMeta,
        succ_pos: &[usize],
    ) {
        doc.ops_mut().add_succ(&obj.id, succ_pos, &op);

        if !op.is_delete() {
            doc.ops_mut().insert(pos, &obj.id, op.clone());
        }

        self.finalize_op(patch_log, obj, prop, op, None);
    }

    pub(crate) fn insert<V: Into<ScalarValue>>(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj, patch_log.text_rep())?;
        if !matches!(obj.typ, ObjType::List | ObjType::Text) {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let value = value.into();
        tracing::trace!(obj=?obj, value=?value, "inserting value");
        self.do_insert(doc, patch_log, &obj, index, value.into())?;
        Ok(())
    }

    pub(crate) fn insert_object(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        index: usize,
        value: ObjType,
    ) -> Result<ExId, AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj, patch_log.text_rep())?;
        if !matches!(obj.typ, ObjType::List | ObjType::Text) {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let id = self.do_insert(doc, patch_log, &obj, index, value.into())?;
        let id = doc.id_to_exid(id);
        Ok(id)
    }

    fn do_insert(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
        index: usize,
        action: OpType,
    ) -> Result<OpId, AutomergeError> {
        let id = self.next_id();

        let query = doc.ops().search(
            &obj.id,
            query::InsertNth::new(index, obj.encoding, self.scope.clone()),
        );
        let marks = query.marks(&doc.ops().m);
        let pos = query.pos();
        let key = query.key()?;

        let op = Op {
            id,
            action,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        };

        doc.ops_mut().insert(pos, &obj.id, op.clone());

        self.finalize_op(patch_log, obj, Prop::Seq(index), op, marks);

        Ok(id)
    }

    pub(crate) fn local_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
        prop: Prop,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        match prop {
            Prop::Map(s) => self.local_map_op(doc, patch_log, obj, s, action),
            Prop::Seq(n) => self.local_list_op(doc, patch_log, obj, n, action),
        }
    }

    fn local_map_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
        prop: String,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        if prop.is_empty() {
            return Err(AutomergeError::EmptyStringKey);
        }

        let id = self.next_id();
        let prop_index = doc.ops_mut().m.props.cache(prop.clone());
        let key = Key::Map(prop_index);
        let prop: Prop = prop.into();
        let query =
            doc.ops()
                .seek_ops_by_prop(&obj.id, prop.clone(), obj.encoding, self.scope.as_ref());
        let ops = query.ops;
        let ops_pos = query.ops_pos;

        // no key present to delete
        if ops.is_empty() && action == OpType::Delete {
            return Ok(None);
        }

        if ops.len() == 1 && ops[0].is_noop(&action) {
            return Ok(None);
        }

        // increment operations are only valid against counter values.
        // if there are multiple values (from conflicts) then we just need one of them to be a counter.
        if matches!(action, OpType::Increment(_)) && ops.iter().all(|op| !op.is_counter()) {
            return Err(AutomergeError::MissingCounter);
        }

        let pred = doc.ops().m.sorted_opids(ops.iter().map(|o| o.id));

        let op = Op {
            id,
            action,
            key,
            succ: Default::default(),
            pred,
            insert: false,
        };

        let pos = query.end_pos;
        self.insert_local_op(doc, patch_log, prop, op, pos, obj, &ops_pos);

        Ok(Some(id))
    }

    fn local_list_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
        index: usize,
        action: OpType,
    ) -> Result<Option<OpId>, AutomergeError> {
        let query = doc.ops().search(
            &obj.id,
            query::Nth::new(index, obj.encoding, self.scope.clone()),
        );

        let id = self.next_id();
        let pred = doc.ops().m.sorted_opids(query.ops.iter().map(|o| o.id));
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

        let pos = query.pos();
        let ops_pos = query.ops_pos;
        self.insert_local_op(doc, patch_log, Prop::Seq(index), op, pos, obj, &ops_pos);

        Ok(Some(id))
    }

    pub(crate) fn increment<P: Into<Prop>>(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ExId,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(obj, patch_log.text_rep())?;
        self.local_op(doc, patch_log, &obj, prop.into(), OpType::Increment(value))?;
        Ok(())
    }

    pub(crate) fn delete<P: Into<Prop>>(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        prop: P,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj, patch_log.text_rep())?;
        let prop = prop.into();
        if obj.typ == ObjType::Text {
            let index = prop.as_index().ok_or(AutomergeError::InvalidOp(obj.typ))?;
            self.inner_splice(
                doc,
                patch_log,
                SpliceArgs {
                    obj,
                    index,
                    del: 1,
                    values: vec![],
                    splice_type: SpliceType::Text(""),
                },
            )?;
        } else {
            self.local_op(doc, patch_log, &obj, prop, OpType::Delete)?;
        }
        Ok(())
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements
    pub(crate) fn splice(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        index: usize,
        del: isize,
        vals: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj, patch_log.text_rep())?;
        if !matches!(obj.typ, ObjType::List | ObjType::Text) {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let values = vals.into_iter().collect();
        self.inner_splice(
            doc,
            patch_log,
            SpliceArgs {
                obj,
                index,
                del,
                values,
                splice_type: SpliceType::List,
            },
        )
    }

    /// Splice string into a text object
    pub(crate) fn splice_text(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        index: usize,
        del: isize,
        text: &str,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj, patch_log.text_rep())?;
        if obj.typ != ObjType::Text {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let values = text.chars().map(ScalarValue::from).collect();
        self.inner_splice(
            doc,
            patch_log,
            SpliceArgs {
                obj,
                index,
                del,
                values,
                splice_type: SpliceType::Text(text),
            },
        )
    }

    fn inner_splice(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        SpliceArgs {
            obj,
            mut index,
            mut del,
            values,
            splice_type,
        }: SpliceArgs<'_>,
    ) -> Result<(), AutomergeError> {
        if del < 0 {
            if let Some(n) = index.checked_add_signed(del) {
                index = n;
                del = del.abs();
            } else {
                return Err(AutomergeError::InvalidIndex(index));
            }
        }

        //let ex_obj = doc.ops().id_to_exid(obj.0);
        let encoding = splice_type.encoding();
        // delete `del` items - performing the query for each one
        let mut deleted: usize = 0;
        while deleted < (del as usize) {
            // TODO: could do this with a single custom query
            let query = doc.ops().search(
                &obj.id,
                query::Nth::new(index, encoding, self.scope.clone()),
            );

            // if we delete in the middle of a multi-character
            // move cursor back to the beginning and expand the del width
            let adjusted_index = query.index();
            if adjusted_index < index {
                del += (index - adjusted_index) as isize;
                index = adjusted_index;
            }

            let step = if let Some(op) = query.ops.last() {
                op.width(encoding)
            } else {
                break;
            };

            let op = self.next_delete(query.key()?, query.pred(doc.ops()));

            let ops_pos = query.ops_pos;
            doc.ops_mut().add_succ(&obj.id, &ops_pos, &op);

            self.operations.push((obj.id, op));

            deleted += step;
        }

        if deleted > 0 && patch_log.is_active() {
            patch_log.delete_seq(&obj, index, deleted, None);
        }

        // do the insert query for the first item and then
        // insert the remaining ops one after the other
        if !values.is_empty() {
            let query = doc.ops().search(
                &obj.id,
                query::InsertNth::new(index, encoding, self.scope.clone()),
            );
            let mut pos = query.pos();
            let mut key = query.key()?;
            let marks = query.marks(&doc.ops().m);
            let mut cursor = index;
            let mut width = 0;

            for v in &values {
                let op = self.next_insert(key, v.clone());

                doc.ops_mut().insert(pos, &obj.id, op.clone());

                width = op.width(encoding);
                cursor += width;
                pos += 1;
                key = op.id.into();

                self.operations.push((obj.id, op));
            }

            doc.ops_mut()
                .hint(&obj.id, cursor - width, pos - 1, width, key);

            if patch_log.is_active() {
                match splice_type {
                    SpliceType::Text(text)
                        if matches!(patch_log.text_rep(), TextRepresentation::String) =>
                    {
                        patch_log.splice(&obj, index, text, marks);
                    }
                    SpliceType::List | SpliceType::Text(..) => {
                        let start = self.operations.len() - values.len();
                        for (offset, v) in values.iter().enumerate() {
                            let op = &self.operations[start + offset].1;
                            patch_log.insert(&obj, index + offset, v.clone().into(), op.id, false);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn mark(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        mark: Mark<'_>,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj, patch_log.text_rep())?;
        let action = OpType::MarkBegin(expand.before(), mark.data.clone().into_owned());
        self.do_insert(doc, patch_log, &obj, mark.start, action)?;
        self.do_insert(
            doc,
            patch_log,
            &obj,
            mark.end,
            OpType::MarkEnd(expand.after()),
        )?;
        if patch_log.is_active() {
            patch_log.mark(&obj, mark.start, mark.len(), &mark.into_mark_set());
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn unmark(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        name: &str,
        start: usize,
        end: usize,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        let mark = Mark::new(name.to_string(), ScalarValue::Null, start, end);
        self.mark(doc, patch_log, ex_obj, mark, expand)
    }

    pub(crate) fn split_block(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        index: usize,
    ) -> Result<ExId, AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj, patch_log.text_rep())?;
        if obj.typ != ObjType::Text {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }

        let action = OpType::Make(ObjType::Map);
        let id = self.next_id();

        let query = doc.ops().search(
            &obj.id,
            query::InsertNth::new(index, obj.encoding, self.scope.clone()),
        );
        let pos = query.pos();
        let key = query.key()?;

        let op = Op {
            id,
            action,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        };

        doc.ops_mut().insert(pos, &obj.id, op.clone());

        patch_log.insert(&obj, index, Value::Map(Map::default()), op.id, false);

        self.operations.push((obj.id, op));

        let id = doc.id_to_exid(id);
        Ok(id)
    }

    pub(crate) fn join_block(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        block: &ExId,
    ) -> Result<(), AutomergeError> {
        let block = doc.exid_to_obj(block, patch_log.text_rep())?;
        // FIXME - unwrap
        let obj = doc.ops().parent_obj_id(&block.id).unwrap();
        let obj = doc.get_obj_meta(obj, patch_log.text_rep())?;

        if obj.typ != ObjType::Text {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }

        // TODO - inline this
        self.update_block_inner(doc, patch_log, obj, &block.id.into())?;

        Ok(())
    }

    fn update_block_inner(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: ObjMeta,
        block_id: &OpId,
    ) -> Result<(), AutomergeError> {
        let key = Key::Seq((*block_id).into());

        let action = OpType::Delete;

        let mut op = Op {
            id: self.next_id(),
            action,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: false,
        };

        let ops = doc.ops_mut();
        let query = ops.search(&obj.id, query::OpIdSearch::op(&op, obj.encoding));
        let index = query.index();
        let mut pos = query.pos();
        let mut pred_ids = vec![];
        let mut succ_pos = vec![];
        {
            let mut iter = ops.iter_ops(&obj.id);
            let mut next = iter.nth(pos);
            while let Some(e) = next {
                if e.elemid_or_key() != op.elemid_or_key() {
                    break;
                }
                let visible = e.visible_at(self.scope.as_ref());
                if visible {
                    succ_pos.push(pos);
                }
                pos += 1;
                pred_ids.push(e.id);
                next = iter.next();
            }
        }

        op.pred = ops.m.sorted_opids(pred_ids.into_iter());

        ops.add_succ(&obj.id, &succ_pos, &op);

        patch_log.delete_seq(&obj, index, 1, Some(*block_id));

        self.operations.push((obj.id, op));

        Ok(())
    }

    fn finalize_op(
        &mut self,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
        prop: Prop,
        op: Op,
        marks: Option<Rc<RichText>>,
    ) {
        // TODO - id_to_exid should be a noop if not used - change type to Into<ExId>?
        if patch_log.is_active() {
            if op.insert {
                if !op.is_mark() {
                    assert!(obj.typ.is_sequence());
                    match (obj.typ, prop) {
                        (ObjType::List, Prop::Seq(index)) => {
                            //let value = (op.value(), doc.ops().id_to_exid(op.id));
                            patch_log.insert(obj, index, op.value().into(), op.id, false);
                        }
                        (ObjType::Text, Prop::Seq(index)) => {
                            if matches!(patch_log.text_rep(), TextRepresentation::Array) {
                                //let value = (op.value(), doc.ops().id_to_exid(op.id));
                                patch_log.insert(obj, index, op.value().into(), op.id, false);
                            } else {
                                patch_log.splice(obj, index, op.to_str(), marks);
                            }
                        }
                        _ => {}
                    }
                }
            } else if op.is_delete() {
                match prop {
                    Prop::Seq(index) => patch_log.delete_seq(obj, index, 1, op.block_id()),
                    Prop::Map(key) => patch_log.delete_map(obj, &key),
                }
            } else if let Some(value) = op.get_increment_value() {
                patch_log.increment(obj, &prop, value, op.id);
            } else {
                patch_log.put(obj, &prop, op.value().into(), op.id, false, false);
            }
        }
        self.operations.push((obj.id, op));
    }

    pub(crate) fn get_scope(&self) -> &Option<Clock> {
        &self.scope
    }

    pub(crate) fn get_deps(&self) -> Vec<ChangeHash> {
        self.deps.clone()
    }
}

enum SpliceType<'a> {
    List,
    Text(&'a str),
}

impl<'a> SpliceType<'a> {
    fn encoding(&self) -> ListEncoding {
        match self {
            SpliceType::List => ListEncoding::List,
            SpliceType::Text(_) => ListEncoding::Text,
        }
    }
}

struct SpliceArgs<'a> {
    obj: ObjMeta,
    index: usize,
    del: isize,
    values: Vec<ScalarValue>,
    splice_type: SpliceType<'a>,
}

#[cfg(test)]
mod tests {
    use crate::{transaction::Transactable, ReadDoc, ROOT};

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

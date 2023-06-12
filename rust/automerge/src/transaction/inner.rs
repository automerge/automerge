use std::num::NonZeroU64;
use std::sync::Arc;

use crate::block::Block;
use crate::exid::ExId;
use crate::marks::{ExpandMark, Mark, RichText};
use crate::op_set::{ChangeOpIter, OpIdx, OpIdxRange};
use crate::patches::{InsertArgs, PatchLog, PutArgs, PutSeqArgs, TextRepresentation};
use crate::query::{self, OpIdSearch};
use crate::storage::Change as StoredChange;
use crate::types::{Clock, Key, ListEncoding, ObjId, ObjMeta, OpId, OpIds};
use crate::{op_tree::OpSetData, types::OpBuilder, Automerge, Change, ChangeHash, Prop};
use crate::{AutomergeError, Cursor, ObjType, OpType, ScalarValue};

#[derive(Debug, Clone)]
pub(crate) struct TransactionInner {
    actor: usize,
    seq: u64,
    start_op: NonZeroU64,
    time: i64,
    message: Option<String>,
    deps: Vec<ChangeHash>,
    scope: Option<Clock>,
    idx_range: OpIdxRange,
}

/// Arguments required to create a new transaction
pub(crate) struct TransactionArgs {
    /// The index of the actor ID this transaction will create ops for in the
    /// [`OpSetData::actors`]
    pub(crate) actor_index: usize,
    /// The sequence number of the change this transaction will create
    pub(crate) seq: u64,
    /// The start op of the change this transaction will create
    pub(crate) start_op: NonZeroU64,
    /// The index of the first op in the opset
    pub(crate) idx_range: OpIdxRange,
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
            idx_range,
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
            idx_range,
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
        self.idx_range.len()
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
        let change = self.export(doc.osd());
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

    fn operations<'a>(&self, osd: &'a OpSetData) -> ChangeOpIter<'a> {
        osd.get_ops(self.idx_range)
    }

    #[tracing::instrument(skip(self, osd))]
    pub(crate) fn export(self, osd: &OpSetData) -> Change {
        use crate::storage::{change::PredOutOfOrder, convert::op_as_actor_id};

        let actor = osd.actors.get(self.actor).clone();
        let deps = self.deps.clone();
        let stored = match StoredChange::builder()
            .with_actor(actor)
            .with_seq(self.seq)
            .with_start_op(self.start_op)
            .with_message(self.message.clone())
            .with_dependencies(deps)
            .with_timestamp(self.time)
            .build(
                self.operations(osd)
                    .map(|op| op_as_actor_id(op.obj(), op, osd)),
            ) {
            Ok(s) => s,
            Err(PredOutOfOrder) => {
                // SAFETY: types::Op::preds is `types::OpIds` which ensures ops are always sorted
                panic!("preds out of order");
            }
        };
        #[cfg(debug_assertions)]
        {
            let realized_ops = self.operations(osd).collect::<Vec<_>>();
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
        let ops: Vec<_> = self
            .operations(doc.osd())
            .rev()
            .map(|op| {
                (
                    *op.obj(),
                    *op.id(),
                    op.action().clone(),
                    op.pred().cloned().collect::<Vec<_>>(),
                )
            })
            .collect();
        for (obj, opid, action, pred) in ops.into_iter() {
            for pred_id in &pred {
                if let Some(p) = doc
                    .ops()
                    .search(&obj, OpIdSearch::opid(*pred_id, encoding, None))
                    .found()
                {
                    doc.ops_mut()
                        .change_vis(&obj, p, |o| o.remove_succ(&opid, &action));
                }
            }
            if let Some(pos) = doc
                .ops()
                .search(&obj, OpIdSearch::opid(opid, encoding, None))
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
        let obj = doc.exid_to_obj(ex_obj)?;
        let value = value.into();
        let prop = prop.into();
        match (&prop, obj.typ) {
            (Prop::Map(_), ObjType::Map) => Ok(()),
            (Prop::Seq(_), ObjType::List) => Ok(()),
            (Prop::Seq(_), ObjType::Text) => Ok(()),
            _ => Err(AutomergeError::InvalidOp(obj.typ)),
        }?;
        self.local_op(doc, patch_log, obj.id, prop, value.into())?;
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
        let obj = doc.exid_to_obj(ex_obj)?;
        let prop = prop.into();
        match (&prop, obj.typ) {
            (Prop::Map(_), ObjType::Map) => Ok(()),
            (Prop::Seq(_), ObjType::List) => Ok(()),
            _ => Err(AutomergeError::InvalidOp(obj.typ)),
        }?;
        self.local_op(doc, patch_log, obj.id, prop, value.into())
            .map(|val| val.unwrap().as_op2(doc.osd()).exid())
    }

    fn next_id(&mut self) -> OpId {
        OpId::new(self.start_op.get() + self.pending_ops() as u64, self.actor)
    }

    fn next_insert(&mut self, key: Key, value: ScalarValue) -> OpBuilder {
        OpBuilder {
            id: self.next_id(),
            action: OpType::Put(value),
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        }
    }

    fn next_delete(&mut self, key: Key, pred: OpIds) -> OpBuilder {
        OpBuilder {
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
        idx: OpIdx,
        is_delete: bool,
        pos: usize,
        obj: ObjId,
        succ_pos: &[usize],
    ) {
        doc.ops_mut().add_succ(&obj, succ_pos, idx);

        if !is_delete {
            doc.ops_mut().insert(pos, &obj, idx);
        }

        self.finalize_op(doc, patch_log, obj, prop, idx, None);
    }

    pub(crate) fn insert<V: Into<ScalarValue>>(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        index: usize,
        value: V,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        if !matches!(obj.typ, ObjType::List | ObjType::Text) {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let value = value.into();
        tracing::trace!(obj=?obj, value=?value, "inserting value");
        self.do_insert(
            doc,
            patch_log,
            obj.id,
            index,
            ListEncoding::List,
            value.into(),
        )?;
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
        let obj = doc.exid_to_obj(ex_obj)?;
        if !matches!(obj.typ, ObjType::List | ObjType::Text) {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let idx = self.do_insert(
            doc,
            patch_log,
            obj.id,
            index,
            ListEncoding::List,
            value.into(),
        )?;
        Ok(idx.as_op2(doc.osd()).exid())
    }

    fn do_insert(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: ObjId,
        index: usize,
        encoding: ListEncoding,
        action: OpType,
    ) -> Result<OpIdx, AutomergeError> {
        let id = self.next_id();

        let query = doc.ops().search(
            &obj,
            query::InsertNth::new(index, encoding, self.scope.clone()),
        );
        let marks = query.marks(doc.osd());
        let pos = query.pos();
        let key = query.key()?;

        let op = OpBuilder {
            id,
            action,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        };

        let idx = doc
            .ops_mut()
            .load_with_range(obj, op.clone(), &mut self.idx_range);
        doc.ops_mut().insert(pos, &obj, idx);

        self.finalize_op(doc, patch_log, obj, Prop::Seq(index), idx, marks);

        Ok(idx)
    }

    pub(crate) fn local_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: ObjId,
        prop: Prop,
        action: OpType,
    ) -> Result<Option<OpIdx>, AutomergeError> {
        match prop {
            Prop::Map(s) => self.local_map_op(doc, patch_log, obj, s, action),
            Prop::Seq(n) => self.local_list_op(doc, patch_log, obj, n, action),
        }
    }

    fn local_map_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: ObjId,
        prop: String,
        action: OpType,
    ) -> Result<Option<OpIdx>, AutomergeError> {
        if prop.is_empty() {
            return Err(AutomergeError::EmptyStringKey);
        }

        let id = self.next_id();
        let prop_index = doc.ops_mut().osd.props.cache(prop.clone());
        let key = Key::Map(prop_index);
        let prop: Prop = prop.into();
        let query =
            doc.ops()
                .seek_ops_by_prop(&obj, prop.clone(), ListEncoding::List, self.scope.as_ref());
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

        let pred = doc
            .ops()
            .osd
            .sorted_opids(query.ops.iter().map(|o| *o.id()));

        let op = OpBuilder {
            id,
            action,
            key,
            succ: Default::default(),
            pred,
            insert: false,
        };
        let pos = query.end_pos;
        let ops_pos = query.ops_pos;

        let is_delete = op.is_delete();
        let idx = doc.ops_mut().load_with_range(obj, op, &mut self.idx_range);

        self.insert_local_op(doc, patch_log, prop, idx, is_delete, pos, obj, &ops_pos);

        Ok(Some(idx))
    }

    fn local_list_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: ObjId,
        index: usize,
        action: OpType,
    ) -> Result<Option<OpIdx>, AutomergeError> {
        let osd = doc.osd();
        let query = doc.ops().search(
            &obj,
            query::Nth::new(index, ListEncoding::List, self.scope.clone(), osd),
        );

        let id = self.next_id();

        let pred = query.pred();
        let key = query.key()?;

        if query.ops.len() == 1 && query.ops[0].is_noop(&action) {
            return Ok(None);
        }

        // increment operations are only valid against counter values.
        // if there are multiple values (from conflicts) then we just need one of them to be a counter.
        if matches!(action, OpType::Increment(_)) && query.ops.iter().all(|op| !op.is_counter()) {
            return Err(AutomergeError::MissingCounter);
        }

        let op = OpBuilder {
            id,
            action,
            key,
            succ: Default::default(),
            pred,
            insert: false,
        };
        let pos = query.pos();
        let ops_pos = query.ops_pos;
        let is_delete = op.is_delete();
        let idx = doc.ops_mut().load_with_range(obj, op, &mut self.idx_range);

        self.insert_local_op(
            doc,
            patch_log,
            Prop::Seq(index),
            idx,
            is_delete,
            pos,
            obj,
            &ops_pos,
        );

        Ok(Some(idx))
    }

    pub(crate) fn increment<P: Into<Prop>>(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ExId,
        prop: P,
        value: i64,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(obj)?;
        self.local_op(
            doc,
            patch_log,
            obj.id,
            prop.into(),
            OpType::Increment(value),
        )?;
        Ok(())
    }

    pub(crate) fn delete<P: Into<Prop>>(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        prop: P,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        let prop = prop.into();
        if obj.typ == ObjType::Text {
            let index = prop.as_index().ok_or(AutomergeError::InvalidOp(obj.typ))?;
            self.inner_splice(
                doc,
                patch_log,
                SpliceArgs {
                    obj: obj.id,
                    index,
                    del: 1,
                    values: vec![],
                    splice_type: SpliceType::Text(""),
                },
            )?;
        } else {
            self.local_op(doc, patch_log, obj.id, prop, OpType::Delete)?;
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
        let obj = doc.exid_to_obj(ex_obj)?;
        if !matches!(obj.typ, ObjType::List | ObjType::Text) {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let values = vals.into_iter().collect();
        self.inner_splice(
            doc,
            patch_log,
            SpliceArgs {
                obj: obj.id,
                index,
                del,
                values,
                splice_type: SpliceType::List,
            },
        )?;
        Ok(())
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
        let obj = doc.exid_to_obj(ex_obj)?;
        if obj.typ != ObjType::Text {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let values = text.chars().map(ScalarValue::from).collect();
        self.inner_splice(
            doc,
            patch_log,
            SpliceArgs {
                obj: obj.id,
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
                &obj,
                query::Nth::new(index, encoding, self.scope.clone(), doc.osd()),
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

            let query_key = query.key()?;
            let query_pred = query.pred();
            let ops_pos = query.ops_pos;
            let op = self.next_delete(query_key, query_pred);
            let idx = doc.ops_mut().load_with_range(obj, op, &mut self.idx_range);

            doc.ops_mut().add_succ(&obj, &ops_pos, idx);

            deleted += step;
        }

        if deleted > 0 && patch_log.is_active() {
            patch_log.delete_seq(obj, index, deleted, None);
        }

        // do the insert query for the first item and then
        // insert the remaining ops one after the other
        if !values.is_empty() {
            let query = doc.ops().search(
                &obj,
                query::InsertNth::new(index, encoding, self.scope.clone()),
            );
            let mut pos = query.pos();
            let mut key = query.key()?;
            let marks = query.marks(doc.osd());
            let mut cursor = index;
            let mut width = 0;

            for v in &values {
                let op = self.next_insert(key, v.clone());

                width = op.width(encoding);
                key = op.id.into();

                let idx = doc.ops_mut().load_with_range(obj, op, &mut self.idx_range);
                doc.ops_mut().insert(pos, &obj, idx);

                cursor += width;
                pos += 1;
            }

            doc.ops_mut()
                .hint(&obj, cursor - width, pos - 1, width, key);

            if patch_log.is_active() {
                match splice_type {
                    SpliceType::Text(text)
                        if matches!(patch_log.text_rep(), TextRepresentation::String) =>
                    {
                        patch_log.splice(obj, index, text, marks.clone());
                    }
                    SpliceType::List | SpliceType::Text(..) => {
                        let mut opid = self.next_id().minus(values.len());
                        for (offset, v) in values.iter().enumerate() {
                            opid = opid.next();
                            patch_log.insert(InsertArgs {
                                obj,
                                index: index + offset,
                                value: v.clone().into(),
                                id: opid,
                                conflict: false,
                                marks: marks.clone(),
                                // the values we are inserting are all scalar values
                                block_id: None,
                            });
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
        let obj = doc.exid_to_obj(ex_obj)?;
        let action = OpType::MarkBegin(expand.before(), mark.data.clone().into_owned());

        self.do_insert(doc, patch_log, obj.id, mark.start, obj.encoding, action)?;
        self.do_insert(
            doc,
            patch_log,
            obj.id,
            mark.end,
            obj.encoding,
            OpType::MarkEnd(expand.after()),
        )?;
        if patch_log.is_active() {
            patch_log.mark(obj.id, mark.start, mark.len(), &mark.into_mark_set());
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
        block: Block,
    ) -> Result<Cursor, AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        let obj_type = doc
            .ops()
            .object_type(&obj.id)
            .ok_or(AutomergeError::NotAnObject)?;
        if obj_type != ObjType::Text {
            return Err(AutomergeError::InvalidOp(obj_type));
        }

        let action = Some(&block).into();
        let id = self.next_id();

        let query = doc.ops().search(
            &obj.id,
            query::InsertNth::new(index, ListEncoding::List, self.scope.clone()),
        );
        let pos = query.pos();
        let key = query.key()?;

        let op = OpBuilder {
            id,
            action,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: true,
        };

        let idx = doc
            .ops_mut()
            .load_with_range(obj.id, op, &mut self.idx_range);
        doc.ops_mut().insert(pos, &obj.id, idx);

        let op = idx.as_op2(&doc.ops().osd);

        patch_log.insert(InsertArgs {
            obj: obj.id,
            index,
            value: block.into(),
            id: *op.id(),
            conflict: false,
            marks: None,
            block_id: Some(*op.id()),
        });

        Ok(Cursor::new(id, doc.osd()))
    }

    pub(crate) fn join_block(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        block_id: &Cursor,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        let block_id = doc.cursor_to_opid(block_id, None)?;
        let obj_type = doc
            .ops()
            .object_type(&obj.id)
            .ok_or(AutomergeError::NotAnObject)?;

        if obj_type != ObjType::Text {
            return Err(AutomergeError::InvalidOp(obj_type));
        }

        self.update_block_inner(doc, patch_log, obj, &block_id, None)?;

        Ok(())
    }

    fn update_block_inner(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: ObjMeta,
        block_id: &OpId,
        block: Option<Block>,
    ) -> Result<(), AutomergeError> {
        let key = Key::Seq((*block_id).into());

        let action = block.as_ref().into();

        let mut op_builder = OpBuilder {
            id: self.next_id(),
            action,
            key,
            succ: Default::default(),
            pred: Default::default(),
            insert: false,
        };

        let ops = doc.ops();
        let query = ops.search(
            &obj.id,
            query::OpIdSearch::opid(*block_id, obj.encoding, None),
        );
        let index = query.index();
        let mut pos = query.pos();
        let mut pred_ids = vec![];
        let mut succ_pos = vec![];
        {
            let mut iter = ops.iter_ops(&obj.id);
            let mut next = iter.nth(pos);
            while let Some(e) = next {
                if e.elemid_or_key() != op_builder.elemid_or_key() {
                    break;
                }
                let visible = e.visible_at(self.scope.as_ref());
                if visible {
                    succ_pos.push(pos);
                }
                pos += 1;
                pred_ids.push(e.id());
                next = iter.next();
            }
        }

        op_builder.pred = ops.osd.sorted_opids(pred_ids.into_iter().copied());

        let ops = doc.ops_mut();
        let op_idx = ops.load_with_range(obj.id, op_builder, &mut self.idx_range);

        ops.add_succ(&obj.id, &succ_pos, op_idx);
        let op = op_idx.as_op2(&ops.osd);

        if let Some(b) = block {
            patch_log.put_seq(PutSeqArgs {
                obj: obj.id,
                index,
                value: b.into(),
                id: *op.id(),
                conflict: false,
                expose: false,
                block_id: op.block_id(),
            });
        } else {
            patch_log.delete_seq(obj.id, index, 1, Some(*block_id))
        }

        Ok(())
    }

    pub(crate) fn update_block(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        block_id: &Cursor,
        block: Block,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        let block_id = doc.cursor_to_opid(block_id, None)?;
        let obj_type = doc
            .ops()
            .object_type(&obj.id)
            .ok_or(AutomergeError::NotAnObject)?;

        if obj_type != ObjType::Text {
            return Err(AutomergeError::InvalidOp(obj_type));
        }

        self.update_block_inner(doc, patch_log, obj, &block_id, Some(block))?;

        Ok(())
    }

    fn finalize_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: ObjId,
        prop: Prop,
        idx: OpIdx,
        marks: Option<Arc<RichText>>,
    ) {
        let op = idx.as_op2(doc.osd());
        // TODO - id_to_exid should be a noop if not used - change type to Into<ExId>?
        if patch_log.is_active() {
            //let ex_obj = doc.ops().id_to_exid(obj.0);
            if op.insert() {
                if !op.is_mark() {
                    let obj_type = doc.ops().object_type(&obj);
                    assert!(obj_type.unwrap().is_sequence());
                    match (obj_type, prop) {
                        (Some(ObjType::List), Prop::Seq(index)) => {
                            //let value = (op.value(), doc.ops().id_to_exid(op.id));
                            patch_log.insert(InsertArgs {
                                obj,
                                index,
                                value: op.value().into(),
                                id: *op.id(),
                                conflict: false,
                                marks,
                                block_id: op.block_id(),
                            });
                        }
                        (Some(ObjType::Text), Prop::Seq(index)) => {
                            if matches!(patch_log.text_rep(), TextRepresentation::Array) {
                                //let value = (op.value(), doc.ops().id_to_exid(op.id));
                                patch_log.insert(InsertArgs {
                                    obj,
                                    index,
                                    value: op.value().into(),
                                    id: *op.id(),
                                    conflict: false,
                                    marks,
                                    block_id: op.block_id(),
                                });
                            } else {
                                patch_log.splice(obj, index, op.as_str(), marks);
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
                patch_log.increment(obj, &prop, value, *op.id());
            } else {
                patch_log.put(PutArgs {
                    obj,
                    prop: &prop,
                    value: op.value().into(),
                    id: *op.id(),
                    conflict: false,
                    expose: false,
                    block_id: op.block_id(),
                });
            }
        }
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
    obj: ObjId,
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

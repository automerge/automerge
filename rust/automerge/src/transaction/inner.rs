use std::collections::HashSet;
use std::num::NonZeroU64;
use std::sync::Arc;

use crate::exid::ExId;
use crate::iter::{ListRangeItem, MapRangeItem};
use crate::marks::{ExpandMark, Mark, MarkSet};
use crate::op_set::{ChangeOpIter, OpIdx, OpIdxRange};
use crate::patches::{PatchLog, TextRepresentation};
use crate::query::{self, OpIdSearch};
use crate::storage::Change as StoredChange;
use crate::types::{Clock, Key, ListEncoding, ObjMeta, OpId};
use crate::{op_tree::OpSetData, types::OpBuilder, Automerge, Change, ChangeHash, Prop};
use crate::{AutomergeError, ObjType, OpType, ReadDoc, ScalarValue};

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
            .build(self.operations(osd).map(op_as_actor_id))
        {
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
                    op.idx(),
                    *op.obj(),
                    *op.id(),
                    op.pred().map(|op| *op.id()).collect::<Vec<_>>(),
                )
            })
            .collect();
        for (idx, obj, opid, pred) in ops.into_iter() {
            for pred_id in &pred {
                if let Some(p) = doc
                    .ops()
                    .search(&obj, OpIdSearch::opid(*pred_id, encoding, None))
                    .found()
                {
                    doc.ops_mut().remove_succ(&obj, p, idx);
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
        let obj = doc.exid_to_obj(ex_obj)?;
        let prop = prop.into();
        match (&prop, obj.typ) {
            (Prop::Map(_), ObjType::Map) => Ok(()),
            (Prop::Seq(_), ObjType::List) => Ok(()),
            _ => Err(AutomergeError::InvalidOp(obj.typ)),
        }?;
        self.local_op(doc, patch_log, &obj, prop, value.into())
            .map(|val| val.unwrap().as_op(doc.osd()).exid())
    }

    fn next_id(&mut self) -> OpId {
        OpId::new(self.start_op.get() + self.pending_ops() as u64, self.actor)
    }

    fn next_insert(&mut self, key: Key, value: ScalarValue) -> OpBuilder {
        OpBuilder {
            id: self.next_id(),
            action: OpType::Put(value),
            key,
            insert: true,
        }
    }

    fn next_delete(&mut self, key: Key) -> OpBuilder {
        OpBuilder {
            id: self.next_id(),
            action: OpType::Delete,
            key,
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
        obj: &ObjMeta,
        succ_pos: &[usize],
    ) {
        doc.ops_mut().add_succ(&obj.id, succ_pos, idx);

        if !is_delete {
            doc.ops_mut().insert(pos, &obj.id, idx);
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
        let obj = doc.exid_to_obj(ex_obj)?;
        if !matches!(obj.typ, ObjType::List | ObjType::Text) {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let idx = self.do_insert(doc, patch_log, &obj, index, value.into())?;
        Ok(idx.as_op(doc.osd()).exid())
    }

    fn do_insert(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
        index: usize,
        action: OpType,
    ) -> Result<OpIdx, AutomergeError> {
        let id = self.next_id();

        let query = doc.ops().search(
            &obj.id,
            query::InsertNth::new(
                index,
                patch_log.text_rep().encoding(obj.typ),
                self.scope.clone(),
            ),
        );
        let marks = query.marks(doc.osd());
        let pos = query.pos();
        let key = query.key()?;

        let op = OpBuilder {
            id,
            action,
            key,
            insert: true,
        };

        let idx = doc
            .ops_mut()
            .load_with_range(obj.id, op.clone(), &mut self.idx_range);
        doc.ops_mut().insert(pos, &obj.id, idx);

        self.finalize_op(doc, patch_log, obj, Prop::Seq(index), idx, marks);

        Ok(idx)
    }

    pub(crate) fn local_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
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
        obj: &ObjMeta,
        prop: String,
        action: OpType,
    ) -> Result<Option<OpIdx>, AutomergeError> {
        let id = self.next_id();
        let prop_index = doc.ops_mut().osd.props.cache(prop.clone());
        let key = Key::Map(prop_index);
        let prop: Prop = prop.into();
        let query = doc.ops().seek_ops_by_prop(
            &obj.id,
            prop.clone(),
            patch_log.text_rep().encoding(obj.typ),
            self.scope.as_ref(),
        );
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

        let op = OpBuilder {
            id,
            action,
            key,
            insert: false,
        };
        let pos = query.end_pos;
        let ops_pos = query.ops_pos;

        let is_delete = op.is_delete();
        let idx = doc
            .ops_mut()
            .load_with_range(obj.id, op, &mut self.idx_range);

        self.insert_local_op(doc, patch_log, prop, idx, is_delete, pos, obj, &ops_pos);

        Ok(Some(idx))
    }

    fn local_list_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
        index: usize,
        action: OpType,
    ) -> Result<Option<OpIdx>, AutomergeError> {
        let osd = doc.osd();
        let query = doc.ops().search(
            &obj.id,
            query::Nth::new(index, ListEncoding::List, self.scope.clone(), osd),
        );

        let id = self.next_id();

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
            insert: false,
        };
        let pos = query.pos();
        let ops_pos = query.ops_pos;
        let is_delete = op.is_delete();
        let idx = doc
            .ops_mut()
            .load_with_range(obj.id, op, &mut self.idx_range);

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
        let obj = doc.exid_to_obj(ex_obj)?;
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
        let obj = doc.exid_to_obj(ex_obj)?;
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
            let ops_pos = query.ops_pos;
            let op = self.next_delete(query_key);
            let idx = doc
                .ops_mut()
                .load_with_range(obj.id, op, &mut self.idx_range);

            doc.ops_mut().add_succ(&obj.id, &ops_pos, idx);

            deleted += step;
        }

        if deleted > 0 && patch_log.is_active() {
            patch_log.delete_seq(obj.id, index, deleted);
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
            let marks = query.marks(doc.osd());
            let mut cursor = index;
            let mut width = 0;

            for v in &values {
                let op = self.next_insert(key, v.clone());

                key = op.id.into();

                let idx = doc
                    .ops_mut()
                    .load_with_range(obj.id, op, &mut self.idx_range);
                doc.ops_mut().insert(pos, &obj.id, idx);

                width = idx.as_op(doc.osd()).width(encoding);
                cursor += width;
                pos += 1;
            }

            doc.ops_mut()
                .hint(&obj.id, cursor - width, pos - 1, width, key, marks.clone());

            if patch_log.is_active() {
                match splice_type {
                    SpliceType::Text(text)
                        if matches!(patch_log.text_rep(), TextRepresentation::String) =>
                    {
                        patch_log.splice(obj.id, index, text, marks);
                    }
                    SpliceType::List | SpliceType::Text(..) => {
                        let mut opid = self.next_id().minus(values.len());
                        for (offset, v) in values.iter().enumerate() {
                            opid = opid.next();
                            patch_log.insert(obj.id, index + offset, v.clone().into(), opid, false);
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
        if mark.start == mark.end && expand == ExpandMark::None {
            // In peritext terms this is the same as a mark which has a begin anchor before one
            // character and an end anchor after the character preceding that character. E.g in the
            // following sequence where the "<",">" symbols represent the mark anchor points:
            //
            // |   |  |   |  |   |
            // < a >  < b >  < c >
            // |   |  |   |  |   |
            //
            // A mark from 1 to 1 with expand set to none would begin at the anchor point before
            // "b" and end at the anchor point after "a". This is nonsensical so we ignore it.
            return Ok(());
        }
        let obj = doc.exid_to_obj(ex_obj)?;
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
    ) -> Result<ExId, AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        if obj.typ != ObjType::Text {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }

        let action = OpType::Make(ObjType::Map);
        let id = self.next_id();

        let query = doc.ops().search(
            &obj.id,
            query::InsertNth::new(
                index,
                patch_log.text_rep().encoding(obj.typ),
                self.scope.clone(),
            ),
        );
        let pos = query.pos();
        let key = query.key()?;

        let op = OpBuilder {
            id,
            action,
            key,
            insert: true,
        };

        let op_idx = doc
            .ops_mut()
            .load_with_range(obj.id, op, &mut self.idx_range);
        doc.ops_mut().insert(pos, &obj.id, op_idx);
        let op = op_idx.as_op(doc.osd());

        patch_log.insert(
            obj.id,
            index,
            crate::hydrate::Value::Map(crate::hydrate::Map::default()),
            *op.id(),
            false,
        );

        Ok(op.exid())
    }

    pub(crate) fn join_block(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        text: &ExId,
        index: usize,
    ) -> Result<(), AutomergeError> {
        let text_obj = doc.exid_to_obj(text)?;

        if text_obj.typ != ObjType::Text {
            return Err(AutomergeError::InvalidOp(text_obj.typ));
        }

        let target = doc
            .ops()
            .seek_ops_by_prop(
                &text_obj.id,
                Prop::Seq(index),
                patch_log.text_rep().encoding(text_obj.typ),
                self.scope.as_ref(),
            )
            .ops
            .into_iter()
            .last()
            .ok_or(AutomergeError::InvalidIndex(index))?;
        let block_id = *target.id();

        let key = Key::Seq(block_id.into());

        let action = OpType::Delete;

        let op = OpBuilder {
            id: self.next_id(),
            action,
            key,
            insert: false,
        };

        let query = doc.ops().search(
            &text_obj.id,
            query::OpIdSearch::opid(block_id, patch_log.text_rep().encoding(text_obj.typ), None),
        );
        let index = query.index();
        let mut pos = query.pos();
        let mut pred_ids = vec![];
        let mut succ_pos = vec![];
        {
            let mut iter = doc.ops().iter_ops(&text_obj.id);
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
                pred_ids.push(e.id());
                next = iter.next();
            }
        }

        let op_idx = doc
            .ops_mut()
            .load_with_range(text_obj.id, op, &mut self.idx_range);

        doc.ops_mut().add_succ(&text_obj.id, &succ_pos, op_idx);

        patch_log.delete_seq(text_obj.id, index, 1);

        Ok(())
    }

    pub(crate) fn replace_block(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        text: &ExId,
        index: usize,
    ) -> Result<ExId, AutomergeError> {
        self.join_block(doc, patch_log, text, index)?;
        self.split_block(doc, patch_log, text, index)
    }

    fn finalize_op(
        &mut self,
        doc: &Automerge,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
        prop: Prop,
        idx: OpIdx,
        marks: Option<Arc<MarkSet>>,
    ) {
        let op = idx.as_op(doc.osd());
        // TODO - id_to_exid should be a noop if not used - change type to Into<ExId>?
        if patch_log.is_active() {
            //let ex_obj = doc.ops().id_to_exid(obj.0);
            if op.insert() {
                if !op.is_mark() {
                    assert!(obj.typ.is_sequence());
                    match (obj.typ, prop) {
                        (ObjType::List, Prop::Seq(index)) => {
                            //let value = (op.value(), doc.ops().id_to_exid(op.id));
                            patch_log.insert(obj.id, index, op.value().into(), *op.id(), false);
                        }
                        (ObjType::Text, Prop::Seq(index)) => {
                            if matches!(patch_log.text_rep(), TextRepresentation::Array) {
                                //let value = (op.value(), doc.ops().id_to_exid(op.id));
                                patch_log.insert(obj.id, index, op.value().into(), *op.id(), false);
                            } else {
                                patch_log.splice(obj.id, index, op.as_str(), marks);
                            }
                        }
                        _ => {}
                    }
                }
            } else if op.is_delete() {
                match prop {
                    Prop::Seq(index) => patch_log.delete_seq(obj.id, index, 1),
                    Prop::Map(key) => patch_log.delete_map(obj.id, &key),
                }
            } else if let Some(value) = op.get_increment_value() {
                patch_log.increment(obj.id, &prop, value, *op.id());
            } else {
                patch_log.put(obj.id, &prop, op.value().into(), *op.id(), false, false);
            }
        }
    }

    pub(crate) fn update_object(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ExId,
        new_value: &crate::hydrate::Value,
    ) -> Result<(), crate::error::UpdateObjectError> {
        let obj_meta = doc.exid_to_obj(obj)?;
        match (obj_meta.typ, new_value) {
            (ObjType::Map, crate::hydrate::Value::Map(map)) => {
                Ok(self.update_map(doc, patch_log, obj, map)?)
            }
            (ObjType::List, crate::hydrate::Value::List(list)) => {
                Ok(self.update_list(doc, patch_log, obj, list)?)
            }
            (ObjType::Text, crate::hydrate::Value::Text(new_text)) => {
                Ok(crate::text_diff::myers_diff(
                    doc,
                    self,
                    patch_log,
                    obj,
                    new_text.to_string().as_str(),
                )?)
            }
            _ => Err(crate::error::UpdateObjectError::ChangeType),
        }
    }

    pub(crate) fn update_map(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        map: &crate::ObjId,
        new_value: &crate::hydrate::Map,
    ) -> Result<(), AutomergeError> {
        let mut delenda = HashSet::new();
        let obj = doc.exid_to_obj(map)?;
        let current_vals = doc
            .ops()
            .map_range(&obj.id, .., self.scope.clone())
            .map(|MapRangeItem { key, value, id, .. }| (key.to_string(), value.into_owned(), id))
            .collect::<Vec<_>>();

        let mut present_keys = HashSet::new();
        for (key, value, id) in current_vals {
            present_keys.insert(key.clone());
            match new_value.get(&key) {
                Some(new_value) => self.update_value(
                    doc,
                    patch_log,
                    map,
                    key.into(),
                    new_value,
                    Some((id, value)),
                )?,
                None => {
                    delenda.insert(key.clone());
                }
            }
        }
        for (key, new_value) in new_value.iter() {
            if !present_keys.contains(key) {
                self.update_value(doc, patch_log, map, key.into(), &new_value.value, None)?;
            }
        }
        for key in delenda {
            self.delete(doc, patch_log, map, key)?;
        }
        Ok(())
    }

    pub(crate) fn update_list(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        list: &crate::ObjId,
        new_value: &crate::hydrate::List,
    ) -> Result<(), AutomergeError> {
        let old_items = doc
            .list_range(list, ..)
            .map(|ListRangeItem { value, id, .. }| Some((value.into_owned(), id)))
            .collect::<Vec<_>>()
            .into_iter()
            .chain(std::iter::repeat_with(|| None));
        let new_values = new_value
            .iter()
            .map(Some)
            .chain(std::iter::repeat_with(|| None));

        let mut to_delete = 0;
        for (index, (old, new)) in std::iter::zip(old_items, new_values).enumerate() {
            match (old, new) {
                (Some((value, id)), Some(new_value)) => {
                    self.update_value(
                        doc,
                        patch_log,
                        list,
                        Prop::Seq(index),
                        &new_value.value,
                        Some((id, value)),
                    )?;
                }
                (Some(_), None) => {
                    to_delete += 1;
                }
                (None, Some(new_value)) => {
                    self.update_value(
                        doc,
                        patch_log,
                        list,
                        Prop::Seq(index),
                        &new_value.value,
                        None,
                    )?;
                }
                (None, None) => {
                    break;
                }
            }
        }
        for i in (0..to_delete).rev() {
            self.delete(doc, patch_log, list, Prop::Seq(i))?;
        }
        Ok(())
    }

    fn update_value(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        parent: &crate::ObjId,
        key: Prop,
        new_value: &crate::hydrate::Value,
        old_value: Option<(ExId, crate::Value<'_>)>,
    ) -> Result<(), AutomergeError> {
        match (old_value, new_value) {
            (Some((id, crate::Value::Object(ObjType::Map))), crate::hydrate::Value::Map(new)) => {
                self.update_map(doc, patch_log, &id, new)
            }
            (Some((id, crate::Value::Object(ObjType::List))), crate::hydrate::Value::List(new)) => {
                self.update_list(doc, patch_log, &id, new)
            }
            (Some((id, crate::Value::Object(ObjType::Text))), crate::hydrate::Value::Text(new)) => {
                crate::text_diff::myers_diff(doc, self, patch_log, &id, new.to_string().as_str())
            }
            (old, new) => {
                // Here we are either changing the type of the existing object, or inserting an
                // entirely new object
                let mut make_obj = |typ: ObjType| match (&old, &key) {
                    (None, Prop::Seq(index)) => {
                        self.insert_object(doc, patch_log, parent, *index, typ)
                    }
                    _ => self.put_object(doc, patch_log, parent, key.clone(), typ),
                };
                match new {
                    crate::hydrate::Value::Map(new) => {
                        let map_id = make_obj(ObjType::Map)?;
                        self.update_map(doc, patch_log, &map_id, new)
                    }

                    crate::hydrate::Value::List(new) => {
                        let list_id = make_obj(ObjType::List)?;
                        self.update_list(doc, patch_log, &list_id, new)
                    }

                    crate::hydrate::Value::Text(new) => {
                        let text_id = make_obj(ObjType::Text)?;
                        self.splice_text(doc, patch_log, &text_id, 0, 0, new.to_string().as_str())
                    }

                    crate::hydrate::Value::Scalar(val) => match (old, &key) {
                        (None, Prop::Seq(index)) => {
                            self.insert(doc, patch_log, parent, *index, val.clone())
                        }
                        _ => self.put(doc, patch_log, parent, key.clone(), val.clone()),
                    },
                }
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

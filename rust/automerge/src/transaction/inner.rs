use std::borrow::Cow;
use std::collections::HashSet;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::Arc;

use crate::change_graph::ChangeGraph;
use unicode_segmentation::UnicodeSegmentation;

use crate::exid::ExId;
use crate::marks::{ExpandMark, Mark, MarkSet};
use crate::op_set2::change::build_change;
use crate::op_set2::{Op, OpSet, OpSetCheckpoint, PropRef, SuccInsert, TxOp};
use crate::patches::PatchLog;
use crate::types::{Clock, ElemId, ObjMeta, OpId, ScalarValue, SequenceType, TextEncoding};
use crate::Automerge;
use crate::{AutomergeError, ObjType, OpType, ReadDoc};
use crate::{Change, ChangeHash, Prop};

#[derive(Debug, Clone)]
pub(crate) struct TransactionInner {
    actor: usize,
    seq: u64,
    start_op: NonZeroU64,
    time: i64,
    message: Option<String>,
    deps: Vec<ChangeHash>,
    scope: Option<Clock>,
    checkpoint: OpSetCheckpoint,
    pending: Vec<TxOp>,
}

/// Arguments required to create a new transaction
pub(crate) struct TransactionArgs {
    /// The index of the actor ID this transaction will create ops for in the
    /// [`OpSet::actors`]
    pub(crate) actor_index: usize,
    /// The sequence number of the change this transaction will create
    pub(crate) seq: u64,
    /// checkpoint of the op_set state needed for rollback
    pub(crate) checkpoint: OpSetCheckpoint,
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
            checkpoint,
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
            checkpoint,
            deps,
            pending: vec![],
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
        self.pending.len()
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
            if self.seq == 1 {
                // we added an actor for this tx - now roll it back
                doc.remove_actor(self.actor);
            }
            doc.remove_unused_actors(true);
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

        let change = self.export(doc.ops(), doc.changes());
        let hash = change.hash();
        #[cfg(not(debug_assertions))]
        tracing::trace!(commit=?hash, deps=?change.deps(), "committing transaction");
        #[cfg(debug_assertions)]
        {
            let ops = change.iter_ops().collect::<Vec<_>>();
            tracing::trace!(commit=?hash, ?ops, deps=?change.deps(), "committing transaction");
        }
        doc.update_history(&change);
        doc.remove_unused_actors(true);
        hash
    }

    pub(crate) fn change_meta<'a>(
        &self,
        deps: Vec<u64>,
    ) -> crate::op_set2::change::BuildChangeMetadata<'a> {
        crate::op_set2::change::BuildChangeMetadata {
            actor: self.actor,
            seq: self.seq,
            start_op: self.start_op.get(),
            max_op: self.start_op.get() + self.pending.len() as u64 - 1,
            timestamp: self.time,
            message: self.message.as_ref().map(|s| Cow::Owned(s.to_string())),
            extra: Cow::Borrowed(&[]),
            builder: 0,
            deps,
        }
    }

    pub(crate) fn export(mut self, op_set: &OpSet, change_graph: &ChangeGraph) -> Change {
        self.deps.sort_unstable();
        let deps_index = self
            .deps
            .iter()
            .filter_map(|hash| Some(change_graph.hash_to_index(hash)? as u64))
            .collect();
        let meta = self.change_meta(deps_index);
        let stored = build_change(&self.pending, &meta, change_graph, &op_set.actors);
        Change::new(stored)
    }

    /// Undo the operations added in this transaction, returning the number of cancelled
    /// operations.
    pub(crate) fn rollback(self, doc: &mut Automerge) -> usize {
        let num = self.pending.len();
        doc.ops_mut().load_checkpoint(self.checkpoint);
        if self.seq == 1 {
            doc.remove_actor(self.actor);
        }
        doc.remove_unused_actors(true);
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
            .map(|opid| doc.id_to_exid(opid.unwrap()))
    }

    fn next_id(&mut self) -> OpId {
        OpId::new(self.start_op.get() + self.pending_ops() as u64, self.actor)
    }

    fn next_delete(&mut self, obj: ObjMeta, index: usize, elemid: ElemId, ops: &[Op<'_>]) -> TxOp {
        TxOp::list_del(
            self.next_id(),
            obj,
            index,
            elemid,
            ops.iter().map(|op| op.id),
        )
    }

    fn insert_local_op(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        op: TxOp,
        succ: &[SuccInsert],
        range: Range<usize>,
    ) {
        let added = doc.ops_mut().splice(op.pos, &[&op]);

        doc.ops_mut().add_succ(succ);

        if self.scope.is_some() {
            doc.ops_mut().reset_top(range.start..(range.end + added));
        }

        self.finalize_op(doc.text_encoding(), patch_log, &op, None);

        self.pending.push(op);
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
        let Some(seq_type) = obj.typ.as_sequence_type() else {
            return Err(AutomergeError::InvalidOp(obj.typ));
        };
        let value = value.into();
        tracing::trace!(obj=?obj, value=?value, "inserting value");
        self.do_insert(doc, patch_log, &obj, seq_type, index, value.into())?;
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
        let Some(seq_type) = obj.typ.as_sequence_type() else {
            return Err(AutomergeError::InvalidOp(obj.typ));
        };
        let id = self.do_insert(doc, patch_log, &obj, seq_type, index, value.into())?;
        Ok(doc.ops().id_to_exid(id))
    }

    fn do_insert(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        obj: &ObjMeta,
        seq_type: SequenceType,
        index: usize,
        action: OpType,
    ) -> Result<OpId, AutomergeError> {
        let id = self.next_id();

        let query = doc
            .ops()
            .query_insert_at(&obj.id, index, seq_type, self.scope.clone())?;

        let marks = query.marks;
        let pos = query.pos;

        //let key = query.elemid.into();

        let op = TxOp::insert(id, *obj, pos, index, action, query.elemid);

        doc.ops_mut().splice(op.pos, &[&op]);
        self.finalize_op(doc.text_encoding(), patch_log, &op, marks);
        self.pending.push(op);

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
        let id = self.next_id();

        let mut query = doc
            .ops()
            .seek_ops_by_map_key(&obj.id, &prop, self.scope.as_ref());

        let Some(resolved_action) = query.resolve_action(action) else {
            return Ok(None);
        };

        // increment operations are only valid against counter values.
        // if there are multiple values (from conflicts) then we just need one of them to be a counter.
        if resolved_action.is_increment() && query.ops.iter().all(|op| !op.is_counter()) {
            return Err(AutomergeError::MissingCounter);
        }

        let pred = query.ops.iter().map(|op| op.id).collect();
        let op = TxOp::map(id, *obj, query.end_pos, resolved_action, prop, pred);

        let inc_value = op.get_increment_value();

        let succ: Vec<_> = query
            .ops
            .iter()
            .map(|op| op.add_succ(id, inc_value))
            .collect();

        self.insert_local_op(doc, patch_log, op, &succ, query.range);

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
        let Some(seq_type) = obj.typ.as_sequence_type() else {
            return Err(AutomergeError::InvalidOp(obj.typ));
        };
        let mut query = doc
            .ops()
            .seek_ops_by_index(&obj.id, index, seq_type, self.scope.as_ref());
        let id = self.next_id();
        let eid = query
            .ops
            .first()
            .and_then(|op| op.cursor().ok())
            .ok_or(AutomergeError::InvalidIndex(index))?;

        let Some(resolved_action) = query.resolve_action(action) else {
            return Ok(None);
        };

        // increment operations are only valid against counter values.
        // if there are multiple values (from conflicts) then we just need one of them to be a counter.

        if resolved_action.is_increment() && query.ops.iter().all(|op| !op.is_counter()) {
            return Err(AutomergeError::MissingCounter);
        }

        let pred = query.ops.iter().map(|op| op.id).collect();
        let op = TxOp::list(id, *obj, query.end_pos, index, resolved_action, eid, pred);
        let inc_value = op.get_increment_value();
        let succ = query
            .ops
            .iter()
            .map(|op| op.add_succ(id, inc_value))
            .collect::<Vec<_>>();

        self.insert_local_op(doc, patch_log, op, &succ, query.range);

        // inserts can delete a conflicted value reveal a counter
        if let Some((i, s)) = succ.iter().rev().enumerate().find(|(_, s)| s.inc.is_some()) {
            if i > 0 {
                doc.ops.expose(s.pos)
            }
        }

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
        let values = match doc.text_encoding() {
            // Arguably we should do this for all text, rather than just the grapheme cluster encoding.
            // However, at the time which I (Alex Good) am writing this code the existing implementation
            // uses the unicode code points and the grapheme cluster text encoding is a new thing. I
            // don't want to change the existing behaviour for the existing text encodings without a
            // little more thought.
            TextEncoding::GraphemeCluster => text.graphemes(true).map(ScalarValue::from).collect(),
            _ => text.chars().map(ScalarValue::from).collect(),
        };
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

        let seq_type = splice_type.seq_type();

        let mut inserted_width = 0;

        // do the insert query for the first item and then
        // insert the remaining ops one after the other
        if !values.is_empty() {
            let query = doc
                .ops()
                .query_insert_at(&obj.id, index, seq_type, self.scope.clone())?;

            index = query.index;

            let mut pos = query.pos;
            let mut elemid = query.elemid;
            let marks = query.marks;

            let start = self.pending.len();
            let start_pos = pos;

            for v in &values {
                let op = TxOp::insert_val(self.next_id(), obj, pos, v.clone(), elemid);

                inserted_width += op.bld.width(seq_type, doc.text_encoding());

                elemid = ElemId(op.id());

                self.pending.push(op);
                pos += 1;
            }

            doc.ops_mut().splice(start_pos, &self.pending[start..]);

            if patch_log.is_active() {
                match splice_type {
                    SpliceType::Text(text) => {
                        patch_log.splice(obj.id, index, text, marks);
                    }
                    SpliceType::List => {
                        let mut opid = self.next_id().minus(values.len());
                        for (offset, v) in values.iter().enumerate() {
                            opid = opid.next();
                            let hydrated =
                                crate::hydrate::Value::new(v.clone(), doc.text_encoding());
                            patch_log.insert(obj.id, index + offset, hydrated, opid, false);
                        }
                    }
                }
            }
        }

        // delete `del` items - performing the query for each one
        let mut delete_index = index + inserted_width;
        let mut deleted: usize = 0;
        while deleted < (del as usize) {
            // TODO: could do this with a single custom query

            let query =
                doc.ops()
                    .seek_ops_by_index(&obj.id, delete_index, seq_type, self.scope.as_ref());

            let step = if let Some(op) = query.ops.last() {
                op.width(seq_type, doc.text_encoding())
            } else {
                break;
            };

            // if we delete in the middle of a multi-character
            // move cursor to the next character
            if query.index < delete_index {
                delete_index = query.index + step;
                continue;
            }

            let query_elemid = query.elemid().ok_or(AutomergeError::InvalidIndex(index))?;
            let op = self.next_delete(obj, delete_index, query_elemid, &query.ops);
            let ops_pos = query
                .ops
                .iter()
                .map(|o| o.add_succ(op.id(), None))
                .collect::<Vec<_>>();

            doc.ops_mut().add_succ(&ops_pos);

            deleted += step;

            self.pending.push(op);
        }

        if deleted > 0 && patch_log.is_active() {
            patch_log.delete_seq(obj.id, delete_index, deleted);
        }

        Ok(())
    }

    pub(crate) fn mark(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        mark: Mark,
        expand: ExpandMark,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        if ObjType::Text != obj.typ {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
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
        let action = OpType::MarkBegin(expand.before(), mark.old_data());

        self.do_insert(doc, patch_log, &obj, SequenceType::Text, mark.start, action)?;
        self.do_insert(
            doc,
            patch_log,
            &obj,
            SequenceType::Text,
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

        let query =
            doc.ops()
                .query_insert_at(&obj.id, index, SequenceType::Text, self.scope.clone())?;

        let pos = query.pos;

        let id = self.next_id();

        let op = TxOp::insert_obj(id, obj, pos, index, ObjType::Map, query.elemid);

        doc.ops_mut().splice(op.pos, &[&op]);

        patch_log.insert(
            obj.id,
            index,
            crate::hydrate::Value::Map(crate::hydrate::Map::default()),
            id,
            false,
        );

        self.pending.push(op);

        Ok(doc.ops().id_to_exid(id))
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

        // FIXME - how is this different than a normal delete?
        // 1. can only happen on a text
        // 2. it doesn't seem to validate that what its deleting is a block??
        // --> self.local_op(doc, patch_log, &obj, Prop::Seq(index), OpType::Delete)?;

        let target = doc
            .ops()
            .seek_ops_by_index(&text_obj.id, index, SequenceType::Text, self.scope.as_ref())
            .ops
            .into_iter()
            .next_back()
            .ok_or(AutomergeError::InvalidIndex(index))?;
        let block_id = target.id;

        let elemid = target.elemid_or_key().elemid().unwrap();

        // FIXME - no clock?
        let found = doc
            .ops()
            .seek_list_opid(
                &text_obj.id,
                block_id,
                SequenceType::Text,
                self.scope.as_ref(),
            )
            .unwrap();

        let op = TxOp::list_del(self.next_id(), text_obj, index, elemid, [found.op.id]);

        let succ_pos = vec![found.op.add_succ(op.id(), None)];

        doc.ops_mut().add_succ(&succ_pos);

        patch_log.delete_seq(text_obj.id, index, 1);

        self.pending.push(op);

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
        encoding: TextEncoding,
        patch_log: &mut PatchLog,
        op: &TxOp,
        marks: Option<Arc<MarkSet>>,
    ) {
        let obj_typ = op.obj_type;
        let obj = op.bld.obj;
        if patch_log.is_active() && !op.noop {
            if op.bld.insert {
                if !op.is_mark() {
                    assert!(obj_typ.is_sequence());
                    match (obj_typ, op.prop()) {
                        (ObjType::List, PropRef::Seq(index)) => {
                            patch_log.insert(
                                obj,
                                index,
                                op.hydrate_value(encoding),
                                op.id(),
                                false,
                            );
                        }
                        (ObjType::Text, PropRef::Seq(index)) => {
                            patch_log.splice(obj, index, op.as_str(), marks);
                        }
                        _ => {}
                    }
                }
            } else if op.is_delete() {
                match op.prop() {
                    PropRef::Seq(index) => patch_log.delete_seq(obj, index, 1),
                    PropRef::Map(key) => patch_log.delete_map(obj, &key),
                }
            } else if let Some(value) = op.get_increment_value() {
                patch_log.increment(obj, op.prop(), value, op.id());
            } else {
                patch_log.put(
                    obj,
                    op.prop(),
                    op.hydrate_value(encoding),
                    op.id(),
                    false,
                    false,
                );
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
            .map(|m| (m.key.to_string(), m.value.to_value(), m.id()))
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
            .map(|item| Some((item.value.to_value(), item.id())))
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

impl SpliceType<'_> {
    fn seq_type(&self) -> SequenceType {
        match self {
            SpliceType::List => SequenceType::List,
            SpliceType::Text(_) => SequenceType::Text,
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

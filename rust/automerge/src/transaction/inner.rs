use std::borrow::Cow;
use std::collections::{HashSet, VecDeque};
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::Arc;

use crate::change_graph::ChangeGraph;
use crate::op_set2::op_set::ResolvedAction;
use unicode_segmentation::UnicodeSegmentation;

use crate::exid::ExId;
use crate::marks::{ExpandMark, Mark, MarkSet};
use crate::op_set2::change::build_change;
use crate::op_set2::{Op, OpSet, OpSetCheckpoint, PropRef, SuccInsert, TxOp};
use crate::patches::PatchLog;
use crate::types::{Clock, ElemId, ObjMeta, OpId, ScalarValue, SequenceType, TextEncoding, HEAD};
use crate::Automerge;
use crate::{hydrate, AutomergeError, ObjType, OpType, ReadDoc};
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
        #[cfg(not(feature = "slow_path_assertions"))]
        tracing::trace!(commit=?hash, deps=?change.deps(), "committing transaction");
        #[cfg(feature = "slow_path_assertions")]
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

    pub(super) fn next_id(&self) -> OpId {
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
                    splice_type: SpliceType::Text(""),
                },
            )?;
        } else {
            self.local_op(doc, patch_log, &obj, prop, OpType::Delete)?;
        }
        Ok(())
    }

    /// Splice new elements into the given sequence. Returns a vector of the OpIds used to insert
    /// the new elements.
    ///
    /// Values can be scalars or nested objects (maps, lists, text). Scalar
    /// values are inserted directly, while nested objects are created using
    /// batch insertion for efficiency.
    pub(crate) fn splice(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_obj: &ExId,
        index: usize,
        del: isize,
        vals: impl IntoIterator<Item = impl Into<hydrate::Value>>,
    ) -> Result<(), AutomergeError> {
        let obj = doc.exid_to_obj(ex_obj)?;
        if !matches!(obj.typ, ObjType::List | ObjType::Text) {
            return Err(AutomergeError::InvalidOp(obj.typ));
        }
        let values: Vec<hydrate::Value> = vals.into_iter().map(Into::into).collect();
        self.inner_splice(
            doc,
            patch_log,
            SpliceArgs {
                obj,
                index,
                del,
                splice_type: SpliceType::List(values),
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
        self.inner_splice(
            doc,
            patch_log,
            SpliceArgs {
                obj,
                index,
                del,
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

        let inserted_width = if !splice_type.is_empty() {
            let query = doc
                .ops()
                .query_insert_at(&obj.id, index, seq_type, self.scope.clone())?;

            index = query.index;

            let inserted_width = match splice_type {
                SpliceType::Text(t) => {
                    let mut batch = BatchInsertion::new(self, doc, patch_log, query.pos);
                    let SpliceResult { inserted_width } =
                        batch.splice_text(obj, query.index, query.elemid, t, query.marks);
                    batch.finish();
                    inserted_width
                }
                SpliceType::List(values) => {
                    let num_values = values.len();
                    let mut queue: VecDeque<(ObjMeta, &hydrate::Value)> = VecDeque::new();

                    // Batch 1: root insert ops at query.pos
                    {
                        let mut batch = BatchInsertion::new(self, doc, patch_log, query.pos);
                        let mut elemid = query.elemid;

                        for (i, value) in values.iter().enumerate() {
                            let (child_obj_type, op_type) = value_to_op_type(value);

                            let id = batch.append(move |pos, id| {
                                TxOp::insert(id, obj, pos, query.index + i, op_type, elemid)
                            });
                            elemid = ElemId(id);

                            if let Some(obj_type) = child_obj_type {
                                let child_obj_meta = ObjMeta {
                                    id: crate::types::ObjId(id),
                                    typ: obj_type,
                                };
                                queue.push_back((child_obj_meta, value));
                            }
                        }

                        batch.finish();
                    }

                    // Batch 2: all descendants at end of OpSet
                    if !queue.is_empty() {
                        let desc_pos = doc.ops().len();
                        let mut batch = BatchInsertion::new(self, doc, patch_log, desc_pos);
                        batch_bfs(&mut batch, &mut queue)?;
                        batch.finish();
                    }

                    num_values
                }
            };

            inserted_width
        } else {
            0
        };

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

    /// Put or insert a nested `hydrate::Value` into the document as a batch
    /// operation.
    ///
    /// This is much more efficient than decomposing the value into individual
    /// put/insert operations because it only requires two OpSet splices:
    /// one for the root op in the parent, and one bulk append for all descendants.
    ///
    /// When `insert` is true and `prop` is `Prop::Seq`, the value is inserted
    /// at the given index (shifting subsequent elements). When `insert` is
    /// false, the value replaces the existing element at that index. For
    /// `Prop::Map` the `insert` flag is ignored (maps always use put semantics).
    pub(crate) fn batch_create_object(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        ex_parent: &ExId,
        prop: Prop,
        value: &hydrate::Value,
        insert: bool,
    ) -> Result<ExId, AutomergeError> {
        let parent = doc.exid_to_obj(ex_parent)?;

        // Determine the ObjType for the root of the value being inserted
        let root_obj_type = match value {
            hydrate::Value::Map(_) => ObjType::Map,
            hydrate::Value::List(_) => ObjType::List,
            hydrate::Value::Text(_) => ObjType::Text,
            hydrate::Value::Scalar(_) => return Err(AutomergeError::NotAnObject),
        };

        // First insert the root of the new object
        let root_id = match (&prop, insert) {
            (Prop::Seq(index), true) => self.do_insert(
                doc,
                patch_log,
                &parent,
                parent
                    .typ
                    .as_sequence_type()
                    .ok_or(AutomergeError::InvalidOp(parent.typ))?,
                *index,
                OpType::Make(root_obj_type),
            )?,
            _ => self
                .local_op(doc, patch_log, &parent, prop, OpType::Make(root_obj_type))?
                .expect("creating a new object"),
        };

        let root_obj_id = crate::types::ObjId(root_id);
        let root_obj_meta = ObjMeta {
            id: root_obj_id,
            typ: root_obj_type,
        };

        let mut queue: VecDeque<(ObjMeta, &hydrate::Value)> = VecDeque::new();
        queue.push_back((root_obj_meta, value));
        let insert_pos = doc.ops().len();
        let mut batch = BatchInsertion::new(self, doc, patch_log, insert_pos);

        batch_bfs(&mut batch, &mut queue)?;

        batch.finish();

        Ok(doc.id_to_exid(root_id))
    }

    /// Initialize the root object of an empty document from a `hydrate::Map`.
    pub(crate) fn batch_init_root_map(
        &mut self,
        doc: &mut Automerge,
        patch_log: &mut PatchLog,
        value: &hydrate::Map,
    ) -> Result<(), AutomergeError> {
        let root_meta = ObjMeta {
            id: crate::types::ObjId::root(),
            typ: ObjType::Map,
        };

        let insert_pos = doc.ops().len();
        let mut batch = BatchInsertion::new(self, doc, patch_log, insert_pos);
        let mut queue: VecDeque<(ObjMeta, &hydrate::Value)> = VecDeque::new();

        let mut keys: Vec<_> = value.iter().collect();
        keys.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (key, map_value) in keys {
            let child_value = &map_value.value;
            let (child_obj_type, op_type) = value_to_op_type(child_value);

            let id = batch.append(|pos, id| {
                TxOp::map(
                    id,
                    root_meta,
                    pos,
                    ResolvedAction::VisibleUpdate(op_type),
                    key.to_string(),
                    vec![],
                )
            });

            if let Some(obj_type) = child_obj_type {
                let child_obj_meta = ObjMeta {
                    id: crate::types::ObjId(id),
                    typ: obj_type,
                };
                queue.push_back((child_obj_meta, child_value));
            }
        }

        batch_bfs(&mut batch, &mut queue)?;

        batch.finish();
        Ok(())
    }

    pub(crate) fn get_scope(&self) -> &Option<Clock> {
        &self.scope
    }

    pub(crate) fn get_deps(&self) -> Vec<ChangeHash> {
        self.deps.clone()
    }
}

enum SpliceType<'a> {
    List(Vec<hydrate::Value>),
    Text(&'a str),
}

impl SpliceType<'_> {
    fn seq_type(&self) -> SequenceType {
        match self {
            SpliceType::List(_) => SequenceType::List,
            SpliceType::Text(_) => SequenceType::Text,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::List(v) => v.is_empty(),
            Self::Text(v) => v.is_empty(),
        }
    }
}

struct SpliceArgs<'a> {
    obj: ObjMeta,
    index: usize,
    del: isize,
    splice_type: SpliceType<'a>,
}

struct BatchInsertion<'a> {
    inner: &'a mut TransactionInner,
    doc: &'a mut Automerge,
    patch_log: &'a mut PatchLog,
    pending_start: usize,
    insert_pos: usize,
}

impl<'a> BatchInsertion<'a> {
    fn new(
        inner: &'a mut TransactionInner,
        doc: &'a mut Automerge,
        patch_log: &'a mut PatchLog,
        start_pos: usize,
    ) -> Self {
        let pending_start = inner.pending.len();
        Self {
            inner,
            doc,
            patch_log,
            pending_start,
            insert_pos: start_pos,
        }
    }

    fn next_pos(&self) -> usize {
        self.inner.pending[self.pending_start..].len() + self.insert_pos
    }

    fn append<F: FnOnce(usize, OpId) -> TxOp>(&mut self, factory: F) -> OpId {
        let id = self.inner.next_id();
        let op = factory(self.next_pos(), id);
        self.inner
            .finalize_op(self.doc.text_encoding(), self.patch_log, &op, None);
        self.inner.pending.push(op);
        id
    }

    fn splice_text(
        &mut self,
        container: ObjMeta,
        index: usize,
        after: ElemId,
        text_str: &str,
        marks: Option<Arc<MarkSet>>,
    ) -> SpliceResult {
        let char_values: Vec<ScalarValue> = match self.doc.text_encoding() {
            // Arguably we should do this for all text, rather than just the grapheme cluster encoding.
            // However, at the time which I (Alex Good) am writing this code the existing implementation
            // uses the unicode code points and the grapheme cluster text encoding is a new thing. I
            // don't want to change the existing behaviour for the existing text encodings without a
            // little more thought.
            TextEncoding::GraphemeCluster => {
                text_str.graphemes(true).map(ScalarValue::from).collect()
            }
            _ => text_str.chars().map(ScalarValue::from).collect(),
        };
        let mut inserted_width = 0;
        let mut elemid = after;
        for char in char_values {
            let op = TxOp::insert_val(
                self.inner.next_id(),
                container,
                self.next_pos(),
                char,
                elemid,
            );
            inserted_width += op.bld.width(SequenceType::Text, self.doc.text_encoding());
            elemid = ElemId(op.id());
            self.inner.pending.push(op);
        }

        if self.patch_log.is_active() {
            self.patch_log.splice(container.id, index, text_str, marks);
        }

        SpliceResult { inserted_width }
    }

    fn finish(self) {
        let new_ops = &self.inner.pending[self.pending_start..];
        if !new_ops.is_empty() {
            self.doc.ops_mut().splice(self.insert_pos, new_ops);
        }
    }
}

struct SpliceResult {
    inserted_width: usize,
}

fn value_to_op_type(value: &hydrate::Value) -> (Option<ObjType>, OpType) {
    match value {
        hydrate::Value::Map(_) => (Some(ObjType::Map), OpType::Make(ObjType::Map)),
        hydrate::Value::List(_) => (Some(ObjType::List), OpType::Make(ObjType::List)),
        hydrate::Value::Text(_) => (Some(ObjType::Text), OpType::Make(ObjType::Text)),
        hydrate::Value::Scalar(s) => (None, OpType::Put(s.clone())),
    }
}

/// BFS traversal of nested hydrate values, appending ops to a `BatchInsertion`.
///
/// This is the shared logic used by `batch_create_object`, `batch_init_map`,
/// and `inner_splice` to populate the children of container objects.
fn batch_bfs(
    batch: &mut BatchInsertion<'_>,
    queue: &mut VecDeque<(ObjMeta, &'_ hydrate::Value)>,
) -> Result<(), AutomergeError> {
    while let Some((container_meta, container_value)) = queue.pop_front() {
        match (container_meta.typ, container_value) {
            (ObjType::Map, hydrate::Value::Map(map)) => {
                let mut keys: Vec<_> = map.iter().collect();
                keys.sort_by(|(a, _), (b, _)| a.cmp(b));

                for (key, map_value) in keys {
                    let child_value = &map_value.value;
                    let (child_obj_type, op_type) = value_to_op_type(child_value);

                    let id = batch.append(|pos, id| {
                        TxOp::map(
                            id,
                            container_meta,
                            pos,
                            ResolvedAction::VisibleUpdate(op_type),
                            key.to_string(),
                            vec![],
                        )
                    });

                    if let Some(obj_type) = child_obj_type {
                        let child_obj_meta = ObjMeta {
                            id: crate::types::ObjId(id),
                            typ: obj_type,
                        };
                        queue.push_back((child_obj_meta, child_value));
                    }
                }
            }
            (ObjType::List, hydrate::Value::List(list)) => {
                let mut elemid = HEAD;
                for (index, list_value) in list.iter().enumerate() {
                    let child_value = &list_value.value;
                    let (child_obj_type, op_type) = value_to_op_type(child_value);

                    let id = batch.append(move |pos, id| {
                        TxOp::insert(id, container_meta, pos, index, op_type, elemid)
                    });
                    elemid = ElemId(id);

                    if let Some(obj_type) = child_obj_type {
                        let child_obj_meta = ObjMeta {
                            id: crate::types::ObjId(id),
                            typ: obj_type,
                        };
                        queue.push_back((child_obj_meta, child_value));
                    }
                }
            }
            (ObjType::Text, hydrate::Value::Text(text)) => {
                let text_str = text.to_string();
                batch.splice_text(container_meta, 0, ElemId::head(), &text_str, None);
            }
            _ => {
                return Err(AutomergeError::InvalidOp(container_meta.typ));
            }
        }
    }
    Ok(())
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

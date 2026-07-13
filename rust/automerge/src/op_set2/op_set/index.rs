use crate::iter::tools::Shiftable;
use crate::op_set2::meta::ValueType;
use crate::op_set2::op_set::OpIdIter;
use crate::op_set2::op_set::{MarkIdx, MarkIndexBuilder, MarkIndexColumn};
use crate::op_set2::types::{Action, ScalarValue};
use crate::op_set2::MarkData;
#[cfg(test)]
use crate::op_set2::OpSet;
use crate::op_set2::ValueMeta;
use crate::op_set2::{ChangeOp, Op, OpBuilder, ReadOpError};
#[cfg(test)]
use crate::types::SequenceType;
use crate::types::{ObjId, ObjType, OpId, TextEncoding};
use hexane::encoder::{BoolEncoder, RleEncoder};
use hexane::{EncoderApi, RunSrc};
use std::collections::HashMap;

/// Streaming index builder.
///
/// Ops are buffered per register (the `group` state machine) and flushed to
/// run-length encoders at each register boundary; the encoders hand their
/// slabs directly to the final columns. All the group-retroactive
/// complexity — electing the `top` op, counter increments rewriting the
/// visibility and inc entries of their counter — is confined to a single
/// register, so the buffer never reaches back past the last flush.
pub(crate) struct IndexBuilder {
    /// pending counter successor ids of the current group:
    /// succ id -> [(absolute inc index, absolute op index)]
    counters: HashMap<OpId, Vec<(usize, usize)>>,
    /// ops of the current (unflushed) register
    group: Vec<GroupOp>,
    /// inc-column entries of the current register (one per succ id)
    group_incs: Vec<Option<i64>>,
    /// mark entries of the current register
    group_marks: Vec<Option<MarkIdx>>,
    text: RleEncoder<'static, Option<u32>>,
    top: BoolEncoder,
    visible: BoolEncoder,
    inc: RleEncoder<'static, Option<i64>>,
    marks: RleEncoder<'static, Option<MarkIdx>>,
    mark_cache: HashMap<OpId, MarkData<'static>>,
    obj_info: ObjIndex,
    text_encoding: TextEncoding,
    mark_order: MarkOrderValidator,
}

#[derive(Debug, Clone, Copy)]
struct GroupOp {
    /// succ count (`u32::MAX` for increment ops), decremented when a
    /// counter's increment is applied
    succ: u32,
    /// text width — only read if this op is elected top
    width: u32,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct MarkOrderValidator {
    begins: HashMap<OpId, ObjId>,
    error: Option<String>,
}

impl MarkOrderValidator {
    pub(crate) fn process_op(&mut self, op: &Op<'_>) {
        let mark_index = op.mark_index();
        self.process_mark_index(op, &mark_index);
    }

    pub(crate) fn process_mark_index(
        &mut self,
        op: &Op<'_>,
        mark_index: &Option<MarkIndexBuilder>,
    ) {
        if self.error.is_some() {
            return;
        }
        self.check_mark_op(op, mark_index);
    }

    pub(crate) fn take_error(&mut self) -> Option<String> {
        self.error.take()
    }

    /// Check that mark ops:
    /// * Always start and end in the same object
    /// * Have the start op appear before the end op
    fn check_mark_op(&mut self, op: &Op<'_>, mark_index: &Option<MarkIndexBuilder>) {
        self.check_mark(op.obj, op.id, mark_index)
    }

    fn check_mark(&mut self, obj: ObjId, op_id: OpId, mark_index: &Option<MarkIndexBuilder>) {
        match mark_index {
            Some(MarkIndexBuilder::Start(id, _)) => {
                self.begins.insert(*id, obj);
            }
            Some(MarkIndexBuilder::End(begin)) => match self.begins.get(begin) {
                Some(o) if *o == obj => {}
                Some(_) => {
                    self.error = Some(format!(
                        "mark end {:?} references mark begin {:?} in a different object",
                        op_id, begin
                    ));
                }
                None => {
                    self.error = Some(format!(
                        "mark end {:?} occurs before mark begin {:?}",
                        op_id, begin
                    ));
                }
            },
            None => {}
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ObjIndex(pub(crate) HashMap<OpId, ObjInfo>);

impl ObjIndex {
    pub(crate) fn object_type(&self, obj: &ObjId) -> Option<ObjType> {
        if obj.is_root() {
            Some(ObjType::Map)
        } else {
            self.0.get(&obj.0).map(|p| p.obj_type)
        }
    }

    pub(crate) fn object_parent(&self, obj: &ObjId) -> Option<ObjId> {
        if obj.is_root() {
            None
        } else {
            self.0.get(&obj.0).map(|p| p.parent)
        }
    }

    pub(crate) fn insert(&mut self, id: OpId, obj_info: ObjInfo) {
        self.0.insert(id, obj_info);
    }

    pub(crate) fn remove(&mut self, id: OpId) {
        self.0.remove(&id);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjInfo {
    pub(crate) parent: ObjId,
    pub(crate) obj_type: ObjType,
}

impl ObjInfo {
    pub(crate) fn with_new_actor(self, idx: usize) -> Self {
        Self {
            parent: self.parent.with_new_actor(idx),
            obj_type: self.obj_type,
        }
    }

    pub(crate) fn without_actor(self, idx: usize) -> Option<Self> {
        Some(Self {
            parent: self.parent.without_actor(idx)?,
            obj_type: self.obj_type,
        })
    }
}

impl Op<'_> {
    #[cfg(test)]
    pub(crate) fn obj_info(&self) -> Option<ObjInfo> {
        let obj_type = ObjType::try_from(self.action).ok()?;
        let parent = self.obj;
        Some(ObjInfo { parent, obj_type })
    }
}

impl ChangeOp {
    pub(crate) fn obj_info(&self) -> Option<ObjInfo> {
        self.bld.obj_info()
    }
}

impl OpBuilder<'_> {
    pub(crate) fn obj_info(&self) -> Option<ObjInfo> {
        let obj_type = ObjType::try_from(self.action).ok()?;
        let parent = self.obj;
        Some(ObjInfo { parent, obj_type })
    }
}

pub(crate) struct Indexes {
    pub(crate) text: hexane::PrefixColumn<Option<u32>>,
    pub(crate) top: hexane::PrefixColumn<bool>,
    pub(crate) visible: hexane::Column<bool>,
    pub(crate) inc: hexane::Column<Option<i64>>,
    pub(crate) mark: MarkIndexColumn,
    pub(crate) obj_info: ObjIndex,
}

/// Object ranges derived by zipping the obj_actor / obj_ctr run streams,
/// validating as it goes: ids must be fully null or fully set, and
/// strictly increasing (which also guarantees each object's ops are
/// contiguous). This replaces `column_validation`'s obj walk on the load
/// path.
pub(crate) struct ObjRunWalk<A, C> {
    actor: A,
    ctr: C,
    actor_head: Option<(Option<crate::op_set2::ActorIdx>, usize)>,
    ctr_head: Option<(Option<u32>, usize)>,
    pos: usize,
    prev: Option<ObjId>,
}

impl<A, C> ObjRunWalk<A, C> {
    pub(crate) fn new(actor: A, ctr: C) -> Self {
        Self {
            actor,
            ctr,
            actor_head: None,
            ctr_head: None,
            pos: 0,
            prev: None,
        }
    }

    fn try_next_obj<'a>(&mut self) -> Result<Option<(ObjId, usize)>, ReadOpError>
    where
        A: RunSrc<'a, Option<crate::op_set2::ActorIdx>>,
        C: RunSrc<'a, Option<u32>>,
    {
        if self.actor_head.is_none() {
            self.actor_head = self.actor.try_next_run()?.map(|r| (r.value, r.count));
        }
        if self.ctr_head.is_none() {
            self.ctr_head = self.ctr.try_next_run()?.map(|r| (r.value, r.count));
        }
        let (Some((actor, a_left)), Some((ctr, c_left))) = (self.actor_head, self.ctr_head) else {
            return Ok(None);
        };
        let obj =
            ObjId::load(ctr.map(|c| c as u64), actor).ok_or(ReadOpError::InvalidObjId(self.pos))?;
        if self.prev.is_some_and(|p| obj <= p) {
            return Err(ReadOpError::ObjOutOfOrder(self.pos));
        }
        self.prev = Some(obj);
        let count = a_left.min(c_left);
        self.pos += count;
        self.actor_head = (a_left > count).then_some((actor, a_left - count));
        self.ctr_head = (c_left > count).then_some((ctr, c_left - count));
        Ok(Some((obj, count)))
    }
}

/// Forward-only access to the columns the rare-op path needs — everything
/// that is not carried by the index run stream itself. Built either from a
/// loaded op set or from the phase-1 columns during a fused load.
pub(crate) struct RareOps<'a> {
    ids: OpIdIter<'a>,
    marks: super::MarkInfoIter<'a>,
    succ_ids: OpIdIter<'a>,
    raw: hexane::RawColumnIter<'a>,
}

impl<'a> RareOps<'a> {
    pub(crate) fn new(
        ids: OpIdIter<'a>,
        marks: super::MarkInfoIter<'a>,
        succ_ids: OpIdIter<'a>,
        raw: hexane::RawColumnIter<'a>,
    ) -> Self {
        Self {
            ids,
            marks,
            succ_ids,
            raw,
        }
    }

    fn id_at(&mut self, pos: usize) -> Result<OpId, ReadOpError> {
        self.ids
            .shift_next(pos..pos + 1)
            .ok_or(ReadOpError::MissingValue("id"))
    }

    fn mark_at(&mut self, pos: usize) -> Result<Option<&'a str>, ReadOpError> {
        self.marks
            .shift_next(pos..pos + 1)
            .map(|(name, _expand)| name)
            .ok_or(ReadOpError::MissingValue("mark_name"))
    }

    fn raw_at(&mut self, start: usize, len: usize) -> &'a [u8] {
        self.raw.seek_to(start);
        self.raw.take(len)
    }

    fn value_at(&mut self, meta: ValueMeta, start: usize) -> Result<ScalarValue<'a>, ReadOpError> {
        let raw = self.raw_at(start, meta.length());
        Ok(ScalarValue::from_raw(meta, raw)?)
    }
}

impl IndexBuilder {
    pub(crate) fn new(encoding: TextEncoding) -> Self {
        Self {
            counters: HashMap::new(),
            group: Vec::new(),
            group_incs: Vec::new(),
            group_marks: Vec::new(),
            text: RleEncoder::new().with_segments(),
            top: BoolEncoder::new().with_segments(),
            visible: BoolEncoder::new().with_segments(),
            inc: RleEncoder::new().with_segments(),
            marks: RleEncoder::new().with_segments(),
            mark_cache: HashMap::new(),
            obj_info: ObjIndex::default(),
            text_encoding: encoding,
            mark_order: MarkOrderValidator::default(),
        }
    }

    /// Number of ops already flushed to the encoders — the absolute index
    /// of the current group's first op.
    fn ops_flushed(&self) -> usize {
        self.visible.len()
    }

    /// Total ops processed so far (flushed and buffered).
    pub(crate) fn ops_len(&self) -> usize {
        self.ops_flushed() + self.group.len()
    }

    /// Number of inc entries already flushed — the absolute inc index of
    /// the current group's first succ entry.
    fn incs_flushed(&self) -> usize {
        self.inc.len()
    }

    /// Close the current register: elect its top op and stream the
    /// buffered entries out to the encoders.
    pub(crate) fn flush(&mut self) {
        if self.group.is_empty() {
            debug_assert!(self.group_incs.is_empty());
            return;
        }
        // the top op is the last op of the register that is still visible
        match self.group.iter().rposition(|g| g.succ == 0) {
            Some(t) => {
                self.top.append_n(false, t);
                self.top.append(true);
                self.top.append_n(false, self.group.len() - t - 1);
                self.text.append_n(None, t);
                self.text.append(Some(self.group[t].width));
                self.text.append_n(None, self.group.len() - t - 1);
            }
            None => {
                self.top.append_n(false, self.group.len());
                self.text.append_n(None, self.group.len());
            }
        }
        for g in &self.group {
            self.visible.append(g.succ == 0);
        }
        for &v in &self.group_incs {
            self.inc.append(v);
        }
        for &m in &self.group_marks {
            self.marks.append(m);
        }
        self.group.clear();
        self.group_incs.clear();
        self.group_marks.clear();
        // successors live in the same register as their target: anything
        // left is a delete (which has no op row) and can never match
        self.counters.clear();
    }

    /// The shared per-op path: both the op-at-a-time reference builder
    /// (the test-only `process_op`) and the rare path of the column walk
    /// feed through here.
    #[allow(clippy::too_many_arguments)]
    fn process_op_parts(
        &mut self,
        id: OpId,
        mark_index: Option<MarkIndexBuilder>,
        inc_value: Option<i64>,
        obj_info: Option<ObjInfo>,
        succ_vis: u32,
        width: u32,
    ) {
        self.mark_order.check_mark(
            obj_info.map(|o| o.parent).unwrap_or_default(),
            id,
            &mark_index,
        );
        self.group_marks.push(match mark_index {
            Some(MarkIndexBuilder::Start(mark_id, mark)) => {
                self.mark_cache.insert(mark_id, mark);
                Some(MarkIdx::Start(mark_id))
            }
            Some(MarkIndexBuilder::End(mark_id)) => Some(MarkIdx::End(mark_id)),
            None => None,
        });

        let count = self.counters.remove(&id);

        if let Some(i) = inc_value {
            let incs_flushed = self.incs_flushed();
            let ops_flushed = self.ops_flushed();
            for (inc_idx, op_idx) in count.into_iter().flatten() {
                // group-local: a counter and its increments share a register
                self.group_incs[inc_idx - incs_flushed] = Some(i);
                self.group[op_idx - ops_flushed].succ -= 1;
            }
        }

        if let Some(obj_info) = obj_info {
            self.obj_info.insert(id, obj_info);
        }

        self.group.push(GroupOp {
            succ: succ_vis,
            width,
        });
    }

    #[cfg(test)]
    pub(crate) fn process_op(&mut self, op: &Op<'_>) {
        let mark_index = op.mark_index();
        self.mark_order.process_mark_index(op, &mark_index);
        self.group_marks.push(match mark_index {
            Some(MarkIndexBuilder::Start(id, mark)) => {
                self.mark_cache.insert(id, mark);
                Some(MarkIdx::Start(id))
            }
            Some(MarkIndexBuilder::End(id)) => Some(MarkIdx::End(id)),
            None => None,
        });

        let count = self.counters.remove(&op.id);

        if let Some(i) = op.get_increment_value() {
            let incs_flushed = self.incs_flushed();
            let ops_flushed = self.ops_flushed();
            for (inc_idx, op_idx) in count.into_iter().flatten() {
                // group-local: a counter and its increments share a register
                self.group_incs[inc_idx - incs_flushed] = Some(i);
                self.group[op_idx - ops_flushed].succ -= 1;
            }
        }

        if let Some(obj_info) = op.obj_info() {
            self.obj_info.insert(op.id, obj_info);
        }

        self.group.push(GroupOp {
            succ: vis_num(op),
            width: op.width(SequenceType::Text, self.text_encoding) as u32,
        });
    }

    pub(crate) fn process_succ(&mut self, op_is_counter: bool, id: OpId) {
        if op_is_counter {
            let entry = (
                self.incs_flushed() + self.group_incs.len(),
                self.ops_flushed() + self.group.len() - 1,
            );
            self.counters.entry(id).or_default().push(entry);
        }
        self.group_incs.push(None);
    }

    /// True if this run needs its ops materialized one at a time.
    fn is_rare(&self, run: &IndexRun) -> bool {
        run.action == Action::Mark
            || run.action == Action::Increment
            || ObjType::try_from(run.action).is_ok()
            || run.meta.type_code() == ValueType::Counter
            || !self.counters.is_empty()
    }

    /// Build the index from a loaded op set's columns — the equality
    /// test's way of running [`Self::process_columns`] over in-memory
    /// iterators to compare against the op-at-a-time reference builder.
    /// Production always builds indexes during column load.
    #[cfg(test)]
    pub(crate) fn process_op_set(&mut self, op_set: &OpSet) -> Result<(), ReadOpError> {
        let rare = RareOps::new(
            OpIdIter::new(op_set.cols.id_actor.iter(), op_set.cols.id_ctr.iter()),
            super::MarkInfoIter::new(op_set.cols.mark_name.iter(), op_set.cols.expand.iter()),
            OpIdIter::new(op_set.cols.succ_actor.iter(), op_set.cols.succ_ctr.iter()),
            op_set.cols.value.iter(),
        );
        self.process_columns(
            ObjRunWalk::new(op_set.cols.obj_actor.iter(), op_set.cols.obj_ctr.iter()),
            op_set.cols.action.iter(),
            op_set.cols.value_meta.values().iter(),
            op_set.cols.succ_count.values().iter(),
            op_set.cols.insert.values().iter(),
            op_set.cols.key_str.iter(),
            rare,
        )
    }

    /// Build the index by walking column run streams directly instead of
    /// materializing every op.
    ///
    /// Per op this touches only the action, succ-count and value-meta
    /// streams, advanced run-at-a-time. Register boundaries come from run
    /// lengths: the key_str stream for maps (each run is one key's
    /// register) and the insert stream for sequences (a register is one
    /// `true` followed by zero or more `false`s). Uniform runs of
    /// single-op registers stream straight to the encoders; everything
    /// else goes through the same per-register buffer as the op-at-a-time
    /// path. Rare ops (marks, object creation, increments, counters) are
    /// materialized individually from the [`RareOps`] columns.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn process_columns<'a, OA, OC, A, M, S, I, K>(
        &mut self,
        mut objs: ObjRunWalk<OA, OC>,
        action: A,
        meta: M,
        succ: S,
        inserts: I,
        keys: K,
        mut rare: RareOps<'_>,
    ) -> Result<(), ReadOpError>
    where
        OA: RunSrc<'a, Option<crate::op_set2::ActorIdx>>,
        OC: RunSrc<'a, Option<u32>>,
        A: RunSrc<'a, Action>,
        M: RunSrc<'a, ValueMeta>,
        S: RunSrc<'a, u32>,
        I: RunSrc<'a, bool>,
        K: RunSrc<'a, Option<String>>,
    {
        let objrep = self.text_encoding.width("\u{fffc}") as u32;
        let mut iter = IndexIter::new(action, meta, succ);
        let mut bounds = BoundaryIter::new(inserts, keys);
        let mut pos = 0;

        while let Some((obj, obj_len)) = objs.try_next_obj()? {
            let seq = matches!(
                self.obj_info.object_type(&obj),
                Some(ObjType::List) | Some(ObjType::Text)
            );
            bounds.start_obj(seq, obj_len)?;

            while let Some((group_len, repeat)) = bounds.next_batch()? {
                if group_len == 1 && repeat > 1 {
                    // a stretch of single-op registers: every op is its own
                    // group, so uniform runs bypass the buffer entirely
                    let mut remaining = repeat;
                    while remaining > 0 {
                        let run = iter
                            .next_run(remaining)?
                            .ok_or(ReadOpError::MissingValue("index columns"))?;
                        remaining -= run.count;
                        if self.is_rare(&run) {
                            self.rare_run(&run, pos, obj, objrep, &mut rare, true)?;
                        } else {
                            self.stream_singletons(&run, objrep, &mut rare)?;
                        }
                        pos += run.count;
                    }
                } else {
                    for _ in 0..repeat {
                        let mut remaining = group_len;
                        while remaining > 0 {
                            let run = iter
                                .next_run(remaining)?
                                .ok_or(ReadOpError::MissingValue("index columns"))?;
                            remaining -= run.count;
                            if self.is_rare(&run) {
                                self.rare_run(&run, pos, obj, objrep, &mut rare, false)?;
                            } else {
                                self.buffer_run(&run, objrep, &mut rare)?;
                            }
                            pos += run.count;
                        }
                        self.flush();
                    }
                }
            }
        }
        Ok(())
    }

    /// The text width of one op in a long-string run, from the raw value
    /// bytes (mirrors `Op::as_str`: only `Set` string values render as
    /// their text, everything else is an object replacement char; marks
    /// are rare and never reach here).
    fn str_width(
        &mut self,
        run: &IndexRun,
        i: usize,
        objrep: u32,
        rare: &mut RareOps<'_>,
    ) -> Result<u32, ReadOpError> {
        if run.action != Action::Set {
            return Ok(objrep);
        }
        let len = run.meta.length();
        let value = rare.value_at(run.meta, run.raw_prefix as usize + i * len)?;
        Ok(match &value {
            ScalarValue::Str(s) => self.text_encoding.width(s) as u32,
            _ => objrep,
        })
    }

    /// Stream a uniform run of single-op registers straight to the
    /// encoders — no per-op work at all in the common case.
    fn stream_singletons(
        &mut self,
        run: &IndexRun,
        objrep: u32,
        rare: &mut RareOps<'_>,
    ) -> Result<(), ReadOpError> {
        debug_assert!(self.group.is_empty());
        let vis = run.succ == 0;
        self.visible.append_n(vis, run.count);
        self.top.append_n(vis, run.count);
        self.marks.append_n(None, run.count);
        self.inc.append_n(None, run.count * run.succ as usize);
        if !vis {
            self.text.append_n(None, run.count);
        } else if run.meta.type_code() != ValueType::String {
            self.text.append_n(Some(objrep), run.count);
        } else if run.meta.length() == 1 {
            // a 1-byte utf8 string is ascii: width 1 in every encoding
            self.text.append_n(Some(1), run.count);
        } else {
            for i in 0..run.count {
                let w = self.str_width(run, i, objrep, rare)?;
                self.text.append(Some(w));
            }
        }
        Ok(())
    }

    /// Append a uniform run to the current register's buffer.
    fn buffer_run(
        &mut self,
        run: &IndexRun,
        objrep: u32,
        rare: &mut RareOps<'_>,
    ) -> Result<(), ReadOpError> {
        if run.meta.type_code() == ValueType::String && run.meta.length() > 1 {
            for i in 0..run.count {
                let width = self.str_width(run, i, objrep, rare)?;
                self.group.push(GroupOp {
                    succ: run.succ,
                    width,
                });
            }
        } else {
            let width = if run.meta.type_code() == ValueType::String {
                1 // ascii, see above
            } else {
                objrep
            };
            self.group.extend(std::iter::repeat_n(
                GroupOp {
                    succ: run.succ,
                    width,
                },
                run.count,
            ));
        }
        self.group_incs
            .extend(std::iter::repeat_n(None, run.count * run.succ as usize));
        self.group_marks
            .extend(std::iter::repeat_n(None, run.count));
        Ok(())
    }

    /// Materialize each op of a rare run from the [`RareOps`] columns and
    /// feed it through the same per-op path the op-at-a-time builder uses.
    /// With `singletons` each op is its own register and is flushed
    /// immediately.
    fn rare_run(
        &mut self,
        run: &IndexRun,
        pos: usize,
        obj: ObjId,
        objrep: u32,
        rare: &mut RareOps<'_>,
        singletons: bool,
    ) -> Result<(), ReadOpError> {
        let len = run.meta.length();
        for i in 0..run.count {
            let p = pos + i;
            let id = rare.id_at(p)?;
            let value = rare.value_at(run.meta, run.raw_prefix as usize + i * len)?;

            // mirrors `Op::mark_index`
            let mark_index = if run.action == Action::Mark {
                match rare.mark_at(p)? {
                    Some(name) => Some(MarkIndexBuilder::Start(
                        id,
                        MarkData {
                            name: std::borrow::Cow::Owned(name.to_string()),
                            value: value.clone().into_owned(),
                        },
                    )),
                    None => Some(MarkIndexBuilder::End(id.prev())),
                }
            } else {
                None
            };

            // mirrors `OpBuilder::get_increment_value`
            let inc_value = match (run.action, &value) {
                (Action::Increment, ScalarValue::Int(n)) => Some(*n),
                (Action::Increment, ScalarValue::Uint(n)) => Some(*n as i64),
                _ => None,
            };

            let obj_info = ObjType::try_from(run.action).ok().map(|obj_type| ObjInfo {
                parent: obj,
                obj_type,
            });

            // mirrors `OpBuilder::width` / `as_str`
            let width = match (run.action, &value) {
                (Action::Mark, _) => 0,
                (Action::Set, ScalarValue::Str(s)) => self.text_encoding.width(s) as u32,
                _ => objrep,
            };

            let is_counter = matches!(value, ScalarValue::Counter(_));
            let succ_vis = if run.action == Action::Increment {
                u32::MAX
            } else {
                run.succ
            };

            self.process_op_parts(id, mark_index, inc_value, obj_info, succ_vis, width);

            if run.succ > 0 {
                if is_counter {
                    let start = run.succ_prefix as usize + i * run.succ as usize;
                    let end = start + run.succ as usize;
                    let mut next_id = rare.succ_ids.shift_next(start..end);
                    while let Some(succ_id) = next_id {
                        self.process_succ(true, succ_id);
                        next_id = rare.succ_ids.next();
                    }
                } else {
                    self.group_incs
                        .extend(std::iter::repeat_n(None, run.succ as usize));
                }
            }
            if singletons {
                self.flush();
            }
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> (Indexes, MarkOrderValidator) {
        self.flush();
        let text = hexane::PrefixColumn::from_column(self.text.into_column());
        let top = hexane::PrefixColumn::from_column(self.top.into_column());
        let visible: hexane::Column<bool> = self.visible.into_column();
        let inc: hexane::Column<Option<i64>> = self.inc.into_column();
        let mark = MarkIndexColumn::from_parts(
            hexane::PrefixColumn::from_column(self.marks.into_column()),
            self.mark_cache,
        );

        (
            Indexes {
                text,
                top,
                visible,
                inc,
                mark,
                obj_info: self.obj_info,
            },
            self.mark_order,
        )
    }
}

impl Indexes {
    /// Test-only drift guard: the column-walking builder must produce
    /// exactly the same indexes as the op-at-a-time reference builder.
    #[cfg(test)]
    pub(crate) fn assert_same(&self, other: &Self) {
        assert_eq!(
            self.visible.save(),
            other.visible.save(),
            "index drift: visible"
        );
        assert_eq!(self.top.save(), other.top.save(), "index drift: top");
        assert_eq!(self.text.save(), other.text.save(), "index drift: text");
        assert_eq!(self.inc.save(), other.inc.save(), "index drift: inc");
        self.mark.assert_same(&other.mark);
        assert_eq!(self.obj_info.0, other.obj_info.0, "index drift: obj_info");
    }
}

/// Yields the register lengths of the current object: key_str runs for
/// maps (each run is one key's register — the run streams are canonical,
/// so adjacent runs never carry equal values), insert-run structure for
/// sequences (a register is one `true` followed by zero or more
/// `false`s).
///
/// The sources are consumed strictly sequentially, so runs that span an
/// object boundary are clipped against the current object, and whichever
/// stream an object does not consult (inserts for maps, keys for
/// sequences) is drained lazily to stay position-aligned.
struct BoundaryIter<I, K> {
    inserts: I,
    keys: K,
    insert_head: Option<(bool, usize)>,
    key_head: Option<usize>,
    /// items owed to each stream by objects that did not consult it
    insert_owed: usize,
    keys_owed: usize,
    seq: bool,
    /// ops left in the current object
    remaining: usize,
    /// pending trues from the current insert run; all but the last are
    /// singleton registers, the last stays open for its trailing falses
    ones: usize,
}

impl<I, K> BoundaryIter<I, K> {
    fn new(inserts: I, keys: K) -> Self {
        Self {
            inserts,
            keys,
            insert_head: None,
            key_head: None,
            insert_owed: 0,
            keys_owed: 0,
            seq: false,
            remaining: 0,
            ones: 0,
        }
    }

    fn start_obj(&mut self, seq: bool, len: usize) -> Result<(), ReadOpError> {
        debug_assert_eq!(self.remaining, 0);
        debug_assert_eq!(self.ones, 0);
        self.seq = seq;
        self.remaining = len;
        if seq {
            self.keys_owed += len;
        } else {
            self.insert_owed += len;
        }
        Ok(())
    }

    /// Pull the next insert run, clipped to the current object, after
    /// draining anything owed by objects that didn't consult this stream.
    fn next_insert_run<'a>(&mut self) -> Result<Option<(bool, usize)>, ReadOpError>
    where
        I: RunSrc<'a, bool>,
    {
        while self.insert_owed > 0 {
            let (value, left) = match self.insert_head.take() {
                Some(h) => h,
                None => match self.inserts.try_next_run()? {
                    Some(r) => (r.value, r.count),
                    None => return Err(ReadOpError::MissingValue("insert")),
                },
            };
            let take = left.min(self.insert_owed);
            self.insert_owed -= take;
            if left > take {
                self.insert_head = Some((value, left - take));
            }
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let (value, left) = match self.insert_head.take() {
            Some(h) => h,
            None => match self.inserts.try_next_run()? {
                Some(r) => (r.value, r.count),
                None => return Err(ReadOpError::MissingValue("insert")),
            },
        };
        let take = left.min(self.remaining);
        self.remaining -= take;
        if left > take {
            self.insert_head = Some((value, left - take));
        }
        Ok(Some((value, take)))
    }

    /// Pull the next key run length, clipped to the current object, after
    /// draining anything owed.
    fn next_key_run<'a>(&mut self) -> Result<Option<usize>, ReadOpError>
    where
        K: RunSrc<'a, Option<String>>,
    {
        while self.keys_owed > 0 {
            let left = match self.key_head.take() {
                Some(h) => h,
                None => match self.keys.try_next_run()? {
                    Some(r) => r.count,
                    None => return Err(ReadOpError::MissingValue("key_str")),
                },
            };
            let take = left.min(self.keys_owed);
            self.keys_owed -= take;
            if left > take {
                self.key_head = Some(left - take);
            }
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let left = match self.key_head.take() {
            Some(h) => h,
            None => match self.keys.try_next_run()? {
                Some(r) => r.count,
                None => return Err(ReadOpError::MissingValue("key_str")),
            },
        };
        let take = left.min(self.remaining);
        self.remaining -= take;
        if left > take {
            self.key_head = Some(left - take);
        }
        Ok(Some(take))
    }

    /// The next batch of registers as `(len, repeat)`: `repeat` consecutive
    /// registers of `len` ops each. `repeat > 1` only for single-op
    /// registers (a run of inserts), which is the batch the streaming fast
    /// path feeds on.
    fn next_batch<'a>(&mut self) -> Result<Option<(usize, usize)>, ReadOpError>
    where
        I: RunSrc<'a, bool>,
        K: RunSrc<'a, Option<String>>,
    {
        if self.seq {
            self.next_seq()
        } else {
            Ok(self.next_key_run()?.map(|count| (count, 1)))
        }
    }

    fn next_seq<'a>(&mut self) -> Result<Option<(usize, usize)>, ReadOpError>
    where
        I: RunSrc<'a, bool>,
    {
        loop {
            // more than one pending true: all but the last are singleton
            // registers, the last stays open for its trailing falses
            if self.ones > 1 {
                let repeat = self.ones - 1;
                self.ones = 1;
                return Ok(Some((1, repeat)));
            }
            match self.next_insert_run()? {
                Some((true, count)) => self.ones += count,
                Some((false, count)) => {
                    // falses close the one pending true (if any); `ones`
                    // is 0 only for a defensive headless run
                    let result = self.ones + count;
                    self.ones = 0;
                    return Ok(Some((result, 1)));
                }
                None if self.ones > 0 => {
                    self.ones = 0;
                    return Ok(Some((1, 1)));
                }
                None => return Ok(None),
            }
        }
    }
}

/// The per-op streams consumed by [`IndexBuilder::process_columns`] —
/// action, value *meta* and succ count — advanced run-at-a-time. The raw
/// value bytes are never touched here: rare ops and long-string widths
/// read them through [`RareOps`] at offsets derived from the meta stream.
struct IndexIter<A, M, S> {
    action: A,
    meta: M,
    succ: S,
    action_head: Option<(Action, usize)>,
    meta_head: Option<(ValueMeta, usize)>,
    succ_head: Option<(u32, usize)>,
    /// absolute succ-column offset at the current position
    succ_prefix: u64,
    /// absolute raw value byte offset at the current position
    raw_prefix: u64,
}

/// A block of consecutive ops sharing one action, one value meta and one
/// succ count.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexRun {
    pub(crate) count: usize,
    pub(crate) action: Action,
    pub(crate) meta: ValueMeta,
    pub(crate) succ: u32,
    /// absolute succ-column offset of the first op's succ entries
    pub(crate) succ_prefix: u64,
    /// absolute raw value byte offset of the first op's value
    pub(crate) raw_prefix: u64,
}

impl<A, M, S> IndexIter<A, M, S> {
    fn new(action: A, meta: M, succ: S) -> Self {
        Self {
            action,
            meta,
            succ,
            action_head: None,
            meta_head: None,
            succ_head: None,
            succ_prefix: 0,
            raw_prefix: 0,
        }
    }

    /// The next run of at most `max` ops sharing action, meta and succ.
    fn next_run<'a>(&mut self, max: usize) -> Result<Option<IndexRun>, ReadOpError>
    where
        A: RunSrc<'a, Action>,
        M: RunSrc<'a, ValueMeta>,
        S: RunSrc<'a, u32>,
    {
        if self.action_head.is_none() {
            self.action_head = self.action.try_next_run()?.map(|r| (r.value, r.count));
        }
        if self.meta_head.is_none() {
            self.meta_head = self.meta.try_next_run()?.map(|r| (r.value, r.count));
        }
        if self.succ_head.is_none() {
            self.succ_head = self.succ.try_next_run()?.map(|r| (r.value, r.count));
        }
        let (Some((action, a_left)), Some((meta, m_left)), Some((succ, s_left))) =
            (self.action_head, self.meta_head, self.succ_head)
        else {
            return Ok(None);
        };

        let count = max.min(a_left).min(m_left).min(s_left);
        debug_assert!(count > 0);

        self.action_head = (a_left > count).then_some((action, a_left - count));
        self.meta_head = (m_left > count).then_some((meta, m_left - count));
        self.succ_head = (s_left > count).then_some((succ, s_left - count));

        let run = IndexRun {
            count,
            action,
            meta,
            succ,
            succ_prefix: self.succ_prefix,
            raw_prefix: self.raw_prefix,
        };
        self.succ_prefix += succ as u64 * count as u64;
        self.raw_prefix += meta.length() as u64 * count as u64;
        Ok(Some(run))
    }
}

/// Compress an iterator into `(value, count)` runs.
pub(crate) fn runs<T: PartialEq>(
    iter: impl Iterator<Item = T>,
) -> impl Iterator<Item = (T, usize)> {
    let mut iter = iter.peekable();
    std::iter::from_fn(move || {
        let value = iter.next()?;
        let mut count = 1;
        while iter.next_if(|v| *v == value).is_some() {
            count += 1;
        }
        Some((value, count))
    })
}

#[cfg(test)]
fn vis_num(op: &Op<'_>) -> u32 {
    if op.is_inc() {
        u32::MAX
    } else {
        op.succ().len() as u32
    }
}

#[cfg(test)]
mod tests {
    use crate::op_set2::change::IndexedChangeCollector;
    use crate::transaction::Transactable;
    use crate::{Automerge, ObjType, ROOT};

    fn assert_builders_match(doc: &Automerge) {
        // rebuild from a fresh load so neither builder sees existing indexes
        let bytes = doc.save();
        let reloaded = Automerge::load(&bytes).unwrap();
        let op_set = reloaded.ops();

        let mut by_ops = op_set.index_builder();
        let mut icc = IndexedChangeCollector::index_only(&mut by_ops);
        icc.process_ops(op_set).unwrap();
        let (ops_indexes, _) = by_ops.finish();

        let mut by_cols = op_set.index_builder();
        by_cols.process_op_set(op_set).unwrap();
        let (cols_indexes, _) = by_cols.finish();

        ops_indexes.assert_same(&cols_indexes);
    }

    #[test]
    fn column_index_builder_matches_op_index_builder() {
        use crate::marks::{ExpandMark, Mark};

        // text with marks, splices and deletes
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "hello wörld £5 <20").unwrap();
        tx.mark(
            &text,
            Mark::new("bold".into(), true, 2, 9),
            ExpandMark::After,
        )
        .unwrap();
        tx.splice_text(&text, 4, 3, "XY").unwrap();
        tx.unmark(&text, "bold", 3, 6, ExpandMark::After).unwrap();
        tx.commit();
        assert_builders_match(&doc);

        // counters with increments, incl. deleted counters
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        tx.put(ROOT, "c", crate::ScalarValue::Counter(10.into()))
            .unwrap();
        tx.increment(ROOT, "c", 5).unwrap();
        tx.increment(ROOT, "c", -3).unwrap();
        tx.put(ROOT, "d", crate::ScalarValue::Counter(1.into()))
            .unwrap();
        tx.put(ROOT, "d", 99).unwrap(); // overwrite counter with non-increment
        tx.commit();
        assert_builders_match(&doc);

        // map conflicts across actors + nested objects + list ops
        let mut doc1 = Automerge::new()
            .with_actor("aaaaaa".try_into().unwrap())
            .unwrap();
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
        tx.insert(&list, 0, 1).unwrap();
        tx.insert(&list, 1, 2).unwrap();
        tx.insert(&list, 2, 3).unwrap();
        let map = tx.put_object(ROOT, "map", ObjType::Map).unwrap();
        tx.put(&map, "k", "v").unwrap();
        tx.commit();
        let mut doc2 = doc1
            .fork()
            .with_actor("bbbbbb".try_into().unwrap())
            .unwrap();
        let mut tx = doc2.transaction();
        tx.put(&map, "k", "w").unwrap();
        tx.put(&list, 1, 20).unwrap();
        tx.commit();
        let mut tx = doc1.transaction();
        tx.put(&map, "k", "x").unwrap();
        tx.delete(&list, 0).unwrap();
        tx.commit();
        doc1.merge(&mut doc2).unwrap();
        assert_builders_match(&doc1);

        // counter in a list with concurrent increments
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let l = tx.put_object(ROOT, "l", ObjType::List).unwrap();
        tx.insert(&l, 0, crate::ScalarValue::Counter(0.into()))
            .unwrap();
        tx.increment(&l, 0, 7).unwrap();
        tx.commit();
        assert_builders_match(&doc);
    }
}

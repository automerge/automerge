use crate::iter::tools::Shiftable;
use crate::op_set2::meta::ValueType;
use crate::op_set2::op_set::OpIdIter;
use crate::op_set2::op_set::{MarkIdx, MarkIndexBuilder, MarkIndexColumn};
use crate::op_set2::types::{Action, ScalarValue};
use crate::op_set2::MarkData;
use crate::op_set2::ValueMeta;
use crate::op_set2::{ChangeOp, Op, OpBuilder, OpSet, ReadOpError};
use crate::types::{ObjId, ObjType, OpId, SequenceType, TextEncoding};
use hexane::encoder::{BoolEncoder, RleEncoder};
use hexane::EncoderApi;
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

impl IndexBuilder {
    pub(crate) fn new(_op_set: &OpSet, encoding: TextEncoding) -> Self {
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

    /// Build the index by walking columns directly instead of materializing
    /// every op.
    ///
    /// Per op this touches only the action, succ-count and value-meta
    /// columns, advanced run-at-a-time. Register boundaries come from run
    /// lengths: the key_str column for maps (each run is one key's
    /// register) and the insert column for sequences (a register is one
    /// `true` followed by zero or more `false`s). Uniform runs of
    /// single-op registers stream straight to the encoders; everything
    /// else goes through the same per-register buffer as the op-at-a-time
    /// path. Rare ops (marks, object creation, increments, counters) are
    /// materialized individually.
    pub(crate) fn process_columns(&mut self, op_set: &OpSet) -> Result<(), ReadOpError> {
        let mut iter = IndexIter::new(op_set);
        let mut bounds = BoundaryIter::new(op_set);
        let mut op_iter = op_set.iter();
        let mut succ_id_iter =
            OpIdIter::new(op_set.cols.succ_actor.iter(), op_set.cols.succ_ctr.iter());
        let objrep = self.text_encoding.width("\u{fffc}") as u32;

        for (obj, range) in op_set.iter_obj_ids() {
            let seq = matches!(
                self.obj_info.object_type(&obj),
                Some(ObjType::List) | Some(ObjType::Text)
            );

            iter.shift(range.clone());
            bounds.shift(seq, range.clone());

            let mut pos = range.start;
            while let Some((group_len, repeat)) = bounds.next_batch() {
                if group_len == 1 && repeat > 1 {
                    // a stretch of single-op registers: every op is its own
                    // group, so uniform runs bypass the buffer entirely
                    let mut remaining = repeat;
                    while remaining > 0 {
                        let run = iter
                            .next_run(remaining)
                            .ok_or(ReadOpError::MissingValue("index columns"))?;
                        remaining -= run.count;
                        if self.is_rare(&run) {
                            self.rare_run(&run, pos, &mut op_iter, &mut succ_id_iter, true)?;
                        } else {
                            self.stream_singletons(&run, objrep, pos, &mut op_iter)?;
                        }
                        pos += run.count;
                    }
                } else {
                    for _ in 0..repeat {
                        let mut remaining = group_len;
                        while remaining > 0 {
                            let run = iter
                                .next_run(remaining)
                                .ok_or(ReadOpError::MissingValue("index columns"))?;
                            remaining -= run.count;
                            if self.is_rare(&run) {
                                self.rare_run(&run, pos, &mut op_iter, &mut succ_id_iter, false)?;
                            } else {
                                self.buffer_run(&run, objrep, pos, &mut op_iter)?;
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

    /// Stream a uniform run of single-op registers straight to the
    /// encoders — no per-op work at all in the common case.
    fn stream_singletons(
        &mut self,
        run: &IndexRun,
        objrep: u32,
        pos: usize,
        op_iter: &mut super::OpIter<'_>,
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
            op_iter.shift(pos..pos + run.count);
            for _ in 0..run.count {
                let op = op_iter.try_next()?.ok_or(ReadOpError::MissingValue("op"))?;
                self.text
                    .append(Some(op.width(SequenceType::Text, self.text_encoding) as u32));
            }
        }
        Ok(())
    }

    /// Append a uniform run to the current register's buffer.
    fn buffer_run(
        &mut self,
        run: &IndexRun,
        objrep: u32,
        pos: usize,
        op_iter: &mut super::OpIter<'_>,
    ) -> Result<(), ReadOpError> {
        if run.meta.type_code() == ValueType::String && run.meta.length() > 1 {
            op_iter.shift(pos..pos + run.count);
            for _ in 0..run.count {
                let op = op_iter.try_next()?.ok_or(ReadOpError::MissingValue("op"))?;
                self.group.push(GroupOp {
                    succ: run.succ,
                    width: op.width(SequenceType::Text, self.text_encoding) as u32,
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

    /// Materialize each op of a rare run and feed it through the same
    /// per-op path the op-at-a-time builder uses. With `singletons` each
    /// op is its own register and is flushed immediately.
    fn rare_run(
        &mut self,
        run: &IndexRun,
        pos: usize,
        op_iter: &mut super::OpIter<'_>,
        succ_id_iter: &mut OpIdIter<'_>,
        singletons: bool,
    ) -> Result<(), ReadOpError> {
        op_iter.shift(pos..pos + run.count);
        for i in 0..run.count {
            let op = op_iter.try_next()?.ok_or(ReadOpError::MissingValue("op"))?;
            let is_counter = matches!(op.value, ScalarValue::Counter(_));
            self.process_op(&op);
            if run.succ > 0 {
                if is_counter {
                    let start = run.succ_prefix as usize + i * run.succ as usize;
                    let end = start + run.succ as usize;
                    let mut next_id = succ_id_iter.shift_next(start..end);
                    while let Some(id) = next_id {
                        self.process_succ(true, id);
                        next_id = succ_id_iter.next();
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
    /// Debug-only drift guard: the column-walking builder must produce
    /// exactly the same indexes as the op-at-a-time builder.
    #[cfg(any(debug_assertions, test))]
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
/// maps (each run is one key's register — hexane guarantees adjacent runs
/// never carry equal values), insert-run structure for sequences (a
/// register is one `true` followed by zero or more `false`s).
#[derive(Debug)]
struct BoundaryIter<'a> {
    seq: bool,
    inserts: hexane::Iter<'a, bool>,
    keys: hexane::Iter<'a, Option<String>>,
    /// pending trues from the current insert run; all but the last are
    /// singleton registers, the last stays open for its trailing falses
    ones: usize,
}

impl<'a> BoundaryIter<'a> {
    fn new(op_set: &'a OpSet) -> Self {
        Self {
            seq: false,
            inserts: op_set.cols.insert.values().iter(),
            keys: op_set.key_str_iter(),
            ones: 0,
        }
    }

    fn shift(&mut self, seq: bool, range: std::ops::Range<usize>) {
        self.seq = seq;
        self.ones = 0;
        if seq {
            self.inserts.shift(range);
        } else {
            self.keys.shift(range);
        }
    }

    /// The next batch of registers as `(len, repeat)`: `repeat` consecutive
    /// registers of `len` ops each. `repeat > 1` only for single-op
    /// registers (a run of inserts), which is the batch the streaming fast
    /// path feeds on.
    fn next_batch(&mut self) -> Option<(usize, usize)> {
        if self.seq {
            self.next_seq()
        } else {
            self.keys.next_run().map(|r| (r.count, 1))
        }
    }

    fn next_seq(&mut self) -> Option<(usize, usize)> {
        loop {
            // more than one pending true: all but the last are singleton
            // registers, the last stays open for its trailing falses
            if self.ones > 1 {
                let repeat = self.ones - 1;
                self.ones = 1;
                return Some((1, repeat));
            }
            match self.inserts.next_run() {
                Some(run) if run.value => self.ones += run.count,
                Some(run) => {
                    // falses close the one pending true (if any); `ones`
                    // is 0 only for a defensive headless run
                    let result = self.ones + run.count;
                    self.ones = 0;
                    return Some((result, 1));
                }
                None if self.ones > 0 => {
                    self.ones = 0;
                    return Some((1, 1));
                }
                None => return None,
            }
        }
    }
}

/// The per-op columns consumed by [`IndexBuilder::process_columns`] —
/// action, value *meta* and succ count — advanced run-at-a-time. The raw
/// value bytes are never touched here: rare ops that need them are
/// materialized individually by the consumer.
#[derive(Debug)]
pub(crate) struct IndexIter<'a> {
    action: hexane::Iter<'a, Action>,
    meta: hexane::PrefixIter<'a, ValueMeta>,
    succ: hexane::PrefixIter<'a, u32>,
    action_head: Option<(Action, usize)>,
    meta_head: Option<(ValueMeta, usize)>,
    succ_head: Option<(u32, u64, usize)>,
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
}

impl<'a> IndexIter<'a> {
    fn new(op_set: &'a OpSet) -> Self {
        Self {
            action: op_set.cols.action.iter(),
            meta: op_set.cols.value_meta.iter(),
            succ: op_set.cols.succ_count.iter(),
            action_head: None,
            meta_head: None,
            succ_head: None,
        }
    }

    fn shift(&mut self, range: std::ops::Range<usize>) {
        self.action.shift(range.clone());
        self.meta.shift(range.clone());
        self.succ.shift(range);
        self.action_head = None;
        self.meta_head = None;
        self.succ_head = None;
    }

    /// The next run of at most `max` ops sharing action, meta and succ.
    fn next_run(&mut self, max: usize) -> Option<IndexRun> {
        if self.action_head.is_none() {
            let run = self.action.next_run()?;
            self.action_head = Some((run.value, run.count));
        }
        if self.meta_head.is_none() {
            let run = self.meta.next_run()?;
            self.meta_head = Some((run.value.value, run.count));
        }
        if self.succ_head.is_none() {
            let run = self.succ.next_run()?;
            // total is the inclusive prefix over the whole run
            let end = run.value.total();
            let start = end - (run.value.value as u64 * run.count as u64);
            self.succ_head = Some((run.value.value, start, run.count));
        }
        let (action, a_left) = self.action_head.unwrap();
        let (meta, m_left) = self.meta_head.unwrap();
        let (succ, s_prefix, s_left) = self.succ_head.unwrap();

        let count = max.min(a_left).min(m_left).min(s_left);
        debug_assert!(count > 0);

        self.action_head = (a_left > count).then_some((action, a_left - count));
        self.meta_head = (m_left > count).then_some((meta, m_left - count));
        self.succ_head = (s_left > count).then_some((
            succ,
            s_prefix + succ as u64 * count as u64,
            s_left - count,
        ));

        Some(IndexRun {
            count,
            action,
            meta,
            succ,
            succ_prefix: s_prefix,
        })
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
        by_cols.process_columns(op_set).unwrap();
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

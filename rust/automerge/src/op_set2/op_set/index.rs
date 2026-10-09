use crate::actor::{ActorRefs, ActorRemoval, ActorShift, HasActorIndices};
use crate::iter::tools::Shiftable;
use crate::op_set2::meta::ValueType;
use crate::op_set2::op_set::OpIdIter;
use crate::op_set2::op_set::{MarkIdx, MarkIndexBuilder, MarkIndexColumn};
use crate::op_set2::types::{Action, ScalarValue};
use crate::op_set2::MarkData;
use crate::op_set2::OpSet;
use crate::op_set2::ValueMeta;
use crate::op_set2::{ChangeOp, Op, OpBuilder, ReadOpError};
#[cfg(test)]
use crate::types::SequenceType;
use crate::types::{ElemId, ObjId, ObjType, OpId, TextEncoding};
use hexane::encoder::{BoolEncoder, RleEncoder};
use hexane::{EncoderApi, RunSrc};
use std::collections::HashMap;

/// Streaming index builder: ops are buffered one register at a time and
/// flushed to run-length encoders at each register boundary.
pub(crate) struct IndexBuilder {
    /// counter succ id -> [(absolute inc index, absolute op index)]
    counters: HashMap<OpId, Vec<(usize, usize)>>,
    /// the current, unflushed register
    group: Vec<GroupOp>,
    group_incs: Vec<Option<i64>>,
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
    split_by_elem: bool,
    /// objects the input creates itself, whose registers are always
    /// insert-bounded; a `Make` row always precedes its object's rows
    own_objs: rustc_hash::FxHashSet<ObjId>,
}

#[derive(Debug, Clone, Copy)]
struct GroupOp {
    /// see [`vis_succ`]; decremented as a counter's increments apply
    succ: u32,
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

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct ObjIndex(pub(crate) ActorRefs<HashMap<OpId, ObjInfo>>);

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

impl HasActorIndices for ObjInfo {
    fn shifted(self, shift: &ActorShift) -> Self {
        Self {
            parent: self.parent.shifted(shift),
            obj_type: self.obj_type,
        }
    }

    fn removed(self, removal: &ActorRemoval) -> Option<Self> {
        Some(Self {
            parent: self.parent.removed(removal)?,
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

/// Object ranges from the obj_actor / obj_ctr run streams. Errors if an id
/// is half null or the ids are not strictly increasing.
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

/// Forward-only access to the columns needed to materialize rare ops.
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
            split_by_elem: false,
            own_objs: rustc_hash::FxHashSet::default(),
        }
    }

    /// Bound sequence registers by element identity rather than by the
    /// insert column alone. Needed for a fragment with deps.
    pub(crate) fn split_by_elem(&mut self) {
        self.split_by_elem = true;
    }

    /// Seed the object-type index with objects from outside the columns
    /// being processed.
    pub(crate) fn seed_obj_info(&mut self, info: &ObjIndex) {
        self.obj_info = info.clone();
    }

    fn ops_flushed(&self) -> usize {
        self.visible.len()
    }

    /// Ops processed so far, flushed or buffered.
    pub(crate) fn ops_len(&self) -> usize {
        self.ops_flushed() + self.group.len()
    }

    fn incs_flushed(&self) -> usize {
        self.inc.len()
    }

    /// Close the current register.
    pub(crate) fn flush(&mut self) {
        if self.group.is_empty() {
            debug_assert!(self.group_incs.is_empty());
            return;
        }
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
        // successors share their target's register, so leftovers can never match
        self.counters.clear();
    }

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
        self.push_mark(mark_index);
        self.apply_increment(id, inc_value);

        if let Some(obj_info) = obj_info {
            self.obj_info.insert(id, obj_info);
            if self.split_by_elem {
                self.own_objs.insert(ObjId(id));
            }
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
        self.push_mark(mark_index);
        self.apply_increment(op.id, op.get_increment_value());

        if let Some(obj_info) = op.obj_info() {
            self.obj_info.insert(op.id, obj_info);
        }

        self.group.push(GroupOp {
            succ: vis_num(op),
            width: op.width(SequenceType::Text, self.text_encoding) as u32,
        });
    }

    fn push_mark(&mut self, mark_index: Option<MarkIndexBuilder>) {
        self.group_marks.push(match mark_index {
            Some(MarkIndexBuilder::Start(id, mark)) => {
                self.mark_cache.insert(id, mark);
                Some(MarkIdx::Start(id))
            }
            Some(MarkIndexBuilder::End(id)) => Some(MarkIdx::End(id)),
            None => None,
        });
    }

    fn apply_increment(&mut self, id: OpId, inc_value: Option<i64>) {
        let count = self.counters.remove(&id);
        if let Some(i) = inc_value {
            let incs_flushed = self.incs_flushed();
            let ops_flushed = self.ops_flushed();
            for (inc_idx, op_idx) in count.into_iter().flatten() {
                // a counter and its increments share a register
                self.group_incs[inc_idx - incs_flushed] = Some(i);
                self.group[op_idx - ops_flushed].succ -= 1;
            }
        }
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

    /// Build the index from a loaded op set's columns.
    pub(crate) fn process_op_set(&mut self, op_set: &OpSet) -> Result<(), ReadOpError> {
        let rare = RareOps::new(
            OpIdIter::new(op_set.cols.id_actor.iter(), op_set.cols.id_ctr.iter()),
            super::MarkInfoIter::new(op_set.cols.mark_name.iter(), op_set.cols.expand.iter()),
            OpIdIter::new(op_set.cols.succ_actor.iter(), op_set.cols.succ_ctr.iter()),
            op_set.cols.value.iter(),
        );
        let elem = self.split_by_elem.then(|| ElemBounds {
            elems: super::op_iter::ElemIdIter::new(
                op_set.cols.key_actor.iter(),
                op_set.cols.key_ctr.iter(),
            ),
            ids: OpIdIter::new(op_set.cols.id_actor.iter(), op_set.cols.id_ctr.iter()),
            ids_pos: 0,
            span: None,
            peeked: None,
        });
        self.process_columns(
            ObjRunWalk::new(op_set.cols.obj_actor.iter(), op_set.cols.obj_ctr.iter()),
            op_set.cols.action.iter(),
            op_set.cols.value_meta.values().iter(),
            op_set.cols.succ_count.values().iter(),
            op_set.cols.insert.values().iter(),
            op_set.cols.key_str.iter(),
            rare,
            elem,
        )
    }

    /// Build the index by walking column run streams. Only rare ops
    /// (marks, object creation, increments, counters) are materialized.
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
        elem: Option<ElemBounds<'_>>,
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
        let mut bounds = BoundaryIter::new(inserts, keys, elem);
        let mut pos = 0;

        while let Some((obj, obj_len)) = objs.try_next_obj()? {
            let seq = matches!(
                self.obj_info.object_type(&obj),
                Some(ObjType::List) | Some(ObjType::Text)
            );
            let split = self.split_by_elem && !self.own_objs.contains(&obj);
            bounds.start_obj(seq, obj_len, split)?;

            while let Some((group_len, repeat)) = bounds.next_batch()? {
                if group_len == 1 && repeat > 1 {
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

    /// The text width of the `i`th op of a non-mark run; mirrors `Op::width`.
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

    fn stream_singletons(
        &mut self,
        run: &IndexRun,
        objrep: u32,
        rare: &mut RareOps<'_>,
    ) -> Result<(), ReadOpError> {
        debug_assert!(self.group.is_empty());
        let vis = vis_succ(run) == 0;
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
                    succ: vis_succ(run),
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
                    succ: vis_succ(run),
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

    /// With `singletons`, each op is its own register.
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
            let succ_vis = vis_succ(run);

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

/// Element identity and op id at a row, for splitting sequence registers
/// in a fragment with deps: such a fragment can update an element it does
/// not contain, which the insert column alone would attach to the
/// register before it.
pub(crate) struct ElemBounds<'a> {
    elems: super::op_iter::ElemIdIter<'a>,
    ids: OpIdIter<'a>,
    /// row after the last one `ids` read
    ids_pos: usize,
    /// the span [`elem_run`](Self::elem_run) last measured, which the
    /// next call may ask for again
    span: Option<(usize, Option<ElemId>, usize)>,
    /// the row that ended the last span
    peeked: Option<(usize, Option<ElemId>)>,
}

impl ElemBounds<'_> {
    fn id_at(&mut self, pos: usize) -> Result<OpId, ReadOpError> {
        let skip = pos
            .checked_sub(self.ids_pos)
            .expect("ElemBounds::id_at went backwards");
        self.ids_pos = pos + 1;
        self.ids
            .maybe_try_nth(skip)?
            .ok_or(ReadOpError::MissingValue("id"))
    }

    /// The run of rows from `start` sharing one element id, capped at
    /// `max`: the id and the run's length.
    fn elem_run(
        &mut self,
        start: usize,
        max: usize,
    ) -> Result<(Option<ElemId>, usize), ReadOpError> {
        if let Some((s, elem, n)) = self.span {
            if s == start {
                return Ok((elem, n.min(max)));
            }
        }
        let first = match self.peeked.take() {
            Some((p, v)) if p == start => v,
            _ => self.elems.at(start)?,
        };
        let mut n = 1;
        while n < max {
            let next = self.elems.at(start + n)?;
            if next != first {
                self.peeked = Some((start + n, next));
                break;
            }
            n += 1;
        }
        self.span = Some((start, first, n));
        Ok((first, n))
    }
}

/// Yields the register lengths of the current object: key_str runs for
/// maps (runs are canonical, so each is one key), insert-run structure for
/// sequences, refined by [`ElemBounds`] when present.
struct BoundaryIter<'a, I, K> {
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
    /// rows pulled from the insert stream so far
    abs: usize,
    elem: Option<ElemBounds<'a>>,
    split: bool,
    /// non-insert rows of the current run not yet assigned to a register
    /// (split objects only)
    falses: usize,
    false_start: usize,
}

impl<'e, I, K> BoundaryIter<'e, I, K> {
    fn new(inserts: I, keys: K, elem: Option<ElemBounds<'e>>) -> Self {
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
            abs: 0,
            elem,
            split: false,
            falses: 0,
            false_start: 0,
        }
    }

    fn start_obj(&mut self, seq: bool, len: usize, split: bool) -> Result<(), ReadOpError> {
        debug_assert_eq!(self.remaining, 0);
        debug_assert_eq!(self.ones, 0);
        debug_assert_eq!(self.falses, 0);
        self.seq = seq;
        self.split = seq && split && self.elem.is_some();
        self.remaining = len;
        if seq {
            self.keys_owed += len;
        } else {
            self.insert_owed += len;
        }
        Ok(())
    }

    /// The next insert run, clipped to the current object.
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
            self.abs += take;
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
        self.abs += take;
        if left > take {
            self.insert_head = Some((value, left - take));
        }
        Ok(Some((value, take)))
    }

    /// The next key run length, clipped to the current object.
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
    /// registers of `len` ops each. `repeat > 1` only when `len == 1`.
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
            if self.ones > 1 {
                let repeat = self.ones - 1;
                self.ones = 1;
                return Ok(Some((1, repeat)));
            }
            if self.falses > 0 {
                return self.next_elem_span().map(Some);
            }
            match self.next_insert_run()? {
                Some((true, count)) => self.ones += count,
                Some((false, count)) => {
                    if self.split {
                        self.false_start = self.abs - count;
                        self.falses = count;
                        continue;
                    }
                    // `ones` is 0 only for a headless run
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

    /// The next register out of the pending non-insert rows. Only the
    /// run's first span can continue the insert before it, and only if it
    /// names that insert.
    fn next_elem_span(&mut self) -> Result<(usize, usize), ReadOpError> {
        let start = self.false_start;
        let bounds = self.elem.as_mut().expect("split object without elem ids");
        let (elem, n) = bounds.elem_run(start, self.falses)?;
        if self.ones == 1 {
            let ins = bounds.id_at(start - 1)?;
            self.ones = 0;
            if elem != Some(ElemId(ins)) {
                // the insert is alone; this span is asked for again next call
                return Ok((1, 1));
            }
            self.falses -= n;
            self.false_start += n;
            return Ok((1 + n, 1));
        }
        self.falses -= n;
        self.false_start += n;
        Ok((n, 1))
    }
}

/// The action, value meta and succ count streams, advanced run-at-a-time.
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

/// The successor count a row's visibility is judged by; `u32::MAX` means
/// never visible. A fragment's delete rows are dropped by the merge, so
/// they must never take `top`.
fn vis_succ(run: &IndexRun) -> u32 {
    match run.action {
        Action::Increment | Action::Delete => u32::MAX,
        _ => run.succ,
    }
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

/// Compress an iterator into [`hexane::Run`]s.
pub(crate) fn runs<T: PartialEq>(
    iter: impl Iterator<Item = T>,
) -> impl Iterator<Item = hexane::Run<T>> {
    let mut iter = iter.peekable();
    std::iter::from_fn(move || {
        let value = iter.next()?;
        let mut count = 1;
        while iter.next_if(|v| *v == value).is_some() {
            count += 1;
        }
        Some(hexane::Run { count, value })
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
    use crate::automerge::Automerge;
    use crate::op_set2::change::IndexedChangeCollector;
    use crate::tx::Transactable;
    use crate::{ObjType, ROOT};

    fn assert_builders_match<H: crate::hash_retention::HashRetention>(doc: &Automerge<H>) {
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

        let mut doc1 = Automerge::new().with_actor("aaaaaa".try_into().unwrap());
        let mut tx = doc1.transaction();
        let list = tx.put_object(ROOT, "list", ObjType::List).unwrap();
        tx.insert(&list, 0, 1).unwrap();
        tx.insert(&list, 1, 2).unwrap();
        tx.insert(&list, 2, 3).unwrap();
        let map = tx.put_object(ROOT, "map", ObjType::Map).unwrap();
        tx.put(&map, "k", "v").unwrap();
        tx.commit();
        let mut doc2 = doc1.fork().with_actor("bbbbbb".try_into().unwrap());
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

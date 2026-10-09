use std::ops::Range;

use super::OpSet;
use crate::op_set2::change::ActorMapper;
use crate::op_set2::columns::Columns;
use crate::op_set2::meta::ValueMeta;
use crate::op_set2::types::Action;
use crate::op_set2::types::ActorIdx;
use crate::storage::change_set::ChangeSetOpWriter;

impl OpSet {
    pub(crate) fn write_all_change_set_ops<'a>(
        &'a self,
        writer: &mut ChangeSetOpWriter<'a>,
        mapper: &mut ActorMapper<'_>,
    ) {
        RowCopier::new(self).copy(0..self.len(), writer, mapper);
    }
}

/// Copies ascending row ranges into a change set, run by run, with one
/// forward cursor per column.
pub(crate) struct RowCopier<'a> {
    cols: &'a Columns,
    id_actor: hexane::Iter<'a, ActorIdx>,
    id_ctr: hexane::DeltaIter<'a, u32>,
    obj_actor: hexane::Iter<'a, Option<ActorIdx>>,
    obj_ctr: hexane::Iter<'a, Option<u32>>,
    key_actor: hexane::Iter<'a, Option<ActorIdx>>,
    key_ctr: hexane::DeltaIter<'a, Option<u32>>,
    key_str: hexane::Iter<'a, Option<String>>,
    insert: hexane::PrefixIter<'a, bool>,
    action: hexane::Iter<'a, Action>,
    value_meta: hexane::PrefixIter<'a, ValueMeta>,
    succ_count: hexane::PrefixIter<'a, u32>,
    succ_actor: hexane::Iter<'a, ActorIdx>,
    succ_ctr: hexane::DeltaIter<'a, u32>,
    expand: hexane::Iter<'a, bool>,
    mark_name: hexane::Iter<'a, Option<String>>,
}

impl<'a> RowCopier<'a> {
    pub(crate) fn new(op_set: &'a OpSet) -> Self {
        let c = &op_set.cols;
        RowCopier {
            cols: c,
            id_actor: c.id_actor.stored_iter(),
            id_ctr: c.id_ctr.iter(),
            obj_actor: c.obj_actor.stored_iter(),
            obj_ctr: c.obj_ctr.iter(),
            key_actor: c.key_actor.stored_iter(),
            key_ctr: c.key_ctr.iter(),
            key_str: c.key_str.iter(),
            insert: c.insert.iter(),
            action: c.action.iter(),
            value_meta: c.value_meta.iter(),
            succ_count: c.succ_count.iter(),
            succ_actor: c.succ_actor.stored_iter(),
            succ_ctr: c.succ_ctr.iter(),
            expand: c.expand.iter(),
            mark_name: c.mark_name.iter(),
        }
    }

    /// The rows' successors and referenced ops must all be in the change
    /// set, and none may take a pred from outside it. Successive calls
    /// must name ascending rows.
    pub(crate) fn copy(
        &mut self,
        rows: Range<usize>,
        w: &mut ChangeSetOpWriter<'a>,
        mapper: &mut ActorMapper<'_>,
    ) {
        debug_assert!(rows.start >= self.id_actor.pos(), "row ranges must ascend");
        let c = self.cols;
        let n = rows.len();

        self.obj_actor.shift(rows.clone());
        while let Some(run) = self.obj_actor.next_run() {
            let v = c.obj_actor.log_val(run.value);
            if let Some(a) = v {
                mapper.process_actor(usize::from(a));
            }
            w.obj_actor.append_n(v, run.count);
        }
        self.obj_ctr.shift(rows.clone());
        while let Some(run) = self.obj_ctr.next_run() {
            w.obj_ctr.append_n(run.value.map(u64::from), run.count);
        }
        self.key_actor.shift(rows.clone());
        while let Some(run) = self.key_actor.next_run() {
            let v = c.key_actor.log_val(run.value);
            if let Some(a) = v {
                mapper.process_actor(usize::from(a));
            }
            w.key_actor.append_n(v, run.count);
        }
        self.key_ctr.shift(rows.clone());
        while let Some(run) = self.key_ctr.next_run() {
            w.key_ctr.append_run(run);
        }
        self.key_str.shift(rows.clone());
        while let Some(run) = self.key_str.next_run() {
            w.key_str.append_n(run.value, run.count);
        }
        self.id_actor.shift(rows.clone());
        while let Some(run) = self.id_actor.next_run() {
            w.id_actor
                .append_n(c.id_actor.log_val(run.value), run.count);
        }
        self.insert.shift(rows.clone());
        while let Some(run) = self.insert.next_run() {
            w.insert.append_n(run.value.value, run.count);
        }
        self.action.shift(rows.clone());
        while let Some(run) = self.action.next_run() {
            w.action.append_n(run.value, run.count);
        }
        self.value_meta.shift(rows.clone());
        let start = self.value_meta.prefix() as usize;
        while let Some(run) = self.value_meta.next_run() {
            w.value_meta.append_n(run.value.value, run.count);
        }
        let end = self.value_meta.prefix() as usize;
        c.value.extend_range_to(start..end, &mut w.value);
        w.pred_count.append_n(0, n);
        self.succ_count.shift(rows.clone());
        let start = self.succ_count.prefix() as usize;
        while let Some(run) = self.succ_count.next_run() {
            w.succ_count.append_n(run.value.value, run.count);
        }
        let succ = start..self.succ_count.prefix() as usize;
        self.succ_actor.shift(succ.clone());
        while let Some(run) = self.succ_actor.next_run() {
            w.succ_actor
                .append_n(c.succ_actor.log_val(run.value), run.count);
        }
        self.succ_ctr.shift(succ);
        while let Some(run) = self.succ_ctr.next_run() {
            w.succ_ctr.append_run(run);
        }
        self.expand.shift(rows.clone());
        while let Some(run) = self.expand.next_run() {
            w.expand.append_n(run.value, run.count);
        }
        self.mark_name.shift(rows.clone());
        while let Some(run) = self.mark_name.next_run() {
            w.mark_name.append_n(run.value, run.count);
        }
        self.id_ctr.shift(rows);
        while let Some(run) = self.id_ctr.next_run() {
            w.id_ctr.append_run(run);
        }
    }
}

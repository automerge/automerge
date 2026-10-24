use std::ops::Range;

use super::run_head::{Rle, RunHead};
use super::{MappedIter, OpSet, ReadOpError};
use crate::clock::Clock;
use crate::op_set2::change::ActorMapper;
use crate::op_set2::columns::Columns;
use crate::op_set2::meta::ValueMeta;
use crate::op_set2::types::Action;
use crate::op_set2::types::ActorIdx;
use crate::storage::bundle::{BundleOpWriter, BundleOpsColumns};
use crate::types::{ObjId, OpId};

impl OpSet {
    /// Every op as bundle op columns, written straight from the document's
    /// columns. The bundle's actor table must be the document's.
    pub(crate) fn write_bundle_op_columns(&self, data: &mut Vec<u8>) -> BundleOpsColumns {
        let c = &self.cols;
        let empty = data.len()..data.len();
        BundleOpsColumns {
            hint: empty.clone(),
            obj_actor: c.obj_actor.save_to_unless(data, None),
            obj_ctr: c.obj_ctr.save_to_unless(data, None),
            key_actor: c.key_actor.save_to_unless(data, None),
            key_ctr: c.key_ctr.save_to_unless(data, None),
            key_str: c.key_str.save_to_unless(data, None),
            id_actor: c.id_actor.save_to(data),
            insert: c.insert.save_to(data),
            action: c.action.save_to(data),
            value_meta: c.value_meta.save_to(data),
            value: c.value.save_to(data),
            pred_count: empty.clone(),
            pred_actor: empty.clone(),
            pred_ctr: empty,
            succ_count: c.succ_count.save_to(data),
            succ_actor: c.succ_actor.save_to(data),
            succ_ctr: c.succ_ctr.save_to(data),
            expand: c.expand.save_to_unless(data, false),
            mark_name: c.mark_name.save_to_unless(data, None),
            id_ctr: c.id_ctr.save_to(data),
        }
    }

    pub(crate) fn write_all_bundle_ops<'a>(
        &'a self,
        writer: &mut BundleOpWriter<'a>,
        mapper: &mut ActorMapper<'_>,
    ) {
        RowCopier::new(self).copy(0..self.len(), writer, mapper);
    }
}

/// Copies ascending row ranges into a bundle, run by run, with one
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

    /// The rows' successors and referenced ops must all be in the bundle,
    /// and none may take a pred from outside it. Successive calls must name
    /// ascending rows.
    pub(crate) fn copy(
        &mut self,
        rows: Range<usize>,
        w: &mut BundleOpWriter<'a>,
        mapper: &mut ActorMapper<'_>,
    ) {
        self.copy_with(rows, w, mapper, None::<fn(OpId) -> bool>);
    }

    /// [`Self::copy`] for rows some of whose successors are not in the
    /// bundle: only those `keep` accepts are written.
    pub(crate) fn copy_keeping(
        &mut self,
        rows: Range<usize>,
        w: &mut BundleOpWriter<'a>,
        mapper: &mut ActorMapper<'_>,
        keep: impl FnMut(OpId) -> bool,
    ) {
        self.copy_with(rows, w, mapper, Some(keep));
    }

    fn copy_with(
        &mut self,
        rows: Range<usize>,
        w: &mut BundleOpWriter<'a>,
        mapper: &mut ActorMapper<'_>,
        keep: Option<impl FnMut(OpId) -> bool>,
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
        match keep {
            None => {
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
            }
            Some(mut keep) => {
                let end = c.succ_count.get_prefix(rows.end) as usize;
                self.succ_actor.shift(start..end);
                self.succ_ctr.shift(start..end);
                while let Some(run) = self.succ_count.next_run() {
                    for _ in 0..run.count {
                        let mut kept = 0;
                        for _ in 0..run.value.value {
                            let actor = c.succ_actor.log_val(self.succ_actor.next().unwrap());
                            let ctr = self.succ_ctr.next().unwrap();
                            let id = OpId::new(ctr as u64, usize::from(actor));
                            if keep(id) {
                                w.succ_actor.append(actor);
                                w.succ_ctr.append(ctr as i64);
                                kept += 1;
                            }
                        }
                        w.succ_count.append(kept);
                    }
                }
            }
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

/// `actor` with counters `first, first + step, …`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Steps {
    pub(crate) actor: usize,
    pub(crate) first: u64,
    pub(crate) step: i64,
}

impl Steps {
    pub(crate) fn nth(&self, i: usize) -> OpId {
        let ctr = self.first as i64 + self.step * i as i64;
        OpId::new(ctr as u64, self.actor)
    }
}

/// Consecutive rows of one object sharing an id actor, a key element
/// actor, an insert flag and a successor count, whose id and key element
/// counters each step evenly.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RowRun {
    pub(crate) count: usize,
    pub(crate) obj: ObjId,
    pub(crate) insert: bool,
    pub(crate) id: Steps,
    /// `None` for a map key or the list head
    pub(crate) key: Option<Steps>,
    pub(crate) succ: u32,
}

type ActorHead<'a, T> = RunHead<Rle<'a, T, MappedIter<'a, T>>>;

/// [`RowRun`]s from a starting row. Reads only the object, insert, id, key
/// element and successor columns.
pub(crate) struct RowRuns<'a> {
    obj_actor: ActorHead<'a, Option<ActorIdx>>,
    obj_ctr: RunHead<Rle<'a, Option<u32>, hexane::Iter<'a, Option<u32>>>>,
    insert: RunHead<Rle<'a, bool, hexane::Iter<'a, bool>>>,
    id_actor: ActorHead<'a, ActorIdx>,
    id_ctr: RunHead<hexane::DeltaIter<'a, u32>>,
    key_actor: ActorHead<'a, Option<ActorIdx>>,
    key_ctr: RunHead<hexane::DeltaIter<'a, Option<u32>>>,
    succ_count: RunHead<Rle<'a, u32, hexane::Iter<'a, u32>>>,
    succ: SuccRuns<'a>,
}

/// The successor ids from some row on, a run of one actor and evenly
/// stepping counters at a time.
#[derive(Clone)]
pub(crate) struct SuccRuns<'a> {
    actor: ActorHead<'a, ActorIdx>,
    ctr: RunHead<hexane::DeltaIter<'a, u32>>,
}

impl OpSet {
    pub(crate) fn row_runs(&self, start: usize) -> RowRuns<'_> {
        let c = &self.cols;
        let rows = start..self.len();
        let entries = c.succ_count.get_prefix(start) as usize..c.succ_actor.len();
        RowRuns {
            obj_actor: RunHead::rle(c.obj_actor.iter_range(rows.clone()), "obj_actor"),
            obj_ctr: RunHead::rle(c.obj_ctr.iter_range(rows.clone()), "obj_ctr"),
            insert: RunHead::rle(c.insert.values().iter_range(rows.clone()), "insert"),
            id_actor: RunHead::rle(c.id_actor.iter_range(rows.clone()), "id_actor"),
            id_ctr: RunHead::new(c.id_ctr.iter_range(rows.clone()), "id_ctr"),
            key_actor: RunHead::rle(c.key_actor.iter_range(rows.clone()), "key_actor"),
            key_ctr: RunHead::new(c.key_ctr.iter_range(rows.clone()), "key_ctr"),
            succ_count: RunHead::rle(c.succ_count.values().iter_range(rows), "succ_count"),
            succ: SuccRuns {
                actor: RunHead::rle(c.succ_actor.iter_range(entries.clone()), "succ_actor"),
                ctr: RunHead::new(c.succ_ctr.iter_range(entries), "succ_ctr"),
            },
        }
    }
}

fn first_of(run: &hexane::DeltaRun, name: &'static str) -> Result<(u64, i64), ReadOpError> {
    let step = run.delta.ok_or(ReadOpError::MissingValue(name))?;
    let first = run.prefix + step;
    if first < 0 {
        return Err(ReadOpError::InvalidOpId("negative counter".to_string()));
    }
    Ok((first as u64, step))
}

impl<'a> RowRuns<'a> {
    /// The next run, at most `max` rows long; `None` at the last row.
    pub(crate) fn peek(&mut self, max: usize) -> Result<Option<RowRun>, ReadOpError> {
        let Some(id_actor) = self.id_actor.peek()? else {
            return Ok(None);
        };
        let id_ctr = self.id_ctr.peek_some()?;
        let key_actor = self.key_actor.peek_some()?;
        let key_ctr = self.key_ctr.peek_some()?;
        let succ = self.succ_count.peek_some()?;
        let obj_actor = self.obj_actor.peek_some()?;
        let obj_ctr = self.obj_ctr.peek_some()?;
        let insert = self.insert.peek_some()?;
        let obj = ObjId::load(obj_ctr.value.map(u64::from), obj_actor.value)
            .ok_or(ReadOpError::MissingValue("obj"))?;
        let count = max
            .min(obj_actor.count)
            .min(obj_ctr.count)
            .min(insert.count)
            .min(id_actor.count)
            .min(id_ctr.count)
            .min(key_actor.count)
            .min(key_ctr.count)
            .min(succ.count);
        let (first, step) = first_of(&id_ctr, "id_ctr")?;
        let id = Steps {
            actor: usize::from(id_actor.value),
            first,
            step,
        };
        let key = match key_actor.value {
            Some(actor) => {
                let (first, step) = first_of(&key_ctr, "key_ctr")?;
                Some(Steps {
                    actor: usize::from(actor),
                    first,
                    step,
                })
            }
            None => None,
        };
        Ok(Some(RowRun {
            count,
            obj,
            insert: insert.value,
            id,
            key,
            succ: succ.value,
        }))
    }

    /// Consumes `n` rows of the run [`Self::peek`] returned.
    pub(crate) fn consume(&mut self, run: &RowRun, n: usize) -> Result<(), ReadOpError> {
        self.obj_actor.consume(n);
        self.obj_ctr.consume(n);
        self.insert.consume(n);
        self.id_actor.consume(n);
        self.id_ctr.consume(n);
        self.key_actor.consume(n);
        self.key_ctr.consume(n);
        self.succ_count.consume(n);
        self.succ.skip(n * run.succ as usize)
    }

    /// The successors of the rows from the current one on.
    pub(crate) fn succ(&self) -> SuccRuns<'a> {
        self.succ.clone()
    }
}

impl SuccRuns<'_> {
    /// The next run, at most `max` successors long.
    pub(crate) fn next_run(&mut self, max: usize) -> Result<(Steps, usize), ReadOpError> {
        let actor = self.actor.peek_some()?;
        let ctr = self.ctr.peek_some()?;
        let count = max.min(actor.count).min(ctr.count);
        let (first, step) = first_of(&ctr, "succ_ctr")?;
        self.actor.consume(count);
        self.ctr.consume(count);
        let steps = Steps {
            actor: usize::from(actor.value),
            first,
            step,
        };
        Ok((steps, count))
    }

    fn skip(&mut self, n: usize) -> Result<(), ReadOpError> {
        self.actor.skip(n)?;
        self.ctr.skip(n)
    }
}

/// Rows `row..row + count` whose ids step evenly.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IdRun {
    pub(crate) row: usize,
    pub(crate) count: usize,
    id: Steps,
}

impl IdRun {
    /// How many of the first `n` ids `clock` covers.
    pub(crate) fn covered(&self, n: usize, clock: &Clock) -> usize {
        at_most(
            self.id.first,
            self.id.step,
            n,
            u64::from(clock.max_op(self.id.actor)),
        )
    }
}

impl OpSet {
    /// The id runs of `rows`.
    pub(crate) fn id_runs(&self, rows: Range<usize>) -> Result<Vec<IdRun>, ReadOpError> {
        let mut actor = RunHead::rle(self.cols.id_actor.iter_range(rows.clone()), "id_actor");
        let mut ctr = RunHead::new(self.cols.id_ctr.iter_range(rows.clone()), "id_ctr");
        let mut runs = Vec::new();
        let mut row = rows.start;
        while row < rows.end {
            let a = actor.peek_some()?;
            let c = ctr.peek_some()?;
            let count = (rows.end - row).min(a.count).min(c.count);
            let (first, step) = first_of(&c, "id_ctr")?;
            runs.push(IdRun {
                row,
                count,
                id: Steps {
                    actor: usize::from(a.value),
                    first,
                    step,
                },
            });
            actor.consume(count);
            ctr.consume(count);
            row += count;
        }
        Ok(runs)
    }
}

/// How many of `first, first + step, …` (`n` of them) are at most `limit`.
fn at_most(first: u64, step: i64, n: usize, limit: u64) -> usize {
    match step {
        _ if first > limit && step >= 0 => 0,
        0 => n,
        1 => n.min((limit - first) as usize + 1),
        s if s > 0 => n.min(((limit - first) / s as u64) as usize + 1),
        _ if first <= limit => n,
        s => {
            let over = (first - limit).div_ceil(s.unsigned_abs());
            n.saturating_sub(over as usize)
        }
    }
}

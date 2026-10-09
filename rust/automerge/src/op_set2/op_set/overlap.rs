//! Cutting the rows a document already has out of a change set's fragment.
//!
//! A dropped row's surviving successors take the relationship into their
//! pred column, and a delete successor with no row of its own gains one at
//! the end of its register, as `ChangeSetBuilder::flush_deletes` would
//! have written it.

use super::super::columns::Columns;
use super::super::meta::ValueMeta;
use super::super::types::{Action, ActorIdx, ScalarValue};
use super::OpSet;
use crate::clock::Clock;
use crate::storage::change_set::ops as spec;
use crate::storage::columns::compression::Uncompressed;
use crate::storage::{RawColumn, RawColumns};

use std::ops::Range;

impl OpSet {
    /// Cut the rows `clock` covers out of this fragment.
    ///
    /// `raw`/`data` are the change set's op columns, read for their pred
    /// and hint columns; returns those rebuilt for the rows that remain.
    /// `actor_map` maps the change set's actor indexes to the document's.
    pub(crate) fn drop_covered(
        &mut self,
        raw: &RawColumns<Uncompressed>,
        data: &[u8],
        clock: &Clock,
        actor_map: &[usize],
    ) -> PredCols {
        let covered: Vec<u64> = actor_map.iter().map(|&a| clock.max_op(a)).collect();
        let plan = Plan::build(&self.cols, PredSrc::new(raw, data), &covered);
        plan.apply(&mut self.cols)
    }

    /// This fragment's op columns plus `preds`, in change set form for
    /// [`crate::storage::change_set::ManifoldOps`], which tolerates the
    /// appended columns being out of spec order.
    pub(crate) fn export_change_set(&self, preds: PredCols) -> (RawColumns<Uncompressed>, Vec<u8>) {
        let (raw, mut data) = self.cols.export();
        let extra = preds.save_to(&mut data);
        (raw.iter().cloned().chain(extra).collect(), data)
    }
}

/// An op id in the change set's actor space, ordered like
/// [`crate::types::OpId`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct RawId {
    ctr: u64,
    actor: ActorIdx,
}

impl RawId {
    fn covered(&self, covered: &[u64]) -> bool {
        covered[usize::from(self.actor)] >= self.ctr
    }
}

#[derive(Clone, Copy, PartialEq)]
struct Register<'a> {
    obj: (Option<ActorIdx>, Option<u32>),
    key: (Option<ActorIdx>, Option<u32>, Option<&'a str>),
}

/// Dropped rows with their sub-column spans. Runs rather than rows: a
/// skipped member's ops are contiguous.
struct DropRun {
    row: usize,
    len: usize,
    succ: Range<usize>,
    value: Range<usize>,
}

/// A delete whose target is dropped: it gets its own row at the end of
/// its register, naming the target as a pred.
struct NewDelete {
    row: usize,
    id: RawId,
    obj: (Option<ActorIdx>, Option<u32>),
    key: (Option<ActorIdx>, Option<u32>, Option<String>),
}

struct Plan {
    drops: Vec<DropRun>,
    inserts: Vec<NewDelete>,
    preds: PredCols,
    /// false once inserts land at the last run's end: a run must not
    /// span an insert
    open_run: bool,
}

/// Ascending; an insert sorts before a drop at the same row, since it
/// closes the register the dropped row has already left.
enum RowOp<'a> {
    Insert(&'a NewDelete),
    Drop(&'a DropRun),
}

impl RowOp<'_> {
    fn row(&self) -> usize {
        match self {
            RowOp::Insert(n) => n.row,
            RowOp::Drop(d) => d.row,
        }
    }
}

impl Plan {
    fn build(cols: &Columns, mut src: PredSrc<'_>, covered: &[u64]) -> Self {
        let mut id_actor = cols.id_actor.iter();
        let mut id_ctr = cols.id_ctr.iter();
        let mut obj_actor = cols.obj_actor.iter();
        let mut obj_ctr = cols.obj_ctr.iter();
        let mut key_actor = cols.key_actor.iter();
        let mut key_ctr = cols.key_ctr.iter();
        let mut key_str = cols.key_str.iter();
        let mut insert = cols.insert.values().iter();
        let mut succ_count = cols.succ_count.values().iter();
        let mut succ_actor = cols.succ_actor.iter();
        let mut succ_ctr = cols.succ_ctr.iter();
        let mut value_meta = cols.value_meta.values().iter();

        let mut plan = Plan {
            drops: vec![],
            inserts: vec![],
            preds: PredCols::default(),
            open_run: true,
        };
        let mut succ_pos = 0;
        let mut value_pos = 0;
        // (successor, dropped target). Per register: a successor shares
        // its target's register, so any still here at its close have no row
        let mut orphans: Vec<(RawId, RawId)> = vec![];
        let mut register: Option<Register<'_>> = None;

        for row in 0..cols.len() {
            let id = RawId {
                actor: id_actor.next().expect("id actor"),
                ctr: u64::from(id_ctr.next().expect("id ctr")),
            };
            let obj = (
                obj_actor.next().expect("obj actor"),
                obj_ctr.next().expect("obj ctr"),
            );
            let ka = key_actor.next().expect("key actor");
            let kc = key_ctr.next().expect("key ctr");
            let ks = key_str.next().expect("key str");
            let ins = insert.next().expect("insert");
            // an insert's register is the element it creates
            let key = if ins {
                (Some(id.actor), Some(id.ctr as u32), None)
            } else {
                (ka, kc, ks)
            };

            let here = Register { obj, key };
            if register != Some(here) {
                plan.close_register(register, row, &mut orphans);
                register = Some(here);
            }

            let n_succ = succ_count.next().expect("succ count") as usize;
            let n_val = value_meta.next().expect("value meta").length();
            let n_pred = src.next_count();
            let hint = src.next_hint();
            let dropped = id.covered(covered);

            for _ in 0..n_succ {
                let s = RawId {
                    actor: succ_actor.next().expect("succ actor"),
                    ctr: u64::from(succ_ctr.next().expect("succ ctr")),
                };
                if !dropped {
                    debug_assert!(!s.covered(covered), "covered successor of a new op");
                } else if !s.covered(covered) {
                    orphans.push((s, id));
                }
            }

            if dropped {
                for _ in 0..n_pred {
                    src.next_pred();
                }
                plan.push_drop(
                    row,
                    succ_pos..succ_pos + n_succ,
                    value_pos..value_pos + n_val,
                );
            } else {
                let adopted = take_orphans(&mut orphans, id);
                plan.preds.count.append((n_pred + adopted.len()) as u32);
                for _ in 0..n_pred {
                    let p = src.next_pred();
                    plan.preds.push(p);
                }
                for p in adopted {
                    plan.preds.push(p);
                }
                plan.preds.hint.append(hint);
            }
            succ_pos += n_succ;
            value_pos += n_val;
        }
        plan.close_register(register, cols.len(), &mut orphans);
        plan
    }

    fn push_drop(&mut self, row: usize, succ: Range<usize>, value: Range<usize>) {
        if self.open_run {
            if let Some(last) = self.drops.last_mut() {
                if last.row + last.len == row {
                    last.len += 1;
                    last.succ.end = succ.end;
                    last.value.end = value.end;
                    return;
                }
            }
        }
        self.open_run = true;
        self.drops.push(DropRun {
            row,
            len: 1,
            succ,
            value,
        });
    }

    fn close_register(
        &mut self,
        register: Option<Register<'_>>,
        row: usize,
        orphans: &mut Vec<(RawId, RawId)>,
    ) {
        if orphans.is_empty() {
            return;
        }
        let register = register.expect("orphans with no register");
        // one row per delete op, in the order `flush_deletes` writes them
        orphans.sort_unstable();
        for group in orphans.chunk_by(|a, b| a.0 == b.0) {
            self.preds.count.append(group.len() as u32);
            for (_, target) in group {
                self.preds.push(*target);
            }
            self.preds.hint.append(None);
            self.inserts.push(NewDelete {
                row,
                id: group[0].0,
                obj: register.obj,
                key: (
                    register.key.0,
                    register.key.1,
                    register.key.2.map(str::to_owned),
                ),
            });
            self.open_run = false;
        }
        orphans.clear();
    }

    fn apply(self, cols: &mut Columns) -> PredCols {
        let ops = self.row_ops();

        let null = ValueMeta::from(&ScalarValue::Null);

        macro_rules! edit_rows {
            ($col:expr, $insert:expr) => {{
                let value = $insert;
                let mut e = $col.edit();
                for op in &ops {
                    match op {
                        RowOp::Insert(n) => {
                            e.seek(n.row).insert(value(*n));
                        }
                        RowOp::Drop(d) => {
                            e.seek(d.row).delete(d.len);
                        }
                    }
                }
                e.finish();
            }};
        }

        edit_rows!(cols.id_actor.identity_mut(), |n: &NewDelete| n.id.actor);
        edit_rows!(cols.id_ctr, |n: &NewDelete| n.id.ctr as u32);
        edit_rows!(cols.obj_actor.identity_mut(), |n: &NewDelete| n.obj.0);
        edit_rows!(cols.obj_ctr, |n: &NewDelete| n.obj.1);
        edit_rows!(cols.key_actor.identity_mut(), |n: &NewDelete| n.key.0);
        edit_rows!(cols.key_ctr, |n: &NewDelete| n.key.1);
        edit_rows!(cols.key_str, key_str);
        edit_rows!(cols.insert, |_: &NewDelete| false);
        edit_rows!(cols.action, |_: &NewDelete| Action::Delete);
        edit_rows!(cols.value_meta, |_: &NewDelete| null);
        edit_rows!(cols.mark_name, |_: &NewDelete| None::<&str>);
        edit_rows!(cols.expand, |_: &NewDelete| false);
        edit_rows!(cols.succ_count, |_: &NewDelete| 0);

        macro_rules! delete_subs {
            ($col:expr) => {{
                let mut e = $col.edit();
                for d in &self.drops {
                    e.seek(d.succ.start).delete(d.succ.len());
                }
                e.finish();
            }};
        }
        delete_subs!(cols.succ_actor.identity_mut());
        delete_subs!(cols.succ_ctr);
        // back to front, so the spans still to cut keep their coordinates
        for d in self.drops.iter().rev() {
            if !d.value.is_empty() {
                cols.value.splice_slice(d.value.start, d.value.len(), &[]);
            }
        }

        debug_assert!(cols.columns_agree());
        self.preds
    }

    fn row_ops(&self) -> Vec<RowOp<'_>> {
        let mut ops = Vec::with_capacity(self.drops.len() + self.inserts.len());
        let mut inserts = self.inserts.iter().peekable();
        for d in &self.drops {
            while inserts.peek().is_some_and(|n| n.row <= d.row) {
                ops.push(RowOp::Insert(inserts.next().unwrap()));
            }
            ops.push(RowOp::Drop(d));
        }
        ops.extend(inserts.map(RowOp::Insert));
        debug_assert!(ops.windows(2).all(|w| w[0].row() <= w[1].row()));
        ops
    }
}

/// Not a closure: elision ties the borrow to the row.
fn key_str(n: &NewDelete) -> Option<&str> {
    n.key.2.as_deref()
}

fn take_orphans(orphans: &mut Vec<(RawId, RawId)>, id: RawId) -> Vec<RawId> {
    let mut taken = vec![];
    orphans.retain(|(s, target)| {
        if *s == id {
            taken.push(*target);
            false
        } else {
            true
        }
    });
    taken
}

struct PredSrc<'a> {
    count: hexane::Decoder<'a, Option<u64>>,
    actor: hexane::Decoder<'a, Option<ActorIdx>>,
    ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    hint: hexane::DeltaDecoder<'a, Option<i64>>,
}

impl<'a> PredSrc<'a> {
    fn new(columns: &RawColumns<Uncompressed>, data: &'a [u8]) -> Self {
        let mut s = PredSrc {
            count: hexane::decoder::<Option<u64>>(&[]),
            actor: hexane::decoder::<Option<ActorIdx>>(&[]),
            ctr: hexane::DeltaDecoder::new(&[]),
            hint: hexane::DeltaDecoder::new(&[]),
        };
        for col in columns.iter() {
            let d = &data[col.data()];
            match col.spec() {
                spec::PRED_COUNT => s.count = hexane::decoder::<Option<u64>>(d),
                spec::PRED_ACTOR => s.actor = hexane::decoder::<Option<ActorIdx>>(d),
                spec::PRED_CTR => s.ctr = hexane::DeltaDecoder::new(d),
                spec::HINT => s.hint = hexane::DeltaDecoder::new(d),
                _ => {}
            }
        }
        s
    }

    /// An absent column decodes as empty: no preds.
    fn next_count(&mut self) -> usize {
        self.count.next().flatten().unwrap_or(0) as usize
    }

    fn next_hint(&mut self) -> Option<i64> {
        self.hint.next().flatten()
    }

    fn next_pred(&mut self) -> RawId {
        RawId {
            actor: self.actor.next().flatten().expect("pred actor"),
            ctr: self.ctr.next().flatten().expect("pred ctr") as u64,
        }
    }
}

/// A fragment's rebuilt pred and hint columns.
#[derive(Default)]
pub(crate) struct PredCols {
    count: hexane::Encoder<'static, u32>,
    actor: hexane::Encoder<'static, ActorIdx>,
    ctr: hexane::DeltaEncoder<'static, i64>,
    hint: hexane::DeltaEncoder<'static, Option<i64>>,
}

impl PredCols {
    fn push(&mut self, id: RawId) {
        self.actor.append(id.actor);
        self.ctr.append(id.ctr as i64);
    }

    /// Like the change set writer, omits a column of only defaults.
    fn save_to(self, data: &mut Vec<u8>) -> Vec<RawColumn<Uncompressed>> {
        [
            (spec::PRED_COUNT, self.count.save_to_unless(data, 0)),
            (spec::PRED_ACTOR, self.actor.save_to(data)),
            (spec::PRED_CTR, self.ctr.save_to(data)),
            (spec::HINT, self.hint.save_to_unless(data, None)),
        ]
        .into_iter()
        .filter_map(|(spec, range)| RawColumn::try_new(spec, range))
        .collect()
    }
}

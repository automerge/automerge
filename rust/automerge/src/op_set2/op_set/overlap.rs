//! Cutting a bundle's already-present rows out of its fragment.
//!
//! A bundle whose members are only partly new — an *overlap* — carries
//! rows for members the document already has. Applying them again would
//! be an error, so they are cut out before the fragment meets the
//! manifold: one walk marks every row's fate, then one cursor per column
//! applies the marks.
//!
//! Cutting a row is not always just a delete. The bundle format keeps a
//! relationship between two members in the *succ* column of the older
//! one and gives an op a row of its own only when its pred names
//! something the bundle does not contain (`BundleBuilder::flush_deletes`).
//! Dropping a row therefore leaves its surviving successors in exactly
//! the position an out-of-bundle target puts them: the relationship
//! moves to the successor's pred column — and a successor that had no
//! row of its own, a delete, gains one at the end of its register, which
//! is where the builder would have written it.

use super::super::columns::Columns;
use super::super::meta::ValueMeta;
use super::super::types::{Action, ActorIdx, ScalarValue};
use super::OpSet;
use crate::clock::Clock;
use crate::storage::bundle::ops as spec;
use crate::storage::columns::compression::Uncompressed;
use crate::storage::{RawColumn, RawColumns};

use std::ops::Range;

impl OpSet {
    /// Cut the rows `clock` covers out of this fragment, which the
    /// document already has.
    ///
    /// `raw`/`data` are the bundle's own op columns — the source of the
    /// pred and hint columns, which are not part of an op set. Returns
    /// them rebuilt for the rows that remain: every dropped row takes
    /// its preds with it and some kept rows gain one, so they are
    /// re-encoded rather than edited (they are the two smallest columns
    /// a fragment has, and usually empty).
    ///
    /// `actor_map` maps this bundle's actor indexes to the document's;
    /// the columns stay in bundle space, so it is only consulted to ask
    /// the clock about a row.
    pub(crate) fn drop_covered(
        &mut self,
        raw: &RawColumns<Uncompressed>,
        data: &[u8],
        clock: &Clock,
        actor_map: &[usize],
    ) -> PredCols {
        // the clock in the fragment's own actor space: an id is covered
        // exactly when its counter has been reached
        let covered: Vec<u64> = actor_map.iter().map(|&a| clock.max_op(a)).collect();
        let plan = Plan::build(&self.cols, PredSrc::new(raw, data), &covered);
        plan.apply(&mut self.cols)
    }

    /// This fragment's op columns as a bundle's, for the streaming read
    /// the manifold makes ([`crate::storage::bundle::FragOps`]) — the op
    /// columns plus the pred and hint columns it holds separately.
    ///
    /// Only that reader consumes the result, and it looks columns up by
    /// spec, so the appended columns need no place in the spec order.
    pub(crate) fn export_frag(&self, preds: PredCols) -> (RawColumns<Uncompressed>, Vec<u8>) {
        let (raw, mut data) = self.cols.export();
        let extra = preds.save_to(&mut data);
        (raw.iter().cloned().chain(extra).collect(), data)
    }
}

/// An op id as the fragment stores it: bundle actor space, which is
/// where every comparison and every write in this pass happens.
///
/// Ordered as an [`crate::types::OpId`] is — by counter, then actor —
/// so a group's synthesized deletes come out in a stable order.
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

/// The register a walked row belongs to: its object and its element or
/// map key, an insert naming the element it creates.
#[derive(Clone, Copy, PartialEq)]
struct Register<'a> {
    obj: (Option<ActorIdx>, Option<u32>),
    key: (Option<ActorIdx>, Option<u32>, Option<&'a str>),
}

/// A run of rows the walk drops, with the sub-column spans it takes
/// with it. A skipped member's ops are contiguous, so a bundle that is
/// nearly all present comes out as a handful of runs rather than one
/// cut per row — the difference between a few cursor writes and
/// thousands.
struct DropRun {
    row: usize,
    len: usize,
    succ: Range<usize>,
    value: Range<usize>,
}

/// A delete op that lost the row it rode on: its target is being
/// dropped, so the deletion needs a row of its own, carrying the target
/// as a pred.
struct NewDelete {
    /// where the row lands — the end of its register
    row: usize,
    id: RawId,
    obj: (Option<ActorIdx>, Option<u32>),
    key: (Option<ActorIdx>, Option<u32>, Option<String>),
}

/// What the walk decided, in the columns' original coordinates.
struct Plan {
    drops: Vec<DropRun>,
    inserts: Vec<NewDelete>,
    preds: PredCols,
    /// whether the last drop run may still take the next row — false
    /// once a register has closed with rows to insert at that seam
    open_run: bool,
}

/// One row edit, as the column cursors take them: ascending, and an
/// insert before a drop at the same row — the insert closes the register
/// the dropped row has already left.
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
    /// The walk: every column the fate of a row depends on, read once,
    /// in step.
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
        // running positions in the sub columns, in original coordinates
        let mut sub = 0;
        let mut val = 0;
        // successors of dropped rows that have not found their own row
        // yet, as (successor, target). Cleared at every register: a
        // successor is an op of the same register, so it is either
        // ahead of the cursor and inside it, or it has no row at all
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
            // an insert's register is the element it creates, not the
            // one it is anchored to
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
                    // a successor of a kept row cannot be covered: the
                    // document would then hold a change whose ancestor
                    // it is still missing
                    debug_assert!(!s.covered(covered), "covered successor of a new op");
                } else if !s.covered(covered) {
                    // the row that carried this relationship is going,
                    // so the successor has to carry it instead
                    orphans.push((s, id));
                }
            }

            if dropped {
                for _ in 0..n_pred {
                    src.next_pred();
                }
                plan.push_drop(row, sub..sub + n_succ, val..val + n_val);
            } else {
                // the row keeps its own preds and adopts the dropped
                // rows it succeeds
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
            sub += n_succ;
            val += n_val;
        }
        plan.close_register(register, cols.len(), &mut orphans);
        plan
    }

    /// Extend the open run of dropped rows, or start one. A run never
    /// spans a row an insert lands on: the cursor would then have to cut
    /// it in two, which is what `open_run` is holding the door for.
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

    /// Give every orphan left in the closing register a row: it is a
    /// delete whose target the bundle held, and no longer does.
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
        // by successor: one delete op takes one row however many of the
        // register's values it deletes. Sorted so the rows land in a
        // fragment's own order, as the builder's `flush_deletes` does
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

    /// One cursor per column, walking the marks in order.
    fn apply(self, cols: &mut Columns) -> PredCols {
        let ops = self.row_ops();

        // a delete op carries no value, and its row is the only kind
        // this pass inserts
        let null = ValueMeta::from(&ScalarValue::Null);

        // Each arm gets the value an inserted delete row writes; a
        // dropped row is a plain delete everywhere.
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

        // the sub columns: a new delete row has no successors, so only
        // the dropped rows' spans move
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
        // the value blob has no cursor: splicing back to front keeps
        // the spans ahead of each cut in their original coordinates
        for d in self.drops.iter().rev() {
            if !d.value.is_empty() {
                cols.value.splice_slice(d.value.start, d.value.len(), &[]);
            }
        }

        debug_assert!(cols.columns_agree());
        self.preds
    }

    /// The two mark lists as one ascending stream.
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

/// A free function so elision ties the borrow to the row, which a
/// closure's would not.
fn key_str(n: &NewDelete) -> Option<&str> {
    n.key.2.as_deref()
}

/// The orphans naming `id`, taken out of the list.
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

/// A fragment's pred and hint columns, being read.
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

    /// An elided column decodes as empty, which is what an absent one
    /// means: no preds, no hint.
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

/// A fragment's pred and hint columns, being written.
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

    /// Write the columns into `data`, as the bundle writer does — a
    /// fragment with no preds (or no hints) drops the column rather
    /// than carrying a run of defaults.
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

use crate::op_set2::op_set::{OpIter, RowRun, RowRuns, Steps, SuccRuns};
use crate::op_set2::types::KeyRef;
use crate::op_set2::{OpSet, ReadOpError};
use crate::types::{ElemId, OpId};

use super::BundleBuilder;

/// Gaps shorter than this between rows to visit are read through rather
/// than re-seeking every column.
const GAP: usize = 64;

/// What the walk does with a stretch of rows.
enum Step {
    /// members needing no rewrite: copied column by column
    Copy(usize),
    /// members copied the same way but for successors outside the bundle
    CopyKeeping(usize),
    /// rows that cannot touch the bundle
    Skip(usize),
    /// other changes' sequence rows that members delete or overwrite,
    /// read from the narrow columns
    Foreign(usize),
    /// rows decoded and rebuilt one at a time
    Decode(usize),
}

impl<'a> BundleBuilder<'a> {
    /// Feeds every op of the members, and every op they delete or
    /// overwrite, through the builder. Rows that can be copied as they
    /// are are never decoded.
    pub(crate) fn walk(&mut self, op_set: &'a OpSet) -> Result<(), ReadOpError> {
        let mut ops = op_set.iter();
        let mut succ = Vec::new();
        for rows in op_set.ctr_ranges(self.op_counters(), GAP) {
            let mut runs = op_set.row_runs(rows.start);
            let mut row = rows.start;
            while row < rows.end {
                let run = runs
                    .peek(rows.end - row)?
                    .ok_or(ReadOpError::MissingValue("id_actor"))?;
                let n = match self.classify(&run, &runs)? {
                    Step::Copy(n) => {
                        self.member_rows.extend(row..row + n);
                        self.push_verbatim_rows(row..row + n);
                        n
                    }
                    Step::CopyKeeping(n) => {
                        self.member_rows.extend(row..row + n);
                        self.copy_keeping_member_succ(row..row + n);
                        n
                    }
                    Step::Skip(n) => n,
                    Step::Foreign(n) => {
                        self.foreign_rows(&run, n, row, runs.succ(), &mut succ)?;
                        n
                    }
                    Step::Decode(n) => {
                        for r in row..row + n {
                            self.decode_row(&mut ops, r, &mut succ)?;
                        }
                        n
                    }
                };
                runs.consume(&run, n)?;
                row += n;
            }
        }
        Ok(())
    }

    fn classify(&self, run: &RowRun, runs: &RowRuns<'_>) -> Result<Step, ReadOpError> {
        let (member, mut n) = self.member_span(&run.id, run.count);
        // the narrow columns hold no map key
        let narrow = run.insert || run.key.is_some();
        // a pending delete must see every row of its key group go by
        if !self.preds.is_empty() {
            return Ok(match member || !narrow {
                true => Step::Decode(1),
                false => Step::Foreign(n),
            });
        }
        let target = match &run.key {
            Some(key) => {
                let (key_member, k) = self.member_span(key, n);
                n = k;
                !key_member
            }
            None => false,
        };
        if member && target {
            return Ok(Step::Decode(n));
        }
        if run.succ == 0 {
            return Ok(if member { Step::Copy(n) } else { Step::Skip(n) });
        }
        // a member row is copied when all its successors are members; a
        // non-member row is skipped when none are
        let per_row = run.succ as usize;
        let mut succ = runs.succ();
        let mut left = n * per_row;
        let mut same = 0;
        while left > 0 {
            let (steps, count) = succ.next_run(left)?;
            let (succ_member, m) = self.member_span(&steps, count);
            if succ_member != member {
                break;
            }
            same += m;
            if m < count {
                break;
            }
            left -= count;
        }
        let rows = same / per_row;
        Ok(match (rows, member) {
            (0, true) => Step::CopyKeeping(n),
            (0, false) if narrow => Step::Foreign(n),
            (0, false) => Step::Decode(1),
            (r, true) => Step::Copy(r),
            (r, false) => Step::Skip(r),
        })
    }

    /// Whether the first of `steps` is a member's op, and how many of the
    /// first `max` share that answer.
    fn member_span(&self, steps: &Steps, max: usize) -> (bool, usize) {
        let first = steps.first;
        let i = self
            .builders
            .partition_point(|b| (b.actor, b.max_op) < (steps.actor, first));
        let same_actor = |j: usize| self.builders.get(j).filter(|b| b.actor == steps.actor);
        match same_actor(i) {
            Some(b) if b.start_op <= first => (true, within(steps, b.start_op, b.max_op, max)),
            next => {
                let lo = match i.checked_sub(1).and_then(same_actor) {
                    Some(b) => b.max_op + 1,
                    None => 0,
                };
                let hi = next.map_or(u64::MAX, |b| b.start_op - 1);
                (false, within(steps, lo, hi, max))
            }
        }
    }

    /// [`Self::decode_row`] for rows of changes outside the bundle, whose
    /// only effect is to name the preds of members and to end key groups.
    fn foreign_rows(
        &mut self,
        run: &RowRun,
        n: usize,
        row: usize,
        mut succ_runs: SuccRuns<'_>,
        succ: &mut Vec<OpId>,
    ) -> Result<(), ReadOpError> {
        let per_row = run.succ as usize;
        for i in 0..n {
            let id = run.id.nth(i);
            let key = match (&run.key, run.insert) {
                (_, true) => ElemId(id),
                (Some(key), false) => ElemId(key.nth(i)),
                (None, false) => unreachable!("only sequence rows are foreign"),
            };
            succ.clear();
            let mut left = per_row;
            while left > 0 {
                let (steps, count) = succ_runs.next_run(left)?;
                succ.extend((0..count).map(|j| steps.nth(j)));
                left -= count;
            }
            self.process_foreign(row + i, run.obj, KeyRef::Seq(key), id, succ);
        }
        Ok(())
    }

    fn decode_row(
        &mut self,
        ops: &mut OpIter<'a>,
        row: usize,
        succ: &mut Vec<OpId>,
    ) -> Result<(), ReadOpError> {
        let op = ops
            .try_nth(row - ops.pos())?
            .ok_or(ReadOpError::MissingValue("op"))?;
        let id = op.id;
        succ.clear();
        succ.extend(op.succ());
        self.process_op(op, succ);
        for s in succ.iter() {
            self.process_succ(id, row, *s);
        }
        Ok(())
    }
}

/// How many of the first `max` counters of `steps` lie in `lo..=hi`, the
/// first being one of them.
fn within(steps: &Steps, lo: u64, hi: u64, max: usize) -> usize {
    let room = match steps.step {
        0 => return max,
        1 => hi - steps.first,
        s if s > 0 => (hi - steps.first) / s as u64,
        s => (steps.first - lo) / s.unsigned_abs(),
    };
    max.min(room.saturating_add(1) as usize)
}

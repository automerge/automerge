use crate::automerge::view::ReadAt;
use crate::clock::Clock;
use crate::iter::tools::{BoolColumnSkipper, PeekShift, Shiftable, SkipIter, Skipper};
use crate::marks::MarkSet;
use crate::op_set2::op::SuccCursors;
use crate::op_set2::types::{Action, KeyRef, ScalarValue};
use crate::types::{ObjId, OpId};

use super::{
    ActionIter, FixCounters, InsertIter, OpIdIter, OpIter, OpQueryTerm, OpSet, ReadOpError,
    SuccIterIter, VisIter,
};

use std::ops::Range;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct TopOps<'a> {
    inner: FixCounters<'a, SkipIter<OpIter<'a>, TopIter<'a>>>,
    visible: VisIter<'a>,
    visible_pos: usize,
}

impl<'a> TopOps<'a> {
    /// Iterate over the top op of each key or element in `range`, marking an
    /// op as a conflict when other visible ops share its key.
    ///
    /// The `read` decides how visibility is determined. A current read, with
    /// or without a write-frontier mask, uses the op set's indexes: the mask
    /// is already applied to them by [`OpSet::recompute_indexes`]. A
    /// historical read scans the ops against its clock instead. Counter values
    /// are summed from successors at read time, so the clock from
    /// [`ReadAt::historical_slow`] is applied to them on either path.
    pub(crate) fn new(op_set: &'a OpSet, read: &ReadAt<'_>, range: Range<usize>) -> Self {
        #[cfg(any(test, feature = "seek-counters"))]
        crate::seek_counters::count_top_ops_seek();
        let scan = read.historical().cloned();
        let visible_pos = range.start;
        let visible = VisIter::new(op_set, scan.as_ref(), range.clone());
        let iter = SkipIter::new(op_set.iter_range(&range), TopIter::new(op_set, scan, range));
        let inner = FixCounters::new(iter, read.historical_slow().cloned());
        Self {
            inner,
            visible,
            visible_pos,
        }
    }
}

impl<'a> Iterator for TopOps<'a> {
    type Item = super::Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut op = self.inner.next()?;
        let mut conflict = false;
        loop {
            let skip = self.visible.next().expect("a top op must also be visible");
            let pos = self.visible_pos + skip;
            self.visible_pos = pos + 1;
            assert!(
                pos <= op.pos,
                "visible op at {pos} advanced past top op at {}",
                op.pos
            );
            if pos == op.pos {
                break;
            }
            conflict = true;
        }
        op.conflict = conflict;
        Some(op)
    }
}

impl<'a> OpQueryTerm<'a> for TopOps<'a> {
    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.inner.get_marks()
    }

    fn range(&self) -> Range<usize> {
        self.inner.range()
    }
}

#[cfg(feature = "slow_path_assertions")]
pub(super) fn assert_matches_slow(
    op_set: &OpSet,
    obj: &ObjId,
    clock: Option<Clock>,
    mut fast: TopOps<'_>,
) {
    use super::OpQuery;

    let mut slow = SlowTopOpIter::new(op_set.iter_obj(obj).visible_slow(clock));
    let mut index = 0;
    loop {
        match (fast.next(), slow.next()) {
            (Some(fast), Some(slow)) => {
                assert_eq!(fast.id, slow.id, "fast and slow top op IDs differ at {index}");
                assert_eq!(
                    fast.value, slow.value,
                    "fast and slow top op values differ at {index}"
                );
                assert_eq!(
                    fast.conflict, slow.conflict,
                    "fast and slow top op conflict flags differ at {index}"
                );
            }
            (None, None) => break,
            (fast, slow) => panic!(
                "fast and slow top op iterator lengths differ at {index}: fast={fast:?}, slow={slow:?}"
            ),
        }
        index += 1;
    }
}

#[cfg(feature = "slow_path_assertions")]
#[derive(Clone, Debug)]
struct SlowTopOpIter<'a, I: Iterator<Item = super::Op<'a>>> {
    iter: I,
    last_op: Option<super::Op<'a>>,
}

#[cfg(feature = "slow_path_assertions")]
impl<'a, I: Iterator<Item = super::Op<'a>>> SlowTopOpIter<'a, I> {
    fn new(iter: I) -> Self {
        Self {
            iter,
            last_op: None,
        }
    }
}

#[cfg(feature = "slow_path_assertions")]
impl<'a, I: Iterator<Item = super::Op<'a>>> Iterator for SlowTopOpIter<'a, I> {
    type Item = super::Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        for mut next in self.iter.by_ref() {
            if let Some(last) = self.last_op.take() {
                if next.obj != last.obj || next.elemid_or_key() != last.elemid_or_key() {
                    self.last_op = Some(next);
                    return Some(last);
                }
                next.conflict = true;
            }
            self.last_op = Some(next);
        }
        self.last_op.take()
    }
}

/// The `top` twin of [`VisIter`]: a [`Skipper`] which yields runs of
/// non-top ops so a `SkipIter` can jump from one top op (each element's
/// winning, visible op) to the next.
///
/// Like [`VisIter`] it comes in two flavors: `Current` reads the `top`
/// index column directly, `Scan` recomputes topness under a historical
/// [`Clock`] from the cheap columns only.
///
/// The range must be scoped to a single object: the scan flavor detects
/// group boundaries from inserts and `key_str` changes alone, which cannot
/// distinguish equal map keys across an object boundary.
#[derive(Clone, Debug)]
pub(crate) struct TopIter<'a> {
    range: Range<usize>,
    cursor: usize,
    exhausted: bool,
    inner: TopIterInner<'a>,
}

#[derive(Clone, Debug)]
enum TopIterInner<'a> {
    Empty,
    Current(BoolColumnSkipper<'a>),
    // peekable because the row which terminates one group is the first
    // row of the next
    Scan(Box<PeekShift<ScanTopIter<'a>>>),
}

impl Default for TopIter<'_> {
    fn default() -> Self {
        Self {
            range: 0..0,
            cursor: 0,
            exhausted: true,
            inner: TopIterInner::Empty,
        }
    }
}

impl<'a> TopIter<'a> {
    pub(crate) fn new(op_set: &'a OpSet, clock: Option<Clock>, range: Range<usize>) -> Self {
        let cursor = range.start;
        let inner = if let Some(clock) = clock {
            TopIterInner::Scan(Box::new(PeekShift::new(ScanTopIter::new(
                op_set, clock, &range,
            ))))
        } else {
            TopIterInner::Current(BoolColumnSkipper::new(
                op_set.top_index_range(&range),
                range.clone(),
            ))
        };
        Self {
            range,
            cursor,
            exhausted: false,
            inner,
        }
    }
}

impl Skipper for TopIter<'_> {}

impl Shiftable for TopIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.cursor = range.start;
        self.range = range.clone();
        self.exhausted = false;
        match &mut self.inner {
            TopIterInner::Empty => None,
            TopIterInner::Current(iter) => iter.shift_next(range),
            TopIterInner::Scan(iter) => {
                iter.shift(range.clone());
                if iter.peek().is_some() {
                    self.next()
                } else {
                    self.exhausted = true;
                    Some(range.end.saturating_sub(range.start))
                }
            }
        }
    }
}

impl Iterator for TopIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match &mut self.inner {
            TopIterInner::Empty => None,
            TopIterInner::Current(iter) => iter.next(),
            TopIterInner::Scan(iter) => loop {
                let Some(first) = iter.next() else {
                    self.exhausted = true;
                    let skip = self.range.end.saturating_sub(self.cursor);
                    self.cursor = self.range.end.saturating_add(1);
                    return Some(skip);
                };
                // a group is one map key or one list element; its ops are
                // contiguous, and the next group starts at the next insert
                // op (list element or mark) or `key_str` change (map key)
                let group_key = first.key_str;
                let mut top = first.visible.then_some(first.pos);
                while let Some(row) = iter.next_if(|r| !r.insert && r.key_str == group_key) {
                    if row.visible {
                        top = Some(row.pos);
                    }
                }
                if let Some(pos) = top {
                    let skip = pos.checked_sub(self.cursor)?;
                    self.cursor = pos + 1;
                    return Some(skip);
                }
            },
        }
    }
}

/// The columns [`TopIter`]'s scan flavor needs: `insert` and `key_str` for
/// group boundaries, `id`/`action`/`succ` for visibility under the clock.
#[derive(Clone, Debug)]
struct ScanTopIter<'a> {
    pos: usize,
    clock: Clock,
    key_str: hexane::Iter<'a, Option<String>>,
    id: OpIdIter<'a>,
    insert: InsertIter<'a>,
    action: ActionIter<'a>,
    succ: SuccIterIter<'a>,
}

impl<'a> ScanTopIter<'a> {
    fn new(op_set: &'a OpSet, clock: Clock, range: &Range<usize>) -> Self {
        Self {
            pos: range.start,
            clock,
            key_str: op_set.key_str_iter_range(range),
            id: op_set.id_iter_range(range),
            insert: op_set.insert_iter_range(range),
            action: op_set.action_iter_range(range),
            succ: op_set.succ_iter_range(range),
        }
    }

    fn row(
        &mut self,
        pos: usize,
        id: OpId,
        key_str: Option<&'a str>,
        insert: bool,
        action: Action,
        succ: SuccCursors<'a>,
    ) -> ScanTopRow<'a> {
        let visible = is_visible(id, action, succ, &self.clock);
        ScanTopRow {
            pos,
            insert,
            key_str,
            visible,
        }
    }
}

impl<'a> Shiftable for ScanTopIter<'a> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<ScanTopRow<'a>> {
        let pos = range.start;
        self.pos = pos + 1;
        let id = self.id.shift_next(range.clone())?;
        let key_str = self.key_str.shift_next(range.clone())?;
        let insert = self.insert.shift_next(range.clone())?;
        let action = self.action.shift_next(range.clone())?;
        let succ = self.succ.shift_next(range)?;
        Some(self.row(pos, id, key_str, insert, action, succ))
    }
}

impl<'a> Iterator for ScanTopIter<'a> {
    type Item = ScanTopRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos;
        self.pos += 1;
        let id = self.id.next()?;
        let key_str = self.key_str.next()?;
        let insert = self.insert.next()?;
        let action = self.action.next()?;
        let succ = self.succ.next()?;
        Some(self.row(pos, id, key_str, insert, action, succ))
    }
}

/// One op reduced to what topness needs: where its group starts and
/// whether it is visible.
#[derive(Clone, Debug)]
struct ScanTopRow<'a> {
    pos: usize,
    insert: bool,
    key_str: Option<&'a str>,
    visible: bool,
}

fn is_visible(id: OpId, action: Action, succ: SuccCursors<'_>, clock: &Clock) -> bool {
    if action == Action::Increment || !clock.covers(&id) {
        return false;
    }
    for (id, inc) in succ.with_inc() {
        if inc.is_none() && clock.covers(&id) {
            return false;
        }
    }
    true
}

/// One top op as [`TopRows`] reads it: only the columns a read of the whole
/// document keeps. `key_col` is the op's raw key column, not its
/// `elemid_or_key`, so an element of a sequence has a [`KeyRef::Seq`] key,
/// though not its own element id; `value` has a counter's increments added,
/// as [`TopOps`] gives it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TopRow<'a> {
    pub(crate) pos: usize,
    pub(crate) obj: ObjId,
    pub(crate) id: OpId,
    pub(crate) key_col: KeyRef<'a>,
    pub(crate) action: Action,
    pub(crate) value: ScalarValue<'a>,
    pub(crate) conflict: bool,
}

/// The top ops of the whole op set under a current read, as [`TopOps`]
/// gives them, but decoding only the columns of a [`TopRow`] at each top
/// op.
///
/// It jumps between top ops with the `top` and visibility indexes, as
/// [`TopOps`] does for a current read, and decodes the object, id, key,
/// action and value columns there, and the successors only for a counter.
/// The insert and mark columns are never read, and nothing is built as a
/// whole [`super::Op`].
#[derive(Clone, Debug)]
pub(crate) struct TopRows<'a> {
    columns: OpIter<'a>,
    top: TopIter<'a>,
    visible: VisIter<'a>,
    visible_pos: usize,
    /// The clock a counter's increments must be covered by: the
    /// write-frontier mask, if any.
    counter_clock: Option<Clock>,
    /// The position the object, id, key, action and value columns read
    /// next.
    pos: usize,
    /// The position the successor column reads next.
    succ_pos: usize,
}

impl<'a> TopRows<'a> {
    /// The top ops of all of `op_set` under `read`, which must be a current
    /// read: only a current read's top ops are indexed.
    pub(crate) fn new(op_set: &'a OpSet, read: &ReadAt<'_>) -> Self {
        assert!(
            read.historical().is_none(),
            "TopRows reads the indexes, so only a current read"
        );
        let range = 0..op_set.len();
        Self {
            columns: op_set.iter_range(&range),
            top: TopIter::new(op_set, None, range.clone()),
            visible: VisIter::new(op_set, None, range),
            visible_pos: 0,
            counter_clock: read.historical_slow().cloned(),
            pos: 0,
            succ_pos: 0,
        }
    }

    fn try_next(&mut self) -> Result<Option<TopRow<'a>>, ReadOpError> {
        let Some(skip) = self.top.next() else {
            return Ok(None);
        };
        let columns = &mut self.columns;
        let Some(id) = columns.id.maybe_try_nth(skip)? else {
            return Ok(None);
        };
        let obj = columns.obj.try_nth(skip)?;
        let key_col = columns.key.try_nth(skip)?;
        let action = columns.action.try_nth(skip)?;
        let mut value = columns.value.try_nth(skip)?;
        #[cfg(any(test, feature = "decode-counters"))]
        crate::decode_counters::count_op_columns(5);
        let pos = self.pos + skip;
        self.pos = pos + 1;
        if let ScalarValue::Counter(n) = value {
            let succ = columns.succ.try_nth(pos - self.succ_pos)?;
            #[cfg(any(test, feature = "decode-counters"))]
            crate::decode_counters::count_op_columns(1);
            self.succ_pos = pos + 1;
            let clock = self.counter_clock.as_ref();
            let inc: i64 = succ
                .with_inc()
                .filter(|(id, _)| clock.is_none_or(|clock| clock.covers(id)))
                .filter_map(|(_, inc)| inc)
                .sum();
            value = ScalarValue::Counter(n + inc);
        }
        let mut conflict = false;
        loop {
            let skip = self.visible.next().expect("a top op must also be visible");
            let visible = self.visible_pos + skip;
            self.visible_pos = visible + 1;
            assert!(
                visible <= pos,
                "visible op at {visible} advanced past top op at {pos}"
            );
            if visible == pos {
                break;
            }
            conflict = true;
        }
        Ok(Some(TopRow {
            pos,
            obj,
            id,
            key_col,
            action,
            value,
            conflict,
        }))
    }
}

impl<'a> From<super::Op<'a>> for TopRow<'a> {
    /// The row of a top op read whole, as [`TopOps`] gives it.
    fn from(op: super::Op<'a>) -> Self {
        TopRow {
            pos: op.pos,
            obj: op.obj,
            id: op.id,
            key_col: op.key,
            action: op.action,
            value: op.value,
            conflict: op.conflict,
        }
    }
}

impl<'a> Iterator for TopRows<'a> {
    type Item = TopRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().expect("op set columns decode")
    }
}

#[cfg(test)]
mod tests {
    //! A current read must use the indexes whether or not a write-frontier
    //! mask is present: the mask is baked into the indexes by
    //! `recompute_indexes`. Only a historical read scans. These tests pin the
    //! dispatch, which is otherwise visible only as a 10–60× slowdown.

    use super::TopOps;
    use crate::op_set2::op_set::VisIter;
    use crate::transaction::Transactable;
    use crate::{Author, AutoCommit, ObjType, ReadDoc, ROOT};

    fn author(n: u8) -> Author<'static> {
        Author::from(vec![n])
    }

    /// A two-author text document, masked at its heads so that nothing is
    /// hidden but a mask is present.
    fn masked_doc() -> AutoCommit {
        let mut doc = AutoCommit::new().with_author(Some(author(1)));
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "one ").unwrap();
        doc.commit();
        doc.set_author(Some(author(2)));
        doc.splice_text(&text, 4, 0, "two").unwrap();
        doc.commit();
        let heads = doc.get_heads();
        doc.mask_author(author(2), &heads);
        doc
    }

    fn top_ops_for(doc: &mut AutoCommit, historical: bool) -> TopOps<'_> {
        // The first change: strictly in the past, so `read_at` is historical
        // rather than collapsing to a current read at the heads.
        let heads = vec![doc.get_changes(&[])[0].hash()];
        let (_, text) = doc.get(ROOT, "text").unwrap().unwrap();
        let am = doc.document();
        let obj = am.exid_to_obj(&text).unwrap();
        let range = am.ops().scope_to_obj(&obj.id);
        let read = if historical {
            am.read_at(Some(&heads))
        } else {
            am.read_current()
        };
        TopOps::new(am.ops(), &read, range)
    }

    #[test]
    fn masked_current_read_uses_the_indexes() {
        let mut doc = masked_doc();
        assert!(doc.is_author_masked(&author(2)), "mask is present");
        let top = top_ops_for(&mut doc, false);
        assert!(
            matches!(top.visible, VisIter::Indexed(_)),
            "VisIter must be indexed under a mask"
        );
    }

    #[test]
    fn historical_read_scans() {
        let mut doc = masked_doc();
        let top = top_ops_for(&mut doc, true);
        assert!(matches!(top.visible, VisIter::Scan(_)));
    }

    /// A document with ops of every kind that stop being top ops:
    /// overwritten, deleted, incremented, conflicting after a merge of three
    /// actors, in maps, tables, lists and text with marks, some left
    /// uncommitted.
    fn superseded_and_deleted() -> AutoCommit {
        use crate::{ActorId, ScalarValue};

        let mut doc = AutoCommit::new().with_actor(ActorId::from(b"aaaa".as_slice()));
        doc.put(ROOT, "name", "first").unwrap();
        doc.put(ROOT, "name", "second").unwrap();
        doc.put(ROOT, "gone", 1).unwrap();
        doc.delete(ROOT, "gone").unwrap();
        doc.put(ROOT, "count", ScalarValue::Counter(5.into()))
            .unwrap();
        doc.increment(ROOT, "count", 3).unwrap();
        doc.put(ROOT, "big", "x".repeat(300).as_str()).unwrap();
        let children = doc.put_object(ROOT, "children", ObjType::Map).unwrap();
        for i in 0..30 {
            let child = doc
                .put_object(&children, format!("c{i:02}"), ObjType::Map)
                .unwrap();
            doc.put(&child, "at", i as i64).unwrap();
            let list = doc.put_object(&child, "items", ObjType::List).unwrap();
            for j in 0..(i % 5) {
                doc.insert(&list, j, j as i64).unwrap();
            }
            if i % 5 > 2 {
                doc.delete(&list, 1).unwrap();
                doc.put(&list, 0, "overwritten").unwrap();
                doc.put(&list, 0, ScalarValue::Counter(1.into())).unwrap();
                doc.increment(&list, 0, 2).unwrap();
            }
            if i % 4 == 0 {
                let text = doc.put_object(&child, "lines", ObjType::Text).unwrap();
                doc.splice_text(&text, 0, 0, "hello world").unwrap();
                doc.splice_text(&text, 2, 3, "").unwrap();
                doc.mark(
                    &text,
                    crate::marks::Mark::new("bold".to_owned(), true, 0, 3),
                    crate::marks::ExpandMark::After,
                )
                .unwrap();
                doc.split_block(&text, 2).unwrap();
            }
            if i % 6 == 0 {
                doc.put_object(&child, "table", ObjType::Table).unwrap();
            }
        }
        doc.delete(&children, "c03").unwrap();
        doc.put(&children, "c04", "now a scalar").unwrap();
        // Counters side by side, so that their successors are read at
        // adjacent top ops: some incremented, some not.
        let counters = doc.put_object(ROOT, "counters", ObjType::List).unwrap();
        for i in 0..8 {
            doc.insert(&counters, i, ScalarValue::Counter((i as i64).into()))
                .unwrap();
            if i % 3 != 1 {
                doc.increment(&counters, i, 10 * i as i64 + 1).unwrap();
            }
        }
        let tally = doc.put_object(ROOT, "tally", ObjType::Map).unwrap();
        for key in ["a", "b", "c", "d"] {
            doc.put(&tally, key, ScalarValue::Counter(1.into()))
                .unwrap();
        }
        doc.increment(&tally, "b", 2).unwrap();
        doc.increment(&tally, "c", 3).unwrap();
        doc.commit();
        let mut right = doc.fork().with_actor(ActorId::from(b"bbbb".as_slice()));
        let mut third = doc.fork().with_actor(ActorId::from(b"cccc".as_slice()));
        doc.put(ROOT, "name", "left").unwrap();
        right.put(ROOT, "name", "right").unwrap();
        third.put(ROOT, "name", "third").unwrap();
        right.put_object(&children, "c10", ObjType::List).unwrap();
        doc.put(&children, "c10", "left").unwrap();
        right.increment(ROOT, "count", 10).unwrap();
        third.increment(ROOT, "count", 100).unwrap();
        third.delete(&children, "c11").unwrap();
        right.put(&children, "c11", "kept by right").unwrap();
        doc.merge(&mut right).unwrap();
        doc.merge(&mut third).unwrap();
        // Uncommitted ops are read too.
        doc.put(ROOT, "pending", true).unwrap();
        doc.increment(ROOT, "count", 1000).unwrap();
        doc
    }

    /// `TopRows` reads, row for row, what `TopOps` gives for a current read
    /// of the whole op set.
    fn top_rows_match(am: &crate::Automerge) {
        let ops = am.ops();
        let read = am.read_current();
        let rows: Vec<_> = super::TopRows::new(ops, &read).collect();
        let tops: Vec<_> = TopOps::new(ops, &read, 0..ops.len())
            .map(super::TopRow::from)
            .collect();
        assert_eq!(rows, tops);
    }

    #[test]
    fn top_rows_read_what_top_ops_give() {
        let mut doc = superseded_and_deleted();
        // Many ops are no longer on top.
        let ops = doc.doc.ops();
        let read = doc.doc.read_current();
        let tops = super::TopRows::new(ops, &read).count();
        assert!(ops.len() > tops + 50, "{} ops, {tops} top", ops.len());
        assert!(
            super::TopRows::new(ops, &read).any(|row| row.conflict),
            "some keys are in conflict"
        );
        let rows: Vec<_> = super::TopRows::new(ops, &read).collect();
        assert!(
            rows.windows(2).any(|pair| {
                pair[1].pos == pair[0].pos + 1
                    && pair.iter().all(|row| {
                        matches!(row.value, crate::op_set2::types::ScalarValue::Counter(_))
                    })
            }),
            "some counters are adjacent top ops"
        );
        top_rows_match(&doc.doc);
        doc.commit();
        let loaded = AutoCommit::load(&doc.save()).unwrap();
        top_rows_match(&loaded.doc);
        top_rows_match(&AutoCommit::new().doc);
    }

    /// Under a write-frontier mask, a counter's increments by the masked
    /// author are left out, as `TopOps` leaves them out.
    #[test]
    fn top_rows_leave_out_masked_increments() {
        let mut doc = superseded_and_deleted().with_author(Some(author(1)));
        doc.commit();
        let heads = doc.get_heads();
        doc.set_author(Some(author(2)));
        doc.increment(ROOT, "count", 5000).unwrap();
        doc.put(ROOT, "name", "masked").unwrap();
        // Masked increments, overwrites and deletes of adjacent counters.
        let (_, counters) = doc.get(ROOT, "counters").unwrap().unwrap();
        doc.increment(&counters, 1, 7).unwrap();
        doc.increment(&counters, 2, 7).unwrap();
        doc.put(&counters, 3, "no longer a counter").unwrap();
        doc.put(&counters, 4, crate::ScalarValue::Counter(100.into()))
            .unwrap();
        doc.delete(&counters, 5).unwrap();
        let (_, tally) = doc.get(ROOT, "tally").unwrap().unwrap();
        doc.increment(&tally, "a", 9).unwrap();
        doc.put(&tally, "b", "overwritten").unwrap();
        doc.delete(&tally, "c").unwrap();
        doc.commit();
        let unmasked_counters = format!("{:?}", doc.hydrate(&counters, None).unwrap());
        let unmasked_tally = format!("{:?}", doc.hydrate(&tally, None).unwrap());
        let unmasked = format!("{:?}", doc.get(ROOT, "count").unwrap().unwrap().0);
        doc.mask_author(author(2), &heads);
        let masked = format!("{:?}", doc.get(ROOT, "count").unwrap().unwrap().0);
        assert_ne!(masked, unmasked);
        let masked_counters = format!("{:?}", doc.hydrate(&counters, None).unwrap());
        let masked_tally = format!("{:?}", doc.hydrate(&tally, None).unwrap());
        assert_ne!(masked_counters, unmasked_counters);
        assert_ne!(masked_tally, unmasked_tally);
        top_rows_match(&doc.doc);
    }
}

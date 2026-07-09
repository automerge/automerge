use crate::clock::Clock;
use crate::iter::tools::{BoolColumnSkipper, Shiftable, SkipIter, Skipper};
use crate::marks::MarkSet;
use crate::op_set2::op::SuccCursors;
use crate::op_set2::types::{Action, KeyRef};
use crate::types::{ElemId, ObjId, OpId};

use super::{
    ActionIter, FixCounters, InsertIter, KeyIter, ObjIdIter, OpIdIter, OpIter, OpQueryTerm, OpSet,
    SuccIterIter, VisIter,
};

use std::ops::Range;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct TopOps<'a> {
    inner: FixCounters<'a, SkipIter<OpIter<'a>, SkipToTopIter<'a>>>,
    visible: VisIter<'a>,
    visible_pos: usize,
}

impl<'a> TopOps<'a> {
    pub(crate) fn new(op_set: &'a OpSet, clock: Option<Clock>, range: Range<usize>) -> Self {
        let visible_pos = range.start;
        let visible = VisIter::new(op_set, clock.as_ref(), range.clone());
        let iter = SkipIter::new(
            op_set.iter_range(&range),
            SkipToTopIter::new(op_set, clock.clone(), range),
        );
        let inner = FixCounters::new(iter, clock);
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

/// An iterator which returns runs of non-top ops, this can be used with a
/// SkipIter to skip to the next top op
#[derive(Clone, Debug)]
pub(crate) struct SkipToTopIter<'a> {
    range: Range<usize>,
    clock: Option<Clock>,
    cursor: usize,
    exhausted: bool,
    inner: SkipToTopIterInner<'a>,
}

#[derive(Clone, Debug)]
enum SkipToTopIterInner<'a> {
    Empty,
    Current(BoolColumnSkipper<'a>),
    Scan {
        iter: Box<TopScanIter<'a>>,
        buffered: Option<Box<TopScanRow<'a>>>,
    },
}

impl Default for SkipToTopIter<'_> {
    fn default() -> Self {
        Self {
            range: 0..0,
            clock: None,
            cursor: 0,
            exhausted: true,
            inner: SkipToTopIterInner::Empty,
        }
    }
}

impl<'a> SkipToTopIter<'a> {
    pub(crate) fn new(op_set: &'a OpSet, clock: Option<Clock>, range: Range<usize>) -> Self {
        let cursor = range.start;
        let inner = if clock.is_some() {
            SkipToTopIterInner::Scan {
                iter: Box::new(TopScanIter::new(op_set, &range)),
                buffered: None,
            }
        } else {
            SkipToTopIterInner::Current(BoolColumnSkipper::new(
                op_set.top_index_range(&range),
                range.clone(),
            ))
        };
        Self {
            range,
            clock,
            cursor,
            exhausted: false,
            inner,
        }
    }
}

impl Skipper for SkipToTopIter<'_> {}

impl Shiftable for SkipToTopIter<'_> {
    fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.cursor = range.start;
        self.range = range.clone();
        self.exhausted = false;
        match &mut self.inner {
            SkipToTopIterInner::Empty => None,
            SkipToTopIterInner::Current(iter) => iter.shift_next(range),
            SkipToTopIterInner::Scan { iter, buffered } => {
                *buffered = None;
                if let Some(first) = iter.shift_next(range.clone()) {
                    *buffered = Some(Box::new(first));
                    self.next()
                } else {
                    self.exhausted = true;
                    Some(range.end.saturating_sub(range.start))
                }
            }
        }
    }
}

impl Iterator for SkipToTopIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match &mut self.inner {
            SkipToTopIterInner::Empty => None,
            SkipToTopIterInner::Current(iter) => iter.next(),
            SkipToTopIterInner::Scan { iter, buffered } => {
                let clock = self.clock.as_ref();
                loop {
                    let Some(first) = buffered.take().or_else(|| iter.next().map(Box::new)) else {
                        self.exhausted = true;
                        let skip = self.range.end.saturating_sub(self.cursor);
                        self.cursor = self.range.end.saturating_add(1);
                        return Some(skip);
                    };
                    let obj = first.obj;
                    let key = first.key.clone();
                    let mut top = if first.is_visible(clock) {
                        Some(first.pos)
                    } else {
                        None
                    };

                    for row in iter.by_ref() {
                        if row.obj != obj || row.key != key {
                            *buffered = Some(Box::new(row));
                            break;
                        }
                        if row.is_visible(clock) {
                            top = Some(row.pos);
                        }
                    }

                    if let Some(pos) = top {
                        let skip = pos.checked_sub(self.cursor)?;
                        self.cursor = pos + 1;
                        return Some(skip);
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct TopScanIter<'a> {
    pos: usize,
    obj: ObjIdIter<'a>,
    key: KeyIter<'a>,
    id: OpIdIter<'a>,
    insert: InsertIter<'a>,
    action: ActionIter<'a>,
    succ: SuccIterIter<'a>,
}

impl<'a> TopScanIter<'a> {
    fn new(op_set: &'a OpSet, range: &Range<usize>) -> Self {
        Self {
            pos: range.start,
            obj: op_set.obj_id_iter_range(range),
            key: op_set.key_iter_range(range),
            id: op_set.id_iter_range(range),
            insert: op_set.insert_iter_range(range),
            action: op_set.action_iter_range(range),
            succ: op_set.succ_iter_range(range),
        }
    }

    fn row_at(&mut self, pos: usize) -> Option<TopScanRow<'a>> {
        let id = self.id.next()?;
        let key = self.key.next()?;
        let key = if self.insert.next()? {
            KeyRef::Seq(ElemId(id))
        } else {
            key
        };
        Some(TopScanRow {
            pos,
            obj: self.obj.next()?,
            key,
            id,
            action: self.action.next()?,
            succ: self.succ.next()?,
        })
    }

    fn shift_next(&mut self, range: Range<usize>) -> Option<TopScanRow<'a>> {
        let pos = range.start;
        self.pos = pos + 1;
        let id = self.id.shift_next(range.clone())?;
        let key = self.key.shift_next(range.clone())?;
        let key = if self.insert.shift_next(range.clone())? {
            KeyRef::Seq(ElemId(id))
        } else {
            key
        };
        Some(TopScanRow {
            pos,
            obj: self.obj.shift_next(range.clone())?,
            key,
            id,
            action: self.action.shift_next(range.clone())?,
            succ: self.succ.shift_next(range)?,
        })
    }
}

impl<'a> Iterator for TopScanIter<'a> {
    type Item = TopScanRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos;
        self.pos += 1;
        self.row_at(pos)
    }
}

#[derive(Clone, Debug)]
struct TopScanRow<'a> {
    pos: usize,
    obj: ObjId,
    key: KeyRef<'a>,
    id: OpId,
    action: Action,
    succ: SuccCursors<'a>,
}

impl TopScanRow<'_> {
    fn is_visible(&self, clock: Option<&Clock>) -> bool {
        if self.action == Action::Increment || clock.is_some_and(|clock| !clock.covers(&self.id)) {
            return false;
        }
        for (id, inc) in self.succ.clone().with_inc() {
            if inc.is_none() && clock.map(|clock| clock.covers(&id)).unwrap_or(true) {
                return false;
            }
        }
        true
    }
}

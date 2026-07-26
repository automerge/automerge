use std::borrow::Cow;

use hexane::PrefixIter;

use crate::clock::ClockRange;
use crate::iter::spans::{MarkDiff, RichTextDiff};
use crate::iter::tools::Diff;
use crate::op_set2::op_set::{MarkIdx, MarkIndexColumn, MarkPrefix, OpSet};
use crate::op_set2::types::MarkData;

/// The open-mark set, carried along a forward walk of the op set.
///
/// The mark index is a prefix column whose accumulator *is* the set of
/// marks spanning a position, so this holds a cursor into it rather than
/// re-deriving the set from the mark ops. Two consequences:
///
/// * a walk pays nothing for marks until it crosses a mark row —
///   mark-free stretches are a single run step, however long;
/// * a walk can start (or jump) anywhere, because the set at that
///   position is whatever the prefix says it is. No accumulating from
///   the start of the object.
///
/// Visibility is decided by the clock alone: mark ops are excluded from
/// delete targets ([`OpSet::seek_ops_by`]'s `action != Action::Mark`),
/// so a mark has no succ and `covers` is the whole story. That is what
/// lets this agree with a diff derived from the op stream.
#[derive(Debug, Clone, Default)]
pub(crate) struct MarkCursor<'a> {
    marks: Option<&'a MarkIndexColumn>,
    iter: PrefixIter<'a, Option<MarkIdx>>,
    clock: ClockRange,
    /// the prefix as of the last materialize; retaining it is what makes
    /// [`MarkPrefix::same_set`] an exact change probe
    seen: MarkPrefix,
    /// resolved view of `seen`
    state: RichTextDiff<'a>,
}

impl<'a> MarkCursor<'a> {
    pub(crate) fn new(op_set: &'a OpSet, clock: ClockRange, pos: usize) -> Self {
        let marks = op_set.mark_index();
        let mut cursor = Self {
            iter: marks.prefix_at(pos),
            marks: Some(marks),
            clock,
            seen: MarkPrefix::default(),
            state: RichTextDiff::default(),
        };
        cursor.refresh();
        cursor
    }

    /// Carry the set forward so it covers rows `..=pos`.
    ///
    /// Forward-only — every consumer walks the op set in document order
    /// — and idempotent, so asking twice at the same row is free.
    pub(crate) fn advance_to(&mut self, pos: usize) {
        // going backwards would silently leave the set too far along
        // rather than panic, so say so here: the sibling column
        // iterators assert the same contract
        debug_assert!(
            pos + 1 >= self.iter.pos(),
            "MarkCursor is forward-only (at {} want {pos})",
            self.iter.pos(),
        );
        self.iter.advance_to(pos + 1);
        self.refresh();
    }

    /// The mark diff at the current position.
    pub(crate) fn current(&self) -> MarkDiff {
        self.state.current()
    }

    /// Rebuild the resolved view, but only if the open set actually
    /// moved: the common case is a pointer comparison and nothing else.
    fn refresh(&mut self) {
        let now = self.iter.total();
        if self.seen.same_set(&now) {
            return;
        }
        debug_assert!(
            !now.has_dangling_closes(),
            "mark prefix has dangling closes — malformed mark column?"
        );
        self.state = RichTextDiff::default();
        if let Some(marks) = self.marks {
            for id in now.opens() {
                let diff = match (
                    self.clock.visible_before(&id),
                    self.clock.visible_after(&id),
                ) {
                    (true, true) => Diff::Same,
                    (true, false) => Diff::Del,
                    (false, true) => Diff::Add,
                    (false, false) => continue,
                };
                let Some(data) = marks.mark_data(&id) else {
                    continue;
                };
                // the cache owns the name and value; borrow them rather
                // than copy the string out
                let data = MarkData {
                    name: Cow::Borrowed(data.name.as_ref()),
                    value: data.value.clone(),
                };
                self.state.mark_begin_diff(diff, id, data);
            }
        }
        self.seen = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marks::{ExpandMark, Mark, MarkSet};
    use crate::transaction::Transactable;
    use crate::{AutoCommit, ObjType, ScalarValue, ROOT};

    /// Text carrying overlapping, nested and removed marks, with a block
    /// and a deletion so the mark rows are not contiguous.
    fn marked_doc() -> (AutoCommit, Vec<crate::ChangeId>) {
        let mut doc = AutoCommit::new();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        doc.splice_text(&text, 0, 0, "the quick brown fox jumps")
            .unwrap();
        let before = doc.get_heads();

        let mark = |name: &'static str, start, end, value: ScalarValue| {
            Mark::new(name.to_string(), value, start, end)
        };
        doc.mark(&text, mark("bold", 4, 15, true.into()), ExpandMark::Both)
            .unwrap();
        // nested inside bold
        doc.mark(&text, mark("italic", 6, 9, true.into()), ExpandMark::None)
            .unwrap();
        // overlaps the end of bold
        doc.mark(&text, mark("link", 10, 20, "a".into()), ExpandMark::After)
            .unwrap();
        // an unmark: a mark op with a null value
        doc.mark(
            &text,
            mark("bold", 8, 11, ScalarValue::Null),
            ExpandMark::None,
        )
        .unwrap();
        doc.splice_text(&text, 2, 3, "").unwrap();
        (doc, before)
    }

    /// The after-side mark set, normalized the way a reader sees it —
    /// `MarkSet::from_query_state` drops unmark tombstones, so compare
    /// against the same shape.
    fn after_set(diff: &MarkDiff) -> Option<MarkSet> {
        match diff {
            MarkDiff::After(m) | MarkDiff::Diff(_, m) => {
                let set = m.as_ref().clone().without_unmarks();
                (!set.is_empty()).then_some(set)
            }
            _ => None,
        }
    }

    /// Walking the cursor forward must agree with landing on a position
    /// cold. The walk carries and patches its set incrementally and
    /// skips rebuilds when the prefix identity is unchanged; a fresh
    /// cursor descends the tree and rebuilds from scratch. If the
    /// staleness probe or the inclusive-position convention were wrong,
    /// these would diverge.
    #[test]
    fn walking_matches_seeking() {
        let (mut doc, before) = marked_doc();
        let after = doc.get_heads();
        let diff = doc.document().clock_range(&before, &after).unwrap();
        let ops = doc.document().ops();
        let clocks = [ClockRange::current(None), diff];

        for clock in clocks {
            let mut walk = MarkCursor::new(ops, clock.clone(), 0);
            for pos in 0..ops.len() {
                walk.advance_to(pos);
                let cold = MarkCursor::new(ops, clock.clone(), pos);
                assert_eq!(walk.current(), cold.current(), "row {pos}");
                // re-asking must not move anything
                walk.advance_to(pos);
                assert_eq!(walk.current(), cold.current(), "row {pos} re-read");
            }
        }
    }

    /// And the walk must agree with the point query the read path
    /// already trusts (`query_nth` resolves its marks this way).
    #[test]
    fn walking_matches_rich_text_at() {
        let (mut doc, _) = marked_doc();
        let ops = doc.document().ops();
        let mut cursor = MarkCursor::new(ops, ClockRange::current(None), 0);
        let mut saw_marks = false;

        for pos in 0..ops.len() {
            cursor.advance_to(pos);
            let expected = MarkSet::from_query_state(&ops.mark_index().rich_text_at(pos, None))
                .map(|m| m.as_ref().clone());
            saw_marks |= expected.is_some();
            assert_eq!(after_set(&cursor.current()), expected, "row {pos}");
        }
        assert!(saw_marks, "test document produced no marks");
    }
}

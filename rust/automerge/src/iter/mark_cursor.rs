use std::borrow::Cow;

use hexane::PrefixIter;

use crate::clock::ClockRange;
use crate::iter::spans::{MarkDiff, RichTextDiff};
use crate::iter::tools::Diff;
use crate::op_set2::op_set::{MarkIdx, MarkIndexColumn, MarkPrefix, OpSet};
use crate::op_set2::types::MarkData;

/// The open-mark set along a forward walk of the op set. Free between mark
/// rows, and can start at any position without walking from the object start.
///
/// Mark ops are never delete targets, so the clock alone decides visibility.
#[derive(Debug, Clone, Default)]
pub(crate) struct MarkCursor<'a> {
    marks: Option<&'a MarkIndexColumn>,
    iter: PrefixIter<'a, Option<MarkIdx>>,
    clock: ClockRange,
    /// retained so [`MarkPrefix::same_set`] is an exact change probe
    seen: MarkPrefix,
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

    /// Covers rows `..=pos`. Forward only; repeating a position is free.
    pub(crate) fn advance_to(&mut self, pos: usize) {
        debug_assert!(
            pos + 1 >= self.iter.pos(),
            "MarkCursor is forward-only (at {} want {pos})",
            self.iter.pos(),
        );
        self.iter.advance_to(pos + 1);
        self.refresh();
    }

    pub(crate) fn current(&self) -> MarkDiff {
        self.state.current()
    }

    /// A pointer comparison when the open set hasn't moved.
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
    use crate::autocommit::AutoCommit;
    use crate::marks::{ExpandMark, Mark, MarkSet};
    use crate::tx::Transactable;
    use crate::{ObjType, ScalarValue, ROOT};

    /// Overlapping, nested and removed marks; a deletion keeps mark rows non-contiguous.
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

    /// Without unmarks, as `MarkSet::from_query_state` reports it.
    fn after_set(diff: &MarkDiff) -> Option<MarkSet> {
        match diff {
            MarkDiff::After(m) | MarkDiff::Diff(_, m) => {
                let set = m.as_ref().clone().without_unmarks();
                (!set.is_empty()).then_some(set)
            }
            _ => None,
        }
    }

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

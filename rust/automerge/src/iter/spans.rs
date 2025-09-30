use crate::clock::{Clock, ClockRange};
use crate::hydrate::Value;
use crate::iter::tools::{Diff, DiffIter, Unshift};
use crate::marks::{MarkSet, MarkSetIter, MarkStateMachine};
use crate::op_set2::op_set::{ActionValueIter, MarkInfoIter, OpIdIter, OpSet};
use crate::op_set2::types::{Action, MarkData, ScalarValue};
use crate::patches::PatchLog;
use crate::types::{ObjId, OpId, TextEncoding};
use crate::value;

use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct SpanDiff {
    pub(crate) diff: Diff,
    pub(crate) span: SpanInternal,
}

impl SpanDiff {
    pub(crate) fn log(self, obj: ObjId, log: &mut PatchLog, encoding: TextEncoding) {
        match (self.diff, self.span) {
            (Diff::Add, SpanInternal::Text(text, index, marks)) => {
                log.splice(obj, index, &text, marks.export());
            }
            (Diff::Add, SpanInternal::Obj(id, index, expose)) => {
                let conflict = false;
                let value = crate::hydrate::Value::map();
                log.insert_and_maybe_expose(obj, index, value, id, conflict, expose);
            }
            (Diff::Same, SpanInternal::Text(text, index, marks)) => {
                if let Some(m) = marks.export() {
                    log.mark(obj, index, encoding.width(&text), &m);
                }
            }
            (Diff::Del, SpanInternal::Text(text, index, _marks)) => {
                log.delete_seq(obj, index, encoding.width(&text));
            }
            (Diff::Del, SpanInternal::Obj(_, index, _)) => {
                log.delete_seq(obj, index, encoding.width(PLACEHOLDER));
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpansDiff<'a> {
    action_value: Unshift<DiffIter<'a, ActionValueIter<'a>>>,
    mark_info: MarkInfoIter<'a>,
    op_id: OpIdIter<'a>,
    marks: RichTextDiff<'a>,
    op_set: Option<&'a OpSet>,
    clock: ClockRange,
    state: SpanState,
    pos: usize,
}

impl Iterator for SpansDiff<'_> {
    type Item = SpanDiff;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(span) = self.state.pop() {
            return Some(span);
        }
        while let Some((diff, av)) = self.action_value.next() {
            if let Some(span) = self.process_action_val_diff(diff, av) {
                return Some(span);
            }
        }
        self.state.flush()
    }
}

impl<'a> SpansDiff<'a> {
    pub(crate) fn empty(encoding: TextEncoding) -> Self {
        Self {
            action_value: Default::default(),
            mark_info: Default::default(),
            op_id: Default::default(),
            marks: Default::default(),
            op_set: Default::default(),
            clock: Default::default(),
            state: SpanState::empty(encoding),
            pos: Default::default(),
        }
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.mark_info.set_max(range.end);
        self.op_id.set_max(range.end);
        self.action_value.shift(range);

        self.marks = Default::default();
        self.state = SpanState::empty(self.state.encoding);
        self.next()
    }

    pub(crate) fn new(
        op_set: &'a OpSet,
        range: Range<usize>,
        clock: ClockRange,
        encoding: TextEncoding,
    ) -> Self {
        let pos = range.start;
        let op_id = op_set.id_iter_range(&range);
        let mark_info = op_set.mark_info_iter_range(&range);

        let skip = {
            let value = op_set.value_iter_range(&range);
            let action = op_set.action_iter_range(&range);
            let iter = ActionValueIter::new(action, value);
            DiffIter::new(op_set, iter, clock.clone(), range)
        };

        let action_value = Unshift::new(skip);
        let marks = Default::default();
        let state = SpanState::empty(encoding);
        let op_set = Some(op_set);

        Self {
            state,
            action_value,
            mark_info,
            op_id,
            op_set,
            clock,
            marks,
            pos,
        }
    }

    fn push_block(&mut self, diff: Diff) -> Option<SpanDiff> {
        let id = self.next_opid()?;
        let expose = self.clock.predates(&id);
        Some(self.state.push_block(diff, id, expose))
    }

    fn next_opid(&mut self) -> Option<OpId> {
        let id_pos = self.op_id.pos();
        self.op_id.nth(self.pos - id_pos)
    }

    fn next_mark_name(&mut self) -> Option<Cow<'a, str>> {
        let mark_pos = self.mark_info.pos();
        let (mark_name, _expand) = self.mark_info.nth(self.pos - mark_pos)?;
        mark_name
    }

    fn process_mark(&mut self, diff: Diff, value: ScalarValue<'a>) {
        let Some(id) = self.next_opid() else {
            return;
        };
        if let Some(name) = self.next_mark_name() {
            let data = MarkData { name, value };
            self.marks.mark_begin_diff(diff, id, data);
        } else {
            self.marks.mark_end_diff(diff, id);
        }
        let current = self.marks.current();
        self.state.push_marks(current);
    }

    fn process_action_val_diff(
        &mut self,
        diff: Diff,
        av: (Action, ScalarValue<'a>, usize),
    ) -> Option<SpanDiff> {
        self.pos = av.2;
        match av {
            (Action::Set, ScalarValue::Str(s), _) => self.state.push_str(diff, &s),
            (Action::MakeMap, _, _) => self.push_block(diff),
            (Action::Mark, value, _) => {
                self.process_mark(diff, value);
                None
            }
            (Action::Delete, _, _) | (Action::Increment, _, _) => None,
            _ => self.state.push_str(diff, PLACEHOLDER),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpansInternal<'a> {
    iter: SpansDiff<'a>,
}

impl<'a> SpansInternal<'a> {
    pub(crate) fn empty(encoding: TextEncoding) -> Self {
        Self {
            iter: SpansDiff::empty(encoding),
        }
    }

    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        Some(self.iter.shift_next(range)?.span)
    }

    pub(crate) fn clock(&self) -> Option<&Clock> {
        self.iter.clock.after()
    }

    pub(crate) fn encoding(&self) -> TextEncoding {
        self.iter.state.encoding
    }

    pub(crate) fn new(
        op_set: &'a OpSet,
        range: Range<usize>,
        clock: Option<Clock>,
        encoding: TextEncoding,
    ) -> Self {
        let iter = SpansDiff::new(
            op_set,
            range.clone(),
            ClockRange::current(clock.clone()),
            encoding,
        );
        Self { iter }
    }
}

/// A sequence of block markers and text spans. Returned by [`crate::ReadDoc::spans`] and
/// [`crate::ReadDoc::spans_at`]
#[derive(Clone, Debug)]
pub struct Spans<'a> {
    internal: SpansInternal<'a>,
}

#[derive(Debug, Default, Clone)]
pub(crate) enum MarkDiff {
    #[default]
    Nothing,
    After(Arc<MarkSet>),
    Before(Arc<MarkSet>),
    Diff(Arc<MarkSet>, Arc<MarkSet>),
}

impl PartialEq for MarkDiff {
    // this was a big performance issue
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Nothing, Self::Nothing) => true,
            (Self::After(a), Self::After(b)) if Arc::ptr_eq(a, b) => true,
            (Self::Before(a), Self::Before(b)) if Arc::ptr_eq(a, b) => true,
            (Self::Diff(a1, a2), Self::Diff(b1, b2))
                if Arc::ptr_eq(a1, b1) && Arc::ptr_eq(a2, b2) =>
            {
                true
            }
            _ => self.iter().eq(other.iter()),
        }
    }
}

impl MarkDiff {
    fn with(&self, diff: Diff) -> Self {
        match (diff, self) {
            (Diff::Add, MarkDiff::Diff(_, m)) => MarkDiff::After(m.clone()),
            (Diff::Add, MarkDiff::After(_)) => self.clone(),
            (Diff::Same, _) => self.clone(),
            _ => MarkDiff::Nothing,
        }
    }

    fn iter_before(&self) -> impl Iterator<Item = (&str, &value::ScalarValue)> {
        match &self {
            MarkDiff::Before(b) => b.iter(),
            MarkDiff::Diff(b, _) => b.iter(),
            _ => MarkSetIter::default(),
        }
    }

    fn iter_after(&self) -> impl Iterator<Item = (&str, &value::ScalarValue)> {
        match &self {
            MarkDiff::After(a) => a.iter(),
            MarkDiff::Diff(_, a) => a.iter(),
            _ => MarkSetIter::default(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&str, &value::ScalarValue)> {
        let mut before = self.iter_before().peekable();
        let mut after = self.iter_after().peekable();
        enum Pop {
            Before,
            After,
            Both,
        }
        std::iter::from_fn(move || loop {
            let pop = match (before.peek(), after.peek()) {
                (None, Some(_)) => Pop::After,
                (Some(_), None) => Pop::Before,
                (Some((k1, _)), Some((k2, _))) if k1 < k2 => Pop::Before,
                (Some((k1, _)), Some((k2, _))) if k1 > k2 => Pop::After,
                _ => Pop::Both,
            };
            return match pop {
                Pop::Before => {
                    let (k, v) = before.next()?;
                    if v.is_null() {
                        continue;
                    } else {
                        Some((k, &value::ScalarValue::Null))
                    }
                }
                Pop::After => {
                    let (k, v) = after.next()?;
                    if v.is_null() {
                        continue;
                    } else {
                        Some((k, v))
                    }
                }
                Pop::Both => {
                    let (k1, v1) = before.next()?;
                    let (k2, v2) = after.next()?;
                    assert_eq!(k1, k2);
                    if v1 == v2 {
                        continue;
                    } else {
                        Some((k2, v2))
                    }
                }
            };
        })
    }

    pub(crate) fn export(self) -> Option<Arc<MarkSet>> {
        // TODO : there should be a fast path here where we just clone after;
        let marks: MarkSet = self
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        if marks.is_empty() {
            None
        } else {
            Some(Arc::new(marks))
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum SpanInternal {
    Text(String, usize, MarkDiff), // Option<Arc<MarkSet>>),
    Obj(OpId, usize, bool),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Span {
    /// A span of text and the marks that were active for that span
    Text {
        text: String,
        marks: Option<Arc<MarkSet>>,
    },
    /// A block marker
    Block(crate::hydrate::Map),
}

impl Span {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Text { text, .. } => text,
            Self::Block(_) => PLACEHOLDER,
        }
    }
}

const PLACEHOLDER: &str = "\u{fffc}";

#[derive(Clone, Debug)]
struct SpanState {
    index: usize,
    // The next text span we will emit
    next_text: Option<NextText>,
    // The currently active marks. Note that this can be different to the marks
    // on `next_text` because we may encounter multiple open and closing marks
    // with no text in between them. E.g. if we encounter:
    //
    // * start bold
    // * 'aaa'
    // * start italic
    // * end italic
    // * 'bbb'
    //
    // Then the next_text will always have 'bold' marks but the active marks
    // will pass through ['bold', 'bold,italic', 'bold']
    marks: MarkDiff,
    // The next span we will emit if any. This is really used when we encounter
    // a block marker which requires us to emit the current text span and put
    // the block marker in `next_span` to be emitted on the next call to
    // `next()`.
    next_diff: Option<(SpanDiff, usize)>,
    encoding: TextEncoding,
}

#[derive(Clone, Default, Debug)]
struct NextText {
    // The actual text
    buff: String,
    // type of text
    diff: Diff,
    // The length of this text according to the text encoding
    len: usize,
    // The marks for this text
    marks: MarkDiff,
}

impl NextText {
    fn new(diff: Diff, marks: &MarkDiff) -> Self {
        NextText {
            buff: String::new(),
            len: 0,
            diff,
            marks: marks.with(diff),
        }
    }
}

impl SpanState {
    fn empty(encoding: TextEncoding) -> Self {
        Self {
            index: 0,
            next_text: None,
            next_diff: None,
            marks: MarkDiff::default(),
            encoding,
        }
    }

    fn push_str(&mut self, diff: Diff, s: &str) -> Option<SpanDiff> {
        debug_assert!(self.next_diff.is_none());

        let flush_needed = match &self.next_text {
            Some(next) => diff != next.diff || self.marks != next.marks,
            None => false,
        };

        let span = if flush_needed { self.flush() } else { None };

        let next_text = self
            .next_text
            .get_or_insert_with(|| NextText::new(diff, &self.marks));
        debug_assert!(next_text.diff == diff || next_text.len == 0);
        next_text.diff = diff;
        next_text.buff.push_str(s);
        next_text.len += self.encoding.width(s);

        span
    }

    fn diff_width(&self, diff: Diff, s: &str) -> usize {
        if diff == Diff::Del {
            0
        } else {
            self.encoding.width(s)
        }
    }

    fn push_block(&mut self, diff: Diff, id: OpId, expose: bool) -> SpanDiff {
        assert!(self.next_diff.is_none());
        let width = self.diff_width(diff, PLACEHOLDER);
        if let Some(result) = self.flush() {
            let span = SpanInternal::Obj(id, self.index, expose);
            self.next_diff = Some((SpanDiff { diff, span }, width));
            result
        } else {
            let span = SpanInternal::Obj(id, self.index, expose);
            self.index += width;
            SpanDiff { diff, span }
        }
    }

    fn push_marks(&mut self, new_marks: MarkDiff) {
        assert!(self.next_diff.is_none());
        self.marks = new_marks;
    }

    fn flush(&mut self) -> Option<SpanDiff> {
        assert!(self.next_diff.is_none());
        let Some(NextText {
            diff,
            buff,
            len,
            marks,
        }) = self.next_text.take()
        else {
            // No text to flush
            return None;
        };
        if len == 0 {
            return None;
        }

        let span = SpanInternal::Text(buff, self.index, marks);

        if diff != Diff::Del {
            self.index += len;
        }

        Some(SpanDiff { diff, span })
    }

    fn pop(&mut self) -> Option<SpanDiff> {
        let (result, width) = self.next_diff.take()?;
        if result.diff != Diff::Del {
            self.index += width;
        }
        Some(result)
    }
}

impl Iterator for SpansInternal<'_> {
    type Item = SpanInternal;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.iter.next()?.span)
    }
}

impl SpanInternal {
    pub(crate) fn export(
        self,
        op_set: &OpSet,
        clock: Option<&Clock>,
        encoding: TextEncoding,
    ) -> Span {
        match self {
            SpanInternal::Text(text, _, marks) => Span::Text {
                text,
                marks: marks.export(),
            },
            SpanInternal::Obj(opid, _, _) => {
                let value = op_set.hydrate_map(&opid.into(), clock, encoding);
                let Value::Map(value) = value else {
                    tracing::warn!("unexpected non map object in text");
                    return Span::Block(crate::hydrate::Map::new());
                };
                Span::Block(value)
            }
        }
    }
}

impl Spans<'_> {
    pub(crate) fn new(internal: SpansInternal<'_>) -> Spans<'_> {
        Spans { internal }
    }
}

impl Iterator for Spans<'_> {
    type Item = Span;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.internal.next()?.export(
            self.internal.iter.op_set?,
            self.internal.iter.clock.after(),
            self.internal.iter.state.encoding,
        ))
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct RichTextDiff<'a> {
    pub(crate) before: MarkStateMachine<'a>,
    pub(crate) after: MarkStateMachine<'a>,
}

impl<'a> RichTextDiff<'a> {
    pub(crate) fn current(&self) -> MarkDiff {
        match (self.before.current(), self.after.current()) {
            (None, None) => MarkDiff::Nothing,
            (None, Some(a)) => MarkDiff::After(a.clone()),
            (Some(b), Some(a)) => MarkDiff::Diff(b.clone(), a.clone()),
            (Some(b), None) => MarkDiff::Before(b.clone()),
        }
    }

    fn mark_begin_diff(&mut self, diff: Diff, id: OpId, data: MarkData<'a>) -> bool {
        match diff {
            Diff::Add => self.after.mark_begin(id, data),
            Diff::Del => self.before.mark_begin(id, data),
            Diff::Same => {
                self.before.mark_begin(id, data.clone());
                self.after.mark_begin(id, data)
            }
        }
    }

    fn mark_end_diff(&mut self, diff: Diff, id: OpId) -> bool {
        match diff {
            Diff::Add => self.after.mark_end(id),
            Diff::Del => self.before.mark_end(id),
            Diff::Same => {
                self.before.mark_end(id);
                self.after.mark_end(id)
            }
        }
    }
}

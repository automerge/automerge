use crate::hydrate::Value;
use crate::iter::tools::SkipIter;
use crate::iter::tools::Unshift;
use crate::marks::{MarkSet, MarkStateMachine};
use crate::op_set2::op_set::{ActionValueIter, MarkInfoIter, OpIdIter, OpSet, VisIter};
use crate::op_set2::types::{Action, MarkData, ScalarValue};
use crate::types::{Clock, OpId, Shared, TextEncoding};

use std::borrow::Cow;
use std::ops::Range;

#[derive(Debug, Clone)]
pub(crate) struct SpansInternal<'a> {
    action_value: Unshift<SkipIter<ActionValueIter<'a>, VisIter<'a>>>,
    mark_info: MarkInfoIter<'a>,
    op_id: OpIdIter<'a>,
    marks: MarkStateMachine<'a>,
    op_set: Option<&'a OpSet>,
    pub(super) clock: Option<Clock>,
    state: SpanState,
    pos: usize,
}

/// A sequence of block markers and text spans. Returned by [`crate::ReadDoc::spans`] and
/// [`crate::ReadDoc::spans_at`]
#[derive(Clone, Debug)]
pub struct Spans<'a> {
    internal: SpansInternal<'a>,
}

#[derive(Debug, Clone)]
pub(crate) enum SpanInternal {
    Text(String, usize, Option<Shared<MarkSet>>),
    Obj(OpId, usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Span {
    /// A span of text and the marks that were active for that span
    Text {
        text: String,
        marks: Option<Shared<MarkSet>>,
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

impl<'a> SpansInternal<'a> {
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

        self.marks = MarkStateMachine::default();
        self.state = SpanState::empty(self.state.encoding);
        self.next()
    }

    pub(crate) fn clock(&self) -> Option<&Clock> {
        self.clock.as_ref()
    }
    pub(crate) fn encoding(&self) -> TextEncoding {
        self.state.encoding
    }

    pub(crate) fn new(
        op_set: &'a OpSet,
        range: Range<usize>,
        clock: Option<Clock>,
        encoding: TextEncoding,
    ) -> Self {
        let pos = range.start;
        let op_id = op_set.id_iter_range(&range);
        let mark_info = op_set.mark_info_iter_range(&range);
        let action_value = Unshift::new(op_set.action_value_iter(range, clock.as_ref()));
        let marks = MarkStateMachine::default();
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

    fn push_block(&mut self) -> Option<SpanInternal> {
        let id = self.next_opid()?;
        Some(self.state.push_block(id))
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

    fn process_mark(&mut self, value: ScalarValue<'a>) {
        let Some(id) = self.next_opid() else {
            return;
        };
        if let Some(name) = self.next_mark_name() {
            self.marks.mark_begin(id, MarkData { name, value });
        } else {
            self.marks.mark_end(id);
        }
        self.state.push_marks(self.marks.current());
    }

    fn process_action_val(&mut self, av: (Action, ScalarValue<'a>, usize)) -> Option<SpanInternal> {
        self.pos = av.2;
        match av {
            (Action::Set, ScalarValue::Str(s), _) => self.state.push_str(&s),
            (Action::MakeMap, _, _) => self.push_block(),
            (Action::Mark, value, _) => {
                self.process_mark(value);
                None
            }
            (Action::Delete, _, _) | (Action::Increment, _, _) => None,
            _ => self.state.push_str(PLACEHOLDER),
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
    marks: Option<Shared<MarkSet>>,
    // The next span we will emit if any. This is really used when we encounter
    // a block marker which requires us to emit the current text span and put
    // the block marker in `next_span` to be emitted on the next call to
    // `next()`.
    next_span: Option<(SpanInternal, usize)>,
    encoding: TextEncoding,
}

#[derive(Clone, Default, Debug)]
struct NextText {
    // The actual text
    buff: String,
    // The length of this text according to the text encoding
    len: usize,
    // The marks for this text
    marks: Option<Shared<MarkSet>>,
}

impl SpanState {
    fn empty(encoding: TextEncoding) -> Self {
        Self {
            index: 0,
            next_text: None,
            next_span: None,
            marks: None,
            encoding,
        }
    }

    fn push_str(&mut self, s: &str) -> Option<SpanInternal> {
        assert!(self.next_span.is_none());

        let flush_needed = match &self.next_text {
            Some(NextText { len, marks, .. }) => {
                if *len == 0 {
                    false
                } else {
                    match (marks, self.marks.as_ref()) {
                        (Some(next_flush), Some(active)) => {
                            next_flush.non_deleted_marks() != active.non_deleted_marks()
                        }
                        (None, None) => false,
                        (None, Some(active)) => !active.non_deleted_marks().is_empty(),
                        (Some(next_flush), None) => !next_flush.non_deleted_marks().is_empty(),
                    }
                }
            }
            None => false,
        };
        let span = if flush_needed { self.flush() } else { None };
        let next_text = self.next_text.get_or_insert_with(|| NextText {
            buff: String::new(),
            len: 0,
            marks: self.marks.clone(),
        });
        next_text.buff.push_str(s);
        let width = self.encoding.width(s);
        next_text.len += width;
        span
    }

    fn push_block(&mut self, id: OpId) -> SpanInternal {
        assert!(self.next_span.is_none());
        let width = self.encoding.width(PLACEHOLDER);
        if let Some(text) = self.flush() {
            let block = SpanInternal::Obj(id, self.index);
            self.next_span = Some((block, width));
            text
        } else {
            let block = SpanInternal::Obj(id, self.index);
            self.index += width;
            block
        }
    }

    fn push_marks(&mut self, new_marks: Option<&Shared<MarkSet>>) {
        assert!(self.next_span.is_none());
        self.marks = new_marks.cloned();
    }

    fn flush(&mut self) -> Option<SpanInternal> {
        assert!(self.next_span.is_none());
        let Some(NextText { buff, len, marks }) = self.next_text.take() else {
            // No text to flush
            return None;
        };
        assert!(len > 0);

        let span = SpanInternal::Text(buff, self.index, marks);
        self.index += len;

        Some(span)
    }

    fn pop(&mut self) -> Option<SpanInternal> {
        let (span, width) = self.next_span.take()?;
        self.index += width;
        Some(span)
    }
}

impl Iterator for SpansInternal<'_> {
    type Item = SpanInternal;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(span) = self.state.pop() {
            return Some(span);
        }
        while let Some(av) = self.action_value.next() {
            if let Some(span) = self.process_action_val(av) {
                return Some(span);
            }
        }
        self.state.flush()
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
                marks: marks.and_then(|m| {
                    if m.non_deleted_marks().is_empty() {
                        None
                    } else {
                        Some(Shared::new(m.as_ref().clone().without_unmarks()))
                    }
                }),
            },
            SpanInternal::Obj(opid, _) => {
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
            self.internal.op_set?,
            self.internal.clock.as_ref(),
            self.internal.state.encoding,
        ))
    }
}

use crate::hydrate::Value;
use crate::iter::tools::SkipIter;
use crate::iter::tools::Unshift;
use crate::marks::{MarkSet, MarkStateMachine};
use crate::op_set2::op_set::{ActionValueIter, MarkInfoIter, OpIdIter, OpSet, VisIter};
use crate::op_set2::types::{Action, MarkData, ScalarValue};
use crate::types::OpId;
use crate::types::{Clock, TextEncoding};

use std::borrow::Cow;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
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
#[derive(Clone, Default, Debug)]
pub struct Spans<'a> {
    internal: SpansInternal<'a>,
}

#[derive(Debug, Clone)]
pub(crate) enum SpanInternal {
    Text(String, usize, Option<Arc<MarkSet>>),
    Obj(OpId, usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Span {
    /// A span of text and the marks that were active for that span
    Text(String, Option<Arc<MarkSet>>),
    /// A block marker
    Block(crate::hydrate::Map),
}

impl Span {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Text(s, _) => s,
            Self::Block(_) => PLACEHOLDER,
        }
    }
}

impl<'a> SpansInternal<'a> {
    pub(crate) fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        self.mark_info.set_max(range.end);
        self.op_id.set_max(range.end);
        self.action_value.shift(range);

        self.marks = MarkStateMachine::default();
        self.state = SpanState::default();
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
        let state = SpanState::new(encoding);
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

    fn process_mark(&mut self, value: ScalarValue<'a>) -> Option<SpanInternal> {
        let id = self.next_opid()?;
        if let Some(name) = self.next_mark_name() {
            self.marks.mark_begin(id, MarkData { name, value });
        } else {
            self.marks.mark_end(id);
        }
        self.state.push_marks(self.marks.current());
        None
    }

    fn process_action_val(&mut self, av: (Action, ScalarValue<'a>, usize)) -> Option<SpanInternal> {
        self.pos = av.2;
        match av {
            (Action::Set, ScalarValue::Str(s), _) => self.state.push_str(&s),
            (Action::MakeMap, _, _) => self.push_block(),
            (Action::Mark, value, _) => self.process_mark(value),
            (Action::Delete, _, _) | (Action::Increment, _, _) => None,
            _ => self.state.push_str(PLACEHOLDER),
        }
    }
}

const PLACEHOLDER: &str = "\u{fffc}";

#[derive(Clone, Default, Debug)]
struct SpanState {
    buff: String,
    len: usize,
    index: usize,
    marks: Option<Arc<MarkSet>>,
    next_marks: Option<Option<Arc<MarkSet>>>,
    next_span: Option<(SpanInternal, usize)>,
    encoding: TextEncoding,
}

impl SpanState {
    fn new(encoding: TextEncoding) -> Self {
        Self {
            buff: String::new(),
            len: 0,
            index: 0,
            marks: None,
            next_marks: None,
            next_span: None,
            encoding,
        }
    }

    fn push_str(&mut self, s: &str) -> Option<SpanInternal> {
        assert!(self.next_span.is_none());
        if self.next_marks.is_some() {
            let text = self.take_text();
            let width = self.encoding.width(s);
            self.buff.push_str(s);
            self.len += width;
            Some(text)
        } else {
            let width = self.encoding.width(s);
            self.buff.push_str(s);
            self.len += width;
            None
        }
    }

    fn push_block(&mut self, id: OpId) -> SpanInternal {
        assert!(self.next_span.is_none());
        let width = self.encoding.width(PLACEHOLDER);
        let block = SpanInternal::Obj(id, self.index);
        if let Some(text) = self.flush() {
            self.next_span = Some((block, width));
            text
        } else {
            self.index += width;
            block
        }
    }

    fn push_marks(&mut self, new_marks: Option<&Arc<MarkSet>>) {
        assert!(self.next_span.is_none());
        if self.marks.as_ref() != new_marks {
            if self.len > 0 {
                self.next_marks = Some(new_marks.cloned());
            } else {
                self.marks = new_marks.cloned();
            }
        } else if self.next_marks.is_some() {
            self.next_marks = None;
        }
    }

    fn flush(&mut self) -> Option<SpanInternal> {
        if self.len > 0 {
            Some(self.take_text())
        } else {
            None
        }
    }

    fn take_text(&mut self) -> SpanInternal {
        assert!(self.next_span.is_none());
        assert!(self.len > 0);
        let buff = mem::take(&mut self.buff);
        let len = mem::take(&mut self.len);
        let marks = mem::take(&mut self.marks);
        let text = SpanInternal::Text(buff, self.index, marks);
        self.marks = mem::take(&mut self.next_marks).unwrap_or_default();
        self.index += len;
        text
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
            SpanInternal::Text(txt, _, marks) => Span::Text(txt, marks),
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

impl<'a> Spans<'a> {
    pub(crate) fn new(
        op_set: &'a OpSet,
        range: Range<usize>,
        clock: Option<Clock>,
        encoding: TextEncoding,
    ) -> Self {
        Spans {
            internal: SpansInternal::new(op_set, range, clock, encoding),
        }
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

use crate::marks::{MarkSet, MarkStateMachine};
use crate::op_set2::op_set::{ActionValueIter, MarkInfoIter, OpIdIter};
use crate::op_set2::types::{Action, MarkData, ScalarValue};

use crate::hydrate::Value;
use crate::types::OpId;
use crate::types::{Clock, TextEncoding};
use crate::Automerge;

use std::borrow::Cow;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct SpansInternal<'a> {
    action_value: ActionValueIter<'a>,
    mark_info: MarkInfoIter<'a>,
    op_id: OpIdIter<'a>,
    marks: MarkStateMachine<'a>,
    doc: &'a Automerge,
    clock: Option<Clock>,
    state: SpanState,
}

/// A sequence of block markers and text spans. Returned by [`crate::ReadDoc::spans`] and
/// [`crate::ReadDoc::spans_at`]
#[derive(Debug)]
pub struct Spans<'a> {
    internal: SpansInternal<'a>,
}

#[derive(Debug)]
pub(crate) enum SpanInternal {
    Text(String, usize, Option<Arc<MarkSet>>),
    Obj(OpId, usize),
}

#[derive(Debug, PartialEq)]
pub enum Span {
    /// A span of text and the marks that were active for that span
    Text(String, Option<Arc<MarkSet>>),
    /// A block marker
    Block(crate::hydrate::Map),
}

impl<'a> SpansInternal<'a> {
    pub(crate) fn new(
        doc: &'a Automerge,
        range: Range<usize>,
        clock: Option<Clock>,
        encoding: TextEncoding,
    ) -> Self {
        let op_id = doc.ops.id_iter_range(&range);
        let mark_info = doc.ops.mark_info_iter_range(&range);
        let action_value = doc.ops.action_value_iter(range, clock.as_ref());
        let marks = MarkStateMachine::default();
        let state = SpanState::new(encoding);

        Self {
            state,
            action_value,
            mark_info,
            op_id,
            doc,
            clock,
            marks,
        }
    }

    fn push_block(&mut self) -> Option<SpanInternal> {
        let id = self.next_opid()?;
        Some(self.state.push_block(id))
    }

    fn next_opid(&mut self) -> Option<OpId> {
        let pos = self.action_value.pos() - 1;
        let id_pos = self.op_id.pos();
        self.op_id.nth(pos - id_pos)
    }

    fn next_mark_name(&mut self) -> Option<Cow<'a, str>> {
        let pos = self.action_value.pos() - 1;
        let mark_pos = self.mark_info.pos();
        let (mark_name, _expand) = self.mark_info.nth(pos - mark_pos)?;
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

    fn process_action_val(&mut self, av: (Action, ScalarValue<'a>)) -> Option<SpanInternal> {
        match av {
            (Action::Set, ScalarValue::Str(s)) => self.state.push_str(&s),
            (Action::MakeMap, _) => self.push_block(),
            (Action::Mark, value) => self.process_mark(value),
            (Action::Delete, _) | (Action::Increment, _) => None,
            _ => self.state.push_str(PLACEHOLDER),
        }
    }
}

const PLACEHOLDER: &str = "\u{fffc}";

#[derive(Debug)]
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

impl<'a> Iterator for SpansInternal<'a> {
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

impl<'a> Spans<'a> {
    pub(crate) fn new(
        doc: &'a Automerge,
        range: Range<usize>,
        clock: Option<Clock>,
        encoding: TextEncoding,
    ) -> Self {
        Spans {
            internal: SpansInternal::new(doc, range, clock, encoding),
        }
    }
}

impl<'a> Iterator for Spans<'a> {
    type Item = Span;

    fn next(&mut self) -> Option<Self::Item> {
        match self.internal.next() {
            Some(SpanInternal::Text(txt, _, marks)) => Some(Span::Text(txt, marks)),
            Some(SpanInternal::Obj(opid, _)) => {
                let value = self
                    .internal
                    .doc
                    .hydrate_map(&opid.into(), self.internal.clock.as_ref());
                let Value::Map(value) = value else {
                    tracing::warn!("unexpected non map object in text");
                    return None;
                };
                Some(Span::Block(value))
            }
            None => None,
        }
    }
}

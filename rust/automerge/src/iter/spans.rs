//use crate::exid::ExId;
use crate::hydrate;
use crate::marks::{RichText, RichTextStateMachine};
//use crate::port::HasMetadata;
use crate::op_set::Op;
use crate::op_tree::OpTreeIter;
use crate::types::Clock;
use crate::types::{Key, ListEncoding, ObjType, OpId, OpType};
use crate::Automerge;

use std::sync::Arc;

#[derive(Default, Debug)]
struct SpansState<'a> {
    key: Option<Key>,
    last_op: Option<Op<'a>>,
    current: Option<Arc<RichText>>,
    len: usize,
    index: usize,
    text: String,
    block: Option<Op<'a>>,
    marks: RichTextStateMachine<'a>,
}

#[derive(Debug)]
pub(crate) struct SpansInternal<'a, I>
where
    I: Iterator<Item = Op<'a>>,
{
    iter: I,
    doc: &'a Automerge,
    clock: Option<Clock>,
    state: SpansState<'a>,
}

pub struct Spans<'a> {
    internal: Option<SpansInternal<'a, OpTreeOpIter<'a>>>,
}

// clippy made me do this :/
impl<'a> std::fmt::Debug for Spans<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.internal {
            Some(_) => write!(f, "Some(...)"),
            None => write!(f, "None"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum SpanInternal {
    Text(String, usize, Option<Arc<RichText>>),
    Obj(OpId, usize),
}

#[derive(Debug)]
pub enum Span {
    Text(String, Option<Arc<RichText>>),
    Block(hydrate::Value),
}

impl<'a, I> SpansInternal<'a, I>
where
    I: Iterator<Item = Op<'a>>,
{
    pub(crate) fn new(iter: I, doc: &'a Automerge, clock: Option<Clock>) -> Self {
        Self {
            iter,
            doc,
            clock,
            state: Default::default(),
        }
    }
}

impl<'a> SpansState<'a> {
    fn process_op(&mut self, op: Op<'a>, doc: &Automerge) -> Option<SpanInternal> {
        if self.marks.process(*op.id(), op.action(), doc.osd()) {
            self.flush()
        } else {
            match op.action() {
                OpType::Make(ObjType::Map) => {
                    self.block = Some(op);
                    self.flush()
                }
                OpType::Make(_) | OpType::Put(_) => {
                    self.len += op.width(ListEncoding::Text);
                    self.text.push_str(op.as_str());
                    None
                }
                _ => None,
            }
        }
    }

    fn flush(&mut self) -> Option<SpanInternal> {
        if self.len > 0 {
            let index = self.index;

            let mut text = String::new();
            let mut current = self.marks.current().cloned();

            std::mem::swap(&mut text, &mut self.text);
            std::mem::swap(&mut current, &mut self.current);

            let span = SpanInternal::Text(text, index, current);

            self.index += self.len;
            self.len = 0;

            Some(span)
        } else if let Some(block) = self.block.take() {
            let width = block.width(ListEncoding::Text);
            let block = SpanInternal::Obj(*block.id(), self.index);
            self.index += width;
            Some(block)
        } else {
            self.current = self.marks.current().cloned();
            None
        }
    }
}

impl<'a, I> Iterator for SpansInternal<'a, I>
where
    I: Iterator<Item = Op<'a>>,
{
    type Item = SpanInternal;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(block) = self.state.block.take() {
            let width = block.width(ListEncoding::Text);
            let block = SpanInternal::Obj(*block.id(), self.state.index);
            self.state.index += width;
            return Some(block);
        }
        for op in &mut self.iter {
            if !(op.is_mark() || op.visible_at(self.clock.as_ref())) {
                continue;
            }
            let key = op.elemid_or_key();
            match &self.state.key {
                Some(k) if k != &key => {
                    if let Some(op) = self.state.last_op.replace(op) {
                        if let Some(span) = self.state.process_op(op, self.doc) {
                            return Some(span);
                        }
                    }
                }
                Some(_) => {
                    self.state.last_op = Some(op);
                }
                None => {
                    self.state.last_op = Some(op);
                }
            }
            self.state.key = Some(key);
        }
        self.state
            .last_op
            .take()
            .and_then(|op| self.state.process_op(op, self.doc))
            .or_else(|| self.state.flush())
    }
}

impl<'a> Spans<'a> {
    pub(crate) fn new(
        iter: Option<OpTreeIter<'a>>,
        doc: &'a Automerge,
        clock: Option<Clock>,
    ) -> Self {
        let op_iter = iter.map(|i| OpTreeOpIter {
            iter: i,
            osd: doc.osd(),
        });
        Spans {
            internal: op_iter.map(|i| SpansInternal::new(i, doc, clock)),
        }
    }
}

impl<'a> Iterator for Spans<'a> {
    type Item = Span;

    fn next(&mut self) -> Option<Self::Item> {
        self.internal
            .as_mut()
            .and_then(|internal| match internal.next() {
                Some(SpanInternal::Text(txt, _, marks)) => Some(Span::Text(txt, marks)),
                Some(SpanInternal::Obj(opid, _)) => {
                    let value = internal
                        .doc
                        .hydrate_map(&opid.into(), internal.clock.as_ref());
                    Some(Span::Block(value))
                }
                None => None,
            })
    }
}

struct OpTreeOpIter<'a> {
    iter: OpTreeIter<'a>,
    osd: &'a crate::op_set::OpSetData,
}

impl<'a> Iterator for OpTreeOpIter<'a> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|idx| idx.as_op(self.osd))
    }
}

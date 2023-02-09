use crate::query::{OpSetMetadata, QueryResult, TreeQuery};
use crate::types::{ElemId, ListEncoding, MarkName, Op, OpType, ScalarValue};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Spans<'a> {
    pos: usize,
    seen: usize,
    encoding: ListEncoding,
    last_seen: Option<ElemId>,
    last_insert: Option<ElemId>,
    seen_at_this_mark: Option<ElemId>,
    seen_at_last_mark: Option<ElemId>,
    ops: Vec<&'a Op>,
    marks: HashMap<MarkName, &'a ScalarValue>,
    changed: bool,
    spans: Vec<InternalSpan<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Span<'a> {
    pub pos: usize,
    pub marks: Vec<(smol_str::SmolStr, Cow<'a, ScalarValue>)>,
}

#[derive(Debug, Clone, PartialEq)]
struct InternalSpan<'a> {
    pos: usize,
    marks: Vec<(MarkName, Cow<'a, ScalarValue>)>,
}

impl<'a> InternalSpan<'a> {
    fn into_span(self, m: &OpSetMetadata) -> Span<'a> {
        Span {
            pos: self.pos,
            marks: self
                .marks
                .into_iter()
                .map(|(name, value)| (m.props.get(name.props_index()).into(), value))
                .collect(),
        }
    }
}

impl<'a> Spans<'a> {
    pub(crate) fn new(encoding: ListEncoding) -> Self {
        Spans {
            pos: 0,
            seen: 0,
            encoding,
            last_seen: None,
            last_insert: None,
            seen_at_last_mark: None,
            seen_at_this_mark: None,
            changed: false,
            ops: Vec::new(),
            marks: HashMap::new(),
            spans: Vec::new(),
        }
    }

    pub(crate) fn into_spans(self, m: &OpSetMetadata) -> Vec<Span<'a>> {
        self.spans.into_iter().map(|s| s.into_span(m)).collect()
    }

    pub(crate) fn check_marks(&mut self) {
        let mut new_marks = HashMap::new();
        for op in &self.ops {
            if let OpType::MarkBegin(m) = &op.action {
                new_marks.insert(m.name, &m.value);
            }
        }
        if new_marks != self.marks {
            self.changed = true;
            self.marks = new_marks;
        }
        if self.changed
            && (self.seen_at_last_mark != self.seen_at_this_mark
                || self.seen_at_last_mark.is_none() && self.seen_at_this_mark.is_none())
        {
            self.changed = false;
            self.seen_at_last_mark = self.seen_at_this_mark;
            let mut marks: Vec<_> = self
                .marks
                .iter()
                .map(|(key, val)| (*key, Cow::Borrowed(*val)))
                .collect();
            marks.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
            self.spans.push(InternalSpan {
                pos: self.seen,
                marks,
            });
        }
    }
}

impl<'a> TreeQuery<'a> for Spans<'a> {
    /*
    fn query_node(&mut self, _child: &OpTreeNode) -> QueryResult {
        unimplemented!()
    }
    */

    fn query_element_with_metadata(&mut self, element: &'a Op, m: &OpSetMetadata) -> QueryResult {
        // find location to insert
        // mark or set
        if element.succ.is_empty() {
            if let OpType::MarkBegin(_) = &element.action {
                let pos = self
                    .ops
                    .binary_search_by(|probe| m.lamport_cmp(probe.id, element.id))
                    .unwrap_err();
                self.ops.insert(pos, element);
            }
            if let OpType::MarkEnd(_) = &element.action {
                self.ops.retain(|op| op.id != element.id.prev());
            }
        }
        if element.insert {
            self.last_seen = None;
            self.last_insert = element.elemid();
        }
        if self.last_seen.is_none() && element.visible() {
            self.check_marks();
            let last_width = element.width(self.encoding);
            self.seen += last_width;
            self.last_seen = element.elemid();
            self.seen_at_this_mark = element.elemid();
        }
        self.pos += 1;
        QueryResult::Next
    }
}

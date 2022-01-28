use crate::query::{OpSetMetadata, QueryResult, TreeQuery};
use crate::types::{ElemId, Op, OpType, ScalarValue};
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Spans<const B: usize> {
    pos: usize,
    seen: usize,
    last_seen: Option<ElemId>,
    last_insert: Option<ElemId>,
    seen_at_this_mark: Option<ElemId>,
    seen_at_last_mark: Option<ElemId>,
    ops: Vec<Op>,
    marks: HashMap<String, ScalarValue>,
    changed: bool,
    pub spans: Vec<Span>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Span {
    pub pos: usize,
    pub marks: Vec<(String, ScalarValue)>,
}

impl<const B: usize> Spans<B> {
    pub fn new() -> Self {
        Spans {
            pos: 0,
            seen: 0,
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

    pub fn check_marks(&mut self) {
        let mut new_marks = HashMap::new();
        for op in &self.ops {
            if let OpType::MarkBegin(m) = &op.action {
                new_marks.insert(m.name.clone(), m.value.clone());
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
                .map(|(key, val)| (key.clone(), val.clone()))
                .collect();
            marks.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
            self.spans.push(Span {
                pos: self.seen,
                marks,
            });
        }
    }
}

impl<const B: usize> TreeQuery<B> for Spans<B> {
    /*
    fn query_node(&mut self, _child: &OpTreeNode<B>) -> QueryResult {
        unimplemented!()
    }
    */

    fn query_element_with_metadata(&mut self, element: &Op, m: &OpSetMetadata) -> QueryResult {
        // find location to insert
        // mark or set
        if element.succ.is_empty() {
            if let OpType::MarkBegin(_) = &element.action {
                let pos = self
                    .ops
                    .binary_search_by(|probe| m.lamport_cmp(probe.id, element.id))
                    .unwrap_err();
                self.ops.insert(pos, element.clone());
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
            self.seen += 1;
            self.last_seen = element.elemid();
            self.seen_at_this_mark = element.elemid();
        }
        self.pos += 1;
        QueryResult::Next
    }
}

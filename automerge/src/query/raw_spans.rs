use crate::query::{OpSetMetadata, QueryResult, TreeQuery};
use crate::types::{ElemId, Op, OpId, OpType, ScalarValue};
use std::fmt::Debug;
use serde::{ Serialize };

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RawSpans<const B: usize> {
    pos: usize,
    seen: usize,
    last_seen: Option<ElemId>,
    last_insert: Option<ElemId>,
    changed: bool,
    pub spans: Vec<RawSpan>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct RawSpan {
    #[serde(skip)]
    id: OpId,
    pub start: usize,
    pub end: usize,
    pub name: String,
    pub value: ScalarValue,
}

impl<const B: usize> RawSpans<B> {
    pub fn new() -> Self {
        RawSpans {
            pos: 0,
            seen: 0,
            last_seen: None,
            last_insert: None,
            changed: false,
            spans: Vec::new(),
        }
    }
}

impl<const B: usize> TreeQuery<B> for RawSpans<B> {

    fn query_element_with_metadata(&mut self, element: &Op, m: &OpSetMetadata) -> QueryResult {
        // find location to insert
        // mark or set
        if element.succ.is_empty() {
            if let OpType::MarkBegin(md) = &element.action {
                let pos = self
                    .spans
                    .binary_search_by(|probe| m.lamport_cmp(probe.id, element.id))
                    .unwrap_err();
                self.spans.insert(pos, RawSpan { id: element.id, start: self.seen, end: 0, name: md.name.clone(), value: md.value.clone() });
            }
            if let OpType::MarkEnd(_) = &element.action {
                for s in self.spans.iter_mut() {
                  if s.id == element.id.prev() {
                    s.end = self.seen;
                    break;
                  }
                }
            }
        }
        if element.insert {
            self.last_seen = None;
            self.last_insert = element.elemid();
        }
        if self.last_seen.is_none() && element.visible() {
            self.seen += 1;
            self.last_seen = element.elemid();
        }
        self.pos += 1;
        QueryResult::Next
    }
}

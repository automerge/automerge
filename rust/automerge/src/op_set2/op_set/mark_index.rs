use crate::types::OpId;
use super::super::op::Op;
use packer::{SpanWeight, SpanTree};
use std::collections::HashSet;


#[derive(Clone, Debug)]
pub(crate) struct MarkIndex(SpanTree<MarkSpan, ActiveMarks>);

impl MarkIndex {
  fn splice<'a, I: IntoIterator<Item=Op<'a>>>(&mut self, index: usize, mut del: usize, values: I) {
/*
    let cursor = self.0.get_where_or_last(|acc, next| index < acc.pos + next.pos);
    let mut spans = vec![];
    let mut i = cursor.index;
    let mut pos = cursor.weight.pos;
    let mut post = None;
    let mut element = cursor.element;
    if pos < index {
      assert!(index - post <= element.pos);
      if index - post == element.pos { }
    }
    loop {
      if pos < index {
        match element {
          MarkSpan::Span(w) => {}
          MarkSpan::Start(_) | MarkSpan::End(_) => {
            panic!("something is wrong with get_where");
          }
        }
      }
      if del > 0 {
        match element {
          MarkSpan::Span(w) => {}
          MarkSpan::Start(_) | MarkSpan::End(_) => {
            del -= 1;
            i += 1;
            if let Some(e) = self.0.get(i) {
              element = e;
            } else {
              break
            }
          }
        }
      }
    }
*/
  }
}

impl MarkIndex {
    pub(crate) fn new() -> Self {
        Self(SpanTree::default())
    }
}

impl Default for MarkIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
enum MarkSpan {
    Start(OpId),
    End(OpId),
    Span(u32),
}

#[derive(Clone, Default, Debug, PartialEq)]
struct ActiveMarks {
  start: HashSet<OpId>,
  end: HashSet<OpId>,
  pos: u32,
}

impl SpanWeight<MarkSpan> for ActiveMarks {
  fn alloc(span: &MarkSpan) -> ActiveMarks {
    match span {
      MarkSpan::Start(id) => { 
        ActiveMarks {
          start: HashSet::from([*id]),
          end: HashSet::default(),
          pos: 1,
        }
      }
      MarkSpan::End(id) => { 
        ActiveMarks {
          start: HashSet::default(),
          end: HashSet::from([*id]),
          pos: 1,
        }
      }
      MarkSpan::Span(n) => { 
        ActiveMarks {
          start: HashSet::default(),
          end: HashSet::default(),
          pos: *n,
        }
      }
    }
  }
  fn and(mut self, other: &Self) -> Self {
    self.union(other);
    self
  }
  fn union(&mut self, other: &Self) {
    for id in &other.end {
      if self.start.contains(id) {
        self.start.remove(id);
      } else {
        self.end.insert(*id);
      }
    }
    for id in &other.start {
      self.start.insert(*id);
    }
    self.pos += other.pos;
  }
  fn maybe_sub(&mut self, _other: &Self) -> bool {
    false
  }
}

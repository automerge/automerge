use crate::marks::Mark;
use crate::op_tree::OpSetMetadata;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{Key, ListEncoding, Op, OpId, OpType};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekMark {
    /// the mark we are looking for
    id: OpId,
    end: usize,
    encoding: ListEncoding,
    found: bool,
    mark_name: smol_str::SmolStr,
    next_mark: Mark,
    pos: usize,
    seen: usize,
    last_seen: Option<Key>,
    super_marks: HashMap<OpId, smol_str::SmolStr>,
    pub(crate) marks: Vec<Mark>,
}

impl SeekMark {
    pub(crate) fn new(id: OpId, end: usize, encoding: ListEncoding) -> Self {
        SeekMark {
            id,
            encoding,
            end,
            found: false,
            next_mark: Default::default(),
            mark_name: "".into(),
            pos: 0,
            seen: 0,
            last_seen: None,
            super_marks: Default::default(),
            marks: Default::default(),
        }
    }

    fn count_visible(&mut self, e: &Op) {
        if e.insert {
            self.last_seen = None
        }
        if e.visible() && self.last_seen.is_none() {
            self.seen += e.width(self.encoding);
            self.last_seen = Some(e.elemid_or_key())
        }
    }
}

impl TreeQuery<'_> for SeekMark {
    fn query_element_with_metadata(&mut self, op: &Op, m: &OpSetMetadata) -> QueryResult {
        match &op.action {
            OpType::MarkBegin(mark) if op.id == self.id => {
                if !op.succ.is_empty() {
                    return QueryResult::Finish;
                }
                self.found = true;
                self.mark_name = mark.name.clone();
                // retain the name and the value
                self.next_mark.name = mark.name.clone();
                self.next_mark.value = mark.value.clone();
                // change id to the end id
                self.id = self.id.next();
                // begin a new mark if nothing supersedes us
                if self.super_marks.is_empty() {
                    self.next_mark.start = self.seen;
                }
                // remove all marks that dont match
                self.super_marks.retain(|_, v| v == &mark.name);
            }
            OpType::MarkBegin(mark) => {
                if m.lamport_cmp(op.id, self.id) == Ordering::Greater {
                    if self.found {
                        // gather marks of the same type that supersede us
                        if mark.name == self.mark_name {
                            self.super_marks.insert(op.id.next(), mark.name.clone());
                            if self.super_marks.len() == 1 {
                                // complete a mark
                                self.next_mark.end = self.seen;
                                self.marks.push(self.next_mark.clone());
                            }
                        }
                    } else {
                        // gather all marks until we know what our mark's name is
                        self.super_marks.insert(op.id.next(), mark.name.clone());
                    }
                }
            }
            OpType::MarkEnd(_) if self.end == self.pos => {
                if self.super_marks.is_empty() {
                    // complete a mark
                    self.next_mark.end = self.seen;
                    self.marks.push(self.next_mark.clone());
                }
                return QueryResult::Finish;
            }
            OpType::MarkEnd(_) if self.super_marks.contains_key(&op.id) => {
                self.super_marks.remove(&op.id);
                if self.found && self.super_marks.is_empty() {
                    // begin a new mark
                    self.next_mark.start = self.seen;
                }
            }
            _ => {}
        }
        // the end op hasn't been inserted yet so we need to work off the position
        if self.end == self.pos {
            if self.super_marks.is_empty() {
                // complete a mark
                self.next_mark.end = self.seen;
                self.marks.push(self.next_mark.clone());
            }
            return QueryResult::Finish;
        }

        self.pos += 1;
        self.count_visible(op);
        QueryResult::Next
    }
}

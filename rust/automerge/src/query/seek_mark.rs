use crate::marks::Mark;
use crate::op_tree::OpSetMetadata;
use crate::query::{QueryResult, TreeQuery};
use crate::types::{Key, ListEncoding, Op, OpId, OpType};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeekMark<'a> {
    /// the mark we are looking for
    id: OpId,
    end: usize,
    encoding: ListEncoding,
    found: bool,
    mark_key: smol_str::SmolStr,
    next_mark: Option<Mark<'a>>,
    pos: usize,
    seen: usize,
    last_seen: Option<Key>,
    super_marks: HashMap<OpId, smol_str::SmolStr>,
    pub(crate) marks: Vec<Mark<'a>>,
}

impl<'a> SeekMark<'a> {
    pub(crate) fn new(id: OpId, end: usize, encoding: ListEncoding) -> Self {
        SeekMark {
            id,
            encoding,
            end,
            found: false,
            next_mark: None,
            mark_key: "".into(),
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

impl<'a> TreeQuery<'a> for SeekMark<'a> {
    fn query_element_with_metadata(&mut self, op: &'a Op, m: &OpSetMetadata) -> QueryResult {
        match &op.action {
            OpType::MarkBegin(_, data) if op.id == self.id => {
                if !op.succ.is_empty() {
                    return QueryResult::Finish;
                }
                self.found = true;
                self.mark_key = data.key.clone();
                // retain the name and the value
                self.next_mark = Some(Mark::from_data(self.seen, self.seen, data));
                // change id to the end id
                self.id = self.id.next();
                // remove all marks that dont match
                self.super_marks.retain(|_, v| v == &data.key);
            }
            OpType::MarkBegin(_, mark) => {
                if m.lamport_cmp(op.id, self.id) == Ordering::Greater {
                    if let Some(next_mark) = &mut self.next_mark {
                        // gather marks of the same type that supersede us
                        if mark.key == self.mark_key {
                            self.super_marks.insert(op.id.next(), mark.key.clone());
                            if self.super_marks.len() == 1 {
                                // complete a mark
                                next_mark.end = self.seen;
                                self.marks.push(next_mark.clone());
                            }
                        }
                    } else {
                        // gather all marks until we know what our mark's name is
                        self.super_marks.insert(op.id.next(), mark.key.clone());
                    }
                }
            }
            OpType::MarkEnd(_) if self.end == self.pos => {
                if self.super_marks.is_empty() {
                    // complete a mark
                    if let Some(next_mark) = &mut self.next_mark {
                        next_mark.end = self.seen;
                        self.marks.push(next_mark.clone());
                    }
                }
                return QueryResult::Finish;
            }
            OpType::MarkEnd(_) if self.super_marks.contains_key(&op.id) => {
                self.super_marks.remove(&op.id);
                if let Some(next_mark) = &mut self.next_mark {
                    if self.super_marks.is_empty() {
                        // begin a new mark
                        next_mark.start = self.seen;
                    }
                }
            }
            _ => {}
        }
        // the end op hasn't been inserted yet so we need to work off the position
        if self.end == self.pos {
            if self.super_marks.is_empty() {
                // complete a mark
                if let Some(next_mark) = &mut self.next_mark {
                    next_mark.end = self.seen;
                    self.marks.push(next_mark.clone());
                }
            }
            return QueryResult::Finish;
        }

        self.pos += 1;
        self.count_visible(op);
        QueryResult::Next
    }
}

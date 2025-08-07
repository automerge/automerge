use crate::{
    error::AutomergeError,
    marks::{MarkSet, RichTextQueryState},
    types::{Clock, ElemId, OpId, SequenceType},
    TextEncoding,
};

use super::{Action, Op, OpIter, OpType, QueryNth};

use std::fmt::Debug;

#[derive(Clone, Debug)]
pub(crate) struct InsertQuery<'a> {
    iter: OpIter<'a>,
    marks: RichTextQueryState<'a>,
    seq_type: SequenceType,
    text_encoding: TextEncoding,
    clock: Option<Clock>,
    candidates: Vec<Loc>,
    last_visible_cursor: Option<ElemId>,
    target: usize,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(
        iter: OpIter<'a>,
        target: usize,
        seq_type: SequenceType,
        text_encoding: TextEncoding,
        clock: Option<Clock>,
        marks: RichTextQueryState<'a>,
    ) -> Self {
        //let marks = RichTextQueryState::default();
        let mut candidates = vec![];
        let last_visible_cursor = None;
        if target == 0 {
            candidates.push(Loc::new(iter.pos(), ElemId::head()));
        };
        Self {
            iter,
            marks,
            seq_type,
            text_encoding,
            target,
            candidates,
            clock,
            last_visible_cursor,
        }
    }

    fn identify_valid_insertion_spot(&mut self, op: &Op<'a>, cursor: ElemId) {
        // first insert we see after list_state.done()
        if op.insert && self.candidates.is_empty() {
            if let Some(cursor) = self.last_visible_cursor {
                self.candidates.push(Loc::new(op.pos, cursor))
            }
        }

        // sticky marks
        if !self.candidates.is_empty() {
            // if we find a begin/end pair - ignore them
            if let OpType::MarkEnd(_) = op.action() {
                if let Some(pos) = self.candidates.iter().position(|loc| loc.matches(op)) {
                    // mark points between begin and end are invalid
                    self.candidates.truncate(pos);
                    return;
                }
            }
            if matches!(
                op.action(),
                OpType::MarkBegin(true, _) | OpType::MarkEnd(false)
            ) {
                self.candidates.push(Loc::mark(op.pos + 1, cursor, op.id));
            }
        }
    }

    // this query is particularly tricky b/c
    // we care about marks, visible ops, and non-visible ops all at once

    pub(crate) fn resolve(&mut self, mut index: usize) -> Result<QueryNth, AutomergeError> {
        let mut last_width = None;
        let mut done = index >= self.target;
        let mut pos = self.iter.pos();
        let mut post_marks = vec![];
        while let Some(mut op) = self.iter.next() {
            let op_pos = op.pos;
            if op.is_inc() {
                continue;
            }
            let visible = op.scope_to_clock(self.clock.as_ref());
            if op.insert {
                // this is the one place where we need non-visible ops
                if let Some(last) = last_width.take() {
                    index += last;
                    done = index >= self.target;
                }
            }
            let cursor = op.cursor().unwrap();
            if done {
                self.identify_valid_insertion_spot(&op, cursor);
                if visible {
                    if op.action == Action::Mark {
                        post_marks.push(op);
                    } else if !self.candidates.is_empty() {
                        break;
                    }
                }
            } else if visible {
                if !op.is_mark() {
                    self.last_visible_cursor = Some(cursor);
                    last_width = Some(op.width(self.seq_type, self.text_encoding));
                }
                self.marks.process(op, None);
            }
            pos = op_pos;
        }

        if let Some(last) = last_width.take() {
            index += last;
            done = index >= self.target;
        }

        if !done {
            Err(AutomergeError::InvalidIndex(self.target))
        } else if let Some(loc) = self.candidates.pop() {
            // process all the marks before the final pos
            for op in post_marks {
                if op.pos < loc.pos {
                    self.marks.process(op, None);
                }
            }
            Ok(QueryNth {
                pos: loc.pos,
                marks: MarkSet::from_query_state(&self.marks),
                elemid: loc.cursor,
                index,
            })
        } else if let Some(cursor) = self.last_visible_cursor {
            Ok(QueryNth {
                pos: pos + 1,
                marks: MarkSet::from_query_state(&self.marks),
                elemid: cursor,
                index,
            })
        } else {
            Err(AutomergeError::InvalidIndex(self.target))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Loc {
    pub(crate) cursor: ElemId,
    pub(crate) pos: usize,
    id: Option<OpId>,
}

impl Loc {
    fn new(pos: usize, cursor: ElemId) -> Self {
        Loc {
            cursor,
            pos,
            id: None,
        }
    }

    fn mark(pos: usize, cursor: ElemId, id: OpId) -> Self {
        Loc {
            cursor,
            pos,
            id: Some(id),
        }
    }

    fn matches(&self, op: &Op<'_>) -> bool {
        self.id == Some(op.id.prev())
    }
}

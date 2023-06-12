use crate::error::AutomergeError;
use crate::marks::RichText;
use crate::op_tree::OpTreeNode;
use crate::query::{ListState, OpSetMetadata, OpTree, QueryResult, RichTextQueryState, TreeQuery};
use crate::types::{Clock, Key, ListEncoding, Op, OpId, OpType, HEAD};
use std::fmt::Debug;
use std::rc::Rc;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertNth<'a> {
    idx: ListState,
    clock: Option<Clock>,
    last_visible_key: Option<Key>,
    candidates: Vec<Loc>,
    marks: RichTextQueryState<'a>,
}

#[derive(Debug, Clone, PartialEq)]
struct Loc {
    key: Key,
    pos: usize,
    id: Option<OpId>,
}

impl Loc {
    fn new(pos: usize, key: Key) -> Self {
        Loc { key, pos, id: None }
    }

    fn mark(pos: usize, key: Key, id: OpId) -> Self {
        Loc {
            key,
            pos,
            id: Some(id),
        }
    }

    fn matches(&self, op: &Op) -> bool {
        self.id == Some(op.id.prev())
    }
}

impl<'a> InsertNth<'a> {
    pub(crate) fn new(target: usize, encoding: ListEncoding, clock: Option<Clock>) -> Self {
        let idx = ListState::new(encoding, target);
        if target == 0 {
            InsertNth {
                idx,
                last_visible_key: None,
                candidates: vec![Loc::new(0, Key::Seq(HEAD))],
                clock,
                marks: Default::default(),
            }
        } else {
            InsertNth {
                idx,
                last_visible_key: None,
                candidates: vec![],
                clock,
                marks: Default::default(),
            }
        }
    }

    pub(crate) fn marks(&self, m: &OpSetMetadata) -> Option<Rc<RichText>> {
        RichText::from_query_state(&self.marks, m)
    }

    pub(crate) fn pos(&self) -> usize {
        self.candidates
            .last()
            .map(|loc| loc.pos)
            .unwrap_or(self.idx.pos())
    }

    pub(crate) fn key(&self) -> Result<Key, AutomergeError> {
        self.candidates
            .last()
            .map(|loc| loc.key)
            .or(self.last_visible_key)
            .ok_or(AutomergeError::InvalidIndex(self.idx.target()))
    }

    fn identify_valid_insertion_spot(&mut self, op: &'a Op, key: &Key) {
        if !self.idx.done() {
            return;
        }

        // first insert we see after idx.done()
        if op.insert && self.candidates.is_empty() && self.last_visible_key.is_some() {
            if let Some(key) = self.last_visible_key {
                self.candidates.push(Loc::new(self.idx.pos(), key))
            }
        }

        // sticky marks
        if !self.candidates.is_empty() {
            // if we find a begin/end pair - ignore them
            if let OpType::MarkEnd(_) = &op.action {
                if let Some(pos) = self.candidates.iter().position(|loc| loc.matches(op)) {
                    // mark points between begin and end are invalid
                    self.candidates.truncate(pos);
                    return;
                }
            }
            if matches!(
                op.action,
                OpType::MarkBegin(true, _) | OpType::MarkEnd(false)
            ) {
                self.candidates
                    .push(Loc::mark(self.idx.pos() + 1, *key, op.id));
            }
        }
    }
}

impl<'a> TreeQuery<'a> for InsertNth<'a> {
    fn equiv(&mut self, other: &Self) -> bool {
        self.pos() == other.pos() && self.key() == other.key()
    }

    fn can_shortcut_search(&mut self, tree: &'a OpTree) -> bool {
        if let Some(last) = &tree.last_insert {
            if last.index + last.width == self.idx.target() {
                self.candidates.push(Loc::new(last.pos + 1, last.key));
                return true;
            }
        }
        false
    }

    fn query_node(&mut self, child: &'a OpTreeNode, ops: &'a [Op]) -> QueryResult {
        self.idx.check_if_node_is_clean(child);
        if self.clock.is_none() {
            self.idx.process_node(child, ops, Some(&mut self.marks))
        } else {
            QueryResult::Descend
        }
    }

    fn query_element(&mut self, op: &'a Op) -> QueryResult {
        self.marks.process(op);
        let key = op.elemid_or_key();
        let visible = op.visible_at(self.clock.as_ref());
        self.identify_valid_insertion_spot(op, &key);
        if visible {
            if !self.candidates.is_empty() {
                return QueryResult::Finish;
            }
            self.last_visible_key = Some(key);
        }
        self.idx.process_op(op, key, visible);
        QueryResult::Next
    }
}

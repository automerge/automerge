use crate::error::AutomergeError;
use crate::marks::MarkSet;
use crate::op_set::Op;
use crate::op_tree::OpTreeNode;
use crate::query::{
    Index, ListState, OpSetData, OpTree, QueryResult, RichTextQueryState, TreeQuery,
};
use crate::types::{Clock, Key, ListEncoding, OpType, HEAD};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertNth<'a> {
    list_state: ListState,
    clock: Option<Clock>,
    last_visible_key: Option<Key>,
    candidates: Vec<Loc<'a>>,
    marks: QueriedMarks<'a>,
}

#[derive(Debug, Clone, PartialEq)]
enum QueriedMarks<'a> {
    FromQuery(RichTextQueryState<'a>),
    FromLastSeen(Arc<MarkSet>),
}

impl<'a> std::default::Default for QueriedMarks<'a> {
    fn default() -> Self {
        QueriedMarks::FromQuery(Default::default())
    }
}

#[derive(Clone, PartialEq)]
struct Loc<'a> {
    key: Key,
    pos: usize,
    id: Option<Op<'a>>,
}

impl<'a> std::fmt::Debug for Loc<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Loc {{ key: {:?}, pos: {:?}, id: {:?} }}",
            self.key,
            self.pos,
            self.id.map(|i| (i.id(), i.action()))
        )
    }
}

impl<'a> Loc<'a> {
    fn new(pos: usize, key: Key) -> Self {
        Loc { key, pos, id: None }
    }

    fn mark(pos: usize, key: Key, op: Op<'a>) -> Self {
        Loc {
            key,
            pos,
            id: Some(op),
        }
    }

    fn matches(&self, op: Op<'a>) -> bool {
        self.id.map(|o| o.id()) == Some(&op.id().prev())
    }
}

impl<'a> InsertNth<'a> {
    pub(crate) fn new(target: usize, encoding: ListEncoding, clock: Option<Clock>) -> Self {
        let list_state = ListState::new(encoding, target);
        if target == 0 {
            InsertNth {
                list_state,
                last_visible_key: None,
                candidates: vec![Loc::new(0, Key::Seq(HEAD))],
                clock,
                marks: Default::default(),
            }
        } else {
            InsertNth {
                list_state,
                last_visible_key: None,
                candidates: vec![],
                clock,
                marks: Default::default(),
            }
        }
    }

    pub(crate) fn marks(&self, osd: &OpSetData) -> Option<Arc<MarkSet>> {
        match self.marks {
            QueriedMarks::FromQuery(ref state) => MarkSet::from_query_state(state, osd),
            QueriedMarks::FromLastSeen(ref rt) => Some(rt.clone()),
        }
    }

    pub(crate) fn pos(&self) -> usize {
        self.candidates
            .last()
            .map(|loc| loc.pos)
            .unwrap_or(self.list_state.pos())
    }

    pub(crate) fn key(&self) -> Result<Key, AutomergeError> {
        self.candidates
            .last()
            .map(|loc| loc.key)
            .or(self.last_visible_key)
            .ok_or(AutomergeError::InvalidIndex(self.list_state.target()))
    }

    fn identify_valid_insertion_spot(&mut self, op: Op<'a>, key: &Key) {
        if !self.list_state.done() {
            return;
        }

        // first insert we see after list_state.done()
        if op.insert() && self.candidates.is_empty() && self.last_visible_key.is_some() {
            if let Some(key) = self.last_visible_key {
                self.candidates.push(Loc::new(self.list_state.pos(), key))
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
                if let QueriedMarks::FromQuery(ref mut marks) = self.marks {
                    marks.process(op, self.clock.as_ref());
                }
                self.candidates
                    .push(Loc::mark(self.list_state.pos() + 1, *key, op));
            }
        }
    }
}

impl<'a> TreeQuery<'a> for InsertNth<'a> {
    fn can_shortcut_search(&mut self, tree: &'a OpTree, _osd: &'a OpSetData) -> bool {
        if let Some(last) = &tree.last_insert {
            if last.index + last.width == self.list_state.target() {
                self.candidates.push(Loc::new(last.pos + 1, last.key));
                if let Some(marks) = &last.marks {
                    self.marks = QueriedMarks::FromLastSeen(marks.clone());
                }
                return true;
            }
        }
        false
    }

    fn query_node(
        &mut self,
        child: &'a OpTreeNode,
        index: &'a Index,
        osd: &'a OpSetData,
    ) -> QueryResult {
        self.list_state.check_if_node_is_clean(index);
        if self.clock.is_none() {
            let marks = match &mut self.marks {
                QueriedMarks::FromQuery(state) => Some(state),
                QueriedMarks::FromLastSeen(_) => None,
            };
            self.list_state.process_node(child, index, osd, marks)
        } else {
            QueryResult::Descend
        }
    }

    fn query_element(&mut self, op: Op<'a>) -> QueryResult {
        if !self.list_state.done() {
            if let QueriedMarks::FromQuery(ref mut marks) = self.marks {
                marks.process(op, self.clock.as_ref());
            }
        }
        let key = op.elemid_or_key();
        let visible = op.visible_at(self.clock.as_ref());
        self.identify_valid_insertion_spot(op, &key);
        if visible {
            if !self.candidates.is_empty() {
                return QueryResult::Finish;
            }
            self.last_visible_key = Some(key);
        }
        self.list_state.process_op(op, key, visible);
        QueryResult::Next
    }
}

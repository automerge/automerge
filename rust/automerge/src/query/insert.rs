use crate::error::AutomergeError;
use crate::op_tree::OpTreeNode;
use crate::query::{ListState, OpTree, QueryResult, TreeQuery};
use crate::types::{Key, ListEncoding, Op, HEAD};
use crate::OpType;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertNth {
    idx: ListState,
    valid: Option<usize>,
    last_valid_insert: Option<Key>,
}

impl InsertNth {
    pub(crate) fn new(target: usize, encoding: ListEncoding) -> Self {
        let idx = ListState::new(encoding, target);
        if target == 0 {
            InsertNth {
                idx,
                valid: Some(0),
                last_valid_insert: Some(Key::Seq(HEAD)),
            }
        } else {
            InsertNth {
                idx,
                valid: None,
                last_valid_insert: None,
            }
        }
    }

    pub(crate) fn pos(&self) -> usize {
        self.valid.unwrap_or(self.idx.pos())
    }

    pub(crate) fn key(&self) -> Result<Key, AutomergeError> {
        self.last_valid_insert
            .ok_or(AutomergeError::InvalidIndex(self.idx.target()))
    }
}

impl<'a> TreeQuery<'a> for InsertNth {
    fn equiv(&mut self, other: &Self) -> bool {
        self.pos() == other.pos() && self.key() == other.key()
    }

    fn can_shortcut_search(&mut self, tree: &'a OpTree) -> bool {
        if let Some(last) = &tree.last_insert {
            if last.index + last.width == self.idx.target() {
                self.valid = Some(last.pos + 1);
                self.last_valid_insert = Some(last.key);
                return true;
            }
        }
        false
    }

    fn query_node(&mut self, child: &OpTreeNode, ops: &[Op]) -> QueryResult {
        self.idx.check_if_node_is_clean(child);
        self.idx.process_node(child, ops)
    }

    fn query_element(&mut self, element: &Op) -> QueryResult {
        let key = element.elemid_or_key();
        let visible = element.visible();
        // an insert after we're done - could be a valid insert point
        if element.insert && self.valid.is_none() && self.idx.done() {
            self.valid = Some(self.idx.pos());
        }
        // Sticky marks, for which we scan forward until we encounter another
        // visible element, or the end of the sequence. Following these three
        // rules:
        //
        // 1. If we encounter a begin mark, insert before the begin mark
        // 2. If we encounter an end mark with expand set to true, insert before that
        //    mark
        // 3. If we encounter an end mark with expand set to false, insert after that
        //    mark
        //
        // See the notes at the bottom of this file for a much more detailed
        // description.
        if self.valid.is_some() {
            match element.action {
                OpType::MarkBegin(_, _) => {
                    return QueryResult::Finish;
                }
                OpType::MarkEnd(false) => {
                    self.valid = None;
                    self.last_valid_insert = Some(key);
                }
                _ => {}
            }
        }
        if visible {
            if self.valid.is_some() {
                return QueryResult::Finish;
            }
            self.last_valid_insert = Some(key);
        }
        self.idx.process_op(element, key, visible);
        QueryResult::Next
    }
}

// # Marks and tombstones
//
// Normally when we're inserting into a sequence we would insert after the last
// visible element before the index we are inserting at. However, if there are
// tombstones at the location we are inserting at we need to do some additional
// logic. In the following we note the state of the opset using the notation
//
// | a | (b) | <strong,expand> |  </strong,noexpand>
//
// Where 'a' represents a visible character, (b) a deleted character, <strong>
// the beginning of a mark called <strong> with the expand flag set, and
// </strong,noexpand> the end of the mark with the expand flag not set.
//
// ## Preventing non-expanding ranges from expanding
//
// Consider the following state:
//
// | T | (t) | h | e | ␣ | <link,noexpand> | f | o | x | ␣ | (j) | (u) | (m) | (p) | (e) | (d) | </link,noexpand> | . |
//
// The link mark originally contained the word "jumped" but the user deleted
// the word "jumped". If the user starts to add the word "frolicked" they
// insert the letter "f" at index 8 (after the space). If we follow our normal
// logic of inserting after the last visible element before the index we insert
// at we get this:
//
// | T | (t) | h | e | ␣ | <link,noexpand> | f | o | x | ␣ | f | (j) | (u) | (m) | (p) | (e) | (d) | </link,noexpand> | . |
//
// But now the "f" is inside the link mark, which is not intended. The noexpand
// means that link marks should not grow. To prevent this when we are inserting
// a character we first look for the first visible element before the index,
// but then we continue to scan the tombstones after the visible element (i.e.
// we scan forward until we encounter another visible element or the end of the
// sequence). If any of the tombstones are begin marks with the expand flag set
// to true, or end marks with the expand flag set to false, we insert after the
// last such tombstone. With one exception, which we cover next:
//
// ## Preventing deleted ranges from re-appearing
//
// Consider this sequence of events
//
// 1. Insert the string "hello world"
//
//   | h | e | l | l | o |   | w | o | r | l | d |
//
// 2. Mark characters 2-8 as bold, which has expand set to true
//
//   | h | e | <strong,expand> | l | l | o | w | o | r | l | </strong,expand> | d |
//
// 3. Mark characters 3-6 as a link, which has expand set to false
//
//   | h | e | <strong,expand> | l | <link,noexpand> | l | o | w | o | </link,noexpand> |r | l | </strong,expand> | d |
//
// 4. Delete characters 1-10
//
//   | h | (e) | <strong,expand> | (l) | <link,noexpand> | (l) | (o) | (w) | (o) | </link,noexpand> | (r) | (l) | </strong,expand> | (d) |
//
// 5. insert the character 'a' at the head
//
//  | a | h | (e) | <strong,expand> | (l) | <link,noexpand> | (l) | (o) | (w) | (o) | </link,noexpand> | (r) | (l) | </strong,expand> | (d) |
//
// Now, consider what happens if we follow the rule we defined above, where
// we seek forward to the last mark which is either a <,expand> or a </,noexpand>.
//
//  | h | e | <strong,expand> | (l) | <link,noexpand> | (l) | (o) | (w) | (o) | </link,noexpand> | a | (r) | (l) | </strong,expand> | (d) |
//
// But this is very surprising, the inserted 'a' is bold, even though no
// visible characters were bold. To avoid this we have another rule, when
// scanning forward after the last visible element to search for an insertion
// point, if we encounter a begin mark we insert before the begin mark.
//
// This gives rise to the following two rules which we use to scan the
// tombstones after a visible element to find an insertion point:
//
// 1. If we encounter a begin mark, insert before the begin mark
// 2. If we encounter an end mark with expand set to true, insert before that
//    element
// 3. If we encounter an end mark with expand set to false, insert after that
//    element

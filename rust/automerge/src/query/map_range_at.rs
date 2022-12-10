use crate::clock::Clock;
use crate::exid::ExId;
use crate::op_tree::{OpSetMetadata, OpTreeInternal};
use crate::types::{Key, OpId};
use crate::values::ValueIter;
use crate::{Automerge, Value};
use std::fmt::Debug;
use std::ops::RangeBounds;

use super::VisWindow;

#[derive(Debug)]
pub(crate) struct MapRangeAt<'a, R: RangeBounds<String>> {
    clock: Clock,
    window: VisWindow,

    range: R,
    index: usize,
    last_key: Option<Key>,
    next_result: Option<(&'a str, Value<'a>, OpId)>,

    index_back: usize,
    last_key_back: Option<Key>,

    op_tree: &'a OpTreeInternal,
    meta: &'a OpSetMetadata,
}

impl<'a, R: RangeBounds<String>> ValueIter<'a> for MapRangeAt<'a, R> {
    fn next_value(&mut self, doc: &'a Automerge) -> Option<(Value<'a>, ExId)> {
        self.next().map(|(_, val, id)| (val, doc.id_to_exid(id)))
    }
}

impl<'a, R: RangeBounds<String>> MapRangeAt<'a, R> {
    pub(crate) fn new(
        range: R,
        op_tree: &'a OpTreeInternal,
        meta: &'a OpSetMetadata,
        clock: Clock,
    ) -> Self {
        Self {
            clock,
            window: VisWindow::default(),
            range,
            index: 0,
            last_key: None,
            next_result: None,
            index_back: op_tree.len(),
            last_key_back: None,
            op_tree,
            meta,
        }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRangeAt<'a, R> {
    type Item = (&'a str, Value<'a>, OpId);

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.index_back {
            let op = self.op_tree.get(i)?;
            let visible = self.window.visible_at(op, i, &self.clock);
            self.index += 1;
            if visible {
                let prop = match op.key {
                    Key::Map(m) => self.meta.props.get(m),
                    Key::Seq(_) => return None, // this is a list
                };
                if self.range.contains(prop) {
                    let result = self.next_result.replace((prop, op.value(), op.id));
                    if Some(op.key) != self.last_key {
                        self.last_key = Some(op.key);
                        if result.is_some() {
                            return result;
                        }
                    }
                }
            }
        }
        self.next_result.take()
    }
}

impl<'a, R: RangeBounds<String>> DoubleEndedIterator for MapRangeAt<'a, R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        for i in (self.index..self.index_back).rev() {
            let op = self.op_tree.get(i)?;
            let visible = self.window.visible_at(op, i, &self.clock);
            self.index_back -= 1;
            if Some(op.key) != self.last_key_back && visible {
                self.last_key_back = Some(op.key);
                let prop = match op.key {
                    Key::Map(m) => self.meta.props.get(m),
                    Key::Seq(_) => return None, // this is a list
                };
                if self.range.contains(prop) {
                    return Some((prop, op.value(), op.id));
                }
            }
        }

        // we're now overlapping the index and index_back so try and take the result from the next query
        if let Some((prop, a, b)) = self.next_result.take() {
            let last_prop = match self.last_key_back {
                None => None,
                Some(Key::Map(u)) => Some(self.meta.props.get(u).as_str()),
                Some(Key::Seq(_)) => None,
            };

            // we can only use this result if we haven't ended in the prop's state (to account for
            // conflicts).
            if Some(prop) != last_prop {
                return Some((prop, a, b));
            }
        }
        None
    }
}

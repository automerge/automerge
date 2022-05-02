use crate::clock::Clock;
use crate::op_tree::{OpSetMetadata, OpTreeNode};
use crate::types::{Key, OpId};
use crate::Value;
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

    root_child: &'a OpTreeNode,
    meta: &'a OpSetMetadata,
}

impl<'a, R: RangeBounds<String>> MapRangeAt<'a, R> {
    pub(crate) fn new(
        range: R,
        root_child: &'a OpTreeNode,
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
            index_back: root_child.len(),
            last_key_back: None,
            root_child,
            meta,
        }
    }
}

impl<'a, R: RangeBounds<String>> Iterator for MapRangeAt<'a, R> {
    type Item = (&'a str, Value<'a>, OpId);

    fn next(&mut self) -> Option<Self::Item> {
        for i in self.index..self.index_back {
            let op = self.root_child.get(i)?;
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
            let op = self.root_child.get(i)?;
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
        None
    }
}

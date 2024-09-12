use super::{Op, OpIter, OpQuery, OpQueryTerm, TopOpIter, VisibleOpIter};

use std::fmt::Debug;

#[derive(Clone, Debug, Default)]
pub struct Keys<'a> {
    pub(crate) iter: Option<TopOpIter<'a, VisibleOpIter<'a, OpIter<'a>>>>,
}

impl<'a> Iterator for Keys<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.iter.as_mut()?.next()?;
        Some(
            self.iter
                .as_ref()?
                .get_opiter()
                .op_set
                .to_string(op.elemid_or_key()),
        )
    }
}

impl<'a> Keys<'a> {
    pub(crate) fn new(iter: TopOpIter<'a, VisibleOpIter<'a, OpIter<'a>>>) -> Self {
        Self { iter: Some(iter) }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KeyIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    head: Option<Op<'a>>,
    iter: I,
}

impl<'a, I: OpQuery<'a>> KeyIter<'a, I> {
    pub(crate) fn new(op: Op<'a>, iter: I) -> Self {
        KeyIter {
            head: Some(op),
            iter,
        }
    }
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> Iterator for KeyIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let head = self.head.take()?;
        if let Some(next) = self.iter.next() {
            if next.elemid_or_key() == head.elemid_or_key() {
                self.head = Some(next);
            }
        }
        Some(head)
    }
}

#[cfg(test)]
pub(crate) struct KeyOpIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    iter: I,
    next_op: Option<Op<'a>>,
}

#[cfg(test)]
impl<'a, I: Iterator<Item = Op<'a>> + Clone> KeyOpIter<'a, I> {
    pub(crate) fn new(iter: I) -> Self {
        KeyOpIter {
            iter,
            next_op: None,
        }
    }
}

#[cfg(test)]
impl<'a, I: Iterator<Item = Op<'a>> + Clone> Iterator for KeyOpIter<'a, I> {
    type Item = KeyIter<'a, I>;

    fn next(&mut self) -> Option<Self::Item> {
        let head = match self.next_op.take() {
            Some(head) => head,
            None => self.iter.next()?,
        };
        let iter = self.iter.clone();
        let key = head.elemid_or_key();
        for next in self.iter.by_ref() {
            if next.elemid_or_key() != key {
                self.next_op = Some(next);
                break;
            }
        }
        Some(KeyIter {
            head: Some(head),
            iter,
        })
    }
}

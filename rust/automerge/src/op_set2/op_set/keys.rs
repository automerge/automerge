
use super::{ Op, OpScope, TopOpIter, VisibleOpIter, OpIter, Verified };

use std::fmt::Debug;

#[derive(Default)]
pub struct Keys<'a> {
    pub(crate) iter: TopOpIter<'a, VisibleOpIter<'a, OpIter<'a, Verified>>>,
    pub(crate) op_set: Option<&'a super::OpSet>,
}

impl<'a> Iterator for Keys<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.iter.next()?;
        Some(self.op_set?.to_string(op.elemid_or_key()))
    }
}

#[derive(Debug)]
pub(crate) struct KeyIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    head: Option<Op<'a>>,
    iter: I,
}

impl<'a, I: OpScope<'a>> KeyIter<'a, I> {
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

#[derive(Default)]
pub(crate) struct KeyOpIter<'a, I: Iterator<Item = Op<'a>> + Clone> {
    iter: I,
    next_op: Option<Op<'a>>,
    count: usize,
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> KeyOpIter<'a,I> {
  pub(crate) fn new(iter: I) -> Self {
      KeyOpIter {
          iter,
          next_op: None,
          count: 0,
      }
  }
}

impl<'a, I: Iterator<Item = Op<'a>> + Clone> Iterator for KeyOpIter<'a, I> {
    type Item = KeyIter<'a, I>;

    fn next(&mut self) -> Option<Self::Item> {
        let head = match self.next_op.take() {
            Some(head) => head,
            None => self.iter.next()?,
        };
        let iter = self.iter.clone();
        //log!("KeyOpIter head = {:?}", head);
        let key = head.elemid_or_key();
        while let Some(next) = self.iter.next() {
            if next.elemid_or_key() != key {
                //log!("next_op = {:?}", next);
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


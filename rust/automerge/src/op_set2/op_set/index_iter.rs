use crate::marks::MarkSet;
use crate::types::{Clock, ListEncoding};

use super::{Op, OpIter, OpQuery, OpQueryTerm};

use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct IndexIter<'a, I: Iterator<Item = Op<'a>>> {
    iter: I,
    index: usize,
    encoding: ListEncoding,
}

impl<'a, I: Iterator<Item = Op<'a>>> IndexIter<'a, I> {
    pub(crate) fn new(iter: I, encoding: ListEncoding) -> Self {
        Self {
            iter,
            index: 0,
            encoding,
        }
    }

    pub(crate) fn unwrap(self) -> I {
        self.iter
    }
}

impl<'a, I: Iterator<Item = Op<'a>>> Iterator for IndexIter<'a, I> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|mut op| {
            op.index = self.index;
            self.index += op.width(self.encoding);
            op
        })
    }
}

impl<'a, I: OpQueryTerm<'a>> OpQueryTerm<'a> for IndexIter<'a, I> {
    fn get_opiter(&self) -> &OpIter<'a> {
        self.iter.get_opiter()
    }

    fn get_marks(&self) -> Option<&Arc<MarkSet>> {
        self.iter.get_marks()
    }
}

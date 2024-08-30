use crate::{
    exid::ExId,
    op_set2::types::{ActorIdx, ScalarValue},
    types,
    types::Clock,
};

use super::{Op, OpIter, OpQuery, OpQueryTerm};

use std::fmt::Debug;

#[derive(Debug)]
pub struct Values<'a> {
    iter: Option<Box<dyn OpQueryTerm<'a> + 'a>>,
}

impl<'a> Default for Values<'a> {
    fn default() -> Self {
        Self { iter: None }
    }
}

impl<'a> Values<'a> {
    pub(crate) fn new<I: OpQueryTerm<'a> + 'a>(iter: I, clock: Option<Clock>) -> Self {
        Self {
            iter: Some(Box::new(iter)),
        }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = (types::Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        let op = self.iter.as_mut()?.next()?;
        let value = op.value().into_owned();
        let op_set = &self.iter.as_ref()?.get_opiter().op_set;
        let id = op_set.id_to_exid(op.id);
        Some((value, id))
    }
}

use crate::{exid::ExId, types, types::Clock};

use crate::op_set2::op_set::OpQueryTerm;
use crate::op_set2::OpSet;

use std::fmt::Debug;

#[derive(Default, Debug)]
pub struct Values<'a> {
    iter: Option<(&'a OpSet, Box<dyn OpQueryTerm<'a> + 'a>)>,
}

impl<'a> Values<'a> {
    // FIXME - ignore clock?
    pub(crate) fn new<I: OpQueryTerm<'a> + 'a>(
        op_set: &'a OpSet,
        iter: I,
        _clock: Option<Clock>,
    ) -> Self {
        Self {
            iter: Some((op_set, Box::new(iter))),
        }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = (types::Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        let (op_set, iter) = self.iter.as_mut()?;
        let op = iter.next()?;
        let value = op.value().to_value();
        let id = op_set.id_to_exid(op.id);
        Some((value, id))
    }
}

use crate::exid::ExId;
use crate::{Automerge, Value};
use std::fmt;

/// An iterator over the values in an object
///
/// This is returned by the [`crate::ReadDoc::values`] and [`crate::ReadDoc::values_at`] methods
pub struct Values<'a> {
    range: Box<dyn 'a + ValueIter<'a>>,
    doc: &'a Automerge,
}

impl<'a> fmt::Debug for Values<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Values").finish()
    }
}

pub(crate) trait ValueIter<'a> {
    fn next_value(&mut self, doc: &'a Automerge) -> Option<(Value<'a>, ExId)>;
}

pub(crate) struct NoValues {}

impl<'a> ValueIter<'a> for NoValues {
    fn next_value(&mut self, _doc: &'a Automerge) -> Option<(Value<'a>, ExId)> {
        None
    }
}

impl<'a> Values<'a> {
    pub(crate) fn new<R: 'a + ValueIter<'a>>(doc: &'a Automerge, range: Option<R>) -> Self {
        if let Some(range) = range {
            Self {
                range: Box::new(range),
                doc,
            }
        } else {
            Self::empty(doc)
        }
    }

    pub(crate) fn empty(doc: &'a Automerge) -> Self {
        Self {
            range: Box::new(NoValues {}),
            doc,
        }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = (Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        self.range.next_value(self.doc)
    }
}

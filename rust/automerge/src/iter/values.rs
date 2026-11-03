use crate::{exid::ExId, types};

use crate::op_set2::op_set::OpQueryTerm;
use crate::op_set2::OpSet;

use std::fmt::Debug;

#[derive(Default, Debug)]
pub struct Values<'a> {
    iter: Option<(&'a OpSet, Box<dyn OpQueryTerm<'a> + 'a>)>,
}

impl<'a> Values<'a> {
    pub(crate) fn new<I: OpQueryTerm<'a> + 'a>(op_set: &'a OpSet, iter: I) -> Self {
        Self {
            iter: Some((op_set, Box::new(iter))),
        }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = (types::Value<'a>, ExId);

    fn next(&mut self) -> Option<Self::Item> {
        let (op_set, iter) = self.iter.as_mut()?;
        let op = iter.find(|op| !op.is_mark())?;
        let value = op.value().to_value();
        let id = op_set.id_to_exid(op.id);
        Some((value, id))
    }
}

#[cfg(test)]
mod tests {
    use crate::marks::{ExpandMark, Mark};
    use crate::transaction::Transactable;
    use crate::{Automerge, ObjType, ReadDoc, Value, ROOT};

    #[test]
    fn values_skips_marks() {
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let text = tx.put_object(&ROOT, "text", ObjType::Text).unwrap();
        tx.splice_text(&text, 0, 0, "hello world").unwrap();
        let mark = Mark::new("highlight".into(), "pink", 2, 7);
        tx.mark(&text, mark, ExpandMark::None).unwrap();
        tx.commit();
        let heads = doc.get_heads();

        let chars: Vec<_> = "hello world"
            .chars()
            .map(|c| Value::str(&c.to_string()))
            .collect();
        let values: Vec<_> = doc.values(&text).map(|(v, _)| v).collect();
        assert_eq!(values, chars);
        let values: Vec<_> = doc.values_at(&text, &heads).map(|(v, _)| v).collect();
        assert_eq!(values, chars);
    }
}

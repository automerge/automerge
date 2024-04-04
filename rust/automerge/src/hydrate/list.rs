use std::collections::HashMap;

use crate::exid::ExId;
use crate::types::Prop;
use crate::{PatchAction, ScalarValue, SequenceTree};

use super::{HydrateError, Value};

#[derive(Clone, Default, PartialEq)]
pub struct List(SequenceTree<ListValue>);

impl std::fmt::Debug for List {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.0.iter()).finish()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListValue {
    pub value: Value,
    pub marks: HashMap<String, ScalarValue>,
    pub conflict: bool,
}

impl List {
    pub fn iter(&self) -> impl Iterator<Item = &ListValue> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn apply(&mut self, patch: PatchAction) -> Result<(), HydrateError> {
        match patch {
            PatchAction::PutSeq {
                index,
                value,
                conflict,
            } => {
                *self
                    .0
                    .get_mut(index)
                    .ok_or(HydrateError::InvalidIndex(index))? =
                    ListValue::new(value.0.into(), conflict);
                Ok(())
            }
            PatchAction::Insert { index, values, .. } => {
                for (n, value) in values.into_iter().enumerate() {
                    self.0
                        .insert(index + n, ListValue::new(value.0.clone().into(), value.2));
                }
                Ok(())
            }
            PatchAction::DeleteSeq { index, length } => {
                for _ in 0..length {
                    self.0.remove(index);
                }
                Ok(())
            }
            PatchAction::Increment {
                prop: Prop::Seq(index),
                value,
            } => {
                self.0
                    .get_mut(index)
                    .ok_or(HydrateError::InvalidIndex(index))?
                    .increment(value)?;
                Ok(())
            }
            PatchAction::Mark { marks: _ } => {
                todo!()
            }
            _ => Err(HydrateError::InvalidListOp),
        }
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.0.get_mut(index).map(|lv| &mut lv.value)
    }

    pub fn get(&mut self, index: usize) -> Option<&Value> {
        self.0.get(index).map(|lv| &lv.value)
    }

    pub(crate) fn push<V: Into<Value>>(&mut self, value: V, _id: ExId, conflict: bool) {
        self.0.push(ListValue::new(value.into(), conflict))
    }

    pub(crate) fn new() -> Self {
        Self(Default::default())
    }
}

impl ListValue {
    pub(crate) fn increment(&mut self, n: i64) -> Result<(), HydrateError> {
        if let Value::Scalar(ScalarValue::Counter(c)) = &mut self.value {
            c.increment(n);
            Ok(())
        } else {
            Err(HydrateError::BadIncrement)
        }
    }

    pub(crate) fn new(value: Value, conflict: bool) -> Self {
        Self {
            value,
            conflict,
            marks: Default::default(),
        }
    }
}

impl From<Vec<Value>> for List {
    fn from(values: Vec<Value>) -> Self {
        let mut s = SequenceTree::new();
        for value in values {
            s.push(ListValue {
                value,
                conflict: false,
                marks: Default::default(),
            })
        }
        List(s)
    }
}

impl From<Vec<Value>> for Value {
    fn from(values: Vec<Value>) -> Self {
        Value::List(List::from(values))
    }
}

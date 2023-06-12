use std::collections::HashMap;

use crate::{text_value::TextValue, PatchAction, ScalarValue};

use super::{HydrateError, Value};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Text {
    value: TextValue,
    marks: HashMap<String, ScalarValue>,
}

impl Text {
    pub(crate) fn apply(&mut self, patch: PatchAction) -> Result<(), HydrateError> {
        match patch {
            PatchAction::SpliceText { index, value, .. } => {
                self.value.splice_text_value(index, &value);
                Ok(())
            }
            PatchAction::DeleteSeq { index, length } => {
                for _ in 0..length {
                    self.value.remove(index);
                }
                Ok(())
            }
            PatchAction::Mark { marks: _ } => {
                todo!()
            }
            p => Err(HydrateError::InvalidTextOp(p)),
        }
    }

    pub(crate) fn new(value: TextValue) -> Self {
        Self {
            value,
            marks: Default::default(),
        }
    }
}

impl From<TextValue> for Value {
    fn from(text: TextValue) -> Self {
        Value::Text(Text::new(text))
    }
}

impl From<&Text> for String {
    fn from(text: &Text) -> Self {
        text.value.make_string()
    }
}

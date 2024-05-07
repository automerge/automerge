use std::{collections::HashMap, fmt::Display};

use crate::{text_value::TextValue, PatchAction, ScalarValue};

use super::{HydrateError, Value};

#[derive(Clone, Default, PartialEq)]
pub struct Text {
    value: TextValue,
    marks: HashMap<String, ScalarValue>,
}

impl std::fmt::Debug for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Text")
            .field("value", &self.value.make_string())
            .field("marks", &self.marks)
            .finish()
    }
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

impl Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value.make_string())
    }
}

impl From<String> for Text {
    fn from(value: String) -> Self {
        Text::new(value.into())
    }
}

impl<'a> From<&'a str> for Text {
    fn from(value: &'a str) -> Self {
        Text::new(value.into())
    }
}

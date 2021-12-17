use std::convert::TryFrom;

use crate::legacy::{InvalidScalarValues, ScalarValue, ScalarValueKind, ScalarValues};

impl TryFrom<Vec<ScalarValue>> for ScalarValues {
    type Error = InvalidScalarValues;
    fn try_from(old_values: Vec<ScalarValue>) -> Result<Self, Self::Error> {
        let mut values: Option<ScalarValues> = None;
        for value in old_values.into_iter() {
            if let Some(ref mut xs) = values {
                if let Some(new_kind) = xs.append(value) {
                    return Err(InvalidScalarValues::UnexpectedKind(xs.kind, new_kind));
                }
            } else {
                values = Some(value.into());
            }
        }
        values.ok_or(InvalidScalarValues::Empty)
    }
}

impl From<ScalarValue> for ScalarValues {
    fn from(value: ScalarValue) -> Self {
        let kind = ScalarValueKind::from(&value);
        Self {
            vec: vec![value],
            kind,
        }
    }
}

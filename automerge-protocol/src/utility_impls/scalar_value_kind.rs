use std::fmt;

use crate::ScalarValueKind;

impl fmt::Display for ScalarValueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

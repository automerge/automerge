use crate::legacy::Diff;
use crate::ScalarValue;

impl From<&ScalarValue> for Diff {
    fn from(v: &ScalarValue) -> Self {
        Diff::Value(v.clone())
    }
}

impl From<ScalarValue> for Diff {
    fn from(v: ScalarValue) -> Self {
        Diff::Value(v)
    }
}

impl From<&str> for Diff {
    fn from(s: &str) -> Self {
        Diff::Value(s.into())
    }
}

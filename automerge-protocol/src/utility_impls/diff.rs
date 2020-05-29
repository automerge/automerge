use crate::{Diff, MapDiff, ScalarValue, SeqDiff};

impl From<MapDiff> for Diff {
    fn from(m: MapDiff) -> Self {
        Diff::Map(m)
    }
}

impl From<SeqDiff> for Diff {
    fn from(s: SeqDiff) -> Self {
        Diff::Seq(s)
    }
}

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

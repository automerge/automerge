use crate::{Diff, MapDiff, SeqDiff, Value};

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

impl From<&Value> for Diff {
    fn from(v: &Value) -> Self {
        Diff::Value(v.clone())
    }
}

impl From<Value> for Diff {
    fn from(v: Value) -> Self {
        Diff::Value(v)
    }
}

impl From<&str> for Diff {
    fn from(s: &str) -> Self {
        Diff::Value(s.into())
    }
}


use crate::RequestKey;

impl From<&str> for RequestKey {
    fn from(s: &str) -> Self {
        RequestKey::Str(s.into())
    }
}

impl From<u64> for RequestKey {
    fn from(i: u64) -> Self {
        RequestKey::Num(i)
    }
}

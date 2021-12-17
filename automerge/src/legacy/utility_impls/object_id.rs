use std::{
    cmp::{Ordering, PartialOrd},
    convert::TryFrom,
    fmt,
    str::FromStr,
};

use crate::error::InvalidObjectId;
use crate::legacy::{ObjectId, OpId};

impl PartialOrd for ObjectId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ObjectId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ObjectId::Root, ObjectId::Root) => Ordering::Equal,
            (ObjectId::Root, _) => Ordering::Less,
            (_, ObjectId::Root) => Ordering::Greater,
            (ObjectId::Id(a), ObjectId::Id(b)) => a.cmp(b),
        }
    }
}

impl From<&OpId> for ObjectId {
    fn from(o: &OpId) -> Self {
        ObjectId::Id(o.clone())
    }
}

impl From<&ObjectId> for ObjectId {
    fn from(o: &ObjectId) -> Self {
        o.clone()
    }
}

impl FromStr for ObjectId {
    type Err = InvalidObjectId;

    fn from_str(s: &str) -> Result<ObjectId, Self::Err> {
        if s == "_root" {
            Ok(ObjectId::Root)
        } else if let Ok(id) = OpId::from_str(s) {
            Ok(ObjectId::Id(id))
        } else {
            Err(InvalidObjectId(s.to_string()))
        }
    }
}

impl From<OpId> for ObjectId {
    fn from(id: OpId) -> Self {
        ObjectId::Id(id)
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectId::Root => write!(f, "_root"),
            ObjectId::Id(oid) => write!(f, "{}", oid),
        }
    }
}

impl TryFrom<&str> for ObjectId {
    type Error = InvalidObjectId;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        ObjectId::from_str(value)
    }
}

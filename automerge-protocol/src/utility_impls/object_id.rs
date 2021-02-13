use crate::error::InvalidObjectID;
use crate::{ObjectID, OpID};
use std::cmp::{Ordering, PartialOrd};
use std::fmt;
use std::{convert::TryFrom, str::FromStr};

impl PartialOrd for ObjectID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ObjectID {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ObjectID::Root, ObjectID::Root) => Ordering::Equal,
            (ObjectID::Root, _) => Ordering::Less,
            (_, ObjectID::Root) => Ordering::Greater,
            (ObjectID::ID(a), ObjectID::ID(b)) => a.cmp(b),
        }
    }
}

impl From<&OpID> for ObjectID {
    fn from(o: &OpID) -> Self {
        ObjectID::ID(o.clone())
    }
}

impl From<&ObjectID> for ObjectID {
    fn from(o: &ObjectID) -> Self {
        o.clone()
    }
}

impl FromStr for ObjectID {
    type Err = InvalidObjectID;

    fn from_str(s: &str) -> Result<ObjectID, Self::Err> {
        if s == "_root" {
            Ok(ObjectID::Root)
        } else if let Ok(id) = OpID::from_str(s) {
            Ok(ObjectID::ID(id))
        } else {
            Err(InvalidObjectID(s.to_string()))
        }
    }
}

impl From<OpID> for ObjectID {
    fn from(id: OpID) -> Self {
        ObjectID::ID(id)
    }
}

impl fmt::Display for ObjectID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectID::Root => write!(f, "_root"),
            ObjectID::ID(oid) => write!(f, "{}", oid),
        }
    }
}

impl TryFrom<&str> for ObjectID {
    type Error = InvalidObjectID;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        ObjectID::from_str(value)
    }
}

use crate::error::InvalidObjectID;
use crate::{ObjectID, OpID};
use std::fmt;
use std::{convert::TryFrom, str::FromStr};

impl From<&OpID> for ObjectID {
    fn from(o: &OpID) -> Self {
        ObjectID::ID(o.clone())
    }
}

impl FromStr for ObjectID {
    type Err = InvalidObjectID;

    fn from_str(s: &str) -> Result<ObjectID, Self::Err> {
        if s == "00000000-0000-0000-0000-000000000000" {
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
            ObjectID::Root => write!(f, "00000000-0000-0000-0000-000000000000"),
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

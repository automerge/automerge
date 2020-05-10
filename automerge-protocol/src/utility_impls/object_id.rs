use crate::{ObjectID, OpID};
use crate::error::InvalidObjectID;
use std::str::FromStr;

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


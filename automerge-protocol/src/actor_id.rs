use serde::{Deserialize, Serialize};
use std::{convert::Infallible, str::FromStr};

#[derive(Deserialize, Serialize, Eq, PartialEq, Hash, Debug, Clone, PartialOrd, Ord)]
pub struct ActorID(pub String);

impl ActorID {
    pub fn to_bytes(&self) -> Vec<u8> {
        // FIXME - I should be storing u8 internally - not strings
        // i need proper error handling for non-hex strings
        hex::decode(&self.0).unwrap()
    }

    pub fn to_string(&self) -> String {
        self.0.clone()
    }

    pub fn from_bytes(bytes: &[u8]) -> ActorID {
        ActorID(hex::encode(bytes))
    }
}

impl From<&str> for ActorID {
    fn from(s: &str) -> Self {
        ActorID(s.into())
    }
}

impl FromStr for ActorID {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ActorID(s.into()))
    }
}

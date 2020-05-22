use crate::error::InvalidActorID;
use crate::ActorID;
use std::convert::TryFrom;
use std::{fmt, str::FromStr};

impl TryFrom<&str> for ActorID {
    type Error = InvalidActorID;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        hex::decode(s)
            .map(ActorID)
            .map_err(|_| InvalidActorID(s.into()))
    }
}

impl From<&[u8]> for ActorID {
    fn from(b: &[u8]) -> Self {
        ActorID(b.to_vec())
    }
}

impl From<Vec<u8>> for ActorID {
    fn from(b: Vec<u8>) -> Self {
        ActorID(b)
    }
}

impl FromStr for ActorID {
    type Err = InvalidActorID;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ActorID::try_from(s)
    }
}

impl fmt::Display for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex_string())
    }
}

use std::{convert::TryFrom, fmt, str::FromStr};

use crate::{error::InvalidActorId, ActorId};

impl TryFrom<&str> for ActorId {
    type Error = InvalidActorId;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        hex::decode(s)
            .map(ActorId)
            .map_err(|_| InvalidActorId(s.into()))
    }
}

impl From<&[u8]> for ActorId {
    fn from(b: &[u8]) -> Self {
        ActorId(b.to_vec())
    }
}

impl From<Vec<u8>> for ActorId {
    fn from(b: Vec<u8>) -> Self {
        ActorId(b)
    }
}

impl FromStr for ActorId {
    type Err = InvalidActorId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ActorId::try_from(s)
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex_string())
    }
}

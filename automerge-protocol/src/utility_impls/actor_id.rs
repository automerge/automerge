use std::{
    convert::{TryFrom, TryInto},
    fmt,
    str::FromStr,
};

use crate::{error::InvalidActorId, ActorId};

impl From<[u8; 16]> for ActorId {
    fn from(a: [u8; 16]) -> Self {
        ActorId(a)
    }
}

impl From<uuid::Uuid> for ActorId {
    fn from(u: uuid::Uuid) -> Self {
        ActorId(*u.as_bytes())
    }
}

impl TryFrom<Vec<u8>> for ActorId {
    type Error = InvalidActorId;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        ActorId::try_from(value.as_slice())
    }
}

impl TryFrom<&[u8]> for ActorId {
    type Error = InvalidActorId;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let a = value
            .try_into()
            .map_err(|_| InvalidActorId(hex::encode(value)))?;
        Ok(ActorId(a))
    }
}

impl TryFrom<&str> for ActorId {
    type Error = InvalidActorId;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let v = hex::decode(s).map_err(|_| InvalidActorId(s.into()))?;
        ActorId::try_from(v.as_slice())
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

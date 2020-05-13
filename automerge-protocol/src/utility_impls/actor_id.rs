use crate::ActorID;
use std::{convert::Infallible, str::FromStr};

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

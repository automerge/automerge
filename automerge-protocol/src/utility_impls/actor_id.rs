use crate::ActorID;
use std::{convert::Infallible, fmt, str::FromStr};

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

impl fmt::Display for ActorID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

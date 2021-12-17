use core::fmt;
use std::{
    cmp::{Ordering, PartialOrd},
    convert::TryFrom,
    str::FromStr,
};

use crate::error::InvalidOpId;
use crate::legacy::{ActorId, OpId};

impl Ord for OpId {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 != other.0 {
            self.0.cmp(&other.0)
        } else {
            self.1.cmp(&other.1)
        }
    }
}

impl fmt::Debug for OpId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_string().as_str())
    }
}

impl fmt::Display for OpId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpId(seq, actor) => write!(f, "{}@{}", seq, actor),
        }
    }
}

impl PartialOrd for OpId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for OpId {
    type Err = InvalidOpId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut i = s.split('@');
        match (i.next(), i.next(), i.next()) {
            (Some(counter_str), Some(actor_str), None) => {
                match (counter_str.parse(), ActorId::from_str(actor_str)) {
                    (Ok(counter), Ok(actor)) => Ok(OpId(counter, actor)),
                    _ => Err(InvalidOpId(s.to_string())),
                }
            }
            _ => Err(InvalidOpId(s.to_string())),
        }
    }
}

impl TryFrom<&str> for OpId {
    type Error = InvalidOpId;
    fn try_from(s: &str) -> Result<Self, InvalidOpId> {
        OpId::from_str(s)
    }
}

impl From<&OpId> for String {
    fn from(id: &OpId) -> Self {
        id.to_string()
    }
}

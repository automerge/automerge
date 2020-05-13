use crate::error::InvalidOpID;
use crate::OpID;
use core::fmt;
use std::{
    cmp::{Ordering, PartialOrd},
    convert::TryFrom,
    str::FromStr,
};

impl Ord for OpID {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 != other.0 {
            self.0.cmp(&other.0)
        } else {
            self.1.cmp(&other.1)
        }
    }
}

impl fmt::Debug for OpID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_string().as_str())
    }
}

impl fmt::Display for OpID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpID(seq, actor) => write!(f, "{}@{}", seq, actor),
        }
    }
}

impl PartialOrd for OpID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for OpID {
    type Err = InvalidOpID;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut i = s.split('@');
        match (i.next(), i.next(), i.next()) {
            (Some(seq_str), Some(actor_str), None) => seq_str
                .parse()
                .map(|seq| OpID(seq, actor_str.to_string()))
                .map_err(|_| InvalidOpID(s.to_string())),
            _ => Err(InvalidOpID(s.to_string())),
        }
    }
}

impl TryFrom<&str> for OpID {
    type Error = InvalidOpID;
    fn try_from(s: &str) -> Result<Self, InvalidOpID> {
        OpID::from_str(s)
    }
}

impl From<&OpID> for String {
    fn from(id: &OpID) -> Self {
        id.to_string()
    }
}

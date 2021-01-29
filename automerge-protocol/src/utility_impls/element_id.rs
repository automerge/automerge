use crate::error::InvalidElementID;
use crate::{ElementID, OpID};
use std::cmp::{Ordering, PartialOrd};
use std::{convert::TryFrom, str::FromStr};

impl PartialOrd for ElementID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ElementID {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ElementID::ID(a), ElementID::ID(b)) => a.cmp(b),
            (ElementID::Head, ElementID::Head) => Ordering::Equal,
            (ElementID::Head, _) => Ordering::Less,
            (_, ElementID::Head) => Ordering::Greater,
        }
    }
}

impl From<OpID> for ElementID {
    fn from(o: OpID) -> Self {
        ElementID::ID(o)
    }
}

impl From<&OpID> for ElementID {
    fn from(o: &OpID) -> Self {
        ElementID::ID(o.clone())
    }
}

impl FromStr for ElementID {
    type Err = InvalidElementID;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "_head" => Ok(ElementID::Head),
            id => Ok(ElementID::ID(
                OpID::from_str(id).map_err(|_| InvalidElementID(id.to_string()))?,
            )),
        }
    }
}

impl TryFrom<&str> for ElementID {
    type Error = InvalidElementID;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        ElementID::from_str(value)
    }
}

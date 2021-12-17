use std::{
    cmp::{Ordering, PartialOrd},
    convert::TryFrom,
    str::FromStr,
};

use crate::error::InvalidElementId;
use crate::legacy::{ElementId, OpId};

impl PartialOrd for ElementId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ElementId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ElementId::Id(a), ElementId::Id(b)) => a.cmp(b),
            (ElementId::Head, ElementId::Head) => Ordering::Equal,
            (ElementId::Head, _) => Ordering::Less,
            (_, ElementId::Head) => Ordering::Greater,
        }
    }
}

impl From<OpId> for ElementId {
    fn from(o: OpId) -> Self {
        ElementId::Id(o)
    }
}

impl From<&OpId> for ElementId {
    fn from(o: &OpId) -> Self {
        ElementId::Id(o.clone())
    }
}

impl FromStr for ElementId {
    type Err = InvalidElementId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "_head" => Ok(ElementId::Head),
            id => Ok(ElementId::Id(
                OpId::from_str(id).map_err(|_| InvalidElementId(id.to_string()))?,
            )),
        }
    }
}

impl TryFrom<&str> for ElementId {
    type Error = InvalidElementId;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        ElementId::from_str(value)
    }
}

impl std::fmt::Display for ElementId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ElementId::Head => write!(f, "_head"),
            ElementId::Id(id) => write!(f, "{}", id),
        }
    }
}

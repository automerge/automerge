use crate::error::InvalidElementID;
use crate::{ElementID, OpID};
use std::str::FromStr;

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

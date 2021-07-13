use std::collections::HashMap;

use automerge_protocol as amp;
use serde::Serialize;

use super::Value;

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct Conflicts(HashMap<amp::OpId, Value>);

impl From<HashMap<amp::OpId, Value>> for Conflicts {
    fn from(hmap: HashMap<amp::OpId, Value>) -> Self {
        Conflicts(hmap)
    }
}

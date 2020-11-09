use crate::error::AutomergeError;
use automerge_protocol as amp;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct ObjAlias(HashMap<String, amp::ObjectID>);

impl ObjAlias {
    pub fn new() -> Self {
        ObjAlias(HashMap::new())
    }

    pub fn cache(&mut self, child: &Option<String>, id: &amp::OpID) -> Option<amp::ObjectID> {
        child.as_ref().map(|child| {
            let obj: amp::ObjectID = id.into();
            self.0.insert(child.to_string(), obj.clone());
            obj
        })
    }

    pub fn fetch(&self, name: &str) -> Result<amp::ObjectID, AutomergeError> {
        amp::ObjectID::from_str(name)
            .ok()
            .or_else(|| self.0.get(name).cloned())
            .ok_or_else(|| AutomergeError::MissingChildID(name.to_string()))
    }
}

use automerge as am;

pub enum AMresult {
    Ok,
    ObjId(am::ObjId),
    Values(Vec<am::Value>),
    Changes(Vec<am::Change>),
    Error(String),
}

impl From<Result<Option<am::ObjId>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Option<am::ObjId>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(None) => AMresult::Ok,
            Ok(Some(obj)) => AMresult::ObjId(obj),
            Err(e) => AMresult::Error(e.to_string()),
        }
    }
}

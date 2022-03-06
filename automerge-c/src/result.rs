use automerge as am;
use std::ffi::CString;

/// \struct AMresult
/// \brief A container of result codes, messages and values.
pub enum AMresult {
    Ok,
    ObjId(am::ObjId),
    Values(Vec<am::Value>),
    Changes(Vec<am::Change>),
    Error(CString),
}

impl AMresult {
    pub(crate) fn err(s: &str) -> Self {
        AMresult::Error(CString::new(s).unwrap())
    }
}

impl From<Result<Option<am::ObjId>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Option<am::ObjId>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(None) => AMresult::Ok,
            Ok(Some(obj)) => AMresult::ObjId(obj),
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

impl From<Result<(), am::AutomergeError>> for AMresult {
    fn from(maybe: Result<(), am::AutomergeError>) -> Self {
        match maybe {
            Ok(()) => AMresult::Ok,
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

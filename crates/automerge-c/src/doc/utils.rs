use std::ffi::CStr;
use std::os::raw::c_char;

macro_rules! to_actor_id {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::err("Invalid AMactorId pointer").into(),
        }
    }};
}

pub(crate) use to_actor_id;

macro_rules! to_doc {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::err("Invalid AMdoc pointer").into(),
        }
    }};
}

pub(crate) use to_doc;

macro_rules! to_doc_mut {
    ($handle:expr) => {{
        let handle = $handle.as_mut();
        match handle {
            Some(b) => b,
            None => return AMresult::err("Invalid AMdoc pointer").into(),
        }
    }};
}

pub(crate) use to_doc_mut;

macro_rules! to_obj_id {
    ($handle:expr) => {{
        match $handle.as_ref() {
            Some(obj_id) => obj_id,
            None => &automerge::ROOT,
        }
    }};
}

pub(crate) use to_obj_id;

pub(crate) unsafe fn to_str(c: *const c_char) -> String {
    if !c.is_null() {
        CStr::from_ptr(c).to_string_lossy().to_string()
    } else {
        String::default()
    }
}

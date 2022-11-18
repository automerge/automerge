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

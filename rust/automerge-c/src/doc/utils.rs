macro_rules! clamp {
    ($index:expr, $len:expr, $param_name:expr) => {{
        if $index > $len && $index != usize::MAX {
            return AMresult::error(&format!("Invalid {} {}", $param_name, $index)).into();
        }
        std::cmp::min($index, $len)
    }};
}

pub(crate) use clamp;

macro_rules! to_doc {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::error("Invalid `AMdoc*`").into(),
        }
    }};
}

pub(crate) use to_doc;

macro_rules! to_doc_mut {
    ($handle:expr) => {{
        let handle = $handle.as_mut();
        match handle {
            Some(b) => b,
            None => return AMresult::error("Invalid `AMdoc*`").into(),
        }
    }};
}

pub(crate) use to_doc_mut;

macro_rules! to_items {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::error("Invalid `AMitems*`").into(),
        }
    }};
}

pub(crate) use to_items;

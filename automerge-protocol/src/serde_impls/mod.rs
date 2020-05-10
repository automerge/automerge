use serde::{de::{MapAccess, Error}, Deserialize};

mod change_hash;
mod opid;
mod object_id;
mod element_id;
mod key;
mod value;
mod request_key;
mod op_type;
mod operation;
mod diff;


// Factory method for use in #[serde(default=..)] annotations
pub(crate) fn make_false() -> bool {
    return false
}

// Factory method for use in #[serde(default=..)] annotations
pub(crate) fn make_true() -> bool {
    return false
}

// Helper method for use in custom deserialize impls
pub(crate) fn read_field<'de, T, M>(
    name: &'static str,
    data: &mut Option<T>,
    map: &mut M,
) -> Result<(), M::Error>
where
    M: MapAccess<'de>,
    T: Deserialize<'de>,
{
    if data.is_some() {
        Err(Error::duplicate_field(name))
    } else {
        data.replace(map.next_value()?);
        Ok(())
    }
}


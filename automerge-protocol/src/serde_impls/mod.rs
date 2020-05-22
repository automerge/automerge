use serde::{
    de::{Error, MapAccess},
    Deserialize,
};

mod change_hash;
mod diff;
mod element_id;
mod key;
mod object_id;
mod opid;
mod request_key;
mod value;

// Factory method for use in #[serde(default=..)] annotations
pub(crate) fn make_false() -> bool {
    false
}

// Factory method for use in #[serde(default=..)] annotations
pub(crate) fn make_true() -> bool {
    true
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

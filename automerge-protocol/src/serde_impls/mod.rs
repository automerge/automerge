use serde::{
    de::{Error, MapAccess},
    Deserialize,
};

mod actor_id;
mod change_hash;
mod cursor_diff;
mod diff;
mod element_id;
mod key;
mod object_id;
mod op;
mod op_type;
mod opid;
mod root_diff;
mod scalar_value;

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

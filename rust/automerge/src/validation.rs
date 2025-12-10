use crate::op_set2::types::ActorIdx;
use std::fmt::Display;

pub(crate) fn nil<T: ?Sized>(_: Option<&T>) -> Option<String> {
    None
}

pub(crate) fn valid_u32<T: PartialOrd<u64> + Display>(v: Option<&T>) -> Option<String> {
    match v {
        Some(v) if *v > u32::MAX as u64 => Some(format!("value too large : {}", v)),
        _ => None,
    }
}

// TODO this only checks the RAW delta value - requires smarter delta type :/
pub(crate) fn ivalid_u32<T: PartialOrd<i64> + Display>(v: Option<&T>) -> Option<String> {
    match v {
        Some(v) if *v > u32::MAX as i64 => Some(format!("value too large : {}", v)),
        _ => None,
    }
}

pub(crate) fn valid_actors(num_actors: usize) -> impl Fn(Option<&ActorIdx>) -> Option<String> {
    let num_actors = num_actors as u32;
    move |a: Option<&ActorIdx>| match a {
        Some(a) if a.0 >= num_actors => Some(format!("Invalid Actor {:?}", a)),
        _ => None,
    }
}

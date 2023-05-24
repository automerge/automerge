use crate::storage::parse;
use crate::types::{ObjId, OpId};
use crate::ActorId;
use serde::Serialize;
use serde::Serializer;
use std::cmp::{Ord, Ordering};
use std::fmt;
use std::hash::{Hash, Hasher};

/// An identifier for an object in a document
///
/// This can be persisted using `to_bytes` and `TryFrom<&[u8]>` breaking changes to the
/// serialization format will be considered breaking changes for this library version.
#[derive(Debug, Clone)]
pub enum ExId {
    Root,
    Id(u64, ActorId, usize),
}

const SERIALIZATION_VERSION_TAG: u8 = 0;
const TYPE_ROOT: u8 = 0;
const TYPE_ID: u8 = 1;

impl ExId {
    /// Serialize this object ID to a byte array.
    ///
    /// This serialization format is versioned and incompatible changes to it will be considered a
    /// breaking change for the version of this library.
    pub fn to_bytes(&self) -> Vec<u8> {
        // The serialized format is
        //
        // .--------------------------------.
        // | version   | type   | data      |
        // +--------------------------------+
        // |  4 bytes  |4 bytes | variable  |
        // '--------------------------------'
        //
        // Version is currently always `0`
        //
        // `data` depends on the type
        //
        // * If the type is `TYPE_ROOT` (0) then there is no data
        // * If the type is `TYPE_ID` (1) then the data is
        //
        // .-------------------------------------------------------.
        // | actor ID len | actor ID bytes | counter | actor index |
        // '-------------------------------------------------------'
        //
        // Where the actor ID len, counter, and actor index are all uLEB encoded
        // integers. The actor ID bytes is just an array of bytes.
        //
        match self {
            ExId::Root => {
                let val: u8 = SERIALIZATION_VERSION_TAG | (TYPE_ROOT << 4);
                vec![val]
            }
            ExId::Id(id, actor, counter) => {
                let actor_bytes = actor.to_bytes();
                let mut bytes = Vec::with_capacity(actor_bytes.len() + 4 + 4);
                let tag = SERIALIZATION_VERSION_TAG | (TYPE_ID << 4);
                bytes.push(tag);
                leb128::write::unsigned(&mut bytes, actor_bytes.len() as u64).unwrap();
                bytes.extend_from_slice(actor_bytes);
                leb128::write::unsigned(&mut bytes, *counter as u64).unwrap();
                leb128::write::unsigned(&mut bytes, *id).unwrap();
                bytes
            }
        }
    }

    pub(crate) fn to_internal_obj(&self) -> ObjId {
        match self {
            ExId::Root => ObjId::root(),
            ExId::Id(ctr, _, actor) => ObjId(OpId::new(*ctr, *actor)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObjIdFromBytesError {
    #[error("no version tag")]
    NoVersion,
    #[error("invalid version tag")]
    InvalidVersion(u8),
    #[error("invalid type tag")]
    InvalidType(u8),
    #[error("invalid Actor ID length: {0}")]
    ParseActorLen(String),
    #[error("Not enough bytes in actor ID")]
    ParseActor,
    #[error("invalid counter: {0}")]
    ParseCounter(String),
    #[error("invalid actor index hint: {0}")]
    ParseActorIdxHint(String),
}

impl<'a> TryFrom<&'a [u8]> for ExId {
    type Error = ObjIdFromBytesError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let i = parse::Input::new(value);
        let (i, tag) = parse::take1::<()>(i).map_err(|_| ObjIdFromBytesError::NoVersion)?;
        let version = tag & 0b1111;
        if version != SERIALIZATION_VERSION_TAG {
            return Err(ObjIdFromBytesError::InvalidVersion(version));
        }
        let type_tag = tag >> 4;
        match type_tag {
            TYPE_ROOT => Ok(ExId::Root),
            TYPE_ID => {
                let (i, len) = parse::leb128_u64::<parse::leb128::Error>(i)
                    .map_err(|e| ObjIdFromBytesError::ParseActorLen(e.to_string()))?;
                let (i, actor) = parse::take_n::<()>(len as usize, i)
                    .map_err(|_| ObjIdFromBytesError::ParseActor)?;
                let (i, counter) = parse::leb128_u64::<parse::leb128::Error>(i)
                    .map_err(|e| ObjIdFromBytesError::ParseCounter(e.to_string()))?;
                let (_i, actor_idx_hint) = parse::leb128_u64::<parse::leb128::Error>(i)
                    .map_err(|e| ObjIdFromBytesError::ParseActorIdxHint(e.to_string()))?;
                Ok(Self::Id(actor_idx_hint, actor.into(), counter as usize))
            }
            other => Err(ObjIdFromBytesError::InvalidType(other)),
        }
    }
}

impl PartialEq for ExId {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ExId::Root, ExId::Root) => true,
            (ExId::Id(ctr1, actor1, _), ExId::Id(ctr2, actor2, _))
                if ctr1 == ctr2 && actor1 == actor2 =>
            {
                true
            }
            _ => false,
        }
    }
}

impl Eq for ExId {}

impl fmt::Display for ExId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExId::Root => write!(f, "_root"),
            ExId::Id(ctr, actor, _) => write!(f, "{}@{}", ctr, actor),
        }
    }
}

impl Hash for ExId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ExId::Root => 0.hash(state),
            ExId::Id(ctr, actor, _) => {
                ctr.hash(state);
                actor.hash(state);
            }
        }
    }
}

impl Serialize for ExId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

impl AsRef<ExId> for ExId {
    fn as_ref(&self) -> &ExId {
        self
    }
}

impl Ord for ExId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ExId::Root, ExId::Root) => Ordering::Equal,
            (ExId::Root, _) => Ordering::Less,
            (_, ExId::Root) => Ordering::Greater,
            (ExId::Id(c1, a1, _), ExId::Id(c2, a2, _)) if c1 == c2 => a1.cmp(a2),
            (ExId::Id(c1, _, _), ExId::Id(c2, _, _)) => c1.cmp(c2),
        }
    }
}

impl PartialOrd for ExId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::ExId;
    use proptest::prelude::*;

    use crate::ActorId;

    fn gen_actorid() -> impl Strategy<Value = ActorId> {
        proptest::collection::vec(any::<u8>(), 0..100).prop_map(ActorId::from)
    }

    prop_compose! {
        fn gen_non_root_objid()(actor in gen_actorid(), counter in any::<usize>(), idx in any::<usize>()) -> ExId {
            ExId::Id(idx as u64, actor, counter)
        }
    }

    fn gen_obji() -> impl Strategy<Value = ExId> {
        prop_oneof![Just(ExId::Root), gen_non_root_objid()]
    }

    proptest! {
        #[test]
        fn objid_roundtrip(objid in gen_obji()) {
            let bytes = objid.to_bytes();
            let objid2 = ExId::try_from(&bytes[..]).unwrap();
            assert_eq!(objid, objid2);
        }
    }

    #[test]
    fn test_root_roundtrip() {
        let bytes = ExId::Root.to_bytes();
        let objid2 = ExId::try_from(&bytes[..]).unwrap();
        assert_eq!(ExId::Root, objid2);
    }
}

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
///
/// `Id` holds the counter, the actor, and a hint for the actor's index in the document's
/// actors. The hint is `Some` when the document produced this ID, and `None` when it was
/// read from bytes or the index doesn't fit in a `u32`. Equality, ordering, hashing,
/// formatting and serialization all ignore it.
#[derive(Debug, Clone)]
pub enum ExId {
    Root,
    Id(u64, ActorId, Option<u32>),
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
        // | actor ID len | actor ID bytes | actor index | counter |
        // '-------------------------------------------------------'
        //
        // Where the actor ID len, actor index, and counter are all uLEB encoded
        // integers. The actor ID bytes is just an array of bytes.
        //
        // The actor index is always written as `0`. It was once the index hint, which
        // depends on the document's other actors, so the same object could serialize
        // to different bytes. It's kept so that the format doesn't change, and ignored
        // when reading.
        //
        match self {
            ExId::Root => {
                let val: u8 = SERIALIZATION_VERSION_TAG | (TYPE_ROOT << 4);
                vec![val]
            }
            ExId::Id(counter, actor, _) => {
                let actor_bytes = actor.to_bytes();
                let mut bytes = Vec::with_capacity(actor_bytes.len() + 4 + 4);
                let tag = SERIALIZATION_VERSION_TAG | (TYPE_ID << 4);
                bytes.push(tag);
                leb128::write::unsigned(&mut bytes, actor_bytes.len() as u64).unwrap();
                bytes.extend_from_slice(actor_bytes);
                leb128::write::unsigned(&mut bytes, 0).unwrap();
                leb128::write::unsigned(&mut bytes, *counter).unwrap();
                bytes
            }
        }
    }

    /// Convert to an internal object ID using the actor index hint
    ///
    /// The hint isn't checked against the document's actors, so this is only for IDs the
    /// document produced. Anything else goes through `Automerge::exid_to_obj`.
    ///
    /// # Panics
    ///
    /// If this is an `Id` without a hint.
    pub(crate) fn to_internal_obj(&self) -> ObjId {
        match self {
            ExId::Root => ObjId::root(),
            ExId::Id(ctr, _, Some(actor)) => ObjId(OpId::new(*ctr, *actor as usize)),
            ExId::Id(_, _, None) => {
                panic!("ExId without an actor index hint can't be converted to an ObjId")
            }
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
                // The hint is read so that the bytes are validated, but not used: it
                // may be stale, or come from another document.
                let (i, _actor_idx_hint) = parse::leb128_u64::<parse::leb128::Error>(i)
                    .map_err(|e| ObjIdFromBytesError::ParseActorIdxHint(e.to_string()))?;
                let (_i, counter) = parse::leb128_u64::<parse::leb128::Error>(i)
                    .map_err(|e| ObjIdFromBytesError::ParseCounter(e.to_string()))?;
                Ok(Self::Id(counter, actor.into(), None))
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
    use std::cmp::Ordering;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use crate::ActorId;

    fn hash(id: &ExId) -> u64 {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        hasher.finish()
    }

    fn gen_actorid() -> impl Strategy<Value = ActorId> {
        proptest::collection::vec(any::<u8>(), 0..100).prop_map(ActorId::from)
    }

    prop_compose! {
        fn gen_non_root_objid()(actor in gen_actorid(), counter in any::<u64>(), hint in any::<Option<u32>>()) -> ExId {
            ExId::Id(counter, actor, hint)
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

        #[test]
        fn to_bytes_ignores_the_hint(actor in gen_actorid(), counter in any::<u64>(), hint in any::<u32>()) {
            let with_hint = ExId::Id(counter, actor.clone(), Some(hint));
            let without_hint = ExId::Id(counter, actor, None);
            assert_eq!(with_hint.to_bytes(), without_hint.to_bytes());
        }

        #[test]
        fn comparisons_ignore_the_hint(
            actor in gen_actorid(),
            counter in any::<u64>(),
            hint1 in any::<Option<u32>>(),
            hint2 in any::<Option<u32>>(),
        ) {
            let a = ExId::Id(counter, actor.clone(), hint1);
            let b = ExId::Id(counter, actor.clone(), hint2);
            assert_eq!(a, b);
            assert_eq!(hash(&a), hash(&b));
            assert_eq!(a.cmp(&b), Ordering::Equal);
            assert_eq!(a.to_string(), b.to_string());

            // Whatever the hints, a different counter or actor is a different ID
            let other_counter = ExId::Id(counter.wrapping_add(1), actor.clone(), hint1);
            let mut other_actor_bytes = actor.to_bytes().to_vec();
            other_actor_bytes.push(0);
            let other_actor = ExId::Id(counter, ActorId::from(other_actor_bytes), hint1);
            for other in [other_counter, other_actor] {
                assert_ne!(other, b);
                assert_ne!(other.cmp(&b), Ordering::Equal);
                assert_ne!(other.to_string(), b.to_string());
            }
        }

        #[test]
        fn try_from_gives_no_hint(objid in gen_non_root_objid()) {
            let bytes = objid.to_bytes();
            let ExId::Id(_, _, hint) = ExId::try_from(&bytes[..]).unwrap() else {
                panic!("expected an ExId::Id");
            };
            assert_eq!(hint, None);
        }
    }

    #[test]
    fn test_root_roundtrip() {
        let bytes = ExId::Root.to_bytes();
        let objid2 = ExId::try_from(&bytes[..]).unwrap();
        assert_eq!(ExId::Root, objid2);
    }

    #[test]
    fn reads_bytes_with_a_real_hint() {
        // Written by an older version, with a hint of 1 and a counter of 1
        let bytes = hex::decode("1010ffffffffffffffffffffffffffffffff0101").unwrap();
        let ExId::Id(counter, actor, hint) = ExId::try_from(&bytes[..]).unwrap() else {
            panic!("expected an ExId::Id");
        };
        assert_eq!(counter, 1);
        assert_eq!(actor, ActorId::from([0xff; 16]));
        assert_eq!(hint, None);
    }

    #[test]
    fn writes_zero_for_the_hint() {
        let objid = ExId::Id(1, ActorId::from([0xff; 16]), Some(1));
        assert_eq!(
            hex::encode(objid.to_bytes()),
            "1010ffffffffffffffffffffffffffffffff0001"
        );
    }

    #[test]
    #[should_panic(expected = "without an actor index hint")]
    fn to_internal_obj_panics_without_a_hint() {
        ExId::Id(1, ActorId::from([0xff; 16]), None).to_internal_obj();
    }
}

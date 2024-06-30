use crate::op_set::OpSetData;
use crate::storage::parse;
use crate::types::OpId;
#[cfg(doc)]
use crate::ReadDoc;
use crate::{ActorId, AutomergeError};
use std::fmt;

/// An identifier of a position in a Sequence (either Self::List or Self::Text).
///
/// Every element in an Automerge Sequence can be internally identified with an operation ID.
/// While ExId is our default external representation of the Operation ID, it can be quite heavy.
/// Therefore, we use this lightweight specialized structure.
///
/// This can be persisted using [`Self::to_bytes()`] and [`TryFrom<&[u8]>`][TryFrom].
///
/// A cursor is obtained from [`ReadDoc::get_cursor()`] and dereferenced with
/// [`ReadDoc::get_cursor_position()`].
#[derive(Clone, PartialEq, Debug)]
pub struct Cursor {
    ctr: u64,
    actor: ActorId,
}

const SERIALIZATION_VERSION_TAG: u8 = 0;

impl Cursor {
    pub(crate) fn new(id: OpId, osd: &OpSetData) -> Self {
        Self {
            ctr: id.counter(),
            actor: osd.actors.cache[id.actor()].clone(),
        }
    }

    pub(crate) fn actor(&self) -> &ActorId {
        &self.actor
    }

    pub(crate) fn ctr(&self) -> u64 {
        self.ctr
    }

    fn from_str(s: &str) -> Option<Self> {
        let n = s.find('@')?;
        let ctr = s[0..n].parse().ok()?;
        let actor = s[(n + 1)..].try_into().ok()?;
        Some(Cursor { ctr, actor })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        // The serialized format is
        //
        // .----------------------------------------------------------------.
        // | version   | actorId len     | actorId bytes | counter          |
        // +----------------------------------------------------------------+
        // |  1 byte   | unsigned leb128 | variable      | unsigned leb128  |
        // '----------------------------------------------------------------'
        //
        // Version is currently always `0`
        //
        let actor_bytes = self.actor.to_bytes();
        let mut bytes = Vec::with_capacity(actor_bytes.len() + 4 + 4 + 1);
        bytes.push(SERIALIZATION_VERSION_TAG);
        leb128::write::unsigned(&mut bytes, actor_bytes.len() as u64).unwrap();
        bytes.extend_from_slice(actor_bytes);
        leb128::write::unsigned(&mut bytes, self.ctr).unwrap();
        bytes
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.ctr, self.actor)
    }
}

impl TryFrom<&str> for Cursor {
    type Error = AutomergeError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Cursor::from_str(s).ok_or_else(|| AutomergeError::InvalidCursorFormat)
    }
}

impl TryFrom<String> for Cursor {
    type Error = AutomergeError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.as_str().try_into()
    }
}

impl<'a> TryFrom<&'a [u8]> for Cursor {
    type Error = AutomergeError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let i = parse::Input::new(value);
        let (i, version) =
            parse::take1::<()>(i).map_err(|_| AutomergeError::InvalidCursorFormat)?;
        if version != SERIALIZATION_VERSION_TAG {
            return Err(AutomergeError::InvalidCursorFormat);
        }
        let (i, len) = parse::leb128_u64::<parse::leb128::Error>(i)
            .map_err(|_| AutomergeError::InvalidCursorFormat)?;
        let (i, actor) = parse::take_n::<()>(len as usize, i)
            .map_err(|_| AutomergeError::InvalidCursorFormat)?;
        let (_i, ctr) = parse::leb128_u64::<parse::leb128::Error>(i)
            .map_err(|_| AutomergeError::InvalidCursorFormat)?;
        Ok(Self {
            ctr,
            actor: actor.into(),
        })
    }
}

impl TryFrom<Vec<u8>> for Cursor {
    type Error = AutomergeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}

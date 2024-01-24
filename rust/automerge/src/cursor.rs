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

#[derive(PartialEq, Debug, Clone)]
pub enum TextRange {
    RangeFull,
    Range { start: TextPos, end: TextPos },
    RangeFrom { start: TextPos },
    RangeTo { end: TextPos },
    RangeInclusive { start: TextPos, end: TextPos },
    RangeToInclusive { end: TextPos },
    RangeExclusive { start: TextPos, end: TextPos },
    RangeFromExclusive { start: TextPos },
}

impl TextRange {
    pub(crate) fn start(&self) -> Option<&TextPos> {
        match self {
            TextRange::RangeFull => None,
            TextRange::Range { start, .. } => Some(start),
            TextRange::RangeFrom { start } => Some(start),
            TextRange::RangeTo { .. } => None,
            TextRange::RangeInclusive { start, .. } => Some(start),
            TextRange::RangeToInclusive { .. } => None,
            TextRange::RangeExclusive { start, .. } => Some(start),
            TextRange::RangeFromExclusive { start } => Some(start),
        }
    }

    pub(crate) fn end(&self) -> Option<&TextPos> {
        match self {
            TextRange::RangeFull => None,
            TextRange::Range { end, .. } => Some(end),
            TextRange::RangeFrom { .. } => None,
            TextRange::RangeTo { end } => Some(end),
            TextRange::RangeInclusive { end, .. } => Some(end),
            TextRange::RangeToInclusive { end } => Some(end),
            TextRange::RangeExclusive { end, .. } => Some(end),
            TextRange::RangeFromExclusive { .. } => None,
        }
    }
}

fn unpack_range_str(
    s: &str,
    pat: &str,
) -> Result<(Option<TextPos>, Option<TextPos>), AutomergeError> {
    let mut iter = s.split(pat);
    let part1 = iter.next();
    let part2 = iter.next();
    let extra = iter.next();
    match (part1, part2, extra) {
        (Some(""), Some(""), None) => Ok((None, None)),
        (Some(""), Some(b), None) => Ok((None, Some(b.try_into()?))),
        (Some(a), Some(""), None) => Ok((Some(a.try_into()?), None)),
        (Some(a), Some(b), None) => Ok((Some(a.try_into()?), Some(b.try_into()?))),
        _ => Err(AutomergeError::InvalidCursorFormat),
    }
}

impl TryFrom<&str> for TextRange {
    type Error = AutomergeError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match unpack_range_str(s, "...") {
            Ok((Some(start), Some(end))) => return Ok(TextRange::RangeExclusive { start, end }),
            Ok((Some(start), None)) => return Ok(TextRange::RangeFromExclusive { start }),
            Ok((None, Some(end))) => return Ok(TextRange::RangeTo { end }),
            Ok((None, None)) => return Ok(TextRange::RangeFull),
            _ => {}
        }
        match unpack_range_str(s, "..=") {
            Ok((Some(start), Some(end))) => return Ok(TextRange::RangeInclusive { start, end }),
            Ok((None, Some(end))) => return Ok(TextRange::RangeToInclusive { end }),
            Ok((Some(start), None)) => return Ok(TextRange::RangeFrom { start }),
            Ok((None, None)) => return Ok(TextRange::RangeFull),
            _ => {}
        }
        match unpack_range_str(s, "..") {
            Ok((Some(start), Some(end))) => return Ok(TextRange::Range { start, end }),
            Ok((None, Some(end))) => return Ok(TextRange::RangeTo { end }),
            Ok((Some(start), None)) => return Ok(TextRange::RangeFrom { start }),
            Ok((None, None)) => return Ok(TextRange::RangeFull),
            _ => {}
        }
        Err(AutomergeError::InvalidCursorFormat)
    }
}

impl TryFrom<String> for TextRange {
    type Error = AutomergeError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_from(s.as_ref())
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum TextPos {
    Index(usize),
    Cursor(Cursor),
}

impl From<usize> for TextPos {
    fn from(index: usize) -> Self {
        TextPos::Index(index)
    }
}

impl From<Cursor> for TextPos {
    fn from(cursor: Cursor) -> Self {
        TextPos::Cursor(cursor)
    }
}

impl From<&Cursor> for TextPos {
    fn from(cursor: &Cursor) -> Self {
        TextPos::Cursor(cursor.clone())
    }
}

impl TryFrom<&str> for TextPos {
    type Error = AutomergeError;

    fn try_from(s: &str) -> Result<Self, AutomergeError> {
        if let Ok(n) = s.parse::<usize>() {
            Ok(TextPos::Index(n))
        } else if let Ok(c) = Cursor::try_from(s) {
            Ok(TextPos::Cursor(c))
        } else {
            Err(AutomergeError::InvalidCursorFormat)
        }
    }
}

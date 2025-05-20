use crate::op_set2::OpSet;
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
/// A cursor is obtained from [`ReadDoc::get_cursor()`] or [`ReadDoc::get_cursor_moving()`] and
/// is dereferenced to a position using [`ReadDoc::get_cursor_position()`].
#[derive(Clone, PartialEq, Debug)]
pub enum Cursor {
    // cursor always dereferences to position = 0
    Start,

    // cursor always dereferences to position = sequence.length
    End,

    // cursor is attached to a specific op
    Op(OpCursor),
}

/// A cursor which represents a specific op in a sequence.
#[derive(Clone, PartialEq, Debug)]
pub struct OpCursor {
    pub(crate) ctr: u64,
    pub(crate) actor: ActorId,
    pub(crate) move_cursor: MoveCursor,
}

impl OpCursor {
    pub(crate) fn new(id: OpId, op_set: &OpSet, move_cursor: MoveCursor) -> Self {
        OpCursor {
            ctr: id.counter(),
            actor: op_set.actors[id.actor()].clone(),
            move_cursor,
        }
    }
}

/// Locations in sequences that a cursor can represent.
#[derive(Debug)]
pub enum CursorPosition {
    Start,
    End,
    Index(usize),
}

impl From<usize> for CursorPosition {
    fn from(value: usize) -> Self {
        Self::Index(value)
    }
}

/// `MoveCursor` determines how the cursor will resolve its position if the item originally referenced by the cursor is removed.
///
/// With `MoveCursor::Before`, the cursor will shift to the **previous item that was visible at the time of cursor creation.**.
/// If no previous item is found that's still visible, the cursor will dereference to `0`.
///
/// With `MoveCursor::After`, the cursor will shift to the **next item that was visible at the time of cursor creation.**
/// If no next item is found that's still visible, the cursor will dereference to `sequence.length`.
#[derive(Clone, PartialEq, Debug)]
pub enum MoveCursor {
    Before,
    After,
}

impl Default for MoveCursor {
    fn default() -> Self {
        Self::After
    }
}

const VERSION_TAG: u8 = 1;

const START_TAG: u8 = 1;
const END_TAG: u8 = 2;
const OP_TAG: u8 = 3;

const MOVE_BEFORE_TAG: u8 = 1;
const MOVE_AFTER_TAG: u8 = 2;

impl Cursor {
    fn from_str(s: &str) -> Option<Self> {
        if s.len() == 1 {
            // start and end cursors are just "s" and "e" respectively
            match s {
                "s" => Some(Self::Start),
                "e" => Some(Self::End),
                _ => None,
            }
        } else {
            // MoveCursor::Before is prefixed with '-'
            let (move_cursor, i) = match &s[0..1] {
                "-" => (MoveCursor::Before, 1),
                _ => (MoveCursor::After, 0),
            };

            let n = s.find('@')?;
            let ctr = s[i..n].parse().ok()?;
            let actor = s[(n + 1)..].try_into().ok()?;

            Some(Self::Op(OpCursor {
                ctr,
                actor,
                move_cursor,
            }))
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        /*
        EBNF:

        byte = %x00-FF
                ; any octet
        bytes = 1*byte
                ; any sequence of octets
        uleb128 = 1*8( %x00-7F ) / ( %x80-FF bytes )
                ; unsigned LEB128 encoding

        ; tags:
        version   = %d01 ; for version 1

        start     = %d01 ; Cursor::Start
        end       = %d02 ; Cursor::End
        opid_tag  = %d03 ; Cursor::Op

        move      = %d01 ; MoveCursor::Before
                  / %d02 ; MoveCursor::After

        counter = uleb128
        actor_id = uleb128 bytes ; length prefixed bytes of the actor ID

        opid = opid_tag actor_id counter move

        cursor = version (start / end / opid)
         */

        match self {
            Self::Start => vec![VERSION_TAG, START_TAG],
            Self::End => vec![VERSION_TAG, END_TAG],
            Self::Op(OpCursor {
                ctr,
                actor,
                move_cursor,
            }) => {
                let actor_bytes = actor.to_bytes();

                // (version + opid_tag + uleb128 + bytes + counter + move)
                let mut bytes = Vec::with_capacity(1 + 1 + 8 + actor_bytes.len() + 8 + 1);

                bytes.push(VERSION_TAG);
                bytes.push(OP_TAG);

                leb128::write::unsigned(&mut bytes, actor_bytes.len() as u64).unwrap();
                bytes.extend_from_slice(actor_bytes);
                leb128::write::unsigned(&mut bytes, *ctr).unwrap();

                bytes.push(match move_cursor {
                    MoveCursor::Before => MOVE_BEFORE_TAG,
                    MoveCursor::After => MOVE_AFTER_TAG,
                });

                bytes
            }
        }
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start => write!(f, "s"),
            Self::End => write!(f, "e"),
            Self::Op(op_cursor) => op_cursor.fmt(f),
        }
    }
}

impl fmt::Display for OpCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}@{}",
            match self.move_cursor {
                MoveCursor::Before => "-",
                MoveCursor::After => "",
            },
            self.ctr,
            self.actor
        )
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

        if version == 0 {
            return parse_0(i);
        } else if version != VERSION_TAG {
            return Err(AutomergeError::InvalidCursorFormat);
        }

        let (i, cursor_type) =
            parse::take1::<()>(i).map_err(|_| AutomergeError::InvalidCursorFormat)?;

        match cursor_type {
            START_TAG => Ok(Self::Start),
            END_TAG => Ok(Self::End),
            OP_TAG => {
                let (i, len) = parse::leb128_u64::<parse::leb128::Error>(i)
                    .map_err(|_| AutomergeError::InvalidCursorFormat)?;
                let (i, actor) = parse::take_n::<()>(len as usize, i)
                    .map_err(|_| AutomergeError::InvalidCursorFormat)?;
                let (i, ctr) = parse::leb128_u64::<parse::leb128::Error>(i)
                    .map_err(|_| AutomergeError::InvalidCursorFormat)?;
                let (_i, move_type) =
                    parse::take1::<()>(i).map_err(|_| AutomergeError::InvalidCursorFormat)?;

                Ok(Self::Op(OpCursor {
                    ctr,
                    actor: actor.into(),
                    move_cursor: match move_type {
                        MOVE_AFTER_TAG => MoveCursor::After,
                        MOVE_BEFORE_TAG => MoveCursor::Before,
                        _ => return Err(AutomergeError::InvalidCursorFormat),
                    },
                }))
            }
            _ => Err(AutomergeError::InvalidCursorFormat),
        }
    }
}

fn parse_0(i: parse::Input<'_>) -> Result<Cursor, AutomergeError> {
    // version = 0 serialized format:
    //
    // .----------------------------------------------------------------.
    // | version   | actorId len     | actorId bytes | counter          |
    // +----------------------------------------------------------------+
    // |  1 byte   | unsigned leb128 | variable      | unsigned leb128  |
    // '----------------------------------------------------------------'
    //
    let (i, len) = parse::leb128_u64::<parse::leb128::Error>(i)
        .map_err(|_| AutomergeError::InvalidCursorFormat)?;
    let (i, actor) =
        parse::take_n::<()>(len as usize, i).map_err(|_| AutomergeError::InvalidCursorFormat)?;
    let (_i, ctr) = parse::leb128_u64::<parse::leb128::Error>(i)
        .map_err(|_| AutomergeError::InvalidCursorFormat)?;

    // `MoveCursor::After` was the default behavior of cursors in version 0
    // and there was no notion of start/end cursors
    Ok(Cursor::Op(OpCursor {
        ctr,
        actor: actor.into(),
        move_cursor: MoveCursor::After,
    }))
}

impl TryFrom<Vec<u8>> for Cursor {
    type Error = AutomergeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}

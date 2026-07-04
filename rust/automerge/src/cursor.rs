use crate::op_set2::OpSet;
use crate::storage::parse;
use crate::types::OpId;
#[cfg(doc)]
use crate::ReadDoc;
use crate::{ActorId, AutomergeError};
use std::fmt;

/// An identifier of a position in a sequence (either [`crate::ObjType::List`] or [`crate::ObjType::Text`]).
///
/// Every element in an Automerge sequence can be internally identified with an operation ID.
/// While [`crate::exid::ExId`] is the default external representation of an operation ID, it
/// can be quite heavy to pass around or persist. A `Cursor` offers a lightweight, specialized
/// alternative.
///
/// Unlike a raw integer index, a cursor remains stable across concurrent edits. If items are inserted
/// or deleted elsewhere in the sequence, the cursor shifts dynamically to maintain its intended position.
///
/// A cursor can be persisted using [`Self::to_bytes()`] and restored using [`TryFrom<&[u8]>`][TryFrom].
/// It can also be stringified via its [`fmt::Display`] implementation and parsed back using [`TryFrom<&str>`].
///
/// A cursor is typically obtained from [`ReadDoc::get_cursor()`] or [`ReadDoc::get_cursor_moving()`] and
/// is dereferenced back to a concrete index using [`ReadDoc::get_cursor_position()`].
#[derive(Clone, PartialEq, Debug)]
pub enum Cursor {
    /// Attached to the beginning of the sequence. It always dereferences to position 0.
    Start,
    /// Attached to the end of the sequence. It always dereferences to the current sequence length.
    End,
    /// Attached to a specific historical operation inside the sequence.
    Op(OpCursor),
}

/// A cursor anchored to a specific operation in a sequence.
///
/// This structure identifies the point in history where the cursor was placed. If the element
/// created by this operation is later deleted, `move_cursor` specifies how the cursor adjusts
/// to an adjacent visible item.
#[derive(Clone, PartialEq, Debug)]
pub struct OpCursor {
    /// The counter component of the operation ID this cursor is anchored to.
    pub(crate) ctr: u64,
    /// The unique identifier of the actor who generated the anchored operation.
    pub(crate) actor: ActorId,
    /// The shifting strategy used if the targeted operation is deleted.
    pub(crate) move_cursor: MoveCursor,
}

impl OpCursor {
    /// Constructs a new `OpCursor` by looking up the absolute `ActorId` from the active set of
    /// operations.
    ///
    /// This resolves the compact, integer-mapped actor reference inside an `OpId` to its full,
    /// shareable `ActorId` representation.
    pub(crate) fn new(id: OpId, op_set: &OpSet, move_cursor: MoveCursor) -> Self {
        OpCursor {
            ctr: id.counter(),
            actor: op_set.actors[id.actor()].clone(),
            move_cursor,
        }
    }
}

/// The locations in a sequence that a cursor can represent.
#[derive(Debug)]
pub enum CursorPosition {
    /// Resolves to the start of the sequence (index 0).
    Start,
    /// Resolves to the current end of the sequence (index is the sequence length).
    End,
    /// Resolves to an offset within the sequence.
    Index(usize),
}

impl From<usize> for CursorPosition {
    fn from(value: usize) -> Self {
        Self::Index(value)
    }
}

/// Shifting strategy determining how a cursor will resolve its position if the item originally referenced by
/// the cursor is deleted.
#[derive(Clone, PartialEq, Debug)]
pub enum MoveCursor {
    /// The cursor will shift to the **previous** item that was visible at the time of cursor creation.
    /// If no previous item is found that's still visible, the cursor will dereference to `0`.
    Before,
    /// The cursor will shift to the **next** item that was visible at the time of cursor creation.
    /// If no next item is found that's still visible, the cursor will dereference to the current sequence length.
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
    /// Parses a cursor from its human-readable serialized string format.
    ///
    /// Expected format shapes:
    /// - `"s"` -> `Cursor::Start`
    /// - `"e"` -> `Cursor::End`
    /// - `"42@actor_hex"` -> `Cursor::Op` with `MoveCursor::After` behavior.
    /// - `"-42@actor_hex"` -> `Cursor::Op` with `MoveCursor::Before` behavior.
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

    /// Serializes the cursor into a compact binary vector suitable for storage or network transit.
    ///
    /// The serialized output combines a version tag, enum variant tags, and ULEB128 encoding for
    /// integers and lengths to minimize storage footprints.
    ///
    /// # EBNF layout specification
    ///
    /// ```text
    /// version      = %d01            ; Version 1 byte identifier
    /// start        = %d01            ; Cursor start variant
    /// end          = %d02            ; Cursor end variant
    /// opid_tag     = %d03            ; Operational cursor variant
    /// move         = %d01 / %d02     ; 1 = Before, 2 = After
    /// counter      = uleb128         ; Variable-length operation counter
    /// actor_id     = uleb128 1*byte  ; Length-prefixed actor ID bytes
    ///
    /// opid         = opid_tag actor_id counter move
    /// cursor       = version (start / end / opid)
    /// ```
    pub fn to_bytes(&self) -> Vec<u8> {
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

    /// Attempts to parse a `Cursor` from a string slice.
    ///
    /// # Errors
    ///
    /// Returns [`AutomergeError::InvalidCursorFormat`] if the syntax is unrecognized,
    /// structural component symbols like `@` are missing, integers fail parsing boundaries,
    /// or actor IDs are incomplete.
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Cursor::from_str(s).ok_or_else(|| AutomergeError::InvalidCursorFormat)
    }
}

impl TryFrom<String> for Cursor {
    type Error = AutomergeError;

    /// Attempts to parse a `Cursor` from bytes.
    ///
    /// This function handles both the current V1 binary tags as well as the legacy V0 format.
    ///
    /// # Errors
    ///
    /// Returns [`AutomergeError::InvalidCursorFormat`] if payload sizes are truncated, LEB128 numbers
    /// fail parsing constraints, or illegal version flags or type tags are discovered.
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

/// Parsing for legacy version 0 cursor formats.
///
/// ```text
/// .----------------------------------------------------------------.
/// | version   | actorId len     | actorId bytes | counter          |
/// +----------------------------------------------------------------+
/// |  1 byte   | unsigned leb128 | variable      | unsigned leb128  |
/// '----------------------------------------------------------------'
/// ```
///
/// `MoveCursor::After` was the default behavior of cursors in version 0
/// and there was no notion of start/end cursors
fn parse_0(i: parse::Input<'_>) -> Result<Cursor, AutomergeError> {
    let (i, len) = parse::leb128_u64::<parse::leb128::Error>(i)
        .map_err(|_| AutomergeError::InvalidCursorFormat)?;
    let (i, actor) =
        parse::take_n::<()>(len as usize, i).map_err(|_| AutomergeError::InvalidCursorFormat)?;
    let (_i, ctr) = parse::leb128_u64::<parse::leb128::Error>(i)
        .map_err(|_| AutomergeError::InvalidCursorFormat)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_actor() -> ActorId {
        ActorId::from("mock_actor_id".as_bytes())
    }

    #[test]
    fn test_string_round_trip() {
        let actor = mock_actor();

        let cases = vec![
            (Cursor::Start, "s"),
            (Cursor::End, "e"),
            (
                Cursor::Op(OpCursor {
                    ctr: 42,
                    actor: actor.clone(),
                    move_cursor: MoveCursor::After,
                }),
                "42@6d6f636b5f6163746f725f6964", // "mock_actor_id" hex representation
            ),
            (
                Cursor::Op(OpCursor {
                    ctr: 123,
                    actor: actor.clone(),
                    move_cursor: MoveCursor::Before,
                }),
                "-123@6d6f636b5f6163746f725f6964",
            ),
        ];

        for (cursor, expected_str) in cases {
            let serialized = cursor.to_string();
            assert_eq!(serialized, expected_str);

            let parsed: Cursor = serialized.as_str().try_into().unwrap();
            assert_eq!(parsed, cursor);
        }
    }

    #[test]
    fn test_binary_v1_round_trip() {
        let actor = mock_actor();

        let cases = vec![
            Cursor::Start,
            Cursor::End,
            Cursor::Op(OpCursor {
                ctr: 0,
                actor: actor.clone(),
                move_cursor: MoveCursor::After,
            }),
            Cursor::Op(OpCursor {
                ctr: 123456, // Multi-byte leb128
                actor: actor.clone(),
                move_cursor: MoveCursor::Before,
            }),
        ];

        for cursor in cases {
            let bytes = cursor.to_bytes();
            let parsed: Cursor = bytes.as_slice().try_into().unwrap();
            assert_eq!(parsed, cursor);
        }
    }

    #[test]
    fn test_binary_v0_round_trip() {
        let actor = mock_actor();
        let actor_bytes = actor.to_bytes();
        let ctr = 42;

        // No v0 serializer so we construct by hand
        let mut v0_bytes = vec![0u8];
        leb128::write::unsigned(&mut v0_bytes, actor_bytes.len() as u64).unwrap();
        v0_bytes.extend_from_slice(actor_bytes);
        leb128::write::unsigned(&mut v0_bytes, ctr).unwrap();

        let expected = Cursor::Op(OpCursor {
            ctr,
            actor,
            move_cursor: MoveCursor::After,
        });

        let parsed: Cursor = v0_bytes.clone().as_slice().try_into().unwrap();
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_invalid_string_parsing() {
        let cases = vec![
            "",        // Empty string
            "x",       // Not "s" or "e"
            "42",      // Missing @
            "42@",     // Missing actor ID
            "@actor",  // Missing counter
            "-@actor", // Another missing counter
            "abc@actor", // Counter doesn't parse to integer
                       // "你好@actor"
        ];

        for invalid in cases {
            let result: Result<Cursor, _> = invalid.try_into();
            assert!(
                matches!(result, Err(AutomergeError::InvalidCursorFormat)),
                "Expected failure for input: {}",
                invalid
            );
        }
    }

    #[test]
    fn test_invalid_binary_parsing() {
        let cases = vec![
            vec![],                       // Empty data
            vec![2, START_TAG],           // Unsupported version
            vec![VERSION_TAG, 99],        // Unsupported inner tag for v1
            vec![VERSION_TAG, OP_TAG, 5], // Incomplete payload
        ];

        for invalid in cases {
            let result: Result<Cursor, _> = Cursor::try_from(invalid.as_slice());
            assert!(matches!(result, Err(AutomergeError::InvalidCursorFormat)));
        }
    }
}

use automerge as am;
use std::ffi::CString;
use std::ops::Deref;

use crate::AMbyteSpan;
use crate::AMchangeHashes;
use crate::AMsyncMessage;
use crate::{AMchange, AMchanges};

/// \struct AMobjId
/// \brief An object's unique identifier.
pub struct AMobjId(am::ObjId);

impl AMobjId {
    pub fn new(obj_id: am::ObjId) -> Self {
        Self(obj_id)
    }
}

impl AsRef<am::ObjId> for AMobjId {
    fn as_ref(&self) -> &am::ObjId {
        &self.0
    }
}

impl Deref for AMobjId {
    type Target = am::ObjId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// \struct AMvalue
/// \brief A discriminated union of value type variants for an `AMresult` struct.
///
/// \enum AMvalueVariant
/// \brief A value type discriminant.
///
/// \var AMvalue::tag
/// The variant discriminator of an `AMvalue` struct.
///
/// \var AMvalue::actor_id
/// An actor ID as an `AMbyteSpan` struct.
///
/// \var AMvalue::boolean
/// A boolean.
///
/// \var AMvalue::bytes
/// An array of bytes as an `AMbyteSpan` struct.
///
/// \var AMvalue::change_hashes
/// A sequence of change hashes as an `AMchangeHashes` struct.
///
/// \var AMvalue::changes
/// A sequence of changes as an `AMchanges` struct.
///
/// \var AMvalue::counter
/// A CRDT counter.
///
/// \var AMvalue::f64
/// A 64-bit float.
///
/// \var AMvalue::int_
/// A 64-bit signed integer.
///
/// \var AMvalue::obj_id
/// An object identifier.
///
/// \var AMvalue::str
/// A UTF-8 string.
///
/// \var AMvalue::timestamp
/// A Lamport timestamp.
///
/// \var AMvalue::uint
/// A 64-bit unsigned integer.
#[repr(C)]
pub enum AMvalue<'a> {
    /// An actor ID variant.
    ActorId(AMbyteSpan),
    /// A boolean variant.
    Boolean(bool),
    /// An array of bytes variant.
    Bytes(AMbyteSpan),
    /// A change hashes variant.
    ChangeHashes(AMchangeHashes),
    /// A changes variant.
    Changes(AMchanges),
    /// A CRDT counter variant.
    Counter(i64),
    /// A 64-bit float variant.
    F64(f64),
    /// A 64-bit signed integer variant.
    Int(i64),
    /*
    /// A keys variant.
    Keys(_),
    */
    /// A null variant.
    Null,
    /// An object identifier variant.
    ObjId(&'a AMobjId),
    /// A UTF-8 string variant.
    Str(*const libc::c_char),
    /// A Lamport timestamp variant.
    Timestamp(i64),
    /*
    /// A transaction variant.
    Transaction(_),
    */
    /// A 64-bit unsigned integer variant.
    Uint(u64),
    /// A synchronization message variant.
    SyncMessage(&'a AMsyncMessage),
    /// A void variant.
    Void,
}

/// \struct AMresult
/// \brief A discriminated union of result variants.
pub enum AMresult {
    ActorId(am::ActorId),
    ChangeHashes(Vec<am::ChangeHash>),
    Changes(Vec<AMchange>),
    Error(CString),
    ObjId(AMobjId),
    Scalars(Vec<am::Value<'static>>, Option<CString>),
    SyncMessage(AMsyncMessage),
    Void,
}

impl AMresult {
    pub(crate) fn err(s: &str) -> Self {
        AMresult::Error(CString::new(s).unwrap())
    }
}

impl From<am::ChangeHash> for AMresult {
    fn from(change_hash: am::ChangeHash) -> Self {
        AMresult::ChangeHashes(vec![change_hash])
    }
}

impl From<Option<am::sync::Message>> for AMresult {
    fn from(maybe: Option<am::sync::Message>) -> Self {
        match maybe {
            Some(message) => AMresult::SyncMessage(AMsyncMessage::new(message)),
            None => AMresult::Void,
        }
    }
}

impl From<Result<(), am::AutomergeError>> for AMresult {
    fn from(maybe: Result<(), am::AutomergeError>) -> Self {
        match maybe {
            Ok(()) => AMresult::Void,
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}
impl From<Result<am::ActorId, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::ActorId, am::AutomergeError>) -> Self {
        match maybe {
            Ok(actor_id) => AMresult::ActorId(actor_id),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<am::ObjId, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::ObjId, am::AutomergeError>) -> Self {
        match maybe {
            Ok(obj_id) => AMresult::ObjId(AMobjId::new(obj_id)),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>) -> Self {
        match maybe {
            // \todo Ensure that it's alright to ignore the `am::ObjId` value.
            Ok(Some((value, _))) => AMresult::Scalars(vec![value], None),
            Ok(None) => AMresult::Void,
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<am::Value<'static>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::Value<'static>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(value) => AMresult::Scalars(vec![value], None),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<usize, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<usize, am::AutomergeError>) -> Self {
        match maybe {
            Ok(size) => AMresult::Scalars(vec![am::Value::uint(size as u64)], None),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Vec<&am::Change>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<&am::Change>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(changes) => AMresult::Changes(
                changes
                    .iter()
                    .map(|&change| AMchange::new(change.clone()))
                    .collect(),
            ),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Vec<am::ChangeHash>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<am::ChangeHash>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(change_hashes) => AMresult::ChangeHashes(change_hashes),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Vec<u8>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<u8>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(bytes) => AMresult::Scalars(vec![am::Value::bytes(bytes)], None),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<AMresult> for *mut AMresult {
    fn from(b: AMresult) -> Self {
        Box::into_raw(Box::new(b))
    }
}

use automerge as am;
use std::ffi::CString;

/// \struct AMobjId
/// \brief An object's unique identifier.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
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

/// \memberof AMvalue
/// \struct AMbyteSpan
/// \brief A contiguous sequence of bytes.
///
#[repr(C)]
pub struct AMbyteSpan {
    /// A pointer to the byte at position zero.
    /// \warning \p src is only valid until the `AMfreeResult()` function is called
    ///          on the `AMresult` struct hosting the array of bytes to which
    ///          it points.
    src: *const u8,
    /// The number of bytes in the sequence.
    count: usize,
}

impl From<&Vec<u8>> for AMbyteSpan {
    fn from(v: &Vec<u8>) -> Self {
        AMbyteSpan {
            src: (*v).as_ptr(),
            count: (*v).len(),
        }
    }
}

impl From<&mut am::ActorId> for AMbyteSpan {
    fn from(actor: &mut am::ActorId) -> Self {
        let slice = actor.to_bytes();
        AMbyteSpan {
            src: slice.as_ptr(),
            count: slice.len(),
        }
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
/// \var AMvalue::counter
/// A CRDT counter.
///
/// \var AMvalue::f64
/// A 64-bit float.
///
/// \var AMvalue::change_hash
/// A change hash as an `AMbyteSpan` struct.
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
    Boolean(libc::c_char),
    /// An array of bytes variant.
    Bytes(AMbyteSpan),
    /*
    /// A changes variant.
    Changes(_),
    */
    /// A CRDT counter variant.
    Counter(i64),
    /// A 64-bit float variant.
    F64(f64),
    /// A change hash variant.
    ChangeHash(AMbyteSpan),
    /// A 64-bit signed integer variant.
    Int(i64),
    /*
    /// A keys variant.
    Keys(_),
    */
    /// A nothing variant.
    Nothing,
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
}

/// \struct AMresult
/// \brief A discriminated union of result variants.
///
pub enum AMresult<'a> {
    ActorId(am::ActorId),
    Changes(Vec<am::Change>),
    Error(CString),
    ObjId(&'a AMobjId),
    Nothing,
    Scalars(Vec<am::Value<'static>>, Option<CString>),
}

impl<'a> AMresult<'a> {
    pub(crate) fn err(s: &str) -> Self {
        AMresult::Error(CString::new(s).unwrap())
    }
}

impl<'a> From<Result<am::ActorId, am::AutomergeError>> for AMresult<'a> {
    fn from(maybe: Result<am::ActorId, am::AutomergeError>) -> Self {
        match maybe {
            Ok(actor_id) => AMresult::ActorId(actor_id),
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

impl<'a> From<Result<&'a AMobjId, am::AutomergeError>> for AMresult<'a> {
    fn from(maybe: Result<&'a AMobjId, am::AutomergeError>) -> Self {
        match maybe {
            Ok(obj_id) => AMresult::ObjId(obj_id),
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

impl<'a> From<Result<(), am::AutomergeError>> for AMresult<'a> {
    fn from(maybe: Result<(), am::AutomergeError>) -> Self {
        match maybe {
            Ok(()) => AMresult::Nothing,
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

impl<'a> From<Result<Option<(am::Value, am::ObjId)>, am::AutomergeError>> for AMresult<'a> {
    fn from(maybe: Result<Option<(am::Value, am::ObjId)>, am::AutomergeError>) -> Self {
        match maybe {
            // \todo Ensure that it's alright to ignore the `am::ObjId` value.
            Ok(Some((value, _))) => AMresult::Scalars(vec![value], None),
            Ok(None) => AMresult::Nothing,
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

impl<'a> From<Result<am::Value, am::AutomergeError>> for AMresult<'a> {
    fn from(maybe: Result<am::Value, am::AutomergeError>) -> Self {
        match maybe {
            Ok(value) => AMresult::Scalars(vec![value], None),
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

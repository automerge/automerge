use automerge as am;
use std::ffi::CString;

/// \struct AMobj
/// \brief A discriminated union of object handle variants.
///
/// \ingroup enumerations
/// \enum AMobjVariant
/// \brief An object handle discriminant.
///
/// \var AMobj::tag
/// The variant discriminator of an `AMobj` struct.
///
/// \struct AMobj_Id
/// \brief An object identifier.
///
#[repr(C)]
pub enum AMobj {
    /// An object identifier variant.
    Id {
        /// The counter component of an object identifier.
        ctr: u64,
        /// The actor component of an object identifier.
        actor: [u8; 16],
        /// The index component of an object identifier.
        idx: usize
    },
    /// A root object signifier variant.
    Root,
}

impl From<&am::ObjId> for AMobj {
    fn from(obj_id: &am::ObjId) -> Self {
        match obj_id {
            am::ObjId::Id(ctr, actor, idx) => {
                let mut am_obj = AMobj::Id {
                    ctr: *ctr,
                    actor: Default::default(),
                    idx: *idx,
                };
                if let AMobj::Id{ctr: _, actor: a, idx: _} = &mut am_obj {
                    a.copy_from_slice(actor.to_bytes());
                }
                am_obj
            },
            am::ObjId::Root => AMobj::Root,
        }
    }
}

impl From<&AMobj> for am::ObjId {
    fn from(am_obj: &AMobj) -> Self {
        match am_obj {
            AMobj::Id{ctr, actor, idx} => {
                am::ObjId::Id(*ctr, am::ActorId::from(actor.as_slice()), *idx)
            },
            AMobj::Root => am::ObjId::Root,
        }
    }
}

/// \memberof AMvalue
/// \struct AMbyteSpan
/// \brief A contiguous sequence of bytes.
///
#[repr(C)]
pub struct AMbyteSpan {
    /// A pointer to the byte at position zero.
    /// \warning \p src is only valid until the `AMclear()` function is called
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
/// \ingroup enumerations
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
/// \var AMvalue::int_
/// A 64-bit signed integer.
///
/// \var AMvalue::obj
/// An object handle.
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
pub enum AMvalue {
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
/*
    /// A heads variant.
    Heads(_),
*/
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
    /// An object handle variant.
    Obj(AMobj),
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
pub enum AMresult {
    ActorId(am::ActorId),
    Changes(Vec<am::Change>),
    Error(CString),
    ObjId(am::ObjId),
    Nothing,
    Scalars(Vec<am::Value>, Option<CString>),
}

impl AMresult {
    pub(crate) fn err(s: &str) -> Self {
        AMresult::Error(CString::new(s).unwrap())
    }
}

impl From<Result<am::ObjId, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::ObjId, am::AutomergeError>) -> Self {
        match maybe {
            Ok(obj) => AMresult::ObjId(obj),
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

impl From<Result<(), am::AutomergeError>> for AMresult {
    fn from(maybe: Result<(), am::AutomergeError>) -> Self {
        match maybe {
            Ok(()) => AMresult::Nothing,
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

impl From<Result<Option<(am::Value, am::ObjId)>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Option<(am::Value, am::ObjId)>, am::AutomergeError>) -> Self {
        match maybe {
            // \todo Ensure that it's alright to ignore the `am::ObjId` value.
            Ok(Some((value, _))) => AMresult::Scalars(vec![value], None),
            Ok(None) => AMresult::Nothing,
            Err(e) => AMresult::Error(CString::new(e.to_string()).unwrap()),
        }
    }
}

use automerge as am;
use std::collections::BTreeMap;
use std::ffi::CString;
use std::os::raw::c_char;

use crate::actor_id::AMactorId;
use crate::byte_span::AMbyteSpan;
use crate::change::AMchange;
use crate::change_hashes::AMchangeHashes;
use crate::changes::AMchanges;
use crate::doc::AMdoc;
use crate::obj::AMobjId;
use crate::strings::AMstrings;
use crate::sync::{AMsyncMessage, AMsyncState};

/// \struct AMvalue
/// \brief A discriminated union of value type variants for a result.
///
/// \enum AMvalueVariant
/// \brief A value type discriminant.
///
/// \var AMvalue::actor_id
/// An actor identifier as a pointer to an `AMactorId` struct.
///
/// \var AMvalue::boolean
/// A boolean.
///
/// \var AMvalue::bytes
/// A sequence of bytes as an `AMbyteSpan` struct.
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
/// \var AMvalue::doc
/// A document as a pointer to an `AMdoc` struct.
///
/// \var AMvalue::f64
/// A 64-bit float.
///
/// \var AMvalue::int_
/// A 64-bit signed integer.
///
/// \var AMvalue::obj_id
/// An object identifier as a pointer to an `AMobjId` struct.
///
/// \var AMvalue::str
/// A UTF-8 string.
///
/// \var AMvalue::strings
/// A sequence of UTF-8 strings as an `AMstrings` struct.
///
/// \var AMvalue::sync_message
/// A synchronization message as a pointer to an `AMsyncMessage` struct.
///
/// \var AMvalue::sync_state
/// A synchronization state as a pointer to an `AMsyncState` struct.
///
/// \var AMvalue::tag
/// The variant discriminator.
///
/// \var AMvalue::timestamp
/// A Lamport timestamp.
///
/// \var AMvalue::uint
/// A 64-bit unsigned integer.
#[repr(C)]
pub enum AMvalue<'a> {
    /// A void variant.
    /// \note This tag is unalphabetized so that a zeroed struct will have it.
    Void,
    /// An actor identifier variant.
    ActorId(&'a AMactorId),
    /// A boolean variant.
    Boolean(bool),
    /// A byte array variant.
    Bytes(AMbyteSpan),
    /// A change hashes variant.
    ChangeHashes(AMchangeHashes),
    /// A changes variant.
    Changes(AMchanges),
    /// A CRDT counter variant.
    Counter(i64),
    /// A document variant.
    Doc(*mut AMdoc),
    /// A 64-bit float variant.
    F64(f64),
    /// A 64-bit signed integer variant.
    Int(i64),
    /// A null variant.
    Null,
    /// An object identifier variant.
    ObjId(&'a AMobjId),
    /// A UTF-8 string variant.
    Str(*const libc::c_char),
    /// A strings variant.
    Strings(AMstrings),
    /// A synchronization message variant.
    SyncMessage(&'a AMsyncMessage),
    /// A synchronization state variant.
    SyncState(&'a mut AMsyncState),
    /// A Lamport timestamp variant.
    Timestamp(i64),
    /// A 64-bit unsigned integer variant.
    Uint(u64),
    /// An unknown type of scalar value variant.
    Unknown(AMunknownValue),
}

/// \struct AMresult
/// \brief A discriminated union of result variants.
pub enum AMresult {
    ActorId(AMactorId),
    ChangeHashes(Vec<am::ChangeHash>),
    Changes(Vec<am::Change>, BTreeMap<usize, AMchange>),
    String(CString),
    Strings(Vec<CString>),
    Doc(Box<AMdoc>),
    Error(CString),
    ObjId(AMobjId),
    SyncMessage(AMsyncMessage),
    SyncState(AMsyncState),
    Value(am::Value<'static>, Option<CString>),
    Void,
}

impl AMresult {
    pub(crate) fn err(s: &str) -> Self {
        AMresult::Error(CString::new(s).unwrap())
    }
}

impl From<am::AutoCommit> for AMresult {
    fn from(auto_commit: am::AutoCommit) -> Self {
        AMresult::Doc(Box::new(AMdoc::new(auto_commit)))
    }
}

impl From<am::ChangeHash> for AMresult {
    fn from(change_hash: am::ChangeHash) -> Self {
        AMresult::ChangeHashes(vec![change_hash])
    }
}

impl From<am::Keys<'_, '_>> for AMresult {
    fn from(keys: am::Keys<'_, '_>) -> Self {
        let cstrings: Vec<CString> = keys.map(|s| CString::new(s).unwrap()).collect();
        AMresult::Strings(cstrings)
    }
}

impl From<am::KeysAt<'_, '_>> for AMresult {
    fn from(keys: am::KeysAt<'_, '_>) -> Self {
        let cstrings: Vec<CString> = keys.map(|s| CString::new(s).unwrap()).collect();
        AMresult::Strings(cstrings)
    }
}

impl From<am::sync::State> for AMresult {
    fn from(state: am::sync::State) -> Self {
        AMresult::SyncState(AMsyncState::new(state))
    }
}

impl From<AMresult> for *mut AMresult {
    fn from(b: AMresult) -> Self {
        Box::into_raw(Box::new(b))
    }
}

impl From<Option<&am::Change>> for AMresult {
    fn from(maybe: Option<&am::Change>) -> Self {
        match maybe {
            Some(change) => AMresult::Changes(vec![change.clone()], BTreeMap::new()),
            None => AMresult::Void,
        }
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
            Ok(actor_id) => AMresult::ActorId(AMactorId::new(actor_id)),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<am::ActorId, am::InvalidActorId>> for AMresult {
    fn from(maybe: Result<am::ActorId, am::InvalidActorId>) -> Self {
        match maybe {
            Ok(actor_id) => AMresult::ActorId(AMactorId::new(actor_id)),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<am::AutoCommit, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::AutoCommit, am::AutomergeError>) -> Self {
        match maybe {
            Ok(auto_commit) => AMresult::Doc(Box::new(AMdoc::new(auto_commit))),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

#[cfg(not(feature = "storage-v2"))]
impl From<Result<am::Change, am::DecodingError>> for AMresult {
    fn from(maybe: Result<am::Change, am::DecodingError>) -> Self {
        match maybe {
            Ok(change) => AMresult::Changes(vec![change], BTreeMap::new()),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

#[cfg(feature = "storage-v2")]
impl From<Result<am::Change, am::LoadChangeError>> for AMresult {
    fn from(maybe: Result<am::Change, am::LoadChangeError>) -> Self {
        match maybe {
            Ok(change) => AMresult::Changes(vec![change], BTreeMap::new()),
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

#[cfg(not(feature = "storage-v2"))]
impl From<Result<am::sync::Message, am::DecodingError>> for AMresult {
    fn from(maybe: Result<am::sync::Message, am::DecodingError>) -> Self {
        match maybe {
            Ok(message) => AMresult::SyncMessage(AMsyncMessage::new(message)),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

#[cfg(feature = "storage-v2")]
impl From<Result<am::sync::Message, am::sync::ReadMessageError>> for AMresult {
    fn from(maybe: Result<am::sync::Message, am::sync::ReadMessageError>) -> Self {
        match maybe {
            Ok(message) => AMresult::SyncMessage(AMsyncMessage::new(message)),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

#[cfg(not(feature = "storage-v2"))]
impl From<Result<am::sync::State, am::DecodingError>> for AMresult {
    fn from(maybe: Result<am::sync::State, am::DecodingError>) -> Self {
        match maybe {
            Ok(state) => AMresult::SyncState(AMsyncState::new(state)),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

#[cfg(feature = "storage-v2")]
impl From<Result<am::sync::State, am::sync::DecodeStateError>> for AMresult {
    fn from(maybe: Result<am::sync::State, am::sync::DecodeStateError>) -> Self {
        match maybe {
            Ok(state) => AMresult::SyncState(AMsyncState::new(state)),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<am::Value<'static>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::Value<'static>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(value) => AMresult::Value(value, None),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>) -> Self {
        match maybe {
            // \todo Ensure that it's alright to ignore the `am::ObjId` value.
            Ok(Some((value, _))) => AMresult::Value(value, None),
            Ok(None) => AMresult::Void,
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<String, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<String, am::AutomergeError>) -> Self {
        match maybe {
            Ok(string) => AMresult::String(CString::new(string).unwrap()),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<usize, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<usize, am::AutomergeError>) -> Self {
        match maybe {
            Ok(size) => AMresult::Value(am::Value::uint(size as u64), None),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Vec<am::Change>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<am::Change>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(changes) => AMresult::Changes(changes, BTreeMap::new()),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Vec<&am::Change>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<&am::Change>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(changes) => {
                let changes: Vec<am::Change> =
                    changes.iter().map(|&change| change.clone()).collect();
                AMresult::Changes(changes, BTreeMap::new())
            }
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

impl From<Result<Vec<am::ChangeHash>, am::InvalidChangeHashSlice>> for AMresult {
    fn from(maybe: Result<Vec<am::ChangeHash>, am::InvalidChangeHashSlice>) -> Self {
        match maybe {
            Ok(change_hashes) => AMresult::ChangeHashes(change_hashes),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Vec<u8>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<u8>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(bytes) => AMresult::Value(am::Value::bytes(bytes), None),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Vec<&am::Change>> for AMresult {
    fn from(changes: Vec<&am::Change>) -> Self {
        let changes: Vec<am::Change> = changes.iter().map(|&change| change.clone()).collect();
        AMresult::Changes(changes, BTreeMap::new())
    }
}

impl From<Vec<am::ChangeHash>> for AMresult {
    fn from(change_hashes: Vec<am::ChangeHash>) -> Self {
        AMresult::ChangeHashes(change_hashes)
    }
}

impl From<Vec<u8>> for AMresult {
    fn from(bytes: Vec<u8>) -> Self {
        AMresult::Value(am::Value::bytes(bytes), None)
    }
}

pub fn to_result<R: Into<AMresult>>(r: R) -> *mut AMresult {
    (r.into()).into()
}

/// \ingroup enumerations
/// \enum AMstatus
/// \brief The status of an API call.
#[derive(Debug)]
#[repr(u8)]
pub enum AMstatus {
    /// Success.
    /// \note This tag is unalphabetized so that `0` indicates success.
    Ok,
    /// Failure due to an error.
    Error,
    /// Failure due to an invalid result.
    InvalidResult,
}

/// \memberof AMresult
/// \brief Gets a result's error message string.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return A UTF-8 string value or `NULL`.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
#[no_mangle]
pub unsafe extern "C" fn AMerrorMessage(result: *const AMresult) -> *const c_char {
    match result.as_ref() {
        Some(AMresult::Error(s)) => s.as_ptr(),
        _ => std::ptr::null::<c_char>(),
    }
}

/// \memberof AMresult
/// \brief Deallocates the storage for a result.
///
/// \param[in,out] result A pointer to an `AMresult` struct.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
#[no_mangle]
pub unsafe extern "C" fn AMfree(result: *mut AMresult) {
    if !result.is_null() {
        let result: AMresult = *Box::from_raw(result);
        drop(result)
    }
}

/// \memberof AMresult
/// \brief Gets the size of a result's value.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return The count of values in \p result.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultSize(result: *const AMresult) -> usize {
    if let Some(result) = result.as_ref() {
        match result {
            AMresult::Error(_) | AMresult::Void => 0,
            AMresult::ActorId(_)
            | AMresult::Doc(_)
            | AMresult::ObjId(_)
            | AMresult::String(_)
            | AMresult::SyncMessage(_)
            | AMresult::SyncState(_)
            | AMresult::Value(_, _) => 1,
            AMresult::ChangeHashes(change_hashes) => change_hashes.len(),
            AMresult::Changes(changes, _) => changes.len(),
            AMresult::Strings(cstrings) => cstrings.len(),
        }
    } else {
        0
    }
}

/// \memberof AMresult
/// \brief Gets the status code of a result.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return An `AMstatus` enum tag.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultStatus(result: *const AMresult) -> AMstatus {
    match result.as_ref() {
        Some(AMresult::Error(_)) => AMstatus::Error,
        None => AMstatus::InvalidResult,
        _ => AMstatus::Ok,
    }
}

/// \memberof AMresult
/// \brief Gets a result's value.
///
/// \param[in,out] result A pointer to an `AMresult` struct.
/// \return An `AMvalue` struct.
/// \pre \p result must be a valid address.
/// \internal
///
/// # Safety
/// result must be a pointer to a valid AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultValue<'a>(result: *mut AMresult) -> AMvalue<'a> {
    let mut content = AMvalue::Void;
    if let Some(result) = result.as_mut() {
        match result {
            AMresult::ActorId(actor_id) => {
                content = AMvalue::ActorId(actor_id);
            }
            AMresult::ChangeHashes(change_hashes) => {
                content = AMvalue::ChangeHashes(AMchangeHashes::new(change_hashes));
            }
            AMresult::Changes(changes, storage) => {
                content = AMvalue::Changes(AMchanges::new(changes, storage));
            }
            AMresult::Doc(doc) => content = AMvalue::Doc(&mut **doc),
            AMresult::Error(_) => {}
            AMresult::ObjId(obj_id) => {
                content = AMvalue::ObjId(obj_id);
            }
            AMresult::String(cstring) => content = AMvalue::Str(cstring.as_ptr()),
            AMresult::Strings(cstrings) => {
                content = AMvalue::Strings(AMstrings::new(cstrings));
            }
            AMresult::SyncMessage(sync_message) => {
                content = AMvalue::SyncMessage(sync_message);
            }
            AMresult::SyncState(sync_state) => {
                content = AMvalue::SyncState(sync_state);
            }
            AMresult::Value(value, value_str) => {
                match value {
                    am::Value::Scalar(scalar) => match scalar.as_ref() {
                        am::ScalarValue::Boolean(flag) => {
                            content = AMvalue::Boolean(*flag);
                        }
                        am::ScalarValue::Bytes(bytes) => {
                            content = AMvalue::Bytes(bytes.as_slice().into());
                        }
                        am::ScalarValue::Counter(counter) => {
                            content = AMvalue::Counter(counter.into());
                        }
                        am::ScalarValue::F64(float) => {
                            content = AMvalue::F64(*float);
                        }
                        am::ScalarValue::Int(int) => {
                            content = AMvalue::Int(*int);
                        }
                        am::ScalarValue::Null => {
                            content = AMvalue::Null;
                        }
                        am::ScalarValue::Str(smol_str) => {
                            *value_str = CString::new(smol_str.to_string()).ok();
                            if let Some(cstring) = value_str {
                                content = AMvalue::Str(cstring.as_ptr());
                            }
                        }
                        am::ScalarValue::Timestamp(timestamp) => {
                            content = AMvalue::Timestamp(*timestamp);
                        }
                        am::ScalarValue::Uint(uint) => {
                            content = AMvalue::Uint(*uint);
                        }
                        am::ScalarValue::Unknown { bytes, type_code } => {
                            content = AMvalue::Unknown(AMunknownValue {
                                bytes: bytes.as_slice().into(),
                                type_code: *type_code,
                            })
                        }
                    },
                    // \todo Confirm that an object variant should be ignored
                    //       when there's no object ID variant.
                    am::Value::Object(_) => {}
                }
            }
            AMresult::Void => {}
        }
    };
    content
}

/// \struct AMunknownValue
/// \brief A value (typically for a `set` operation) whose type is unknown.
///
#[repr(C)]
pub struct AMunknownValue {
    bytes: AMbyteSpan,
    type_code: u8,
}

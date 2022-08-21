use automerge as am;
use libc::strcmp;
use smol_str::SmolStr;
use std::any::type_name;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::CString;
use std::ops::{Range, RangeFrom, RangeFull, RangeTo};
use std::os::raw::c_char;

use crate::actor_id::AMactorId;
use crate::byte_span::AMbyteSpan;
use crate::change::AMchange;
use crate::change_hashes::AMchangeHashes;
use crate::changes::AMchanges;
use crate::doc::list::{item::AMlistItem, items::AMlistItems};
use crate::doc::map::{item::AMmapItem, items::AMmapItems};
use crate::doc::utils::to_str;
use crate::doc::AMdoc;
use crate::obj::item::AMobjItem;
use crate::obj::items::AMobjItems;
use crate::obj::AMobjId;
use crate::strs::AMstrs;
use crate::sync::{AMsyncMessage, AMsyncState};

/// \struct AMvalue
/// \installed_headerfile
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
/// \var AMvalue::list_items
/// A sequence of list object items as an `AMlistItems` struct.
///
/// \var AMvalue::map_items
/// A sequence of map object items as an `AMmapItems` struct.
///
/// \var AMvalue::obj_id
/// An object identifier as a pointer to an `AMobjId` struct.
///
/// \var AMvalue::obj_items
/// A sequence of object items as an `AMobjItems` struct.
///
/// \var AMvalue::str
/// A UTF-8 string.
///
/// \var AMvalue::strs
/// A sequence of UTF-8 strings as an `AMstrs` struct.
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
#[repr(u8)]
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
    /// A list items variant.
    ListItems(AMlistItems),
    /// A map items variant.
    MapItems(AMmapItems),
    /// A null variant.
    Null,
    /// An object identifier variant.
    ObjId(&'a AMobjId),
    /// An object items variant.
    ObjItems(AMobjItems),
    /// A UTF-8 string variant.
    Str(*const libc::c_char),
    /// A UTF-8 strings variant.
    Strs(AMstrs),
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

impl<'a> PartialEq for AMvalue<'a> {
    fn eq(&self, other: &Self) -> bool {
        use AMvalue::*;

        match (self, other) {
            (ActorId(lhs), ActorId(rhs)) => *lhs == *rhs,
            (Boolean(lhs), Boolean(rhs)) => lhs == rhs,
            (Bytes(lhs), Bytes(rhs)) => lhs == rhs,
            (ChangeHashes(lhs), ChangeHashes(rhs)) => lhs == rhs,
            (Changes(lhs), Changes(rhs)) => lhs == rhs,
            (Counter(lhs), Counter(rhs)) => lhs == rhs,
            (Doc(lhs), Doc(rhs)) => *lhs == *rhs,
            (F64(lhs), F64(rhs)) => lhs == rhs,
            (Int(lhs), Int(rhs)) => lhs == rhs,
            (ListItems(lhs), ListItems(rhs)) => lhs == rhs,
            (MapItems(lhs), MapItems(rhs)) => lhs == rhs,
            (ObjId(lhs), ObjId(rhs)) => *lhs == *rhs,
            (ObjItems(lhs), ObjItems(rhs)) => lhs == rhs,
            (Str(lhs), Str(rhs)) => unsafe { strcmp(*lhs, *rhs) == 0 },
            (Strs(lhs), Strs(rhs)) => lhs == rhs,
            (SyncMessage(lhs), SyncMessage(rhs)) => *lhs == *rhs,
            (SyncState(lhs), SyncState(rhs)) => *lhs == *rhs,
            (Timestamp(lhs), Timestamp(rhs)) => lhs == rhs,
            (Uint(lhs), Uint(rhs)) => lhs == rhs,
            (Unknown(lhs), Unknown(rhs)) => lhs == rhs,
            (Null, Null) | (Void, Void) => true,
            _ => false,
        }
    }
}

impl From<(&am::Value<'_>, &RefCell<Option<CString>>)> for AMvalue<'_> {
    fn from((value, c_str): (&am::Value<'_>, &RefCell<Option<CString>>)) -> Self {
        match value {
            am::Value::Scalar(scalar) => match scalar.as_ref() {
                am::ScalarValue::Boolean(flag) => AMvalue::Boolean(*flag),
                am::ScalarValue::Bytes(bytes) => AMvalue::Bytes(bytes.as_slice().into()),
                am::ScalarValue::Counter(counter) => AMvalue::Counter(counter.into()),
                am::ScalarValue::F64(float) => AMvalue::F64(*float),
                am::ScalarValue::Int(int) => AMvalue::Int(*int),
                am::ScalarValue::Null => AMvalue::Null,
                am::ScalarValue::Str(smol_str) => {
                    let mut c_str = c_str.borrow_mut();
                    AMvalue::Str(match c_str.as_mut() {
                        None => {
                            let value_str = CString::new(smol_str.to_string()).unwrap();
                            c_str.insert(value_str).as_ptr()
                        }
                        Some(value_str) => value_str.as_ptr(),
                    })
                }
                am::ScalarValue::Timestamp(timestamp) => AMvalue::Timestamp(*timestamp),
                am::ScalarValue::Uint(uint) => AMvalue::Uint(*uint),
                am::ScalarValue::Unknown { bytes, type_code } => AMvalue::Unknown(AMunknownValue {
                    bytes: bytes.as_slice().into(),
                    type_code: *type_code,
                }),
            },
            // \todo Confirm that an object variant should be ignored
            //       when there's no object ID variant.
            am::Value::Object(_) => AMvalue::Void,
        }
    }
}

impl From<&AMvalue<'_>> for u8 {
    fn from(value: &AMvalue) -> Self {
        use AMvalue::*;

        // \warning These numbers must correspond to the order in which the
        //          variants of an AMvalue are declared within it.
        match value {
            ActorId(_) => 1,
            Boolean(_) => 2,
            Bytes(_) => 3,
            ChangeHashes(_) => 4,
            Changes(_) => 5,
            Counter(_) => 6,
            Doc(_) => 7,
            F64(_) => 8,
            Int(_) => 9,
            ListItems(_) => 10,
            MapItems(_) => 11,
            Null => 12,
            ObjId(_) => 13,
            ObjItems(_) => 14,
            Str(_) => 15,
            Strs(_) => 16,
            SyncMessage(_) => 17,
            SyncState(_) => 18,
            Timestamp(_) => 19,
            Uint(_) => 20,
            Unknown(..) => 21,
            Void => 0,
        }
    }
}

impl TryFrom<&AMvalue<'_>> for am::ScalarValue {
    type Error = am::AutomergeError;

    fn try_from(c_value: &AMvalue) -> Result<Self, Self::Error> {
        use am::AutomergeError::InvalidValueType;
        use AMvalue::*;

        let expected = type_name::<am::ScalarValue>().to_string();
        match c_value {
            Boolean(b) => Ok(am::ScalarValue::Boolean(*b)),
            Bytes(span) => {
                let slice = unsafe { std::slice::from_raw_parts(span.src, span.count) };
                Ok(am::ScalarValue::Bytes(slice.to_vec()))
            }
            Counter(c) => Ok(am::ScalarValue::Counter(c.into())),
            F64(f) => Ok(am::ScalarValue::F64(*f)),
            Int(i) => Ok(am::ScalarValue::Int(*i)),
            Str(c_str) => {
                let smol_str = unsafe { SmolStr::new(to_str(*c_str)) };
                Ok(am::ScalarValue::Str(smol_str))
            }
            Timestamp(t) => Ok(am::ScalarValue::Timestamp(*t)),
            Uint(u) => Ok(am::ScalarValue::Uint(*u)),
            Null => Ok(am::ScalarValue::Null),
            Unknown(AMunknownValue { bytes, type_code }) => {
                let slice = unsafe { std::slice::from_raw_parts(bytes.src, bytes.count) };
                Ok(am::ScalarValue::Unknown {
                    bytes: slice.to_vec(),
                    type_code: *type_code,
                })
            }
            ActorId(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMactorId>().to_string(),
            }),
            ChangeHashes(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMchangeHashes>().to_string(),
            }),
            Changes(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMchanges>().to_string(),
            }),
            Doc(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMdoc>().to_string(),
            }),
            ListItems(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMlistItems>().to_string(),
            }),
            MapItems(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMmapItems>().to_string(),
            }),
            ObjId(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMobjId>().to_string(),
            }),
            ObjItems(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMobjItems>().to_string(),
            }),
            Strs(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMstrs>().to_string(),
            }),
            SyncMessage(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMsyncMessage>().to_string(),
            }),
            SyncState(_) => Err(InvalidValueType {
                expected,
                unexpected: type_name::<AMsyncState>().to_string(),
            }),
            Void => Err(InvalidValueType {
                expected,
                unexpected: type_name::<()>().to_string(),
            }),
        }
    }
}

/// \memberof AMvalue
/// \brief Tests the equality of two values.
///
/// \param[in] value1 A pointer to an `AMvalue` struct.
/// \param[in] value2 A pointer to an `AMvalue` struct.
/// \return `true` if \p value1 `==` \p value2 and `false` otherwise.
/// \pre \p value1 `!= NULL`.
/// \pre \p value2 `!= NULL`.
/// \internal
///
/// #Safety
/// value1 must be a valid AMvalue pointer
/// value2 must be a valid AMvalue pointer
#[no_mangle]
pub unsafe extern "C" fn AMvalueEqual(value1: *const AMvalue, value2: *const AMvalue) -> bool {
    match (value1.as_ref(), value2.as_ref()) {
        (Some(value1), Some(value2)) => *value1 == *value2,
        (None, Some(_)) | (Some(_), None) | (None, None) => false,
    }
}

/// \struct AMresult
/// \installed_headerfile
/// \brief A discriminated union of result variants.
pub enum AMresult {
    ActorId(am::ActorId, Option<AMactorId>),
    ChangeHashes(Vec<am::ChangeHash>),
    Changes(Vec<am::Change>, Option<BTreeMap<usize, AMchange>>),
    Doc(Box<AMdoc>),
    Error(CString),
    ListItems(Vec<AMlistItem>),
    MapItems(Vec<AMmapItem>),
    ObjId(AMobjId),
    ObjItems(Vec<AMobjItem>),
    String(CString),
    Strings(Vec<CString>),
    SyncMessage(AMsyncMessage),
    SyncState(Box<AMsyncState>),
    Value(am::Value<'static>, RefCell<Option<CString>>),
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

impl From<am::ListRange<'static, Range<usize>>> for AMresult {
    fn from(list_range: am::ListRange<'static, Range<usize>>) -> Self {
        AMresult::ListItems(
            list_range
                .map(|(i, v, o)| AMlistItem::new(i, v.clone(), o))
                .collect(),
        )
    }
}

impl From<am::ListRangeAt<'static, Range<usize>>> for AMresult {
    fn from(list_range: am::ListRangeAt<'static, Range<usize>>) -> Self {
        AMresult::ListItems(
            list_range
                .map(|(i, v, o)| AMlistItem::new(i, v.clone(), o))
                .collect(),
        )
    }
}

impl From<am::MapRange<'static, Range<String>>> for AMresult {
    fn from(map_range: am::MapRange<'static, Range<String>>) -> Self {
        let map_items: Vec<AMmapItem> = map_range
            .map(|(k, v, o): (&'_ str, am::Value<'_>, am::ObjId)| AMmapItem::new(k, v.clone(), o))
            .collect();
        AMresult::MapItems(map_items)
    }
}

impl From<am::MapRangeAt<'static, Range<String>>> for AMresult {
    fn from(map_range: am::MapRangeAt<'static, Range<String>>) -> Self {
        let map_items: Vec<AMmapItem> = map_range
            .map(|(k, v, o): (&'_ str, am::Value<'_>, am::ObjId)| AMmapItem::new(k, v.clone(), o))
            .collect();
        AMresult::MapItems(map_items)
    }
}

impl From<am::MapRange<'static, RangeFrom<String>>> for AMresult {
    fn from(map_range: am::MapRange<'static, RangeFrom<String>>) -> Self {
        let map_items: Vec<AMmapItem> = map_range
            .map(|(k, v, o): (&'_ str, am::Value<'_>, am::ObjId)| AMmapItem::new(k, v.clone(), o))
            .collect();
        AMresult::MapItems(map_items)
    }
}

impl From<am::MapRangeAt<'static, RangeFrom<String>>> for AMresult {
    fn from(map_range: am::MapRangeAt<'static, RangeFrom<String>>) -> Self {
        let map_items: Vec<AMmapItem> = map_range
            .map(|(k, v, o): (&'_ str, am::Value<'_>, am::ObjId)| AMmapItem::new(k, v.clone(), o))
            .collect();
        AMresult::MapItems(map_items)
    }
}

impl From<am::MapRange<'static, RangeFull>> for AMresult {
    fn from(map_range: am::MapRange<'static, RangeFull>) -> Self {
        let map_items: Vec<AMmapItem> = map_range
            .map(|(k, v, o): (&'_ str, am::Value<'_>, am::ObjId)| AMmapItem::new(k, v.clone(), o))
            .collect();
        AMresult::MapItems(map_items)
    }
}

impl From<am::MapRangeAt<'static, RangeFull>> for AMresult {
    fn from(map_range: am::MapRangeAt<'static, RangeFull>) -> Self {
        let map_items: Vec<AMmapItem> = map_range
            .map(|(k, v, o): (&'_ str, am::Value<'_>, am::ObjId)| AMmapItem::new(k, v.clone(), o))
            .collect();
        AMresult::MapItems(map_items)
    }
}

impl From<am::MapRange<'static, RangeTo<String>>> for AMresult {
    fn from(map_range: am::MapRange<'static, RangeTo<String>>) -> Self {
        let map_items: Vec<AMmapItem> = map_range
            .map(|(k, v, o): (&'_ str, am::Value<'_>, am::ObjId)| AMmapItem::new(k, v.clone(), o))
            .collect();
        AMresult::MapItems(map_items)
    }
}

impl From<am::MapRangeAt<'static, RangeTo<String>>> for AMresult {
    fn from(map_range: am::MapRangeAt<'static, RangeTo<String>>) -> Self {
        let map_items: Vec<AMmapItem> = map_range
            .map(|(k, v, o): (&'_ str, am::Value<'_>, am::ObjId)| AMmapItem::new(k, v.clone(), o))
            .collect();
        AMresult::MapItems(map_items)
    }
}

impl From<am::sync::State> for AMresult {
    fn from(state: am::sync::State) -> Self {
        AMresult::SyncState(Box::new(AMsyncState::new(state)))
    }
}

impl From<am::Values<'static>> for AMresult {
    fn from(pairs: am::Values<'static>) -> Self {
        AMresult::ObjItems(pairs.map(|(v, o)| AMobjItem::new(v.clone(), o)).collect())
    }
}

impl From<Result<Vec<(am::Value<'static>, am::ObjId)>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<(am::Value<'static>, am::ObjId)>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(pairs) => AMresult::ObjItems(
                pairs
                    .into_iter()
                    .map(|(v, o)| AMobjItem::new(v, o))
                    .collect(),
            ),
            Err(e) => AMresult::err(&e.to_string()),
        }
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
            Some(change) => AMresult::Changes(vec![change.clone()], None),
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
            Ok(actor_id) => AMresult::ActorId(actor_id, None),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<am::ActorId, am::InvalidActorId>> for AMresult {
    fn from(maybe: Result<am::ActorId, am::InvalidActorId>) -> Self {
        match maybe {
            Ok(actor_id) => AMresult::ActorId(actor_id, None),
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

impl From<Result<am::Change, am::LoadChangeError>> for AMresult {
    fn from(maybe: Result<am::Change, am::LoadChangeError>) -> Self {
        match maybe {
            Ok(change) => AMresult::Changes(vec![change], None),
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

impl From<Result<am::sync::Message, am::sync::ReadMessageError>> for AMresult {
    fn from(maybe: Result<am::sync::Message, am::sync::ReadMessageError>) -> Self {
        match maybe {
            Ok(message) => AMresult::SyncMessage(AMsyncMessage::new(message)),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<am::sync::State, am::sync::DecodeStateError>> for AMresult {
    fn from(maybe: Result<am::sync::State, am::sync::DecodeStateError>) -> Self {
        match maybe {
            Ok(state) => AMresult::SyncState(Box::new(AMsyncState::new(state))),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<am::Value<'static>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::Value<'static>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(value) => AMresult::Value(value, RefCell::<Option<CString>>::default()),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(Some((value, obj_id))) => match value {
                am::Value::Object(_) => AMresult::ObjId(AMobjId::new(obj_id)),
                _ => AMresult::Value(value, RefCell::<Option<CString>>::default()),
            },
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
            Ok(size) => AMresult::Value(
                am::Value::uint(size as u64),
                RefCell::<Option<CString>>::default(),
            ),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Result<Vec<am::Change>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<am::Change>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(changes) => AMresult::Changes(changes, None),
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
                AMresult::Changes(changes, None)
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
            Ok(bytes) => AMresult::Value(
                am::Value::bytes(bytes),
                RefCell::<Option<CString>>::default(),
            ),
            Err(e) => AMresult::err(&e.to_string()),
        }
    }
}

impl From<Vec<&am::Change>> for AMresult {
    fn from(changes: Vec<&am::Change>) -> Self {
        let changes: Vec<am::Change> = changes.iter().map(|&change| change.clone()).collect();
        AMresult::Changes(changes, None)
    }
}

impl From<Vec<am::ChangeHash>> for AMresult {
    fn from(change_hashes: Vec<am::ChangeHash>) -> Self {
        AMresult::ChangeHashes(change_hashes)
    }
}

impl From<Vec<u8>> for AMresult {
    fn from(bytes: Vec<u8>) -> Self {
        AMresult::Value(
            am::Value::bytes(bytes),
            RefCell::<Option<CString>>::default(),
        )
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
/// \pre \p result `!= NULL`.
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
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
/// \pre \p result `!= NULL`.
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
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
/// \pre \p result `!= NULL`.
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultSize(result: *const AMresult) -> usize {
    if let Some(result) = result.as_ref() {
        use AMresult::*;

        match result {
            Error(_) | Void => 0,
            ActorId(_, _)
            | Doc(_)
            | ObjId(_)
            | String(_)
            | SyncMessage(_)
            | SyncState(_)
            | Value(_, _) => 1,
            ChangeHashes(change_hashes) => change_hashes.len(),
            Changes(changes, _) => changes.len(),
            ListItems(list_items) => list_items.len(),
            MapItems(map_items) => map_items.len(),
            ObjItems(obj_items) => obj_items.len(),
            Strings(cstrings) => cstrings.len(),
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
/// \pre \p result `!= NULL`.
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
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
/// \param[in] result A pointer to an `AMresult` struct.
/// \return An `AMvalue` struct.
/// \pre \p result `!= NULL`.
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultValue<'a>(result: *mut AMresult) -> AMvalue<'a> {
    let mut content = AMvalue::Void;
    if let Some(result) = result.as_mut() {
        match result {
            AMresult::ActorId(actor_id, c_actor_id) => match c_actor_id {
                None => {
                    content = AMvalue::ActorId(&*c_actor_id.insert(AMactorId::new(&*actor_id)));
                }
                Some(c_actor_id) => {
                    content = AMvalue::ActorId(&*c_actor_id);
                }
            },
            AMresult::ChangeHashes(change_hashes) => {
                content = AMvalue::ChangeHashes(AMchangeHashes::new(change_hashes));
            }
            AMresult::Changes(changes, storage) => {
                content = AMvalue::Changes(AMchanges::new(
                    changes,
                    storage.get_or_insert(BTreeMap::new()),
                ));
            }
            AMresult::Doc(doc) => content = AMvalue::Doc(&mut **doc),
            AMresult::Error(_) => {}
            AMresult::ListItems(list_items) => {
                content = AMvalue::ListItems(AMlistItems::new(list_items));
            }
            AMresult::MapItems(map_items) => {
                content = AMvalue::MapItems(AMmapItems::new(map_items));
            }
            AMresult::ObjId(obj_id) => {
                content = AMvalue::ObjId(obj_id);
            }
            AMresult::ObjItems(obj_items) => {
                content = AMvalue::ObjItems(AMobjItems::new(obj_items));
            }
            AMresult::String(cstring) => content = AMvalue::Str(cstring.as_ptr()),
            AMresult::Strings(cstrings) => {
                content = AMvalue::Strs(AMstrs::new(cstrings));
            }
            AMresult::SyncMessage(sync_message) => {
                content = AMvalue::SyncMessage(sync_message);
            }
            AMresult::SyncState(sync_state) => {
                content = AMvalue::SyncState(&mut *sync_state);
            }
            AMresult::Value(value, value_str) => {
                content = (&*value, &*value_str).into();
            }
            AMresult::Void => {}
        }
    };
    content
}

/// \struct AMunknownValue
/// \installed_headerfile
/// \brief A value (typically for a `set` operation) whose type is unknown.
#[derive(PartialEq)]
#[repr(C)]
pub struct AMunknownValue {
    bytes: AMbyteSpan,
    type_code: u8,
}

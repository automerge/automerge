use am::marks::Mark;
use automerge as am;

use crate::byte_span::AMbyteSpan;
use crate::index::AMindex;
use crate::item::AMitem;
use crate::items::AMitems;

/// \struct AMresult
/// \installed_headerfile
/// \brief A discriminated union of result variants.
pub enum AMresult {
    Items(Vec<AMitem>),
    Error(String),
}

impl AMresult {
    pub(crate) fn error(s: &str) -> Self {
        Self::Error(s.to_string())
    }

    pub(crate) fn item(item: AMitem) -> Self {
        Self::Items(vec![item])
    }

    pub(crate) fn items(items: Vec<AMitem>) -> Self {
        Self::Items(items)
    }
}

impl Default for AMresult {
    fn default() -> Self {
        Self::Items(vec![])
    }
}

impl From<am::AutoCommit> for AMresult {
    fn from(auto_commit: am::AutoCommit) -> Self {
        Self::item(AMitem::exact(am::ROOT, auto_commit.into()))
    }
}

impl From<am::Change> for AMresult {
    fn from(change: am::Change) -> Self {
        Self::item(change.into())
    }
}

impl From<am::ChangeHash> for AMresult {
    fn from(change_hash: am::ChangeHash) -> Self {
        Self::item(change_hash.into())
    }
}

impl From<Option<am::ChangeHash>> for AMresult {
    fn from(maybe: Option<am::ChangeHash>) -> Self {
        match maybe {
            Some(change_hash) => change_hash.into(),
            None => Self::item(Default::default()),
        }
    }
}

impl From<Result<am::ChangeHash, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::ChangeHash, am::AutomergeError>) -> Self {
        match maybe {
            Ok(change_hash) => change_hash.into(),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<&am::ScalarValue> for AMresult {
    fn from(value: &am::ScalarValue) -> Self {
        Self::item(value.into())
    }
}

impl From<am::sync::State> for AMresult {
    fn from(state: am::sync::State) -> Self {
        Self::item(state.into())
    }
}

impl From<am::iter::Values<'static>> for AMresult {
    fn from(pairs: am::iter::Values<'static>) -> Self {
        Self::items(pairs.map(|(v, o)| AMitem::exact(o, v.into())).collect())
    }
}

impl From<AMresult> for *mut AMresult {
    fn from(b: AMresult) -> Self {
        Box::into_raw(Box::new(b))
    }
}

impl From<am::iter::Keys<'_>> for AMresult {
    fn from(keys: am::iter::Keys<'_>) -> Self {
        Self::items(keys.map(|s| s.into()).collect())
    }
}

impl From<am::iter::ListRange<'static>> for AMresult {
    fn from(list_range: am::iter::ListRange<'static>) -> Self {
        Self::items(
            list_range
                .map(|item| AMitem::indexed(AMindex::Pos(item.index), item.id(), item.value.into()))
                .collect(),
        )
    }
}

impl From<am::iter::MapRange<'static>> for AMresult {
    fn from(map_range: am::iter::MapRange<'static>) -> Self {
        Self::items(
            map_range
                .map(|item| {
                    AMitem::indexed(
                        AMindex::Key(item.key.clone().into()),
                        item.id(),
                        item.value.into(),
                    )
                })
                .collect(),
        )
    }
}

impl From<Option<am::Change>> for AMresult {
    fn from(maybe: Option<am::Change>) -> Self {
        Self::item(match maybe {
            Some(change) => change.into(),
            None => Default::default(),
        })
    }
}

impl From<Option<am::sync::Message>> for AMresult {
    fn from(maybe: Option<am::sync::Message>) -> Self {
        Self::item(match maybe {
            Some(message) => message.into(),
            None => Default::default(),
        })
    }
}

impl From<Result<(), am::AutomergeError>> for AMresult {
    fn from(maybe: Result<(), am::AutomergeError>) -> Self {
        match maybe {
            Ok(()) => Self::item(Default::default()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<am::ActorId, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::ActorId, am::AutomergeError>) -> Self {
        match maybe {
            Ok(actor_id) => Self::item(actor_id.into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<am::ActorId, am::InvalidActorId>> for AMresult {
    fn from(maybe: Result<am::ActorId, am::InvalidActorId>) -> Self {
        match maybe {
            Ok(actor_id) => Self::item(actor_id.into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<am::AutoCommit, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::AutoCommit, am::AutomergeError>) -> Self {
        match maybe {
            Ok(auto_commit) => Self::item(auto_commit.into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<am::Change, am::LoadChangeError>> for AMresult {
    fn from(maybe: Result<am::Change, am::LoadChangeError>) -> Self {
        match maybe {
            Ok(change) => Self::item(change.into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<am::Cursor, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::Cursor, am::AutomergeError>) -> Self {
        match maybe {
            Ok(cursor) => Self::item(cursor.into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<(Result<am::ObjId, am::AutomergeError>, &str, am::ObjType)> for AMresult {
    fn from(
        (maybe, key, obj_type): (Result<am::ObjId, am::AutomergeError>, &str, am::ObjType),
    ) -> Self {
        match maybe {
            Ok(obj_id) => Self::item(AMitem::indexed(
                AMindex::Key(key.into()),
                obj_id,
                am::Value::Object(obj_type).into(),
            )),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<(Result<am::ObjId, am::AutomergeError>, usize, am::ObjType)> for AMresult {
    fn from(
        (maybe, pos, obj_type): (Result<am::ObjId, am::AutomergeError>, usize, am::ObjType),
    ) -> Self {
        match maybe {
            Ok(obj_id) => Self::item(AMitem::indexed(
                AMindex::Pos(pos),
                obj_id,
                am::Value::Object(obj_type).into(),
            )),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<am::sync::Message, am::sync::ReadMessageError>> for AMresult {
    fn from(maybe: Result<am::sync::Message, am::sync::ReadMessageError>) -> Self {
        match maybe {
            Ok(message) => Self::item(message.into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<am::sync::State, am::sync::DecodeStateError>> for AMresult {
    fn from(maybe: Result<am::sync::State, am::sync::DecodeStateError>) -> Self {
        match maybe {
            Ok(state) => Self::item(state.into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<am::Value<'static>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<am::Value<'static>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(value) => Self::item(value.into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl
    From<(
        Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>,
        &str,
    )> for AMresult
{
    fn from(
        (maybe, key): (
            Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>,
            &str,
        ),
    ) -> Self {
        match maybe {
            Ok(Some((value, obj_id))) => Self::item(AMitem::indexed(
                AMindex::Key(key.into()),
                obj_id,
                value.into(),
            )),
            Ok(None) => Self::item(Default::default()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl
    From<(
        Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>,
        usize,
    )> for AMresult
{
    fn from(
        (maybe, pos): (
            Result<Option<(am::Value<'static>, am::ObjId)>, am::AutomergeError>,
            usize,
        ),
    ) -> Self {
        match maybe {
            Ok(Some((value, obj_id))) => {
                Self::item(AMitem::indexed(AMindex::Pos(pos), obj_id, value.into()))
            }
            Ok(None) => Self::item(Default::default()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<String, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<String, am::AutomergeError>) -> Self {
        match maybe {
            Ok(string) => Self::item(string.into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<usize, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<usize, am::AutomergeError>) -> Self {
        match maybe {
            Ok(size) => Self::item(am::Value::uint(size as u64).into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<Vec<am::Change>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<am::Change>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(changes) => Self::items(changes.into_iter().map(|change| change.into()).collect()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<Vec<&am::Change>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<&am::Change>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(changes) => Self::items(
                changes
                    .into_iter()
                    .map(|change| change.clone().into())
                    .collect(),
            ),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<Vec<am::Change>, am::LoadChangeError>> for AMresult {
    fn from(maybe: Result<Vec<am::Change>, am::LoadChangeError>) -> Self {
        match maybe {
            Ok(changes) => Self::items(changes.into_iter().map(|change| change.into()).collect()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<Vec<am::ChangeHash>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<am::ChangeHash>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(change_hashes) => Self::items(
                change_hashes
                    .into_iter()
                    .map(|change_hash| change_hash.into())
                    .collect(),
            ),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<Vec<am::ChangeHash>, am::InvalidChangeHashSlice>> for AMresult {
    fn from(maybe: Result<Vec<am::ChangeHash>, am::InvalidChangeHashSlice>) -> Self {
        match maybe {
            Ok(change_hashes) => Self::items(
                change_hashes
                    .into_iter()
                    .map(|change_hash| change_hash.into())
                    .collect(),
            ),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<Vec<(am::Value<'static>, am::ObjId)>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<(am::Value<'static>, am::ObjId)>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(pairs) => Self::items(
                pairs
                    .into_iter()
                    .map(|(v, o)| AMitem::exact(o, v.into()))
                    .collect(),
            ),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<Vec<Mark>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<Mark>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(marks) => Self::items(marks.iter().map(|mark| mark.clone().into()).collect()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<Result<Vec<u8>, am::AutomergeError>> for AMresult {
    fn from(maybe: Result<Vec<u8>, am::AutomergeError>) -> Self {
        match maybe {
            Ok(bytes) => Self::item(am::Value::bytes(bytes).into()),
            Err(e) => Self::error(&e.to_string()),
        }
    }
}

impl From<&[am::Change]> for AMresult {
    fn from(changes: &[am::Change]) -> Self {
        Self::items(changes.iter().map(|change| change.clone().into()).collect())
    }
}

impl From<Vec<am::Change>> for AMresult {
    fn from(changes: Vec<am::Change>) -> Self {
        Self::items(changes.into_iter().map(|change| change.into()).collect())
    }
}

impl From<&[am::ChangeHash]> for AMresult {
    fn from(change_hashes: &[am::ChangeHash]) -> Self {
        Self::items(
            change_hashes
                .iter()
                .map(|change_hash| (*change_hash).into())
                .collect(),
        )
    }
}

impl From<&[am::sync::Have]> for AMresult {
    fn from(haves: &[am::sync::Have]) -> Self {
        Self::items(haves.iter().map(|have| have.clone().into()).collect())
    }
}

impl From<Vec<am::ChangeHash>> for AMresult {
    fn from(change_hashes: Vec<am::ChangeHash>) -> Self {
        Self::items(
            change_hashes
                .into_iter()
                .map(|change_hash| change_hash.into())
                .collect(),
        )
    }
}

impl From<Vec<am::sync::Have>> for AMresult {
    fn from(haves: Vec<am::sync::Have>) -> Self {
        Self::items(haves.into_iter().map(|have| have.into()).collect())
    }
}

impl From<Vec<u8>> for AMresult {
    fn from(bytes: Vec<u8>) -> Self {
        Self::item(am::Value::bytes(bytes).into())
    }
}

pub fn to_result<R: Into<AMresult>>(r: R) -> *mut AMresult {
    (r.into()).into()
}

/// \ingroup enumerations
/// \enum AMstatus
/// \installed_headerfile
/// \brief The status of an API call.
#[derive(Eq, PartialEq)]
#[repr(C)]
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
/// \brief Concatenates the items from two results.
///
/// \param[in] dest A pointer to an `AMresult` struct.
/// \param[in] src A pointer to an `AMresult` struct.
/// \return A pointer to an `AMresult` struct with the items from \p dest in
///         their original order followed by the items from \p src in their
///         original order.
/// \pre \p dest `!= NULL`
/// \pre \p src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// dest must be a valid pointer to an AMresult
/// src must be a valid pointer to an AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultCat(dest: *const AMresult, src: *const AMresult) -> *mut AMresult {
    use AMresult::*;

    match (dest.as_ref(), src.as_ref()) {
        (Some(dest), Some(src)) => match (dest, src) {
            (Items(dest_items), Items(src_items)) => AMresult::items(
                dest_items
                    .iter()
                    .cloned()
                    .chain(src_items.iter().cloned())
                    .collect(),
            )
            .into(),
            (Error(_), Error(_)) | (Error(_), Items(_)) | (Items(_), Error(_)) => {
                AMresult::error("Invalid `AMresult`").into()
            }
        },
        (None, None) | (None, Some(_)) | (Some(_), None) => {
            AMresult::error("Invalid `AMresult*`").into()
        }
    }
}

/// \memberof AMresult
/// \brief Gets a result's error message string.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return A UTF-8 string view as an `AMbyteSpan` struct.
/// \pre \p result `!= NULL`
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultError(result: *const AMresult) -> AMbyteSpan {
    use AMresult::*;

    if let Some(Error(message)) = result.as_ref() {
        return message.as_bytes().into();
    }
    Default::default()
}

/// \memberof AMresult
/// \brief Deallocates the storage for a result.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \pre \p result `!= NULL`
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultFree(result: *mut AMresult) {
    if !result.is_null() {
        let result: AMresult = *Box::from_raw(result);
        drop(result)
    }
}

/// \memberof AMresult
/// \brief Gets a result's first item.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return A pointer to an `AMitem` struct.
/// \pre \p result `!= NULL`
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultItem(result: *mut AMresult) -> *mut AMitem {
    use AMresult::*;

    if let Some(Items(items)) = result.as_mut() {
        if !items.is_empty() {
            return &mut items[0];
        }
    }
    std::ptr::null_mut()
}

/// \memberof AMresult
/// \brief Gets a result's items.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return An `AMitems` struct.
/// \pre \p result `!= NULL`
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultItems<'a>(result: *mut AMresult) -> AMitems<'a> {
    use AMresult::*;

    if let Some(Items(items)) = result.as_mut() {
        if !items.is_empty() {
            return AMitems::new(items);
        }
    }
    Default::default()
}

/// \memberof AMresult
/// \brief Gets the size of a result.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return The count of items within \p result.
/// \pre \p result `!= NULL`
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultSize(result: *const AMresult) -> usize {
    use self::AMresult::*;

    if let Some(Items(items)) = result.as_ref() {
        return items.len();
    }
    0
}

/// \memberof AMresult
/// \brief Gets the status code of a result.
///
/// \param[in] result A pointer to an `AMresult` struct.
/// \return An `AMstatus` enum tag.
/// \pre \p result `!= NULL`
/// \internal
///
/// # Safety
/// result must be a valid pointer to an AMresult
#[no_mangle]
pub unsafe extern "C" fn AMresultStatus(result: *const AMresult) -> AMstatus {
    use AMresult::*;

    if let Some(result) = result.as_ref() {
        match result {
            Error(_) => {
                return AMstatus::Error;
            }
            _ => {
                return AMstatus::Ok;
            }
        }
    }
    AMstatus::InvalidResult
}

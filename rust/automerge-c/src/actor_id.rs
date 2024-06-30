use automerge as am;
use libc::c_int;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::str::FromStr;

use crate::byte_span::AMbyteSpan;
use crate::result::{to_result, AMresult};

macro_rules! to_actor_id {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::error("Invalid `AMactorId*`").into(),
        }
    }};
}

pub(crate) use to_actor_id;

/// \struct AMactorId
/// \installed_headerfile
/// \brief An actor's unique identifier.
#[derive(Eq, PartialEq)]
pub struct AMactorId {
    body: *const am::ActorId,
    hex_str: RefCell<Option<Box<str>>>,
}

impl AMactorId {
    pub fn new(actor_id: &am::ActorId) -> Self {
        Self {
            body: actor_id,
            hex_str: Default::default(),
        }
    }

    fn to_str(&self) -> AMbyteSpan {
        let mut hex_str = self.hex_str.borrow_mut();
        match hex_str.as_mut() {
            None => {
                let hex_string = unsafe { (*self.body).to_hex_string() };
                hex_str
                    .insert(hex_string.into_boxed_str())
                    .as_bytes()
                    .into()
            }
            Some(hex_str) => hex_str.as_bytes().into(),
        }
    }
}

impl AsRef<am::ActorId> for AMactorId {
    fn as_ref(&self) -> &am::ActorId {
        unsafe { &*self.body }
    }
}

/// \memberof AMactorId
/// \brief Gets the value of an actor identifier as an array of bytes.
///
/// \param[in] actor_id A pointer to an `AMactorId` struct.
/// \return An `AMbyteSpan` struct for an array of bytes.
/// \pre \p actor_id `!= NULL`
/// \internal
///
/// # Safety
/// actor_id must be a valid pointer to an AMactorId
#[no_mangle]
pub unsafe extern "C" fn AMactorIdBytes(actor_id: *const AMactorId) -> AMbyteSpan {
    match actor_id.as_ref() {
        Some(actor_id) => actor_id.as_ref().into(),
        None => Default::default(),
    }
}

/// \memberof AMactorId
/// \brief Compares two actor identifiers.
///
/// \param[in] actor_id1 A pointer to an `AMactorId` struct.
/// \param[in] actor_id2 A pointer to an `AMactorId` struct.
/// \return `-1` if \p actor_id1 `<` \p actor_id2, `0` if
///         \p actor_id1 `==` \p actor_id2 and `1` if
///         \p actor_id1 `>` \p actor_id2.
/// \pre \p actor_id1 `!= NULL`
/// \pre \p actor_id2 `!= NULL`
/// \internal
///
/// #Safety
/// actor_id1 must be a valid pointer to an AMactorId
/// actor_id2 must be a valid pointer to an AMactorId
#[no_mangle]
pub unsafe extern "C" fn AMactorIdCmp(
    actor_id1: *const AMactorId,
    actor_id2: *const AMactorId,
) -> c_int {
    match (actor_id1.as_ref(), actor_id2.as_ref()) {
        (Some(actor_id1), Some(actor_id2)) => match actor_id1.as_ref().cmp(actor_id2.as_ref()) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        },
        (None, Some(_)) => -1,
        (None, None) => 0,
        (Some(_), None) => 1,
    }
}

/// \memberof AMactorId
/// \brief Allocates a new actor identifier and initializes it from a random
///        UUID value.
///
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_ACTOR_ID` item.
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
#[no_mangle]
pub unsafe extern "C" fn AMactorIdInit() -> *mut AMresult {
    to_result(Ok::<am::ActorId, am::AutomergeError>(am::ActorId::random()))
}

/// \memberof AMactorId
/// \brief Allocates a new actor identifier and initializes it from an array of
///        bytes value.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The count of bytes to copy from the array pointed to by
///                  \p src.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_ACTOR_ID` item.
/// \pre \p src `!= NULL`
/// \pre `sizeof(`\p src `) > 0`
/// \pre \p count `<= sizeof(`\p src `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// src must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMactorIdFromBytes(src: *const u8, count: usize) -> *mut AMresult {
    if !src.is_null() {
        let value = std::slice::from_raw_parts(src, count);
        to_result(Ok::<am::ActorId, am::InvalidActorId>(am::ActorId::from(
            value,
        )))
    } else {
        AMresult::error("Invalid uint8_t*").into()
    }
}

/// \memberof AMactorId
/// \brief Allocates a new actor identifier and initializes it from a
///        hexadecimal UTF-8 string view value.
///
/// \param[in] value A UTF-8 string view as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_ACTOR_ID` item.
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// value.src must be a byte array of length >= value.count
#[no_mangle]
pub unsafe extern "C" fn AMactorIdFromStr(value: AMbyteSpan) -> *mut AMresult {
    use am::AutomergeError::InvalidActorId;

    to_result(match (&value).try_into() {
        Ok(s) => match am::ActorId::from_str(s) {
            Ok(actor_id) => Ok(actor_id),
            Err(_) => Err(InvalidActorId(String::from(s))),
        },
        Err(e) => Err(e),
    })
}

/// \memberof AMactorId
/// \brief Gets the value of an actor identifier as a UTF-8 hexadecimal string
///        view.
///
/// \param[in] actor_id A pointer to an `AMactorId` struct.
/// \return A UTF-8 string view as an `AMbyteSpan` struct.
/// \pre \p actor_id `!= NULL`
/// \internal
///
/// # Safety
/// actor_id must be a valid pointer to an AMactorId
#[no_mangle]
pub unsafe extern "C" fn AMactorIdStr(actor_id: *const AMactorId) -> AMbyteSpan {
    match actor_id.as_ref() {
        Some(actor_id) => actor_id.to_str(),
        None => Default::default(),
    }
}

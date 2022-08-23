use automerge as am;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::str::FromStr;

use crate::byte_span::AMbyteSpan;
use crate::result::{to_result, AMresult};

/// \struct AMactorId
/// \installed_headerfile
/// \brief An actor's unique identifier.
#[derive(PartialEq)]
pub struct AMactorId {
    body: *const am::ActorId,
    c_str: RefCell<Option<CString>>,
}

impl AMactorId {
    pub fn new(actor_id: &am::ActorId) -> Self {
        Self {
            body: actor_id,
            c_str: Default::default(),
        }
    }

    pub fn as_c_str(&self) -> *const c_char {
        let mut c_str = self.c_str.borrow_mut();
        match c_str.as_mut() {
            None => {
                let hex_str = unsafe { (*self.body).to_hex_string() };
                c_str.insert(CString::new(hex_str).unwrap()).as_ptr()
            }
            Some(hex_str) => hex_str.as_ptr(),
        }
    }
}

impl AsRef<am::ActorId> for AMactorId {
    fn as_ref(&self) -> &am::ActorId {
        unsafe { &*self.body }
    }
}

/// \memberof AMactorId
/// \brief Gets the value of an actor identifier as a sequence of bytes.
///
/// \param[in] actor_id A pointer to an `AMactorId` struct.
/// \pre \p actor_id `!= NULL`.
/// \return An `AMbyteSpan` struct.
/// \internal
///
/// # Safety
/// actor_id must be a valid pointer to an AMactorId
#[no_mangle]
pub unsafe extern "C" fn AMactorIdBytes(actor_id: *const AMactorId) -> AMbyteSpan {
    match actor_id.as_ref() {
        Some(actor_id) => actor_id.as_ref().into(),
        None => AMbyteSpan::default(),
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
/// \pre \p actor_id1 `!= NULL`.
/// \pre \p actor_id2 `!= NULL`.
/// \internal
///
/// #Safety
/// actor_id1 must be a valid pointer to an AMactorId
/// actor_id2 must be a valid pointer to an AMactorId
#[no_mangle]
pub unsafe extern "C" fn AMactorIdCmp(
    actor_id1: *const AMactorId,
    actor_id2: *const AMactorId,
) -> isize {
    match (actor_id1.as_ref(), actor_id2.as_ref()) {
        (Some(actor_id1), Some(actor_id2)) => match actor_id1.as_ref().cmp(actor_id2.as_ref()) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        },
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (None, None) => 0,
    }
}

/// \memberof AMactorId
/// \brief Allocates a new actor identifier and initializes it with a random
///        UUID.
///
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMactorId` struct.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
#[no_mangle]
pub unsafe extern "C" fn AMactorIdInit() -> *mut AMresult {
    to_result(Ok::<am::ActorId, am::AutomergeError>(am::ActorId::random()))
}

/// \memberof AMactorId
/// \brief Allocates a new actor identifier and initializes it from a sequence
///        of bytes.
///
/// \param[in] src A pointer to a contiguous sequence of bytes.
/// \param[in] count The number of bytes to copy from \p src.
/// \pre `0 <` \p count `<= sizeof(`\p src`)`.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMactorId` struct.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// src must be a byte array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMactorIdInitBytes(src: *const u8, count: usize) -> *mut AMresult {
    let slice = std::slice::from_raw_parts(src, count);
    to_result(Ok::<am::ActorId, am::InvalidActorId>(am::ActorId::from(
        slice,
    )))
}

/// \memberof AMactorId
/// \brief Allocates a new actor identifier and initializes it from a
///        hexadecimal string.
///
/// \param[in] hex_str A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMactorId` struct.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// hex_str must be a null-terminated array of `c_char`
#[no_mangle]
pub unsafe extern "C" fn AMactorIdInitStr(hex_str: *const c_char) -> *mut AMresult {
    to_result(am::ActorId::from_str(
        CStr::from_ptr(hex_str).to_str().unwrap(),
    ))
}

/// \memberof AMactorId
/// \brief Gets the value of an actor identifier as a hexadecimal string.
///
/// \param[in] actor_id A pointer to an `AMactorId` struct.
/// \pre \p actor_id `!= NULL`.
/// \return A UTF-8 string.
/// \internal
///
/// # Safety
/// actor_id must be a valid pointer to an AMactorId
#[no_mangle]
pub unsafe extern "C" fn AMactorIdStr(actor_id: *const AMactorId) -> *const c_char {
    match actor_id.as_ref() {
        Some(actor_id) => actor_id.as_c_str(),
        None => std::ptr::null::<c_char>(),
    }
}

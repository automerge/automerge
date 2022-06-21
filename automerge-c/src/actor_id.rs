use automerge as am;
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::str::FromStr;

use crate::byte_span::AMbyteSpan;
use crate::result::{to_result, AMresult};

/// \struct AMactorId
/// \brief An actor's unique identifier.
pub struct AMactorId {
    body: am::ActorId,
    c_str: RefCell<Option<CString>>,
}

impl AMactorId {
    pub fn new(body: am::ActorId) -> Self {
        Self {
            body,
            c_str: RefCell::<Option<CString>>::default(),
        }
    }

    pub fn as_c_str(&self) -> *const c_char {
        let mut c_str = self.c_str.borrow_mut();
        match c_str.as_mut() {
            None => {
                let hex_str = self.body.to_hex_string();
                c_str.insert(CString::new(hex_str).unwrap()).as_ptr()
            }
            Some(value) => value.as_ptr(),
        }
    }
}

impl AsRef<am::ActorId> for AMactorId {
    fn as_ref(&self) -> &am::ActorId {
        &self.body
    }
}

/// \memberof AMactorId
/// \brief Gets the value of an actor ID as a sequence of bytes.
///
/// \param[in] actor_id A pointer to an `AMactorId` struct.
/// \pre \p actor_id must be a valid address.
/// \return An `AMbyteSpan` struct.
/// \internal
///
/// # Safety
/// actor_id must be a pointer to a valid AMactorId
#[no_mangle]
pub unsafe extern "C" fn AMactorIdBytes(actor_id: *const AMactorId) -> AMbyteSpan {
    match actor_id.as_ref() {
        Some(actor_id) => actor_id.as_ref().into(),
        None => AMbyteSpan::default(),
    }
}

/// \memberof AMactorId
/// \brief Allocates a new actor ID and initializes it with a random UUID.
///
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMactorId` struct.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
#[no_mangle]
pub unsafe extern "C" fn AMactorIdInit() -> *mut AMresult {
    to_result(Ok::<am::ActorId, am::AutomergeError>(am::ActorId::random()))
}

/// \memberof AMactorId
/// \brief Allocates a new actor ID and initializes it from a sequence of
///        bytes.
///
/// \param[in] src A pointer to a contiguous sequence of bytes.
/// \param[in] count The number of bytes to copy from \p src.
/// \pre `0 <=` \p count `<=` size of \p src.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMactorId` struct.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
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
/// \brief Allocates a new actor ID and initializes it from a hexadecimal
///        string.
///
/// \param[in] hex_str A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMactorId` struct.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// hex_str must be a null-terminated array of `c_char`
#[no_mangle]
pub unsafe extern "C" fn AMactorIdInitStr(hex_str: *const c_char) -> *mut AMresult {
    to_result(am::ActorId::from_str(
        CStr::from_ptr(hex_str).to_str().unwrap(),
    ))
}

/// \memberof AMactorId
/// \brief Gets the value of an actor ID as a hexadecimal string.
///
/// \param[in] actor_id A pointer to an `AMactorId` struct.
/// \pre \p actor_id must be a valid address.
/// \return A UTF-8 string.
/// \internal
///
/// # Safety
/// actor_id must be a pointer to a valid AMactorId
#[no_mangle]
pub unsafe extern "C" fn AMactorIdStr(actor_id: *const AMactorId) -> *const c_char {
    match actor_id.as_ref() {
        Some(actor_id) => actor_id.as_c_str(),
        None => std::ptr::null::<c_char>(),
    }
}

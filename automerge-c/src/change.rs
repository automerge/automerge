use automerge as am;
use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;

use crate::byte_span::AMbyteSpan;
use crate::change_hashes::AMchangeHashes;
use crate::result::{to_result, AMresult};

macro_rules! to_change {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::err("Invalid AMchange pointer").into(),
        }
    }};
}

/// \struct AMchange
/// \installed_headerfile
/// \brief A group of operations performed by an actor.
#[derive(Eq, PartialEq)]
pub struct AMchange {
    body: *mut am::Change,
    c_msg: RefCell<Option<CString>>,
    c_changehash: RefCell<Option<am::ChangeHash>>,
}

impl AMchange {
    pub fn new(change: &mut am::Change) -> Self {
        Self {
            body: change,
            c_msg: Default::default(),
            c_changehash: Default::default(),
        }
    }

    pub fn message(&self) -> *const c_char {
        let mut c_msg = self.c_msg.borrow_mut();
        match c_msg.as_mut() {
            None => {
                if let Some(message) = unsafe { (*self.body).message() } {
                    return c_msg
                        .insert(CString::new(message.as_bytes()).unwrap())
                        .as_ptr();
                }
            }
            Some(message) => {
                return message.as_ptr();
            }
        }
        std::ptr::null()
    }

    pub fn hash(&self) -> AMbyteSpan {
        let mut c_changehash = self.c_changehash.borrow_mut();
        if let Some(c_changehash) = c_changehash.as_ref() {
            c_changehash.into()
        } else {
            let hash = unsafe { (*self.body).hash() };
            let ptr = c_changehash.insert(hash);
            AMbyteSpan {
                src: ptr.0.as_ptr(),
                count: hash.as_ref().len(),
            }
        }
    }
}

impl AsMut<am::Change> for AMchange {
    fn as_mut(&mut self) -> &mut am::Change {
        unsafe { &mut *self.body }
    }
}

impl AsRef<am::Change> for AMchange {
    fn as_ref(&self) -> &am::Change {
        unsafe { &*self.body }
    }
}

/// \memberof AMchange
/// \brief Gets the first referenced actor identifier in a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \pre \p change `!= NULL`.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMactorId` struct.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeActorId(change: *const AMchange) -> *mut AMresult {
    let change = to_change!(change);
    to_result(Ok::<am::ActorId, am::AutomergeError>(
        change.as_ref().actor_id().clone(),
    ))
}

/// \memberof AMchange
/// \brief Compresses the raw bytes of a change.
///
/// \param[in,out] change A pointer to an `AMchange` struct.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeCompress(change: *mut AMchange) {
    if let Some(change) = change.as_mut() {
        let _ = change.as_mut().bytes();
    };
}

/// \memberof AMchange
/// \brief Gets the dependencies of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A pointer to an `AMchangeHashes` struct or `NULL`.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeDeps(change: *const AMchange) -> AMchangeHashes {
    match change.as_ref() {
        Some(change) => AMchangeHashes::new(change.as_ref().deps()),
        None => AMchangeHashes::default(),
    }
}

/// \memberof AMchange
/// \brief Gets the extra bytes of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return An `AMbyteSpan` struct.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeExtraBytes(change: *const AMchange) -> AMbyteSpan {
    if let Some(change) = change.as_ref() {
        change.as_ref().extra_bytes().into()
    } else {
        AMbyteSpan::default()
    }
}

/// \memberof AMchange
/// \brief Loads a sequence of bytes into a change.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes in \p src to load.
/// \return A pointer to an `AMresult` struct containing an `AMchange` struct.
/// \pre \p src `!= NULL`.
/// \pre `0 <` \p count `<= sizeof(`\p src`)`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// src must be a byte array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMchangeFromBytes(src: *const u8, count: usize) -> *mut AMresult {
    let mut data = Vec::new();
    data.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(am::Change::from_bytes(data))
}

/// \memberof AMchange
/// \brief Gets the hash of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A change hash as an `AMbyteSpan` struct.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeHash(change: *const AMchange) -> AMbyteSpan {
    match change.as_ref() {
        Some(change) => change.hash(),
        None => AMbyteSpan::default(),
    }
}

/// \memberof AMchange
/// \brief Tests the emptiness of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A boolean.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeIsEmpty(change: *const AMchange) -> bool {
    if let Some(change) = change.as_ref() {
        change.as_ref().is_empty()
    } else {
        true
    }
}

/// \memberof AMchange
/// \brief Gets the maximum operation index of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeMaxOp(change: *const AMchange) -> u64 {
    if let Some(change) = change.as_ref() {
        change.as_ref().max_op()
    } else {
        u64::MAX
    }
}

/// \memberof AMchange
/// \brief Gets the message of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A UTF-8 string or `NULL`.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeMessage(change: *const AMchange) -> *const c_char {
    if let Some(change) = change.as_ref() {
        return change.message();
    };
    std::ptr::null()
}

/// \memberof AMchange
/// \brief Gets the index of a change in the changes from an actor.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeSeq(change: *const AMchange) -> u64 {
    if let Some(change) = change.as_ref() {
        change.as_ref().seq()
    } else {
        u64::MAX
    }
}

/// \memberof AMchange
/// \brief Gets the size of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeSize(change: *const AMchange) -> usize {
    if let Some(change) = change.as_ref() {
        change.as_ref().len()
    } else {
        0
    }
}

/// \memberof AMchange
/// \brief Gets the start operation index of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeStartOp(change: *const AMchange) -> u64 {
    if let Some(change) = change.as_ref() {
        u64::from(change.as_ref().start_op())
    } else {
        u64::MAX
    }
}

/// \memberof AMchange
/// \brief Gets the commit time of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A 64-bit signed integer.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeTime(change: *const AMchange) -> i64 {
    if let Some(change) = change.as_ref() {
        change.as_ref().timestamp()
    } else {
        i64::MAX
    }
}

/// \memberof AMchange
/// \brief Gets the raw bytes of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return An `AMbyteSpan` struct.
/// \pre \p change `!= NULL`.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeRawBytes(change: *const AMchange) -> AMbyteSpan {
    if let Some(change) = change.as_ref() {
        change.as_ref().raw_bytes().into()
    } else {
        AMbyteSpan::default()
    }
}

/// \memberof AMchange
/// \brief Loads a document into a sequence of changes.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes in \p src to load.
/// \return A pointer to an `AMresult` struct containing a sequence of
///         `AMchange` structs.
/// \pre \p src `!= NULL`.
/// \pre `0 <` \p count `<= sizeof(`\p src`)`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// src must be a byte array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMchangeLoadDocument(src: *const u8, count: usize) -> *mut AMresult {
    let mut data = Vec::new();
    data.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result::<Result<Vec<am::Change>, _>>(
        am::Automerge::load(&data)
            .and_then(|d| d.get_changes(&[]).map(|c| c.into_iter().cloned().collect())),
    )
}

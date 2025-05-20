use automerge as am;
use std::cell::RefCell;

use crate::byte_span::AMbyteSpan;
use crate::result::{to_result, AMresult};

macro_rules! to_change {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::error("Invalid `AMchange*`").into(),
        }
    }};
}

/// \struct AMchange
/// \installed_headerfile
/// \brief A group of operations performed by an actor.
#[derive(Eq, PartialEq)]
pub struct AMchange {
    body: *mut am::Change,
    change_hash: RefCell<Option<am::ChangeHash>>,
}

impl AMchange {
    pub fn new(change: &mut am::Change) -> Self {
        Self {
            body: change,
            change_hash: Default::default(),
        }
    }

    fn message(&self) -> AMbyteSpan {
        if let Some(message) = unsafe { (*self.body).message() } {
            return message.as_str().as_bytes().into();
        }
        Default::default()
    }

    fn hash(&self) -> AMbyteSpan {
        let mut change_hash = self.change_hash.borrow_mut();
        if let Some(change_hash) = change_hash.as_ref() {
            change_hash.into()
        } else {
            let hash = unsafe { (*self.body).hash() };
            let ptr = change_hash.insert(hash);
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
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_ACTOR_ID` item.
/// \pre \p change `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \param[in] change A pointer to an `AMchange` struct.
/// \pre \p change `!= NULL`
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
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE_HASH` items.
/// \pre \p change `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeDeps(change: *const AMchange) -> *mut AMresult {
    to_result(match change.as_ref() {
        Some(change) => change.as_ref().deps(),
        None => Default::default(),
    })
}

/// \memberof AMchange
/// \brief Gets the extra bytes of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return An `AMbyteSpan` struct.
/// \pre \p change `!= NULL`
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeExtraBytes(change: *const AMchange) -> AMbyteSpan {
    if let Some(change) = change.as_ref() {
        change.as_ref().extra_bytes().into()
    } else {
        Default::default()
    }
}

/// \memberof AMchange
/// \brief Allocates a new change and initializes it from an array of bytes value.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The count of bytes to load from the array pointed to by
///                  \p src.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_CHANGE` item.
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
pub unsafe extern "C" fn AMchangeFromBytes(src: *const u8, count: usize) -> *mut AMresult {
    let data = std::slice::from_raw_parts(src, count);
    to_result(am::Change::from_bytes(data.to_vec()))
}

/// \memberof AMchange
/// \brief Gets the hash of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return An `AMbyteSpan` struct for a change hash.
/// \pre \p change `!= NULL`
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeHash(change: *const AMchange) -> AMbyteSpan {
    match change.as_ref() {
        Some(change) => change.hash(),
        None => Default::default(),
    }
}

/// \memberof AMchange
/// \brief Tests the emptiness of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return `true` if \p change is empty, `false` otherwise.
/// \pre \p change `!= NULL`
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
/// \brief Loads a document into a sequence of changes.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The count of bytes to load from the array pointed to by
///                  \p src.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE` items.
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
pub unsafe extern "C" fn AMchangeLoadDocument(src: *const u8, count: usize) -> *mut AMresult {
    let data = std::slice::from_raw_parts(src, count);
    to_result::<Result<Vec<am::Change>, _>>(
        am::Automerge::load(data).map(|d| d.get_changes(&[]).into_iter().collect()),
    )
}

/// \memberof AMchange
/// \brief Gets the maximum operation index of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p change `!= NULL`
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
/// \return A UTF-8 string view as an `AMbyteSpan` struct.
/// \pre \p change `!= NULL`
/// \post `(`\p change `== NULL) -> (AMbyteSpan){NULL, 0}`
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeMessage(change: *const AMchange) -> AMbyteSpan {
    if let Some(change) = change.as_ref() {
        return change.message();
    };
    Default::default()
}

/// \memberof AMchange
/// \brief Gets the index of a change in the changes from an actor.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p change `!= NULL`
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
/// \pre \p change `!= NULL`
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeSize(change: *const AMchange) -> usize {
    if let Some(change) = change.as_ref() {
        return change.as_ref().len();
    }
    0
}

/// \memberof AMchange
/// \brief Gets the start operation index of a change.
///
/// \param[in] change A pointer to an `AMchange` struct.
/// \return A 64-bit unsigned integer.
/// \pre \p change `!= NULL`
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
/// \pre \p change `!= NULL`
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
/// \return An `AMbyteSpan` struct for an array of bytes.
/// \pre \p change `!= NULL`
/// \internal
///
/// # Safety
/// change must be a valid pointer to an AMchange
#[no_mangle]
pub unsafe extern "C" fn AMchangeRawBytes(change: *const AMchange) -> AMbyteSpan {
    if let Some(change) = change.as_ref() {
        change.as_ref().raw_bytes().into()
    } else {
        Default::default()
    }
}

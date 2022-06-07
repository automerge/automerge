use automerge as am;
use automerge::transaction::{CommitOptions, Transactable};
use smol_str::SmolStr;
use std::borrow::Cow;
use std::ops::{Deref, DerefMut};
use std::os::raw::c_char;

use crate::change::AMchange;
use crate::change_hashes::AMchangeHashes;
use crate::obj::AMobjId;
use crate::result::{to_result, AMresult};
use crate::sync::{to_sync_message, AMsyncMessage, AMsyncState};

mod list;
mod map;
mod utils;

use crate::changes::AMchanges;
use crate::doc::utils::to_str;
use crate::doc::utils::{to_doc, to_obj_id};

macro_rules! to_changes {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::err("Invalid AMchanges pointer").into(),
        }
    }};
}

macro_rules! to_sync_state_mut {
    ($handle:expr) => {{
        let handle = $handle.as_mut();
        match handle {
            Some(b) => b,
            None => return AMresult::err("Invalid AMsyncState pointer").into(),
        }
    }};
}

/// \struct AMdoc
/// \brief A JSON-like CRDT.
#[derive(Clone)]
pub struct AMdoc(am::AutoCommit);

impl AMdoc {
    pub fn new(body: am::AutoCommit) -> Self {
        Self(body)
    }
}

impl AsRef<am::AutoCommit> for AMdoc {
    fn as_ref(&self) -> &am::AutoCommit {
        &self.0
    }
}

impl Deref for AMdoc {
    type Target = am::AutoCommit;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AMdoc {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// \memberof AMdoc
/// \brief Applies a sequence of changes to a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \pre \p doc must be a valid address.
/// \pre \p changes must be a valid address.
/// \return A pointer to an `AMresult` struct containing a void.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// changes must be a pointer to a valid AMchanges.
#[no_mangle]
pub unsafe extern "C" fn AMapplyChanges(
    doc: *mut AMdoc,
    changes: *const AMchanges,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let changes = to_changes!(changes);
    to_result(doc.apply_changes(changes.as_ref().to_vec()))
}

/// \memberof AMdoc
/// \brief Allocates a new document and initializes it with defaults.
///
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMdoc` struct.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
#[no_mangle]
pub extern "C" fn AMcreate() -> *mut AMresult {
    to_result(am::AutoCommit::new())
}

/// \memberof AMdoc
/// \brief Commits the current operations on a document with an optional
///        message and/or time override as seconds since the epoch.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] message A UTF-8 string or `NULL`.
/// \param[in] time A pointer to a `time_t` value or `NULL`.
/// \return A pointer to an `AMresult` struct containing a change hash as an
///         `AMbyteSpan` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMcommit(
    doc: *mut AMdoc,
    message: *const c_char,
    time: *const libc::time_t,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let mut options = CommitOptions::default();
    if !message.is_null() {
        options.set_message(to_str(message));
    }
    if let Some(time) = time.as_ref() {
        options.set_time(*time);
    }
    to_result(doc.commit_with(options))
}

/// \memberof AMdoc
/// \brief Allocates storage for a document and initializes it by duplicating
///        the given document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMdoc` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMdup(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.as_ref().clone())
}

/// \memberof AMdoc
/// \brief Tests the equality of two documents after closing their respective
///        transactions.
///
/// \param[in] doc1 An `AMdoc` struct.
/// \param[in] doc2 An `AMdoc` struct.
/// \return `true` if \p doc1 `==` \p doc2 and `false` otherwise.
/// \pre \p doc1 must be a valid address.
/// \pre \p doc2 must be a valid address.
/// \internal
///
/// #Safety
/// doc1 must be a pointer to a valid AMdoc
/// doc2 must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMequal(doc1: *mut AMdoc, doc2: *mut AMdoc) -> bool {
    match (doc1.as_mut(), doc2.as_mut()) {
        (Some(doc1), Some(doc2)) => doc1.document().get_heads() == doc2.document().get_heads(),
        (None, Some(_)) | (Some(_), None) | (None, None) => false,
    }
}

/// \memberof AMdoc
/// \brief Generates a synchronization message for a peer based upon the given
///        synchronization state.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] sync_state A pointer to an `AMsyncState` struct.
/// \return A pointer to an `AMresult` struct containing either a pointer to an
///         `AMsyncMessage` struct or a void.
/// \pre \p doc must b e a valid address.
/// \pre \p sync_state must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// sync_state must be a pointer to a valid AMsyncState
#[no_mangle]
pub unsafe extern "C" fn AMgenerateSyncMessage(
    doc: *mut AMdoc,
    sync_state: *mut AMsyncState,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let sync_state = to_sync_state_mut!(sync_state);
    to_result(doc.generate_sync_message(sync_state.as_mut()))
}

/// \memberof AMdoc
/// \brief Gets an `AMdoc` struct's actor ID value as an array of bytes.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an actor ID as an
///         `AMbyteSpan` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetActor(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(Ok(doc.get_actor().clone()))
}

/// \memberof AMdoc
/// \brief Gets an `AMdoc` struct's actor ID value as a hexadecimal string.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing a `char const*`.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetActorHex(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc!(doc);
    let hex_str = doc.get_actor().to_hex_string();
    let value = am::Value::Scalar(Cow::Owned(am::ScalarValue::Str(SmolStr::new(hex_str))));
    to_result(Ok(value))
}

/// \memberof AMdoc
/// \brief Gets the changes added to a document by their respective hashes.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] have_deps A pointer to an `AMchangeHashes` struct or `NULL`.
/// \return A pointer to an `AMresult` struct containing an `AMchanges` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetChanges(
    doc: *mut AMdoc,
    have_deps: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let empty_deps = Vec::<am::ChangeHash>::new();
    let have_deps = match have_deps.as_ref() {
        Some(have_deps) => have_deps.as_ref(),
        None => &empty_deps,
    };
    to_result(doc.get_changes(have_deps))
}

/// \memberof AMdoc
/// \brief Gets the current heads of a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an `AMchangeHashes`
///         struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetHeads(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(Ok(doc.get_heads()))
}

/// \memberof AMdoc
/// \brief Gets the hashes of the changes in a document that aren't transitive
///        dependencies of the given hashes of changes.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] heads A pointer to an `AMchangeHashes` struct or `NULL`.
/// \return A pointer to an `AMresult` struct containing an `AMchangeHashes`
///         struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetMissingDeps(
    doc: *mut AMdoc,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let empty_heads = Vec::<am::ChangeHash>::new();
    let heads = match heads.as_ref() {
        Some(heads) => heads.as_ref(),
        None => &empty_heads,
    };
    to_result(doc.get_missing_deps(heads))
}

/// \memberof AMdoc
/// \brief Gets the last change made to a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing either an `AMchange`
///         struct or a void.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetLastLocalChange(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.get_last_local_change())
}

/// \memberof AMdoc
/// \brief Allocates storage for a document and initializes it with the compact
///        form of an incremental save.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes in \p src to load.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMdoc` struct.
/// \pre \p src must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p src.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// src must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMload(src: *const u8, count: usize) -> *mut AMresult {
    let mut data = Vec::new();
    data.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(am::AutoCommit::load(&data))
}

/// \memberof AMdoc
/// \brief Loads the compact form of an incremental save into a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes in \p src to load.
/// \return A pointer to an `AMresult` struct containing the number of
///         operations loaded from \p src.
/// \pre \p doc must be a valid address.
/// \pre \p src must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p src.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// src must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMloadIncremental(
    doc: *mut AMdoc,
    src: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let mut data = Vec::new();
    data.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(doc.load_incremental(&data))
}

/// \memberof AMdoc
/// \brief Applies a sequence of changes to a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] changes A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an `AMchangeHashes`
///         struct.
/// \pre \p dest must be a valid address.
/// \pre \p src must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// dest must be a pointer to a valid AMdoc
/// src must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMmerge(dest: *mut AMdoc, src: *mut AMdoc) -> *mut AMresult {
    let dest = to_doc!(dest);
    to_result(dest.merge(to_doc!(src)))
}

/// \memberof AMdoc
/// \brief Gets the size of an object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \return The count of values in the object identified by \p obj_id.
/// \pre \p doc must be a valid address.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
#[no_mangle]
pub unsafe extern "C" fn AMobjSize(doc: *const AMdoc, obj_id: *const AMobjId) -> usize {
    if let Some(doc) = doc.as_ref() {
        doc.length(to_obj_id!(obj_id))
    } else {
        0
    }
}

/// \memberof AMdoc
/// \brief Gets the historical size of an object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `NULL`.
/// \param[in] change A pointer to an `AMchange` struct or `NULL`.
/// \return The count of values in the object identified by \p obj_id at
///         \p change.
/// \pre \p doc must be a valid address.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// obj_id must be a pointer to a valid AMobjId or NULL
/// change must be a pointer to a valid AMchange or NULL
#[no_mangle]
pub unsafe extern "C" fn AMobjSizeAt(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    change: *const AMchange,
) -> usize {
    if let Some(doc) = doc.as_ref() {
        if let Some(change) = change.as_ref() {
            let change: &am::Change = change.as_ref();
            let change_hashes = vec![change.hash];
            return doc.length_at(to_obj_id!(obj_id), &change_hashes);
        }
    };
    0
}

/// \memberof AMdoc
/// \brief Receives a synchronization message from a peer based upon a given
///        synchronization state.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] sync_state A pointer to an `AMsyncState` struct.
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p sync_state must be a valid address.
/// \pre \p sync_message must be a valid address.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// sync_state must be a pointer to a valid AMsyncState
/// sync_message must be a pointer to a valid AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMreceiveSyncMessage(
    doc: *mut AMdoc,
    sync_state: *mut AMsyncState,
    sync_message: *const AMsyncMessage,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let sync_state = to_sync_state_mut!(sync_state);
    let sync_message = to_sync_message!(sync_message);
    to_result(doc.receive_sync_message(sync_state.as_mut(), sync_message.as_ref().clone()))
}

/// \memberof AMdoc
/// \brief Saves the entirety of a document into a compact form.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an array of bytes as
///         an `AMbyteSpan` struct.
/// \pre \p doc must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMsave(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(Ok(doc.save()))
}

/// \memberof AMdoc
/// \brief Puts a sequence of bytes as the actor ID value of a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] value A pointer to a contiguous sequence of bytes.
/// \param[in] count The number of bytes to copy from \p value.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p value must be a valid address.
/// \pre `0 <=` \p count `<=` length of \p value.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// value must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMsetActor(
    doc: *mut AMdoc,
    value: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let slice = std::slice::from_raw_parts(value, count);
    doc.set_actor(am::ActorId::from(slice));
    to_result(Ok(()))
}

/// \memberof AMdoc
/// \brief Puts a hexadecimal string as the actor ID value of a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] hex_str A string of hexadecimal characters.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc must be a valid address.
/// \pre \p hex_str must be a valid address.
/// \warning To avoid a memory leak, the returned `AMresult` struct must be
///          deallocated with `AMfree()`.
/// \internal
///
/// # Safety
/// doc must be a pointer to a valid AMdoc
/// hex_str must be a null-terminated array of `c_char`
#[no_mangle]
pub unsafe extern "C" fn AMsetActorHex(doc: *mut AMdoc, hex_str: *const c_char) -> *mut AMresult {
    let doc = to_doc!(doc);
    let slice = std::slice::from_raw_parts(hex_str as *const u8, libc::strlen(hex_str));
    to_result(match hex::decode(slice) {
        Ok(vec) => {
            doc.set_actor(vec.into());
            Ok(())
        }
        Err(error) => Err(am::AutomergeError::HexDecode(error)),
    })
}

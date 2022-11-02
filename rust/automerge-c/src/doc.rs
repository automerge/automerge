use automerge as am;
use automerge::transaction::{CommitOptions, Transactable};
use std::ops::{Deref, DerefMut};
use std::os::raw::c_char;

use crate::actor_id::AMactorId;
use crate::change_hashes::AMchangeHashes;
use crate::obj::{AMobjId, AMobjType};
use crate::result::{to_result, AMresult, AMvalue};
use crate::sync::{to_sync_message, AMsyncMessage, AMsyncState};

pub mod list;
pub mod map;
pub mod utils;

use crate::changes::AMchanges;
use crate::doc::utils::to_str;
use crate::doc::utils::{to_actor_id, to_doc, to_doc_mut, to_obj_id};

macro_rules! to_changes {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::err("Invalid AMchanges pointer").into(),
        }
    }};
}

macro_rules! to_index {
    ($index:expr, $len:expr, $param_name:expr) => {{
        if $index > $len && $index != usize::MAX {
            return AMresult::err(&format!("Invalid {} {}", $param_name, $index)).into();
        }
        std::cmp::min($index, $len)
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
/// \installed_headerfile
/// \brief A JSON-like CRDT.
#[derive(Clone)]
pub struct AMdoc(am::AutoCommit);

impl AMdoc {
    pub fn new(auto_commit: am::AutoCommit) -> Self {
        Self(auto_commit)
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
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] changes A pointer to an `AMchanges` struct.
/// \pre \p doc `!= NULL`.
/// \pre \p changes `!= NULL`.
/// \return A pointer to an `AMresult` struct containing a void.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// changes must be a valid pointer to an AMchanges.
#[no_mangle]
pub unsafe extern "C" fn AMapplyChanges(
    doc: *mut AMdoc,
    changes: *const AMchanges,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let changes = to_changes!(changes);
    to_result(doc.apply_changes(changes.as_ref().to_vec()))
}

/// \memberof AMdoc
/// \brief Allocates storage for a document and initializes it by duplicating
///        the given document.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMdoc` struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMclone(doc: *const AMdoc) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(doc.as_ref().clone())
}

/// \memberof AMdoc
/// \brief Allocates a new document and initializes it with defaults.
///
/// \param[in] actor_id A pointer to an `AMactorId` struct or `NULL` for a
///                     random one.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMdoc` struct.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
///
/// # Safety
/// actor_id must be a valid pointer to an AMactorId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMcreate(actor_id: *const AMactorId) -> *mut AMresult {
    to_result(match actor_id.as_ref() {
        Some(actor_id) => am::AutoCommit::new().with_actor(actor_id.as_ref().clone()),
        None => am::AutoCommit::new(),
    })
}

/// \memberof AMdoc
/// \brief Commits the current operations on a document with an optional
///        message and/or time override as seconds since the epoch.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] message A UTF-8 string or `NULL`.
/// \param[in] time A pointer to a `time_t` value or `NULL`.
/// \return A pointer to an `AMresult` struct containing an `AMchangeHashes`
///         with one element.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMcommit(
    doc: *mut AMdoc,
    message: *const c_char,
    time: *const libc::time_t,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
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
/// \brief Tests the equality of two documents after closing their respective
///        transactions.
///
/// \param[in,out] doc1 An `AMdoc` struct.
/// \param[in,out] doc2 An `AMdoc` struct.
/// \return `true` if \p doc1 `==` \p doc2 and `false` otherwise.
/// \pre \p doc1 `!= NULL`.
/// \pre \p doc2 `!= NULL`.
/// \internal
///
/// #Safety
/// doc1 must be a valid pointer to an AMdoc
/// doc2 must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMequal(doc1: *mut AMdoc, doc2: *mut AMdoc) -> bool {
    match (doc1.as_mut(), doc2.as_mut()) {
        (Some(doc1), Some(doc2)) => doc1.document().get_heads() == doc2.document().get_heads(),
        (None, Some(_)) | (Some(_), None) | (None, None) => false,
    }
}

/// \memberof AMdoc
/// \brief Forks this document at the current or a historical point for use by
///        a different actor.
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for a historical
///                  point or `NULL` for the current point.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMdoc` struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMfork(doc: *mut AMdoc, heads: *const AMchangeHashes) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    match heads.as_ref() {
        None => to_result(doc.fork()),
        Some(heads) => to_result(doc.fork_at(heads.as_ref())),
    }
}

/// \memberof AMdoc
/// \brief Generates a synchronization message for a peer based upon the given
///        synchronization state.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in,out] sync_state A pointer to an `AMsyncState` struct.
/// \return A pointer to an `AMresult` struct containing either a pointer to an
///         `AMsyncMessage` struct or a void.
/// \pre \p doc `!= NULL`.
/// \pre \p sync_state `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// sync_state must be a valid pointer to an AMsyncState
#[no_mangle]
pub unsafe extern "C" fn AMgenerateSyncMessage(
    doc: *mut AMdoc,
    sync_state: *mut AMsyncState,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let sync_state = to_sync_state_mut!(sync_state);
    to_result(doc.generate_sync_message(sync_state.as_mut()))
}

/// \memberof AMdoc
/// \brief Gets a document's actor identifier.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMactorId` struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetActorId(doc: *const AMdoc) -> *mut AMresult {
    let doc = to_doc!(doc);
    to_result(Ok::<am::ActorId, am::AutomergeError>(
        doc.get_actor().clone(),
    ))
}

/// \memberof AMdoc
/// \brief Gets the change added to a document by its respective hash.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes in \p src.
/// \return A pointer to an `AMresult` struct containing an `AMchanges` struct.
/// \pre \p doc `!= NULL`.
/// \pre \p src `!= NULL`.
/// \pre \p count `>= AM_CHANGE_HASH_SIZE`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// src must be a byte array of size `>= automerge::types::HASH_SIZE`
#[no_mangle]
pub unsafe extern "C" fn AMgetChangeByHash(
    doc: *mut AMdoc,
    src: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let slice = std::slice::from_raw_parts(src, count);
    match slice.try_into() {
        Ok(change_hash) => to_result(doc.get_change_by_hash(&change_hash)),
        Err(e) => AMresult::err(&e.to_string()).into(),
    }
}

/// \memberof AMdoc
/// \brief Gets the changes added to a document by their respective hashes.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] have_deps A pointer to an `AMchangeHashes` struct or `NULL`.
/// \return A pointer to an `AMresult` struct containing an `AMchanges` struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetChanges(
    doc: *mut AMdoc,
    have_deps: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let empty_deps = Vec::<am::ChangeHash>::new();
    let have_deps = match have_deps.as_ref() {
        Some(have_deps) => have_deps.as_ref(),
        None => &empty_deps,
    };
    to_result(doc.get_changes(have_deps))
}

/// \memberof AMdoc
/// \brief Gets the changes added to a second document that weren't added to
///        a first document.
///
/// \param[in,out] doc1 An `AMdoc` struct.
/// \param[in,out] doc2 An `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an `AMchanges` struct.
/// \pre \p doc1 `!= NULL`.
/// \pre \p doc2 `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc1 must be a valid pointer to an AMdoc
/// doc2 must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetChangesAdded(doc1: *mut AMdoc, doc2: *mut AMdoc) -> *mut AMresult {
    let doc1 = to_doc_mut!(doc1);
    let doc2 = to_doc_mut!(doc2);
    to_result(doc1.get_changes_added(doc2))
}

/// \memberof AMdoc
/// \brief Gets the current heads of a document.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an `AMchangeHashes`
///         struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetHeads(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    to_result(Ok::<Vec<am::ChangeHash>, am::AutomergeError>(
        doc.get_heads(),
    ))
}

/// \memberof AMdoc
/// \brief Gets the hashes of the changes in a document that aren't transitive
///        dependencies of the given hashes of changes.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] heads A pointer to an `AMchangeHashes` struct or `NULL`.
/// \return A pointer to an `AMresult` struct containing an `AMchangeHashes`
///         struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMgetMissingDeps(
    doc: *mut AMdoc,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
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
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing either an `AMchange`
///         struct or a void.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetLastLocalChange(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    to_result(doc.get_last_local_change())
}

/// \memberof AMdoc
/// \brief Gets the current or historical keys of a map object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for historical
///                  keys or `NULL` for current keys.
/// \return A pointer to an `AMresult` struct containing an `AMstrs` struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMkeys(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    match heads.as_ref() {
        None => to_result(doc.keys(obj_id)),
        Some(heads) => to_result(doc.keys_at(obj_id, heads.as_ref())),
    }
}

/// \memberof AMdoc
/// \brief Allocates storage for a document and initializes it with the compact
///        form of an incremental save.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes in \p src to load.
/// \return A pointer to an `AMresult` struct containing a pointer to an
///         `AMdoc` struct.
/// \pre \p src `!= NULL`.
/// \pre `0 <` \p count `<= sizeof(`\p src`)`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// src must be a byte array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMload(src: *const u8, count: usize) -> *mut AMresult {
    let mut data = Vec::new();
    data.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(am::AutoCommit::load(&data))
}

/// \memberof AMdoc
/// \brief Loads the compact form of an incremental save into a document.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes in \p src to load.
/// \return A pointer to an `AMresult` struct containing the number of
///         operations loaded from \p src.
/// \pre \p doc `!= NULL`.
/// \pre \p src `!= NULL`.
/// \pre `0 <` \p count `<= sizeof(`\p src`)`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// src must be a byte array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMloadIncremental(
    doc: *mut AMdoc,
    src: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let mut data = Vec::new();
    data.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(doc.load_incremental(&data))
}

/// \memberof AMdoc
/// \brief Applies all of the changes in \p src which are not in \p dest to
///        \p dest.
///
/// \param[in,out] dest A pointer to an `AMdoc` struct.
/// \param[in,out] src A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an `AMchangeHashes`
///         struct.
/// \pre \p dest `!= NULL`.
/// \pre \p src `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// dest must be a valid pointer to an AMdoc
/// src must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMmerge(dest: *mut AMdoc, src: *mut AMdoc) -> *mut AMresult {
    let dest = to_doc_mut!(dest);
    to_result(dest.merge(to_doc_mut!(src)))
}

/// \memberof AMdoc
/// \brief Gets the current or historical size of an object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for historical
///            size or `NULL` for current size.
/// \return A 64-bit unsigned integer.
/// \pre \p doc `!= NULL`.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMobjSize(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMchangeHashes,
) -> usize {
    if let Some(doc) = doc.as_ref() {
        let obj_id = to_obj_id!(obj_id);
        match heads.as_ref() {
            None => doc.length(obj_id),
            Some(heads) => doc.length_at(obj_id, heads.as_ref()),
        }
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn AMobjIdObjType(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMchangeHashes,
) -> AMobjType {
    if let Some(doc) = doc.as_ref() {
        let obj_id = to_obj_id!(obj_id);
        let obj_type = match heads.as_ref() {
            None => doc.object_type(obj_id),
            Some(_) => todo!(),
        };
        match obj_type {
            Some(am::ObjType::Map) => AMobjType::Map,
            Some(am::ObjType::List) => AMobjType::List,
            Some(am::ObjType::Text) => AMobjType::Text,
            Some(am::ObjType::Table) => AMobjType::Table,
            None => AMobjType::Invalid,
        }
    } else {
        AMobjType::Invalid
    }
}

/// \memberof AMdoc
/// \brief Gets the current or historical values of an object within its entire
///        range.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for historical
///                  items or `NULL` for current items.
/// \return A pointer to an `AMresult` struct containing an `AMobjItems` struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMobjValues(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    match heads.as_ref() {
        None => to_result(doc.values(obj_id)),
        Some(heads) => to_result(doc.values_at(obj_id, heads.as_ref())),
    }
}

/// \memberof AMdoc
/// \brief Gets the number of pending operations added during a document's
///        current transaction.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return The count of pending operations for \p doc.
/// \pre \p doc `!= NULL`.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMpendingOps(doc: *const AMdoc) -> usize {
    if let Some(doc) = doc.as_ref() {
        doc.pending_ops()
    } else {
        0
    }
}

/// \memberof AMdoc
/// \brief Receives a synchronization message from a peer based upon a given
///        synchronization state.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in,out] sync_state A pointer to an `AMsyncState` struct.
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p sync_state `!= NULL`.
/// \pre \p sync_message `!= NULL`.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// sync_state must be a valid pointer to an AMsyncState
/// sync_message must be a valid pointer to an AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMreceiveSyncMessage(
    doc: *mut AMdoc,
    sync_state: *mut AMsyncState,
    sync_message: *const AMsyncMessage,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let sync_state = to_sync_state_mut!(sync_state);
    let sync_message = to_sync_message!(sync_message);
    to_result(doc.receive_sync_message(sync_state.as_mut(), sync_message.as_ref().clone()))
}

/// \memberof AMdoc
/// \brief Cancels the pending operations added during a document's current
///        transaction and gets the number of cancellations.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \return The count of pending operations for \p doc that were cancelled.
/// \pre \p doc `!= NULL`.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMrollback(doc: *mut AMdoc) -> usize {
    if let Some(doc) = doc.as_mut() {
        doc.rollback()
    } else {
        0
    }
}

/// \memberof AMdoc
/// \brief Saves the entirety of a document into a compact form.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an array of bytes as
///         an `AMbyteSpan` struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMsave(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    to_result(Ok(doc.save()))
}

/// \memberof AMdoc
/// \brief Saves the changes to a document since its last save into a compact
///        form.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing an array of bytes as
///         an `AMbyteSpan` struct.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMsaveIncremental(doc: *mut AMdoc) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    to_result(Ok(doc.save_incremental()))
}

/// \memberof AMdoc
/// \brief Puts the actor identifier of a document.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] actor_id A pointer to an `AMactorId` struct.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre \p actor_id `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// actor_id must be a valid pointer to an AMactorId
#[no_mangle]
pub unsafe extern "C" fn AMsetActorId(
    doc: *mut AMdoc,
    actor_id: *const AMactorId,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let actor_id = to_actor_id!(actor_id);
    doc.set_actor(actor_id.as_ref().clone());
    to_result(Ok(()))
}

/// \memberof AMdoc
/// \brief Splices values into and/or removes values from the identified object
///        at a given position within it.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos A position in the object identified by \p obj_id or
///                `SIZE_MAX` to indicate one past its end.
/// \param[in] del The number of characters to delete or `SIZE_MAX` to indicate
///                all of them.
/// \param[in] src A pointer to an array of `AMvalue` structs.
/// \param[in] count The number of `AMvalue` structs in \p src to load.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id`)` or \p pos `== SIZE_MAX`.
/// \pre `0 <=` \p del `<= AMobjSize(`\p obj_id`)` or \p del `== SIZE_MAX`.
/// \pre `(`\p src `!= NULL and 1 <=` \p count `<= sizeof(`\p src`)/
///      sizeof(AMvalue)) or `\p src `== NULL or `\p count `== 0`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// src must be an AMvalue array of size `>= count` or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMsplice(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    del: usize,
    src: *const AMvalue,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let len = doc.length(obj_id);
    let pos = to_index!(pos, len, "pos");
    let del = to_index!(del, len, "del");
    let mut vals: Vec<am::ScalarValue> = vec![];
    if !(src.is_null() || count == 0) {
        let c_vals = std::slice::from_raw_parts(src, count);
        for c_val in c_vals {
            match c_val.try_into() {
                Ok(s) => {
                    vals.push(s);
                }
                Err(e) => {
                    return AMresult::err(&e.to_string()).into();
                }
            }
        }
    }
    to_result(doc.splice(obj_id, pos, del, vals))
}

/// \memberof AMdoc
/// \brief Splices characters into and/or removes characters from the
///        identified object at a given position within it.
///
/// \param[in,out] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos A position in the text object identified by \p obj_id or
///                `SIZE_MAX` to indicate one past its end.
/// \param[in] del The number of characters to delete or `SIZE_MAX` to indicate
///                all of them.
/// \param[in] text A UTF-8 string.
/// \return A pointer to an `AMresult` struct containing a void.
/// \pre \p doc `!= NULL`.
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id`)` or \p pos `== SIZE_MAX`.
/// \pre `0 <=` \p del `<= AMobjSize(`\p obj_id`)` or \p del `== SIZE_MAX`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// text must be a null-terminated array of `c_char` or NULL.
#[no_mangle]
pub unsafe extern "C" fn AMspliceText(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    del: usize,
    text: *const c_char,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let len = doc.length(obj_id);
    let pos = to_index!(pos, len, "pos");
    let del = to_index!(del, len, "del");
    to_result(doc.splice_text(obj_id, pos, del, &to_str(text)))
}

/// \memberof AMdoc
/// \brief Gets the current or historical string represented by a text object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] heads A pointer to an `AMchangeHashes` struct for historical
///                  keys or `NULL` for current keys.
/// \return A pointer to an `AMresult` struct containing a UTF-8 string.
/// \pre \p doc `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMchangeHashes or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMtext(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMchangeHashes,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    match heads.as_ref() {
        None => to_result(doc.text(obj_id)),
        Some(heads) => to_result(doc.text_at(obj_id, heads.as_ref())),
    }
}

use automerge as am;
use automerge::sync::SyncDoc;
use automerge::transaction::{CommitOptions, Transactable};
use automerge::ReadDoc;
use std::ops::{Deref, DerefMut};

use crate::actor_id::{to_actor_id, AMactorId};
use crate::byte_span::{to_str, AMbyteSpan};
use crate::cursor::{to_cursor, AMcursor};
use crate::items::AMitems;
use crate::obj::{to_obj_id, AMobjId, AMobjType};
use crate::result::{to_result, AMresult};
use crate::sync::{to_sync_message, AMsyncMessage, AMsyncState};

pub mod list;
pub mod map;
pub mod mark;
pub mod utils;

use crate::doc::utils::{clamp, to_doc, to_doc_mut, to_items};

macro_rules! to_sync_state_mut {
    ($handle:expr) => {{
        let handle = $handle.as_mut();
        match handle {
            Some(b) => b,
            None => return AMresult::error("Invalid `AMsyncState*`").into(),
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

    fn is_equal_to(&mut self, other: &mut Self) -> bool {
        self.document().get_heads() == other.document().get_heads()
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
/// \param[in] items A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE`
///                  items.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p items `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// items must be a valid pointer to an AMitems.
#[no_mangle]
pub unsafe extern "C" fn AMapplyChanges(doc: *mut AMdoc, items: *const AMitems) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let items = to_items!(items);
    match Vec::<am::Change>::try_from(items) {
        Ok(changes) => to_result(doc.apply_changes(changes)),
        Err(e) => AMresult::error(&e.to_string()).into(),
    }
}

/// \memberof AMdoc
/// \brief Allocates storage for a document and initializes it by duplicating
///        the given document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_DOC` item.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_DOC` item.
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
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
///        message and/or *nix timestamp (milliseconds).
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] message A UTF-8 string view as an `AMbyteSpan` struct.
/// \param[in] timestamp A pointer to a 64-bit integer or `NULL`.
/// \return A pointer to an `AMresult` struct with one `AM_VAL_TYPE_CHANGE_HASH`
///         item if there were operations to commit or an `AM_VAL_TYPE_VOID` item
///         if there were no operations to commit.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMcommit(
    doc: *mut AMdoc,
    message: AMbyteSpan,
    timestamp: *const i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let mut options = CommitOptions::default();
    if !message.is_null() {
        options.set_message(to_str!(message));
    }
    if let Some(timestamp) = timestamp.as_ref() {
        options.set_time(*timestamp);
    }
    to_result(doc.commit_with(options))
}

/// \memberof AMdoc
/// \brief Creates an empty change with an optional message and/or *nix
///        timestamp (milliseconds).
///
/// \details This is useful if you wish to create a "merge commit" which has as
///          its dependents the current heads of the document but you don't have
///          any operations to add to the document.
///
/// \note If there are outstanding uncommitted changes to the document
///       then two changes will be created: one for creating the outstanding
///       changes and one for the empty change. The empty change will always be
///       the latest change in the document after this call and the returned
///       hash will be the hash of that empty change.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] message A UTF-8 string view as an `AMbyteSpan` struct.
/// \param[in] timestamp A pointer to a 64-bit integer or `NULL`.
/// \return A pointer to an `AMresult` struct with one `AM_VAL_TYPE_CHANGE_HASH`
///         item.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMemptyChange(
    doc: *mut AMdoc,
    message: AMbyteSpan,
    timestamp: *const i64,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let mut options = CommitOptions::default();
    if !message.is_null() {
        options.set_message(to_str!(message));
    }
    if let Some(timestamp) = timestamp.as_ref() {
        options.set_time(*timestamp);
    }
    to_result(doc.empty_change(options))
}

/// \memberof AMdoc
/// \brief Tests the equality of two documents after closing their respective
///        transactions.
///
/// \param[in] doc1 A pointer to an `AMdoc` struct.
/// \param[in] doc2 A pointer to an `AMdoc` struct.
/// \return `true` if \p doc1 `==` \p doc2 and `false` otherwise.
/// \pre \p doc1 `!= NULL`
/// \pre \p doc2 `!= NULL`
/// \internal
///
/// #Safety
/// doc1 must be a valid pointer to an AMdoc
/// doc2 must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMequal(doc1: *mut AMdoc, doc2: *mut AMdoc) -> bool {
    match (doc1.as_mut(), doc2.as_mut()) {
        (Some(doc1), Some(doc2)) => doc1.is_equal_to(doc2),
        (None, None) | (None, Some(_)) | (Some(_), None) => false,
    }
}

/// \memberof AMdoc
/// \brief Forks this document at its current or a historical point for use by
///        a different actor.
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select a historical point or `NULL` to select its
///                  current point.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMfork(doc: *mut AMdoc, heads: *const AMitems) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    match heads.as_ref() {
        None => to_result(doc.fork()),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result(doc.fork_at(&heads)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Generates a synchronization message for a peer based upon the given
///        synchronization state.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] sync_state A pointer to an `AMsyncState` struct.
/// \return A pointer to an `AMresult` struct with either an
///         `AM_VAL_TYPE_SYNC_MESSAGE` or `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p sync_state `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
    to_result(doc.sync().generate_sync_message(sync_state.as_mut()))
}

/// \memberof AMdoc
/// \brief Gets a document's actor identifier.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_ACTOR_ID` item.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The count of bytes to copy from the array pointed to by
///                  \p src.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_CHANGE` item.
/// \pre \p doc `!= NULL`
/// \pre \p src `!= NULL`
/// \pre `sizeof(`\p src') >= AM_CHANGE_HASH_SIZE`
/// \pre \p count `<= sizeof(`\p src `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// src must be a byte array of length `>= automerge::types::HASH_SIZE`
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
        Err(e) => AMresult::error(&e.to_string()).into(),
    }
}

/// \memberof AMdoc
/// \brief Gets the changes added to a document by their respective hashes.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] have_deps A pointer to an `AMitems` struct with
///                      `AM_VAL_TYPE_CHANGE_HASH` items or `NULL`.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE` items.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMgetChanges(doc: *mut AMdoc, have_deps: *const AMitems) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let have_deps = match have_deps.as_ref() {
        Some(have_deps) => match Vec::<am::ChangeHash>::try_from(have_deps) {
            Ok(change_hashes) => change_hashes,
            Err(e) => return AMresult::error(&e.to_string()).into(),
        },
        None => Vec::<am::ChangeHash>::new(),
    };
    to_result(doc.get_changes(&have_deps))
}

/// \memberof AMdoc
/// \brief Gets the changes added to a second document that weren't added to
///        a first document.
///
/// \param[in] doc1 A pointer to an `AMdoc` struct.
/// \param[in] doc2 A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE` items.
/// \pre \p doc1 `!= NULL`
/// \pre \p doc2 `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \brief Gets an `AMcursor` i.e. a stable address for a position within a list
///        object or text object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] position The absolute position of the cursor.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select a historical object or `NULL` to select the
///                  current object.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_CURSOR` item.
/// \pre \p doc `!= NULL`
/// \pre \p position `< AMobjSize(`\p doc, \p obj_id, \p heads `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMgetCursor(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    position: usize,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    match heads.as_ref() {
        None => to_result(doc.get_cursor(obj_id, position, None)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result(doc.get_cursor(obj_id, position, Some(heads.as_slice()))),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Gets the absolute position of an `AMcursor` within a list object or
///        text object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] cursor A pointer to an `AMcursor` struct.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select a historical object or `NULL` to select the
///                  current object.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_UINT` item.
///         For an `AM_OBJ_TYPE_TEXT` object, if `AUTOMERGE_C_UTF8` is defined
///         then the item's value is in bytes but if `AUTOMERGE_C_UTF32` is
///         defined then the item's value is in Unicode code points.
/// \pre \p doc `!= NULL`
/// \pre \p cursor `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// cursor must be a valid pointer to an AMcursor
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMgetCursorPosition(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    cursor: *const AMcursor,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    let cursor = to_cursor!(cursor);
    match heads.as_ref() {
        None => to_result(doc.get_cursor_position(obj_id, cursor.as_ref(), None)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => {
                to_result(doc.get_cursor_position(obj_id, cursor.as_ref(), Some(heads.as_slice())))
            }
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Gets the current heads of a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE_HASH` items.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items or `NULL`.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE_HASH` items.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMgetMissingDeps(doc: *mut AMdoc, heads: *const AMitems) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let heads = match heads.as_ref() {
        None => Vec::<am::ChangeHash>::new(),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => heads,
            Err(e) => {
                return AMresult::error(&e.to_string()).into();
            }
        },
    };
    to_result(doc.get_missing_deps(heads.as_slice()))
}

/// \memberof AMdoc
/// \brief Gets the last change made to a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct containing either an
///         `AM_VAL_TYPE_CHANGE` or `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select historical keys or `NULL` to select current
///                  keys.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_STR` items.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMkeys(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    match heads.as_ref() {
        None => to_result(doc.keys(obj_id)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result(doc.keys_at(obj_id, &heads)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Allocates storage for a document and initializes it with the compact
///        form of an incremental save.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The count of bytes to load from the array pointed to by
///                  \p src.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_DOC` item.
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
pub unsafe extern "C" fn AMload(src: *const u8, count: usize) -> *mut AMresult {
    let data = std::slice::from_raw_parts(src, count);
    to_result(am::AutoCommit::load(data))
}

/// \memberof AMdoc
/// \brief Loads the compact form of an incremental save into a document.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The count of bytes to load from the array pointed to by
///                  \p src.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_UINT` item.
/// \pre \p doc `!= NULL`
/// \pre \p src `!= NULL`
/// \pre `sizeof(`\p src `) > 0`
/// \pre \p count `<= sizeof(`\p src `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// src must be a byte array of length `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMloadIncremental(
    doc: *mut AMdoc,
    src: *const u8,
    count: usize,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let data = std::slice::from_raw_parts(src, count);
    to_result(doc.load_incremental(data))
}

/// \memberof AMdoc
/// \brief Applies all of the changes in \p src which are not in \p dest to
///        \p dest.
///
/// \param[in] dest A pointer to an `AMdoc` struct.
/// \param[in] src A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE_HASH` items.
/// \pre \p dest `!= NULL`
/// \pre \p src `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select a historical size or `NULL` to select its
///                  current size.
/// \return The count of items in the object identified by \p obj_id.
///         For an `AM_OBJ_TYPE_TEXT` object, if `AUTOMERGE_C_UTF8` is defined
///         then the items are bytes but if `AUTOMERGE_C_UTF32` is defined then
///         the items are Unicode code points.
/// \pre \p doc `!= NULL`
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMobjSize(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMitems,
) -> usize {
    if let Some(doc) = doc.as_ref() {
        let obj_id = to_obj_id!(obj_id);
        match heads.as_ref() {
            None => {
                return doc.length(obj_id);
            }
            Some(heads) => {
                if let Ok(heads) = <Vec<am::ChangeHash>>::try_from(heads) {
                    return doc.length_at(obj_id, &heads);
                }
            }
        }
    }
    0
}

/// \memberof AMdoc
/// \brief Gets the type of an object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \return An `AMobjType` tag or `0`.
/// \pre \p doc `!= NULL`
/// \pre \p obj_id `!= NULL`
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMobjObjType(doc: *const AMdoc, obj_id: *const AMobjId) -> AMobjType {
    if let Some(doc) = doc.as_ref() {
        let obj_id = to_obj_id!(obj_id);
        if let Ok(obj_type) = doc.object_type(obj_id) {
            return (&obj_type).into();
        }
    }
    Default::default()
}

/// \memberof AMdoc
/// \brief Gets the current or historical items of an entire object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] heads A pointer to an `AMitems` struct with `AM_VAL_TYPE_CHANGE_HASH`
///                  items to select its historical items or `NULL` to select
///                  its current items.
/// \return A pointer to an `AMresult` struct with an `AMitems` struct.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMobjItems(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    match heads.as_ref() {
        None => to_result(doc.values(obj_id)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result(doc.values_at(obj_id, &heads)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

/// \memberof AMdoc
/// \brief Gets the number of pending operations added during a document's
///        current transaction.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return The count of pending operations for \p doc.
/// \pre \p doc `!= NULL`
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMpendingOps(doc: *const AMdoc) -> usize {
    if let Some(doc) = doc.as_ref() {
        return doc.pending_ops();
    }
    0
}

/// \memberof AMdoc
/// \brief Receives a synchronization message from a peer based upon a given
///        synchronization state.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] sync_state A pointer to an `AMsyncState` struct.
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p sync_state `!= NULL`
/// \pre \p sync_message `!= NULL`
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
    to_result(
        doc.sync()
            .receive_sync_message(sync_state.as_mut(), sync_message.as_ref().clone()),
    )
}

/// \memberof AMdoc
/// \brief Cancels the pending operations added during a document's current
///        transaction and gets the number of cancellations.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return The count of pending operations for \p doc that were cancelled.
/// \pre \p doc `!= NULL`
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
#[no_mangle]
pub unsafe extern "C" fn AMrollback(doc: *mut AMdoc) -> usize {
    if let Some(doc) = doc.as_mut() {
        return doc.rollback();
    }
    0
}

/// \memberof AMdoc
/// \brief Saves the entirety of a document into a compact form.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_BYTES` item.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_BYTES` item.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] actor_id A pointer to an `AMactorId` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre \p actor_id `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos A position in the object identified by \p obj_id or
///                `SIZE_MAX` to indicate one past its end.
/// \param[in] del The number of values to delete. If \p del `> 0` then
///                deletion begins at \p pos but if \p del `< 0` then deletion
///                ends at \p pos.
/// \param[in] values A copy of an `AMitems` struct from which values will be
///                   spliced <b>starting at its current position</b>; call
///                   `AMitemsRewound()` on a used `AMitems` first to ensure
///                   that all of its values are spliced in. Pass `(AMitems){0}`
///                   when zero values should be spliced in.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \pre `-AMobjSize(`\p obj_id `) <=` \p del `<= AMobjSize(`\p obj_id `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// values must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMsplice(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    del: isize,
    values: AMitems,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let len = doc.length(obj_id);
    let pos = clamp!(pos, len, "pos");
    match Vec::<am::ScalarValue>::try_from(&values) {
        Ok(vals) => to_result(doc.splice(obj_id, pos, del, vals)),
        Err(e) => AMresult::error(&e.to_string()).into(),
    }
}

/// \memberof AMdoc
/// \brief Splices characters into and/or removes characters from the
///        identified object at a given position within it.
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] pos A position in the text object identified by \p obj_id or
///                `SIZE_MAX` to indicate one past its end.
///                If `AUTOMERGE_C_UTF8` is defined then \p pos is in units of
///                bytes but if `AUTOMERGE_C_UTF32` is defined then \p pos is in
///                units of Unicode code points.
/// \param[in] del The number of characters to delete. If \p del `> 0` then
///                deletion begins at \p pos but if \p del `< 0` then deletion
///                ends at \p pos.
///                If `AUTOMERGE_C_UTF8` is defined then \p del is in units of
///                bytes but if `AUTOMERGE_C_UTF32` is defined then \p del is in
///                units of Unicode code points.
/// \param[in] text A UTF-8 string view as an `AMbyteSpan` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_VOID` item.
/// \pre \p doc `!= NULL`
/// \pre `0 <=` \p pos `<= AMobjSize(`\p obj_id `)` or \p pos `== SIZE_MAX`
/// \pre `-AMobjSize(`\p obj_id `) <=` \p del `<= AMobjSize(`\p obj_id `)`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMspliceText(
    doc: *mut AMdoc,
    obj_id: *const AMobjId,
    pos: usize,
    del: isize,
    text: AMbyteSpan,
) -> *mut AMresult {
    let doc = to_doc_mut!(doc);
    let obj_id = to_obj_id!(obj_id);
    let len = doc.length(obj_id);
    let pos = clamp!(pos, len, "pos");
    to_result(doc.splice_text(obj_id, pos, del, to_str!(text)))
}

/// \memberof AMdoc
/// \brief Gets the current or historical string represented by a text object.
///
/// \param[in] doc A pointer to an `AMdoc` struct.
/// \param[in] obj_id A pointer to an `AMobjId` struct or `AM_ROOT`.
/// \param[in] heads A pointer to an `AMitems` struct containing
///                  `AM_VAL_TYPE_CHANGE_HASH` items to select a historical string
///                  or `NULL` to select the current string.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_STR` item.
/// \pre \p doc `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// doc must be a valid pointer to an AMdoc
/// obj_id must be a valid pointer to an AMobjId or std::ptr::null()
/// heads must be a valid pointer to an AMitems or std::ptr::null()
#[no_mangle]
pub unsafe extern "C" fn AMtext(
    doc: *const AMdoc,
    obj_id: *const AMobjId,
    heads: *const AMitems,
) -> *mut AMresult {
    let doc = to_doc!(doc);
    let obj_id = to_obj_id!(obj_id);
    match heads.as_ref() {
        None => to_result(doc.text(obj_id)),
        Some(heads) => match <Vec<am::ChangeHash>>::try_from(heads) {
            Ok(heads) => to_result(doc.text_at(obj_id, &heads)),
            Err(e) => AMresult::error(&e.to_string()).into(),
        },
    }
}

use automerge as am;
use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::change::AMchange;
use crate::change_hashes::AMchangeHashes;
use crate::changes::AMchanges;
use crate::result::{to_result, AMresult};
use crate::sync::have::AMsyncHave;
use crate::sync::haves::AMsyncHaves;

macro_rules! to_sync_message {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::err("Invalid AMsyncMessage pointer").into(),
        }
    }};
}

pub(crate) use to_sync_message;

/// \struct AMsyncMessage
/// \installed_headerfile
/// \brief A synchronization message for a peer.
#[derive(PartialEq)]
pub struct AMsyncMessage {
    body: am::sync::Message,
    changes_storage: RefCell<BTreeMap<usize, AMchange>>,
    haves_storage: RefCell<BTreeMap<usize, AMsyncHave>>,
}

impl AMsyncMessage {
    pub fn new(message: am::sync::Message) -> Self {
        Self {
            body: message,
            changes_storage: RefCell::new(BTreeMap::new()),
            haves_storage: RefCell::new(BTreeMap::new()),
        }
    }
}

impl AsRef<am::sync::Message> for AMsyncMessage {
    fn as_ref(&self) -> &am::sync::Message {
        &self.body
    }
}

/// \memberof AMsyncMessage
/// \brief Gets the changes for the recipient to apply.
///
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return An `AMchanges` struct.
/// \pre \p sync_message `!= NULL`.
/// \internal
///
/// # Safety
/// sync_message must be a valid pointer to an AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMsyncMessageChanges(sync_message: *const AMsyncMessage) -> AMchanges {
    if let Some(sync_message) = sync_message.as_ref() {
        AMchanges::new(
            &sync_message.body.changes,
            &mut sync_message.changes_storage.borrow_mut(),
        )
    } else {
        AMchanges::default()
    }
}

/// \memberof AMsyncMessage
/// \brief Decodes a sequence of bytes into a synchronization message.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The number of bytes in \p src to decode.
/// \return A pointer to an `AMresult` struct containing an `AMsyncMessage`
///         struct.
/// \pre \p src `!= NULL`.
/// \pre `0 <` \p count `<= sizeof(`\p src`)`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// src must be a byte array of size `>= count`
#[no_mangle]
pub unsafe extern "C" fn AMsyncMessageDecode(src: *const u8, count: usize) -> *mut AMresult {
    let mut data = Vec::new();
    data.extend_from_slice(std::slice::from_raw_parts(src, count));
    to_result(am::sync::Message::decode(&data))
}

/// \memberof AMsyncMessage
/// \brief Encodes a synchronization message as a sequence of bytes.
///
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return A pointer to an `AMresult` struct containing an array of bytes as
///         an `AMbyteSpan` struct.
/// \pre \p sync_message `!= NULL`.
/// \warning The returned `AMresult` struct must be deallocated with `AMfree()`
///          in order to prevent a memory leak.
/// \internal
/// # Safety
/// sync_message must be a valid pointer to an AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMsyncMessageEncode(sync_message: *const AMsyncMessage) -> *mut AMresult {
    let sync_message = to_sync_message!(sync_message);
    to_result(sync_message.as_ref().clone().encode())
}

/// \memberof AMsyncMessage
/// \brief Gets a summary of the changes that the sender already has.
///
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return An `AMhaves` struct.
/// \pre \p sync_message `!= NULL`.
/// \internal
///
/// # Safety
/// sync_message must be a valid pointer to an AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMsyncMessageHaves(sync_message: *const AMsyncMessage) -> AMsyncHaves {
    if let Some(sync_message) = sync_message.as_ref() {
        AMsyncHaves::new(
            &sync_message.as_ref().have,
            &mut sync_message.haves_storage.borrow_mut(),
        )
    } else {
        AMsyncHaves::default()
    }
}

/// \memberof AMsyncMessage
/// \brief Gets the heads of the sender.
///
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return An `AMchangeHashes` struct.
/// \pre \p sync_message `!= NULL`.
/// \internal
///
/// # Safety
/// sync_message must be a valid pointer to an AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMsyncMessageHeads(sync_message: *const AMsyncMessage) -> AMchangeHashes {
    if let Some(sync_message) = sync_message.as_ref() {
        AMchangeHashes::new(&sync_message.as_ref().heads)
    } else {
        AMchangeHashes::default()
    }
}

/// \memberof AMsyncMessage
/// \brief Gets the hashes of any changes that are being explicitly requested
///        by the recipient.
///
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return An `AMchangeHashes` struct.
/// \pre \p sync_message `!= NULL`.
/// \internal
///
/// # Safety
/// sync_message must be a valid pointer to an AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMsyncMessageNeeds(sync_message: *const AMsyncMessage) -> AMchangeHashes {
    if let Some(sync_message) = sync_message.as_ref() {
        AMchangeHashes::new(&sync_message.as_ref().need)
    } else {
        AMchangeHashes::default()
    }
}

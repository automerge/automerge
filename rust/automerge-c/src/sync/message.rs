use automerge as am;
use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::change::AMchange;
use crate::result::{to_result, AMresult};
use crate::sync::have::AMsyncHave;

macro_rules! to_sync_message {
    ($handle:expr) => {{
        let handle = $handle.as_ref();
        match handle {
            Some(b) => b,
            None => return AMresult::error("Invalid `AMsyncMessage*`").into(),
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
/// \brief Decodes an array of bytes into a synchronization message.
///
/// \param[in] src A pointer to an array of bytes.
/// \param[in] count The count of bytes to decode from the array pointed to by
///                  \p src.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_SYNC_MESSAGE` item.
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
pub unsafe extern "C" fn AMsyncMessageDecode(src: *const u8, count: usize) -> *mut AMresult {
    let data = std::slice::from_raw_parts(src, count);
    to_result(am::sync::Message::decode(data))
}

/// \memberof AMsyncMessage
/// \brief Encodes a synchronization message as an array of bytes.
///
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return A pointer to an `AMresult` struct with an `AM_VAL_TYPE_BYTES` item.
/// \pre \p sync_message `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
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
/// \return A pointer to an `AMresult` struct with `AM_SYNC_HAVE` items.
/// \pre \p sync_message `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// sync_message must be a valid pointer to an AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMsyncMessageHaves(sync_message: *const AMsyncMessage) -> *mut AMresult {
    to_result(match sync_message.as_ref() {
        Some(sync_message) => sync_message.as_ref().have.as_slice(),
        None => Default::default(),
    })
}

/// \memberof AMsyncMessage
/// \brief Gets the heads of the sender.
///
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE_HASH` items.
/// \pre \p sync_message `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// sync_message must be a valid pointer to an AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMsyncMessageHeads(sync_message: *const AMsyncMessage) -> *mut AMresult {
    to_result(match sync_message.as_ref() {
        Some(sync_message) => sync_message.as_ref().heads.as_slice(),
        None => Default::default(),
    })
}

/// \memberof AMsyncMessage
/// \brief Gets the hashes of any changes that are being explicitly requested
///        by the recipient.
///
/// \param[in] sync_message A pointer to an `AMsyncMessage` struct.
/// \return A pointer to an `AMresult` struct with `AM_VAL_TYPE_CHANGE_HASH` items.
/// \pre \p sync_message `!= NULL`
/// \warning The returned `AMresult` struct pointer must be passed to
///          `AMresultFree()` in order to avoid a memory leak.
/// \internal
///
/// # Safety
/// sync_message must be a valid pointer to an AMsyncMessage
#[no_mangle]
pub unsafe extern "C" fn AMsyncMessageNeeds(sync_message: *const AMsyncMessage) -> *mut AMresult {
    to_result(match sync_message.as_ref() {
        Some(sync_message) => sync_message.as_ref().need.as_slice(),
        None => Default::default(),
    })
}

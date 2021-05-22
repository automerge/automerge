extern crate automerge_backend;
extern crate errno;
extern crate libc;
extern crate serde;

use core::fmt::Debug;
use std::{
    convert::TryInto,
    ffi::{CStr, CString},
    ops::{Deref, DerefMut},
    os::raw::c_char,
    ptr,
};

use automerge_backend::{AutomergeError, AutomergeErrorDiscriminants, Change};
use automerge_protocol::{ChangeHash, UncompressedChange};
use errno::{set_errno, Errno};
use serde::ser::Serialize;
use thiserror::Error;

/// All possible errors that a C caller could face
#[derive(Error, Debug)]
pub enum CError {
    // TODO: The `NullBackend` error is not attached to anything
    // (since normally we attach errors to a specific backend)
    // We could solve this by using a technique like this:
    // https://michael-f-bryan.github.io/rust-ffi-guide/errors/return_types.html
    // to create a `get_last_error_message` function, but the benefit seems very low
    // b/c the NullBackend error message is always the same
    #[error("Invalid pointer to backend")]
    NullBackend,
    #[error(transparent)]
    Json(#[from] serde_json::error::Error),
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    Automerge(#[from] AutomergeError),
}

impl CError {
    fn error_code(&self) -> isize {
        // 0 is reserved for "success"
        // -1 is reserved for "we had an error & we could't convert it to a CString"
        const BASE: isize = 2;
        let code = match self {
            CError::NullBackend => BASE,
            CError::Json(_) => BASE + 1,
            CError::FromUtf8(_) => BASE + 2,
            CError::Automerge(e) => {
                let kind = AutomergeErrorDiscriminants::from(e);
                (BASE + 3) + (kind as isize)
            }
        };
        -code
    }
}

#[derive(Clone)]
pub struct Backend {
    handle: automerge_backend::Backend,
    text: Option<String>,
    last_local_change: Option<Change>,
    binary: Vec<Vec<u8>>,
    queue: Option<Vec<Vec<u8>>>,
    error: Option<CString>,
}

struct BinaryResults(Result<Vec<Vec<u8>>, AutomergeError>);

/// A byte buffer
#[repr(C)]
pub struct Buffer {
    /// A pointer to the bytes
    data: *mut u8,
    /// The number of bytes
    data_len: usize,
}

/// A sequence of byte buffers that are contiguous in memory
//  This struct is accidentally an SoA layout, so it should be more performant!
#[repr(C)]
pub struct Buffers {
    /// A pointer to the bytes
    data: *mut u8,
    /// The total number of bytes across all buffers
    data_len: usize,
    /// The length (in bytes) of each buffer
    buf_lens: *mut usize,
    /// The number of buffers
    bufs: usize,
}

impl Deref for Backend {
    type Target = automerge_backend::Backend;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

unsafe fn from_buf_raw<T>(ptr: *const T, elts: usize) -> Vec<T> {
    let mut dst = Vec::with_capacity(elts);
    dst.set_len(elts);
    ptr::copy(ptr, dst.as_mut_ptr(), elts);
    dst
}

fn err<T, V: Debug>(result: Result<T, V>) -> Result<T, String> {
    match result {
        Ok(val) => Ok(val),
        Err(err) => Err(format!("{:?}", err)),
    }
}

impl Backend {
    fn init(handle: automerge_backend::Backend) -> Backend {
        Backend {
            handle,
            text: None,
            last_local_change: None,
            binary: Vec::new(),
            queue: None,
            error: None,
        }
    }

    //fn handle_result(&mut self, result: Result<isize, String>) -> isize {
    //    match result {
    //        Ok(len) => {
    //            self.error = None;
    //            len
    //        }
    //        Err(err) => self.handle_error(err),
    //    }
    //}

    //fn generate_json<T: Serialize>(&mut self, val: Result<T, AutomergeError>) -> isize {
    //    let result = err(val)
    //        .and_then(|val| err(serde_json::to_string(&val)))
    //        .map(|text| {
    //            let len = (text.len() + 1) as isize;
    //            self.text = Some(text);
    //            len
    //        });
    //    self.handle_result(result)
    //}

    //fn handle_binary(&mut self, b: Result<Vec<u8>, AutomergeError>) -> isize {
    //    let result = err(b).map(|bin| {
    //        let len = bin.len();
    //        self.binary = vec![bin];
    //        len as isize
    //    });
    //    self.handle_result(result)
    //}

    //fn handle_ok(&mut self) -> isize {
    //    self.error = None;
    //    0
    //}

    fn handle_error(&mut self, err: CError) -> isize {
        let c_error = match CString::new(format!("{:?}", err)) {
            Ok(e) => e,
            Err(e) => {
                return -1;
            }
        };
        self.error = Some(c_error);
        err.error_code()
    }

    //fn handle_binaries(&mut self, b: BinaryResults) -> isize {
    //    let result = err(b.0).map(|bin| {
    //        self.error = None;
    //        if !bin.is_empty() {
    //            let len = bin[0].len();
    //            self.binary = bin;
    //            self.binary.reverse();
    //            len as isize
    //        } else {
    //            0
    //        }
    //    });
    //    self.handle_result(result)
    //}
}

impl DerefMut for Backend {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.handle
    }
}

impl From<Backend> for *mut Backend {
    fn from(b: Backend) -> Self {
        Box::into_raw(Box::new(b))
    }
}

impl From<Vec<&Change>> for BinaryResults {
    fn from(changes: Vec<&Change>) -> Self {
        BinaryResults(Ok(changes.iter().map(|b| b.raw_bytes().into()).collect()))
    }
}

impl From<Result<Vec<&Change>, AutomergeError>> for BinaryResults {
    fn from(result: Result<Vec<&Change>, AutomergeError>) -> Self {
        BinaryResults(result.map(|changes| changes.iter().map(|b| b.raw_bytes().into()).collect()))
    }
}

impl From<Vec<ChangeHash>> for BinaryResults {
    fn from(heads: Vec<ChangeHash>) -> Self {
        BinaryResults(Ok(heads.iter().map(|head| head.0.to_vec()).collect()))
    }
}

/*
  init => automerge_init
  clone => automerge_clone
  free => automerge_free
  save => automerge_save
  load => automerge_load
  applyLocalChange => automerge_apply_local_change
  getPatch => automerge_get_patch
  applyChanges => automerge_apply_changes
  loadChanges => automerge_load_changes
  getChangesForActor => automerge_get_changes_for_actor
  getChanges => automerge_get_changes
  getMissingDeps => automerge_get_missing_deps
*/

#[no_mangle]
pub extern "C" fn automerge_init() -> *mut Backend {
    Backend::init(automerge_backend::Backend::new()).into()
}

/// # Safety
/// This must be called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_free(backend: *mut Backend) {
    // TODO: Can we do a null pointer check here by using `get_backend_mut`
    let backend: Backend = *Box::from_raw(backend);
    drop(backend)
}

// I dislike using macros but it saves me a bunch of typing
// This is especially true b/c the V2 backend returns a bunch more errors
/// Try to turn a `*mut Backend` into a &mut Backend,
/// return an error code if failure
macro_rules! get_backend_mut {
    ($backend:expr) => {{
        let backend = $backend.as_mut();
        match backend {
            Some(b) => b,
            // Don't call `handle_error` b/c there is no valid backend!
            None => return CError::NullBackend.error_code(),
        }
    }};
}

/// Try to turn a ptr + len into a utf-8 String,
/// return an error code if failure
macro_rules! from_json {
    ($backend:expr, $ptr:expr, $len:expr) => {{
        let mut slice = std::slice::from_raw_parts($ptr, $len);
        let s = String::from_utf8(slice.to_vec());
        let s = match s {
            Ok(s) => s,
            Err(e) => return $backend.handle_error(e.into()),
        };
        match serde_json::from_str(&s) {
            Ok(v) => v,
            Err(e) => return $backend.handle_error(e.into()),
        }
    }};
}

/// Try to call an Automerge method,
/// return an error code if failure
macro_rules! call_automerge {
    ($backend:expr, $expr:expr) => {
        match $expr {
            Ok(x) => x,
            Err(e) => return $backend.handle_error(e.into()),
        }
    };
}

/// Allocate a buffer struct on the heap
unsafe fn create_buf(buf: *mut *mut Buffer, bytes: &mut Box<[u8]>) {
    // https://users.rust-lang.org/t/understanding-rust-memory-allocation-api/60061
    let buffer = Box::new(Buffer {
        data_len: bytes.len(),
        data: bytes.as_mut_ptr(),
    });
    // TODO: Is this forget necessary if we do `into_raw`?
    // (My guess is, yes, the `Buffer` is managed by the box, the data the `Buffer`
    // points to is not so we have to manually forget it)
    std::mem::forget(bytes);
    let ptr = Box::into_raw(buffer);
    // By now, we are now responsible for freeing `Buffer` (& the memory it points to)
    *buf = ptr;
}

/// Free a buffer struct (which is on the heap)
/// and the memory it points to (which is also on the heap)
unsafe fn free_buf(buf: *mut Buffer) {
    // TODO: null ptr check?
    let slice = std::slice::from_raw_parts_mut((*buf).data, (*buf).data_len);
    let mut ptr = slice.as_mut_ptr();
    // The Boxes are dropped at the end of this function,
    // so the underlying memory is also dropped
    Box::from_raw(ptr);
    Box::from_raw(buf);
}

/// # Safety
/// This should be called with a valid backend pointer
/// and valid out pointers
#[no_mangle]
pub unsafe extern "C" fn automerge_apply_local_change(
    backend: *mut Backend,
    // More efficient if caller passes in length
    // (no need to call `strlen`)
    request: *const u8,
    len: usize,

    // Out params
    change_buf: *mut *mut Buffer,
    json_buf: *mut *mut Buffer,
) -> isize {
    let backend = get_backend_mut!(backend);
    let request: UncompressedChange = from_json!(backend, request, len);
    let (patch, mut change) = call_automerge!(backend, backend.apply_local_change(request));

    let bytes = change.raw_bytes();
    let mut buf: Vec<u8> = Vec::with_capacity(bytes.len());
    buf.extend(bytes);
    println!("{:?}", buf);
    //let ptr = bytes.as_mut_ptr();

    0
    //let v = vec![];
    //v.into_boxed_slice();
    //let x = Box::new(change.raw_bytes());
    //create_buf(change_buf, change.raw_bytes())
}

///// # Safety
///// This must me called with a valid backend pointer
///// request must be a valid pointer pointing to a cstring
//#[no_mangle]
//pub unsafe extern "C" fn automerge_apply_local_change(
//    backend: *mut Backend,
//    // More efficient if caller passes in length
//    // (no need to call `strlen`)
//    request: *const u8,
//    len: usize,
//
//    // Out params
//    change_buf: *mut *mut Buffer,
//    json_buf: *mut *mut Buffer,
//) -> isize {
//    let backend = get_backend_mut!(backend);
//    let request: UncompressedChange = from_json!(backend, request, len);
//    let (patch, change) = call_automerge!(backend, backend.apply_local_change(request));
//
//    let change_bytes = change.raw_bytes();
//
//    match request {
//        Ok(request) => {
//            let result = (*backend).apply_local_change(request);
//            match result {
//                Ok((patch, change)) => {
//                    (*backend).last_local_change = Some(change);
//                    (*backend).generate_json(Ok(patch))
//                }
//                Err(err) => (*backend).handle_error(err),
//            }
//        }
//        Err(err) => (*backend).handle_error(err),
//    }
//}
//
///// # Safety
///// This must me called with a valid backend pointer
///// change must point to a valid memory location with at least len bytes
//#[no_mangle]
//pub unsafe extern "C" fn automerge_write_change(
//    backend: *mut Backend,
//    len: usize,
//    change: *const u8,
//) {
//    let bytes = from_buf_raw(change, len);
//    if let Some(ref mut queue) = (*backend).queue {
//        queue.push(bytes)
//    } else {
//        (*backend).queue = Some(vec![bytes])
//    }
//}
//
///// # Safety
///// This must me called with a valid backend pointer
//#[no_mangle]
//pub unsafe extern "C" fn automerge_apply_changes(backend: *mut Backend) -> isize {
//    match (*backend).queue.take() {
//        Some(changes) => {
//            let changes = changes
//                .iter()
//                .map(|c| Change::from_bytes(c.to_vec()).unwrap())
//                .collect();
//            let patch = (*backend).apply_changes(changes);
//            (*backend).generate_json(patch)
//        }
//        None => (*backend).handle_error("no changes queued"),
//    }
//}
//
///// # Safety
///// This must me called with a valid backend pointer
//#[no_mangle]
//pub unsafe extern "C" fn automerge_get_patch(backend: *mut Backend) -> isize {
//    let patch = (*backend).get_patch();
//    (*backend).generate_json(patch)
//}
//
///// # Safety
///// This must me called with a valid backend pointer
//#[no_mangle]
//pub unsafe extern "C" fn automerge_load_changes(backend: *mut Backend) -> isize {
//    if let Some(changes) = (*backend).queue.take() {
//        let changes = changes
//            .iter()
//            .map(|c| Change::from_bytes(c.to_vec()).unwrap())
//            .collect();
//        if (*backend).load_changes(changes).is_ok() {
//            return (*backend).handle_ok();
//        }
//    }
//    (*backend).handle_error("no changes queued")
//}
//
///// # Safety
///// This must me called with a valid backend pointer
//#[no_mangle]
//pub unsafe extern "C" fn automerge_clone(backend: *mut Backend) -> *mut Backend {
//    (*backend).clone().into()
//}
//
///// # Safety
///// This must me called with a valid backend pointer
//#[no_mangle]
//pub unsafe extern "C" fn automerge_save(backend: *mut Backend) -> isize {
//    let data = (*backend).save();
//    (*backend).handle_binary(data)
//}
//
///// # Safety
///// data pointer must be a valid pointer to len bytes
//#[no_mangle]
//pub unsafe extern "C" fn automerge_load(len: usize, data: *const u8) -> *mut Backend {
//    let bytes = from_buf_raw(data, len);
//    let result = automerge_backend::Backend::load(bytes);
//    if let Ok(backend) = result {
//        Backend::init(backend).into()
//    } else {
//        set_errno(Errno(1));
//        ptr::null_mut()
//    }
//}
//
///// # Safety
///// This must me called with a valid backend pointer
//#[no_mangle]
//pub unsafe extern "C" fn automerge_get_changes_for_actor(
//    backend: *mut Backend,
//    actor: *const c_char,
//) -> isize {
//    let actor: &CStr = CStr::from_ptr(actor);
//    let actor = actor.to_string_lossy();
//    match actor.as_ref().try_into() {
//        Ok(actor) => {
//            let changes = (*backend).get_changes_for_actor_id(&actor);
//            (*backend).handle_binaries(changes.into())
//        }
//        Err(err) => (*backend).handle_error(err),
//    }
//}
//
///// # Safety
///// This must me called with a valid pointer to a change and the correct len
//#[no_mangle]
//pub unsafe extern "C" fn automerge_decode_change(
//    backend: *mut Backend,
//    len: usize,
//    change: *const u8,
//) -> isize {
//    let bytes = from_buf_raw(change, len);
//    let change = Change::from_bytes(bytes).unwrap();
//    (*backend).generate_json(Ok(change.decode()))
//}
//
///// # Safety
///// This must me called with a valid pointer a json string of a change
//#[no_mangle]
//pub unsafe extern "C" fn automerge_encode_change(
//    backend: *mut Backend,
//    change: *const c_char,
//) -> isize {
//    let change: &CStr = CStr::from_ptr(change);
//    let change = change.to_string_lossy();
//    let uncomp_change: UncompressedChange = serde_json::from_str(&change).unwrap();
//    let change: Change = uncomp_change.try_into().unwrap();
//    (*backend).handle_binary(Ok(change.raw_bytes().into()))
//}
//
///// # Safety
///// This must me called with a valid pointer to a backend
///// the automerge api changed to return a change and a patch
///// this C api was not designed to returned mixed values so i borrowed the
///// get_last_local_change call from the javascript api to solve the same problem
//#[no_mangle]
//pub unsafe extern "C" fn automerge_get_last_local_change(backend: *mut Backend) -> isize {
//    match (*backend).last_local_change.as_ref() {
//        Some(change) => (*backend).handle_binary(Ok(change.raw_bytes().into())),
//        None => (*backend).handle_error("no last change"),
//    }
//}
//
///// # Safety
///// This must me called with a valid pointer a json string of a change
//#[no_mangle]
//pub unsafe extern "C" fn automerge_get_heads(backend: *mut Backend) -> isize {
//    let heads = (*backend).get_heads();
//    (*backend).handle_binaries(heads.into())
//}
//
///// # Safety
///// This must me called with a valid backend pointer
///// binary must be a valid pointer to len bytes
//#[no_mangle]
//pub unsafe extern "C" fn automerge_get_changes(
//    backend: *mut Backend,
//    len: usize,
//    binary: *const u8,
//) -> isize {
//    let mut have_deps = Vec::new();
//    for i in 0..len {
//        have_deps.push(
//            from_buf_raw(binary.offset(i as isize * 32), 32)
//                .as_slice()
//                .try_into()
//                .unwrap(),
//        )
//    }
//    let changes = (*backend).get_changes(&have_deps);
//    (*backend).handle_binaries(Ok(changes).into())
//}
//
///// # Safety
///// This must me called with a valid backend pointer
///// binary must be a valid pointer to len bytes
//#[no_mangle]
//pub unsafe extern "C" fn automerge_get_missing_deps(
//    backend: *mut Backend,
//    len: usize,
//    binary: *const u8,
//) -> isize {
//    let mut heads = Vec::new();
//    for i in 0..len {
//        heads.push(
//            from_buf_raw(binary.offset(i as isize * 32), 32)
//                .as_slice()
//                .try_into()
//                .unwrap(),
//        )
//    }
//    let missing = (*backend).get_missing_deps(&heads);
//    (*backend).generate_json(Ok(missing))
//}
//
///// # Safety
///// This must me called with a valid backend pointer
//#[no_mangle]
//pub unsafe extern "C" fn automerge_error(backend: *mut Backend) -> *const c_char {
//    (*backend)
//        .error
//        .as_ref()
//        .map(|e| e.as_ptr())
//        .unwrap_or_else(|| ptr::null_mut())
//}
//
///// # Safety
///// This must me called with a valid backend pointer
///// and buffer must be a valid pointer of at least the number of bytes returned by the previous
///// call that generated a json result
//#[no_mangle]
//pub unsafe extern "C" fn automerge_read_json(backend: *mut Backend, buffer: *mut c_char) -> isize {
//    if let Some(text) = &(*backend).text {
//        let len = text.len();
//        buffer.copy_from(text.as_ptr().cast(), len);
//        (*buffer.add(len)) = 0; // null terminate
//        (*backend).text = None;
//        0
//    } else {
//        (*buffer) = 0;
//        (*backend).handle_error("no json to be read")
//    }
//}
//
///// # Safety
/////
///// This must me called with a valid backend pointer
///// the buffer must be a valid pointer pointing to at least as much space as was
///// required by the previous binary result call
//#[no_mangle]
//pub unsafe extern "C" fn automerge_read_binary(backend: *mut Backend, buffer: *mut u8) -> isize {
//    if let Some(bin) = (*backend).binary.pop() {
//        let len = bin.len();
//        buffer.copy_from(bin.as_ptr(), len);
//        if let Some(next) = (*backend).binary.last() {
//            next.len() as isize
//        } else {
//            0
//        }
//    } else {
//        (*backend).handle_error("no binary to be read")
//    }
//}
//
//#[derive(Debug)]
//pub struct SyncState {
//    handle: automerge_backend::SyncState,
//}
//
//impl From<SyncState> for *mut SyncState {
//    fn from(s: SyncState) -> Self {
//        Box::into_raw(Box::new(s))
//    }
//}
//
///// # Safety
///// Must be called with a valid backend pointer
///// sync_state must be a valid pointer to a SyncState
///// `encoded_msg_[ptr|len]` must be the address & length of a byte array
//// Returns an `isize` indicating the length of the patch as a JSON string
//// (-1 if there was an error, 0 if there is no patch)
//#[no_mangle]
//pub unsafe extern "C" fn automerge_receive_sync_message(
//    backend: *mut Backend,
//    sync_state: &mut SyncState,
//    encoded_msg_ptr: *const u8,
//    encoded_msg_len: usize,
//) -> isize {
//    let slice = std::slice::from_raw_parts(encoded_msg_ptr, encoded_msg_len);
//    let decoded = automerge_backend::SyncMessage::decode(slice);
//    let msg = match decoded {
//        Ok(msg) => msg,
//        Err(e) => {
//            return (*backend).handle_error(e);
//        }
//    };
//    let patch = (*backend).receive_sync_message(&mut sync_state.handle, msg);
//    if let Ok(None) = patch {
//        0
//    } else {
//        (*backend).generate_json(patch)
//    }
//}
//
///// # Safety
///// Must be called with a valid backend pointer
///// sync_state must be a valid pointer to a SyncState
///// Returns an `isize` indicating the length of the binary message
///// (-1 if there was an error, 0 if there is no message)
//#[no_mangle]
//pub unsafe extern "C" fn automerge_generate_sync_message(
//    backend: *mut Backend,
//    sync_state: &mut SyncState,
//) -> isize {
//    let msg = (*backend).generate_sync_message(&mut sync_state.handle);
//    if let Some(msg) = msg {
//        (*backend).handle_binary(msg.encode().or(Err(AutomergeError::EncodeFailed)))
//    } else {
//        0
//    }
//}
//
//#[no_mangle]
//pub extern "C" fn automerge_sync_state_init() -> *mut SyncState {
//    let state = SyncState {
//        handle: automerge_backend::SyncState::default(),
//    };
//    state.into()
//}
//
///// # Safety
///// Must be called with a valid backend pointer
///// sync_state must be a valid pointer to a SyncState
///// Returns an `isize` indicating the length of the binary message
///// (-1 if there was an error)
//#[no_mangle]
//pub unsafe extern "C" fn automerge_encode_sync_state(
//    backend: *mut Backend,
//    sync_state: &mut SyncState,
//) -> isize {
//    (*backend).handle_binary(
//        sync_state
//            .handle
//            .encode()
//            .or(Err(AutomergeError::EncodeFailed)),
//    )
//}
//
///// # Safety
///// `encoded_state_[ptr|len]` must be the address & length of a byte array
///// Returns an opaque pointer to a SyncState
///// panics (segfault?) if the buffer was invalid
//#[no_mangle]
//pub unsafe extern "C" fn automerge_decode_sync_state(
//    encoded_state_ptr: *const u8,
//    encoded_state_len: usize,
//) -> *mut SyncState {
//    let slice = std::slice::from_raw_parts(encoded_state_ptr, encoded_state_len);
//    let decoded_state = automerge_backend::SyncState::decode(slice);
//    // TODO: Is there a way to avoid `unwrap` here?
//    let state = decoded_state.unwrap();
//    let state = SyncState { handle: state };
//    state.into()
//}
//
///// # Safety
///// sync_state must be a valid pointer to a SyncState
//#[no_mangle]
//pub unsafe extern "C" fn automerge_sync_state_free(sync_state: *mut SyncState) {
//    let sync_state: SyncState = *Box::from_raw(sync_state);
//    drop(sync_state);
//}

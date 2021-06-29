extern crate automerge_backend;
extern crate errno;
extern crate libc;
extern crate serde;

use core::fmt::Debug;
use std::{
    borrow::Cow,
    convert::TryInto,
    ffi::{CStr, CString},
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    os::raw::c_char,
    ptr,
};

use automerge_backend::{AutomergeError, Change};
use automerge_protocol as amp;
use automerge_protocol::{error::InvalidActorId, ActorId, ChangeHash, Patch};
use errno::{set_errno, Errno};

// I dislike using macros but it saves me a bunch of typing
// This is especially true b/c the V2 backend returns a bunch more errors
// And we need to return an `isize` (not a Result), so we can't use the `?` operator

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

/// Turn a `*mut Buffer` into a `&mut Buffer`
macro_rules! get_buff_mut {
    ($buffs:expr) => {{
        let buffs = $buffs.as_mut();
        match buffs {
            Some(b) => b,
            None => return CError::NullBuffers.error_code(),
        }
    }};
}

macro_rules! get_data_vec {
    ($buff:expr) => {{
        let data: Vec<u8> = Vec::from_raw_parts($buff.data, $buff.len, $buff.cap);
        data
    }};
}

/// Turn a `*const CBuffers` into a `&CBuffers`
macro_rules! write_to_buff_epilogue {
    ($buff:expr, $vec:expr) => {{
        $buff.cap = $vec.capacity();
        $buff.len = $vec.len();
        $buff.data = $vec.as_mut_ptr();
        let _ = ManuallyDrop::new($vec);
    }};
}

/// Try to deserialize some bytes into a value using MessagePack
/// return an error code if failure
macro_rules! from_msgpack {
    ($backend:expr, $ptr:expr, $len:expr) => {{
        // Null pointer check?
        if $ptr.as_ref().is_none() {
            return $backend.handle_error(CError::NullChange);
        }
        let slice = std::slice::from_raw_parts($ptr, $len);
        match rmp_serde::from_read_ref(slice) {
            Ok(v) => v,
            Err(e) => return $backend.handle_error(CError::FromMessagePack(e)),
        }
    }};
}

/// Get hashes from a binary buffer
macro_rules! get_hashes {
    ($backend:expr, $bin:expr, $hashes:expr) => {{
        let mut hashes: Vec<ChangeHash> = vec![];
        if $hashes > 0 {
            let bytes: Vec<Vec<u8>> = from_msgpack!($backend, $bin, $hashes);
            for chunk in bytes {
                let hash: ChangeHash = match chunk.as_slice().try_into() {
                    Ok(v) => v,
                    Err(e) => return $backend.handle_error(CError::InvalidHashes(e.to_string())),
                };
                hashes.push(hash);
            }
        }
        hashes
    }};
}

/// Try to call an Automerge method,
/// return an error code if failure
macro_rules! call_automerge {
    ($backend:expr, $expr:expr) => {
        match $expr {
            Ok(x) => x,
            // We have to do `AutomergeError::from` to convert a `DecodeError` to a
            // `AutomergeError`
            Err(e) => return $backend.handle_error(CError::Automerge(AutomergeError::from(e))),
        }
    };
}

/// Get a `Vec<Change>` from a `*const CBuffers`
/// Using a macro instead of a method so we can return if there is an error
macro_rules! get_changes {
    ($backend:expr, $changes:expr, $len:expr) => {{
        let raws: Vec<Vec<u8>> = from_msgpack!($backend, $changes, $len);
        let mut changes = vec![];
        for raw in raws {
            let change = call_automerge!($backend, Change::from_bytes(raw));
            changes.push(change);
        }
        changes
    }};
}

/// All possible errors that a C caller could face
#[derive(thiserror::Error, Debug)]
pub enum CError {
    // TODO: The `NullBackend` and error is not attached to anything
    // (since normally we attach errors to a specific backend)
    // We could solve this by using a technique like this:
    // https://michael-f-bryan.github.io/rust-ffi-guide/errors/return_types.html
    // to create a `get_last_error_message` function, but the benefit seems very low
    // b/c the NullBackend error message is always the same
    #[error("Invalid pointer to Backend")]
    NullBackend,
    #[error("Invalid pointer to Buffers")]
    NullBuffers,
    #[error("Invalid pointer to CBuffers")]
    NullCBuffers,
    #[error("Invalid pointer to change")]
    NullChange,
    #[error("Invalid byte buffer of hashes: `{0}`")]
    InvalidHashes(String),
    #[error(transparent)]
    ToMessagePack(#[from] rmp_serde::encode::Error),
    #[error(transparent)]
    FromMessagePack(#[from] rmp_serde::decode::Error),
    #[error(transparent)]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error("No local change")]
    NoLocalChange,
    #[error(transparent)]
    Automerge(#[from] AutomergeError),
    #[error(transparent)]
    InvalidActorid(#[from] InvalidActorId),
}

impl CError {
    fn error_code(&self) -> isize {
        // 0 is reserved for "success"
        // TODO: This -1 code might be useless since we wipe the *actual* error code
        // and replace it with an uninformative`-1` that only tells us we couldn't
        // format the error message.
        // -1 is reserved for "we had an error & we could't convert it to a CString"
        const BASE: isize = 2;
        let code = match self {
            CError::NullBackend => BASE,
            CError::NullBuffers => BASE + 1,
            CError::NullCBuffers => BASE + 2,
            CError::NullChange => BASE + 3,
            CError::InvalidHashes(_) => BASE + 4,
            CError::ToMessagePack(_) => BASE + 5,
            CError::FromMessagePack(_) => BASE + 6,
            CError::FromUtf8(_) => BASE + 7,
            CError::InvalidActorid(_) => BASE + 8,
            CError::NoLocalChange => BASE + 9,
            CError::Automerge(_) => BASE + 10,
        };
        -code
    }
}

#[derive(Clone)]
pub struct Backend {
    handle: automerge_backend::Backend,
    error: Option<CString>,
    last_local_change: Option<Vec<u8>>,
}

/// A sequence of byte buffers that are contiguous in memory
/// The C caller allocates one of these with `create_buffs`
/// and passes it into each API call. This prevents allocating memory
/// on each call. The struct fields are just the constituent fields in a Vec
/// This is used for returning data to C.
//  This struct is accidentally an SoA layout, so it should be more performant!
#[repr(C)]
pub struct Buffer {
    /// A pointer to the bytes
    data: *mut u8,
    /// The amount of meaningful bytes
    len: usize,
    /// The total allocated memory `data` points to
    /// This is needed so Rust can free `data`
    cap: usize,
}

impl Backend {
    fn init(handle: automerge_backend::Backend) -> Backend {
        Backend {
            handle,
            error: None,
            last_local_change: None,
        }
    }

    fn handle_error(&mut self, err: CError) -> isize {
        let c_error = match CString::new(format!("{}", err)) {
            Ok(e) => e,
            Err(_) => {
                return -1;
            }
        };
        self.error = Some(c_error);
        err.error_code()
    }

    unsafe fn write_msgpack<T: serde::ser::Serialize>(
        &mut self,
        vals: &T,
        buffers: &mut Buffer,
    ) -> isize {
        match write_msgpack_to_buff(vals, buffers) {
            Ok(()) => 0,
            Err(e) => self.handle_error(CError::ToMessagePack(e)),
        }
    }
}

impl Deref for Backend {
    type Target = automerge_backend::Backend;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
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

#[no_mangle]
pub extern "C" fn automerge_init() -> *mut Backend {
    Backend::init(automerge_backend::Backend::new()).into()
}

/// # Safety
/// This must be called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_free(backend: *mut Backend) {
    Box::from_raw(backend);
}

/// Create a `Buffers` struct to store return values
#[no_mangle]
pub extern "C" fn automerge_create_buff() -> *mut Buffer {
    // Don't drop the vectors so their underlying buffers aren't de-allocated
    let mut data = ManuallyDrop::new(Vec::new());
    Box::into_raw(Box::new(Buffer {
        data: data.as_mut_ptr(),
        len: data.len(),
        cap: data.capacity(),
    }))
}

/// # Safety
/// Must point to a valid `Buffers` struct
/// Free the memory a `Buffers` struct points to
#[no_mangle]
pub unsafe extern "C" fn automerge_free_buff(buffs: *mut Buffer) -> isize {
    let buff = get_buff_mut!(buffs);
    // We construct the vec & drop it at the end of this function
    get_data_vec!(buff);
    0
}

unsafe fn write_msgpack_to_buff<T: serde::ser::Serialize>(
    vals: &T,
    buff: &mut Buffer,
) -> Result<(), rmp_serde::encode::Error> {
    let mut data = get_data_vec!(buff);
    let mut writer = std::io::Cursor::new(&mut data);
    rmp_serde::encode::write_named(&mut writer, &vals)?;
    write_to_buff_epilogue!(buff, data);
    Ok(())
}

unsafe fn write_bin_to_buff(bin: &[u8], buff: &mut Buffer) {
    let mut data = get_data_vec!(buff);
    data.set_len(0);
    data.extend(bin);
    write_to_buff_epilogue!(buff, data);
}

unsafe fn clear_buffs(buff: &mut Buffer) {
    let mut data = get_data_vec!(buff);
    data.set_len(0);
    write_to_buff_epilogue!(buff, data);
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
/// and a valid pointer to a `Buffers``
#[no_mangle]
pub unsafe extern "C" fn automerge_apply_local_change(
    backend: *mut Backend,
    buffs: *mut Buffer,
    request: *const u8,
    len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buff_mut!(buffs);
    let request: amp::Change = from_msgpack!(backend, request, len);
    let (patch, change) = call_automerge!(backend, backend.apply_local_change(request));
    backend.last_local_change = Some(change.raw_bytes().to_vec());
    backend.write_msgpack(&patch, buffs)
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn automerge_get_last_local_change(
    backend: *mut Backend,
    buffs: *mut Buffer,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buff = get_buff_mut!(buffs);
    let change = match &backend.last_local_change {
        Some(c) => c,
        None => return backend.handle_error(CError::NoLocalChange),
    };
    write_bin_to_buff(change, buff);
    0
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
/// `CBuffers` should be non-null & have valid fields.
#[no_mangle]
pub unsafe extern "C" fn automerge_apply_changes(
    backend: *mut Backend,
    buffs: *mut Buffer,
    changes: *const u8,
    changes_len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buff_mut!(buffs);
    let changes = get_changes!(backend, changes, changes_len);
    let patch = call_automerge!(backend, backend.apply_changes(changes));
    backend.write_msgpack(&patch, buffs)
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
/// and a valid pointer to a `Buffers``
#[no_mangle]
pub unsafe extern "C" fn automerge_get_patch(backend: *mut Backend, buffs: *mut Buffer) -> isize {
    let backend = get_backend_mut!(backend);
    let buff = get_buff_mut!(buffs);
    let patch = call_automerge!(backend, backend.get_patch());
    backend.write_msgpack(&patch, buff)
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
/// and a valid pointers to a `CBuffers`
#[no_mangle]
pub unsafe extern "C" fn automerge_load_changes(
    backend: *mut Backend,
    changes: *const u8,
    changes_len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let changes = get_changes!(backend, changes, changes_len);
    call_automerge!(backend, backend.load_changes(changes));
    0
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
#[no_mangle]
pub unsafe extern "C" fn automerge_save(backend: *mut Backend, buffs: *mut Buffer) -> isize {
    let backend = get_backend_mut!(backend);
    let buff = get_buff_mut!(buffs);
    let bin = call_automerge!(backend, backend.save());
    write_bin_to_buff(&bin, buff);
    0
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
#[no_mangle]
pub unsafe extern "C" fn automerge_clone(backend: *mut Backend, new: *mut *mut Backend) -> isize {
    let backend = get_backend_mut!(backend);
    (*new) = backend.clone().into();
    0
}

/// # Safety
/// This must be called with a valid pointer to len bytes
#[no_mangle]
pub unsafe extern "C" fn automerge_load(data: *const u8, len: usize) -> *mut Backend {
    let bytes = std::slice::from_raw_parts(data, len);
    let result = automerge_backend::Backend::load(bytes.to_vec());
    match result {
        Ok(b) => Backend::init(b).into(),
        Err(_) => {
            set_errno(Errno(1));
            ptr::null_mut()
        }
    }
}

/// # Safety
/// Lossily converts a C String into a Cow<...>
// TODO: Should we do UTF-8 check?
unsafe fn from_cstr<'a>(s: *const c_char) -> Cow<'a, str> {
    let s: &'a CStr = CStr::from_ptr(s);
    s.to_string_lossy()
}

/// # Safety
/// This must be called with a valid pointer to a `Backend`
/// and a valid C String
#[no_mangle]
pub unsafe extern "C" fn automerge_get_changes_for_actor(
    backend: *mut Backend,
    buffs: *mut Buffer,
    actor: *const c_char,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buff_mut!(buffs);
    let actor = from_cstr(actor);
    let actor_id: ActorId = match actor.as_ref().try_into() {
        Ok(id) => id,
        Err(e) => return backend.handle_error(CError::InvalidActorid(e)),
    };
    let changes = call_automerge!(backend, backend.get_changes_for_actor_id(&actor_id));
    let bytes: Vec<_> = changes
        .into_iter()
        .map(|c| c.raw_bytes().to_vec())
        .collect();
    backend.write_msgpack(&bytes, buffs)
}

/// # Safety
/// This must me called with a valid pointer to a change and the correct len
#[no_mangle]
pub unsafe extern "C" fn automerge_decode_change(
    backend: *mut Backend,
    buffs: *mut Buffer,
    change: *const u8,
    len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buff_mut!(buffs);
    let bytes = std::slice::from_raw_parts(change, len);
    let change = call_automerge!(backend, Change::from_bytes(bytes.to_vec()));
    backend.write_msgpack(&change.decode(), buffs);
    0
}

/// # Safety
/// This must me called with a valid pointer to a JSON string of a change
#[no_mangle]
pub unsafe extern "C" fn automerge_encode_change(
    backend: *mut Backend,
    buffs: *mut Buffer,
    change: *const u8,
    len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buff = get_buff_mut!(buffs);
    let uncomp: amp::Change = from_msgpack!(backend, change, len);
    // This should never panic?
    let change: Change = uncomp.try_into().unwrap();
    write_bin_to_buff(change.raw_bytes(), buff);
    0
}

/// # Safety
/// This must be called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_get_heads(backend: *mut Backend, buffs: *mut Buffer) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buff_mut!(buffs);
    let hashes = backend.get_heads();
    let bytes: Vec<_> = hashes.iter().map(|h| h.0.as_ref()).collect();
    backend.write_msgpack(&bytes, buffs)
}

/// # Safety
/// This must be called with a valid backend pointer,
/// binary must be a valid pointer to `hashes` hashes
#[no_mangle]
pub unsafe extern "C" fn automerge_get_changes(
    backend: *mut Backend,
    buffs: *mut Buffer,
    bin: *const u8,
    hashes: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buff_mut!(buffs);
    let hashes = get_hashes!(backend, bin, hashes);
    let changes = backend.get_changes(&hashes);
    let bytes: Vec<_> = changes
        .into_iter()
        .map(|c| c.raw_bytes().to_vec())
        .collect();
    backend.write_msgpack(&bytes, buffs)
}

/// # Safety
/// This must be called with a valid backend pointer,
/// binary must be a valid pointer to len bytes
#[no_mangle]
pub unsafe extern "C" fn automerge_get_missing_deps(
    backend: *mut Backend,
    buffs: *mut Buffer,
    bin: *const u8,
    len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buff_mut!(buffs);
    let heads = get_hashes!(backend, bin, len);
    let missing = backend.get_missing_deps(&heads);
    backend.write_msgpack(&missing, buffs)
}

/// # Safety
/// This must be called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_error(backend: *mut Backend) -> *const c_char {
    (*backend)
        .error
        .as_ref()
        .map(|e| e.as_ptr())
        .unwrap_or_else(|| ptr::null_mut())
}

#[derive(Debug)]
pub struct SyncState {
    handle: automerge_backend::SyncState,
}

impl From<SyncState> for *mut SyncState {
    fn from(s: SyncState) -> Self {
        Box::into_raw(Box::new(s))
    }
}

/// # Safety
/// Must be called with a valid backend pointer
/// sync_state must be a valid pointer to a SyncState
/// `encoded_msg_[ptr|len]` must be the address & length of a byte array
// Returns an `isize` indicating the length of the patch as a JSON string
// (-1 if there was an error, 0 if there is no patch)
#[no_mangle]
pub unsafe extern "C" fn automerge_receive_sync_message(
    backend: *mut Backend,
    buffs: *mut Buffer,
    sync_state: &mut SyncState,
    encoded_msg_ptr: *const u8,
    encoded_msg_len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buff_mut!(buffs);
    let slice = std::slice::from_raw_parts(encoded_msg_ptr, encoded_msg_len);
    let msg = call_automerge!(backend, automerge_backend::SyncMessage::decode(slice));
    let patch = call_automerge!(
        backend,
        backend.receive_sync_message(&mut sync_state.handle, msg)
    );
    if let Some(patch) = patch {
        backend.write_msgpack(&patch, buffs)
    } else {
        // There is nothing to return, clear the buffs
        clear_buffs(buffs);
        0
    }
}

/// # Safety
/// Must be called with a valid backend pointer
/// sync_state must be a valid pointer to a SyncState
/// Returns an `isize` indicating the length of the binary message
/// (-1 if there was an error, 0 if there is no message)
#[no_mangle]
pub unsafe extern "C" fn automerge_generate_sync_message(
    backend: *mut Backend,
    buffs: *mut Buffer,
    sync_state: &mut SyncState,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buff = get_buff_mut!(buffs);
    let msg = backend.generate_sync_message(&mut sync_state.handle);
    if let Some(msg) = msg {
        let bytes = call_automerge!(backend, msg.encode());
        write_bin_to_buff(&bytes, buff);
    } else {
        clear_buffs(buff);
    }
    0
}

#[no_mangle]
pub extern "C" fn automerge_sync_state_init() -> *mut SyncState {
    let state = SyncState {
        handle: automerge_backend::SyncState::default(),
    };
    state.into()
}

/// # Safety
/// sync_state must be a valid pointer to a SyncState
#[no_mangle]
pub unsafe extern "C" fn automerge_sync_state_free(sync_state: *mut SyncState) {
    let sync_state: SyncState = *Box::from_raw(sync_state);
    drop(sync_state);
}

/// # Safety
/// Must be called with a pointer to a valid Backend, sync_state, and buffs
#[no_mangle]
pub unsafe extern "C" fn automerge_encode_sync_state(
    backend: *mut Backend,
    buffs: *mut Buffer,
    sync_state: &mut SyncState,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buff_mut!(buffs);
    let encoded = call_automerge!(backend, sync_state.handle.encode());
    write_bin_to_buff(&encoded, buffs);
    0
}

/// # Safety
/// `encoded_state_[ptr|len]` must be the address & length of a byte array
#[no_mangle]
pub unsafe extern "C" fn automerge_decode_sync_state(
    backend: *mut Backend,
    encoded_state_ptr: *const u8,
    encoded_state_len: usize,
    sync_state: *mut *mut SyncState,
) -> isize {
    let backend = get_backend_mut!(backend);
    let slice = std::slice::from_raw_parts(encoded_state_ptr, encoded_state_len);
    let decoded_state = call_automerge!(backend, automerge_backend::SyncState::decode(slice));
    let state = SyncState {
        handle: decoded_state,
    };
    (*sync_state) = state.into();
    0
}

/// # Safety
/// This must be called with a valid C-string
#[no_mangle]
pub unsafe extern "C" fn debug_json_change_to_msgpack(
    change: *const c_char,
    out_msgpack: *mut *mut u8,
    out_len: *mut usize,
) -> isize {
    let s = from_cstr(change);
    // `unwrap` here is ok b/c this is a debug function
    let uncomp: amp::Change = serde_json::from_str(&s).unwrap();

    // `unwrap` here is ok b/c this is a debug function
    let mut bytes = ManuallyDrop::new(rmp_serde::to_vec_named(&uncomp).unwrap());
    *out_msgpack = bytes.as_mut_ptr();
    *out_len = bytes.len();
    0
}

/// # Safety
/// `prefix` & `buff` must be valid pointers
#[no_mangle]
pub unsafe extern "C" fn debug_print_msgpack_patch(
    prefix: *const c_char,
    buff: *const u8,
    len: usize,
) {
    if prefix.is_null() {
        panic!("null ptr: prefix");
    }
    if buff.is_null() {
        panic!("null ptr: buff");
    }
    if len == 0 {
        panic!("invalid len: 0");
    }
    let prefix = from_cstr(prefix);
    let slice = std::slice::from_raw_parts(buff, len);
    let patch: Patch = rmp_serde::from_read_ref(slice).unwrap();
    let as_json = serde_json::to_string(&patch).unwrap();
    println!("{}: {}", prefix, as_json);
}

/// # Safety
/// This must be called with a valid pointer to len bytes
#[no_mangle]
pub unsafe extern "C" fn debug_msgpack_change_to_json(
    msgpack: *const u8,
    len: usize,
    out_json: *mut u8,
) -> isize {
    let slice = std::slice::from_raw_parts(msgpack, len);
    let uncomp: amp::Change = rmp_serde::from_slice(slice).unwrap();
    let json = serde_json::to_vec(&uncomp).unwrap();
    ptr::copy_nonoverlapping(json.as_ptr(), out_json, json.len());
    // null-terminate
    *out_json.add(json.len()) = 0;
    json.len() as isize
}

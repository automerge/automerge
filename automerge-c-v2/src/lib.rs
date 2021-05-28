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
use automerge_protocol::{error::InvalidActorId, ActorId, ChangeHash, Patch, UncompressedChange};
use errno::{set_errno, Errno};
use rmp_serde;

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

/// Turn a `*mut Buffers` into a `&mut Buffers`
macro_rules! get_buffs_mut {
    ($buffs:expr) => {{
        let buffs = $buffs.as_mut();
        match buffs {
            Some(b) => b,
            None => return CError::NullBuffers.error_code(),
        }
    }};
}

/// Turn a `*const CBuffers` into a `&CBuffers`
macro_rules! get_cbuffs {
    ($backend:expr, $buffs:expr) => {{
        let buffs = $buffs.as_ref();
        match buffs {
            Some(b) => b,
            None => return $backend.handle_error(CError::NullCBuffers),
        }
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

/// Try to serialize a value to bytes using msgpack
/// return an error code if failure
macro_rules! to_msgpack {
    ($backend:expr,$val:expr) => {{
        match rmp_serde::to_vec_named($val) {
            Ok(v) => v,
            Err(e) => return $backend.handle_error(CError::ToMessagePack(e)),
        }
    }};
}

/// Get hashes from a binary buffer
macro_rules! get_hashes {
    ($backend:expr, $bin:expr, $hashes:expr) => {{
        let mut hashes: Vec<ChangeHash> = vec![];
        if $hashes > 0 {
            let slice = std::slice::from_raw_parts($bin, 32 * $hashes);
            let iter = slice.chunks_exact(32);
            let rem = iter.remainder().len();
            if rem > 0 {
                return $backend.handle_error(CError::InvalidHashes(format!(
                    "Byte buffer had: {} leftover bytes",
                    rem
                )));
            }
            for chunk in iter {
                let hash: ChangeHash = match chunk.try_into() {
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
    ($backend:expr, $cbuffs:expr) => {{
        let cbuffs = get_cbuffs!($backend, $cbuffs);
        let lens = std::slice::from_raw_parts(cbuffs.lens, cbuffs.lens_len);
        let data = std::slice::from_raw_parts(cbuffs.data, cbuffs.data_len);
        let mut changes = vec![];
        let mut cur_pos = 0;
        for len in lens {
            let buff = data[cur_pos..cur_pos + len].to_vec();
            let change = call_automerge!($backend, Change::from_bytes(buff));
            changes.push(change);
            cur_pos += len;
        }
        changes
    }};
}

macro_rules! get_data_vec {
    ($buffs:expr) => {{
        let v: Vec<u8> = Vec::from_raw_parts($buffs.data, $buffs.data_len, $buffs.data_cap);
        v
    }};
}

macro_rules! get_buff_lens_vec {
    ($buffs:expr) => {{
        let v: Vec<usize> = Vec::from_raw_parts($buffs.lens, $buffs.lens_len, $buffs.lens_cap);
        v
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
            CError::Automerge(_) => BASE + 9,
        };
        -code
    }
}

#[derive(Clone)]
pub struct Backend {
    handle: automerge_backend::Backend,
    error: Option<CString>,
}

/// A sequence of byte buffers that are contiguous in memory
/// The C caller allocates one of these with `create_buffs`
/// and passes it into each API call. This prevents allocating memory
/// on each call. The struct fields are just the constituent fields in a Vec
/// This is used for returning data to C.
//  This struct is accidentally an SoA layout, so it should be more performant!
#[repr(C)]
pub struct Buffers {
    /// A pointer to the bytes
    data: *mut u8,
    /// The total number of bytes across all buffers
    // TODO: This might not be needed b/c it can be calculated from `lens`
    data_len: usize,
    /// The total allocated memory `data` points to
    /// This is needed so Rust can free `data`
    data_cap: usize,
    /// The length (in bytes) of each buffer
    lens: *mut usize,
    /// The number of buffers
    lens_len: usize,
    /// The total allocated memory `buf_lens` points to
    /// This is needed so Rust can free `buf_lens`
    lens_cap: usize,
}

/// Similar to `Buffers`, except this struct
/// should be allocated / freed by C.
/// Used to pass an the C-equivalent of `Vec<Vec<u8>>` to Rust
// We don't need the `*_cap` fields b/c the Rust code
// doesn't need to free the referenced memory
#[repr(C)]
pub struct CBuffers {
    data: *mut u8,
    // TODO: This field isn't strictly needed since it can
    // just be calculated from `lens`. But it seems useful to
    // include since it will probably be known by the caller anyways?
    data_len: usize,
    lens: *mut usize,
    lens_len: usize,
}

impl Backend {
    fn init(handle: automerge_backend::Backend) -> Backend {
        Backend {
            handle,
            error: None,
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
        val: &T,
        buffers: &mut Buffers,
    ) -> isize {
        let bytes = to_msgpack!(self, val);
        write_to_buffs(vec![&bytes], buffers);
        0
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
    // TODO: Can we do a null pointer check here by using `get_backend_mut`
    let backend: Backend = *Box::from_raw(backend);
    drop(backend)
}

/// Create a `Buffers` struct to store return values
#[no_mangle]
pub extern "C" fn automerge_create_buffs() -> Buffers {
    // Don't drop the vectors so their underlying buffers aren't de-allocated
    let mut data = ManuallyDrop::new(Vec::new());
    let mut lens = ManuallyDrop::new(Vec::new());
    Buffers {
        data: data.as_mut_ptr(),
        data_len: data.len(),
        data_cap: data.capacity(),
        lens: lens.as_mut_ptr(),
        lens_len: lens.len(),
        lens_cap: lens.capacity(),
    }
}

/// # Safety
/// Must point to a valid `Buffers` struct
/// Free the memory a `Buffers` struct points to
#[no_mangle]
pub unsafe extern "C" fn automerge_free_buffs(buffs: *mut Buffers) -> isize {
    let buffs = get_buffs_mut!(buffs);
    // We construct the vecs & drop them at the end of this function
    get_data_vec!(buffs);
    get_buff_lens_vec!(buffs);
    0
}

// util_* functions could be implemented in the parent language,
// I have just written them in Rust b/c that's what I'm familiar with

/// # Safety
/// Must be called with a valid buffs pointer
/// Copy a single buff from a Buffers to a destination.
/// The destination must be large enough to hold all of dest
#[no_mangle]
pub unsafe extern "C" fn util_read_buffs(
    buffs: *const Buffers,
    idx: usize,
    dest: *mut u8,
) -> usize {
    let data = std::slice::from_raw_parts((*buffs).data, (*buffs).data_cap);
    let lens = std::slice::from_raw_parts((*buffs).lens, (*buffs).lens_cap);
    if idx >= (*buffs).lens_len {
        // This panic is ok b/c it indicates a truly unrecoverable error
        panic!("idx: {} out of range: {}", idx, (*buffs).lens_len);
    }
    let start: usize = lens.iter().take(idx).sum();
    let len = lens[idx];
    let end = start + len;
    let buff = &data[start..end];
    ptr::copy_nonoverlapping(buff.as_ptr(), dest, len);
    len
}

/// # Safety
/// Must be called with a valid buffs pointer
/// Copy a single buff from a Buffers to a destination & null terminate it
/// The destination must be large enough to hold all of dest
#[no_mangle]
pub unsafe extern "C" fn util_read_buffs_str(buffs: *mut Buffers, idx: usize, dest: *mut u8) {
    let len = util_read_buffs(buffs, idx, dest);
    // null terminate
    *dest.add(len) = 0;
}

/// # Safety
/// This function should not fail insofar the fields of `buffs` are valid
/// Write the contents of a `Vec<&[u8]>` to the `data` field of a `Buffers` struct
/// Write the lengths of each `&[u8]` to the `lens` field of a `Buffers` struct
/// Re-allocate the the buffers `data` and `lens` point to if they aren't large enough
/// and update the `*_cap` fields as appropriate. Always update the `*_len` fields.
unsafe fn write_to_buffs(bytes: Vec<&[u8]>, buffs: &mut Buffers) {
    let total_buffs: usize = bytes.len();
    let mut buf_lens: ManuallyDrop<Vec<usize>> =
        ManuallyDrop::new(if total_buffs > buffs.lens_cap {
            // Drop the old `Vec` so its underlying memory is freed
            get_buff_lens_vec!(buffs);
            // Create a new vec that can store `total_buffs` worth of usizes
            let mut v = Vec::with_capacity(total_buffs);
            buffs.lens_cap = total_buffs;
            buffs.lens = v.as_mut_ptr();
            v
        } else {
            get_buff_lens_vec!(buffs)
        });
    buf_lens.set_len(total_buffs);

    let total_bytes: usize = bytes.iter().map(|b| b.len()).sum();
    let mut data: ManuallyDrop<Vec<u8>> = ManuallyDrop::new(if total_bytes > buffs.data_cap {
        // Drop the old `Vec` so its underlying memory is freed
        get_data_vec!(buffs);
        // Create a new vec that can store `total_bytes` worth of bytes
        let mut v = Vec::with_capacity(total_bytes);
        buffs.data_cap = total_bytes;
        buffs.data = v.as_mut_ptr();
        v
    } else {
        get_data_vec!(buffs)
    });
    // Prevents out of bounds errors when writing to `data`
    data.set_len(total_bytes);

    let mut start = 0;
    // Write `bytes` to `data` & update `buf_lens`
    for (idx, buff) in bytes.iter().enumerate() {
        data[start..start + buff.len()].copy_from_slice(buff);
        buf_lens[idx] = buff.len();
        start += buff.len();
    }

    buffs.data_len = total_bytes;
    buffs.lens_len = total_buffs;
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
/// and a valid pointer to a `Buffers``
#[no_mangle]
pub unsafe extern "C" fn automerge_apply_local_change(
    backend: *mut Backend,
    buffs: *mut Buffers,
    request: *const u8,
    len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let request: UncompressedChange = from_msgpack!(backend, request, len);
    let (patch, change) = call_automerge!(backend, backend.apply_local_change(request));
    let patch_bytes = to_msgpack!(backend, &patch);
    write_to_buffs(vec![change.raw_bytes(), &patch_bytes], buffs);
    0
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
/// `CBuffers` should be non-null & have valid fields.
#[no_mangle]
pub unsafe extern "C" fn automerge_apply_changes(
    backend: *mut Backend,
    buffs: *mut Buffers,
    cbuffs: *const CBuffers,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let changes = get_changes!(backend, cbuffs);
    let patch = call_automerge!(backend, backend.apply_changes(changes));
    backend.write_msgpack(&patch, buffs)
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
/// and a valid pointer to a `Buffers``
#[no_mangle]
pub unsafe extern "C" fn automerge_get_patch(backend: *mut Backend, buffs: *mut Buffers) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let patch = call_automerge!(backend, backend.get_patch());
    backend.write_msgpack(&patch, buffs)
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
/// and a valid pointers to a `CBuffers`
#[no_mangle]
pub unsafe extern "C" fn automerge_load_changes(
    backend: *mut Backend,
    cbuffs: *const CBuffers,
) -> isize {
    let backend = get_backend_mut!(backend);
    let changes = get_changes!(backend, cbuffs);
    call_automerge!(backend, backend.load_changes(changes));
    0
}

/// # Safety
/// This should be called with a valid pointer to a `Backend`
#[no_mangle]
pub unsafe extern "C" fn automerge_save(backend: *mut Backend, buffs: *mut Buffers) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let bin = call_automerge!(backend, backend.save());
    write_to_buffs(vec![&bin], buffs);
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
    buffs: *mut Buffers,
    actor: *const c_char,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let actor = from_cstr(actor);
    let actor_id: ActorId = match actor.as_ref().try_into() {
        Ok(id) => id,
        Err(e) => return backend.handle_error(CError::InvalidActorid(e)),
    };
    let changes = call_automerge!(backend, backend.get_changes_for_actor_id(&actor_id));
    let bytes: Vec<_> = changes.into_iter().map(|c| c.raw_bytes()).collect();
    write_to_buffs(bytes, buffs);
    0
}

/// # Safety
/// This must me called with a valid pointer to a change and the correct len
#[no_mangle]
pub unsafe extern "C" fn automerge_decode_change(
    backend: *mut Backend,
    buffs: *mut Buffers,
    change: *const u8,
    len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
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
    buffs: *mut Buffers,
    change: *const u8,
    len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let uncomp: UncompressedChange = from_msgpack!(backend, change, len);
    // This should never panic?
    let change: Change = uncomp.try_into().unwrap();
    write_to_buffs(vec![change.raw_bytes()], buffs);
    0
}

/// # Safety
/// This must be called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_get_heads(backend: *mut Backend, buffs: *mut Buffers) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let hashes = backend.get_heads();
    let bytes: Vec<_> = hashes.iter().map(|h| h.0.as_ref()).collect();
    write_to_buffs(bytes, buffs);
    0
}

/// # Safety
/// This must be called with a valid backend pointer,
/// binary must be a valid pointer to `hashes` hashes
#[no_mangle]
pub unsafe extern "C" fn automerge_get_changes(
    backend: *mut Backend,
    buffs: *mut Buffers,
    bin: *const u8,
    hashes: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let hashes = get_hashes!(backend, bin, hashes);
    let changes = backend.get_changes(&hashes);
    let bytes: Vec<_> = changes.into_iter().map(|c| c.raw_bytes()).collect();
    write_to_buffs(bytes, buffs);
    0
}

/// # Safety
/// This must be called with a valid backend pointer,
/// binary must be a valid pointer to len bytes
#[no_mangle]
pub unsafe extern "C" fn automerge_get_missing_deps(
    backend: *mut Backend,
    buffs: *mut Buffers,
    bin: *const u8,
    len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let heads = get_hashes!(backend, bin, len);
    let missing = backend.get_missing_deps(&heads);
    backend.write_msgpack(&missing, buffs);
    0
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
    buffs: *mut Buffers,
    sync_state: &mut SyncState,
    encoded_msg_ptr: *const u8,
    encoded_msg_len: usize,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let slice = std::slice::from_raw_parts(encoded_msg_ptr, encoded_msg_len);
    let msg = call_automerge!(backend, automerge_backend::SyncMessage::decode(slice));
    let patch = call_automerge!(
        backend,
        backend.receive_sync_message(&mut sync_state.handle, msg)
    );

    if let Some(patch) = patch {
        backend.write_msgpack(&patch, buffs);
    } else {
        // There is nothing to return, clear the buffs
        write_to_buffs(vec![], buffs);
    }
    0
}

/// # Safety
/// Must be called with a valid backend pointer
/// sync_state must be a valid pointer to a SyncState
/// Returns an `isize` indicating the length of the binary message
/// (-1 if there was an error, 0 if there is no message)
#[no_mangle]
pub unsafe extern "C" fn automerge_generate_sync_message(
    backend: *mut Backend,
    buffs: *mut Buffers,
    sync_state: &mut SyncState,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let msg = backend.generate_sync_message(&mut sync_state.handle);
    if let Some(msg) = msg {
        let bytes = call_automerge!(backend, msg.encode());
        write_to_buffs(vec![&bytes], buffs);
    } else {
        write_to_buffs(vec![], buffs);
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
    buffs: *mut Buffers,
    sync_state: &mut SyncState,
) -> isize {
    let backend = get_backend_mut!(backend);
    let buffs = get_buffs_mut!(buffs);
    let encoded = call_automerge!(backend, sync_state.handle.encode());
    write_to_buffs(vec![&encoded], buffs);
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
    let uncomp: UncompressedChange = serde_json::from_str(&s).unwrap();

    // `unwrap` here is ok b/c this is a debug function
    let mut bytes = ManuallyDrop::new(rmp_serde::to_vec_named(&uncomp).unwrap());
    *out_msgpack = bytes.as_mut_ptr();
    *out_len = bytes.len();
    0
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
    let uncomp: UncompressedChange = rmp_serde::from_slice(slice).unwrap();
    let json = serde_json::to_vec(&uncomp).unwrap();
    ptr::copy_nonoverlapping(json.as_ptr(), out_json, json.len());
    // null-terminate
    *out_json.add(json.len()) = 0;
    json.len() as isize
}

/// # Safety
/// This must be called with a valid pointer to len bytes
#[no_mangle]
pub unsafe extern "C" fn debug_msgpack_patch_to_json(
    msgpack: *const u8,
    len: usize,
    out_json: *mut u8,
) -> isize {
    let slice = std::slice::from_raw_parts(msgpack, len);
    let patch: Patch = rmp_serde::from_slice(slice).unwrap();
    let json = serde_json::to_vec(&patch).unwrap();
    ptr::copy_nonoverlapping(json.as_ptr(), out_json, json.len());
    // null-terminate
    *out_json.add(json.len()) = 0;
    json.len() as isize
}

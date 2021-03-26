extern crate automerge_backend;
extern crate errno;
extern crate libc;
extern crate serde;

use automerge_backend::{AutomergeError, Change};
use automerge_protocol::{ChangeHash, UncompressedChange};
use core::fmt::Debug;
use errno::{set_errno, Errno};
use serde::ser::Serialize;
use std::convert::TryInto;
use std::ffi::{CStr, CString};
use std::ops::{Deref, DerefMut};
use std::os::raw::c_char;
use std::ptr;
use std::rc::Rc;

#[derive(Clone)]
pub struct Backend {
    handle: automerge_backend::Backend,
    text: Option<String>,
    last_local_change: Option<Rc<Change>>,
    binary: Vec<Vec<u8>>,
    queue: Option<Vec<Vec<u8>>>,
    error: Option<CString>,
}

struct BinaryResults(Result<Vec<Vec<u8>>, AutomergeError>);

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

    fn handle_result(&mut self, result: Result<isize, String>) -> isize {
        match result {
            Ok(len) => {
                self.error = None;
                len
            }
            Err(err) => self.handle_error(err),
        }
    }

    fn generate_json<T: Serialize>(&mut self, val: Result<T, AutomergeError>) -> isize {
        let result = err(val)
            .and_then(|val| err(serde_json::to_string(&val)))
            .map(|text| {
                let len = (text.len() + 1) as isize;
                self.text = Some(text);
                len
            });
        self.handle_result(result)
    }

    fn handle_binary(&mut self, b: Result<Vec<u8>, AutomergeError>) -> isize {
        let result = err(b).map(|bin| {
            let len = bin.len();
            self.binary = vec![bin];
            len as isize
        });
        self.handle_result(result)
    }

    fn handle_ok(&mut self) -> isize {
        self.error = None;
        0
    }

    fn handle_error<E: Debug>(&mut self, err: E) -> isize {
        // in theory - if an error string had embedded nulls
        // we could get a error = None and -1
        self.error = CString::new(format!("{:?}", err)).ok();
        -1
    }

    fn handle_binaries(&mut self, b: BinaryResults) -> isize {
        let result = err(b.0).map(|bin| {
            self.error = None;
            if !bin.is_empty() {
                let len = bin[0].len();
                self.binary = bin;
                self.binary.reverse();
                len as isize
            } else {
                0
            }
        });
        self.handle_result(result)
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
    Backend::init(automerge_backend::Backend::init()).into()
}

/// # Safety
/// This must me called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_free(backend: *mut Backend) {
    let backend: Backend = *Box::from_raw(backend);
    drop(backend)
}

/// # Safety
/// This must me called with a valid backend pointer
/// request must be a valid pointer pointing to a cstring
#[no_mangle]
pub unsafe extern "C" fn automerge_apply_local_change(
    backend: *mut Backend,
    request: *const c_char,
) -> isize {
    let request: &CStr = CStr::from_ptr(request);
    let request = request.to_string_lossy();
    let request: Result<UncompressedChange, _> = serde_json::from_str(&request);
    match request {
        Ok(request) => {
            let result = (*backend).apply_local_change(request);
            match result {
                Ok((patch, change)) => {
                    (*backend).last_local_change = Some(change);
                    (*backend).generate_json(Ok(patch))
                }
                Err(err) => (*backend).handle_error(err),
            }
        }
        Err(err) => (*backend).handle_error(err),
    }
}

/// # Safety
/// This must me called with a valid backend pointer
/// change must point to a valid memory location with at least len bytes
#[no_mangle]
pub unsafe extern "C" fn automerge_write_change(
    backend: *mut Backend,
    len: usize,
    change: *const u8,
) {
    let bytes = from_buf_raw(change, len);
    if let Some(ref mut queue) = (*backend).queue {
        queue.push(bytes)
    } else {
        (*backend).queue = Some(vec![bytes])
    }
}

/// # Safety
/// This must me called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_apply_changes(backend: *mut Backend) -> isize {
    match (*backend).queue.take() {
        Some(changes) => {
            let changes = changes
                .iter()
                .map(|c| Change::from_bytes(c.to_vec()).unwrap())
                .collect();
            let patch = (*backend).apply_changes(changes);
            (*backend).generate_json(patch)
        }
        None => (*backend).handle_error("no changes queued"),
    }
}

/// # Safety
/// This must me called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_get_patch(backend: *mut Backend) -> isize {
    let patch = (*backend).get_patch();
    (*backend).generate_json(patch)
}

/// # Safety
/// This must me called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_load_changes(backend: *mut Backend) -> isize {
    if let Some(changes) = (*backend).queue.take() {
        let changes = changes
            .iter()
            .map(|c| Change::from_bytes(c.to_vec()).unwrap())
            .collect();
        if (*backend).load_changes(changes).is_ok() {
            return (*backend).handle_ok();
        }
    }
    (*backend).handle_error("no changes queued")
}

/// # Safety
/// This must me called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_clone(backend: *mut Backend) -> *mut Backend {
    (*backend).clone().into()
}

/// # Safety
/// This must me called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_save(backend: *mut Backend) -> isize {
    let data = (*backend).save();
    (*backend).handle_binary(data)
}

/// # Safety
/// data pointer must be a valid pointer to len bytes
#[no_mangle]
pub unsafe extern "C" fn automerge_load(len: usize, data: *const u8) -> *mut Backend {
    let bytes = from_buf_raw(data, len);
    let result = automerge_backend::Backend::load(bytes);
    if let Ok(backend) = result {
        Backend::init(backend).into()
    } else {
        set_errno(Errno(1));
        ptr::null_mut()
    }
}

/// # Safety
/// This must me called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_get_changes_for_actor(
    backend: *mut Backend,
    actor: *const c_char,
) -> isize {
    let actor: &CStr = CStr::from_ptr(actor);
    let actor = actor.to_string_lossy();
    match actor.as_ref().try_into() {
        Ok(actor) => {
            let changes = (*backend).get_changes_for_actor_id(&actor);
            (*backend).handle_binaries(changes.into())
        }
        Err(err) => (*backend).handle_error(err),
    }
}

/// # Safety
/// This must me called with a valid pointer to a change and the correct len
#[no_mangle]
pub unsafe extern "C" fn automerge_decode_change(
    backend: *mut Backend,
    len: usize,
    change: *const u8,
) -> isize {
    let bytes = from_buf_raw(change, len);
    let change = Change::from_bytes(bytes).unwrap();
    (*backend).generate_json(Ok(change.decode()))
}

/// # Safety
/// This must me called with a valid pointer a json string of a change
#[no_mangle]
pub unsafe extern "C" fn automerge_encode_change(
    backend: *mut Backend,
    change: *const c_char,
) -> isize {
    let change: &CStr = CStr::from_ptr(change);
    let change = change.to_string_lossy();
    let uncomp_change: UncompressedChange = serde_json::from_str(&change).unwrap();
    let change: Change = uncomp_change.try_into().unwrap();
    (*backend).handle_binary(Ok(change.raw_bytes().into()))
}

/// # Safety
/// This must me called with a valid pointer to a backend
/// the automerge api changed to return a change and a patch
/// this C api was not designed to returned mixed values so i borrowed the
/// get_last_local_change call from the javascript api to solve the same problem
#[no_mangle]
pub unsafe extern "C" fn automerge_get_last_local_change(backend: *mut Backend) -> isize {
    match (*backend).last_local_change.as_ref() {
        Some(change) => (*backend).handle_binary(Ok(change.raw_bytes().into())),
        None => (*backend).handle_error("no last change"),
    }
}

/// # Safety
/// This must me called with a valid pointer a json string of a change
#[no_mangle]
pub unsafe extern "C" fn automerge_get_heads(backend: *mut Backend) -> isize {
    let heads = (*backend).get_heads();
    (*backend).handle_binaries(heads.into())
}

/// # Safety
/// This must me called with a valid backend pointer
/// binary must be a valid pointer to len bytes
#[no_mangle]
pub unsafe extern "C" fn automerge_get_changes(
    backend: *mut Backend,
    len: usize,
    binary: *const u8,
) -> isize {
    let mut have_deps = Vec::new();
    for i in 0..len {
        have_deps.push(
            from_buf_raw(binary.offset(i as isize * 32), 32)
                .as_slice()
                .try_into()
                .unwrap(),
        )
    }
    let changes = (*backend).get_changes(&have_deps);
    (*backend).handle_binaries(Ok(changes).into())
}

/// # Safety
/// This must me called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_get_missing_deps(backend: *mut Backend) -> isize {
    let missing = (*backend).get_missing_deps();
    (*backend).generate_json(Ok(missing))
}

/// # Safety
/// This must me called with a valid backend pointer
#[no_mangle]
pub unsafe extern "C" fn automerge_error(backend: *mut Backend) -> *const c_char {
    (*backend)
        .error
        .as_ref()
        .map(|e| e.as_ptr())
        .unwrap_or_else(|| ptr::null_mut())
}

/// # Safety
/// This must me called with a valid backend pointer
/// and buffer must be a valid pointer of at least the number of bytes returned by the previous
/// call that generated a json result
#[no_mangle]
pub unsafe extern "C" fn automerge_read_json(backend: *mut Backend, buffer: *mut c_char) -> isize {
    if let Some(text) = &(*backend).text {
        let len = text.len();
        buffer.copy_from(text.as_ptr().cast(), len);
        (*buffer.add(len)) = 0; // null terminate
        (*backend).text = None;
        0
    } else {
        (*buffer) = 0;
        (*backend).handle_error("no json to be read")
    }
}

/// # Safety
///
/// This must me called with a valid backend pointer
/// the buffer must be a valid pointer pointing to at least as much space as was
/// required by the previous binary result call
#[no_mangle]
pub unsafe extern "C" fn automerge_read_binary(backend: *mut Backend, buffer: *mut u8) -> isize {
    if let Some(bin) = (*backend).binary.pop() {
        let len = bin.len();
        buffer.copy_from(bin.as_ptr(), len);
        if let Some(next) = (*backend).binary.last() {
            next.len() as isize
        } else {
            0
        }
    } else {
        (*backend).handle_error("no binary to be read")
    }
}
